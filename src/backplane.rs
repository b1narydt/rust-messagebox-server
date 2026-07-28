//! Model B backplane — cross-instance live push over Redis pub/sub, with
//! **directed per-room routing**.
//!
//! One binary, topology chosen by config (transport-architecture WS2-3):
//! no `REDIS_URL` → **Model A** (single self-contained instance, in-process
//! routing — the default); `REDIS_URL` set → **Model B** (N replicas behind a
//! sticky LB, this backplane bridging live push between them).
//!
//! ## Directed routing (no fan-out)
//!
//! Each room `{identityKey}-{messageBox}` maps to its own Redis channel,
//! [`room_channel`] = `mbs:room:{roomId}`. An instance **subscribes to a room's
//! channel only while it owns a local member of that room** — it subscribes on
//! the first local join and unsubscribes on the last local leave/disconnect. A
//! publisher therefore reaches only the instance(s) actually holding the
//! recipient's sockets; an instance with no members of a room never sees its
//! traffic. This is the CF-Durable-Object routing model without the platform
//! lock-in: the recipient's owning instance is the "object", addressed by the
//! room channel.
//!
//! Redis pub/sub subscriptions are dropped automatically when a connection
//! dies, so **the live subscription set IS the routing table** — self-healing,
//! with no directory to keep consistent and no stale-entry reaper. A crashed
//! instance's routes simply vanish; the recipient re-handshakes onto a live
//! instance, which re-subscribes.
//!
//! ## Carry-unsigned / sign-on-owner
//!
//! BRC-103 signing is pinned to the connection-owning instance: only the
//! instance holding a socket's authsocket `Peer` session can produce a signed
//! frame for it. So the backplane carries the **UNSIGNED** [`RoomMessage`]
//! (wrapped in a [`BackplaneEnvelope`]); the owner instance signs for its local
//! members via the same signed local-delivery path Model A uses
//! (`WsBroadcast::deliver_local`).
//!
//! ## Redis is live-push ONLY — never durability
//!
//! Durability lives in shared MySQL (persist pipeline + HTTP `/listMessages`
//! from ANY instance). Accordingly this module **degrades, never fails**:
//! Redis down → publishes are counted and dropped (recipients fall back to the
//! mailbox), subscriptions reconnect with backoff and replay the owned-room
//! set, and a subscribe that races an in-flight publish just misses the live
//! push (the mailbox covers it). Nothing on the send path ever blocks on Redis.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use redis::{FromRedisValue, IntoConnectionInfo};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::ws::RoomMessage;

/// Per-room channel prefix. Tenant isolation is at the Redis-deployment level
/// (each licensee runs their own stack), so no per-tenant namespacing is needed.
pub const ROOM_CHANNEL_PREFIX: &str = "mbs:room:";

/// The Redis pub/sub channel a room's live push travels on.
pub fn room_channel(room_id: &str) -> String {
    format!("{ROOM_CHANNEL_PREFIX}{room_id}")
}

/// Bounded queue between the send paths and the publisher task.
const PUBLISH_QUEUE_CAPACITY: usize = 1024;

/// Bounded queue between the Redis subscription and the local signing fan-out
/// (backpressure → counted drops, never unbounded memory).
const DELIVERY_QUEUE_CAPACITY: usize = 2048;

/// Don't re-attempt a failed publisher connect more than once per interval.
const CONNECT_RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// Cap on a single connect attempt so a task never wedges.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// Subscriber reconnect backoff bounds.
const RECONNECT_MIN: Duration = Duration::from_millis(500);
const RECONNECT_MAX: Duration = Duration::from_secs(30);

/// What crosses the wire: the UNSIGNED room message plus routing metadata.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BackplaneEnvelope {
    pub origin: String,
    #[serde(rename = "roomId")]
    pub room_id: String,
    pub event: String,
    pub message: RoomMessage,
    /// Publisher's wall clock at enqueue time (µs since epoch). Used for the
    /// pub/sub-lag metric on own-origin envelopes. `0`/absent = unknown.
    #[serde(rename = "publishedAtUs", default)]
    pub published_at_us: u64,
}

/// Microseconds since the Unix epoch (publisher clock).
pub(crate) fn now_epoch_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// One item on the publish queue: which channel, what payload.
struct PublishItem {
    channel: String,
    payload: String,
}

/// A subscription-lifecycle command for the subscriber task.
enum RouteCmd {
    Subscribe(String),
    Unsubscribe(String),
}

/// Local membership: which sockets joined which rooms, and the derived
/// per-room member count. These two MUST move together (the count is derived
/// from `sid_rooms`), so they live under one mutex — updating them atomically
/// prevents the count drifting from membership under concurrent same-sid
/// join/leave/disconnect.
#[derive(Default)]
struct Membership {
    /// room → count of local members (the desired-subscription set = keys).
    counts: HashMap<String, usize>,
    /// sid → the rooms that socket joined here (so disconnect can decrement).
    sid_rooms: HashMap<String, HashSet<String>>,
}

/// Per-instance routing state: which rooms this instance owns local members of,
/// and which room channels it has actually subscribed to.
#[derive(Default)]
struct RouteState {
    /// Membership + derived counts, under one lock (atomic transitions).
    members: parking_lot::Mutex<Membership>,
    /// room channels the subscriber task has confirmed SUBSCRIBEd (live).
    /// Mutated ONLY by `subscriber_task`; reconciled against `members.counts`
    /// on every reconnect, so it is eventually-consistent and self-healing.
    active: parking_lot::Mutex<HashSet<String>>,
}

impl RouteState {
    fn rooms_snapshot(&self) -> Vec<String> {
        self.members.lock().counts.keys().cloned().collect()
    }
}

/// Decrement a room's member count under the held `members` lock; on the 0
/// transition remove the key and emit `Unsubscribe`. Sending the command inside
/// the critical section makes command order match the count-transition order,
/// so a concurrent join/leave on the same room can't end subscribed-vs-counts
/// inconsistent.
fn dec_member(m: &mut Membership, tx: &mpsc::UnboundedSender<RouteCmd>, room: &str) {
    if let Some(c) = m.counts.get_mut(room) {
        *c -= 1;
        if *c == 0 {
            m.counts.remove(room);
            let _ = tx.send(RouteCmd::Unsubscribe(room.to_string()));
        }
    }
}

/// Handle to the Model B backplane.
pub struct Backplane {
    instance_id: String,
    publish_tx: mpsc::Sender<PublishItem>,
    delivery_rx: parking_lot::Mutex<Option<mpsc::Receiver<String>>>,
    route: Arc<RouteState>,
    route_tx: mpsc::UnboundedSender<RouteCmd>,
    published: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
    subscribed: Arc<AtomicBool>,
    /// Set when the subscriber gets a RESP3-not-supported error — the Redis is
    /// < 6 and Model B cannot work against it. A misconfiguration, not a
    /// transient, so it is surfaced distinctly (never as "down").
    unsupported: Arc<AtomicBool>,
}

impl Backplane {
    /// Spawn the publisher + subscriber tasks against `redis_url`. Never fails
    /// and never blocks on Redis.
    pub fn new(redis_url: &str) -> Arc<Self> {
        let (publish_tx, publish_rx) = mpsc::channel(PUBLISH_QUEUE_CAPACITY);
        let (delivery_tx, delivery_rx) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
        let (route_tx, route_rx) = mpsc::unbounded_channel();

        let published = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let subscribed = Arc::new(AtomicBool::new(false));
        let unsupported = Arc::new(AtomicBool::new(false));
        let route = Arc::new(RouteState::default());

        tokio::spawn(publisher_task(
            redis_url.to_string(),
            publish_rx,
            published.clone(),
            dropped.clone(),
        ));
        tokio::spawn(subscriber_task(
            redis_url.to_string(),
            delivery_tx,
            subscribed.clone(),
            unsupported.clone(),
            route.clone(),
            route_rx,
        ));

        Arc::new(Self {
            instance_id: new_instance_id(),
            publish_tx,
            delivery_rx: parking_lot::Mutex::new(Some(delivery_rx)),
            route,
            route_tx,
            published,
            dropped,
            subscribed,
            unsupported,
        })
    }

    /// This instance's unique id (fresh per boot).
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Enqueue the UNSIGNED room message to the room's channel. Non-blocking
    /// and best-effort: a full queue (Redis down/backed up) drops+counts.
    pub fn publish(&self, room_id: &str, event: &str, message: &RoomMessage) {
        let envelope = BackplaneEnvelope {
            origin: self.instance_id.clone(),
            room_id: room_id.to_string(),
            event: event.to_string(),
            message: message.clone(),
            published_at_us: now_epoch_us(),
        };
        let payload = match serde_json::to_string(&envelope) {
            Ok(p) => p,
            Err(e) => {
                warn!(error = %e, "backplane: failed to serialize envelope — frame dropped");
                self.dropped.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        let item = PublishItem {
            channel: room_channel(room_id),
            payload,
        };
        if self.publish_tx.try_send(item).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            warn!(
                room = %room_id,
                "backplane: publish queue full — cross-instance live push degraded (mailbox fallback covers delivery)"
            );
        }
    }

    /// A socket joined `room_id` on this instance. Subscribes to the room's
    /// channel on the first local member. Membership + count + the Subscribe
    /// command are all done under one lock, so the command order matches the
    /// count-transition order (no subscribe/unsubscribe inversion).
    pub fn on_room_join(&self, sid: &str, room_id: &str) {
        let mut m = self.route.members.lock();
        let newly = m
            .sid_rooms
            .entry(sid.to_string())
            .or_default()
            .insert(room_id.to_string());
        if !newly {
            return;
        }
        let c = m.counts.entry(room_id.to_string()).or_insert(0);
        *c += 1;
        if *c == 1 {
            let _ = self.route_tx.send(RouteCmd::Subscribe(room_id.to_string()));
        }
    }

    /// A socket left `room_id` on this instance. Unsubscribes on the last local
    /// member.
    pub fn on_room_leave(&self, sid: &str, room_id: &str) {
        let mut m = self.route.members.lock();
        let removed = m.sid_rooms.get_mut(sid).is_some_and(|s| s.remove(room_id));
        if removed {
            dec_member(&mut m, &self.route_tx, room_id);
        }
    }

    /// A socket disconnected: drop all of its local room memberships.
    pub fn on_socket_disconnect(&self, sid: &str) {
        let mut m = self.route.members.lock();
        let rooms = m.sid_rooms.remove(sid).unwrap_or_default();
        for room in rooms {
            dec_member(&mut m, &self.route_tx, &room);
        }
    }

    /// `true` once this instance has a CONFIRMED live subscription to
    /// `room_id`'s channel (i.e. cross-instance live push will reach it). Used
    /// by tests and observability.
    pub fn is_room_active(&self, room_id: &str) -> bool {
        self.route.active.lock().contains(room_id)
    }

    /// Number of room channels this instance is currently subscribed to.
    pub fn active_subscription_count(&self) -> usize {
        self.route.active.lock().len()
    }

    /// Take the raw subscription stream (exactly once).
    pub fn take_delivery_rx(&self) -> Option<mpsc::Receiver<String>> {
        self.delivery_rx.lock().take()
    }

    /// `true` while the subscriber holds a live Redis connection.
    pub fn is_subscribed(&self) -> bool {
        self.subscribed.load(Ordering::Relaxed)
    }

    /// `true` if the configured Redis is too old for Model B (no RESP3 — needs
    /// Redis 6+). A permanent misconfiguration, distinct from a transient
    /// "down".
    pub fn is_unsupported(&self) -> bool {
        self.unsupported.load(Ordering::Relaxed)
    }

    /// Envelopes successfully PUBLISHed to Redis.
    pub fn published(&self) -> u64 {
        self.published.load(Ordering::Relaxed)
    }

    /// Envelopes dropped instead of published.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

/// Random 16-hex-char instance id.
fn new_instance_id() -> String {
    let mut buf = [0u8; 8];
    if getrandom::fill(&mut buf).is_err() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        buf[..4].copy_from_slice(&nanos.to_be_bytes());
        buf[4..].copy_from_slice(&std::process::id().to_be_bytes());
    }
    hex::encode(buf)
}

/// Drains the publish queue into per-room Redis `PUBLISH`, reconnecting lazily.
async fn publisher_task(
    redis_url: String,
    mut rx: mpsc::Receiver<PublishItem>,
    published: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
) {
    let client = match redis::Client::open(redis_url.as_str()) {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "backplane: invalid REDIS_URL — Model B publish disabled, draining queue");
            while rx.recv().await.is_some() {
                dropped.fetch_add(1, Ordering::Relaxed);
            }
            return;
        }
    };

    let mut conn: Option<redis::aio::MultiplexedConnection> = None;
    let mut last_failed_connect: Option<tokio::time::Instant> = None;

    while let Some(item) = rx.recv().await {
        if conn.is_none() {
            let may_retry =
                last_failed_connect.is_none_or(|t| t.elapsed() >= CONNECT_RETRY_INTERVAL);
            if may_retry {
                match tokio::time::timeout(
                    CONNECT_TIMEOUT,
                    client.get_multiplexed_async_connection(),
                )
                .await
                {
                    Ok(Ok(c)) => {
                        info!("backplane: publisher connected to Redis");
                        conn = Some(c);
                        last_failed_connect = None;
                    }
                    Ok(Err(e)) => {
                        warn!(error = %e, "backplane: publisher connect failed — frame dropped");
                        last_failed_connect = Some(tokio::time::Instant::now());
                    }
                    Err(_) => {
                        warn!("backplane: publisher connect timed out — frame dropped");
                        last_failed_connect = Some(tokio::time::Instant::now());
                    }
                }
            }
        }

        match conn.as_mut() {
            Some(c) => {
                let result: redis::RedisResult<i64> = redis::cmd("PUBLISH")
                    .arg(&item.channel)
                    .arg(&item.payload)
                    .query_async(c)
                    .await;
                match result {
                    Ok(receivers) => {
                        published.fetch_add(1, Ordering::Relaxed);
                        debug!(channel = %item.channel, receivers, "backplane: envelope published");
                    }
                    Err(e) => {
                        warn!(error = %e, "backplane: PUBLISH failed — frame dropped, reconnecting");
                        conn = None;
                        dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            None => {
                dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Build a RESP3 client — required for the push-based dynamic-subscription API.
/// RESP3 is requested via the `protocol=resp3` URL param (the connection-info
/// protocol field is not publicly settable), preserving any existing query
/// params / TLS options on the operator's `REDIS_URL`.
fn resp3_client(redis_url: &str) -> redis::RedisResult<redis::Client> {
    // Validate the URL up front (and surface a bad REDIS_URL early).
    let _ = redis_url.into_connection_info()?;
    let url = if redis_url.contains("protocol=") {
        redis_url.to_string()
    } else if redis_url.contains('?') {
        format!("{redis_url}&protocol=resp3")
    } else if url_has_path(redis_url) {
        format!("{redis_url}?protocol=resp3")
    } else {
        // No path segment (e.g. `redis://host:6379`) — a query needs a `/` first.
        format!("{redis_url}/?protocol=resp3")
    };
    redis::Client::open(url)
}

/// Does the redis URL already have a path segment after the authority?
fn url_has_path(redis_url: &str) -> bool {
    redis_url
        .split_once("://")
        .map(|(_, rest)| rest.contains('/'))
        .unwrap_or(false)
}

/// Why the subscriber loop ended. It never ends while healthy, so every
/// variant is a terminal condition worth logging.
enum SubscriberExit {
    /// `REDIS_URL` failed to parse — Model B subscribe never started.
    InvalidUrl(redis::RedisError),
    /// The delivery receiver was dropped: the consumer (delivery task and its
    /// supervisor) is gone, so cross-instance pushes have nowhere to go.
    DeliveryClosed,
    /// The `Backplane` handle was dropped (route command sender gone) —
    /// graceful teardown.
    ControlClosed,
}

/// Holds a RESP3 connection, dynamically SUBSCRIBEs/UNSUBSCRIBEs room channels
/// as local membership changes, and forwards every message payload into
/// `delivery_tx`. Reconnects with capped backoff, replaying the owned-room set.
///
/// Structured as a thin wrapper around [`subscriber_loop`] so that the
/// truth-restoring epilogue (`subscribed = false`, `active` cleared) runs on
/// EVERY exit path — readiness reads these flags, and an exit that skipped
/// them would leave `/health/ready` reporting a live subscription that no
/// longer exists while cross-instance pushes are silently lost.
async fn subscriber_task(
    redis_url: String,
    delivery_tx: mpsc::Sender<String>,
    subscribed: Arc<AtomicBool>,
    unsupported: Arc<AtomicBool>,
    route: Arc<RouteState>,
    control_rx: mpsc::UnboundedReceiver<RouteCmd>,
) {
    let exit = subscriber_loop(
        &redis_url,
        &delivery_tx,
        &subscribed,
        &unsupported,
        &route,
        control_rx,
    )
    .await;
    subscribed.store(false, Ordering::Relaxed);
    route.active.lock().clear();
    match exit {
        SubscriberExit::InvalidUrl(e) => {
            error!(error = %e, "backplane: invalid REDIS_URL — Model B subscribe disabled; cross-instance live push will never work on this instance");
        }
        SubscriberExit::DeliveryClosed => {
            error!("backplane: delivery consumer gone — subscriber stopped; cross-instance live push DISABLED on this instance (readiness now reports redis down)");
        }
        SubscriberExit::ControlClosed => {
            info!("backplane: subscriber stopped (backplane handle dropped — shutdown)");
        }
    }
}

/// The subscriber's connect/replay/event loop. Returns (instead of exiting the
/// task directly) so [`subscriber_task`] can restore the observable state
/// unconditionally.
async fn subscriber_loop(
    redis_url: &str,
    delivery_tx: &mpsc::Sender<String>,
    subscribed: &AtomicBool,
    unsupported: &AtomicBool,
    route: &RouteState,
    mut control_rx: mpsc::UnboundedReceiver<RouteCmd>,
) -> SubscriberExit {
    let client = match resp3_client(redis_url) {
        Ok(c) => c,
        Err(e) => return SubscriberExit::InvalidUrl(e),
    };

    let mut backoff = RECONNECT_MIN;
    let mut logged_unsupported = false;
    loop {
        // Fresh push channel per connection.
        let (push_tx, mut push_rx) = mpsc::unbounded_channel::<redis::PushInfo>();
        let config = redis::AsyncConnectionConfig::new().set_push_sender(push_tx);

        let mut conn = match tokio::time::timeout(
            CONNECT_TIMEOUT,
            client.get_multiplexed_async_connection_with_config(&config),
        )
        .await
        {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => {
                // RESP3NotSupported means Redis < 6 — a permanent, actionable
                // misconfiguration (directed routing needs RESP3). Surface it
                // distinctly and loudly ONCE, not buried at debug like a
                // transient blip; readiness reports it as "unsupported".
                if e.kind() == redis::ErrorKind::RESP3NotSupported {
                    unsupported.store(true, Ordering::Relaxed);
                    if !logged_unsupported {
                        error!(
                            "backplane: Redis does not support RESP3 (HELLO) — Model B requires Redis 6+. Cross-instance live push is DISABLED until Redis is upgraded. {e}"
                        );
                        logged_unsupported = true;
                    }
                } else {
                    unsupported.store(false, Ordering::Relaxed);
                    debug!(error = %e, "backplane: subscriber connect failed — retrying");
                }
                if !reconnect_wait(delivery_tx, &mut backoff).await {
                    return SubscriberExit::DeliveryClosed;
                }
                continue;
            }
            Err(_) => {
                debug!("backplane: subscriber connect timed out — retrying");
                if !reconnect_wait(delivery_tx, &mut backoff).await {
                    return SubscriberExit::DeliveryClosed;
                }
                continue;
            }
        };

        // Replay: subscribe to every room this instance currently owns.
        route.active.lock().clear();
        let mut replay_ok = true;
        for room in route.rooms_snapshot() {
            if conn.subscribe(room_channel(&room)).await.is_err() {
                replay_ok = false;
                break;
            }
            route.active.lock().insert(room);
        }
        if !replay_ok {
            subscribed.store(false, Ordering::Relaxed);
            route.active.lock().clear();
            if !reconnect_wait(delivery_tx, &mut backoff).await {
                return SubscriberExit::DeliveryClosed;
            }
            continue;
        }

        subscribed.store(true, Ordering::Relaxed);
        unsupported.store(false, Ordering::Relaxed);
        logged_unsupported = false;
        backoff = RECONNECT_MIN;
        info!(
            rooms = route.active.lock().len(),
            "backplane: subscriber connected (RESP3, directed routing)"
        );

        // Event loop: subscription control + inbound pushes + consumer
        // liveness. `None` = the connection died, reconnect; `Some(exit)` =
        // terminal, bubble up to the epilogue in `subscriber_task`.
        let exit: Option<SubscriberExit> = loop {
            tokio::select! {
                cmd = control_rx.recv() => match cmd {
                    Some(RouteCmd::Subscribe(room)) => {
                        if conn.subscribe(room_channel(&room)).await.is_ok() {
                            route.active.lock().insert(room);
                        } else {
                            break None; // connection bad — reconnect + replay
                        }
                    }
                    Some(RouteCmd::Unsubscribe(room)) => {
                        let _ = conn.unsubscribe(room_channel(&room)).await;
                        route.active.lock().remove(&room);
                    }
                    None => break Some(SubscriberExit::ControlClosed),
                },
                // A dropped delivery receiver must flip readiness NOW — not at
                // the next inbound push, which for a quiet instance may be
                // never (leaving it lying "subscribed" indefinitely).
                _ = delivery_tx.closed() => break Some(SubscriberExit::DeliveryClosed),
                push = push_rx.recv() => match push {
                    Some(info) => match info.kind {
                        redis::PushKind::Message => {
                            // data = [channel, payload]; forward the payload.
                            if let Some(payload) = info
                                .data
                                .get(1)
                                .and_then(|v| String::from_redis_value_ref(v).ok())
                            {
                                match delivery_tx.try_send(payload) {
                                    Ok(()) => {}
                                    Err(mpsc::error::TrySendError::Full(_)) => warn!(
                                        "backplane: local delivery queue full — dropping cross-instance push (mailbox fallback covers delivery)"
                                    ),
                                    Err(mpsc::error::TrySendError::Closed(_)) => {
                                        break Some(SubscriberExit::DeliveryClosed)
                                    }
                                }
                            }
                        }
                        redis::PushKind::Disconnection => break None,
                        _ => {} // subscribe/unsubscribe confirmations
                    },
                    None => break None, // push stream closed — reconnect
                },
            }
        };

        subscribed.store(false, Ordering::Relaxed);
        route.active.lock().clear();
        if let Some(exit) = exit {
            return exit;
        }
        warn!("backplane: subscription connection lost — reconnecting");
        if !reconnect_wait(delivery_tx, &mut backoff).await {
            return SubscriberExit::DeliveryClosed;
        }
    }
}

/// Sleep the backoff and grow it; returns `false` if the consumer is gone.
async fn reconnect_wait(delivery_tx: &mpsc::Sender<String>, backoff: &mut Duration) -> bool {
    if delivery_tx.is_closed() {
        return false;
    }
    tokio::time::sleep(*backoff).await;
    *backoff = (*backoff * 2).min(RECONNECT_MAX);
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn room_message() -> RoomMessage {
        RoomMessage {
            message_id: "m1".into(),
            sender: "02aa".into(),
            recipient: "03bb".into(),
            message_box: "inbox".into(),
            body: "{\"message\":\"hi\"}".into(),
            created_at: "t".into(),
            updated_at: "t".into(),
        }
    }

    #[test]
    fn room_channel_is_prefixed_per_room() {
        assert_eq!(room_channel("03bb-inbox"), "mbs:room:03bb-inbox");
        assert_ne!(room_channel("03bb-inbox"), room_channel("03cc-inbox"));
    }

    /// The wire shape is stable: routing metadata + the UNSIGNED RoomMessage,
    /// no signature material anywhere.
    #[test]
    fn envelope_wire_shape_is_unsigned_room_message() {
        let env = BackplaneEnvelope {
            origin: "abc123".into(),
            room_id: "03bb-inbox".into(),
            event: "sendMessage-03bb-inbox".into(),
            message: room_message(),
            published_at_us: 1_700_000_000_000_000,
        };
        let v = serde_json::to_value(&env).expect("serialize");
        assert_eq!(v["origin"], "abc123");
        assert_eq!(v["roomId"], "03bb-inbox");
        assert_eq!(v["publishedAtUs"], 1_700_000_000_000_000u64);
        let msg = v["message"].as_object().expect("message object");
        assert_eq!(msg.len(), 7);
        for key in ["signature", "nonce", "yourNonce", "identityKey"] {
            assert!(
                !msg.contains_key(key),
                "unsigned payload must not carry {key}"
            );
        }
        let back: BackplaneEnvelope = serde_json::from_value(v).expect("roundtrips");
        assert_eq!(back.message.message_id, "m1");
    }

    #[test]
    fn instance_ids_are_unique() {
        let a = new_instance_id();
        let b = new_instance_id();
        assert_eq!(a.len(), 16);
        assert_ne!(a, b);
    }

    /// Directed routing bookkeeping (no live Redis needed — the subscriber task
    /// just fails to connect to the dead port): join/leave/disconnect drive the
    /// desired-subscription counts and the subscribe/unsubscribe transitions on
    /// 0↔1 boundaries. `#[tokio::test]` because `Backplane::new` spawns tasks.
    #[tokio::test]
    async fn membership_drives_subscription_transitions() {
        let bp = Backplane::new("redis://127.0.0.1:1/");

        // First join of a room → count 1.
        bp.on_room_join("sockA", "03bb-inbox");
        assert_eq!(
            *bp.route.members.lock().counts.get("03bb-inbox").unwrap(),
            1
        );
        // Second local member of the same room → count 2 (no new subscribe).
        bp.on_room_join("sockB", "03bb-inbox");
        assert_eq!(
            *bp.route.members.lock().counts.get("03bb-inbox").unwrap(),
            2
        );
        // A different room tracked independently.
        bp.on_room_join("sockA", "03cc-inbox");
        assert_eq!(
            *bp.route.members.lock().counts.get("03cc-inbox").unwrap(),
            1
        );

        // One member leaves 03bb-inbox → count 1, still owned.
        bp.on_room_leave("sockA", "03bb-inbox");
        assert_eq!(
            *bp.route.members.lock().counts.get("03bb-inbox").unwrap(),
            1
        );

        // Disconnect sockB → last member of 03bb-inbox gone → room dropped.
        bp.on_socket_disconnect("sockB");
        assert!(!bp.route.members.lock().counts.contains_key("03bb-inbox"));
        // sockA still owns 03cc-inbox.
        assert_eq!(
            *bp.route.members.lock().counts.get("03cc-inbox").unwrap(),
            1
        );

        // Idempotent: re-leaving a room the socket isn't in is a no-op.
        bp.on_room_leave("sockA", "03bb-inbox");
        assert!(!bp.route.members.lock().counts.contains_key("03bb-inbox"));
    }

    /// Redis unreachable: publish must not block or error — frames drop+count,
    /// the caller's send path is untouched.
    #[tokio::test]
    async fn publish_with_redis_down_degrades_without_blocking() {
        let bp = Backplane::new("redis://127.0.0.1:1/");
        let start = std::time::Instant::now();
        bp.publish("03bb-inbox", "sendMessage-03bb-inbox", &room_message());
        assert!(
            start.elapsed() < Duration::from_millis(100),
            "publish must be non-blocking"
        );

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while bp.dropped() == 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "drop must be observed"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(bp.published(), 0);
        assert!(!bp.is_subscribed());
    }
}
