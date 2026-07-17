//! Model B backplane — cross-instance live push over Redis pub/sub.
//!
//! One binary, topology chosen by config (transport-architecture WS2-3):
//! no `REDIS_URL` → **Model A** (single self-contained instance, in-process
//! routing — the default); `REDIS_URL` set → **Model B** (N replicas behind a
//! sticky LB, this backplane bridging live push between them).
//!
//! ## Carry-unsigned / sign-on-owner
//!
//! BRC-103 signing is pinned to the connection-owning instance: only the
//! instance holding a socket's authsocket `Peer` session can produce a signed
//! frame for it. So the backplane carries the **UNSIGNED** [`RoomMessage`]
//! (wrapped in a [`BackplaneEnvelope`]); every instance subscribes, and each
//! one signs **only for its own local room members** — the same signed
//! local-delivery path Model A uses (`WsBroadcast::deliver_local`). A
//! non-owner instance structurally *cannot* sign for a remote socket: it has
//! no `Peer` session for it.
//!
//! ## The room registry is per-instance, rebuilt on reconnect
//!
//! Each instance's in-process authsocket room map is the authoritative record
//! of which sockets it owns (transport-architecture WS2-3: "Room registry
//! stays per-instance"). A reconnecting client re-handshakes and re-joins its
//! room on whichever instance the LB lands it on, rebuilding the registry
//! there. Publishers therefore do not need cross-instance membership
//! knowledge: they PUBLISH unconditionally and owner instances filter by
//! their local membership.
//!
//! ## Redis is live-push ONLY — never durability
//!
//! Durability lives in shared MySQL (persist pipeline + HTTP `/listMessages`
//! from ANY instance). Accordingly this module **degrades, never fails**:
//! Redis down → publishes are counted and dropped (recipients fall back to
//! the mailbox), subscriptions reconnect with backoff, and nothing on the
//! send path ever blocks on Redis (publish is a non-blocking enqueue to a
//! bounded queue drained by a background task).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::ws::RoomMessage;

/// The single pub/sub channel all instances of a deployment share. Tenant
/// isolation is at the Redis-deployment level (each licensee runs their own
/// stack), so no per-tenant namespacing is needed inside the channel name.
pub const CHANNEL: &str = "mbs:backplane:v1";

/// Bounded queue between the send paths and the publisher task. If Redis is
/// down long enough to fill it, further publishes are dropped (counted) —
/// live push is best-effort; the mailbox stays durable.
const PUBLISH_QUEUE_CAPACITY: usize = 1024;

/// Don't re-attempt a failed Redis connect more than once per interval while
/// messages are flowing (avoids a connect storm when Redis is down).
const CONNECT_RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// Cap on a single connect attempt so the publisher task never wedges.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// Subscriber reconnect backoff bounds.
const RECONNECT_MIN: Duration = Duration::from_millis(500);
const RECONNECT_MAX: Duration = Duration::from_secs(30);

/// What crosses the wire: the UNSIGNED room message plus routing metadata.
/// `origin` lets the publishing instance skip its own envelope on the
/// subscribe side (its local leg already delivered at publish time).
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BackplaneEnvelope {
    pub origin: String,
    #[serde(rename = "roomId")]
    pub room_id: String,
    pub event: String,
    pub message: RoomMessage,
    /// Publisher's wall clock at enqueue time (µs since epoch). Used for the
    /// pub/sub-lag metric: the publishing instance observes its OWN envelope
    /// coming back off the channel and measures publish→subscribe round-trip
    /// on a single clock (no cross-instance skew). `0`/absent = unknown
    /// (older publisher during a rolling deploy) — no lag sample is taken.
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

/// Handle to the Model B backplane: a non-blocking publisher plus a
/// self-reconnecting subscription feeding [`Backplane::take_delivery_rx`].
pub struct Backplane {
    instance_id: String,
    publish_tx: mpsc::Sender<String>,
    /// Raw envelope JSON received from the channel (all origins, unfiltered).
    /// Taken exactly once by `WsBroadcast` to drive local signed delivery.
    delivery_rx: parking_lot::Mutex<Option<mpsc::UnboundedReceiver<String>>>,
    published: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
    subscribed: Arc<AtomicBool>,
}

impl Backplane {
    /// Spawn the publisher + subscriber tasks against `redis_url`. Never
    /// fails and never blocks on Redis: a bad/unreachable Redis only means
    /// degraded (dropped) cross-instance live push, observable via
    /// [`Self::dropped`] and warn logs.
    pub fn new(redis_url: &str) -> Arc<Self> {
        let (publish_tx, publish_rx) = mpsc::channel(PUBLISH_QUEUE_CAPACITY);
        let (delivery_tx, delivery_rx) = mpsc::unbounded_channel();

        let published = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let subscribed = Arc::new(AtomicBool::new(false));

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
        ));

        Arc::new(Self {
            instance_id: new_instance_id(),
            publish_tx,
            delivery_rx: parking_lot::Mutex::new(Some(delivery_rx)),
            published,
            dropped,
            subscribed,
        })
    }

    /// This instance's unique id (fresh per boot — a restarted instance is a
    /// new owner; its previous sockets are gone anyway).
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Enqueue the UNSIGNED room message for other instances. Non-blocking
    /// and best-effort: on a full queue (Redis down/backed up) the frame is
    /// dropped and counted — never stalls or fails the caller's send.
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
        if self.publish_tx.try_send(payload).is_err() {
            // Queue full or publisher gone: degrade. The recipient still gets
            // the message from the mailbox (MySQL) via HTTP /listMessages.
            self.dropped.fetch_add(1, Ordering::Relaxed);
            warn!(
                room = %room_id,
                "backplane: publish queue full — cross-instance live push degraded (mailbox fallback covers delivery)"
            );
        }
    }

    /// Take the raw subscription stream (exactly once). `WsBroadcast` drains
    /// it and delivers remote-origin envelopes to local room members.
    pub fn take_delivery_rx(&self) -> Option<mpsc::UnboundedReceiver<String>> {
        self.delivery_rx.lock().take()
    }

    /// `true` while the subscriber holds a live subscription to [`CHANNEL`].
    pub fn is_subscribed(&self) -> bool {
        self.subscribed.load(Ordering::Relaxed)
    }

    /// Envelopes successfully PUBLISHed to Redis.
    pub fn published(&self) -> u64 {
        self.published.load(Ordering::Relaxed)
    }

    /// Envelopes dropped instead of published (Redis unreachable, queue full,
    /// or serialization failure). A rising value = degraded cross-instance
    /// live push.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

/// Random 16-hex-char instance id.
fn new_instance_id() -> String {
    let mut buf = [0u8; 8];
    if getrandom::fill(&mut buf).is_err() {
        // Extremely unlikely; fall back to a time-derived id rather than
        // aborting — a collision only risks a duplicate live push, and the
        // client dedupes on messageId.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        buf[..4].copy_from_slice(&nanos.to_be_bytes());
        buf[4..].copy_from_slice(&std::process::id().to_be_bytes());
    }
    hex::encode(buf)
}

/// Drains the publish queue into Redis `PUBLISH`, reconnecting lazily.
/// Frames that cannot be published are dropped (counted) — live push only.
async fn publisher_task(
    redis_url: String,
    mut rx: mpsc::Receiver<String>,
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

    while let Some(payload) = rx.recv().await {
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
                    .arg(CHANNEL)
                    .arg(&payload)
                    .query_async(c)
                    .await;
                match result {
                    Ok(receivers) => {
                        published.fetch_add(1, Ordering::Relaxed);
                        debug!(receivers, "backplane: envelope published");
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

/// Holds a subscription to [`CHANNEL`] and forwards every payload into
/// `delivery_tx`; reconnects with capped exponential backoff. Exits when the
/// delivery receiver is gone (owner dropped).
async fn subscriber_task(
    redis_url: String,
    delivery_tx: mpsc::UnboundedSender<String>,
    subscribed: Arc<AtomicBool>,
) {
    use futures_util::StreamExt;

    let client = match redis::Client::open(redis_url.as_str()) {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "backplane: invalid REDIS_URL — Model B subscribe disabled");
            return;
        }
    };

    let mut backoff = RECONNECT_MIN;
    loop {
        match tokio::time::timeout(CONNECT_TIMEOUT, client.get_async_pubsub()).await {
            Ok(Ok(mut pubsub)) => match pubsub.subscribe(CHANNEL).await {
                Ok(()) => {
                    info!(channel = CHANNEL, "backplane: subscribed");
                    subscribed.store(true, Ordering::Relaxed);
                    backoff = RECONNECT_MIN;
                    let mut stream = pubsub.into_on_message();
                    while let Some(msg) = stream.next().await {
                        match msg.get_payload::<String>() {
                            Ok(payload) => {
                                if delivery_tx.send(payload).is_err() {
                                    subscribed.store(false, Ordering::Relaxed);
                                    return; // consumer gone — shut down
                                }
                            }
                            Err(e) => {
                                warn!(error = %e, "backplane: non-UTF8 payload on channel — skipped")
                            }
                        }
                    }
                    subscribed.store(false, Ordering::Relaxed);
                    warn!("backplane: subscription stream ended — reconnecting");
                }
                Err(e) => {
                    warn!(error = %e, "backplane: SUBSCRIBE failed — retrying");
                }
            },
            Ok(Err(e)) => {
                debug!(error = %e, "backplane: subscriber connect failed — retrying");
            }
            Err(_) => {
                debug!("backplane: subscriber connect timed out — retrying");
            }
        }
        if delivery_tx.is_closed() {
            return;
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(RECONNECT_MAX);
    }
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

    /// The wire shape is stable: routing metadata + the UNSIGNED RoomMessage
    /// exactly as the local signed-delivery path would serialize it — and no
    /// signature material anywhere (signing happens only on the owner).
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
        assert_eq!(v["event"], "sendMessage-03bb-inbox");
        assert_eq!(v["publishedAtUs"], 1_700_000_000_000_000u64);
        // The carried message is the plain RoomMessage JSON…
        assert_eq!(
            v["message"],
            serde_json::to_value(room_message()).expect("room message")
        );
        // …with exactly its fields — nothing signature-shaped rides along.
        let msg = v["message"].as_object().expect("message object");
        assert_eq!(msg.len(), 7);
        for key in ["signature", "nonce", "yourNonce", "identityKey"] {
            assert!(
                !msg.contains_key(key),
                "unsigned payload must not carry {key}"
            );
        }

        let back: BackplaneEnvelope = serde_json::from_value(v).expect("envelope roundtrips");
        assert_eq!(back.origin, "abc123");
        assert_eq!(back.message.message_id, "m1");
    }

    #[test]
    fn instance_ids_are_unique() {
        let a = new_instance_id();
        let b = new_instance_id();
        assert_eq!(a.len(), 16);
        assert_ne!(a, b);
    }

    /// Redis unreachable: publish must not block or error — frames are
    /// dropped and counted, the caller's send path is untouched.
    #[tokio::test]
    async fn publish_with_redis_down_degrades_without_blocking() {
        // Port 1 on loopback: connection refused immediately.
        let bp = Backplane::new("redis://127.0.0.1:1/");
        let start = std::time::Instant::now();
        bp.publish("03bb-inbox", "sendMessage-03bb-inbox", &room_message());
        assert!(
            start.elapsed() < Duration::from_millis(100),
            "publish must be non-blocking"
        );

        // The publisher task fails to connect and counts the drop.
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
