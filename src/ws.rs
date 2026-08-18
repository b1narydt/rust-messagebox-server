//! WebSocket (Socket.IO) support for the MessageBox server, over the shared
//! [`authsocket`] crate.
//!
//! The BRC-103 protocol core — per-socket Peer sessions, the verified-sender
//! identity registry, room membership, and the signed emit/broadcast
//! primitives — lives in the `authsocket` crate (`AuthSocketServer` +
//! `server_io` helpers). This module keeps:
//!
//! - the **socket wiring** ([`setup_handlers`]) — a fork of the crate's
//!   `attach` that adds the TS-parity failure events
//!   (`authenticationFailed`/`joinFailed`/`leaveFailed`; parity audit
//!   W1/W2/W3) the 0.1.0 adapter swallows, while preserving its security
//!   invariants verbatim (see the function doc);
//! - [`RoomMessage`] — the room-delivery payload shape;
//! - the `sendMessage` app verb ([`handle_ws_send_message`]): push-live-first
//!   signed broadcast, async persistence, signed room-scoped ack, and
//!   `messageFailed` on invalid payloads (W5);
//! - the `mpcEnvelope` app verb ([`handle_ws_mpc_envelope`]): the transient MPC
//!   ceremony relay — an allowlisted, opaque, never-persisted forward into the
//!   recipient's `{recipient}-{box}` room;
//! - the `walletCall` / `walletReply` app verbs ([`handle_ws_wallet_frame`]):
//!   the wallet-RPC relay — an allowlisted, opaque, never-persisted forward into
//!   the recipient's `{recipient}-wallet_inbox` room, preserving the verb. This
//!   is what makes a strictly dial-out enterprise box, whose own HTTP listener
//!   is disabled, reachable at all;
//! - [`WsBroadcast`] — the app-facing handle the HTTP handlers use
//!   (`broadcast_to_room` + `persist_async`).
//!
//! ## Protocol
//!
//! `authMessage` is the ONLY inbound Socket.IO event: every application action
//! arrives as a BRC-103-signed general message, verified by the Peer before it
//! reaches the dispatcher. Every server→client event (acks, room deliveries,
//! `authenticationSuccess`) is a signed general message emitted as
//! `authMessage` — never a raw Socket.IO event. Room naming convention:
//! `{identityKey}-{messageBox}`.
//!
//! Fan-out signing is parallel (`join_all` inside
//! `AuthSocketServer::emit_to_room`) and per-socket state is behind brief
//! `Arc`-clone-out locks — the crate carries forward the design this server
//! adopted after a documented 100%→15% delivery collapse under a single
//! global session lock.
//!
//! ## Topology: Model A / Model B (one code path)
//!
//! [`WsBroadcast::broadcast_to_room`] always runs the same signed
//! local-delivery leg ([`WsBroadcast::deliver_local`]). With a
//! [`crate::backplane::Backplane`] attached (Model B, `REDIS_URL` set) it
//! *additionally* enqueues the UNSIGNED [`RoomMessage`] to Redis so the
//! instances owning the recipient's sockets can sign and deliver to their
//! local members — carry-unsigned/sign-on-owner. Model A simply skips that
//! publish; the delivery path itself never diverges. Redis is live-push
//! only: durability is always the MySQL mailbox + HTTP `/listMessages`.

use std::sync::Arc;

use serde::Serialize;
use socketioxide::extract::{Data, SocketRef};
use socketioxide::SocketIo;
use tracing::{debug, error, info, warn, Instrument};

use authsocket::server::{AuthSocketServer, SharedAuthSocketServer};
use authsocket::server_io::{emit_signed_to_room, emit_signed_to_socket};
use authsocket::{VerifiedEvent, AUTH_MESSAGE_EVENT};
use bsv::auth::types::AuthMessage;
use bsv::wallet::proto_wallet::ProtoWallet as SdkProtoWallet;

use crate::db::DbPool;

/// Room id convention: `{recipientIdentityKey}-{messageBox}` (re-exported from
/// the authsocket wire contract for the HTTP handlers).
pub use authsocket::room_id;

/// Message broadcast to a room when a new message is stored.
///
/// Includes `created_at` and `updated_at` fields required by the client's
/// `ServerPeerMessage` parser. Without these, the client's handler fails to
/// deserialize the broadcast and the callback never fires.
///
/// `Deserialize` exists for the Model B backplane, which carries this exact
/// (unsigned) shape between instances — see [`crate::backplane`].
#[derive(Serialize, serde::Deserialize, Clone, Debug)]
pub struct RoomMessage {
    #[serde(rename = "messageId")]
    pub message_id: String,
    pub sender: String,
    pub recipient: String,
    #[serde(rename = "messageBox")]
    pub message_box: String,
    pub body: String,
    pub created_at: String,
    pub updated_at: String,
}

/// The one Socket.IO application event carrying MPC ceremony envelopes, in both
/// directions. Source of truth:
/// `mpc_transport::direct_authsocket_transport::MPC_ENVELOPE_EVENT` in
/// rust-mpc. (Application events ride inside signed BRC-103 general messages,
/// so this is an eventName string, not a raw Socket.IO event.)
pub const MPC_ENVELOPE_EVENT: &str = "mpcEnvelope";

/// The presence box every dialed-in ceremony party joins as
/// `{identityKey}-mpc_inbox`, and therefore the room the relay harvests sockets
/// from when it auto-joins them into a per-ceremony box room. Source of truth:
/// `mpc_core::envelope::MPC_INBOX` in rust-mpc; carried here as a literal
/// because the relay must not depend on the MPC stack it relays for.
pub const MPC_INBOX: &str = "mpc_inbox";

/// Wire shape of one `mpcEnvelope`, byte-identical to
/// `mpc_transport::direct_authsocket_transport::EnvelopeEvent` in rust-mpc.
/// This struct is the ONE producer of the relayed frame's JSON — the ceremony
/// lane never passes through [`RoomMessage`], so the two lanes' key sets cannot
/// drift into each other.
///
/// The JSON key for the destination box is **`box`**, not `messageBox`: the
/// ceremony client dispatches entirely on this field, and a mismatch makes its
/// `from_value::<EnvelopeEvent>` fail and log "dropping malformed mpcEnvelope
/// event" for every round — a total, silent ceremony outage. `sender` is
/// `default`ed because the sending client may omit its own redundant claim; the
/// relay re-stamps it with the BRC-103-verified socket identity before
/// forwarding (see [`admit_mpc_envelope`]).
#[derive(Serialize, serde::Deserialize, Clone, Debug)]
pub struct MpcEnvelope {
    /// Identity key of the ceremony party this envelope is FOR.
    pub recipient: String,
    /// Destination box on the recipient — the per-ceremony return box, which is
    /// also the room this envelope is routed to.
    #[serde(rename = "box")]
    pub r#box: String,
    /// BRC-78 ciphertext, end-to-end sender → recipient. The relay holds no
    /// share and no session key for it, and never attempts to open it.
    pub body: String,
    /// Original sender, re-stamped by the relay from its verified identity.
    #[serde(default)]
    pub sender: String,
    /// Per-op unique id (the receiving side dedupes per mailbox on it).
    #[serde(rename = "messageId")]
    pub message_id: String,
}

/// App → box: one nested BRC-103 frame of the wallet session. Source of truth:
/// `mpc_transport::wallet_lane_wire::WALLET_CALL_EVENT` in rust-mpc.
pub const WALLET_CALL_EVENT: &str = "walletCall";

/// Box → app: one nested BRC-103 frame back. Source of truth:
/// `mpc_transport::wallet_lane_wire::WALLET_REPLY_EVENT` in rust-mpc.
///
/// A distinct verb from [`WALLET_CALL_EVENT`], and the relay PRESERVES whichever
/// one arrived: the app and the box each dispatch on the verb, so a reply
/// re-emitted as a call is a hang rather than a visible error.
pub const WALLET_REPLY_EVENT: &str = "walletReply";

/// The presence box every dialed-in wallet-lane party joins as
/// `{identityKey}-wallet_inbox`, and the room a frame addressed to `recipient`
/// is delivered into. Source of truth:
/// `mpc_transport::wallet_lane_wire::WALLET_INBOX` in rust-mpc; carried here as
/// a literal for the same reason [`MPC_INBOX`] is.
///
/// DISTINCT from [`MPC_INBOX`], so the two lanes never share a room even for the
/// same identity — the lane-isolation requirement a busy dashboard would
/// otherwise violate by starving a presig deadline.
pub const WALLET_INBOX: &str = "wallet_inbox";

/// Wire shape of one wallet-lane frame, both directions, byte-identical to
/// `mpc_transport::wallet_lane_wire::WalletFrame` in rust-mpc. As with
/// [`MpcEnvelope`], this struct is the ONE producer of the relayed frame's JSON.
///
/// The correlation id is **`correlationId`** on the wire, and the opaque payload
/// is **`frame`**. Both are load-bearing in the same way `box` is on the
/// ceremony lane: the receiving side parses this exact shape, so a renamed key
/// turns every call into a silent timeout rather than a visible error. `sender`
/// is `default`ed because a sending client may omit its own redundant claim; the
/// relay re-stamps it from the BRC-103-verified socket identity (see
/// [`admit_wallet_frame`]).
///
/// `frame` is a STRING, not a nested `Value`, upstream and here: it is a whole
/// nested BRC-103 `AuthMessage` wrapping BRC-2 ciphertext, and the receiving box
/// bounds the raw text before it materializes attacker-relayed JSON. The relay
/// never decodes it — it is not a party to that session, holds neither party's
/// key, and forwards the string verbatim.
#[derive(Serialize, serde::Deserialize, Clone, Debug)]
pub struct WalletFrame {
    /// Identity key of the party this frame is FOR — the box for a
    /// `walletCall`, the app for a `walletReply`. The relay routes on this.
    pub recipient: String,
    /// Claimed origin, re-stamped by the relay from its verified identity.
    #[serde(default)]
    pub sender: String,
    /// Logical request↔response correlation id, chosen by the app and echoed by
    /// the box. Opaque to the relay.
    #[serde(rename = "correlationId", default)]
    pub correlation_id: String,
    /// The opaque nested app↔box BRC-103 `AuthMessage`, serialized as JSON.
    pub frame: String,
}

/// Shared state for WebSocket broadcast.
///
/// Held by the HTTP handlers (via `AppState`) so they can push live messages
/// to rooms and enqueue persistence.
#[derive(Clone)]
pub struct WsBroadcast {
    io: SocketIo,
    /// The authsocket room/session core. `pub(crate)` so tests can drive it.
    pub(crate) core: SharedAuthSocketServer<SdkProtoWallet>,
    /// Server private key hex for the per-connection BRC-103 wallets.
    server_private_key_hex: String,
    /// This server's own identity key, derived once from
    /// `server_private_key_hex`. The MPC relay lane compares it against each
    /// envelope's recipient so a ceremony party can never address the relay
    /// itself. `None` only if the key failed to parse, in which case
    /// [`setup_handlers`] disconnects every socket before it can authenticate
    /// and no envelope reaches the check.
    server_identity: Option<String>,
    /// The transient MPC relay lane's allowlist + payload ceiling. Consulted by
    /// [`handle_ws_mpc_envelope`] and by nothing else — the mailbox verbs keep
    /// their own gating.
    mpc_relay: crate::config::MpcRelayConfig,
    /// The wallet-RPC relay lane's app allowlist + payload ceiling. Consulted by
    /// [`handle_ws_wallet_frame`] and by nothing else. Its admission ALSO reads
    /// [`Self::mpc_relay`]'s peer list (the union rule), but never the reverse —
    /// the ceremony lane cannot see this field, so an admitted app is scoped to
    /// the wallet verbs by construction.
    wallet_relay: crate::config::WalletRelayConfig,
    /// Background, durable message persistence. Both send paths push live first
    /// and then hand the INSERT to this worker so MySQL latency never blocks
    /// live delivery. See [`crate::persist`].
    persist: crate::persist::PersistHandle,
    /// Shared MySQL pool — used by the WS `sendMessage` path to enforce the
    /// same recipient-block rule the HTTP path enforces (a blocked sender must
    /// not be able to route around the block over WebSocket).
    db: DbPool,
    /// Model B backplane (`REDIS_URL` set) — `None` means Model A: single
    /// instance, in-process routing only. See [`crate::backplane`].
    backplane: Option<Arc<crate::backplane::Backplane>>,
    /// Admission/drain state (Phase 3, D3). Both send paths hold a
    /// [`crate::ops::SendGuard`] for their duration so graceful drain can
    /// wait for real in-flight work, never nacking it.
    ops: Arc<crate::ops::OpsState>,
}

impl WsBroadcast {
    /// `backplane: None` → Model A (the default). `Some` → Model B: local
    /// broadcasts are additionally published (unsigned) to Redis, and a
    /// background task delivers remote-origin envelopes to local room
    /// members through the same signed path.
    pub fn new(
        io: SocketIo,
        server_private_key_hex: String,
        db: DbPool,
        backplane: Option<Arc<crate::backplane::Backplane>>,
        ops: Arc<crate::ops::OpsState>,
        mpc_relay: crate::config::MpcRelayConfig,
        wallet_relay: crate::config::WalletRelayConfig,
    ) -> Self {
        // `from_env`, not `default()`: DEAD_LETTER_PATH must reach the worker
        // (the deployed container's cwd `/` is not writable by uid 10001).
        let persist = crate::persist::PersistHandle::spawn(
            db.clone(),
            crate::persist::PersistConfig::from_env(),
        );
        let server_identity = match bsv::primitives::private_key::PrivateKey::from_hex(
            &server_private_key_hex,
        ) {
            Ok(pk) => Some(pk.to_public_key().to_der_hex()),
            Err(e) => {
                warn!(error = %e, "authsocket: server key parse failed — no server identity; every socket will be disconnected at connect time");
                None
            }
        };
        let ws = Self {
            io,
            core: Arc::new(AuthSocketServer::new()),
            server_private_key_hex,
            server_identity,
            mpc_relay,
            wallet_relay,
            persist,
            db,
            backplane,
            ops,
        };
        if let Some(bp) = &ws.backplane {
            match bp.take_delivery_rx() {
                Some(rx) => {
                    tokio::spawn(backplane_delivery_supervisor(ws.clone(), bp.clone(), rx));
                }
                None => {
                    // A Backplane is single-consumer; a second WsBroadcast on
                    // the same handle would split the subscription stream.
                    warn!("backplane delivery stream already taken — this WsBroadcast will not receive cross-instance pushes");
                }
            }
        }
        ws
    }

    /// Enqueue a message for asynchronous, durable persistence (off the hot
    /// path). The caller MUST have already performed all gating checks
    /// (auth/recipient/fee/permission/payment) and the live broadcast.
    ///
    /// Returns the [`Enqueued`] outcome from the worker so callers can observe
    /// when the fast (queued) path was bypassed for an inline write or the job
    /// was dead-lettered. Never silently drops.
    ///
    /// [`Enqueued`]: crate::persist::Enqueued
    pub async fn persist_async(&self, job: crate::persist::PersistJob) -> crate::persist::Enqueued {
        self.persist.enqueue(job).await
    }

    /// Broadcast a message to a specific room over the **authenticated** channel.
    ///
    /// Called from the HTTP `send_message` handler (and the BRC-103 WS
    /// `sendMessage` path) after all gating checks. For every room member with
    /// a completed BRC-103 handshake, the authsocket core signs
    /// `{eventName, data}` as a general message through that socket's session
    /// (concurrently across members) and it is emitted as `authMessage` — the
    /// client's authenticated *primary* receive path. A socket that has not
    /// authenticated is skipped (signing fails closed; a broadcast can never
    /// initiate a handshake). The message is already persisted, so failures
    /// never propagate to the caller.
    ///
    /// Returns the number of **local** room members the signed frame was
    /// successfully emitted to (in Model B, remote deliveries happen on the
    /// instances owning those sockets and are not counted here); a non-empty
    /// room delivering to zero members is logged at `warn` inside the adapter
    /// as a degradation signal.
    ///
    /// One code path across topologies: the signed local leg
    /// ([`Self::deliver_local`]) is identical in Model A and Model B; Model B
    /// only *adds* a non-blocking publish of the UNSIGNED message so owner
    /// instances elsewhere can run the same local leg for their members
    /// (carry-unsigned/sign-on-owner). Model A skips the publish.
    pub async fn broadcast_to_room(&self, room_id: &str, event: &str, msg: &RoomMessage) -> usize {
        let span = tracing::debug_span!(
            "broadcast",
            room = %room_id,
            event = %event,
            msg_id = %msg.message_id,
            delivered = tracing::field::Empty,
        );
        async {
            let data = match serde_json::to_value(msg) {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "failed to serialize room message");
                    return 0;
                }
            };
            // Model B: hand the unsigned message to the backplane first (a
            // non-blocking enqueue) so the cross-instance hop overlaps with the
            // local signing below. Best-effort by design — Redis down degrades
            // to persist + HTTP /listMessages, never fails the send.
            if let Some(bp) = &self.backplane {
                bp.publish(room_id, event, msg);
            }
            // Local leg — identical in Model A and Model B.
            let delivered = self.deliver_local(room_id, event, &data).await;
            tracing::Span::current().record("delivered", delivered);
            delivered
        }
        .instrument(span)
        .await
    }

    /// The one signed local-delivery path: sign for every authenticated local
    /// member of `room_id` (this instance's Peer sessions — the per-instance
    /// room registry) and emit over their sockets. Used by both the direct
    /// broadcast path and the Model B backplane delivery task; keeping them
    /// on one function is what keeps Model A and Model B from diverging.
    ///
    /// Because every delivery goes through here, this is also the single
    /// observation point for the fan-out + sign-latency metrics.
    async fn deliver_local(&self, room_id: &str, event: &str, data: &serde_json::Value) -> usize {
        let start = std::time::Instant::now();
        let delivered = emit_signed_to_room(&self.io, &self.core, room_id, event, data).await;
        crate::metrics::BROADCAST_SIGN_SECONDS.observe(start.elapsed().as_secs_f64());
        crate::metrics::BROADCAST_FANOUT.observe(delivered as f64);
        delivered
    }

    /// The authsocket room/session core. Public so INTEGRATION tests can drive
    /// server-side membership state the way upstream authsocket's own e2e
    /// drives its boot handle — force-drop a membership, then observe the
    /// keepalive re-assert heal it. The `pub(crate) core` field covers the
    /// unit tests in this file; integration tests live in another crate and
    /// cannot see it. Production handlers hold the core internally; nothing
    /// outside tests should need this.
    pub fn authsocket_core(&self) -> SharedAuthSocketServer<SdkProtoWallet> {
        self.core.clone()
    }

    /// Scrape-time sample: (connected sockets, distinct verified identities).
    /// Identities are the room owners (own-room enforcement means one identity
    /// ⇔ one room family) — the `mbs_rooms` lower bound; authsocket 0.1.2
    /// exposes no room enumeration.
    pub fn live_counts(&self) -> (usize, usize) {
        let sockets = self.io.sockets().unwrap_or_default();
        let mut identities = std::collections::HashSet::new();
        for s in &sockets {
            if let Some(key) = self.core.identity_key(&s.id.to_string()) {
                identities.insert(key);
            }
        }
        (sockets.len(), identities.len())
    }

    /// Admission/drain state — shared with the HTTP handlers (send guards),
    /// the admission middleware, health routes, and `/metrics`.
    pub fn ops(&self) -> &Arc<crate::ops::OpsState> {
        &self.ops
    }

    /// Model B directed routing: a socket joined a room here → ensure this
    /// instance is subscribed to the room's Redis channel. No-op in Model A.
    fn route_join(&self, sid: &str, room_id: &str) {
        if let Some(bp) = &self.backplane {
            bp.on_room_join(sid, room_id);
        }
    }

    /// A socket left a room here → drop the room subscription on the last local
    /// member. No-op in Model A.
    fn route_leave(&self, sid: &str, room_id: &str) {
        if let Some(bp) = &self.backplane {
            bp.on_room_leave(sid, room_id);
        }
    }

    /// A socket disconnected → drop all of its room subscriptions. No-op in
    /// Model A.
    fn route_disconnect(&self, sid: &str) {
        if let Some(bp) = &self.backplane {
            bp.on_socket_disconnect(sid);
        }
    }

    /// Persist-pipeline counters (shared with the background worker).
    pub fn persist_stats(&self) -> Arc<crate::persist::PersistStats> {
        Arc::clone(self.persist.stats())
    }

    /// Scrape-time sample of the persist queue: (depth, capacity).
    pub fn persist_queue(&self) -> (usize, usize) {
        (self.persist.queue_depth(), self.persist.queue_capacity())
    }

    /// Bounded wait for the persist queue to fully drain (graceful shutdown).
    /// See [`crate::persist::PersistHandle::flush`].
    pub async fn flush_persist(&self, timeout: std::time::Duration) -> bool {
        self.persist.flush(timeout).await
    }
}

/// Model B: supervise the backplane delivery drain. A panic while processing
/// one envelope must not sever cross-instance live push for the rest of the
/// process lifetime — the drain restarts over the same receiver. Without
/// supervision the dropped receiver would end the Redis subscriber, and every
/// cross-instance push to this instance would be lost until redeploy.
async fn backplane_delivery_supervisor(
    ws: WsBroadcast,
    bp: Arc<crate::backplane::Backplane>,
    rx: tokio::sync::mpsc::Receiver<String>,
) {
    run_delivery_supervised(rx, move |raw| {
        let ws = ws.clone();
        let bp = bp.clone();
        async move { deliver_backplane_envelope(&ws, &bp, raw).await }
    })
    .await;
}

/// Restart-on-panic supervision for a delivery drain over `rx`, mirroring the
/// persist worker's supervisor ([`crate::persist`]): the receiver lives in an
/// `Arc<Mutex<_>>` so a fresh drain task can reclaim it after a panic, and a
/// graceful stream close ends the supervisor cleanly. The poisoned item itself
/// is lost (its recipient falls back to the HTTP mailbox); everything queued
/// behind it is still processed. Generic over the per-item processor so this
/// contract is unit-testable without a Redis or socket stack.
async fn run_delivery_supervised<F, Fut>(rx: tokio::sync::mpsc::Receiver<String>, process: F)
where
    F: Fn(String) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let rx = Arc::new(tokio::sync::Mutex::new(rx));
    loop {
        let drain_rx = Arc::clone(&rx);
        let drain_process = process.clone();
        // Run the drain in a child task so a panic is catchable via the JoinHandle.
        let handle = tokio::spawn(async move {
            loop {
                // Hold the lock only across recv; release it during processing
                // so a restart can reclaim the receiver if processing panics.
                let next = {
                    let mut guard = drain_rx.lock().await;
                    guard.recv().await
                };
                match next {
                    Some(raw) => drain_process(raw).await,
                    None => return,
                }
            }
        });
        match handle.await {
            Ok(()) => {
                debug!("backplane delivery task ended (subscription stream closed)");
                return;
            }
            // A JoinError is a panic OR a cancellation. Nothing aborts this
            // handle today, so this arm is unreachable as written; it is here
            // so that adding an abort later cannot turn a deliberate stop into
            // a false panic alert and a restart of a task meant to end.
            Err(join_err) if join_err.is_cancelled() => {
                debug!("backplane delivery task cancelled — not restarting");
                return;
            }
            Err(join_err) => {
                crate::metrics::BACKPLANE_DELIVERY_PANICS
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                error!(
                    code = "ERR_BACKPLANE_DELIVERY_PANIC",
                    error = %join_err,
                    "backplane delivery task PANICKED — restarting over the same subscription stream. Investigate: one poisoned envelope nearly disabled cross-instance live push on this instance."
                );
            }
        }
    }
}

/// Model B: deliver one **remote-origin** envelope to local room members via
/// the same signed local-delivery path the direct broadcast uses. Own-origin
/// envelopes are skipped — the publishing instance already ran its local leg
/// at publish time (skipping prevents double delivery, not a correctness
/// gate: the client also dedupes on messageId).
///
/// Only this instance holds the authsocket `Peer` sessions for its sockets,
/// so only it can sign for them — the envelope arrives UNSIGNED and signing
/// happens here, on the connection owner.
async fn deliver_backplane_envelope(
    ws: &WsBroadcast,
    bp: &crate::backplane::Backplane,
    raw: String,
) {
    let envelope: crate::backplane::BackplaneEnvelope = match serde_json::from_str(&raw) {
        Ok(e) => e,
        Err(e) => {
            warn!(error = %e, "backplane: undecodable envelope — skipped");
            return;
        }
    };
    if envelope.origin == bp.instance_id() {
        // Own envelope back off the channel: skip delivery (the local leg
        // already ran at publish time), but use it to measure pub/sub
        // round-trip lag on a single clock (publisher == observer).
        if envelope.published_at_us > 0 {
            let lag_us = crate::backplane::now_epoch_us().saturating_sub(envelope.published_at_us);
            crate::metrics::BACKPLANE_LAG_SECONDS.observe(lag_us as f64 / 1e6);
        }
        return;
    }
    // subscribe → sign → deliver: the owner-side leg of the Model B path.
    let span = tracing::debug_span!(
        "backplane_deliver",
        origin = %envelope.origin,
        room = %envelope.room_id,
        msg_id = %envelope.message.message_id,
    );
    async {
        let data = match serde_json::to_value(&envelope.message) {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "backplane: envelope message failed to re-serialize — skipped");
                return;
            }
        };
        let delivered = ws
            .deliver_local(&envelope.room_id, &envelope.event, &data)
            .await;
        debug!(
            delivered,
            "backplane: remote-origin message delivered to local members"
        );
    }
    .instrument(span)
    .await;
}

// ---------------------------------------------------------------------------
// Socket.IO event handlers
// ---------------------------------------------------------------------------

/// Set up Socket.IO event handlers for the default namespace.
///
/// This is a **fork of `authsocket::server_io::attach`** (same protocol, same
/// public API of the crate's core) that adds the TS-parity **failure events**
/// the crate adapter swallows (parity audit rows W1/W2/W3/W5):
/// `authenticationFailed`, `joinFailed`, `leaveFailed` — plus the app-level
/// `messageFailed` in [`handle_ws_send_message`] — so clients see WHY an
/// action failed instead of a silent server-side log. Checked against
/// authsocket 0.1.2: the failure events are still not upstream, so the fork
/// stays. **Every authsocket bump must diff upstream `server_io` against this
/// function** — a behaviour upstream adds does NOT arrive with the version
/// bump. 0.1.1's keepalive room re-assert is the worked example: it is ported
/// by hand in [`handle_verified_event`]'s `authenticated` arm.
///
/// Every security invariant of the crate adapter is preserved verbatim:
/// - `authMessage` is the ONLY inbound event; everything else is dropped.
/// - The socket identity is the **cryptographically verified** sender
///   recorded by `on_auth_message` — never a client-claimed key.
/// - Own-room enforcement: a client may only join `{itsIdentityKey}-…`.
/// - Every server→client emit (failure events included) is a SIGNED general
///   message; a socket without an authenticated session gets nothing
///   (signing fails closed and can never initiate a handshake).
pub fn setup_handlers(io: &SocketIo, ws_broadcast: WsBroadcast) {
    let key_hex = ws_broadcast.server_private_key_hex.clone();
    let core = ws_broadcast.core.clone();

    io.ns("/", move |socket: SocketRef| {
        let sid = socket.id.to_string();
        info!(sid = %sid, "authsocket: new Socket.IO connection");

        // Register the BRC-103 session for this socket up front, so the first
        // inbound frame always finds its PeerHandle.
        match bsv::primitives::private_key::PrivateKey::from_hex(&key_hex) {
            Ok(pk) => core.add_connection(&sid, SdkProtoWallet::new(pk)),
            Err(e) => {
                warn!(sid = %sid, error = %e, "authsocket: server key parse failed — closing socket");
                socket.disconnect().ok();
                return;
            }
        }

        let core_msg = core.clone();
        let core_dc = core.clone();
        let ws = ws_broadcast.clone();
        let ws_dc = ws_broadcast.clone();

        // --- authMessage (BRC-103 mutual auth + general message routing) ---
        socket.on(
            AUTH_MESSAGE_EVENT,
            move |socket: SocketRef, Data(data): Data<serde_json::Value>| {
                let core = core_msg.clone();
                let ws = ws.clone();
                async move {
                    let sid = socket.id.to_string();
                    let incoming: AuthMessage = match serde_json::from_value(data) {
                        Ok(m) => m,
                        Err(e) => {
                            warn!(sid = %sid, error = %e, "authsocket: invalid authMessage payload");
                            // TS-surface parity (W1): tell the client its auth
                            // frame was rejected. Signed — a socket that never
                            // completed a handshake has no session to sign
                            // with, so this fails closed (log-only) for it.
                            emit_signed_to_socket(
                                &socket,
                                &core,
                                "authenticationFailed",
                                &serde_json::json!({"reason": "Invalid authMessage payload"}),
                            )
                            .await;
                            return;
                        }
                    };

                    // Drive the Peer: verifies signatures, runs handshake
                    // steps. The socket identity is recorded from the
                    // VERIFIED sender inside on_auth_message, before events
                    // are returned.
                    let driven = core.on_auth_message(&sid, incoming).await;

                    // Handshake responses / signed replies back over this socket.
                    for msg in driven.outbound {
                        emit_frame(&socket, &sid, &msg);
                    }

                    // Verified app events: room verbs + MessageBox app verbs.
                    for ev in driven.events {
                        handle_verified_event(&core, &socket, &sid, &ws, ev).await;
                    }
                }
            },
        );

        // --- disconnect ---
        socket.on_disconnect(
            move |socket: SocketRef, reason: socketioxide::socket::DisconnectReason| {
                let core = core_dc.clone();
                let ws = ws_dc.clone();
                async move {
                    let sid = socket.id.to_string();
                    core.remove_connection(&sid);
                    ws.route_disconnect(&sid);
                    info!(sid = %sid, reason = ?reason, "authsocket: client disconnected");
                }
            },
        );
    });

    info!("authsocket handlers attached (BRC-103 over Socket.IO, TS-parity failure events)");
}

/// Serialize one signed frame and emit it as `authMessage` (the fork of the
/// crate's private `emit_frame`).
fn emit_frame(socket: &SocketRef, sid: &str, msg: &AuthMessage) {
    match serde_json::to_value(msg) {
        Ok(json) => {
            if let Err(e) = socket.emit(AUTH_MESSAGE_EVENT, &json) {
                warn!(sid = %sid, error = %e, "authsocket: emit failed — signed frame not delivered");
            }
        }
        Err(e) => {
            warn!(sid = %sid, error = %e, "authsocket: failed to serialize signed frame");
        }
    }
}

/// Route one verified event: generic room verbs (with TS-parity failure
/// events) here, the MessageBox app verb and the two relay lanes below.
async fn handle_verified_event(
    core: &AuthSocketServer<SdkProtoWallet>,
    socket: &SocketRef,
    sid: &str,
    ws: &WsBroadcast,
    ev: VerifiedEvent,
) {
    match ev.event_name.as_str() {
        "authenticated" => {
            // Identity was recorded from the verified sender before dispatch;
            // the claimed key in the payload is deliberately ignored (unlike
            // TS, which stores it — the spoofable-claimed-sender bug).
            match core.identity_key(sid) {
                Some(identity) => {
                    // Presence self-heal, hand-ported from authsocket 0.1.1's
                    // `server_io::attach`. Bumping the crate does NOT deliver
                    // it here: `setup_handlers` is a FORK of that function (see
                    // its doc comment), so upstream's copy never runs on this
                    // server. The keepalive probe the 0.1.2 client sends every
                    // 2s carries the rooms it believes it belongs to;
                    // re-asserting each one means server-side routability
                    // cannot silently rot while the socket is live — any skew
                    // (a deploy, a dropped membership) heals within one
                    // keepalive interval instead of waiting for a reconnect.
                    // On a long MPC ceremony, rotted membership is how rounds
                    // go missing.
                    //
                    // Scope the claim honestly: this heals CLIENT-TRACKED
                    // memberships only. The MPC relay lane auto-joins the
                    // recipient's per-ceremony room SERVER-side
                    // (`join_recipient_presence_sockets`); the client never
                    // joined it, so it is not in this snapshot and the
                    // keepalive cannot heal what the client never joined.
                    if let Some(rooms) = ev.data.get("rooms").and_then(serde_json::Value::as_array)
                    {
                        // Dedupe: a probe is client-controlled input, and each
                        // entry costs a lock acquisition in the core. The
                        // checks themselves are in-memory string comparisons,
                        // so there is no per-probe cap — a flat cap would
                        // permanently starve tail rooms, because the client's
                        // snapshot iterates in stable order and truncation
                        // would drop the SAME rooms on every probe.
                        let mut seen = std::collections::HashSet::new();
                        for room_id in rooms.iter().filter_map(serde_json::Value::as_str) {
                            if room_id.is_empty() || !seen.insert(room_id) {
                                continue;
                            }
                            // This fork's HARDENED own-room rule, deliberately
                            // NOT upstream's bare `starts_with(&identity)`
                            // (which would admit any room id that merely begins
                            // with the key): `{key}` or `{key}-…` only, the
                            // same delimiter-anchored check the joinRoom arm
                            // applies. The keepalive must never authorize a
                            // room `joinRoom` itself would refuse. The
                            // empty-identity guard stays — `starts_with("")`
                            // would admit every room.
                            if identity.is_empty()
                                || !(room_id == identity
                                    || room_id.starts_with(&format!("{identity}-")))
                            {
                                warn!(sid = %sid, room = %room_id,
                                    "authsocket: keepalive room re-assert rejected — identity mismatch");
                                continue;
                            }
                            // Pair the core membership with the Model B route,
                            // exactly like the joinRoom arm: `core.join_room`
                            // alone heals local fan-out but leaves the
                            // backplane route dead, so remote-origin
                            // deliveries would stay rotted — the silent half of
                            // the skew. Both calls are idempotent.
                            core.join_room(sid, room_id);
                            ws.route_join(sid, room_id);
                        }
                    }
                    emit_signed_to_socket(
                        socket,
                        core,
                        "authenticationSuccess",
                        &serde_json::json!({ "status": "success", "identityKey": identity }),
                    )
                    .await;
                    debug!(sid = %sid, "authsocket: signed authenticationSuccess sent");
                }
                None => {
                    // Unreachable in practice (a verified event always records
                    // its sender), but if it ever happens the client must not
                    // be left waiting on its 5s timeout (W1).
                    warn!(sid = %sid, "authsocket: authenticated event without a verified identity");
                    emit_signed_to_socket(
                        socket,
                        core,
                        "authenticationFailed",
                        &serde_json::json!({"reason": "Authentication failed"}),
                    )
                    .await;
                }
            }
        }
        "joinRoom" => {
            let room_id = ev.data.as_str().unwrap_or("").to_string();
            if room_id.is_empty() {
                warn!(sid = %sid, "authsocket: joinRoom with empty room id");
                emit_signed_to_socket(
                    socket,
                    core,
                    "joinFailed",
                    &serde_json::json!({"reason": "Invalid room ID"}),
                )
                .await;
                return;
            }
            // A client may only join its OWN room ({identityKey}-{messageBox}).
            // Fail closed: no verified identity -> no join. (`ev.sender` is that
            // identity, but read it back from the server so the check can never
            // drift from what emit_to_room will trust.)
            // Anchor on the `{identityKey}-` boundary, not a bare prefix: a
            // bare starts_with(key) would authorize any room whose id merely
            // begins with the key. 66-hex keys can't prefix one another today,
            // but the delimiter check makes the own-room invariant robust (the
            // CF-Rust reference enforces exactly this `{key}-` form).
            let owns_room = core
                .identity_key(sid)
                .is_some_and(|key| room_id == key || room_id.starts_with(&format!("{key}-")));
            if !owns_room {
                warn!(sid = %sid, room = %room_id,
                    "authsocket: joinRoom rejected — identity mismatch");
                // W2: the rejection is no longer silent — but it stays a
                // rejection (the own-room hardening is a keep, not a revert).
                emit_signed_to_socket(
                    socket,
                    core,
                    "joinFailed",
                    &serde_json::json!({"reason": "You may only join your own room"}),
                )
                .await;
                return;
            }
            core.join_room(sid, &room_id);
            ws.route_join(sid, &room_id);
            debug!(sid = %sid, room = %room_id, "authsocket: joined room");
            emit_signed_to_socket(
                socket,
                core,
                "joinedRoom",
                &serde_json::json!({ "roomId": room_id }),
            )
            .await;
        }
        "leaveRoom" => {
            let room_id = ev.data.as_str().unwrap_or("").to_string();
            if room_id.is_empty() {
                // W3: was a silent return.
                emit_signed_to_socket(
                    socket,
                    core,
                    "leaveFailed",
                    &serde_json::json!({"reason": "Invalid room ID"}),
                )
                .await;
                return;
            }
            core.leave_room(sid, &room_id);
            ws.route_leave(sid, &room_id);
            debug!(sid = %sid, room = %room_id, "authsocket: left room");
            emit_signed_to_socket(
                socket,
                core,
                "leftRoom",
                &serde_json::json!({ "roomId": room_id }),
            )
            .await;
        }
        "sendMessage" => {
            handle_ws_send_message(socket, ws, sid, &ev.sender, ev.data).await;
        }
        MPC_ENVELOPE_EVENT => {
            handle_ws_mpc_envelope(core, ws, sid, ev.data).await;
        }
        // The wallet-RPC lane, spelled as two arms rather than one so the verb
        // each one forwards is the verb it matched. rust-mpc's hub takes the
        // same care (`route_wallet_frame(&event.event_name, …)`): a `walletReply`
        // re-emitted as a `walletCall` would reach the app's socket and be
        // dispatched as an inbound request it has no handler for, so the reply
        // is lost to a timeout with nothing in any log naming the cause.
        WALLET_CALL_EVENT => {
            handle_ws_wallet_frame(core, ws, sid, WALLET_CALL_EVENT, ev.data).await;
        }
        WALLET_REPLY_EVENT => {
            handle_ws_wallet_frame(core, ws, sid, WALLET_REPLY_EVENT, ev.data).await;
        }
        other => {
            debug!(sid = %sid, event = %other, "BRC-103 general message: unhandled event");
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Current UTC timestamp in ISO 8601 format.
fn now_iso8601() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

/// Log the persist outcome so a fast-path bypass (and any dead-letter) is
/// observable. `Queued` is the silent happy path; everything else means the
/// caller paid DB latency inline or the job was dead-lettered to disk.
fn log_persist_outcome(
    sid: &str,
    message_id: &str,
    recipient: &str,
    outcome: crate::persist::Enqueued,
) {
    use crate::persist::Enqueued;
    match outcome {
        Enqueued::Queued => {}
        Enqueued::InlineOk => debug!(
            sid = %sid, msg_id = %message_id, recipient = %recipient,
            "persist: fast path bypassed — persisted inline (DB backpressure), row durably written"
        ),
        Enqueued::InlineDeadLettered => warn!(
            sid = %sid, msg_id = %message_id, recipient = %recipient,
            "persist: inline write exhausted transient retries — dead-lettered to disk; row NOT yet in MySQL"
        ),
        Enqueued::DeadLettered => error!(
            sid = %sid, msg_id = %message_id, recipient = %recipient,
            "persist: inline write hit a permanent error — dead-lettered to disk; row NOT in MySQL"
        ),
        Enqueued::Rejected => warn!(
            sid = %sid, msg_id = %message_id, recipient = %recipient,
            "persist: messageId conflicts with a stored id under the column collation — send rejected, nothing stored and nothing to replay"
        ),
    }
}

/// Maximum allowed message body size (1 MB).
/// Prevents DoS via oversized messages stored in the database.
const MAX_MESSAGE_BODY_BYTES: usize = 1024 * 1024;

/// Handle a sendMessage event received via BRC-103 general message.
///
/// The client sends: `{"roomId": "{recipient}-{mb}", "message": {"messageId":
/// "...", "recipient": "...", "body": "..."}}`. `sender` is the
/// **cryptographically verified** sender key from the general message (the
/// crate records it as the socket identity before dispatch).
///
/// Pushes the message live to the target room, enqueues the durable INSERT,
/// and emits a signed `sendMessageAck-{roomId}` back to the sender. Invalid
/// payloads emit a signed `messageFailed {reason}` to the sender (parity
/// audit W5 — TS surface; previously a silent log meant the client only
/// discovered the failure via ack-timeout → HTTP fallback).
#[tracing::instrument(
    name = "ws_send_message",
    skip_all,
    fields(sid = %sid, sender = %sender, msg_id = tracing::field::Empty, room = tracing::field::Empty)
)]
async fn handle_ws_send_message(
    socket: &SocketRef,
    ws: &WsBroadcast,
    sid: &str,
    sender: &str,
    data: serde_json::Value,
) {
    // In-flight marker for graceful drain: this send belongs to a connected
    // session and is never nacked — drain waits for it (bounded) instead.
    let _send_guard = ws.ops.begin_send();

    /// Signed `messageFailed {reason}` back to the sender (W5).
    async fn message_failed(socket: &SocketRef, ws: &WsBroadcast, reason: &str) {
        emit_signed_to_socket(
            socket,
            &ws.core,
            "messageFailed",
            &serde_json::json!({ "reason": reason }),
        )
        .await;
    }

    // Parse the sendMessage payload
    let room_id_str = match data.get("roomId").and_then(|v| v.as_str()) {
        Some(r) if !r.is_empty() => r.to_string(),
        _ => {
            warn!(sid = %sid, "BRC-103 sendMessage: missing roomId");
            message_failed(socket, ws, "Missing roomId").await;
            return;
        }
    };

    let message = match data.get("message") {
        Some(m) if !m.is_null() => m,
        _ => {
            warn!(sid = %sid, "BRC-103 sendMessage: missing message object");
            message_failed(socket, ws, "Missing message object").await;
            return;
        }
    };

    // Trimmed to match the HTTP path and the column: `messages.messageId` is
    // PAD SPACE, so MySQL already treats "id" and "id  " as the same value.
    // Storing them untrimmed would make a plain client typo a rejected send.
    let message_id = match message.get("messageId").and_then(|v| v.as_str()) {
        Some(id) if !id.trim().is_empty() => id.trim().to_string(),
        _ => {
            warn!(sid = %sid, "BRC-103 sendMessage: missing messageId");
            message_failed(socket, ws, "Missing messageId").await;
            return;
        }
    };
    // Same cap as the HTTP path: the id goes into a VARCHAR(255), so an
    // oversized one fails the INSERT as a permanent error, which dead-letters
    // the whole job to disk. (The FCM half of the HTTP path's rationale does
    // not apply here — this path never sends a push.)
    if message_id.chars().count() > crate::handlers::send_message::MAX_MESSAGE_ID_CHARS {
        warn!(sid = %sid, "BRC-103 sendMessage: messageId exceeds the length limit");
        message_failed(socket, ws, "messageId exceeds the length limit").await;
        return;
    }
    // Backfill the span fields declared Empty on the instrument attribute.
    let span = tracing::Span::current();
    span.record("msg_id", tracing::field::display(&message_id));
    span.record("room", tracing::field::display(&room_id_str));

    let recipient = match message.get("recipient").and_then(|v| v.as_str()) {
        Some(r) if !r.is_empty() => r.to_string(),
        _ => {
            warn!(sid = %sid, "BRC-103 sendMessage: missing recipient");
            message_failed(socket, ws, "Missing recipient").await;
            return;
        }
    };

    let body = match message.get("body") {
        Some(b) if !b.is_null() => {
            if let Some(s) = b.as_str() {
                s.to_string()
            } else {
                b.to_string()
            }
        }
        _ => {
            warn!(sid = %sid, "BRC-103 sendMessage: missing body");
            message_failed(socket, ws, "Missing message body").await;
            return;
        }
    };

    if body.len() > MAX_MESSAGE_BODY_BYTES {
        warn!(sid = %sid, size = body.len(), "BRC-103 sendMessage: body exceeds size limit");
        message_failed(socket, ws, "Message body exceeds size limit").await;
        return;
    }

    if sender.is_empty() {
        warn!(sid = %sid, "BRC-103 sendMessage: no verified sender identity");
        // No verified identity ⇒ no session to sign a failure event with;
        // the emit below fails closed, which is the correct posture.
        message_failed(socket, ws, "No verified sender identity").await;
        return;
    }

    // Extract the identity key + messageBox from roomId. The key is the
    // authoritative delivery target for this send.
    let (room_key, message_box) = match authsocket::split_room_id(&room_id_str) {
        Some((key, mb)) => (key, mb),
        None => {
            warn!(sid = %sid, room = %room_id_str, "BRC-103 sendMessage: invalid roomId format");
            message_failed(socket, ws, "Invalid roomId format").await;
            return;
        }
    };

    // Bind the payload `recipient` to the roomId's identity key. These are two
    // independent client-supplied fields: the block check below keys on
    // `recipient`, but the live broadcast targets `room_id_str`. If they diverge,
    // a sender blocked by victim V can push into `<V>-<box>` (a room only V can
    // join, by own-room enforcement) while naming some OTHER `recipient` whose
    // block row is absent — defeating recipient-block enforcement entirely.
    // Legitimate clients always build roomId as `<recipient>-<box>`, so this
    // equality holds for every honest send; reject any mismatch.
    if room_key != recipient {
        warn!(sid = %sid, room = %room_id_str, "BRC-103 sendMessage: roomId identity does not match recipient — rejected");
        message_failed(socket, ws, "roomId does not match recipient").await;
        return;
    }

    // Bound the two remaining client-controlled strings that reach MySQL.
    //
    // `recipient` and `message_box` land in VARCHAR(255) columns via
    // `ensure_message_box`, whose `INSERT IGNORE` TRUNCATES an oversized value
    // to 255 while the follow-up SELECT looks for the full-length one — so the
    // lookup misses, the job is classified permanent, and the whole message
    // (body included, up to MAX_MESSAGE_BODY_BYTES) is appended to the
    // dead-letter file. The sender is acked "success" and nothing is stored, so
    // a single socket can both silently lose its own messages and fill the
    // disk. The HTTP path is not exposed to this: it validates the recipient
    // key and resolves the box synchronously, failing the request instead.
    //
    // A valid identity key is exactly 66 hex chars, so validating the key also
    // bounds it; `message_box` gets the column's own limit.
    if !crate::handlers::helpers::is_valid_pub_key(&recipient) {
        warn!(sid = %sid, "BRC-103 sendMessage: recipient is not a valid identity key");
        message_failed(socket, ws, "Invalid recipient identity key").await;
        return;
    }
    if message_box.trim().is_empty()
        || message_box.chars().count() > crate::handlers::send_message::MAX_MESSAGE_BOX_CHARS
    {
        warn!(sid = %sid, "BRC-103 sendMessage: messageBox missing or too long");
        message_failed(socket, ws, "Invalid messageBox").await;
        return;
    }

    // Recipient-block enforcement (parity with the HTTP path): a recipient can
    // block a sender (recipient_fee == -1). The WS path honors it too, so a
    // blocked sender can't route around the block over WebSocket. We fail
    // CLOSED on a DB error: a block is a safety control, and this path both
    // pushes live AND persists to the mailbox (which does NOT re-check the
    // block at read time), so delivering on an unverified lookup would durably
    // leak a blocked message. Rejecting on a permissions-store blip is the safe
    // posture — the same MySQL backs persistence anyway, so a real outage
    // degrades the whole send, not just the block check.
    match crate::db::queries::get_recipient_fee(&ws.db, &recipient, sender, &message_box).await {
        Ok(-1) => {
            warn!(sid = %sid, "BRC-103 sendMessage: recipient has blocked the sender — rejected");
            message_failed(socket, ws, "Delivery blocked by recipient").await;
            return;
        }
        Ok(_) => {}
        Err(e) => {
            error!(sid = %sid, error = %e, "BRC-103 sendMessage: recipient-block lookup failed — rejecting (fail closed; a blocked message must not be delivered/persisted on an unverified check)");
            message_failed(socket, ws, "Could not verify delivery permission").await;
            return;
        }
    }

    let ack_event = format!("sendMessageAck-{room_id_str}");

    // Wrap body in {"message": body} format matching the HTTP handler
    let stored_body = serde_json::json!({"message": body}).to_string();

    // ── PUSH-LIVE-FIRST ───────────────────────────────────────────────
    //
    // All gating checks for this path (verified sender, valid roomId,
    // body-size limit) have already passed, and the WS sendMessage path
    // carries no fee/permission/payment gate. So broadcast to the live
    // recipient IMMEDIATELY — before touching MySQL — so live delivery never
    // waits on the DB. Durability is preserved by the async persist below.
    let now = now_iso8601();
    let event = format!("sendMessage-{room_id_str}");
    ws.broadcast_to_room(
        &room_id_str,
        &event,
        &RoomMessage {
            message_id: message_id.clone(),
            sender: sender.to_string(),
            recipient: recipient.clone(),
            message_box: message_box.clone(),
            body: stored_body.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await;

    // ── PERSIST-ASYNC ─────────────────────────────────────────────────
    //
    // Hand the durable INSERT (ensure_message_box + insert_message) to the
    // background persist worker. It retries transient DB errors with backoff
    // and logs permanent failures at ERROR — the row still lands in MySQL for
    // the HTTP fallback (listMessages). A duplicate messageId is caught by the
    // unique constraint at persist time and treated as idempotent success.
    //
    // `PersistJob::new` performs the canonical `{"message": <body>}` wrap from
    // the RAW body (it owns the wrap), so we pass `body`, not the pre-wrapped
    // `stored_body` used for the live broadcast above.
    let enqueued = ws
        .persist_async(crate::persist::PersistJob::new(
            message_id.clone(),
            recipient.clone(),
            message_box.clone(),
            sender.to_string(),
            body,
        ))
        .await;

    log_persist_outcome(sid, &message_id, &recipient, enqueued);

    debug!(
        sid = %sid,
        msg_id = %message_id,
        recipient = %recipient,
        message_box = %message_box,
        "BRC-103 sendMessage: pushed live, persistence enqueued"
    );

    // Emit a signed ack to the sender socket. The ack means "accepted for
    // delivery" (pushed live + durably enqueued), not "committed to MySQL" —
    // permanent or retry-exhausted persist failures are dead-lettered to disk
    // and logged at ERROR, not silently dropped. Payload shape matches the
    // reference message-box-server: { status, messageId }.
    emit_signed_to_socket(
        socket,
        &ws.core,
        &ack_event,
        &serde_json::json!({"status": "success", "messageId": message_id}),
    )
    .await;
    debug!(sid = %sid, ack = %ack_event, "BRC-103 sendMessage: signed ack emitted");
}

/// Admit one inbound `mpcEnvelope` and produce the frame to relay, or refuse it
/// with an operator-facing reason.
///
/// Pure — no socket, no I/O — so every fail-closed rule below is unit-testable
/// without a live BRC-103 handshake, the same separation rust-mpc keeps in
/// `mpc_transport::transient_dispatch::admit_verified_sender`, which this
/// mirrors.
///
/// The rules, in the order a hostile frame meets them:
///
/// 1. **Size, before anything else.** An oversize body must cost the relay
///    neither a room lookup nor a signing fan-out. The body is the whole
///    envelope's weight, so checking it alone bounds the frame.
/// 2. **A verified identity is required.** `verified_sender` is what the
///    authsocket core recorded from the BRC-103-verified general message; no
///    identity means no provenance to stamp, and the envelope is dropped.
/// 3. **The verified identity is on the ceremony allowlist.** Fail closed: an
///    unconfigured lane has an empty allowlist and admits nobody.
/// 4. **Honest sender claim.** The body may omit `sender`, in which case the
///    verified identity fills it in. If it supplies one it must equal the
///    verified identity — a mismatch is an attempt to relay forged provenance
///    to a party that will check it, and is refused rather than corrected.
/// 5. **The relay is never a recipient.** MBS has no MPC identity and holds no
///    key share; it could not open the body if it tried, so an envelope
///    addressed to its own server identity is a misconfigured client rather
///    than traffic to route.
///
/// The returned room is `{recipient}-{box}` via [`room_id`], and the returned
/// envelope carries the re-stamped sender.
fn admit_mpc_envelope(
    ev: MpcEnvelope,
    verified_sender: Option<&str>,
    server_identity: Option<&str>,
    relay: &crate::config::MpcRelayConfig,
) -> Result<(String, MpcEnvelope), &'static str> {
    if ev.body.len() > relay.max_body_bytes {
        return Err("body exceeds the mpcEnvelope size limit");
    }
    let Some(verified) = verified_sender else {
        return Err("no verified sender identity on this socket");
    };
    if !relay
        .peer_identities
        .contains(&crate::config::canon_identity(verified))
    {
        return Err("verified sender is not an allowlisted MPC peer (MPC_PEER_IDENTITIES)");
    }
    if !ev.sender.is_empty() && ev.sender != verified {
        return Err("body sender does not match the verified socket identity");
    }
    if server_identity == Some(ev.recipient.as_str()) {
        return Err("recipient is the relay's own identity");
    }

    let room = room_id(&ev.recipient, &ev.r#box);
    Ok((
        room,
        MpcEnvelope {
            // Re-stamped from the proven identity, so the recipient's own
            // authenticated decrypt is checked against a sender the relay
            // verified rather than one the frame asserted.
            sender: verified.to_string(),
            ..ev
        },
    ))
}

/// Relay one verified `mpcEnvelope`: the transient MPC ceremony lane, which
/// lets an enterprise box and a cosigner run a ceremony through MBS instead of
/// through a coordinator-hosted dial-in hub.
///
/// ## Routing: `{recipient}-{box}`, with auto-join
///
/// The envelope goes to `room_id(recipient, box)` — the same addressing every
/// other lane on this server uses, and the same room the rust-mpc coordinator's
/// `deliver_to_direct` emits into. A ceremony party joins only its PRESENCE
/// room (`{identity}-mpc_inbox`), so the per-ceremony box room starts empty;
/// when it is, the relay harvests the presence room and joins every socket
/// there whose VERIFIED identity equals the recipient into the target room
/// before emitting. That closes the join-before-first-round race with no
/// durable buffer, and it preserves authsocket's own-room-only invariant by
/// construction: a socket is only ever joined into a room prefixed with its own
/// verified identity — the exact condition [`handle_verified_event`] enforces
/// when a client asks to join a room itself.
///
/// ## Known limitation: Model B fan-out is unspecified for this lane
///
/// This lane is correct under **Model A** (single instance, no `REDIS_URL`).
/// Under **Model B** an auto-joined per-ceremony room is not guaranteed to fan
/// out across instances: [`crate::backplane::Backplane`] subscribes an instance
/// to a room's channel only while that instance owns a local member that
/// *joined it itself*, and ceremony clients never join per-ceremony rooms. This
/// is an open design question rather than an oversight — every Redis rule in
/// the transport spec is written against the durable `RoomMessage` lane and
/// leans on MySQL persistence as the fallback, which a transient lane has none
/// of by definition. Run the ceremony lane on a single instance until the
/// multi-instance transient contract exists.
///
/// ## Opaque, transient, unpersisted
///
/// The body is end-to-end BRC-78 ciphertext between two ceremony parties; the
/// relay holds no share and no session key, and never attempts to open it. It
/// also never persists: MPC envelopes are not durable room events — no mailbox
/// row, no `messageId` dedup store, nothing for `/listMessages` to hand back —
/// because a ceremony round that missed its window is worthless and the
/// ceremony's own timeout-and-retry is the recovery mechanism.
///
/// That is why this path emits through [`emit_signed_to_room`] directly instead
/// of [`WsBroadcast::broadcast_to_room`]. Two reasons, both structural: the
/// broadcast path carries a [`RoomMessage`], whose `messageBox` key the ceremony
/// client cannot parse (it reads `box`), so routing through it would need a
/// second shaping step that could drift; and the transient lane is deliberately
/// kept off the durable lane's machinery — the persist queue, the mailbox
/// semantics and the backplane publish are all mailbox concerns this lane does
/// not want. [`MpcEnvelope`] is therefore the ONE producer of this frame's JSON.
#[tracing::instrument(name = "ws_mpc_envelope", skip_all, fields(sid = %sid))]
async fn handle_ws_mpc_envelope(
    core: &AuthSocketServer<SdkProtoWallet>,
    ws: &WsBroadcast,
    sid: &str,
    data: serde_json::Value,
) {
    // In-flight marker for graceful drain, as on the sendMessage path: a
    // relay hop in progress belongs to a connected session, and drain waits
    // (bounded) for it rather than cutting a ceremony round mid-flight.
    let _send_guard = ws.ops.begin_send();

    let ev: MpcEnvelope = match serde_json::from_value(data) {
        Ok(ev) => ev,
        Err(e) => {
            warn!(sid = %sid, error = %e, "mpc relay: dropping malformed mpcEnvelope");
            return;
        }
    };

    // Kept for the refusal log, which must be able to name the frame it
    // dropped after `ev` has been consumed by the admission check.
    let recipient = ev.recipient.clone();
    let message_box = ev.r#box.clone();
    let body_bytes = ev.body.len();

    // The verified identity is read back from the core (rather than taken from
    // the dispatched event) for the same reason the `joinRoom` arm reads it
    // back: the check can then never drift from what `emit_to_room` will trust.
    let admitted = admit_mpc_envelope(
        ev,
        core.identity_key(sid).as_deref(),
        ws.server_identity.as_deref(),
        &ws.mpc_relay,
    );
    let (room, envelope) = match admitted {
        Ok(admitted) => admitted,
        Err(reason) => {
            warn!(
                sid = %sid,
                recipient = %recipient,
                r#box = %message_box,
                body_bytes,
                limit = ws.mpc_relay.max_body_bytes,
                reason,
                "mpc relay: refusing mpcEnvelope (fail-closed)"
            );
            return;
        }
    };

    join_recipient_presence_sockets(core, &envelope.recipient, &room);

    let data = match serde_json::to_value(&envelope) {
        Ok(v) => v,
        Err(e) => {
            warn!(sid = %sid, error = %e, "mpc relay: failed to serialize mpcEnvelope");
            return;
        }
    };
    let delivered = emit_signed_to_room(&ws.io, core, &room, MPC_ENVELOPE_EVENT, &data).await;
    if delivered == 0 {
        warn!(
            sid = %sid,
            recipient = %envelope.recipient,
            r#box = %envelope.r#box,
            room = %room,
            delivered,
            "mpc relay: envelope reached no live socket — the recipient is not dialed in here; \
             the round is dropped and the ceremony will time out and retry"
        );
    } else {
        debug!(
            sid = %sid,
            recipient = %envelope.recipient,
            r#box = %envelope.r#box,
            delivered,
            "mpc relay: envelope forwarded opaquely (not persisted)"
        );
    }
}

/// Admit one inbound wallet-lane frame and produce the frame to relay, or refuse
/// it with an operator-facing reason. The wallet-plane sibling of
/// [`admit_mpc_envelope`], and pure for the same reason: every fail-closed rule
/// is unit-testable without a live BRC-103 handshake.
///
/// The rules are the ceremony lane's, with ONE deliberate difference — the
/// population admitted:
///
/// 1. **Size, before anything else.** `frame` is the whole payload's weight, so
///    bounding it bounds the frame. The ceiling is the wallet lane's own
///    ([`crate::config::WalletRelayConfig::max_frame_bytes`]).
/// 2. **A verified identity is required.** No identity means no provenance to
///    stamp.
/// 3. **The verified identity is admitted by the UNION** of the wallet-app
///    allowlist and the ceremony peer list. Both populations belong here and for
///    different reasons: an app dials in to call a box's BRC-100 surface, and a
///    box or cosigner sends its `walletReply` back under its own ROSTER
///    identity, never an app one. rust-mpc admits exactly this union —
///    `admit_wallet_sender(verified, claimed, roster, apps)` at
///    `crates/transport/src/wallet_lane_wire.rs:96`, called with the hub's
///    roster allowlist and its app allowlist at `bins/coordinator/src/mpc_hub.rs:1963-1968`.
///
///    The union runs ONE WAY, which is the load-bearing part. [`admit_mpc_envelope`]
///    reads the peer list ALONE and cannot see the app list at all, so an app
///    admitted here can never route ceremony traffic — it is "scoped to the
///    wallet verbs by construction" (`wallet_lane_wire.rs:82-85`). Fail-closed:
///    with both populations empty nobody is admitted.
/// 4. **Honest sender claim.** An omitted `sender` is filled from the verified
///    identity; a supplied one must equal it, or the frame is refused rather
///    than corrected.
/// 5. **The relay is never a recipient.** MBS terminates no wallet session and
///    holds no key for the nested one, so a frame addressed to its own server
///    identity is a misconfigured client rather than traffic to route.
///
/// The returned room is `{recipient}-wallet_inbox` via [`room_id`], and the
/// returned frame carries the re-stamped sender.
fn admit_wallet_frame(
    ev: WalletFrame,
    verified_sender: Option<&str>,
    server_identity: Option<&str>,
    mpc_relay: &crate::config::MpcRelayConfig,
    wallet_relay: &crate::config::WalletRelayConfig,
) -> Result<(String, WalletFrame), &'static str> {
    if ev.frame.len() > wallet_relay.max_frame_bytes {
        return Err("frame exceeds the wallet-lane size limit");
    }
    let Some(verified) = verified_sender else {
        return Err("no verified sender identity on this socket");
    };
    let canon = crate::config::canon_identity(verified);
    if !wallet_relay.app_identities.contains(&canon) && !mpc_relay.peer_identities.contains(&canon)
    {
        return Err(
            "verified sender is neither an allowlisted wallet app (MPC_WALLET_APP_IDENTITIES) \
             nor a ceremony peer (MPC_PEER_IDENTITIES)",
        );
    }
    if !ev.sender.is_empty() && ev.sender != verified {
        return Err("body sender does not match the verified socket identity");
    }
    if server_identity == Some(ev.recipient.as_str()) {
        return Err("recipient is the relay's own identity");
    }

    let room = room_id(&ev.recipient, WALLET_INBOX);
    Ok((
        room,
        WalletFrame {
            // Re-stamped from the proven identity: the recipient opens the
            // nested session against a sender this relay verified, not one the
            // frame asserted.
            sender: verified.to_string(),
            ..ev
        },
    ))
}

/// Relay one verified wallet-lane frame: the wallet-RPC lane, which lets an app
/// reach an enterprise box's BRC-100 surface through MBS. The `verb` is
/// `walletCall` or `walletReply` and is FORWARDED AS RECEIVED.
///
/// This is what makes a strictly dial-out box reachable. The box runs with its
/// HTTP listener disabled and serves its whole gated BRC-100 surface over this
/// lane; without the relay carrying these two verbs, nothing can reach it.
///
/// ## Routing: `{recipient}-wallet_inbox`, and NO auto-join
///
/// Unlike the ceremony lane, this lane has exactly ONE room per identity: a
/// party joins `{identity}-wallet_inbox` for itself on connect (rust-mpc's box
/// does so at `bins/enterprise-wallet/src/wallet_lane.rs:506`), and that is the
/// room every frame addressed to it lands in. There is no per-session room to
/// race against, so there is nothing for an auto-join to close — rust-mpc's
/// `route_wallet_frame` (`bins/coordinator/src/mpc_hub.rs:1982-1990`) computes
/// that room and emits into it with no harvest step, and this follows it.
/// [`join_recipient_presence_sockets`] exists for the ceremony lane's
/// per-ceremony box rooms and is deliberately not called here.
///
/// The Model B limitation stated on [`handle_ws_mpc_envelope`] does NOT apply in
/// the same form: the wallet presence room is one the client joined itself, so
/// [`WsBroadcast::route_join`] has already subscribed this instance to its
/// channel. What this lane still shares with the ceremony lane is that it emits
/// through [`emit_signed_to_room`] rather than
/// [`WsBroadcast::broadcast_to_room`], so it never PUBLISHES to the backplane —
/// a frame is delivered by the instance that received it, to the sockets that
/// instance owns. Run both relay lanes on a single instance until the
/// multi-instance transient contract exists.
///
/// ## Opaque, transient, unpersisted
///
/// `frame` is a nested BRC-103 `AuthMessage` wrapping BRC-2 ciphertext between
/// the app and the box. The relay is not a party to that session and holds
/// neither identity's key: it cannot open the frame, cannot forge one, and never
/// tries. Its total compromise is an availability event, never a custody one.
///
/// Nothing here persists, for the same reason nothing on the ceremony lane does:
/// a wallet call is a live request↔response, not a durable room event. No
/// mailbox row, no dedup store, nothing for `/listMessages` to hand back — a
/// call whose reply missed its window is retried by the caller. [`WalletFrame`]
/// is the ONE producer of this frame's JSON, so the `correlationId`/`frame` keys
/// can never drift into [`RoomMessage`]'s `messageBox` shape.
#[tracing::instrument(name = "ws_wallet_frame", skip_all, fields(sid = %sid, verb = %verb))]
async fn handle_ws_wallet_frame(
    core: &AuthSocketServer<SdkProtoWallet>,
    ws: &WsBroadcast,
    sid: &str,
    verb: &str,
    data: serde_json::Value,
) {
    // In-flight marker for graceful drain, as on the other two send paths: a
    // relay hop in progress belongs to a connected session, and drain waits
    // (bounded) for it rather than cutting a wallet call mid-flight.
    let _send_guard = ws.ops.begin_send();

    let ev: WalletFrame = match serde_json::from_value(data) {
        Ok(ev) => ev,
        Err(e) => {
            warn!(sid = %sid, verb = %verb, error = %e, "wallet relay: dropping malformed wallet frame");
            return;
        }
    };

    // Kept for the refusal log, which must be able to name the frame it dropped
    // after `ev` has been consumed by the admission check.
    let recipient = ev.recipient.clone();
    let correlation_id = ev.correlation_id.clone();
    let frame_bytes = ev.frame.len();

    // The verified identity is read back from the core rather than taken from
    // the dispatched event, for the same reason the `joinRoom` arm reads it
    // back: the check can then never drift from what `emit_to_room` will trust.
    let admitted = admit_wallet_frame(
        ev,
        core.identity_key(sid).as_deref(),
        ws.server_identity.as_deref(),
        &ws.mpc_relay,
        &ws.wallet_relay,
    );
    let (room, frame) = match admitted {
        Ok(admitted) => admitted,
        Err(reason) => {
            warn!(
                sid = %sid,
                verb = %verb,
                recipient = %recipient,
                correlation_id = %correlation_id,
                frame_bytes,
                limit = ws.wallet_relay.max_frame_bytes,
                reason,
                "wallet relay: refusing wallet frame (fail-closed)"
            );
            return;
        }
    };

    let data = match serde_json::to_value(&frame) {
        Ok(v) => v,
        Err(e) => {
            warn!(sid = %sid, verb = %verb, error = %e, "wallet relay: failed to serialize wallet frame");
            return;
        }
    };
    // `verb`, not a constant: the arriving verb is the one that leaves.
    let delivered = emit_signed_to_room(&ws.io, core, &room, verb, &data).await;
    if delivered == 0 {
        warn!(
            sid = %sid,
            verb = %verb,
            recipient = %frame.recipient,
            correlation_id = %frame.correlation_id,
            room = %room,
            delivered,
            "wallet relay: frame reached no live socket — the recipient is not dialed in on the \
             wallet lane here; the frame is dropped and the caller's request times out"
        );
    } else {
        debug!(
            sid = %sid,
            verb = %verb,
            recipient = %frame.recipient,
            correlation_id = %frame.correlation_id,
            delivered,
            "wallet relay: frame forwarded opaquely (not persisted)"
        );
    }
}

/// Subscribe the recipient's presence sockets into an empty per-ceremony box
/// room, so the first round of a ceremony is not lost to the gap between the
/// client dialing in and the box room existing.
///
/// Ported from `deliver_to_direct` in rust-mpc's `bins/coordinator/src/mpc_hub.rs`.
/// A socket is joined ONLY when its verified identity equals the recipient the
/// room is named for, which is precisely authsocket's own-room-only invariant —
/// the auto-join can therefore never place a socket somewhere the client could
/// not have joined itself.
///
/// Skipped once the room has members: a party that is already subscribed needs
/// no help, and re-harvesting on every round would re-add sockets the recipient
/// deliberately left.
///
/// The membership an auto-join creates lives as long as the socket: a party that
/// runs many ceremonies over one connection accumulates one room per session
/// until it disconnects, at which point `remove_connection` drops all of them.
/// rust-mpc's hub reclaims its equivalent rooms on a `box_ttl_secs` sweep; this
/// relay has no such sweep yet.
fn join_recipient_presence_sockets(
    core: &AuthSocketServer<SdkProtoWallet>,
    recipient: &str,
    room: &str,
) {
    if !core.room_members(room).is_empty() {
        return;
    }
    let presence = room_id(recipient, MPC_INBOX);
    for member_sid in core.room_members(&presence) {
        if core.identity_key(&member_sid).as_deref() == Some(recipient) {
            debug!(sid = %member_sid, room = %room, "mpc relay: auto-joined a presence socket into the ceremony box room");
            // `core.join_room` WITHOUT the `ws.route_join` that every other join
            // site pairs with, and deliberately so: `route_join` exists to make
            // this instance subscribe to the room's Redis channel, and this lane
            // never publishes to the backplane — it emits through
            // `emit_signed_to_room` directly (see `handle_ws_mpc_envelope`).
            // Subscribing would buy a channel nothing ever writes to.
            //
            // This is NOT the missing-route_join bug the keepalive re-assert had.
            // There the durable lane's `broadcast_to_room` DOES publish, so an
            // unrouted membership silently loses cross-instance delivery. Here
            // the absence is the Model-A scoping stated on the handler, not an
            // oversight — if this lane ever gains multi-instance fan-out, the
            // route_join comes back with it, as part of that design.
            core.join_room(member_sid, room);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bsv::primitives::private_key::PrivateKey;

    /// secp256k1 scalar `1` — a valid private key for the server identity.
    const TEST_SERVER_KEY: &str =
        "0000000000000000000000000000000000000000000000000000000000000001";

    fn test_ws() -> WsBroadcast {
        let (_layer, io) = socketioxide::SocketIo::new_layer();
        // Lazy MySQL pool: the broadcast/codec paths exercised by these tests
        // never execute a query, so the pool is never actually connected.
        let pool = sqlx::mysql::MySqlPoolOptions::new()
            .connect_lazy("mysql://test@127.0.0.1/test")
            .expect("build lazy MySQL pool");
        WsBroadcast::new(
            io,
            TEST_SERVER_KEY.to_string(),
            pool,
            None,
            crate::ops::OpsState::new(0),
            crate::config::MpcRelayConfig::default(),
            crate::config::WalletRelayConfig::default(),
        )
    }

    fn test_room_message() -> RoomMessage {
        RoomMessage {
            message_id: "m1".into(),
            sender: "s".into(),
            recipient: "r".into(),
            message_box: "inbox".into(),
            body: "{}".into(),
            created_at: "t".into(),
            updated_at: "t".into(),
        }
    }

    #[tokio::test]
    async fn broadcast_to_empty_room_is_noop() {
        let ws = test_ws();
        let delivered = ws
            .broadcast_to_room("ghost-room", "sendMessage-ghost-room", &test_room_message())
            .await;
        assert_eq!(delivered, 0);
    }

    /// A socket can be a room member without a completed handshake. Signing
    /// must fail closed (no session → nothing to sign, no handshake initiated)
    /// and the broadcast must skip it gracefully.
    #[tokio::test]
    async fn broadcast_skips_member_without_authenticated_session() {
        let ws = test_ws();
        let wallet = SdkProtoWallet::new(PrivateKey::from_hex(TEST_SERVER_KEY).expect("test key"));
        ws.core.add_connection("sock1", wallet);
        ws.core.join_room("sock1", "keyA-inbox");

        let delivered = ws
            .broadcast_to_room("keyA-inbox", "sendMessage-keyA-inbox", &test_room_message())
            .await;
        // No authenticated session → fails closed → 0 delivered, socket intact.
        assert_eq!(delivered, 0);
        assert!(ws.core.identity_key("sock1").is_none());
    }

    /// RoomMessage serializes with the camelCase field names the client's
    /// ServerPeerMessage parser requires.
    #[test]
    fn room_message_serializes_camel_case() {
        let v = serde_json::to_value(test_room_message()).expect("serialize");
        assert!(v.get("messageId").is_some(), "messageId field");
        assert!(v.get("messageBox").is_some(), "messageBox field");
        assert!(v.get("created_at").is_some(), "created_at field");
        assert!(v.get("updated_at").is_some(), "updated_at field");
    }

    /// The re-exported room_id follows `{recipient}-{messageBox}`.
    #[test]
    fn room_id_convention() {
        assert_eq!(room_id("03abc", "payment_inbox"), "03abc-payment_inbox");
    }

    /// A panic while processing one delivery must not sever the stream: the
    /// supervisor restarts the drain over the SAME receiver, so items queued
    /// behind the poisoned one are still processed, and a graceful stream
    /// close still ends the supervisor cleanly.
    #[tokio::test]
    async fn delivery_supervisor_restarts_drain_after_panic() {
        let (tx, rx) = tokio::sync::mpsc::channel::<String>(8);
        let processed = Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
        let sink = Arc::clone(&processed);
        let supervisor = tokio::spawn(run_delivery_supervised(rx, move |raw: String| {
            let sink = Arc::clone(&sink);
            async move {
                if raw == "poison" {
                    panic!("injected: poisoned envelope");
                }
                sink.lock().push(raw);
            }
        }));

        tx.send("before".into()).await.unwrap();
        tx.send("poison".into()).await.unwrap();
        tx.send("after".into()).await.unwrap();

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while processed.lock().len() < 2 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "drain did not survive the panic"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            *processed.lock(),
            ["before".to_string(), "after".to_string()]
        );

        drop(tx);
        tokio::time::timeout(std::time::Duration::from_secs(5), supervisor)
            .await
            .expect("supervisor ends when the stream closes")
            .expect("supervisor itself must not panic");
    }

    // -- MPC relay lane -----------------------------------------------------

    /// Two ceremony parties. `PEER_A` is the sender in these tests; `PEER_B` the
    /// recipient. Shaped like real compressed identity keys so the allowlist's
    /// canonicalization is exercised on realistic input.
    const PEER_A: &str = "02aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899";
    const PEER_B: &str = "03bbccddeeff00112233445566778899aabbccddeeff001122334455667788990a";

    /// The lane switched on for `peers`, at the derived payload ceiling.
    fn relay_for(peers: &[&str]) -> crate::config::MpcRelayConfig {
        crate::config::MpcRelayConfig {
            peer_identities: peers
                .iter()
                .map(|p| crate::config::canon_identity(p))
                .collect(),
            ..Default::default()
        }
    }

    /// One well-formed envelope from `PEER_A` to `PEER_B`, sender claim omitted
    /// (the shape a client that lets the relay stamp provenance sends).
    fn test_envelope() -> MpcEnvelope {
        MpcEnvelope {
            recipient: PEER_B.into(),
            r#box: "mpc_somesession".into(),
            body: "BRC78ciphertext".into(),
            sender: String::new(),
            message_id: "mpc-1".into(),
        }
    }

    /// The relayed frame carries `box`, NOT `messageBox`, and every field
    /// survives a round trip through the wire shape the ceremony client parses.
    /// A regression here is a total, silent ceremony outage: the client's
    /// `from_value::<EnvelopeEvent>` would fail on every round.
    #[test]
    fn relayed_envelope_uses_the_box_key() {
        let (_room, envelope) = admit_mpc_envelope(
            test_envelope(),
            Some(PEER_A),
            Some("02serveridentity"),
            &relay_for(&[PEER_A]),
        )
        .expect("admitted");

        let v = serde_json::to_value(&envelope).expect("serialize");
        assert!(v.get("box").is_some(), "the destination box key is `box`");
        assert!(
            v.get("messageBox").is_none(),
            "`messageBox` belongs to the mailbox lane, not this one"
        );
        assert_eq!(v["box"], "mpc_somesession");
        assert_eq!(v["recipient"], PEER_B);
        assert_eq!(v["sender"], PEER_A);
        assert_eq!(v["messageId"], "mpc-1");
        assert_eq!(v["body"], "BRC78ciphertext");

        let back: MpcEnvelope = serde_json::from_value(v).expect("round-trips");
        assert_eq!(back.r#box, "mpc_somesession");
        assert_eq!(back.message_id, "mpc-1");
    }

    /// Routing addresses `{recipient}-{box}` — the box the envelope names, not
    /// the recipient's presence box. The per-ceremony room is what the
    /// auto-join then populates.
    #[test]
    fn envelope_routes_to_the_recipient_box_room() {
        let (room, _envelope) =
            admit_mpc_envelope(test_envelope(), Some(PEER_A), None, &relay_for(&[PEER_A]))
                .expect("admitted");
        assert_eq!(room, format!("{PEER_B}-mpc_somesession"));
        assert_ne!(room, room_id(PEER_B, MPC_INBOX));
    }

    /// An omitted body `sender` is filled in from the verified socket identity,
    /// so the recipient always has provenance to check its decrypt against.
    #[test]
    fn empty_body_sender_is_stamped_with_the_verified_identity() {
        let (_room, envelope) =
            admit_mpc_envelope(test_envelope(), Some(PEER_A), None, &relay_for(&[PEER_A]))
                .expect("admitted");
        assert_eq!(envelope.sender, PEER_A);
    }

    /// A body `sender` that contradicts the verified socket identity is forged
    /// provenance: refused outright rather than quietly corrected.
    #[test]
    fn forged_body_sender_is_refused() {
        let forged = MpcEnvelope {
            sender: PEER_B.into(),
            ..test_envelope()
        };
        assert!(
            admit_mpc_envelope(forged, Some(PEER_A), None, &relay_for(&[PEER_A, PEER_B])).is_err()
        );
    }

    /// The size check runs before anything else, so an oversize body costs the
    /// relay no room lookup and no signing fan-out.
    #[test]
    fn oversize_body_is_refused_before_forwarding() {
        let relay = crate::config::MpcRelayConfig {
            max_body_bytes: 16,
            ..relay_for(&[PEER_A])
        };
        let oversize = MpcEnvelope {
            body: "x".repeat(17),
            ..test_envelope()
        };
        assert!(admit_mpc_envelope(oversize, Some(PEER_A), None, &relay).is_err());

        // Exactly at the ceiling still routes — the cap is a maximum, not a
        // strict bound, and DKG rounds sit near it.
        let at_limit = MpcEnvelope {
            body: "x".repeat(16),
            ..test_envelope()
        };
        assert!(admit_mpc_envelope(at_limit, Some(PEER_A), None, &relay).is_ok());
    }

    /// The relay is never a ceremony party: an envelope addressed to its own
    /// server identity is refused. It holds no share and could not open the
    /// body in any case.
    #[test]
    fn self_addressed_envelope_is_refused() {
        let to_relay = MpcEnvelope {
            recipient: "02serveridentity".into(),
            ..test_envelope()
        };
        assert!(admit_mpc_envelope(
            to_relay,
            Some(PEER_A),
            Some("02serveridentity"),
            &relay_for(&[PEER_A])
        )
        .is_err());
    }

    /// Admission is rooted in the VERIFIED identity: a socket that authenticated
    /// as someone who is not an allowlisted ceremony peer routes nothing, even
    /// with a well-formed envelope.
    #[test]
    fn unallowlisted_sender_is_refused() {
        assert!(
            admit_mpc_envelope(test_envelope(), Some(PEER_B), None, &relay_for(&[PEER_A])).is_err()
        );
    }

    /// A socket with no completed handshake has no identity to stamp, so there
    /// is no envelope to relay.
    #[test]
    fn envelope_without_a_verified_identity_is_refused() {
        assert!(admit_mpc_envelope(test_envelope(), None, None, &relay_for(&[PEER_A])).is_err());
    }

    /// The default configuration — no `MPC_PEER_IDENTITIES` — refuses every
    /// envelope. The lane is opt-in; an unconfigured server is not an open MPC
    /// relay.
    #[test]
    fn unconfigured_lane_refuses_everything() {
        let relay = crate::config::MpcRelayConfig::default();
        assert!(!relay.is_enabled());
        assert!(admit_mpc_envelope(test_envelope(), Some(PEER_A), None, &relay).is_err());
    }

    /// The allowlist matches on the canonical form, so an operator who pastes
    /// upper-case hex (or leaves whitespace) still admits the same peer.
    #[test]
    fn allowlist_matching_is_canonical() {
        let relay = relay_for(&[&format!("  {} ", PEER_A.to_uppercase())]);
        assert!(admit_mpc_envelope(test_envelope(), Some(PEER_A), None, &relay).is_ok());
    }

    /// The auto-join preserves authsocket's own-room-only invariant: a socket
    /// sitting in a presence room without a verified identity — or with someone
    /// else's — is never joined into the ceremony room. (The positive path needs
    /// a real handshake to establish an identity, and is covered in
    /// `tests/mpc_envelope_e2e.rs`.)
    #[tokio::test]
    async fn auto_join_skips_sockets_without_a_matching_verified_identity() {
        let ws = test_ws();
        let wallet = SdkProtoWallet::new(PrivateKey::from_hex(TEST_SERVER_KEY).expect("test key"));
        ws.core.add_connection("sock1", wallet);
        ws.core.join_room("sock1", room_id(PEER_B, MPC_INBOX));

        let room = room_id(PEER_B, "mpc_somesession");
        join_recipient_presence_sockets(&ws.core, PEER_B, &room);
        assert!(
            ws.core.room_members(&room).is_empty(),
            "an unauthenticated presence socket must not be auto-joined"
        );
    }

    /// A ceremony room that already has members is left alone: re-harvesting the
    /// presence room on every round would re-add sockets that deliberately left.
    #[tokio::test]
    async fn auto_join_is_skipped_once_the_room_has_members() {
        let ws = test_ws();
        let room = room_id(PEER_B, "mpc_somesession");
        ws.core.join_room("already-here", &room);
        ws.core
            .join_room("presence-sock", room_id(PEER_B, MPC_INBOX));

        join_recipient_presence_sockets(&ws.core, PEER_B, &room);
        assert_eq!(
            ws.core.room_members(&room),
            vec!["already-here".to_string()]
        );
    }

    // -- Wallet-RPC relay lane ----------------------------------------------

    /// A non-roster app identity: allowlisted for the wallet verbs and for
    /// nothing else. `PEER_A`/`PEER_B` keep their ceremony-peer roles, so the
    /// union rule and the isolation rule can be tested against the same pair.
    const APP: &str = "02ccddeeff00112233445566778899aabbccddeeff00112233445566778899aabb";

    /// The wallet lane switched on for `apps`, at the reused frame ceiling.
    fn wallet_for(apps: &[&str]) -> crate::config::WalletRelayConfig {
        crate::config::WalletRelayConfig {
            app_identities: apps
                .iter()
                .map(|a| crate::config::canon_identity(a))
                .collect(),
            ..Default::default()
        }
    }

    /// One well-formed frame addressed to `PEER_B`, sender claim omitted (the
    /// shape a client that lets the relay stamp provenance sends).
    fn test_wallet_frame() -> WalletFrame {
        WalletFrame {
            recipient: PEER_B.into(),
            sender: String::new(),
            correlation_id: "corr-1".into(),
            frame: "{\"nested\":\"BRC-103 AuthMessage\"}".into(),
        }
    }

    /// The relayed frame carries `correlationId` (camelCase) and `frame`, and
    /// every field survives a round trip through the wire shape the app and the
    /// box parse. A regression here is a total, silent wallet-lane outage: every
    /// call would fail its `from_value::<WalletFrame>` and simply time out.
    #[test]
    fn relayed_wallet_frame_uses_the_camel_case_correlation_key() {
        let (_room, frame) = admit_wallet_frame(
            test_wallet_frame(),
            Some(APP),
            Some("02serveridentity"),
            &relay_for(&[PEER_A]),
            &wallet_for(&[APP]),
        )
        .expect("admitted");

        let v = serde_json::to_value(&frame).expect("serialize");
        assert!(
            v.get("correlationId").is_some(),
            "the correlation id key is `correlationId`"
        );
        assert!(
            v.get("correlation_id").is_none(),
            "snake_case would break both ends of the nested session"
        );
        assert!(
            v.get("messageBox").is_none() && v.get("box").is_none(),
            "neither the mailbox lane's nor the ceremony lane's keys belong here"
        );
        assert_eq!(v["correlationId"], "corr-1");
        assert_eq!(v["recipient"], PEER_B);
        assert_eq!(v["sender"], APP);
        assert_eq!(v["frame"], "{\"nested\":\"BRC-103 AuthMessage\"}");

        let back: WalletFrame = serde_json::from_value(v).expect("round-trips");
        assert_eq!(back.correlation_id, "corr-1");
    }

    /// Routing addresses the ONE presence room this lane has,
    /// `{recipient}-wallet_inbox` — never the ceremony lane's `mpc_inbox`, which
    /// is the room separation that keeps a busy dashboard from starving a presig
    /// deadline.
    #[test]
    fn wallet_frame_routes_to_the_recipient_wallet_inbox() {
        let (room, _frame) = admit_wallet_frame(
            test_wallet_frame(),
            Some(APP),
            None,
            &relay_for(&[]),
            &wallet_for(&[APP]),
        )
        .expect("admitted");
        assert_eq!(room, format!("{PEER_B}-{WALLET_INBOX}"));
        assert_ne!(room, room_id(PEER_B, MPC_INBOX));
    }

    /// The union rule, leg 1: an app on the WALLET allowlist is admitted here
    /// with no ceremony peer configured at all.
    #[test]
    fn an_allowlisted_app_is_admitted_on_the_wallet_lane() {
        assert!(admit_wallet_frame(
            test_wallet_frame(),
            Some(APP),
            None,
            &relay_for(&[]),
            &wallet_for(&[APP]),
        )
        .is_ok());
    }

    /// The union rule, leg 2: a CEREMONY peer is admitted on the wallet lane
    /// with no app allowlisted at all. This is not a convenience — a box or
    /// cosigner sends its `walletReply` under its roster identity and never
    /// holds an app one, so without this leg every reply would be dropped and
    /// the lane would carry calls in one direction only.
    #[test]
    fn a_ceremony_peer_is_admitted_on_the_wallet_lane() {
        assert!(admit_wallet_frame(
            test_wallet_frame(),
            Some(PEER_A),
            None,
            &relay_for(&[PEER_A]),
            &wallet_for(&[]),
        )
        .is_ok());
    }

    /// THE isolation property, and the reason the app list lives in its own
    /// struct: an identity admitted for the wallet verbs routes NO ceremony
    /// traffic. [`admit_mpc_envelope`] reads the peer list alone, so the union
    /// runs one way only.
    #[test]
    fn a_wallet_app_can_never_route_ceremony_traffic() {
        // Same server configuration, both lanes, one identity.
        let mpc = relay_for(&[PEER_A]);
        let wallet = wallet_for(&[APP]);

        assert!(
            admit_wallet_frame(test_wallet_frame(), Some(APP), None, &mpc, &wallet).is_ok(),
            "the app is admitted on the lane it was allowlisted for"
        );
        let ceremony = MpcEnvelope {
            recipient: PEER_B.into(),
            r#box: "mpc_somesession".into(),
            body: "BRC78ciphertext".into(),
            sender: String::new(),
            message_id: "mpc-1".into(),
        };
        assert!(
            admit_mpc_envelope(ceremony, Some(APP), None, &mpc).is_err(),
            "and refused on the ceremony lane by the same configuration"
        );
    }

    /// A body `sender` that contradicts the verified socket identity is forged
    /// provenance: refused outright rather than quietly corrected.
    #[test]
    fn forged_wallet_frame_sender_is_refused() {
        let forged = WalletFrame {
            sender: PEER_A.into(),
            ..test_wallet_frame()
        };
        assert!(admit_wallet_frame(
            forged,
            Some(APP),
            None,
            &relay_for(&[PEER_A]),
            &wallet_for(&[APP]),
        )
        .is_err());
    }

    /// An omitted body `sender` is filled in from the verified socket identity,
    /// so the box opens the nested session against provenance the relay proved.
    #[test]
    fn empty_wallet_frame_sender_is_stamped_with_the_verified_identity() {
        let (_room, frame) = admit_wallet_frame(
            test_wallet_frame(),
            Some(APP),
            None,
            &relay_for(&[]),
            &wallet_for(&[APP]),
        )
        .expect("admitted");
        assert_eq!(frame.sender, APP);
    }

    /// The size check runs before anything else, so an oversize frame costs the
    /// relay no room lookup and no signing fan-out.
    #[test]
    fn oversize_wallet_frame_is_refused_before_forwarding() {
        let wallet = crate::config::WalletRelayConfig {
            max_frame_bytes: 16,
            ..wallet_for(&[APP])
        };
        let oversize = WalletFrame {
            frame: "x".repeat(17),
            ..test_wallet_frame()
        };
        assert!(admit_wallet_frame(oversize, Some(APP), None, &relay_for(&[]), &wallet).is_err());

        // Exactly at the ceiling still routes — the cap is a maximum, not a
        // strict bound.
        let at_limit = WalletFrame {
            frame: "x".repeat(16),
            ..test_wallet_frame()
        };
        assert!(admit_wallet_frame(at_limit, Some(APP), None, &relay_for(&[]), &wallet).is_ok());
    }

    /// The relay terminates no wallet session: a frame addressed to its own
    /// server identity is refused. It holds neither party's key and could not
    /// open the nested frame in any case.
    #[test]
    fn self_addressed_wallet_frame_is_refused() {
        let to_relay = WalletFrame {
            recipient: "02serveridentity".into(),
            ..test_wallet_frame()
        };
        assert!(admit_wallet_frame(
            to_relay,
            Some(APP),
            Some("02serveridentity"),
            &relay_for(&[]),
            &wallet_for(&[APP]),
        )
        .is_err());
    }

    /// Admission is rooted in the VERIFIED identity: a socket that authenticated
    /// as someone on neither list routes nothing, however well-formed the frame.
    #[test]
    fn unallowlisted_wallet_sender_is_refused() {
        assert!(admit_wallet_frame(
            test_wallet_frame(),
            Some(PEER_B),
            None,
            &relay_for(&[PEER_A]),
            &wallet_for(&[APP]),
        )
        .is_err());
    }

    /// A socket with no completed handshake has no identity to stamp, so there
    /// is no frame to relay.
    #[test]
    fn wallet_frame_without_a_verified_identity_is_refused() {
        assert!(admit_wallet_frame(
            test_wallet_frame(),
            None,
            None,
            &relay_for(&[]),
            &wallet_for(&[])
        )
        .is_err());
    }

    /// Both populations unset refuses every frame. A deployment that opts into
    /// neither is not an open front door onto the boxes dialed into it.
    #[test]
    fn unconfigured_wallet_lane_refuses_everything() {
        let mpc = crate::config::MpcRelayConfig::default();
        let wallet = crate::config::WalletRelayConfig::default();
        assert!(!wallet.is_enabled(&mpc));
        assert!(admit_wallet_frame(test_wallet_frame(), Some(APP), None, &mpc, &wallet).is_err());
    }

    /// The allowlist matches on the canonical form, so an operator who pastes
    /// upper-case hex (or leaves whitespace) still admits the same app.
    #[test]
    fn wallet_allowlist_matching_is_canonical() {
        let wallet = wallet_for(&[&format!("  {} ", APP.to_uppercase())]);
        assert!(admit_wallet_frame(
            test_wallet_frame(),
            Some(APP),
            None,
            &relay_for(&[]),
            &wallet
        )
        .is_ok());
    }
}
