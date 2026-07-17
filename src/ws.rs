//! WebSocket (Socket.IO) support for the MessageBox server, over the shared
//! [`authsocket`] crate.
//!
//! The BRC-103 protocol plumbing that used to live here — per-socket Peer
//! sessions (`ChannelTransport`/`SocketPeer`), the verified-sender identity
//! registry, room membership, the signed emit/broadcast primitives, and the
//! generic room verbs (`authenticated`/`joinRoom`/`leaveRoom`, including the
//! "a client may only join its own room" check) — moved into the `authsocket`
//! crate (`AuthSocketServer` core + `server_io` socketioxide adapter). This
//! module keeps only the MessageBox **application** logic:
//!
//! - [`RoomMessage`] — the room-delivery payload shape.
//! - the `sendMessage` app verb ([`handle_ws_send_message`]): push-live-first
//!   signed broadcast, async persistence, signed room-scoped ack.
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
use socketioxide::extract::SocketRef;
use socketioxide::SocketIo;
use tracing::{debug, error, info, warn};

use authsocket::server::{AuthSocketServer, SharedAuthSocketServer};
use authsocket::server_io::{attach, emit_signed_to_room, emit_signed_to_socket, AppDispatcher};
use authsocket::VerifiedEvent;
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
    /// Background, durable message persistence. Both send paths push live first
    /// and then hand the INSERT to this worker so MySQL latency never blocks
    /// live delivery. See [`crate::persist`].
    persist: crate::persist::PersistHandle,
    /// Model B backplane (`REDIS_URL` set) — `None` means Model A: single
    /// instance, in-process routing only. See [`crate::backplane`].
    backplane: Option<Arc<crate::backplane::Backplane>>,
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
    ) -> Self {
        let persist = crate::persist::PersistHandle::spawn(
            db.clone(),
            crate::persist::PersistConfig::default(),
        );
        let ws = Self {
            io,
            core: Arc::new(AuthSocketServer::new()),
            server_private_key_hex,
            persist,
            backplane,
        };
        if let Some(bp) = &ws.backplane {
            match bp.take_delivery_rx() {
                Some(rx) => {
                    tokio::spawn(backplane_delivery_task(ws.clone(), bp.clone(), rx));
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
        self.deliver_local(room_id, event, &data).await
    }

    /// The one signed local-delivery path: sign for every authenticated local
    /// member of `room_id` (this instance's Peer sessions — the per-instance
    /// room registry) and emit over their sockets. Used by both the direct
    /// broadcast path and the Model B backplane delivery task; keeping them
    /// on one function is what keeps Model A and Model B from diverging.
    async fn deliver_local(&self, room_id: &str, event: &str, data: &serde_json::Value) -> usize {
        emit_signed_to_room(&self.io, &self.core, room_id, event, data).await
    }
}

/// Model B: drain the backplane subscription and deliver every
/// **remote-origin** envelope to local room members via the same signed
/// local-delivery path the direct broadcast uses. Own-origin envelopes are
/// skipped — the publishing instance already ran its local leg at publish
/// time (skipping prevents double delivery, not a correctness gate: the
/// client also dedupes on messageId).
///
/// Only this instance holds the authsocket `Peer` sessions for its sockets,
/// so only it can sign for them — the envelope arrives UNSIGNED and signing
/// happens here, on the connection owner.
async fn backplane_delivery_task(
    ws: WsBroadcast,
    bp: Arc<crate::backplane::Backplane>,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<String>,
) {
    while let Some(raw) = rx.recv().await {
        let envelope: crate::backplane::BackplaneEnvelope = match serde_json::from_str(&raw) {
            Ok(e) => e,
            Err(e) => {
                warn!(error = %e, "backplane: undecodable envelope — skipped");
                continue;
            }
        };
        if envelope.origin == bp.instance_id() {
            continue;
        }
        let data = match serde_json::to_value(&envelope.message) {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "backplane: envelope message failed to re-serialize — skipped");
                continue;
            }
        };
        let delivered = ws
            .deliver_local(&envelope.room_id, &envelope.event, &data)
            .await;
        debug!(
            origin = %envelope.origin,
            room = %envelope.room_id,
            delivered,
            "backplane: remote-origin message delivered to local members"
        );
    }
    debug!("backplane delivery task ended (subscription stream closed)");
}

// ---------------------------------------------------------------------------
// Socket.IO event handlers
// ---------------------------------------------------------------------------

/// MessageBox application dispatcher: receives verified events the authsocket
/// adapter does not handle itself (everything except the generic room verbs).
struct MbsDispatcher {
    ws: WsBroadcast,
}

#[async_trait::async_trait]
impl AppDispatcher<SdkProtoWallet> for MbsDispatcher {
    async fn dispatch(
        &self,
        _io: &SocketIo,
        _server: &AuthSocketServer<SdkProtoWallet>,
        socket: &SocketRef,
        event: VerifiedEvent,
    ) {
        let sid = socket.id.to_string();
        match event.event_name.as_str() {
            "sendMessage" => {
                handle_ws_send_message(socket, &self.ws, &sid, &event.sender, event.data).await;
            }
            other => {
                debug!(sid = %sid, event = %other, "BRC-103 general message: unhandled event");
            }
        }
    }
}

/// Set up Socket.IO event handlers for the default namespace.
///
/// Wires the authsocket adapter: connect → per-socket BRC-103 session
/// (`ProtoWallet` over the server key), `authMessage` → verify + generic room
/// verbs in the crate, app events → [`MbsDispatcher`], disconnect → teardown.
pub fn setup_handlers(io: &SocketIo, ws_broadcast: WsBroadcast) {
    let key_hex = ws_broadcast.server_private_key_hex.clone();
    let core = ws_broadcast.core.clone();
    attach(
        io,
        core,
        move || {
            let pk = bsv::primitives::private_key::PrivateKey::from_hex(&key_hex)
                .map_err(|e| format!("parse server key: {e}"))?;
            Ok(SdkProtoWallet::new(pk))
        },
        Arc::new(MbsDispatcher { ws: ws_broadcast }),
    );
    info!("authsocket handlers attached (BRC-103 over Socket.IO)");
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
/// and emits a signed `sendMessageAck-{roomId}` back to the sender.
async fn handle_ws_send_message(
    socket: &SocketRef,
    ws: &WsBroadcast,
    sid: &str,
    sender: &str,
    data: serde_json::Value,
) {
    // Parse the sendMessage payload
    let room_id_str = match data.get("roomId").and_then(|v| v.as_str()) {
        Some(r) if !r.is_empty() => r.to_string(),
        _ => {
            warn!(sid = %sid, "BRC-103 sendMessage: missing roomId");
            return;
        }
    };

    let message = match data.get("message") {
        Some(m) if !m.is_null() => m,
        _ => {
            warn!(sid = %sid, "BRC-103 sendMessage: missing message object");
            return;
        }
    };

    let message_id = match message.get("messageId").and_then(|v| v.as_str()) {
        Some(id) if !id.is_empty() => id.to_string(),
        _ => {
            warn!(sid = %sid, "BRC-103 sendMessage: missing messageId");
            return;
        }
    };

    let recipient = match message.get("recipient").and_then(|v| v.as_str()) {
        Some(r) if !r.is_empty() => r.to_string(),
        _ => {
            warn!(sid = %sid, "BRC-103 sendMessage: missing recipient");
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
            return;
        }
    };

    if body.len() > MAX_MESSAGE_BODY_BYTES {
        warn!(sid = %sid, size = body.len(), "BRC-103 sendMessage: body exceeds size limit");
        return;
    }

    if sender.is_empty() {
        warn!(sid = %sid, "BRC-103 sendMessage: no verified sender identity");
        return;
    }

    // Extract messageBox from roomId
    let message_box = match authsocket::split_room_id(&room_id_str) {
        Some((_key, mb)) => mb,
        None => {
            warn!(sid = %sid, room = %room_id_str, "BRC-103 sendMessage: invalid roomId format");
            return;
        }
    };

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
        WsBroadcast::new(io, TEST_SERVER_KEY.to_string(), pool, None)
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
}
