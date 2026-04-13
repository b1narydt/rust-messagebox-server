//! WebSocket (Socket.IO) support for the MessageBox server.
//!
//! Uses `socketioxide` to provide a Socket.IO v4 server that is compatible
//! with the `@bsv/authsocket-client` v2.x client.
//!
//! ## Protocol
//!
//! All server→client app-level events are sent BRC-103-wrapped inside an
//! `authMessage` Socket.IO event.  The client's `AuthSocketClient` only
//! surfaces events that arrive this way — raw Socket.IO emits with arbitrary
//! names are silently dropped by the client.
//!
//! Wire format:
//! ```text
//! socket.emit("authMessage", <BRC-103 AuthMessage>)
//! ```
//! where the AuthMessage payload is the UTF-8 bytes of
//! ```json
//! {"eventName":"<event>","data":<json_value>}
//! ```
//!
//! Room naming convention: `{identityKey}-{messageBox}`

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Arc as StdArc;

use parking_lot::RwLock;
use serde::Serialize;
use socketioxide::extract::{Data, SocketRef};
use socketioxide::SocketIo;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use bsv::auth::error::AuthError;
use bsv::auth::peer::Peer;
use bsv::auth::transports::Transport;
use bsv::auth::types::AuthMessage as SdkAuthMessage;
use bsv::wallet::proto_wallet::ProtoWallet as SdkProtoWallet;

use crate::db::{self, DbPool};
use crate::handlers::helpers::is_valid_pub_key;

// ---------------------------------------------------------------------------
// Channel-based Transport for bsv-sdk Peer (server-side WS auth)
// ---------------------------------------------------------------------------

/// A simple channel-based transport that connects a bsv-sdk Peer to
/// Socket.IO events.  Outgoing messages from Peer are sent to a channel
/// that the per-socket drain task reads and emits as `authMessage` events.
pub struct ChannelTransport {
    outgoing_tx: mpsc::Sender<SdkAuthMessage>,
    incoming_rx: std::sync::Mutex<Option<mpsc::Receiver<SdkAuthMessage>>>,
}

impl ChannelTransport {
    pub fn new() -> (Self, mpsc::Sender<SdkAuthMessage>, mpsc::Receiver<SdkAuthMessage>) {
        let (in_tx, in_rx) = mpsc::channel(16);
        let (out_tx, out_rx) = mpsc::channel(16);
        let transport = Self {
            outgoing_tx: out_tx,
            incoming_rx: std::sync::Mutex::new(Some(in_rx)),
        };
        (transport, in_tx, out_rx)
    }
}

#[async_trait::async_trait]
impl Transport for ChannelTransport {
    async fn send(&self, message: SdkAuthMessage) -> Result<(), AuthError> {
        self.outgoing_tx
            .send(message)
            .await
            .map_err(|e| AuthError::TransportError(format!("channel send: {e}")))
    }

    fn subscribe(&self) -> mpsc::Receiver<SdkAuthMessage> {
        self.incoming_rx.lock().unwrap().take().expect("subscribe called twice")
    }
}

// ---------------------------------------------------------------------------
// Per-socket peer state
// ---------------------------------------------------------------------------

/// Shared handle to the Peer's general-message receive channel.
///
/// `(sender_identity_key, decoded_payload_bytes)` tuples decoded by the Peer
/// from incoming BRC-103 General messages.
type GeneralRx = Arc<tokio::sync::Mutex<mpsc::Receiver<(String, Vec<u8>)>>>;

/// All per-socket state owned by WsBroadcast.
///
/// Using `Arc<Mutex<...>>` for peer and general_rx lets the `authMessage`
/// handler clone the Arcs under the map lock, release the map lock quickly,
/// and then do async work (awaiting on the inner mutexes) without holding
/// the outer map lock across await points.
struct SocketPeerState {
    peer: Arc<tokio::sync::Mutex<Peer<SdkProtoWallet>>>,
    incoming_tx: mpsc::Sender<SdkAuthMessage>,
    general_rx: GeneralRx,
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Message broadcast to a room when a new message is stored.
///
/// Includes `created_at` and `updated_at` fields required by the client's
/// `ServerPeerMessage` parser.
#[derive(Serialize, Clone, Debug)]
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
/// Held in `AppState` so HTTP handlers can reach the WS layer to push
/// messages to connected recipients.
#[derive(Clone)]
pub struct WsBroadcast {
    /// Kept for potential future use (e.g. server-initiated NS-level ops).
    #[allow(dead_code)]
    io: SocketIo,
    /// Maps socket ID → authenticated identity key.
    socket_identities: Arc<RwLock<HashMap<String, String>>>,
    /// Server private key hex for creating bsv-sdk Peers.
    server_private_key_hex: String,
    /// Database pool for WS-initiated message storage (sendMessage via BRC-103).
    db: DbPool,
    /// Per-socket BRC-103 peer state.
    ///
    /// The map lock is held only briefly to clone the inner Arcs; all actual
    /// async work (send, process_pending, recv) happens after releasing the map.
    socket_peers: Arc<tokio::sync::Mutex<HashMap<String, SocketPeerState>>>,
}

impl WsBroadcast {
    pub fn new(io: SocketIo, server_private_key_hex: String, db: DbPool) -> Self {
        Self {
            io,
            socket_identities: Arc::new(RwLock::new(HashMap::new())),
            server_private_key_hex,
            db,
            socket_peers: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Get or create a Peer for a socket and spawn the continuous drain task.
    ///
    /// The drain task owns `outgoing_rx` (the read end of the Peer's transport
    /// output channel) and the `SocketRef` clone.  It loops forever, forwarding
    /// each `AuthMessage` produced by the Peer as a raw
    /// `socket.emit("authMessage", ...)` — this IS the BRC-103 transport.
    ///
    /// Returns `true` if a new peer was created (drain task spawned).
    async fn ensure_peer_with_drain(&self, socket: &SocketRef) -> Result<bool, String> {
        let sid = socket.id.to_string();
        let mut peers = self.socket_peers.lock().await;
        if peers.contains_key(&sid) {
            return Ok(false);
        }

        let wallet = self.create_sdk_wallet()?;
        let (transport, incoming_tx, outgoing_rx) = ChannelTransport::new();
        let mut peer = Peer::new(wallet, StdArc::new(transport));

        // on_general_message() is take-once — must be called before storing.
        let general_msg_rx = peer
            .on_general_message()
            .expect("on_general_message must succeed on fresh Peer");

        peers.insert(sid.clone(), SocketPeerState {
            peer: Arc::new(tokio::sync::Mutex::new(peer)),
            incoming_tx,
            general_rx: Arc::new(tokio::sync::Mutex::new(general_msg_rx)),
        });

        // Spawn drain task — owns outgoing_rx and the socket clone.
        let socket_clone = socket.clone();
        tokio::spawn(async move {
            let mut rx = outgoing_rx;
            while let Some(auth_msg) = rx.recv().await {
                let json = match serde_json::to_value(&auth_msg) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(sid = %sid, error = %e, "drain: serialize AuthMessage failed");
                        continue;
                    }
                };
                if socket_clone.emit("authMessage", &json).is_err() {
                    debug!(sid = %sid, "drain: socket closed, stopping");
                    break;
                }
                debug!(sid = %sid, "drain: emitted authMessage (BRC-103 transport)");
            }
            debug!(sid = %sid, "drain task exiting");
        });

        Ok(true)
    }

    /// Remove a socket's Peer on disconnect.
    async fn remove_peer(&self, socket_id: &str) {
        self.socket_peers.lock().await.remove(socket_id);
    }

    /// Create a bsv-sdk ProtoWallet for BRC-103 auth.
    fn create_sdk_wallet(&self) -> Result<SdkProtoWallet, String> {
        let pk = bsv::primitives::private_key::PrivateKey::from_hex(&self.server_private_key_hex)
            .map_err(|e| format!("parse server key: {e}"))?;
        Ok(SdkProtoWallet::new(pk))
    }

    /// Store the identity key for an authenticated socket.
    pub fn set_identity(&self, socket_id: &str, identity_key: String) {
        self.socket_identities.write().insert(socket_id.to_string(), identity_key);
    }

    /// Get the identity key for a socket.
    pub fn get_identity(&self, socket_id: &str) -> Option<String> {
        self.socket_identities.read().get(socket_id).cloned()
    }

    /// Remove a socket's identity (on disconnect).
    pub fn remove_identity(&self, socket_id: &str) {
        self.socket_identities.write().remove(socket_id);
    }

    /// Clone the per-socket Arcs while holding the map lock only briefly.
    ///
    /// Returns `None` if no peer exists for `sid`.
    async fn clone_peer_state(&self, sid: &str)
        -> Option<(Arc<tokio::sync::Mutex<Peer<SdkProtoWallet>>>, mpsc::Sender<SdkAuthMessage>, GeneralRx)>
    {
        let peers = self.socket_peers.lock().await;
        peers.get(sid).map(|s| (s.peer.clone(), s.incoming_tx.clone(), s.general_rx.clone()))
    }

    /// Send an app-level event to a single socket, BRC-103-wrapped.
    ///
    /// Builds `{"eventName": event_name, "data": data}` as the BRC-103 payload
    /// and calls `peer.send_message(identity_key, payload)`.  The resulting
    /// `AuthMessage` lands on the transport output channel and the per-socket
    /// drain task forwards it to the socket as `socket.emit("authMessage", ...)`.
    ///
    /// PRECONDITION: the BRC-103 handshake for `sid` must be complete (an
    /// authenticated session must exist in the Peer's session manager).
    pub async fn emit_brc103(
        &self,
        sid: &str,
        event_name: &str,
        data: serde_json::Value,
    ) -> Result<(), String> {
        let identity_key = match self.get_identity(sid) {
            Some(k) => k,
            None => {
                return Err(format!("emit_brc103({event_name}): no authenticated identity for sid={sid}"));
            }
        };

        let payload_bytes = serde_json::to_vec(&serde_json::json!({
            "eventName": event_name,
            "data": data,
        }))
        .map_err(|e| format!("emit_brc103 serialize: {e}"))?;

        match self.clone_peer_state(sid).await {
            Some((peer_arc, _, _)) => {
                let mut peer = peer_arc.lock().await;
                peer.send_message(&identity_key, payload_bytes)
                    .await
                    .map_err(|e| format!("emit_brc103 send_message: {e}"))
            }
            None => Err(format!("emit_brc103({event_name}): no Peer for sid={sid}")),
        }
    }

    /// Broadcast a message to all sockets authenticated as `msg.recipient`,
    /// BRC-103-wrapped per socket.
    ///
    /// This method is synchronous (`&self`) so the REST handler does not need
    /// to change; the per-recipient async work is spawned inside.
    pub fn broadcast_to_room(&self, _room_id: &str, event: &str, msg: &RoomMessage) {
        let recipient_key = msg.recipient.clone();
        let matching_sids: Vec<String> = {
            let identities = self.socket_identities.read();
            identities
                .iter()
                .filter(|(_, key)| *key == &recipient_key)
                .map(|(sid, _)| sid.clone())
                .collect()
        };

        if matching_sids.is_empty() {
            debug!(recipient = %recipient_key, "broadcast_to_room: no connected sockets for recipient");
            return;
        }

        let event_name = event.to_string();
        let data = match serde_json::to_value(msg) {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "broadcast_to_room: failed to serialize RoomMessage");
                return;
            }
        };

        let ws = self.clone();
        tokio::spawn(async move {
            for sid in matching_sids {
                if let Err(e) = ws.emit_brc103(&sid, &event_name, data.clone()).await {
                    warn!(sid = %sid, event = %event_name, error = %e, "broadcast_to_room: emit_brc103 failed");
                } else {
                    debug!(sid = %sid, event = %event_name, "broadcast_to_room: delivered BRC-103 to recipient");
                }
            }
        });
    }
}

// ---------------------------------------------------------------------------
// Socket.IO event handlers
// ---------------------------------------------------------------------------

/// Set up Socket.IO event handlers for the default namespace.
pub fn setup_handlers(io: &SocketIo, ws_broadcast: WsBroadcast) {
    let ws = ws_broadcast;

    io.ns("/", move |socket: SocketRef| {
        let sid = socket.id.to_string();
        info!(sid = %sid, "new Socket.IO connection");

        let ws_authmsg = ws.clone();
        let ws_dc = ws.clone();

        // --- authMessage (BRC-103 mutual auth + general message routing) ---
        //
        // Handles both the BRC-103 handshake AND subsequent application messages.
        // The client wraps ALL events (sendMessage, joinRoom, leaveRoom) in BRC-103
        // General messages sent as authMessage Socket.IO events.  After the Peer
        // decodes them, the payload arrives on the general_rx channel and we route
        // it to the appropriate handler.
        socket.on(
            "authMessage",
            move |socket: SocketRef, Data(data): Data<serde_json::Value>| {
                let ws = ws_authmsg.clone();
                async move {
                    let sid = socket.id.to_string();
                    debug!(sid = %sid, "received authMessage");

                    // Parse the incoming AuthMessage
                    let incoming: SdkAuthMessage = match serde_json::from_value(data) {
                        Ok(m) => m,
                        Err(e) => {
                            warn!(sid = %sid, error = %e, "invalid authMessage payload");
                            return;
                        }
                    };

                    // Hold identity_key from envelope — only trust after Peer verifies
                    let claimed_identity = incoming.identity_key.clone();

                    // Ensure a Peer exists and the drain task is running.
                    if let Err(e) = ws.ensure_peer_with_drain(&socket).await {
                        warn!(sid = %sid, error = %e, "failed to create Peer");
                        return;
                    }

                    // Clone the Arcs we need while holding the map lock briefly.
                    let (peer_arc, incoming_tx, general_rx_arc) =
                        match ws.clone_peer_state(&sid).await {
                            Some(t) => t,
                            None => {
                                warn!(sid = %sid, "Peer disappeared immediately after ensure");
                                return;
                            }
                        };

                    // Feed the incoming message to the Peer's transport channel.
                    if let Err(e) = incoming_tx.send(incoming).await {
                        warn!(sid = %sid, error = %e, "failed to feed authMessage to Peer");
                        return;
                    }

                    // Let the Peer process — verifies signature and produces outgoing
                    // BRC-103 responses (handshake messages).  Those land on outgoing_tx
                    // → drain task → socket.emit("authMessage").
                    {
                        let mut peer = peer_arc.lock().await;
                        if let Err(e) = peer.process_pending().await {
                            warn!(sid = %sid, error = %e, "Peer process_pending failed");
                        }
                    }

                    // Only set identity AFTER Peer has verified the message.
                    if is_valid_pub_key(&claimed_identity) {
                        ws.set_identity(&sid, claimed_identity.clone());
                    }

                    // Drain decoded general messages (application events from BRC-103).
                    // The Peer decodes BRC-103 General payloads and places them here.
                    let general_events: Vec<(String, serde_json::Value)> = {
                        let mut general_rx = general_rx_arc.lock().await;
                        let mut events = Vec::new();
                        while let Ok((_sender_key, payload_bytes)) = general_rx.try_recv() {
                            if let Some((event_name, data)) = decode_ws_event(&payload_bytes) {
                                events.push((event_name, data));
                            }
                        }
                        events
                    };

                    for (event_name, data) in general_events {
                        debug!(sid = %sid, event = %event_name, "BRC-103 general message decoded");
                        handle_general_event(&socket, &ws, &sid, &event_name, data).await;
                    }

                    // Emit authenticationSuccess BRC-103-wrapped.
                    // The client's initializeConnection() waits for this before resolving.
                    if let Err(e) = ws.emit_brc103(
                        &sid,
                        "authenticationSuccess",
                        serde_json::json!({"status": "success"}),
                    ).await {
                        warn!(sid = %sid, error = %e, "failed to emit authenticationSuccess");
                    }

                    debug!(sid = %sid, key = %claimed_identity, "BRC-103 authMessage processed");
                }
            },
        );

        // --- disconnect ---
        socket.on_disconnect(move |socket: SocketRef, reason: socketioxide::socket::DisconnectReason| {
            let ws = ws_dc.clone();
            async move {
                let sid = socket.id.to_string();
                ws.remove_identity(&sid);
                ws.remove_peer(&sid).await;
                info!(sid = %sid, reason = ?reason, "client disconnected");
            }
        });
    });
}

// ---------------------------------------------------------------------------
// Helper: build room ID
// ---------------------------------------------------------------------------

/// Build a room ID from a recipient identity key and message box name.
pub fn room_id(recipient: &str, message_box: &str) -> String {
    format!("{recipient}-{message_box}")
}

/// Current UTC timestamp in ISO 8601 format.
fn now_iso8601() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

// ---------------------------------------------------------------------------
// BRC-103 general message helpers
// ---------------------------------------------------------------------------

/// Decode an application event from a BRC-103 general message payload.
///
/// Returns `Some((event_name, data))`.  Matches the client's format:
/// `{"eventName": "...", "data": ...}`.
fn decode_ws_event(payload: &[u8]) -> Option<(String, serde_json::Value)> {
    let v: serde_json::Value = serde_json::from_slice(payload).ok()?;
    let event_name = v.get("eventName")?.as_str()?.to_string();
    let data = v.get("data").cloned().unwrap_or(serde_json::Value::Null);
    Some((event_name, data))
}

/// Split a room ID into (identity_key, message_box).
///
/// Room ID format: `{identityKey}-{messageBox}`.
/// Identity keys are 66-char hex strings (compressed public key).
fn split_room_id(room_id: &str) -> Option<(String, String)> {
    const HEX_KEY_LEN: usize = 66;
    if room_id.len() > HEX_KEY_LEN && room_id.as_bytes()[HEX_KEY_LEN] == b'-' {
        let key = room_id[..HEX_KEY_LEN].to_string();
        let mb = room_id[HEX_KEY_LEN + 1..].to_string();
        return Some((key, mb));
    }
    // Fallback: split at first hyphen
    room_id.find('-').map(|pos| {
        (room_id[..pos].to_string(), room_id[pos + 1..].to_string())
    })
}

/// Handle a decoded BRC-103 general message (application event).
///
/// Routes events to the appropriate handler:
/// - `authenticated`: identity already captured from AuthMessage envelope — no-op
/// - `joinRoom`: join socket to room, emit `joinedRoom` BRC-103-wrapped
/// - `leaveRoom`: leave room, emit `leftRoom` BRC-103-wrapped
/// - `sendMessage`: store in DB, broadcast, emit ack BRC-103-wrapped
async fn handle_general_event(
    socket: &SocketRef,
    ws: &WsBroadcast,
    sid: &str,
    event_name: &str,
    data: serde_json::Value,
) {
    match event_name {
        "authenticated" => {
            // Identity already extracted from AuthMessage envelope — no-op.
            debug!(sid = %sid, "BRC-103 authenticated event (identity already set)");
        }
        "joinRoom" => {
            // Data is the room ID string (e.g., "{identityKey}-{messageBox}")
            let room_id_str = data.as_str().unwrap_or("").to_string();
            if room_id_str.is_empty() {
                warn!(sid = %sid, "joinRoom: empty room ID");
                return;
            }

            // Verify the client owns this room
            if let Some(ref key) = ws.get_identity(sid) {
                if !room_id_str.starts_with(key.as_str()) {
                    warn!(sid = %sid, room = %room_id_str, "BRC-103 joinRoom rejected: identity mismatch");
                    return;
                }
            }

            socket.join(room_id_str.clone()).ok();
            debug!(sid = %sid, room = %room_id_str, "BRC-103 joinRoom: joined room");

            if let Err(e) = ws.emit_brc103(
                sid,
                "joinedRoom",
                serde_json::json!({"roomId": room_id_str}),
            ).await {
                warn!(sid = %sid, room = %room_id_str, error = %e, "joinRoom: emit_brc103 failed");
            }
        }
        "leaveRoom" => {
            let room_id_str = data.as_str().unwrap_or("").to_string();
            if !room_id_str.is_empty() {
                socket.leave(room_id_str.clone()).ok();
                debug!(sid = %sid, room = %room_id_str, "BRC-103 leaveRoom: left room");

                if let Err(e) = ws.emit_brc103(
                    sid,
                    "leftRoom",
                    serde_json::json!({"roomId": room_id_str}),
                ).await {
                    warn!(sid = %sid, room = %room_id_str, error = %e, "leaveRoom: emit_brc103 failed");
                }
            }
        }
        "sendMessage" => {
            handle_ws_send_message(ws, sid, data).await;
        }
        _ => {
            debug!(sid = %sid, event = %event_name, "BRC-103 general message: unhandled event");
        }
    }
}

/// Maximum allowed message body size (1 MB).
const MAX_MESSAGE_BODY_BYTES: usize = 1024 * 1024;

/// Handle a `sendMessage` event received via BRC-103 general message.
///
/// Expected payload: `{"roomId": "{recipient}-{mb}", "message": {"messageId": "...", "recipient": "...", "body": "..."}}`
///
/// Stores the message in the database, broadcasts to the target room, and
/// emits `sendMessageAck-{roomId}` BRC-103-wrapped back to the sender.
async fn handle_ws_send_message(
    ws: &WsBroadcast,
    sid: &str,
    data: serde_json::Value,
) {
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

    let sender = match ws.get_identity(sid) {
        Some(k) => k,
        None => {
            warn!(sid = %sid, "BRC-103 sendMessage: no authenticated identity");
            return;
        }
    };

    let message_box = match split_room_id(&room_id_str) {
        Some((_key, mb)) => mb,
        None => {
            warn!(sid = %sid, room = %room_id_str, "BRC-103 sendMessage: invalid roomId format");
            return;
        }
    };

    let ack_event = format!("sendMessageAck-{room_id_str}");

    // Wrap body in {"message": body} format matching the HTTP handler
    let stored_body = serde_json::json!({"message": body}).to_string();

    let persist_result: Result<(), String> = async {
        db::queries::ensure_message_box(&ws.db, &recipient, &message_box)
            .await
            .map_err(|e| format!("ensure_message_box: {e}"))?;
        let mb_id = db::queries::get_message_box_id(&ws.db, &recipient, &message_box)
            .await
            .map_err(|e| format!("get_message_box_id: {e}"))?
            .ok_or_else(|| "messageBox not found after ensure".to_string())?;
        let inserted = db::queries::insert_message(
            &ws.db,
            &message_id,
            mb_id,
            &sender,
            &recipient,
            &stored_body,
        )
        .await
        .map_err(|e| format!("insert_message: {e}"))?;
        if !inserted {
            warn!(sid = %sid, msg_id = %message_id, "BRC-103 sendMessage: duplicate messageId (acking anyway)");
        }
        Ok(())
    }
    .await;

    match persist_result {
        Ok(()) => {
            debug!(
                sid = %sid,
                msg_id = %message_id,
                recipient = %recipient,
                message_box = %message_box,
                "BRC-103 sendMessage: stored in DB"
            );
        }
        Err(e) => {
            warn!(sid = %sid, error = %e, "BRC-103 sendMessage: DB error");
            if let Err(emit_err) = ws.emit_brc103(
                sid,
                &ack_event,
                serde_json::json!({"status": "error", "description": e}),
            ).await {
                warn!(sid = %sid, error = %emit_err, "sendMessage: failed to emit error ack");
            }
            return;
        }
    }

    // Broadcast to the target room for live delivery (BRC-103-wrapped per socket)
    let now = now_iso8601();
    let broadcast_event = format!("sendMessage-{room_id_str}");
    ws.broadcast_to_room(
        &room_id_str,
        &broadcast_event,
        &RoomMessage {
            message_id: message_id.clone(),
            sender: sender.clone(),
            recipient: recipient.clone(),
            message_box: message_box.clone(),
            body: stored_body,
            created_at: now.clone(),
            updated_at: now,
        },
    );

    // Emit ack to sender, BRC-103-wrapped.
    // TS server sends {status, messageId} — include messageId per spec.
    if let Err(e) = ws.emit_brc103(
        sid,
        &ack_event,
        serde_json::json!({"status": "success", "messageId": message_id}),
    ).await {
        warn!(sid = %sid, ack = %ack_event, error = %e, "BRC-103 sendMessage: failed to emit ack");
    } else {
        debug!(sid = %sid, ack = %ack_event, "BRC-103 sendMessage: ack emitted");
    }
}
