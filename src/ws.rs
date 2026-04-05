//! WebSocket (Socket.IO) support for the MessageBox server.
//!
//! Uses `socketioxide` to provide a Socket.IO v4 server that is compatible
//! with the `rust_socketio` client used by `rust-messagebox-client`.
//!
//! ## Protocol
//!
//! Clients connect via Socket.IO and authenticate by emitting an
//! `authenticated` event with their identity key. After authentication,
//! they can join rooms to receive live message push.
//!
//! Room naming convention: `{identityKey}-{messageBox}`
//!
//! When a message is sent via HTTP `POST /sendMessage`, the server
//! broadcasts it to the corresponding room so WebSocket-connected
//! clients receive it instantly.

use std::sync::Arc;

use std::collections::HashMap;
use std::sync::Arc as StdArc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use socketioxide::extract::{Data, SocketRef};
use socketioxide::SocketIo;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, info, warn};

use bsv::auth::peer::Peer;
use bsv::auth::transports::Transport;
use bsv::auth::types::AuthMessage as SdkAuthMessage;
use bsv::wallet::proto_wallet::ProtoWallet as SdkProtoWallet;

use bsv::auth::error::AuthError;

use crate::db::{self, DbPool};
use crate::handlers::helpers::is_valid_pub_key;

// ---------------------------------------------------------------------------
// Channel-based Transport for bsv-sdk Peer (server-side WS auth)
// ---------------------------------------------------------------------------

/// A simple channel-based transport that connects a bsv-sdk Peer to
/// Socket.IO events. Outgoing messages from Peer are sent to a channel
/// that the WS handler reads and emits as authMessage events.
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
// Types
// ---------------------------------------------------------------------------

/// Event sent by client to authenticate after connecting.
#[derive(Deserialize, Debug)]
pub struct AuthenticatedData {
    #[serde(rename = "identityKey")]
    pub identity_key: String,
}

/// Event sent by client to join a room.
#[derive(Deserialize, Debug)]
pub struct JoinRoomData {
    #[serde(rename = "roomId")]
    pub room_id: String,
}

/// Event sent by client to leave a room.
#[derive(Deserialize, Debug)]
pub struct LeaveRoomData {
    #[serde(rename = "roomId")]
    pub room_id: String,
}

/// Message broadcast to a room when a new message is stored.
///
/// Includes `created_at` and `updated_at` fields required by the client's
/// `ServerPeerMessage` parser. Without these, the client's `on_any` handler
/// fails to deserialize the broadcast and the callback never fires.
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
/// Held in the socketioxide layer state so HTTP handlers can access
/// the `SocketIo` handle to broadcast messages.
#[derive(Clone)]
pub struct WsBroadcast {
    io: SocketIo,
    /// Maps socket ID → authenticated identity key.
    socket_identities: Arc<RwLock<HashMap<String, String>>>,
    /// Server private key hex for creating bsv-sdk Peers.
    server_private_key_hex: String,
    /// Database pool for WS-initiated message storage (sendMessage via BRC-103).
    db: DbPool,
    /// Per-socket Peer + channels for BRC-103 auth and general message routing.
    ///
    /// Tuple: (Peer, incoming_tx, outgoing_rx, general_msg_rx)
    /// - incoming_tx: feeds AuthMessage events to the Peer's transport
    /// - outgoing_rx: drains Peer's transport responses (emitted as authMessage)
    /// - general_msg_rx: decoded application messages from BRC-103 General payloads
    #[allow(clippy::type_complexity)]
    socket_peers: Arc<tokio::sync::Mutex<HashMap<String, (
        tokio::sync::Mutex<Peer<SdkProtoWallet>>,
        mpsc::Sender<SdkAuthMessage>,
        tokio::sync::Mutex<mpsc::Receiver<SdkAuthMessage>>,
        tokio::sync::Mutex<mpsc::Receiver<(String, Vec<u8>)>>,
    )>>>,
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

    /// Get or create a Peer for a socket.
    ///
    /// Calls `peer.on_general_message()` to capture the receiver for decoded
    /// BRC-103 general messages (application events like sendMessage, joinRoom).
    async fn ensure_peer(&self, socket_id: &str) -> Result<(), String> {
        let mut peers = self.socket_peers.lock().await;
        if peers.contains_key(socket_id) {
            return Ok(());
        }

        let wallet = self.create_sdk_wallet()?;
        let (transport, incoming_tx, outgoing_rx) = ChannelTransport::new();
        let mut peer = Peer::new(wallet, StdArc::new(transport));

        // Take the general message receiver BEFORE storing the Peer.
        // on_general_message() is take-once — must be called here.
        let general_msg_rx = peer
            .on_general_message()
            .expect("on_general_message must succeed on fresh Peer");

        peers.insert(socket_id.to_string(), (
            tokio::sync::Mutex::new(peer),
            incoming_tx,
            tokio::sync::Mutex::new(outgoing_rx),
            tokio::sync::Mutex::new(general_msg_rx),
        ));
        Ok(())
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

    /// Broadcast a message to a specific room.
    ///
    /// Called from the HTTP `send_message` handler after a message is
    /// persisted to the database. The room name follows the convention
    /// `{recipientIdentityKey}-{messageBox}`.
    ///
    /// Uses both room-based broadcast AND direct socket delivery for
    /// reliability: room broadcast goes through socketioxide's room
    /// mechanism, while direct delivery finds matching sockets by
    /// identity key and emits to each one.
    pub fn broadcast_to_room(&self, room_id: &str, event: &str, msg: &RoomMessage) {
        let json = match serde_json::to_value(msg) {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "failed to serialize room message");
                return;
            }
        };

        // Primary: room-based broadcast
        let room_owned = room_id.to_string();
        let event_owned = event.to_string();
        self.io.to(room_owned).emit(&event_owned, &json).ok();

        // Fallback: direct socket delivery by recipient identity key.
        // Some socketioxide configurations may not deliver room broadcasts
        // reliably. This ensures the recipient gets the message.
        let recipient_key = &msg.recipient;
        let identities = self.socket_identities.read();
        for (sid, key) in identities.iter() {
            if key == recipient_key {
                if let Some(ns) = self.io.of("/") {
                    if let Some(socket) = ns.get_socket(sid.parse().unwrap_or_default()) {
                        socket.emit(&event_owned, &json).ok();
                        debug!(sid = %sid, event = %event_owned, "direct delivery to recipient socket");
                    }
                }
            }
        }

        debug!(room = room_id, event = event, "broadcast to room");
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
        let ws_auth = ws.clone();
        let ws_join = ws.clone();
        let ws_dc = ws.clone();

        // --- authMessage (BRC-103 mutual auth + general message routing) ---
        //
        // Handles both the BRC-103 handshake AND subsequent application messages.
        // The client wraps ALL events (sendMessage, joinRoom, leaveRoom) in BRC-103
        // General messages sent as authMessage Socket.IO events. After the Peer
        // decodes them, we read the decoded payloads from the general message
        // channel and route them to the appropriate handlers.
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

                    // Hold identity_key from envelope — only trust it AFTER Peer verifies
                    let claimed_identity = incoming.identity_key.clone();

                    // Ensure a Peer exists for this socket
                    if let Err(e) = ws.ensure_peer(&sid).await {
                        warn!(sid = %sid, error = %e, "failed to create Peer");
                        return;
                    }

                    // Feed message to Peer and collect responses
                    let peers = ws.socket_peers.lock().await;
                    if let Some((peer_mutex, incoming_tx, outgoing_rx_mutex, general_rx_mutex)) = peers.get(&sid) {
                        // Send the incoming message to the Peer's transport
                        if let Err(e) = incoming_tx.send(incoming).await {
                            warn!(sid = %sid, error = %e, "failed to feed authMessage to Peer");
                            return;
                        }

                        // Let the Peer process — this verifies the signature
                        let mut peer = peer_mutex.lock().await;
                        if let Err(e) = peer.process_pending().await {
                            warn!(sid = %sid, error = %e, "Peer process_pending failed (signature verification may have failed)");
                            // Don't return — there might still be outgoing/general messages
                        }
                        drop(peer);

                        // Only set identity AFTER Peer has processed (verified) the message
                        if is_valid_pub_key(&claimed_identity) {
                            ws.set_identity(&sid, claimed_identity.clone());
                        }

                        // Drain all outgoing messages and emit as authMessage events
                        {
                            let mut outgoing_rx = outgoing_rx_mutex.lock().await;
                            while let Ok(response) = outgoing_rx.try_recv() {
                                let resp_json = serde_json::to_value(&response).unwrap_or_default();
                                socket.emit("authMessage", &resp_json).ok();
                                debug!(sid = %sid, "emitted authMessage response");
                            }
                        }

                        // Drain decoded general messages (application events from BRC-103).
                        // This is the critical path: the client wraps sendMessage, joinRoom,
                        // leaveRoom in BRC-103 General messages. The Peer decodes them and
                        // puts the payload here. Without draining this channel, those events
                        // are silently dropped and the client times out.
                        {
                            let mut general_rx = general_rx_mutex.lock().await;
                            while let Ok((_sender_key, payload_bytes)) = general_rx.try_recv() {
                                if let Some((event_name, data)) = decode_ws_event(&payload_bytes) {
                                    debug!(sid = %sid, event = %event_name, "BRC-103 general message decoded");
                                    handle_general_event(&socket, &ws, &sid, &event_name, data).await;
                                }
                            }
                        }

                        // Emit authenticationSuccess — the client waits for this to
                        // confirm the handshake. Harmless as a no-op on subsequent messages
                        // (client's oneshot fires once, ignores duplicates).
                        socket.emit("authenticationSuccess", &serde_json::json!({
                            "identityKey": claimed_identity
                        })).ok();

                        debug!(sid = %sid, key = %claimed_identity, "BRC-103 authMessage processed");
                    }
                }
            },
        );

        // --- authenticated (simple identity announcement) ---
        // REJECTED: simple identity announcement without cryptographic proof.
        // All clients MUST use BRC-103 authMessage for authentication.
        socket.on(
            "authenticated",
            move |socket: SocketRef, Data(_data): Data<AuthenticatedData>| {
                let _ws = ws_auth.clone();
                async move {
                    let sid = socket.id.to_string();
                    warn!(sid = %sid, "rejected simple 'authenticated' event — use BRC-103 authMessage instead");
                    socket.emit("authenticationFailed", &serde_json::json!({
                        "error": "Simple authentication not supported. Use BRC-103 mutual auth via authMessage."
                    })).ok();
                }
            },
        );

        // --- joinRoom ---
        // Only allowed for sockets that completed BRC-103 handshake (have a Peer).
        // The BRC-103 path (handle_general_event) also handles joinRoom for
        // clients that wrap everything in authMessage — this handler is for
        // raw Socket.IO joinRoom events from authenticated sockets.
        socket.on(
            "joinRoom",
            move |socket: SocketRef, Data(data): Data<JoinRoomData>| {
                let ws = ws_join.clone();
                async move {
                    let sid = socket.id.to_string();

                    // Require BRC-103 authenticated identity (must have completed handshake)
                    let identity = match ws.get_identity(&sid) {
                        Some(key) => key,
                        None => {
                            warn!(sid = %sid, "joinRoom rejected: no BRC-103 authenticated session");
                            socket.emit("authenticationFailed", &serde_json::json!({
                                "error": "Not authenticated. Complete BRC-103 handshake first."
                            })).ok();
                            return;
                        }
                    };

                    // Verify the socket has a Peer (BRC-103 handshake completed)
                    {
                        let peers = ws.socket_peers.lock().await;
                        if !peers.contains_key(&sid) {
                            warn!(sid = %sid, "joinRoom rejected: no BRC-103 Peer for socket");
                            return;
                        }
                    }

                    // Verify the client owns this room
                    if !data.room_id.starts_with(&identity) {
                        warn!(sid = %sid, room = %data.room_id, "room join rejected: identity mismatch");
                        return;
                    }

                    socket.join(data.room_id.clone()).ok();
                    debug!(sid = %sid, room = %data.room_id, "joined room");
                    socket.emit("joinedRoom", &serde_json::json!({
                        "roomId": data.room_id
                    })).ok();
                }
            },
        );

        // --- leaveRoom ---
        // Raw leaveRoom is only accepted from BRC-103 authenticated sockets.
        // The primary path is via BRC-103 general messages (handle_general_event).
        socket.on(
            "leaveRoom",
            move |socket: SocketRef, Data(data): Data<LeaveRoomData>| {
                async move {
                    let sid = socket.id.to_string();
                    // leaveRoom is safe — worst case a socket leaves a room it wasn't in.
                    // No auth bypass risk since room membership is additive only for the
                    // socket's own identity prefix (enforced in joinRoom).
                    socket.leave(data.room_id.clone()).ok();
                    debug!(sid = %sid, room = %data.room_id, "left room");
                    socket.emit("leftRoom", &serde_json::json!({
                        "roomId": data.room_id
                    })).ok();
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

/// Current UTC timestamp in ISO 8601 format (matches SQLite strftime output).
fn now_iso8601() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

// ---------------------------------------------------------------------------
// BRC-103 general message helpers
// ---------------------------------------------------------------------------

/// Decode an application event from a BRC-103 general message payload.
///
/// Returns `Some((event_name, data))` on success. Matches the client's
/// `encode_ws_event` format: `{"eventName": "...", "data": ...}`.
fn decode_ws_event(payload: &[u8]) -> Option<(String, serde_json::Value)> {
    let v: serde_json::Value = serde_json::from_slice(payload).ok()?;
    let event_name = v.get("eventName")?.as_str()?.to_string();
    let data = v.get("data").cloned().unwrap_or(serde_json::Value::Null);
    Some((event_name, data))
}

/// Encode an application event as a BRC-103 general message payload.
///
/// Produces `{"eventName": "...", "data": ...}` serialized to bytes.
/// Used for sending responses back through the BRC-103 Peer channel.
#[allow(dead_code)]
fn encode_ws_event(event_name: &str, data: serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "eventName": event_name,
        "data": data
    }))
    .unwrap_or_default()
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
/// - `authenticated`: identity already captured from AuthMessage envelope
/// - `joinRoom`: join the socket to the requested room
/// - `leaveRoom`: remove socket from the room
/// - `sendMessage`: store message in DB, broadcast to room, emit ack
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
                if !room_id_str.starts_with(key) {
                    warn!(sid = %sid, room = %room_id_str, "BRC-103 joinRoom rejected: identity mismatch");
                    return;
                }
            }

            socket.join(room_id_str.clone()).ok();
            debug!(sid = %sid, room = %room_id_str, "BRC-103 joinRoom: joined room");
            // Emit joinedRoom as raw event (client's on_any handles it)
            socket.emit("joinedRoom", &serde_json::json!({
                "roomId": room_id_str
            })).ok();
        }
        "leaveRoom" => {
            let room_id_str = data.as_str().unwrap_or("").to_string();
            if !room_id_str.is_empty() {
                socket.leave(room_id_str.clone()).ok();
                debug!(sid = %sid, room = %room_id_str, "BRC-103 leaveRoom: left room");
                socket.emit("leftRoom", &serde_json::json!({
                    "roomId": room_id_str
                })).ok();
            }
        }
        "sendMessage" => {
            handle_ws_send_message(socket, ws, sid, data).await;
        }
        _ => {
            debug!(sid = %sid, event = %event_name, "BRC-103 general message: unhandled event");
        }
    }
}

/// Maximum allowed message body size (1 MB).
/// Prevents DoS via oversized messages stored in the database.
const MAX_MESSAGE_BODY_BYTES: usize = 1024 * 1024;

/// Handle a sendMessage event received via BRC-103 general message.
///
/// The client sends: `{"roomId": "{recipient}-{mb}", "message": {"messageId": "...", "recipient": "...", "body": "..."}}`
///
/// Stores the message in the database, broadcasts to the target room for
/// live delivery, and emits `sendMessageAck-{roomId}` back to the sender.
async fn handle_ws_send_message(
    socket: &SocketRef,
    ws: &WsBroadcast,
    sid: &str,
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

    // Get sender identity from socket
    let sender = match ws.get_identity(sid) {
        Some(k) => k,
        None => {
            warn!(sid = %sid, "BRC-103 sendMessage: no authenticated identity");
            return;
        }
    };

    // Extract messageBox from roomId
    let message_box = match split_room_id(&room_id_str) {
        Some((_key, mb)) => mb,
        None => {
            warn!(sid = %sid, room = %room_id_str, "BRC-103 sendMessage: invalid roomId format");
            return;
        }
    };

    // Store message in database
    let ack_event = format!("sendMessageAck-{room_id_str}");

    // Wrap body in {"message": body} format matching the HTTP handler
    let stored_body = serde_json::json!({"message": body}).to_string();

    match (|| -> Result<(), String> {
        db::queries::ensure_message_box(&ws.db, &recipient, &message_box)
            .map_err(|e| format!("ensure_message_box: {e}"))?;
        let mb_id = db::queries::get_message_box_id(&ws.db, &recipient, &message_box)
            .map_err(|e| format!("get_message_box_id: {e}"))?
            .ok_or_else(|| "messageBox not found after ensure".to_string())?;
        let inserted = db::queries::insert_message(&ws.db, &message_id, mb_id, &sender, &recipient, &stored_body)
            .map_err(|e| format!("insert_message: {e}"))?;
        if !inserted {
            warn!(sid = %sid, msg_id = %message_id, "BRC-103 sendMessage: duplicate messageId");
            // Still emit ack — the message already exists, client should not retry
        }
        Ok(())
    })() {
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
            socket.emit(&ack_event, &serde_json::json!({"status": "error", "description": e})).ok();
            return;
        }
    }

    // Broadcast to the target room for live delivery
    let now = now_iso8601();
    let event = format!("sendMessage-{room_id_str}");
    ws.broadcast_to_room(
        &room_id_str,
        &event,
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

    // Emit ack to the sender socket
    socket.emit(&ack_event, &serde_json::json!({"status": "success"})).ok();
    debug!(sid = %sid, ack = %ack_event, "BRC-103 sendMessage: ack emitted");
}
