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

use std::collections::{HashMap, HashSet};
use std::sync::Arc as StdArc;

use parking_lot::RwLock;
use serde::Serialize;
use socketioxide::extract::{Data, SocketRef};
use socketioxide::SocketIo;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use bsv::auth::peer::Peer;
use bsv::auth::transports::Transport;
use bsv::auth::types::AuthMessage as SdkAuthMessage;
use bsv::wallet::proto_wallet::ProtoWallet as SdkProtoWallet;

use bsv::auth::error::AuthError;

use crate::db::DbPool;
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
    pub fn new() -> (
        Self,
        mpsc::Sender<SdkAuthMessage>,
        mpsc::Receiver<SdkAuthMessage>,
    ) {
        // Sized for handshake/auth bursts under many concurrent connections;
        // room broadcasts bypass this channel (emitted directly), so this only
        // buffers per-socket handshake + reply frames.
        let (in_tx, in_rx) = mpsc::channel(64);
        let (out_tx, out_rx) = mpsc::channel(64);
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
        self.incoming_rx
            .lock()
            .unwrap()
            .take()
            .expect("subscribe called twice")
    }
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

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

/// Per-socket BRC-103 session: the bsv-sdk `Peer` plus the channel ends that
/// bridge it to this socket's Socket.IO events.
///
/// - `incoming_tx`: feeds inbound `authMessage` frames to the Peer's transport.
/// - `outgoing_rx`: drains the Peer's outbound frames (emitted as `authMessage`).
/// - `general_rx`: decoded application events from BRC-103 general payloads.
///
/// Each socket owns its `SocketPeer` behind an `Arc`. Handlers clone the `Arc`
/// out of the registry under a brief lock and then operate on the per-socket
/// async mutexes — so concurrent sockets never serialize on a single global
/// lock (the previous design held one `Mutex<HashMap>` across `process_pending`,
/// which capped throughput under many high-frequency connections).
pub struct SocketPeer {
    peer: tokio::sync::Mutex<Peer<SdkProtoWallet>>,
    incoming_tx: mpsc::Sender<SdkAuthMessage>,
    outgoing_rx: tokio::sync::Mutex<mpsc::Receiver<SdkAuthMessage>>,
    general_rx: tokio::sync::Mutex<mpsc::Receiver<(String, Vec<u8>)>>,
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
    /// Per-socket BRC-103 session, keyed by socket id. Stored as `Arc` so the
    /// map lock is held only long enough to clone the handle out — never across
    /// `await` on a Peer operation.
    socket_peers: Arc<RwLock<HashMap<String, Arc<SocketPeer>>>>,
    /// Room membership: room id (`{identityKey}-{messageBox}`) → set of socket
    /// ids that joined it. Used to fan a signed broadcast out to exactly the
    /// sockets subscribed to that room.
    rooms: Arc<RwLock<HashMap<String, HashSet<String>>>>,
    /// Background, durable message persistence. Both send paths push live first
    /// and then hand the INSERT to this worker so MySQL latency never blocks
    /// live delivery. See [`crate::persist`].
    persist: crate::persist::PersistHandle,
}

impl WsBroadcast {
    pub fn new(io: SocketIo, server_private_key_hex: String, db: DbPool) -> Self {
        let persist = crate::persist::PersistHandle::spawn(
            db.clone(),
            crate::persist::PersistConfig::default(),
        );
        Self {
            io,
            socket_identities: Arc::new(RwLock::new(HashMap::new())),
            server_private_key_hex,
            socket_peers: Arc::new(RwLock::new(HashMap::new())),
            rooms: Arc::new(RwLock::new(HashMap::new())),
            persist,
        }
    }

    /// Enqueue a message for asynchronous, durable persistence (off the hot
    /// path). The caller MUST have already performed all gating checks
    /// (auth/recipient/fee/permission/payment) and the live broadcast.
    ///
    /// Returns the [`Enqueued`] outcome from the worker so callers can observe
    /// when the fast (queued) path was bypassed for an inline write or the job
    /// was dead-lettered. Never silently drops.
    pub async fn persist_async(&self, job: crate::persist::PersistJob) -> crate::persist::Enqueued {
        self.persist.enqueue(job).await
    }

    /// Get or create the `SocketPeer` for a socket and return a handle to it.
    ///
    /// Calls `peer.on_general_message()` to capture the receiver for decoded
    /// BRC-103 general messages (application events like sendMessage, joinRoom).
    /// Idempotent and race-safe: if two inbound frames for a fresh socket arrive
    /// concurrently, the first insert wins and both observe the same handle.
    fn ensure_peer(&self, socket_id: &str) -> Result<Arc<SocketPeer>, String> {
        if let Some(existing) = self.socket_peers.read().get(socket_id) {
            return Ok(existing.clone());
        }

        let wallet = self.create_sdk_wallet()?;
        let (transport, incoming_tx, outgoing_rx) = ChannelTransport::new();
        let peer = Peer::new(wallet, StdArc::new(transport));

        // Take the general message receiver BEFORE storing the Peer.
        // on_general_message() is take-once — must be called here.
        let general_rx = peer
            .on_general_message()
            .expect("on_general_message must succeed on fresh Peer");

        let socket_peer = Arc::new(SocketPeer {
            peer: tokio::sync::Mutex::new(peer),
            incoming_tx,
            outgoing_rx: tokio::sync::Mutex::new(outgoing_rx),
            general_rx: tokio::sync::Mutex::new(general_rx),
        });

        let mut peers = self.socket_peers.write();
        Ok(peers
            .entry(socket_id.to_string())
            .or_insert(socket_peer)
            .clone())
    }

    /// Look up an existing `SocketPeer` handle without creating one.
    fn get_peer(&self, socket_id: &str) -> Option<Arc<SocketPeer>> {
        self.socket_peers.read().get(socket_id).cloned()
    }

    /// Remove a socket's Peer and all its room memberships on disconnect.
    fn remove_peer(&self, socket_id: &str) {
        self.socket_peers.write().remove(socket_id);
        self.remove_socket_from_all_rooms(socket_id);
    }

    /// Record that `socket_id` joined `room_id`.
    fn add_room_member(&self, room_id: &str, socket_id: &str) {
        self.rooms
            .write()
            .entry(room_id.to_string())
            .or_default()
            .insert(socket_id.to_string());
    }

    /// Remove `socket_id` from `room_id`, dropping the room if it empties.
    fn remove_room_member(&self, room_id: &str, socket_id: &str) {
        let mut rooms = self.rooms.write();
        if let Some(members) = rooms.get_mut(room_id) {
            members.remove(socket_id);
            if members.is_empty() {
                rooms.remove(room_id);
            }
        }
    }

    /// Remove `socket_id` from every room (used on disconnect).
    fn remove_socket_from_all_rooms(&self, socket_id: &str) {
        let mut rooms = self.rooms.write();
        for members in rooms.values_mut() {
            members.remove(socket_id);
        }
        rooms.retain(|_, members| !members.is_empty());
    }

    /// Socket ids currently joined to `room_id`.
    fn room_members(&self, room_id: &str) -> Vec<String> {
        self.rooms
            .read()
            .get(room_id)
            .map(|m| m.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Create a bsv-sdk ProtoWallet for BRC-103 auth.
    fn create_sdk_wallet(&self) -> Result<SdkProtoWallet, String> {
        let pk = bsv::primitives::private_key::PrivateKey::from_hex(&self.server_private_key_hex)
            .map_err(|e| format!("parse server key: {e}"))?;
        Ok(SdkProtoWallet::new(pk))
    }

    /// Store the identity key for an authenticated socket.
    pub fn set_identity(&self, socket_id: &str, identity_key: String) {
        self.socket_identities
            .write()
            .insert(socket_id.to_string(), identity_key);
    }

    /// Get the identity key for a socket.
    pub fn get_identity(&self, socket_id: &str) -> Option<String> {
        self.socket_identities.read().get(socket_id).cloned()
    }

    /// Remove a socket's identity (on disconnect).
    pub fn remove_identity(&self, socket_id: &str) {
        self.socket_identities.write().remove(socket_id);
    }

    /// Sign one server→client application event as a BRC-103 **general message**
    /// for `sid`'s authenticated session and emit it as `authMessage`.
    ///
    /// This is the `@bsv/authsocket` contract for *every* server→client event
    /// (`AuthSocket.emit` → `peer.toPeer`): acks, room confirmations, and
    /// `authenticationSuccess` all travel as signed, verifiable general messages
    /// — never raw Socket.IO events. A strict authsocket client only dispatches
    /// verified general messages, so a raw emit would be invisible to it.
    ///
    /// Returns `false` if the socket has no authenticated session yet (the
    /// caller simply skips — there is nothing to sign against).
    async fn emit_signed(
        &self,
        socket: &SocketRef,
        sid: &str,
        event_name: &str,
        data: serde_json::Value,
    ) -> bool {
        let identity = match self.get_identity(sid) {
            Some(k) => k,
            None => return false,
        };
        let socket_peer = match self.get_peer(sid) {
            Some(p) => p,
            None => return false,
        };
        let payload = encode_ws_event(event_name, data);
        let signed = {
            let peer = socket_peer.peer.lock().await;
            match peer.create_general_message(&identity, payload).await {
                Ok(m) => m,
                Err(e) => {
                    debug!(sid = %sid, event = %event_name, error = %e,
                        "emit_signed: no authenticated session, skipping");
                    return false;
                }
            }
        };
        match serde_json::to_value(&signed) {
            Ok(json) => match socket.emit("authMessage", &json) {
                Ok(()) => true,
                Err(e) => {
                    // A failed emit means this signed server→client event (e.g. a
                    // sendMessageAck) never reached the client. Surface it rather
                    // than reporting success to the caller.
                    warn!(sid = %sid, event = %event_name, error = %e,
                        "emit_signed: emit failed — signed event not delivered to client");
                    false
                }
            },
            Err(e) => {
                warn!(sid = %sid, event = %event_name, error = %e, "emit_signed: serialize failed");
                false
            }
        }
    }

    /// Broadcast a message to a specific room over the **authenticated** channel.
    ///
    /// Called from the HTTP `send_message` handler (and the BRC-103 WS
    /// `sendMessage` path) after a message is persisted. The room name follows
    /// the convention `{recipientIdentityKey}-{messageBox}`.
    ///
    /// For every socket joined to the room that has completed the BRC-103
    /// handshake, this signs `{eventName, data}` as a BRC-103 **general
    /// message** through that socket's authenticated `Peer` session and emits it
    /// as an `authMessage` Socket.IO event. That lands on the client's
    /// authenticated *primary* receive path (`general_msg_dispatcher`) —
    /// instant **and** verified — instead of the old raw `io.to(room).emit`,
    /// which only reached the client's defensive `on_any` fallback and left real
    /// delivery to a ~2s HTTP poll.
    ///
    /// A socket that has not authenticated (no session) is skipped; the message
    /// is already persisted and that client will pick it up on (re)subscribe.
    ///
    /// Returns the number of room members the signed frame was successfully
    /// emitted to. Broadcast is best-effort — the message is already persisted,
    /// so failures never propagate to the HTTP caller — but a non-empty room
    /// that delivers to zero members is logged at `warn` as a degradation
    /// signal (live push broken → recipients fall back to the HTTP poll).
    pub async fn broadcast_to_room(&self, room_id: &str, event: &str, msg: &RoomMessage) -> usize {
        let data = match serde_json::to_value(msg) {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "failed to serialize room message");
                return 0;
            }
        };
        let payload = encode_ws_event(event, data);

        let members = self.room_members(room_id);
        let member_count = members.len();
        let mut delivered = 0usize;
        for sid in members {
            let identity = match self.get_identity(&sid) {
                Some(k) => k,
                None => continue, // not authenticated — nothing to sign against
            };
            let socket_peer = match self.get_peer(&sid) {
                Some(p) => p,
                None => continue,
            };

            // Sign a general message for this recipient's session. Uses the
            // non-mutating `create_general_message`, which REQUIRES an existing
            // authenticated session and never initiates a handshake — so a
            // broadcast can only ever reach an already-authenticated socket.
            //
            // We are PAST the `get_identity` guard above, so this socket has a
            // verified identity recorded: a sign failure here is NOT the benign
            // "not yet authenticated" case — it is a real defect (the session
            // exists but signing failed), so log it at `warn` with the error.
            let signed = {
                let peer = socket_peer.peer.lock().await;
                match peer
                    .create_general_message(&identity, payload.clone())
                    .await
                {
                    Ok(m) => m,
                    Err(e) => {
                        warn!(sid = %sid, room = %room_id, error = %e,
                            "broadcast: sign failed for an authenticated session — recipient misses this live push and falls back to the HTTP poll");
                        continue;
                    }
                }
            };

            let json = match serde_json::to_value(&signed) {
                Ok(v) => v,
                Err(e) => {
                    warn!(sid = %sid, error = %e, "broadcast: failed to serialize signed authMessage");
                    continue;
                }
            };
            let socket = match sid.parse() {
                Ok(id) => self.io.of("/").and_then(|ns| ns.get_socket(id)),
                Err(e) => {
                    warn!(sid = %sid, error = %e, "broadcast: unparseable socket id in room registry");
                    None
                }
            };
            if let Some(socket) = socket {
                // Count only successful emits — a full send buffer / mid-teardown
                // socket must not be reported as delivered.
                match socket.emit("authMessage", &json) {
                    Ok(()) => delivered += 1,
                    Err(e) => warn!(sid = %sid, room = %room_id, error = %e,
                        "broadcast: emit failed — recipient falls back to HTTP poll"),
                }
            }
        }

        if member_count > 0 && delivered == 0 {
            warn!(room = %room_id, members = member_count,
                "signed broadcast reached 0 of {member_count} room members — live push may be broken; recipients depend on the HTTP poll");
        }
        debug!(
            room = room_id,
            event = event,
            delivered,
            "signed broadcast to room"
        );
        delivered
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

                    // The envelope's identity_key is a CLAIM, not yet verified.
                    // It is used only to echo back in `authenticationSuccess`
                    // (a handshake-progress signal) — never to establish trust.
                    // The trusted identity is taken from the Peer's verified
                    // general-message sender below (see the general_rx drain).
                    let claimed_identity = incoming.identity_key.clone();

                    // Ensure a Peer exists for this socket and grab its handle.
                    // The registry lock is released immediately — all Peer work
                    // below runs on this socket's own mutexes, so other sockets'
                    // auth/broadcast traffic proceeds concurrently.
                    let socket_peer = match ws.ensure_peer(&sid) {
                        Ok(p) => p,
                        Err(e) => {
                            warn!(sid = %sid, error = %e, "failed to create Peer");
                            return;
                        }
                    };

                    // Send the incoming message to the Peer's transport
                    if let Err(e) = socket_peer.incoming_tx.send(incoming).await {
                        warn!(sid = %sid, error = %e, "failed to feed authMessage to Peer");
                        return;
                    }

                    // Let the Peer process — this runs the handshake and verifies
                    // signatures. A failure here means verification did not
                    // complete; we must NOT trust any claimed identity as a
                    // result (the identity is only ever set from a verified
                    // general-message sender below).
                    {
                        let peer = socket_peer.peer.lock().await;
                        if let Err(e) = peer.process_pending().await {
                            warn!(sid = %sid, error = %e, "Peer process_pending failed (verification did not complete; identity NOT trusted)");
                            // Don't return — there might still be outgoing/general messages
                        }
                    }

                    // Drain all outgoing messages and emit as authMessage events
                    {
                        let mut outgoing_rx = socket_peer.outgoing_rx.lock().await;
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
                    //
                    // SECURITY / @bsv parity: the bsv-sdk Peer pushes to this channel
                    // ONLY after it has verified the nonce, session, and signature
                    // (handle_general_message). So `sender_key` here is the
                    // CRYPTOGRAPHICALLY VERIFIED identity — the only value we trust
                    // for this socket. This mirrors @bsv/authsocket's
                    // `listenForGeneralMessages((senderPublicKey, _) => onIdentityKeyDiscovered(...))`.
                    // A forged authMessage with a bad signature never reaches here,
                    // so it can never set an identity. We set the identity BEFORE
                    // dispatching the event so room-ownership / sender checks in
                    // handle_general_event see the verified identity.
                    {
                        let mut general_rx = socket_peer.general_rx.lock().await;
                        while let Ok((sender_key, payload_bytes)) = general_rx.try_recv() {
                            if is_valid_pub_key(&sender_key) {
                                ws.set_identity(&sid, sender_key.clone());
                            }
                            if let Some((event_name, data)) = decode_ws_event(&payload_bytes) {
                                debug!(sid = %sid, event = %event_name, "BRC-103 general message decoded (verified sender)");
                                handle_general_event(&socket, &ws, &sid, &event_name, data).await;
                            }
                        }
                    }

                    // authenticationSuccess is NOT emitted here as a raw event.
                    // Per the authsocket contract it is a signed general message,
                    // sent from the `authenticated` general-event handler once the
                    // verified identity is known (see handle_general_event).
                    debug!(sid = %sid, claimed = %claimed_identity, "BRC-103 authMessage processed");
                }
            },
        );

        // NOTE: there are intentionally NO raw `socket.on("authenticated" |
        // "joinRoom" | "leaveRoom" | "sendMessage")` handlers. `authMessage` is
        // the ONLY inbound event the server acts on. Every application action
        // (authenticate, join/leave a room, send a message) must arrive as a
        // BRC-103-signed general message, which the Peer verifies before it
        // reaches `handle_general_event`. Accepting raw Socket.IO events would
        // be an authentication-bypass surface — a client could join another
        // identity's room or send as someone else without proving its key.

        // --- disconnect ---
        socket.on_disconnect(move |socket: SocketRef, reason: socketioxide::socket::DisconnectReason| {
            let ws = ws_dc.clone();
            async move {
                let sid = socket.id.to_string();
                ws.remove_identity(&sid);
                ws.remove_peer(&sid);
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
/// Used to sign room broadcasts as BRC-103 general messages (see
/// [`WsBroadcast::broadcast_to_room`]) and to send responses back through the
/// Peer channel.
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
    room_id
        .find('-')
        .map(|pos| (room_id[..pos].to_string(), room_id[pos + 1..].to_string()))
}

/// Handle a decoded BRC-103 general message (application event).
///
/// Routes events to the appropriate handler. Every server→client confirmation
/// is returned as a **signed** general message (`emit_signed`), matching the
/// `@bsv/authsocket` contract — never a raw Socket.IO event.
/// - `authenticated`: identity is already set from the verified sender; confirm
///   with a signed `authenticationSuccess`.
/// - `joinRoom`: join the socket to the requested room; signed `joinedRoom`.
/// - `leaveRoom`: remove socket from the room; signed `leftRoom`.
/// - `sendMessage`: store message in DB, broadcast to room, signed ack.
async fn handle_general_event(
    socket: &SocketRef,
    ws: &WsBroadcast,
    sid: &str,
    event_name: &str,
    data: serde_json::Value,
) {
    match event_name {
        "authenticated" => {
            // Identity was set from the verified general-message sender before
            // dispatch. Confirm the handshake with a signed authenticationSuccess
            // (authsocket sends this via AuthSocket.emit, not a raw event).
            let identity = ws.get_identity(sid).unwrap_or_default();
            ws.emit_signed(
                socket,
                sid,
                "authenticationSuccess",
                serde_json::json!({ "status": "success", "identityKey": identity }),
            )
            .await;
            debug!(sid = %sid, "BRC-103 authenticated event: signed authenticationSuccess sent");
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

            // A swallowed join error would leave the recipient silently absent
            // from the socketioxide room → they never receive live pushes. Log
            // it; we still record local membership so broadcast_to_room can fan
            // out via our own `rooms` registry, but a failed join is a real
            // degradation worth surfacing.
            if let Err(e) = socket.join(room_id_str.clone()) {
                warn!(sid = %sid, room = %room_id_str, error = %e,
                    "BRC-103 joinRoom: socket.join failed — recipient may miss live pushes");
            }
            ws.add_room_member(&room_id_str, sid);
            debug!(sid = %sid, room = %room_id_str, "BRC-103 joinRoom: joined room");
            ws.emit_signed(
                socket,
                sid,
                "joinedRoom",
                serde_json::json!({ "roomId": room_id_str }),
            )
            .await;
        }
        "leaveRoom" => {
            let room_id_str = data.as_str().unwrap_or("").to_string();
            if !room_id_str.is_empty() {
                socket.leave(room_id_str.clone()).ok();
                ws.remove_room_member(&room_id_str, sid);
                debug!(sid = %sid, room = %room_id_str, "BRC-103 leaveRoom: left room");
                ws.emit_signed(
                    socket,
                    sid,
                    "leftRoom",
                    serde_json::json!({ "roomId": room_id_str }),
                )
                .await;
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

    let ack_event = format!("sendMessageAck-{room_id_str}");

    // Wrap body in {"message": body} format matching the HTTP handler
    let stored_body = serde_json::json!({"message": body}).to_string();

    // ── PUSH-LIVE-FIRST ───────────────────────────────────────────────
    //
    // All gating checks for this path (authenticated identity, valid roomId,
    // body-size limit) have already passed above, and the WS sendMessage path
    // carries no fee/permission/payment gate. So broadcast to the live recipient
    // IMMEDIATELY — before touching MySQL — so live delivery never waits on the
    // DB. Durability is preserved by the async persist below.
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
            body: stored_body.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await;

    // ── PERSIST-ASYNC ─────────────────────────────────────────────────
    //
    // Hand the durable INSERT (ensure_message_box + insert_message) to the
    // background persist worker. It retries transient DB errors with backoff and
    // logs permanent failures at ERROR — the row still lands in MySQL for the
    // HTTP fallback (listMessages) used by offline recipients and re-polling
    // recipients. A duplicate messageId is caught by the unique constraint at
    // persist time and treated as idempotent success (see crate::persist).
    //
    // `PersistJob::new` performs the canonical `{"message": <body>}` wrap from
    // the RAW body (it owns the wrap), so we pass `body`, not the pre-wrapped
    // `stored_body` used for the live broadcast above.
    let enqueued = ws
        .persist_async(crate::persist::PersistJob::new(
            message_id.clone(),
            recipient.clone(),
            message_box.clone(),
            sender.clone(),
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

    // Emit a signed ack to the sender socket. The ack now means "accepted for
    // delivery" (pushed live + durably enqueued), not "committed to MySQL".
    // Persistence is handled by the worker: bounded retry + inline fallback
    // under backpressure; permanent or retry-exhausted failures are dead-lettered
    // to disk and logged at ERROR, not silently dropped (recovery is via the
    // dead-letter file). The client treats a later duplicate as idempotent
    // success. Payload shape matches the reference message-box-server:
    // { status, messageId }.
    ws.emit_signed(
        socket,
        sid,
        &ack_event,
        serde_json::json!({"status": "success", "messageId": message_id}),
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

    /// secp256k1 scalar `1` — a valid private key for the server identity.
    const TEST_SERVER_KEY: &str =
        "0000000000000000000000000000000000000000000000000000000000000001";

    fn test_ws() -> WsBroadcast {
        let (_layer, io) = socketioxide::SocketIo::new_layer();
        // Lazy MySQL pool: the membership / broadcast / codec paths exercised
        // by these tests never execute a query, so the pool is never actually
        // connected (no live MySQL required). The struct just requires a pool.
        let pool = sqlx::mysql::MySqlPoolOptions::new()
            .connect_lazy("mysql://test@127.0.0.1/test")
            .expect("build lazy MySQL pool");
        WsBroadcast::new(io, TEST_SERVER_KEY.to_string(), pool)
    }

    #[tokio::test]
    async fn room_membership_add_and_query() {
        let ws = test_ws();
        ws.add_room_member("keyA-inbox", "sock1");
        ws.add_room_member("keyA-inbox", "sock2");
        ws.add_room_member("keyB-inbox", "sock3");

        let mut a = ws.room_members("keyA-inbox");
        a.sort();
        assert_eq!(a, vec!["sock1".to_string(), "sock2".to_string()]);
        assert_eq!(ws.room_members("keyB-inbox"), vec!["sock3".to_string()]);
        assert!(ws.room_members("nonexistent").is_empty());
    }

    #[tokio::test]
    async fn room_membership_is_idempotent() {
        let ws = test_ws();
        ws.add_room_member("room", "sock1");
        ws.add_room_member("room", "sock1");
        assert_eq!(ws.room_members("room").len(), 1);
    }

    #[tokio::test]
    async fn remove_room_member_drops_empty_room() {
        let ws = test_ws();
        ws.add_room_member("room", "sock1");
        ws.remove_room_member("room", "sock1");
        assert!(ws.room_members("room").is_empty());
        // Room key itself is dropped once empty.
        assert!(ws.rooms.read().get("room").is_none());
    }

    #[tokio::test]
    async fn disconnect_removes_socket_from_all_rooms() {
        let ws = test_ws();
        ws.add_room_member("room1", "sock1");
        ws.add_room_member("room2", "sock1");
        ws.add_room_member("room1", "sock2");

        ws.remove_socket_from_all_rooms("sock1");

        assert_eq!(ws.room_members("room1"), vec!["sock2".to_string()]);
        assert!(ws.room_members("room2").is_empty());
    }

    #[tokio::test]
    async fn ensure_peer_is_idempotent_returns_same_handle() {
        let ws = test_ws();
        let a = ws.ensure_peer("sock1").expect("peer");
        let b = ws.ensure_peer("sock1").expect("peer");
        assert!(
            Arc::ptr_eq(&a, &b),
            "second ensure_peer must reuse the handle"
        );
    }

    #[tokio::test]
    async fn remove_peer_clears_peer_and_room_membership() {
        let ws = test_ws();
        ws.ensure_peer("sock1").expect("peer");
        ws.add_room_member("room", "sock1");
        ws.remove_peer("sock1");
        assert!(ws.get_peer("sock1").is_none());
        assert!(ws.room_members("room").is_empty());
    }

    #[test]
    fn ws_event_codec_round_trips() {
        let data = serde_json::json!({"messageId": "m1", "body": "hello"});
        let bytes = encode_ws_event("sendMessage-room1", data.clone());
        let (event, decoded) = decode_ws_event(&bytes).expect("decodes");
        assert_eq!(event, "sendMessage-room1");
        assert_eq!(decoded, data);
    }

    #[tokio::test]
    async fn broadcast_to_empty_room_is_noop() {
        let ws = test_ws();
        // No members → zero delivered, no panic.
        let delivered = ws
            .broadcast_to_room(
                "ghost-room",
                "sendMessage-ghost-room",
                &RoomMessage {
                    message_id: "m1".into(),
                    sender: "s".into(),
                    recipient: "r".into(),
                    message_box: "inbox".into(),
                    body: "{}".into(),
                    created_at: "t".into(),
                    updated_at: "t".into(),
                },
            )
            .await;
        assert_eq!(delivered, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ensure_peer_is_race_safe_under_concurrency() {
        // The refactor's explicit claim: concurrent first-touch of a fresh
        // socket yields ONE shared handle (entry().or_insert()). Hammer it.
        let ws = Arc::new(test_ws());
        let mut handles = Vec::new();
        for _ in 0..16 {
            let ws = ws.clone();
            handles.push(tokio::spawn(async move {
                let p = ws.ensure_peer("sock1").expect("peer");
                Arc::as_ptr(&p) as usize
            }));
        }
        let mut ptrs = Vec::new();
        for h in handles {
            ptrs.push(h.await.unwrap());
        }
        assert!(
            ptrs.iter().all(|p| *p == ptrs[0]),
            "all concurrent ensure_peer calls must observe the same handle"
        );
    }

    #[tokio::test]
    async fn broadcast_skips_member_without_authenticated_session() {
        // A socket can be a room member with a Peer but no completed handshake.
        // `create_general_message` must fail closed (no session) and broadcast
        // must skip it gracefully rather than panic or initiate a handshake.
        let ws = test_ws();
        ws.ensure_peer("sock1").expect("peer");
        ws.set_identity("sock1", "02".to_string() + &"ab".repeat(32)); // 66-char compressed-pubkey shape
        ws.add_room_member("keyA-inbox", "sock1");

        let delivered = ws
            .broadcast_to_room(
                "keyA-inbox",
                "sendMessage-keyA-inbox",
                &RoomMessage {
                    message_id: "m1".into(),
                    sender: "s".into(),
                    recipient: "r".into(),
                    message_box: "inbox".into(),
                    body: "{}".into(),
                    created_at: "t".into(),
                    updated_at: "t".into(),
                },
            )
            .await;
        // No authenticated session → create_general_message fails closed → 0
        // delivered, peer untouched.
        assert_eq!(delivered, 0);
        assert!(ws.get_peer("sock1").is_some());
    }
}
