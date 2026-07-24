//! WS failure-event e2e (parity audit W2/W3/W5 + the forked adapter wiring).
//!
//! Real socketioxide server behind axum on loopback + the real
//! `rust_socketio`-based [`AuthSocketClient`]. The client's `on()` handlers
//! fire only for **verified** BRC-103 general messages, so every event
//! asserted here — including the failure events — arrived SIGNED by the
//! server (never a raw Socket.IO emit).
//!
//! Also regression-proves the forked `setup_handlers` wiring end-to-end:
//! handshake, joinRoom → joinedRoom, and the WS `sendMessage` verb
//! (live push + signed ack), which the backplane e2e does not exercise.
//!
//! No Docker needed — the WS sendMessage happy path uses a lazy MySQL pool
//! whose async persist simply dead-letters; only the live push is asserted.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::mpsc;

use authsocket::client::AuthSocketClient;
use bsv::primitives::private_key::PrivateKey;
use bsv::wallet::interfaces::{GetPublicKeyArgs, WalletInterface};
use bsv::wallet::proto_wallet::ProtoWallet;

use messagebox_server::ws::{self, WsBroadcast};

const SERVER_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000031";
const CLIENT_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000032";

async fn identity_of(key_hex: &str) -> String {
    let w = ProtoWallet::new(PrivateKey::from_hex(key_hex).expect("key"));
    w.get_public_key(
        GetPublicKeyArgs {
            identity_key: true,
            protocol_id: None,
            key_id: None,
            counterparty: None,
            privileged: false,
            privileged_reason: None,
            for_self: None,
            seek_permission: None,
        },
        None,
    )
    .await
    .expect("identity")
    .public_key
    .to_der_hex()
}

/// Boot one MBS WS stack exactly like `main.rs` (Model A, no backplane).
async fn boot_instance() -> (String, WsBroadcast) {
    let (layer, io) = socketioxide::SocketIo::new_layer();
    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .connect_lazy("mysql://unused@127.0.0.1/unused")
        .expect("lazy pool");
    let ws = WsBroadcast::new(
        io.clone(),
        SERVER_KEY.to_string(),
        pool,
        None,
        messagebox_server::ops::OpsState::new(0),
    );
    ws::setup_handlers(&io, ws.clone());

    let app = axum::Router::new().layer(layer);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    (format!("http://{addr}"), ws)
}

/// Connect + complete the BRC-103 handshake, and wire a receiver for `event`.
async fn connect_with_listener(
    url: &str,
    event: &str,
) -> (AuthSocketClient, mpsc::UnboundedReceiver<Value>) {
    let identity = identity_of(CLIENT_KEY).await;
    let wallet = ProtoWallet::new(PrivateKey::from_hex(CLIENT_KEY).expect("key"));
    let client = AuthSocketClient::connect(url, &identity, wallet)
        .await
        .expect("connect + BRC-103 handshake");
    let (tx, rx) = mpsc::unbounded_channel::<Value>();
    client
        .on(
            event.to_string(),
            Arc::new(move |data| {
                let _ = tx.send(data);
            }),
        )
        .await;
    (client, rx)
}

async fn recv_within(rx: &mut mpsc::UnboundedReceiver<Value>, what: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
        .unwrap_or_else(|| panic!("channel closed waiting for {what}"))
}

/// W2: joining ANOTHER identity's room is rejected — and the rejection is now
/// a signed `joinFailed {reason}`, not silence. The own-room hardening itself
/// is unchanged (no `joinedRoom`, no membership).
#[tokio::test]
async fn join_other_room_emits_join_failed() {
    let (url, ws) = boot_instance().await;
    let (client, mut failed_rx) = connect_with_listener(&url, "joinFailed").await;

    let (joined_tx, mut joined_rx) = mpsc::unbounded_channel::<Value>();
    client
        .on(
            "joinedRoom",
            Arc::new(move |data| {
                let _ = joined_tx.send(data);
            }),
        )
        .await;

    // A different identity's room (66-hex key prefix that is not the client's).
    let other_room = "02aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899-inbox";
    client
        .emit("joinRoom", &json!(other_room))
        .await
        .expect("emit joinRoom");

    let failed = recv_within(&mut failed_rx, "joinFailed").await;
    assert!(
        failed
            .get("reason")
            .and_then(Value::as_str)
            .is_some_and(|r| !r.is_empty()),
        "joinFailed must carry a reason: {failed}"
    );

    // The hardening held: no joinedRoom, and a broadcast to that room finds
    // zero members (the join was never granted).
    assert!(
        tokio::time::timeout(Duration::from_millis(500), joined_rx.recv())
            .await
            .is_err(),
        "must NOT receive joinedRoom for another identity's room"
    );
    let delivered = ws
        .broadcast_to_room(
            other_room,
            &format!("sendMessage-{other_room}"),
            &messagebox_server::ws::RoomMessage {
                message_id: "probe-1".into(),
                sender: "s".into(),
                recipient: "r".into(),
                message_box: "inbox".into(),
                body: "{}".into(),
                created_at: "t".into(),
                updated_at: "t".into(),
            },
        )
        .await;
    assert_eq!(delivered, 0, "rejected join must not create membership");

    client.disconnect().await.expect("disconnect");
}

/// W3: leaveRoom with an empty room id → signed `leaveFailed {reason}`.
#[tokio::test]
async fn leave_empty_room_emits_leave_failed() {
    let (url, _ws) = boot_instance().await;
    let (client, mut failed_rx) = connect_with_listener(&url, "leaveFailed").await;

    client
        .emit("leaveRoom", &json!(""))
        .await
        .expect("emit leaveRoom");

    let failed = recv_within(&mut failed_rx, "leaveFailed").await;
    assert_eq!(failed["reason"], "Invalid room ID");

    client.disconnect().await.expect("disconnect");
}

/// W5: a malformed WS sendMessage (missing message object) → signed
/// `messageFailed {reason}` — the client sees WHY instead of an ack timeout.
#[tokio::test]
async fn malformed_send_emits_message_failed() {
    let (url, _ws) = boot_instance().await;
    let (client, mut failed_rx) = connect_with_listener(&url, "messageFailed").await;

    let identity = identity_of(CLIENT_KEY).await;
    let room = format!("{identity}-inbox");
    client
        .emit("sendMessage", &json!({ "roomId": room }))
        .await
        .expect("emit sendMessage");

    let failed = recv_within(&mut failed_rx, "messageFailed").await;
    assert_eq!(failed["reason"], "Missing message object");

    // Missing messageId variant.
    client
        .emit(
            "sendMessage",
            &json!({ "roomId": room, "message": { "recipient": identity, "body": "x" } }),
        )
        .await
        .expect("emit sendMessage");
    let failed = recv_within(&mut failed_rx, "messageFailed (missing messageId)").await;
    assert_eq!(failed["reason"], "Missing messageId");

    client.disconnect().await.expect("disconnect");
}

/// Happy-path regression for the forked wiring: handshake → join own room →
/// WS sendMessage → signed live delivery `sendMessage-{room}` + signed
/// `sendMessageAck-{room}` `{status:'success', messageId}` (TS W4 contract).
#[tokio::test]
async fn ws_send_message_delivers_and_acks() {
    let (url, _ws) = boot_instance().await;
    let identity = identity_of(CLIENT_KEY).await;
    let room = format!("{identity}-inbox");

    let (client, mut delivery_rx) =
        connect_with_listener(&url, &format!("sendMessage-{room}")).await;

    let (ack_tx, mut ack_rx) = mpsc::unbounded_channel::<Value>();
    client
        .on(
            format!("sendMessageAck-{room}"),
            Arc::new(move |data| {
                let _ = ack_tx.send(data);
            }),
        )
        .await;

    let (joined_tx, mut joined_rx) = mpsc::unbounded_channel::<Value>();
    client
        .on(
            "joinedRoom",
            Arc::new(move |data| {
                let _ = joined_tx.send(data);
            }),
        )
        .await;
    client.join_room(&room).await.expect("join_room");
    recv_within(&mut joined_rx, "joinedRoom").await;

    client
        .emit(
            "sendMessage",
            &json!({
                "roomId": room,
                "message": {
                    "messageId": "ws-e2e-1",
                    "recipient": identity,
                    "body": "hello over ws"
                }
            }),
        )
        .await
        .expect("emit sendMessage");

    let ack = recv_within(&mut ack_rx, "sendMessageAck").await;
    assert_eq!(ack["status"], "success");
    assert_eq!(ack["messageId"], "ws-e2e-1");

    let delivered = recv_within(&mut delivery_rx, "live delivery").await;
    assert_eq!(delivered["messageId"], "ws-e2e-1");
    assert_eq!(
        delivered["body"],
        json!({"message": "hello over ws"}).to_string(),
        "WS-stored/live body carries the canonical {{\"message\": ...}} wrap"
    );

    client.disconnect().await.expect("disconnect");
}
