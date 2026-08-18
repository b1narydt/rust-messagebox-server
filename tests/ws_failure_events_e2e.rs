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
//! The failure-event tests run Model A with a lazy (unreachable) MySQL pool —
//! only the live push is asserted, no Docker. The two tests that exercise the
//! WS recipient-block gate (`ws_send_message_delivers_and_acks` and
//! `ws_send_to_blocked_recipient_is_rejected`) need a real MySQL (the block
//! check reads `message_permissions`), so they spin up a testcontainer.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::mpsc;

use authsocket::client::AuthSocketClient;
use bsv::primitives::private_key::PrivateKey;
use bsv::wallet::interfaces::{GetPublicKeyArgs, WalletInterface};
use bsv::wallet::proto_wallet::ProtoWallet;

use messagebox_server::config::{MpcRelayConfig, WalletRelayConfig};
use messagebox_server::db::DbPool;
use messagebox_server::ws::{self, WsBroadcast};

const SERVER_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000031";
const CLIENT_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000032";
const RECIPIENT_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000033";

// --- Real-MySQL harness for the block-gate tests (shared container per binary) ---

use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::mysql::Mysql;
use tokio::sync::OnceCell;

static MYSQL: OnceCell<(ContainerAsync<Mysql>, String)> = OnceCell::const_new();

async fn mysql_base_url() -> &'static str {
    let (_c, url) = MYSQL
        .get_or_init(|| async {
            let c = Mysql::default()
                .start()
                .await
                .expect("start MySQL testcontainer (Docker required)");
            let port = c.get_host_port_ipv4(3306).await.expect("mysql port");
            (c, format!("mysql://root@127.0.0.1:{port}"))
        })
        .await;
    url
}

/// A fresh per-test database with the full migration chain applied.
async fn migrated_pool() -> DbPool {
    use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
    use sqlx::{ConnectOptions, Executor};
    use std::str::FromStr;

    let base = mysql_base_url().await;
    let db_name = format!("wsblock_{}", uuid::Uuid::new_v4().simple());
    let mut root = MySqlConnectOptions::from_str(base)
        .expect("base url")
        .disable_statement_logging()
        .connect()
        .await
        .expect("connect mysql root");
    root.execute(format!("CREATE DATABASE `{db_name}`").as_str())
        .await
        .expect("create per-test db");
    drop(root);

    let pool = MySqlPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&format!("{base}/{db_name}"))
        .await
        .expect("connect per-test pool");
    messagebox_server::db::migrate(&pool)
        .await
        .expect("run migrations");
    pool
}

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

/// Boot one MBS WS stack exactly like `main.rs` (Model A, no backplane) on the
/// given DB pool.
async fn boot_with(pool: DbPool) -> (String, WsBroadcast) {
    let (layer, io) = socketioxide::SocketIo::new_layer();
    let ws = WsBroadcast::new(
        io.clone(),
        SERVER_KEY.to_string(),
        pool,
        None,
        messagebox_server::ops::OpsState::new(0),
        MpcRelayConfig::default(),
        WalletRelayConfig::default(),
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

/// Boot with a lazy (unreachable) pool — for the failure-event tests that never
/// reach the DB-backed sendMessage path. No Docker.
async fn boot_instance() -> (String, WsBroadcast) {
    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .acquire_timeout(Duration::from_millis(200))
        .connect_lazy("mysql://unused@127.0.0.1/unused")
        .expect("lazy pool");
    boot_with(pool).await
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
    let (url, _ws) = boot_with(migrated_pool().await).await;
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

/// The recipient-block gate is enforced over WebSocket (not just HTTP): when the
/// recipient has blocked the sender (`recipient_fee == -1`), a WS `sendMessage`
/// gets a signed `messageFailed` and is NEITHER delivered live NOR acked. This
/// covers the "route around the block over WebSocket" vector directly, and the
/// `Ok(-1)` branch of the block check (the fail-closed error path is covered by
/// the lazy-pool failure-event tests). Needs a real MySQL to hold the permission.
#[tokio::test]
async fn ws_send_to_blocked_recipient_is_rejected() {
    let pool = migrated_pool().await;

    let sender = identity_of(CLIENT_KEY).await; // the connecting client
    let recipient = identity_of(RECIPIENT_KEY).await; // who blocked the sender
    let room = format!("{recipient}-inbox");

    // Recipient blocks this specific sender: recipient_fee = -1.
    messagebox_server::db::queries::set_message_permission(
        &pool,
        &recipient,
        Some(&sender),
        "inbox",
        -1,
    )
    .await
    .expect("seed block permission");

    let (url, _ws) = boot_with(pool).await;

    // Listen for the failure event and the (must-not-arrive) ack + delivery.
    let (client, mut failed_rx) = connect_with_listener(&url, "messageFailed").await;
    let (ack_tx, mut ack_rx) = mpsc::unbounded_channel::<Value>();
    client
        .on(
            format!("sendMessageAck-{room}"),
            Arc::new(move |data| {
                let _ = ack_tx.send(data);
            }),
        )
        .await;
    let (delivery_tx, mut delivery_rx) = mpsc::unbounded_channel::<Value>();
    client
        .on(
            format!("sendMessage-{room}"),
            Arc::new(move |data| {
                let _ = delivery_tx.send(data);
            }),
        )
        .await;

    // The sender does NOT need to join the recipient's room to send to it.
    client
        .emit(
            "sendMessage",
            &json!({
                "roomId": room,
                "message": {
                    "messageId": "ws-blocked-1",
                    "recipient": recipient,
                    "body": "should be blocked"
                }
            }),
        )
        .await
        .expect("emit sendMessage");

    // A signed messageFailed with the block reason.
    let failed = recv_within(&mut failed_rx, "messageFailed").await;
    assert_eq!(
        failed.get("reason").and_then(Value::as_str),
        Some("Delivery blocked by recipient"),
        "blocked WS send must be rejected with the block reason: {failed}"
    );

    // And NO ack and NO live delivery.
    assert!(
        tokio::time::timeout(Duration::from_millis(750), ack_rx.recv())
            .await
            .is_err(),
        "a blocked send must NOT be acked"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), delivery_rx.recv())
            .await
            .is_err(),
        "a blocked send must NOT be delivered live"
    );

    client.disconnect().await.expect("disconnect");
}

/// Regression: a blocked sender must NOT be able to bypass the recipient-block
/// gate by pointing `roomId` at the victim while naming a DIFFERENT `recipient`.
///
/// The block check keys on the payload `recipient`, but the live broadcast
/// targets `roomId`. If those two client-supplied fields are allowed to diverge,
/// a sender blocked by victim V sends `{roomId: "<V>-inbox", recipient: "<self>"}`:
/// the block lookup asks "has <self> blocked <self>?" (no) and the push still
/// lands in `<V>-inbox`. The server now rejects any roomId/recipient identity
/// mismatch before the broadcast, so the send is failed and never delivered.
#[tokio::test]
async fn ws_send_with_mismatched_room_and_recipient_is_rejected() {
    let pool = migrated_pool().await;

    let sender = identity_of(CLIENT_KEY).await; // the connecting (blocked) client
    let victim = identity_of(RECIPIENT_KEY).await; // who blocked the sender
    let victim_room = format!("{victim}-inbox");

    // Victim blocks this sender for inbox.
    messagebox_server::db::queries::set_message_permission(
        &pool,
        &victim,
        Some(&sender),
        "inbox",
        -1,
    )
    .await
    .expect("seed block permission");

    let (url, _ws) = boot_with(pool).await;

    let (client, mut failed_rx) = connect_with_listener(&url, "messageFailed").await;
    let (ack_tx, mut ack_rx) = mpsc::unbounded_channel::<Value>();
    client
        .on(
            format!("sendMessageAck-{victim_room}"),
            Arc::new(move |data| {
                let _ = ack_tx.send(data);
            }),
        )
        .await;

    // The bypass attempt: target the victim's room, but name the sender itself
    // (unblocked) as `recipient` so the old block check would pass.
    client
        .emit(
            "sendMessage",
            &json!({
                "roomId": victim_room,
                "message": {
                    "messageId": "ws-bypass-1",
                    "recipient": sender,
                    "body": "should never reach the victim"
                }
            }),
        )
        .await
        .expect("emit sendMessage");

    let failed = recv_within(&mut failed_rx, "messageFailed").await;
    assert_eq!(
        failed.get("reason").and_then(Value::as_str),
        Some("roomId does not match recipient"),
        "a roomId/recipient mismatch must be rejected before broadcast: {failed}"
    );

    // No ack for the victim's room ⇒ nothing was broadcast there.
    assert!(
        tokio::time::timeout(Duration::from_millis(750), ack_rx.recv())
            .await
            .is_err(),
        "a mismatched send must NOT be acked"
    );

    client.disconnect().await.expect("disconnect");
}

/// An oversized `messageBox` must be REJECTED, not accepted and dead-lettered.
///
/// `messageBox` comes out of the client-supplied `roomId` and lands in a
/// VARCHAR(255). `ensure_message_box` inserts with `INSERT IGNORE` — which
/// truncates to 255 — and then selects the untruncated name, so the lookup
/// misses and the job is classified as a *permanent* failure. The whole
/// message, body included, is then appended to the dead-letter file while the
/// sender is acked `success` and nothing reaches MySQL: one socket can both
/// silently lose its own messages and fill the disk (measured at ~11 MB/s).
/// The edge check keeps that entire path out of reach.
#[tokio::test]
async fn ws_send_with_oversized_message_box_is_rejected_not_dead_lettered() {
    let pool = migrated_pool().await;
    let sender = identity_of(CLIENT_KEY).await;

    // The sender's own room, so the roomId/recipient bind is satisfied — this
    // needs no victim, which is what made the disk-fill trivially reachable.
    let huge_box = "b".repeat(300);
    let room = format!("{sender}-{huge_box}");

    let (url, _ws) = boot_with(pool.clone()).await;
    let (client, mut failed_rx) = connect_with_listener(&url, "messageFailed").await;
    let (ack_tx, mut ack_rx) = mpsc::unbounded_channel::<Value>();
    client
        .on(
            format!("sendMessageAck-{room}"),
            Arc::new(move |data| {
                let _ = ack_tx.send(data);
            }),
        )
        .await;

    client
        .emit(
            "sendMessage",
            &json!({
                "roomId": room,
                "message": {
                    "messageId": "ws-oversized-box-1",
                    "recipient": sender,
                    "body": "should never be accepted"
                }
            }),
        )
        .await
        .expect("emit sendMessage");

    let failed = recv_within(&mut failed_rx, "messageFailed").await;
    assert_eq!(
        failed.get("reason").and_then(Value::as_str),
        Some("Invalid messageBox"),
        "an oversized messageBox must be rejected at the edge: {failed}"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(750), ack_rx.recv())
            .await
            .is_err(),
        "a rejected send must NOT be acked as success"
    );

    client.disconnect().await.expect("disconnect");
}
