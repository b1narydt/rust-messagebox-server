//! Model B two-instance e2e — the Phase-2 test plan of
//! `mbs-enterprise-production-design` (cross-instance signed delivery,
//! sign-on-owner, registry-rebuild-on-reconnect, Redis-down degradation).
//!
//! Mirrors the authsocket crate's e2e harness: real socketioxide servers
//! behind axum on loopback TCP, the real `rust_socketio`-based
//! [`AuthSocketClient`], and a real Redis (testcontainer) between the
//! instances. The client's `on()` handlers fire only for **verified** BRC-103
//! general messages, so a received event proves the push was signed by the
//! instance owning the socket's Peer session.
//!
//! Requires Docker (Redis testcontainer), like the MySQL-backed handler/db
//! tests in this repo.

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::sync::mpsc;

use authsocket::client::AuthSocketClient;
use bsv::primitives::private_key::PrivateKey;
use bsv::wallet::interfaces::{GetPublicKeyArgs, WalletInterface};
use bsv::wallet::proto_wallet::ProtoWallet;

use messagebox_server::backplane::Backplane;
use messagebox_server::ws::{self, RoomMessage, WsBroadcast};

use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::redis::Redis;

const SERVER_KEY_A: &str = "0000000000000000000000000000000000000000000000000000000000000011";
const SERVER_KEY_B: &str = "0000000000000000000000000000000000000000000000000000000000000012";
const CLIENT_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000022";
const SENDER_KEY: &str = "02aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899";

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

/// Start a Redis container and return (container-keepalive, redis URL).
async fn redis_container() -> (ContainerAsync<Redis>, String) {
    let container = Redis::default()
        .start()
        .await
        .expect("start Redis testcontainer (Docker required)");
    let port = container
        .get_host_port_ipv4(6379)
        .await
        .expect("Redis host port");
    (container, format!("redis://127.0.0.1:{port}/"))
}

/// Boot one MBS "instance": the full authsocket WS stack (socketioxide behind
/// axum on an ephemeral loopback port) wired exactly like `main.rs`, with an
/// optional Model B backplane. The DB pool is lazy — these tests never touch
/// the broadcast-only paths that would query it.
async fn boot_instance(
    server_key_hex: &str,
    backplane: Option<Arc<Backplane>>,
) -> (String, WsBroadcast) {
    let (layer, io) = socketioxide::SocketIo::new_layer();
    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .connect_lazy("mysql://unused@127.0.0.1/unused")
        .expect("lazy pool");
    let ws = WsBroadcast::new(
        io.clone(),
        server_key_hex.to_string(),
        pool,
        backplane,
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

/// Connect the real authsocket client, complete the BRC-103 handshake, join
/// `room` (confirmed via the signed `joinedRoom` reply), and register a
/// receiver for the room's `sendMessage-{room}` deliveries.
async fn connect_and_join(
    url: &str,
    room: &str,
) -> (AuthSocketClient, mpsc::UnboundedReceiver<Value>) {
    let identity = identity_of(CLIENT_KEY).await;
    let wallet = ProtoWallet::new(PrivateKey::from_hex(CLIENT_KEY).expect("key"));
    let client = AuthSocketClient::connect(url, &identity, wallet)
        .await
        .expect("connect + BRC-103 handshake");

    let (msg_tx, msg_rx) = mpsc::unbounded_channel::<Value>();
    client
        .on(
            format!("sendMessage-{room}"),
            Arc::new(move |data| {
                let _ = msg_tx.send(data);
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
    client.join_room(room).await.expect("join_room");
    let joined = tokio::time::timeout(Duration::from_secs(10), joined_rx.recv())
        .await
        .expect("joinedRoom within 10s")
        .expect("joinedRoom data");
    assert_eq!(joined.get("roomId").and_then(Value::as_str), Some(room));

    (client, msg_rx)
}

fn room_message(message_id: &str, recipient: &str, message_box: &str) -> RoomMessage {
    RoomMessage {
        message_id: message_id.to_string(),
        sender: SENDER_KEY.to_string(),
        recipient: recipient.to_string(),
        message_box: message_box.to_string(),
        body: "{\"message\":\"encrypted-ceremony-frame\"}".to_string(),
        created_at: "2026-07-17T00:00:00.000Z".to_string(),
        updated_at: "2026-07-17T00:00:00.000Z".to_string(),
    }
}

async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while !cond() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Cross-instance delivery + sign-on-owner: a message broadcast on instance B
/// (which does NOT own the recipient's socket) reaches the recipient
/// connected to instance A as a SIGNED push — signed by A, the only instance
/// holding the socket's Peer session. B itself signs/delivers zero frames.
#[tokio::test]
async fn cross_instance_delivery_is_signed_by_the_owner() {
    let (_redis, url) = redis_container().await;
    let bp_a = Backplane::new(&url);
    let bp_b = Backplane::new(&url);
    let (url_a, _ws_a) = boot_instance(SERVER_KEY_A, Some(bp_a.clone())).await;
    let (_url_b, ws_b) = boot_instance(SERVER_KEY_B, Some(bp_b.clone())).await;

    // Pub/sub has no replay: A must hold its subscription before B publishes.
    wait_until("instance A subscribed", || bp_a.is_subscribed()).await;

    let recipient = identity_of(CLIENT_KEY).await;
    let room = format!("{recipient}-mpc_inbox");
    let (client, mut msg_rx) = connect_and_join(&url_a, &room).await;

    // B broadcasts (as its HTTP /sendMessage handler would, post-gating).
    let msg = room_message("m-cross-1", &recipient, "mpc_inbox");
    let delivered_on_b = ws_b
        .broadcast_to_room(&room, &format!("sendMessage-{room}"), &msg)
        .await;
    assert_eq!(
        delivered_on_b, 0,
        "sign-on-owner: the non-owner instance has no Peer session for the recipient and must sign/deliver nothing locally"
    );

    // The recipient receives the push via A. The client's `on` handlers fire
    // only for VERIFIED general messages, so this proves a signed frame from
    // the connection-owning instance.
    let got = tokio::time::timeout(Duration::from_secs(15), msg_rx.recv())
        .await
        .expect("cross-instance signed push within 15s")
        .expect("delivery payload");
    assert_eq!(
        got.get("messageId").and_then(Value::as_str),
        Some("m-cross-1")
    );
    assert_eq!(
        got.get("recipient").and_then(Value::as_str),
        Some(recipient.as_str())
    );

    wait_until("B counted the publish", || bp_b.published() >= 1).await;
    assert_eq!(bp_b.dropped(), 0);

    client.disconnect().await.expect("disconnect");
}

/// The publishing instance must not double-deliver to its own members: its
/// local leg runs at publish time, and its subscriber skips own-origin
/// envelopes coming back off the channel.
#[tokio::test]
async fn own_origin_envelope_is_not_double_delivered() {
    let (_redis, url) = redis_container().await;
    let bp = Backplane::new(&url);
    let (url_a, ws_a) = boot_instance(SERVER_KEY_A, Some(bp.clone())).await;
    // Subscribed BEFORE publishing, so the skip logic is actually exercised.
    wait_until("subscribed", || bp.is_subscribed()).await;

    let recipient = identity_of(CLIENT_KEY).await;
    let room = format!("{recipient}-mpc_inbox");
    let (client, mut msg_rx) = connect_and_join(&url_a, &room).await;

    let msg = room_message("m-self-1", &recipient, "mpc_inbox");
    let delivered = ws_a
        .broadcast_to_room(&room, &format!("sendMessage-{room}"), &msg)
        .await;
    assert_eq!(delivered, 1, "local leg delivers to the local member");

    // Exactly one delivery: the local one. Wait for the envelope to have
    // round-tripped (published) plus a settle window, then assert no second.
    wait_until("publish counted", || bp.published() >= 1).await;
    let first = tokio::time::timeout(Duration::from_secs(10), msg_rx.recv())
        .await
        .expect("local push within 10s")
        .expect("payload");
    assert_eq!(
        first.get("messageId").and_then(Value::as_str),
        Some("m-self-1")
    );
    let second = tokio::time::timeout(Duration::from_millis(1500), msg_rx.recv()).await;
    assert!(
        second.is_err(),
        "own-origin envelope must be skipped — got a duplicate delivery: {second:?}"
    );

    client.disconnect().await.expect("disconnect");
}

/// Registry rebuilt on reconnect, cross-instance: the client moves from A to
/// B (disconnect + re-handshake + re-join — what a sticky-LB failover does).
/// A subsequent broadcast entering at A must reach the client via B.
#[tokio::test]
async fn reconnect_to_another_instance_moves_delivery_ownership() {
    let (_redis, url) = redis_container().await;
    let bp_a = Backplane::new(&url);
    let bp_b = Backplane::new(&url);
    let (url_a, ws_a) = boot_instance(SERVER_KEY_A, Some(bp_a.clone())).await;
    let (url_b, _ws_b) = boot_instance(SERVER_KEY_B, Some(bp_b.clone())).await;
    wait_until("instance B subscribed", || bp_b.is_subscribed()).await;

    let recipient = identity_of(CLIENT_KEY).await;
    let room = format!("{recipient}-mpc_inbox");

    // Connected to A… then gone (instance drain / LB failover).
    let (client_on_a, _rx_a) = connect_and_join(&url_a, &room).await;
    client_on_a.disconnect().await.expect("disconnect from A");

    // Reconnect to B: fresh BRC-103 handshake + re-join rebuilds the
    // per-instance registry there.
    let (client_on_b, mut rx_b) = connect_and_join(&url_b, &room).await;

    // A message entering at A (which no longer owns any socket for the room).
    let msg = room_message("m-move-1", &recipient, "mpc_inbox");
    let delivered_on_a = ws_a
        .broadcast_to_room(&room, &format!("sendMessage-{room}"), &msg)
        .await;
    assert_eq!(delivered_on_a, 0, "A no longer owns the recipient's socket");

    let got = tokio::time::timeout(Duration::from_secs(15), rx_b.recv())
        .await
        .expect("push via B within 15s")
        .expect("payload");
    assert_eq!(
        got.get("messageId").and_then(Value::as_str),
        Some("m-move-1")
    );

    client_on_b.disconnect().await.expect("disconnect");
}

/// What crosses Redis is the UNSIGNED RoomMessage in the envelope — no
/// signature material on the wire (a raw pub/sub observer sees exactly the
/// plain payload; signing happens per-recipient on the owner instance).
#[tokio::test]
async fn wire_carries_unsigned_room_message() {
    let (_redis, url) = redis_container().await;

    // Raw observer subscribed directly to the backplane channel.
    let client = redis::Client::open(url.as_str()).expect("redis client");
    let mut pubsub = client.get_async_pubsub().await.expect("pubsub");
    pubsub
        .subscribe(messagebox_server::backplane::CHANNEL)
        .await
        .expect("subscribe");

    let bp = Backplane::new(&url);
    let msg = room_message("m-wire-1", "03cc", "mpc_inbox");
    // Publisher connects lazily; retry until the observer sees the frame.
    let payload: String = {
        use futures_util::StreamExt;
        let mut stream = pubsub.into_on_message();
        let mut attempt = 0;
        loop {
            bp.publish("03cc-mpc_inbox", "sendMessage-03cc-mpc_inbox", &msg);
            match tokio::time::timeout(Duration::from_secs(2), stream.next()).await {
                Ok(Some(m)) => break m.get_payload().expect("utf8 payload"),
                _ => {
                    attempt += 1;
                    assert!(attempt < 10, "no frame observed on the channel");
                }
            }
        }
    };

    let v: Value = serde_json::from_str(&payload).expect("envelope JSON");
    assert_eq!(v["origin"], bp.instance_id());
    assert_eq!(v["roomId"], "03cc-mpc_inbox");
    assert_eq!(v["event"], "sendMessage-03cc-mpc_inbox");
    let carried = v["message"].as_object().expect("message object");
    assert_eq!(
        carried.len(),
        7,
        "exactly the RoomMessage fields, nothing else"
    );
    assert_eq!(carried["messageId"], "m-wire-1");
    for key in ["signature", "nonce", "yourNonce", "identityKey", "payload"] {
        assert!(
            !carried.contains_key(key),
            "the wire payload must be UNSIGNED — found {key}"
        );
    }
}

/// Redis down in Model B: live local delivery still works, the send path
/// neither fails nor blocks, and the degradation is observable (dropped
/// counter). Cross-instance recipients fall back to the durable mailbox
/// (persist + HTTP /listMessages) — covered by the handler/db suites.
#[tokio::test]
async fn redis_down_degrades_to_local_delivery_without_failing() {
    // Nothing listens on port 1: connection refused, no Redis at all.
    let bp = Backplane::new("redis://127.0.0.1:1/");
    let (url_a, ws_a) = boot_instance(SERVER_KEY_A, Some(bp.clone())).await;

    let recipient = identity_of(CLIENT_KEY).await;
    let room = format!("{recipient}-mpc_inbox");
    let (client, mut msg_rx) = connect_and_join(&url_a, &room).await;

    let msg = room_message("m-down-1", &recipient, "mpc_inbox");
    let start = std::time::Instant::now();
    let delivered = ws_a
        .broadcast_to_room(&room, &format!("sendMessage-{room}"), &msg)
        .await;
    assert_eq!(
        delivered, 1,
        "local signed delivery must survive Redis loss"
    );
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "broadcast must not block on a dead Redis"
    );

    let got = tokio::time::timeout(Duration::from_secs(10), msg_rx.recv())
        .await
        .expect("local push within 10s")
        .expect("payload");
    assert_eq!(
        got.get("messageId").and_then(Value::as_str),
        Some("m-down-1")
    );

    // Degradation is visible, not silent.
    wait_until("drop counted", || bp.dropped() >= 1).await;
    assert_eq!(bp.published(), 0);
    assert!(!bp.is_subscribed());

    client.disconnect().await.expect("disconnect");
}
