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
use messagebox_server::config::{MpcRelayConfig, WalletRelayConfig};
use messagebox_server::ws::{self, RoomMessage, WsBroadcast};

use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
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
    // Pin Redis 7: the directed-routing backplane uses RESP3 push (HELLO), which
    // needs Redis 6+. The module's default image predates it. Real managed Redis
    // (ElastiCache/Upstash/Redis Cloud) is all 6+.
    let container = Redis::default()
        .with_tag("7-alpine")
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

    wait_until("instance A connected to Redis", || bp_a.is_subscribed()).await;

    let recipient = identity_of(CLIENT_KEY).await;
    let room = format!("{recipient}-mpc_inbox");
    let (client, mut msg_rx) = connect_and_join(&url_a, &room).await;

    // Directed routing: A subscribes to THIS room's channel only after the
    // client joins. Wait for that confirmed subscription before B publishes
    // (pub/sub has no replay; a pre-subscription publish would only hit the
    // mailbox, which this test doesn't exercise).
    wait_until("A subscribed to the room channel", || {
        bp_a.is_room_active(&room)
    })
    .await;

    // B broadcasts (as its HTTP /sendMessage handler would, post-gating). B does
    // NOT own the room, so it is not subscribed to the room channel and never
    // sees its own publish — directed routing, no fan-out to non-owners.
    assert!(!bp_b.is_room_active(&room));
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
    wait_until("connected to Redis", || bp.is_subscribed()).await;

    let recipient = identity_of(CLIENT_KEY).await;
    let room = format!("{recipient}-mpc_inbox");
    let (client, mut msg_rx) = connect_and_join(&url_a, &room).await;
    // Owner IS subscribed to its own room channel here, so the own-origin skip
    // is actually exercised (the envelope round-trips back to this instance).
    wait_until("subscribed to the room channel", || {
        bp.is_room_active(&room)
    })
    .await;

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
    // per-instance registry there, and B subscribes to the room channel.
    let (client_on_b, mut rx_b) = connect_and_join(&url_b, &room).await;
    wait_until("B subscribed to the room channel", || {
        bp_b.is_room_active(&room)
    })
    .await;

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
        .subscribe(messagebox_server::backplane::room_channel("03cc-mpc_inbox"))
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

/// Losing the delivery consumer must flip readiness truthfully: the
/// subscriber notices the dropped receiver, unwinds, and reports
/// not-subscribed with no active room subscriptions — a correctly-failing
/// instance the LB deregisters, never one that keeps claiming `redis: ok`
/// while every cross-instance push to it is lost.
#[tokio::test]
async fn dropped_delivery_consumer_flips_subscribed_off() {
    let (_redis, url) = redis_container().await;
    let bp = Backplane::new(&url);
    wait_until("subscribed", || bp.is_subscribed()).await;
    bp.on_room_join("sock1", "03aa-mpc_inbox");
    wait_until("room channel active", || {
        bp.is_room_active("03aa-mpc_inbox")
    })
    .await;

    // No WsBroadcast is attached in this test, so the raw stream stands in
    // for the delivery task; dropping it is the consumer-death scenario. The
    // flip must not need any inbound traffic to be noticed.
    drop(bp.take_delivery_rx().expect("delivery stream"));

    wait_until("subscribed flipped off", || !bp.is_subscribed()).await;
    assert!(!bp.is_room_active("03aa-mpc_inbox"));
    assert_eq!(bp.active_subscription_count(), 0);
}

/// Server-side pin for the keepalive room re-assert, hand-ported from
/// authsocket 0.1.1's `server_io::attach` into this server's fork of it
/// (`ws::setup_handlers`). Upstream's own
/// `keepalive_reasserts_lost_room_membership` proves nothing about the fork —
/// upstream's copy of that code never runs here — so the behaviour needs its
/// own test driven through OUR handler with a real 0.1.2 client.
///
/// Model A half: server-side membership loss (the skew a deploy or a dropped
/// join produces) must heal within one keepalive interval, with no reconnect.
#[tokio::test]
async fn keepalive_reasserts_lost_room_membership_through_the_fork() {
    let (url, ws) = boot_instance(SERVER_KEY_A, None).await;
    let core = ws.authsocket_core();
    let identity = identity_of(CLIENT_KEY).await;
    let room = format!("{identity}-test_inbox");
    let (client, _rx) = connect_and_join(&url, &room).await;

    // connect_and_join confirmed the signed joinedRoom reply, so the membership
    // is registered; grab the socket id it registered under.
    let members = core.room_members(&room);
    assert_eq!(members.len(), 1, "exactly one member after join");
    let sid = members[0].clone();

    // Server-side membership loss.
    core.leave_room(&sid, &room);
    assert!(
        core.room_members(&room).is_empty(),
        "membership force-dropped"
    );

    // The next keepalive probe (2s cadence + processing) must re-assert it.
    let deadline = std::time::Instant::now() + Duration::from_secs(6);
    loop {
        if core.room_members(&room).contains(&sid) {
            break; // healed through the fork's authenticated handler
        }
        assert!(
            std::time::Instant::now() < deadline,
            "keepalive did not re-assert room membership within 6s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    client.disconnect().await.expect("disconnect");
}

/// Model B half of the same port: healing `core` membership alone leaves the
/// backplane route dead, so cross-instance deliveries still cannot reach the
/// socket — the silent half of the skew. The re-assert must pair
/// `core.join_room` with `route_join`, exactly like the joinRoom arm. Drop
/// BOTH halves (what leaveRoom does) and require BOTH healed.
#[tokio::test]
async fn keepalive_reassert_heals_the_backplane_route_too() {
    let (_redis, redis_url) = redis_container().await;
    let bp = Backplane::new(&redis_url);
    wait_until("subscribed", || bp.is_subscribed()).await;
    let (url, ws) = boot_instance(SERVER_KEY_A, Some(bp.clone())).await;
    let core = ws.authsocket_core();
    let identity = identity_of(CLIENT_KEY).await;
    let room = format!("{identity}-test_inbox");
    let (client, _rx) = connect_and_join(&url, &room).await;
    wait_until("route registered on join", || bp.is_room_active(&room)).await;

    let members = core.room_members(&room);
    assert_eq!(members.len(), 1, "exactly one member after join");
    let sid = members[0].clone();

    // The deploy-skew drop takes both halves, as leaveRoom does. The core half
    // is synchronous (assert immediately); the route half unsubscribes
    // asynchronously AND the 2s-cadence keepalive may heal it before the
    // inactive state is ever observable — so accept either observation. Core
    // membership was provably dropped, so a restored member proves the
    // re-assert ran, and route_join rides the same code path.
    core.leave_room(&sid, &room);
    bp.on_room_leave(&sid, &room);
    assert!(core.room_members(&room).is_empty(), "membership dropped");

    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    let mut saw_route_drop = false;
    loop {
        if !bp.is_room_active(&room) {
            saw_route_drop = true;
        }
        if core.room_members(&room).contains(&sid) && bp.is_room_active(&room) {
            break; // both halves live again through the fork's re-assert
        }
        assert!(
            std::time::Instant::now() < deadline,
            "keepalive did not heal core membership + backplane route within 8s \
             (saw_route_drop={saw_route_drop})"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    client.disconnect().await.expect("disconnect");
}

/// The re-assert must never authorize a room `joinRoom` itself refuses.
///
/// Upstream authsocket 0.1.2's re-assert uses a BARE `room_id.starts_with(key)`
/// own-room test; this fork's joinRoom arm deliberately hardened that to the
/// delimiter-anchored `{key}` / `{key}-…` form, and the port keeps the hardened
/// rule. This test drives the difference through a real client: `{identity}X-…`
/// passes a bare prefix test and fails the anchored one.
///
/// The 0.1.2 client records a room in its local snapshot on `join_room`
/// regardless of whether the server accepted it, so the refused room really
/// does ride every subsequent keepalive probe — the keepalive is a live second
/// door into membership, and it must be no wider than the first.
#[tokio::test]
async fn keepalive_reassert_refuses_a_room_join_room_refused() {
    let (url, ws) = boot_instance(SERVER_KEY_A, None).await;
    let core = ws.authsocket_core();
    let identity = identity_of(CLIENT_KEY).await;
    let wallet = ProtoWallet::new(PrivateKey::from_hex(CLIENT_KEY).expect("key"));
    let client = AuthSocketClient::connect(&url, &identity, wallet)
        .await
        .expect("connect + BRC-103 handshake");

    // Not our room: it merely BEGINS with our identity key.
    let foreign = format!("{identity}X-test_inbox");
    let (failed_tx, mut failed_rx) = mpsc::unbounded_channel::<Value>();
    client
        .on(
            "joinFailed",
            Arc::new(move |data| {
                let _ = failed_tx.send(data);
            }),
        )
        .await;
    client.join_room(&foreign).await.expect("emit joinRoom");
    tokio::time::timeout(Duration::from_secs(10), failed_rx.recv())
        .await
        .expect("joinFailed within 10s")
        .expect("joinFailed data");
    assert!(
        core.room_members(&foreign).is_empty(),
        "joinRoom must refuse a room that only prefixes the identity"
    );

    // Now let several keepalive probes land (2s cadence). Each one carries the
    // refused room in the client's snapshot; none may install it.
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(
        core.room_members(&foreign).is_empty(),
        "keepalive re-assert admitted a room joinRoom refused — the own-room \
         rule is a bare prefix test, not the delimiter-anchored form"
    );

    client.disconnect().await.expect("disconnect");
}
