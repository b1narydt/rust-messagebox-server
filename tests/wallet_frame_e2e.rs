//! Wallet-RPC relay e2e — the `walletCall` / `walletReply` lane end to end.
//!
//! The sibling of `mpc_envelope_e2e.rs`, and deliberately the same harness: a
//! real socketioxide server behind axum on loopback with real
//! `rust_socketio`-based [`AuthSocketClient`]s, each completing its own BRC-103
//! handshake. The parties here are an APP (a dashboard calling a vault's
//! BRC-100 surface) and a BOX (the enterprise wallet serving it).
//!
//! This lane is what makes a strictly dial-out box reachable. The box runs with
//! its own HTTP listener disabled and serves all 28 gated BRC-100 routes over
//! these two verbs; a hub that does not forward them leaves it unreachable by
//! anything.
//!
//! **Every frame asserted here arrived SIGNED.** The client's `on()` handlers
//! fire only for *verified* BRC-103 general messages, so a handler firing at all
//! is proof the server signed the relayed frame for that socket; a raw
//! Socket.IO emit would never reach it.
//!
//! **Nothing here can touch MySQL.** The whole file runs Model A against a lazy
//! pool pointed at an unreachable database (no Docker). A wallet call is a live
//! request↔response, not a durable room event, so a relay that reached for the
//! mailbox would stall or error on the first frame and every test below would
//! fail. Passing against an unreachable database IS the evidence for
//! non-persistence.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::mpsc;

use authsocket::client::AuthSocketClient;
use bsv::primitives::private_key::PrivateKey;
use bsv::wallet::interfaces::{GetPublicKeyArgs, WalletInterface};
use bsv::wallet::proto_wallet::ProtoWallet;

use messagebox_server::config::{MpcRelayConfig, WalletRelayConfig};
use messagebox_server::ws::{
    self, WsBroadcast, MPC_ENVELOPE_EVENT, MPC_INBOX, WALLET_CALL_EVENT, WALLET_INBOX,
    WALLET_REPLY_EVENT,
};

const SERVER_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000051";
/// The app calling a vault's BRC-100 surface — a NON-roster identity, admitted
/// only by `MPC_WALLET_APP_IDENTITIES`.
const APP_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000052";
/// The enterprise box serving that vault — a ROSTER identity, admitted by
/// `MPC_PEER_IDENTITIES` and never named as an app.
const BOX_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000053";
/// A third identity, never connected — used as the forged sender claim.
const OUTSIDER_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000054";

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

/// Boot one MBS WS stack exactly like `main.rs` (Model A, no backplane) with the
/// ceremony lane allowlisting `peers` and the wallet lane allowlisting `apps`,
/// on a lazy pool that can never connect.
///
/// The two lists are separate parameters because the two lanes are separately
/// gated: the isolation test below turns on ONE of them and shows the other lane
/// stays shut for that identity.
async fn boot_with(peers: &[String], apps: &[String]) -> String {
    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .acquire_timeout(Duration::from_millis(200))
        .connect_lazy("mysql://unused@127.0.0.1/unused")
        .expect("lazy pool");

    let (layer, io) = socketioxide::SocketIo::new_layer();
    let ws = WsBroadcast::new(
        io.clone(),
        SERVER_KEY.to_string(),
        pool,
        None,
        messagebox_server::ops::OpsState::new(0),
        MpcRelayConfig {
            peer_identities: peers.iter().cloned().collect(),
            ..Default::default()
        },
        WalletRelayConfig {
            app_identities: apps.iter().cloned().collect(),
            ..Default::default()
        },
    );
    ws::setup_handlers(&io, ws);

    let app = axum::Router::new().layer(layer);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    format!("http://{addr}")
}

/// Connect one party and complete its BRC-103 handshake.
async fn connect_as(url: &str, key_hex: &str) -> AuthSocketClient {
    let identity = identity_of(key_hex).await;
    let wallet = ProtoWallet::new(PrivateKey::from_hex(key_hex).expect("key"));
    AuthSocketClient::connect(url, &identity, wallet)
        .await
        .expect("connect + BRC-103 handshake")
}

/// Wire a receiver for `event` on an already-connected client.
async fn listen_for(client: &AuthSocketClient, event: &str) -> mpsc::UnboundedReceiver<Value> {
    let (tx, rx) = mpsc::unbounded_channel::<Value>();
    client
        .on(
            event.to_string(),
            Arc::new(move |data| {
                let _ = tx.send(data);
            }),
        )
        .await;
    rx
}

async fn recv_within(rx: &mut mpsc::UnboundedReceiver<Value>, what: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
        .unwrap_or_else(|| panic!("channel closed waiting for {what}"))
}

/// Join a room and wait for the signed `joinedRoom`, so a test never races the
/// join against the first relayed frame.
async fn join_and_confirm(client: &AuthSocketClient, room: &str) {
    let mut joined_rx = listen_for(client, "joinedRoom").await;
    client.join_room(room).await.expect("join room");
    recv_within(&mut joined_rx, "joinedRoom").await;
}

/// Join the wallet lane's presence room — the ONE room this lane has, and the
/// only room a wallet-lane party ever joins.
async fn join_wallet_inbox(client: &AuthSocketClient, identity: &str) {
    join_and_confirm(client, &format!("{identity}-{WALLET_INBOX}")).await;
}

/// THE test: a full wallet call over the real wire from an app that holds no
/// ceremony identity at all, into a box that joined only its wallet presence
/// room.
///
/// It pins the wire contract the two ends of the nested session parse:
/// `correlationId` in camelCase and `frame` verbatim, with `sender` re-stamped
/// from the BRC-103-verified socket identity. A drifted key here is a silent
/// total outage — the box's `from_value::<WalletFrame>` fails and every call
/// times out with nothing naming the cause.
#[tokio::test]
async fn wallet_call_relays_to_a_box_on_its_wallet_inbox() {
    let app_identity = identity_of(APP_KEY).await;
    let box_identity = identity_of(BOX_KEY).await;
    // The app is admitted ONLY as an app; the box ONLY as a ceremony peer.
    let url = boot_with(
        std::slice::from_ref(&box_identity),
        std::slice::from_ref(&app_identity),
    )
    .await;

    let enterprise_box = connect_as(&url, BOX_KEY).await;
    join_wallet_inbox(&enterprise_box, &box_identity).await;
    let mut inbound = listen_for(&enterprise_box, WALLET_CALL_EVENT).await;

    let app = connect_as(&url, APP_KEY).await;
    app.emit(
        WALLET_CALL_EVENT,
        &json!({
            "recipient": box_identity,
            "correlationId": "corr-e2e-1",
            "frame": "{\"nested\":\"BRC-103 AuthMessage over BRC-2 ciphertext\"}",
        }),
    )
    .await
    .expect("emit walletCall");

    let received = recv_within(&mut inbound, "relayed walletCall").await;

    assert!(
        received.get("correlationId").is_some(),
        "the correlation id must arrive under the camelCase key: {received}"
    );
    assert!(
        received.get("correlation_id").is_none(),
        "snake_case would break the box's parse: {received}"
    );
    assert_eq!(received["correlationId"], "corr-e2e-1");
    assert_eq!(received["recipient"], box_identity.as_str());
    assert_eq!(
        received["frame"], "{\"nested\":\"BRC-103 AuthMessage over BRC-2 ciphertext\"}",
        "the nested frame is forwarded verbatim — the relay never opens it"
    );
    assert_eq!(
        received["sender"],
        app_identity.as_str(),
        "the relay stamps the BRC-103-verified socket identity as the sender"
    );

    app.disconnect().await.expect("disconnect");
    enterprise_box.disconnect().await.expect("disconnect");
}

/// A `walletReply` arrives as a `walletReply`. The verb is the half of the
/// contract most likely to regress silently: an app dispatches inbound frames on
/// it, so a reply re-emitted as a `walletCall` reaches the socket, matches no
/// handler, and the call it answered dies of timeout with the relay logging a
/// successful delivery.
///
/// The reply travels the opposite direction from the call above — box → app,
/// under the box's ROSTER identity, into the app's own wallet inbox — so this
/// also exercises the union rule over the wire.
#[tokio::test]
async fn wallet_reply_arrives_as_a_reply_not_a_call() {
    let app_identity = identity_of(APP_KEY).await;
    let box_identity = identity_of(BOX_KEY).await;
    let url = boot_with(
        std::slice::from_ref(&box_identity),
        std::slice::from_ref(&app_identity),
    )
    .await;

    let app = connect_as(&url, APP_KEY).await;
    join_wallet_inbox(&app, &app_identity).await;
    let mut replies = listen_for(&app, WALLET_REPLY_EVENT).await;
    let mut calls = listen_for(&app, WALLET_CALL_EVENT).await;

    let enterprise_box = connect_as(&url, BOX_KEY).await;
    enterprise_box
        .emit(
            WALLET_REPLY_EVENT,
            &json!({
                "recipient": app_identity,
                "correlationId": "corr-e2e-1",
                "frame": "{\"nested\":\"signed walletResponse\"}",
            }),
        )
        .await
        .expect("emit walletReply");

    let received = recv_within(&mut replies, "relayed walletReply").await;
    assert_eq!(received["correlationId"], "corr-e2e-1");
    assert_eq!(received["frame"], "{\"nested\":\"signed walletResponse\"}");
    assert_eq!(
        received["sender"],
        box_identity.as_str(),
        "a box replies under its own roster identity — the union rule admits it"
    );

    assert!(
        tokio::time::timeout(Duration::from_millis(750), calls.recv())
            .await
            .is_err(),
        "the reply must NOT also arrive on the walletCall handler"
    );

    app.disconnect().await.expect("disconnect");
    enterprise_box.disconnect().await.expect("disconnect");
}

/// Lane isolation, the key security property: an identity admitted for the
/// wallet verbs can route NOTHING on the ceremony lane. The app is on
/// `MPC_WALLET_APP_IDENTITIES` and its wallet frame relays; the very same socket
/// then sends a well-formed `mpcEnvelope` to a recipient that is dialed in and
/// allowlisted, and nothing arrives.
///
/// This is the property the wire contract calls being "scoped to the wallet
/// verbs by construction"
/// (`rust-mpc/crates/transport/src/wallet_lane_wire.rs:82-85`).
#[tokio::test]
async fn a_wallet_app_is_refused_on_the_ceremony_lane() {
    let app_identity = identity_of(APP_KEY).await;
    let box_identity = identity_of(BOX_KEY).await;
    let url = boot_with(
        std::slice::from_ref(&box_identity),
        std::slice::from_ref(&app_identity),
    )
    .await;

    let enterprise_box = connect_as(&url, BOX_KEY).await;
    join_wallet_inbox(&enterprise_box, &box_identity).await;
    join_and_confirm(&enterprise_box, &format!("{box_identity}-{MPC_INBOX}")).await;
    let mut wallet_inbound = listen_for(&enterprise_box, WALLET_CALL_EVENT).await;
    let mut ceremony_inbound = listen_for(&enterprise_box, MPC_ENVELOPE_EVENT).await;

    let app = connect_as(&url, APP_KEY).await;

    // The lane the app IS allowlisted for carries its frame.
    app.emit(
        WALLET_CALL_EVENT,
        &json!({
            "recipient": box_identity,
            "correlationId": "corr-isolation-1",
            "frame": "{\"nested\":\"admitted\"}",
        }),
    )
    .await
    .expect("emit walletCall");
    let received = recv_within(&mut wallet_inbound, "relayed walletCall").await;
    assert_eq!(received["correlationId"], "corr-isolation-1");

    // The ceremony lane, from the same verified socket, carries nothing.
    app.emit(
        MPC_ENVELOPE_EVENT,
        &json!({
            "recipient": box_identity,
            "box": "mpc_somesession",
            "body": "BRC78-ciphertext",
            "messageId": "mpc-isolation-1",
        }),
    )
    .await
    .expect("emit mpcEnvelope");

    assert!(
        tokio::time::timeout(Duration::from_millis(750), ceremony_inbound.recv())
            .await
            .is_err(),
        "a wallet-lane app must never be able to route ceremony traffic"
    );

    app.disconnect().await.expect("disconnect");
    enterprise_box.disconnect().await.expect("disconnect");
}

/// A body `sender` claiming a third identity is forged provenance, and the frame
/// is dropped rather than re-stamped: the recipient receives NOTHING.
#[tokio::test]
async fn forged_body_sender_never_reaches_the_recipient() {
    let app_identity = identity_of(APP_KEY).await;
    let box_identity = identity_of(BOX_KEY).await;
    let outsider_identity = identity_of(OUTSIDER_KEY).await;
    let url = boot_with(
        std::slice::from_ref(&box_identity),
        std::slice::from_ref(&app_identity),
    )
    .await;

    let enterprise_box = connect_as(&url, BOX_KEY).await;
    join_wallet_inbox(&enterprise_box, &box_identity).await;
    let mut inbound = listen_for(&enterprise_box, WALLET_CALL_EVENT).await;

    let app = connect_as(&url, APP_KEY).await;
    app.emit(
        WALLET_CALL_EVENT,
        &json!({
            "recipient": box_identity,
            "sender": outsider_identity,
            "correlationId": "corr-forged-1",
            "frame": "{\"nested\":\"forged\"}",
        }),
    )
    .await
    .expect("emit walletCall");

    assert!(
        tokio::time::timeout(Duration::from_millis(750), inbound.recv())
            .await
            .is_err(),
        "a frame claiming someone else's provenance must not be relayed"
    );

    app.disconnect().await.expect("disconnect");
    enterprise_box.disconnect().await.expect("disconnect");
}

/// Admission is rooted in the VERIFIED socket identity: an app on neither
/// allowlist routes nothing, even with a perfectly formed frame and a recipient
/// that is allowlisted and dialed in.
#[tokio::test]
async fn un_allowlisted_wallet_sender_is_refused() {
    let box_identity = identity_of(BOX_KEY).await;
    // The recipient box is a ceremony peer; the app is deliberately on neither
    // list.
    let url = boot_with(std::slice::from_ref(&box_identity), &[]).await;

    let enterprise_box = connect_as(&url, BOX_KEY).await;
    join_wallet_inbox(&enterprise_box, &box_identity).await;
    let mut inbound = listen_for(&enterprise_box, WALLET_CALL_EVENT).await;

    let app = connect_as(&url, APP_KEY).await;
    app.emit(
        WALLET_CALL_EVENT,
        &json!({
            "recipient": box_identity,
            "correlationId": "corr-unlisted-1",
            "frame": "{\"nested\":\"unlisted\"}",
        }),
    )
    .await
    .expect("emit walletCall");

    assert!(
        tokio::time::timeout(Duration::from_millis(750), inbound.recv())
            .await
            .is_err(),
        "an un-allowlisted app must not reach a box's BRC-100 surface"
    );

    app.disconnect().await.expect("disconnect");
    enterprise_box.disconnect().await.expect("disconnect");
}

/// Both allowlists unset relays nothing, in either direction. A deployment that
/// opts into neither lane is not an open front door onto the boxes dialed into
/// it.
#[tokio::test]
async fn unconfigured_lane_relays_nothing() {
    let app_identity = identity_of(APP_KEY).await;
    let box_identity = identity_of(BOX_KEY).await;
    let url = boot_with(&[], &[]).await;

    let enterprise_box = connect_as(&url, BOX_KEY).await;
    join_wallet_inbox(&enterprise_box, &box_identity).await;
    let mut calls = listen_for(&enterprise_box, WALLET_CALL_EVENT).await;

    let app = connect_as(&url, APP_KEY).await;
    join_wallet_inbox(&app, &app_identity).await;
    let mut replies = listen_for(&app, WALLET_REPLY_EVENT).await;

    app.emit(
        WALLET_CALL_EVENT,
        &json!({
            "recipient": box_identity,
            "correlationId": "corr-lane-off-1",
            "frame": "{\"nested\":\"call\"}",
        }),
    )
    .await
    .expect("emit walletCall");
    enterprise_box
        .emit(
            WALLET_REPLY_EVENT,
            &json!({
                "recipient": app_identity,
                "correlationId": "corr-lane-off-1",
                "frame": "{\"nested\":\"reply\"}",
            }),
        )
        .await
        .expect("emit walletReply");

    assert!(
        tokio::time::timeout(Duration::from_millis(750), calls.recv())
            .await
            .is_err(),
        "the lane must be off until an operator names an app or a peer"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(750), replies.recv())
            .await
            .is_err(),
        "and off in the reply direction too"
    );

    app.disconnect().await.expect("disconnect");
    enterprise_box.disconnect().await.expect("disconnect");
}
