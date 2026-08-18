//! MPC ceremony relay e2e — the `mpcEnvelope` lane end to end.
//!
//! A real socketioxide server behind axum on loopback with TWO real
//! `rust_socketio`-based [`AuthSocketClient`]s: a sender (the enterprise box's
//! role) and a recipient (the cosigner's role), each completing its own BRC-103
//! handshake. This is the relay hop a live ceremony makes, exercised over the
//! actual wire rather than through the admission function.
//!
//! **Every frame asserted here arrived SIGNED.** The client's `on()` handlers
//! fire only for *verified* BRC-103 general messages, so a handler firing at all
//! is proof the server signed the relayed envelope for that socket; a raw
//! Socket.IO emit would never reach it. That is the security property of the
//! lane, not an incidental detail of the harness.
//!
//! **Nothing here can touch MySQL.** The whole file runs Model A against a lazy
//! pool pointed at an unreachable database (no Docker). The ceremony lane must
//! never persist — MPC envelopes are transient/delete-on-ack, not durable room
//! events — so a relay that reached for the mailbox would stall or error on the
//! first round and every test below would fail. Passing against an unreachable
//! database IS the evidence for non-persistence.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::mpsc;

use authsocket::client::AuthSocketClient;
use bsv::primitives::private_key::PrivateKey;
use bsv::wallet::interfaces::{GetPublicKeyArgs, WalletInterface};
use bsv::wallet::proto_wallet::ProtoWallet;

use messagebox_server::config::{MpcRelayConfig, WalletRelayConfig};
use messagebox_server::ws::{self, WsBroadcast, MPC_ENVELOPE_EVENT, MPC_INBOX};

const SERVER_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000041";
/// The sending ceremony party (the enterprise box's role).
const BOX_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000042";
/// The receiving ceremony party (the cosigner's role).
const COSIGNER_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000043";
/// A third identity, never connected — used as the forged sender claim.
const OUTSIDER_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000044";

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
/// ceremony lane allowlisting `peers`, on a lazy pool that can never connect.
async fn boot_with_peers(peers: &[String]) -> String {
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
        // The wallet lane's own allowlist stays empty here: these tests are the
        // ceremony lane's, and the two lanes' admissions are separate by design.
        // The wallet lane is exercised in `tests/wallet_frame_e2e.rs`.
        WalletRelayConfig::default(),
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

/// Join the recipient's PRESENCE room and wait for the signed `joinedRoom`, so
/// the test never races the join against the first relayed envelope. This is the
/// only room a ceremony party ever joins for itself.
async fn join_presence_room(client: &AuthSocketClient, identity: &str) {
    let mut joined_rx = listen_for(client, "joinedRoom").await;
    client
        .join_room(&format!("{identity}-{MPC_INBOX}"))
        .await
        .expect("join presence room");
    recv_within(&mut joined_rx, "joinedRoom").await;
}

/// THE test: a full ceremony hop over the real wire, into a PER-CEREMONY box the
/// recipient never joined.
///
/// The sender addresses `box: "mpc_<session>"`, which routes to
/// `{recipient}-mpc_<session>` — a room that does not exist when the envelope
/// arrives, because a ceremony party only ever joins its own presence room. The
/// relay's auto-join harvests the presence room and subscribes the recipient's
/// socket into the box room before emitting, which is what closes the
/// join-before-first-round race. If this test fails, ceremonies lose their first
/// round on every session.
///
/// It also pins the wire contract: the payload key is `box` (NOT `messageBox`),
/// and every field survives the hop unchanged apart from `sender`, which the
/// relay re-stamps from the BRC-103-verified socket identity.
#[tokio::test]
async fn envelope_relays_into_a_per_ceremony_box_the_recipient_never_joined() {
    let box_identity = identity_of(BOX_KEY).await;
    let cosigner_identity = identity_of(COSIGNER_KEY).await;
    let url = boot_with_peers(&[box_identity.clone(), cosigner_identity.clone()]).await;

    let cosigner = connect_as(&url, COSIGNER_KEY).await;
    join_presence_room(&cosigner, &cosigner_identity).await;
    let mut inbound = listen_for(&cosigner, MPC_ENVELOPE_EVENT).await;

    let enterprise_box = connect_as(&url, BOX_KEY).await;
    enterprise_box
        .emit(
            MPC_ENVELOPE_EVENT,
            &json!({
                "recipient": cosigner_identity,
                "box": "mpc_somesession",
                "body": "BRC78-ciphertext-round-1",
                "messageId": "mpc-e2e-1",
            }),
        )
        .await
        .expect("emit mpcEnvelope");

    let received = recv_within(&mut inbound, "relayed mpcEnvelope").await;

    assert!(
        received.get("box").is_some(),
        "the destination box must arrive under the key `box`: {received}"
    );
    assert!(
        received.get("messageBox").is_none(),
        "`messageBox` is the mailbox lane's key and would break the ceremony client: {received}"
    );
    assert_eq!(
        received["box"], "mpc_somesession",
        "the per-ceremony box travels through the relay unchanged"
    );
    assert_eq!(received["recipient"], cosigner_identity.as_str());
    assert_eq!(received["body"], "BRC78-ciphertext-round-1");
    assert_eq!(received["messageId"], "mpc-e2e-1");
    assert_eq!(
        received["sender"],
        box_identity.as_str(),
        "the relay stamps the BRC-103-verified socket identity as the sender"
    );

    enterprise_box.disconnect().await.expect("disconnect");
    cosigner.disconnect().await.expect("disconnect");
}

/// A body `sender` claiming a third identity is forged provenance, and the
/// envelope is dropped rather than re-stamped: the recipient receives NOTHING.
#[tokio::test]
async fn forged_body_sender_never_reaches_the_recipient() {
    let box_identity = identity_of(BOX_KEY).await;
    let cosigner_identity = identity_of(COSIGNER_KEY).await;
    let outsider_identity = identity_of(OUTSIDER_KEY).await;
    let url = boot_with_peers(&[box_identity, cosigner_identity.clone()]).await;

    let cosigner = connect_as(&url, COSIGNER_KEY).await;
    join_presence_room(&cosigner, &cosigner_identity).await;
    let mut inbound = listen_for(&cosigner, MPC_ENVELOPE_EVENT).await;

    let enterprise_box = connect_as(&url, BOX_KEY).await;
    enterprise_box
        .emit(
            MPC_ENVELOPE_EVENT,
            &json!({
                "recipient": cosigner_identity,
                "box": "mpc_somesession",
                "body": "BRC78-ciphertext",
                "sender": outsider_identity,
                "messageId": "mpc-forged-1",
            }),
        )
        .await
        .expect("emit mpcEnvelope");

    assert!(
        tokio::time::timeout(Duration::from_millis(750), inbound.recv())
            .await
            .is_err(),
        "an envelope claiming someone else's provenance must not be relayed"
    );

    enterprise_box.disconnect().await.expect("disconnect");
    cosigner.disconnect().await.expect("disconnect");
}

/// Admission is rooted in the VERIFIED socket identity: a party that is not on
/// `MPC_PEER_IDENTITIES` routes nothing, even with a perfectly formed envelope
/// and a recipient that is allowlisted and dialed in.
#[tokio::test]
async fn un_allowlisted_sender_is_refused() {
    let cosigner_identity = identity_of(COSIGNER_KEY).await;
    // The recipient is allowlisted; the sender deliberately is not.
    let url = boot_with_peers(std::slice::from_ref(&cosigner_identity)).await;

    let cosigner = connect_as(&url, COSIGNER_KEY).await;
    join_presence_room(&cosigner, &cosigner_identity).await;
    let mut inbound = listen_for(&cosigner, MPC_ENVELOPE_EVENT).await;

    let enterprise_box = connect_as(&url, BOX_KEY).await;
    enterprise_box
        .emit(
            MPC_ENVELOPE_EVENT,
            &json!({
                "recipient": cosigner_identity,
                "box": "mpc_somesession",
                "body": "BRC78-ciphertext",
                "messageId": "mpc-unlisted-1",
            }),
        )
        .await
        .expect("emit mpcEnvelope");

    assert!(
        tokio::time::timeout(Duration::from_millis(750), inbound.recv())
            .await
            .is_err(),
        "an un-allowlisted sender must not be able to route ceremony traffic"
    );

    enterprise_box.disconnect().await.expect("disconnect");
    cosigner.disconnect().await.expect("disconnect");
}

/// An unconfigured lane (no `MPC_PEER_IDENTITIES`) refuses every envelope,
/// including one between two parties that would otherwise be a legitimate pair.
/// A deployment that never opts in is not an open MPC relay.
#[tokio::test]
async fn unconfigured_lane_relays_nothing() {
    let cosigner_identity = identity_of(COSIGNER_KEY).await;
    let url = boot_with_peers(&[]).await;

    let cosigner = connect_as(&url, COSIGNER_KEY).await;
    join_presence_room(&cosigner, &cosigner_identity).await;
    let mut inbound = listen_for(&cosigner, MPC_ENVELOPE_EVENT).await;

    let enterprise_box = connect_as(&url, BOX_KEY).await;
    enterprise_box
        .emit(
            MPC_ENVELOPE_EVENT,
            &json!({
                "recipient": cosigner_identity,
                "box": "mpc_somesession",
                "body": "BRC78-ciphertext",
                "messageId": "mpc-lane-off-1",
            }),
        )
        .await
        .expect("emit mpcEnvelope");

    assert!(
        tokio::time::timeout(Duration::from_millis(750), inbound.recv())
            .await
            .is_err(),
        "the lane must be off until an operator names its peers"
    );

    enterprise_box.disconnect().await.expect("disconnect");
    cosigner.disconnect().await.expect("disconnect");
}
