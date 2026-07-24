//! Phase-3 admission-control e2e (`mbs-enterprise-production-design` D3) —
//! real sockets against a real loopback instance running the same admission
//! middleware `main.rs` installs.
//!
//! Proves the custody rule end-to-end: at the connection ceiling a NEW
//! connection is rejected with a clear, retryable 503 + `Retry-After`, while
//! the already-connected session keeps receiving signed pushes — capacity
//! pressure never nacks in-flight work. Mirrors the `backplane_e2e` harness
//! (no DB: the broadcast path never touches MySQL).

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use authsocket::client::AuthSocketClient;
use bsv::primitives::private_key::PrivateKey;
use bsv::wallet::interfaces::{GetPublicKeyArgs, WalletInterface};
use bsv::wallet::proto_wallet::ProtoWallet;

use messagebox_server::ops::OpsState;
use messagebox_server::ws::{self, RoomMessage, WsBroadcast};

const SERVER_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000031";
const CLIENT_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000032";
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

/// Boot an MBS instance with the admission middleware exactly as `main.rs`
/// wires it (Model A, connection ceiling from `ops`). Returns
/// (host:port, base url, ws handle).
async fn boot_gated_instance(ops: Arc<OpsState>) -> (String, String, WsBroadcast) {
    let (layer, io) = socketioxide::SocketIo::new_layer();
    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .connect_lazy("mysql://unused@127.0.0.1/unused")
        .expect("lazy pool");
    let ws = WsBroadcast::new(
        io.clone(),
        SERVER_KEY.to_string(),
        pool,
        None,
        Arc::clone(&ops),
    );
    ws::setup_handlers(&io, ws.clone());

    let admission_ops = ops;
    let admission_io = io.clone();
    let admission = axum::middleware::from_fn(
        move |req: axum::extract::Request, next: axum::middleware::Next| {
            let ops = admission_ops.clone();
            let io = admission_io.clone();
            async move {
                messagebox_server::ops::gate_admission(
                    &ops,
                    move || io.sockets().map(|s| s.len()).unwrap_or(0),
                    req,
                    next,
                )
                .await
            }
        },
    );

    let app = axum::Router::new().layer(layer).layer(admission);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    (addr.to_string(), format!("http://{addr}"), ws)
}

/// One raw HTTP/1.1 GET over loopback TCP; returns the full response text.
/// Used to observe the admission rejection exactly as an LB would (status
/// line + headers), without a client library retrying around it.
async fn raw_get(addr: &str, path_query: &str) -> String {
    let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    stream
        .write_all(
            format!("GET {path_query} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .expect("write request");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read response");
    String::from_utf8_lossy(&buf).into_owned()
}

fn room_message(message_id: &str, recipient: &str) -> RoomMessage {
    RoomMessage {
        message_id: message_id.to_string(),
        sender: SENDER_KEY.to_string(),
        recipient: recipient.to_string(),
        message_box: "mpc_inbox".to_string(),
        body: "{\"message\":\"encrypted-ceremony-frame\"}".to_string(),
        created_at: "2026-07-17T00:00:00.000Z".to_string(),
        updated_at: "2026-07-17T00:00:00.000Z".to_string(),
    }
}

/// D3 end-to-end: ceiling of 1 — the connected session is protected, the
/// second connection is shed with a clear retryable signal, and draining
/// rejects new connections the same way.
#[tokio::test]
async fn at_ceiling_new_conn_rejected_while_in_flight_session_still_delivered() {
    let ops = OpsState::new(1);
    let (addr, url, ws) = boot_gated_instance(Arc::clone(&ops)).await;

    // Client A: connect (admitted — under the ceiling), BRC-103 handshake,
    // join its own room, register the delivery listener.
    let identity = identity_of(CLIENT_KEY).await;
    let wallet = ProtoWallet::new(PrivateKey::from_hex(CLIENT_KEY).expect("key"));
    let client = AuthSocketClient::connect(&url, &identity, wallet)
        .await
        .expect("client A must be admitted below the ceiling");

    let room = format!("{identity}-mpc_inbox");
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel::<Value>();
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
    client.join_room(&room).await.expect("join_room");
    tokio::time::timeout(Duration::from_secs(10), joined_rx.recv())
        .await
        .expect("joinedRoom within 10s")
        .expect("joinedRoom data");

    // The instance is now AT its ceiling (1 connection). A NEW handshake
    // (no sid) must be rejected: 503, Retry-After, structured code.
    let response = raw_get(&addr, "/socket.io/?EIO=4&transport=polling").await;
    assert!(
        response.starts_with("HTTP/1.1 503"),
        "new connection at ceiling must get 503, got:\n{response}"
    );
    let lower = response.to_lowercase();
    assert!(
        lower.contains("retry-after: 5"),
        "rejection must carry Retry-After (the retryable signal):\n{response}"
    );
    assert!(
        response.contains("ERR_SERVER_AT_CAPACITY"),
        "rejection must carry the structured code:\n{response}"
    );
    assert!(ops.admission_rejected() >= 1, "rejection must be counted");

    // Custody rule: the in-flight session is untouched — a broadcast still
    // reaches client A as a signed push (its `on` handler fires only for
    // verified BRC-103 general messages).
    let delivered = ws
        .broadcast_to_room(
            &room,
            &format!("sendMessage-{room}"),
            &room_message("m-ceiling-1", &identity),
        )
        .await;
    assert_eq!(delivered, 1, "in-flight session must still be served");
    let got = tokio::time::timeout(Duration::from_secs(10), msg_rx.recv())
        .await
        .expect("signed push within 10s despite the ceiling")
        .expect("payload");
    assert_eq!(
        got.get("messageId").and_then(Value::as_str),
        Some("m-ceiling-1")
    );

    // Draining rejects NEW connections with its own clear code…
    ops.start_drain();
    let response = raw_get(&addr, "/socket.io/?EIO=4&transport=polling").await;
    assert!(
        response.starts_with("HTTP/1.1 503"),
        "draining must reject new connections:\n{response}"
    );
    assert!(response.contains("ERR_SERVER_DRAINING"), "{response}");

    // …while the existing session STILL receives pushes (drain() hasn't
    // disconnected it yet — admission stopping never kills live work).
    let delivered = ws
        .broadcast_to_room(
            &room,
            &format!("sendMessage-{room}"),
            &room_message("m-drain-1", &identity),
        )
        .await;
    assert_eq!(delivered, 1);
    let got = tokio::time::timeout(Duration::from_secs(10), msg_rx.recv())
        .await
        .expect("signed push while draining")
        .expect("payload");
    assert_eq!(
        got.get("messageId").and_then(Value::as_str),
        Some("m-drain-1")
    );

    client.disconnect().await.expect("disconnect");
}

/// Below the ceiling nothing is gated: a second instance-wide sanity check
/// that a fresh client connects fine at ceiling 2, and the ops endpoints are
/// reachable regardless of load (the middleware only ever touches new
/// engine.io handshakes).
#[tokio::test]
async fn below_ceiling_connects_and_ops_surface_is_never_gated() {
    let ops = OpsState::new(2);
    let (addr, url, _ws) = boot_gated_instance(Arc::clone(&ops)).await;

    let identity = identity_of(CLIENT_KEY).await;
    let wallet = ProtoWallet::new(PrivateKey::from_hex(CLIENT_KEY).expect("key"));
    let client = AuthSocketClient::connect(&url, &identity, wallet)
        .await
        .expect("admitted below ceiling");

    // Non-socket.io paths pass the middleware untouched even when the
    // decision would reject (1 of 2 connections used — but these requests
    // are not handshakes at all). 404 here means "reached the router".
    let response = raw_get(&addr, "/anything").await;
    assert!(
        response.starts_with("HTTP/1.1 404"),
        "non-handshake requests must pass the admission layer:\n{response}"
    );
    assert_eq!(ops.admission_rejected(), 0);

    client.disconnect().await.expect("disconnect");
}
