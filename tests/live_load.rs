//! Live load harness (ignored by default). Drives N concurrent real BRC-103
//! `AuthSocketClient`s against a deployed MessageBox server: each connects,
//! joins its own room, and sends `MB_PER` messages to itself (exercising the
//! full sign → broadcast → local-delivery → async-persist path per message).
//! Measures end-to-end signed-push latency (send → verified receive callback)
//! and throughput under concurrency.
//!
//! Run against the live Railway box:
//!   MB_HOST=https://messagebox-production.up.railway.app \
//!   MB_CONNS=10 MB_PER=10 \
//!   cargo test --test live_load live_load -- --ignored --nocapture

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};
use tokio::sync::mpsc;

use authsocket::client::AuthSocketClient;
use bsv::primitives::private_key::PrivateKey;
use bsv::wallet::interfaces::{GetPublicKeyArgs, WalletInterface};
use bsv::wallet::proto_wallet::ProtoWallet;

fn env(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_string())
}
fn now_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

async fn identity_of(w: &ProtoWallet) -> String {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "live load test — set MB_HOST and run with --ignored"]
async fn live_load() {
    let host = std::env::var("MB_HOST").expect("MB_HOST required (e.g. https://…railway.app)");
    let conns: usize = env("MB_CONNS", "10").parse().unwrap();
    let per: usize = env("MB_PER", "10").parse().unwrap();
    let total = conns * per;
    println!("\n=== live load: {conns} clients × {per} msgs = {total} → {host} ===");

    let started = Instant::now();
    // One task per client; each returns its per-message latencies (ns).
    let mut handles = Vec::with_capacity(conns);
    for i in 0..conns {
        let host = host.clone();
        handles.push(tokio::spawn(async move {
            // Distinct valid secp256k1 scalar per client.
            let key = format!("{:064x}", 0x1000u64 + i as u64);
            let wallet = ProtoWallet::new(PrivateKey::from_hex(&key).expect("key"));
            let identity = identity_of(&wallet).await;
            let room = format!("{identity}-load");

            let client = match AuthSocketClient::connect(&host, &identity, wallet).await {
                Ok(c) => c,
                Err(e) => return (0usize, 0usize, vec![], format!("connect: {e}")),
            };

            // Receive channel: on a verified delivery, extract the embedded
            // send-ns from the messageId and record latency.
            let (rx_tx, mut rx_rx) = mpsc::unbounded_channel::<u128>();
            client
                .on(
                    format!("sendMessage-{room}"),
                    Arc::new(move |data: Value| {
                        if let Some(id) = data.get("messageId").and_then(Value::as_str) {
                            if let Some(sent) = id.rsplit('-').next().and_then(|s| s.parse::<u128>().ok())
                            {
                                let _ = rx_tx.send(now_ns().saturating_sub(sent));
                            }
                        }
                    }),
                )
                .await;

            let (joined_tx, mut joined_rx) = mpsc::unbounded_channel::<Value>();
            client
                .on("joinedRoom", Arc::new(move |d| { let _ = joined_tx.send(d); }))
                .await;
            if client.join_room(&room).await.is_err()
                || tokio::time::timeout(Duration::from_secs(10), joined_rx.recv())
                    .await
                    .is_err()
            {
                return (0, 0, vec![], "join".to_string());
            }

            // Fire `per` messages; each messageId ends with the send timestamp.
            let mut sent = 0usize;
            for seq in 0..per {
                let msg_id = format!("m-{i}-{seq}-{}", now_ns());
                if client
                    .emit(
                        "sendMessage",
                        &json!({
                            "roomId": room,
                            "message": { "messageId": msg_id, "recipient": identity, "body": "load" }
                        }),
                    )
                    .await
                    .is_ok()
                {
                    sent += 1;
                }
            }

            // Collect receipts (bounded wait).
            let mut lats = Vec::with_capacity(per);
            let deadline = Instant::now() + Duration::from_secs(20);
            while lats.len() < sent && Instant::now() < deadline {
                if let Ok(Some(l)) = tokio::time::timeout(Duration::from_millis(500), rx_rx.recv()).await {
                    lats.push(l);
                }
            }
            let _ = client.disconnect().await;
            (sent, lats.len(), lats, String::new())
        }));
    }

    let mut sent_total = 0;
    let mut recv_total = 0;
    let mut all_lat: Vec<u128> = Vec::new();
    let mut errors: Vec<String> = Vec::new();
    for h in handles {
        let (s, r, lats, err) = h.await.unwrap();
        sent_total += s;
        recv_total += r;
        all_lat.extend(lats);
        if !err.is_empty() {
            errors.push(err);
        }
    }
    let wall = started.elapsed().as_secs_f64();
    all_lat.sort_unstable();
    let pct = |p: f64| -> f64 {
        if all_lat.is_empty() {
            return 0.0;
        }
        let idx = ((all_lat.len() as f64 - 1.0) * p).round() as usize;
        all_lat[idx] as f64 / 1e6 // ns → ms
    };

    println!("--- results ---");
    println!("  sent:        {sent_total}");
    println!(
        "  received:    {recv_total}  ({:.1}% delivered live)",
        100.0 * recv_total as f64 / sent_total.max(1) as f64
    );
    println!("  wall:        {wall:.2}s");
    println!(
        "  throughput:  {:.0} msg/s (sent), {:.0} msg/s (delivered)",
        sent_total as f64 / wall,
        recv_total as f64 / wall
    );
    println!(
        "  latency ms:  p50={:.1}  p90={:.1}  p99={:.1}  max={:.1}",
        pct(0.50),
        pct(0.90),
        pct(0.99),
        pct(1.0)
    );
    if !errors.is_empty() {
        println!(
            "  client errors ({}): {:?}",
            errors.len(),
            &errors[..errors.len().min(5)]
        );
    }

    // Soft assertions: most messages should be delivered live and the server
    // should not have fallen over.
    assert!(
        sent_total > 0,
        "no messages sent — connection/auth failed: {errors:?}"
    );
}
