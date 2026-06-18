//! MySQL-backed handler tests. Each test builds a fresh Router bound to its
//! own per-test database via `crate::test_support::fresh_pool()`.

use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
    routing::{get, post},
    Router,
};
use http_body_util::BodyExt; // for .collect()
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt; // for .oneshot()

use bsv::primitives::private_key::PrivateKey;
use bsv::wallet::proto_wallet::ProtoWallet as SdkProtoWallet;
use bsv_wallet_toolbox::types::Chain;
use bsv_wallet_toolbox::wallet::setup::WalletBuilder;
use bsv_wallet_toolbox::wallet::wallet::Wallet;

use crate::config::Config;
use crate::db::queries;
use crate::handlers::helpers::{AppState, AuthIdentity};
use crate::test_support::{fresh_pool, RECIPIENT_KEY, TEST_KEY};

fn test_config() -> Config {
    Config {
        node_env: "development".to_string(),
        port: 8080,
        server_private_key: "a".repeat(64),
        routing_prefix: String::new(),
        db_source: "mysql://unused".to_string(),
        db_max_connections: 4,
        bsv_network: "testnet".to_string(),
        enable_websockets: false,
        wallet_storage_url: "https://storage.babbage.systems".to_string(),
        firebase_project_id: None,
        firebase_service_account_json: None,
        firebase_service_account_path: None,
        message_box_fees: Vec::new(),
        message_box_fees_warnings: Vec::new(),
    }
}

/// Build an in-memory `Wallet` for use in tests.
///
/// Uses in-memory SQLite storage so tests stay self-contained and don't
/// touch any remote wallet-storage backend. The funded_wallet is not
/// exercised by any current test (no test sets a delivery_fee > 0), but it
/// must exist for the AppState to compile.
async fn test_funded_wallet() -> Arc<Wallet> {
    let pk = PrivateKey::from_hex(&"a".repeat(64)).unwrap();
    let setup = WalletBuilder::new()
        .chain(Chain::Test)
        .root_key(pk)
        .with_sqlite_memory()
        .with_default_services()
        .build()
        .await
        .expect("failed to build test funded wallet");
    Arc::new(setup.wallet)
}

async fn setup_app() -> Router {
    let pool = fresh_pool().await;

    let pk = PrivateKey::from_hex(&"a".repeat(64)).unwrap();
    let wallet = Arc::new(SdkProtoWallet::new(pk));
    let funded_wallet = test_funded_wallet().await;

    // Create a minimal WsBroadcast for tests (Socket.IO is not exercised
    // but needs a default namespace registered to avoid panics on broadcast).
    let (_sio_layer, io) = socketioxide::SocketIo::new_layer();
    io.ns("/", |_: socketioxide::extract::SocketRef| {});
    let ws_broadcast = crate::ws::WsBroadcast::new(io, "a".repeat(64), pool.clone());

    let state = AppState {
        db: pool,
        config: Arc::new(test_config()),
        wallet,
        funded_wallet,
        ws: ws_broadcast,
    };

    Router::new()
        .route("/sendMessage", post(crate::handlers::send_message::send_message))
        .route("/listMessages", post(crate::handlers::list_messages::list_messages))
        .route("/acknowledgeMessage", post(crate::handlers::acknowledge_message::acknowledge_message))
        .route("/registerDevice", post(crate::handlers::devices::register_device))
        .route("/devices", get(crate::handlers::devices::list_devices))
        .route("/permissions/set", post(crate::handlers::permissions::set_permission))
        .route("/permissions/get", get(crate::handlers::permissions::get_permission))
        .route("/permissions/list", get(crate::handlers::permissions::list_permissions))
        .route("/permissions/quote", get(crate::handlers::permissions::get_quote))
        .layer(axum::middleware::from_fn(auth_middleware))
        .with_state(state)
}

async fn auth_middleware(
    mut request: Request<Body>,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if request.headers().get("x-skip-auth").is_none() {
        request.extensions_mut().insert(AuthIdentity(TEST_KEY.to_string()));
    }
    next.run(request).await
}

async fn post_json(app: &Router, path: &str, body: Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(Method::POST)
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();

    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&body_bytes).unwrap_or(Value::Null);
    (status, body)
}

/// Poll `listMessages` for `message_box` until at least `expected` messages are
/// visible, or the timeout elapses. Persistence is now asynchronous (push-live
/// first, persist in a background worker), so a read immediately after a
/// `sendMessage` 200 may race the worker. Live recipients get the message over
/// WebSocket instantly; the HTTP fallback becomes consistent within a worker
/// tick. This helper makes that read-after-write deterministic in tests without
/// weakening the production durability guarantee.
async fn list_messages_until(app: &Router, message_box: &str, expected: usize) -> Value {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let (status, body) = post_json(app, "/listMessages", json!({
            "messageBox": message_box
        })).await;
        assert_eq!(status, StatusCode::OK);
        let len = body["messages"].as_array().map(|a| a.len()).unwrap_or(0);
        if len >= expected || std::time::Instant::now() >= deadline {
            return body;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

async fn get_path(app: &Router, path: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(Method::GET)
        .uri(path)
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&body_bytes).unwrap_or(Value::Null);
    (status, body)
}

async fn post_json_no_auth(app: &Router, path: &str, body: Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(Method::POST)
        .uri(path)
        .header("content-type", "application/json")
        .header("x-skip-auth", "1")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();

    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&body_bytes).unwrap_or(Value::Null);
    (status, body)
}

async fn get_path_no_auth(app: &Router, path: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(Method::GET)
        .uri(path)
        .header("x-skip-auth", "1")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&body_bytes).unwrap_or(Value::Null);
    (status, body)
}

// Helper: send a valid message and return the response.
async fn send_valid_message(app: &Router, message_id: &str) -> (StatusCode, Value) {
    post_json(app, "/sendMessage", json!({
        "message": {
            "recipient": RECIPIENT_KEY,
            "messageBox": "inbox",
            "messageId": message_id,
            "body": "test message body"
        }
    })).await
}

// ===========================================================================
// sendMessage tests
// ===========================================================================

#[tokio::test]
async fn test_send_message_missing_message() {
    let app = setup_app().await;
    let (status, body) = post_json(&app, "/sendMessage", json!({})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "ERR_MESSAGE_REQUIRED");
}

#[tokio::test]
async fn test_send_message_missing_recipient() {
    let app = setup_app().await;
    let (status, body) = post_json(&app, "/sendMessage", json!({
        "message": {
            "messageBox": "inbox",
            "messageId": "m1",
            "body": "hello"
        }
    })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "ERR_RECIPIENT_REQUIRED");
}

#[tokio::test]
async fn test_send_message_invalid_recipient_key() {
    let app = setup_app().await;
    let (status, body) = post_json(&app, "/sendMessage", json!({
        "message": {
            "recipient": "not-a-valid-key",
            "messageBox": "inbox",
            "messageId": "m1",
            "body": "hello"
        }
    })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "ERR_INVALID_RECIPIENT_KEY");
}

#[tokio::test]
async fn test_send_message_missing_message_box() {
    let app = setup_app().await;
    let (status, body) = post_json(&app, "/sendMessage", json!({
        "message": {
            "recipient": RECIPIENT_KEY,
            "messageId": "m1",
            "body": "hello"
        }
    })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "ERR_INVALID_MESSAGEBOX");
}

#[tokio::test]
async fn test_send_message_missing_body() {
    let app = setup_app().await;
    let (status, body) = post_json(&app, "/sendMessage", json!({
        "message": {
            "recipient": RECIPIENT_KEY,
            "messageBox": "inbox",
            "messageId": "m1"
        }
    })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "ERR_INVALID_MESSAGE_BODY");
}

#[tokio::test]
async fn test_send_message_missing_message_id() {
    let app = setup_app().await;
    let (status, body) = post_json(&app, "/sendMessage", json!({
        "message": {
            "recipient": RECIPIENT_KEY,
            "messageBox": "inbox",
            "body": "hello"
        }
    })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "ERR_MESSAGEID_REQUIRED");
}

#[tokio::test]
async fn test_send_message_id_count_mismatch() {
    let app = setup_app().await;
    let (status, body) = post_json(&app, "/sendMessage", json!({
        "message": {
            "recipients": [RECIPIENT_KEY, TEST_KEY],
            "messageBox": "inbox",
            "messageId": "single-id",
            "body": "hello"
        }
    })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "ERR_MESSAGEID_COUNT_MISMATCH");
}

#[tokio::test]
async fn test_send_message_success() {
    let app = setup_app().await;
    let (status, body) = send_valid_message(&app, "msg-success-1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "success");
    assert!(body["results"].is_array());
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["recipient"], RECIPIENT_KEY);
    assert_eq!(results[0]["messageId"], "msg-success-1");
}

#[tokio::test]
async fn test_send_message_duplicate() {
    let app = setup_app().await;
    let (status, _) = send_valid_message(&app, "dup-msg-1").await;
    assert_eq!(status, StatusCode::OK);

    // Send the same messageId again. Persistence is now asynchronous (push-live
    // first), so the duplicate is no longer rejected synchronously at the HTTP
    // layer — it is caught by the unique messageId constraint (INSERT IGNORE) in
    // the background persist worker and treated as idempotent success. The
    // response is therefore 200, and no second row is ever written.
    let (status, body) = send_valid_message(&app, "dup-msg-1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "success");
}

#[tokio::test]
async fn test_send_message_json_body() {
    let app = setup_app().await;
    let (status, body) = post_json(&app, "/sendMessage", json!({
        "message": {
            "recipient": RECIPIENT_KEY,
            "messageBox": "inbox",
            "messageId": "json-body-msg",
            "body": {"nested": "object", "count": 42}
        }
    })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "success");
}

// ===========================================================================
// listMessages tests
// ===========================================================================

#[tokio::test]
async fn test_list_messages_missing_box() {
    let app = setup_app().await;
    let (status, body) = post_json(&app, "/listMessages", json!({})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "ERR_MESSAGEBOX_REQUIRED");
}

#[tokio::test]
async fn test_list_messages_empty_box() {
    let app = setup_app().await;
    let (status, body) = post_json(&app, "/listMessages", json!({
        "messageBox": "inbox"
    })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "success");
    assert!(body["messages"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_list_messages_with_messages() {
    let app = setup_app().await;

    // The authenticated user is TEST_KEY. We send a message TO TEST_KEY
    // so that when TEST_KEY lists messages, it appears.
    // We need to send as a *different* sender but the recipient is TEST_KEY.
    let (status, _) = post_json(&app, "/sendMessage", json!({
        "message": {
            "recipient": TEST_KEY,
            "messageBox": "inbox",
            "messageId": "list-test-1",
            "body": "hello from test"
        }
    })).await;
    assert_eq!(status, StatusCode::OK);

    // Now list messages for the authenticated user (TEST_KEY). Persistence is
    // asynchronous, so poll until the background worker has committed the row.
    let body = list_messages_until(&app, "inbox", 1).await;
    let messages = body["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["messageId"], "list-test-1");
}

// ===========================================================================
// acknowledgeMessage tests
// ===========================================================================

#[tokio::test]
async fn test_acknowledge_missing_ids() {
    let app = setup_app().await;
    let (status, body) = post_json(&app, "/acknowledgeMessage", json!({})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "ERR_MESSAGE_ID_REQUIRED");
}

#[tokio::test]
async fn test_acknowledge_invalid_ids_format() {
    let app = setup_app().await;
    let (status, body) = post_json(&app, "/acknowledgeMessage", json!({
        "messageIds": [123, 456]
    })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "ERR_INVALID_MESSAGE_ID");
}

#[tokio::test]
async fn test_acknowledge_not_found() {
    let app = setup_app().await;
    let (status, body) = post_json(&app, "/acknowledgeMessage", json!({
        "messageIds": ["nonexistent-id"]
    })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "ERR_INVALID_ACKNOWLEDGMENT");
}

#[tokio::test]
async fn test_acknowledge_success() {
    let app = setup_app().await;

    // Send message TO self (TEST_KEY) so we can acknowledge it
    let (status, _) = post_json(&app, "/sendMessage", json!({
        "message": {
            "recipient": TEST_KEY,
            "messageBox": "inbox",
            "messageId": "ack-msg-1",
            "body": "to be acked"
        }
    })).await;
    assert_eq!(status, StatusCode::OK);

    // Wait for the async persist worker to commit before acknowledging — the
    // HTTP ack path operates on the persisted row.
    list_messages_until(&app, "inbox", 1).await;

    // Acknowledge it
    let (status, body) = post_json(&app, "/acknowledgeMessage", json!({
        "messageIds": ["ack-msg-1"]
    })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "success");

    // Verify it's gone
    let (status, body) = post_json(&app, "/listMessages", json!({
        "messageBox": "inbox"
    })).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["messages"].as_array().unwrap().is_empty());
}

// ===========================================================================
// devices tests
// ===========================================================================

#[tokio::test]
async fn test_register_device_missing_token() {
    let app = setup_app().await;
    let (status, body) = post_json(&app, "/registerDevice", json!({})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "ERR_INVALID_FCM_TOKEN");
}

#[tokio::test]
async fn test_register_device_invalid_platform() {
    let app = setup_app().await;
    let (status, body) = post_json(&app, "/registerDevice", json!({
        "fcmToken": "valid-token-123",
        "platform": "windows"
    })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "ERR_INVALID_PLATFORM");
}

#[tokio::test]
async fn test_register_device_success() {
    let app = setup_app().await;
    let (status, body) = post_json(&app, "/registerDevice", json!({
        "fcmToken": "fcm-token-12345",
        "platform": "android"
    })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "success");
    assert!(body["deviceId"].as_i64().unwrap() > 0);
}

#[tokio::test]
async fn test_list_devices_empty() {
    let app = setup_app().await;
    let (status, body) = get_path(&app, "/devices").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "success");
    assert!(body["devices"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_list_devices_with_registered() {
    let app = setup_app().await;

    // Register a device first
    let (status, _) = post_json(&app, "/registerDevice", json!({
        "fcmToken": "a]very-long-fcm-token-that-gets-masked",
        "platform": "ios"
    })).await;
    assert_eq!(status, StatusCode::OK);

    // List devices
    let (status, body) = get_path(&app, "/devices").await;
    assert_eq!(status, StatusCode::OK);
    let devices = body["devices"].as_array().unwrap();
    assert_eq!(devices.len(), 1);
    // Token should be masked (only last 10 chars shown)
    let token = devices[0]["fcmToken"].as_str().unwrap();
    assert!(token.starts_with("..."), "fcm token should be masked: {}", token);
}

// ===========================================================================
// permissions tests
// ===========================================================================

#[tokio::test]
async fn test_set_permission_success() {
    let app = setup_app().await;
    let (status, body) = post_json(&app, "/permissions/set", json!({
        "messageBox": "inbox",
        "recipientFee": 50
    })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "success");
}

#[tokio::test]
async fn test_get_permission_not_found() {
    let app = setup_app().await;
    let (status, body) = get_path(&app, "/permissions/get?messageBox=inbox").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "success");
    assert!(body["permission"].is_null());
}

#[tokio::test]
async fn test_get_permission_after_set() {
    let app = setup_app().await;

    // Set a permission
    let (status, _) = post_json(&app, "/permissions/set", json!({
        "messageBox": "inbox",
        "recipientFee": 75
    })).await;
    assert_eq!(status, StatusCode::OK);

    // Retrieve it
    let (status, body) = get_path(&app, "/permissions/get?messageBox=inbox").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "success");
    let perm = &body["permission"];
    assert!(!perm.is_null());
    assert_eq!(perm["recipientFee"], 75);
    assert_eq!(perm["messageBox"], "inbox");
}

#[tokio::test]
async fn test_list_permissions() {
    let app = setup_app().await;

    // Set a couple permissions
    post_json(&app, "/permissions/set", json!({
        "messageBox": "inbox",
        "recipientFee": 10
    })).await;
    post_json(&app, "/permissions/set", json!({
        "messageBox": "notifications",
        "recipientFee": 20
    })).await;

    let (status, body) = get_path(&app, "/permissions/list").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "success");
    let perms = body["permissions"].as_array().unwrap();
    assert_eq!(perms.len(), 2);
    assert_eq!(body["totalCount"], 2);
}

#[tokio::test]
async fn test_get_quote_single() {
    let app = setup_app().await;
    let url = format!(
        "/permissions/quote?messageBox=inbox&recipient={}",
        RECIPIENT_KEY
    );
    let (status, body) = get_path(&app, &url).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "success");
    assert!(body["quote"].is_object());
    assert!(body["quote"]["deliveryFee"].is_number());
    assert!(body["quote"]["recipientFee"].is_number());
}

#[tokio::test]
async fn test_get_quote_missing_params() {
    let app = setup_app().await;
    let (status, body) = get_path(&app, "/permissions/quote").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "ERR_MISSING_PARAMETERS");
}

// ===========================================================================
// auth tests
// ===========================================================================

#[tokio::test]
async fn test_no_auth_returns_401() {
    let app = setup_app().await;

    // Test POST endpoints without auth
    let (status, body) = post_json_no_auth(&app, "/sendMessage", json!({
        "message": {"body": "test"}
    })).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["code"], "ERR_AUTH_REQUIRED");

    // Test another POST endpoint
    let app = setup_app().await;
    let (status, _) = post_json_no_auth(&app, "/listMessages", json!({
        "messageBox": "inbox"
    })).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Test GET endpoint without auth
    let app = setup_app().await;
    let (status, _) = get_path_no_auth(&app, "/devices").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Test permissions GET without auth
    let app = setup_app().await;
    let (status, _) = get_path_no_auth(&app, "/permissions/get?messageBox=inbox").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

// ===========================================================================
// TS-parity tests (new)
// ===========================================================================

/// sendMessage.ts:318-327 — if ANY recipient in a multi-recipient batch is
/// blocked (fee=-1), the whole batch is rejected with 403 ERR_DELIVERY_BLOCKED
/// and the blocked recipient listed in `blockedRecipients`.
#[tokio::test]
async fn test_send_message_multi_recipient_one_blocked_blocks_batch() {
    // We need direct DB access to seed the -1 permission on the SAME pool the
    // app is using, so we build the Router inline here with a shared pool.
    let pool = fresh_pool().await;

    let pk = PrivateKey::from_hex(&"a".repeat(64)).unwrap();
    let wallet = Arc::new(SdkProtoWallet::new(pk));
    let funded_wallet = test_funded_wallet().await;
    let (_sio_layer, io) = socketioxide::SocketIo::new_layer();
    io.ns("/", |_: socketioxide::extract::SocketRef| {});
    let ws_broadcast = crate::ws::WsBroadcast::new(io, "a".repeat(64), pool.clone());

    let state = AppState {
        db: pool.clone(),
        config: Arc::new(test_config()),
        wallet,
        funded_wallet,
        ws: ws_broadcast,
    };
    let app = Router::new()
        .route("/sendMessage", post(crate::handlers::send_message::send_message))
        .layer(axum::middleware::from_fn(auth_middleware))
        .with_state(state);

    // RECIPIENT_KEY blocks TEST_KEY on 'inbox'.
    queries::set_message_permission(&pool, RECIPIENT_KEY, Some(TEST_KEY), "inbox", -1)
        .await
        .unwrap();

    // Second recipient must be a valid pub key distinct from RECIPIENT_KEY.
    // A second valid compressed secp256k1 pubkey (66 hex chars, starts with 02/03).
    let other_recipient = "02e876feaf6b7f73fa7d1d7e5b2c2e1a0c7c0b9e3fa5a3d4a7b8c9d0e1f2a3b4c5";

    let (status, body) = post_json(&app, "/sendMessage", json!({
        "message": {
            "recipients": [RECIPIENT_KEY, other_recipient],
            "messageBox": "inbox",
            "messageId": ["mid-1", "mid-2"],
            "body": "should be blocked for one recipient"
        }
    })).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "ERR_DELIVERY_BLOCKED");
    let blocked = body["blockedRecipients"].as_array().unwrap();
    assert!(
        blocked.iter().any(|v| v == RECIPIENT_KEY),
        "blockedRecipients must include the -1 recipient: got {:?}",
        blocked
    );
}

/// listMessages.test.ts:129-148 — listing a messageBox that was never created
/// for the authenticated user must return 200 with an empty `messages` array.
#[tokio::test]
async fn test_list_messages_returns_empty_when_box_does_not_exist() {
    let app = setup_app().await;
    let (status, body) = post_json(&app, "/listMessages", json!({
        "messageBox": "never-created-box-xyz"
    })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "success");
    let msgs = body["messages"].as_array().expect("messages array");
    assert!(msgs.is_empty(), "messages should be empty for nonexistent box");
}

/// Duplicate messageId to the same recipient is idempotent: with async
/// persistence the unique messageId constraint (INSERT IGNORE in the persist
/// worker) dedupes at persist time, so the second send returns 200 success and
/// no second row is written. The client treats the duplicate as idempotent.
#[tokio::test]
async fn test_send_message_duplicate_same_recipient_is_idempotent() {
    let app = setup_app().await;

    let (status, _) = send_valid_message(&app, "dup-parity-1").await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send_valid_message(&app, "dup-parity-1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "success");
}

// ===========================================================================
// Async-persistence durability tests (push-live-first, persist-in-background)
//
// These exercise the wiring between the send paths and the background persist
// worker against the real testcontainers MySQL — they prove the worker writes
// EXACTLY ONE row per distinct messageId (dedup), never persists a rejected
// send (gating-order invariant), and survives concurrent load without loss or
// duplication. The worker's failure-mode units (transient/permanent/exhausted/
// dead-letter) live in `crate::persist` and are not duplicated here.
// ===========================================================================

/// A duplicate messageId must result in EXACTLY ONE persisted row. The first
/// send enqueues the INSERT; the second is caught by the unique-messageId
/// `INSERT IGNORE` in the worker and is a no-op. We poll to 1, then poll again
/// after a worker tick to confirm the duplicate never wrote a second row.
#[tokio::test]
async fn test_send_message_duplicate_writes_exactly_one_row() {
    let app = setup_app().await;

    // Send TO the authenticated lister (TEST_KEY) so listMessages sees the row.
    let mid = "dup-exactly-one";
    for _ in 0..2 {
        let (status, body) = post_json(&app, "/sendMessage", json!({
            "message": {
                "recipient": TEST_KEY,
                "messageBox": "inbox",
                "messageId": mid,
                "body": "duplicate body"
            }
        })).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "success");
    }

    // The worker commits at least one row.
    let body = list_messages_until(&app, "inbox", 1).await;
    let messages = body["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 1, "first poll: exactly one row expected");
    assert_eq!(messages[0]["messageId"], mid);

    // Give the worker another tick to (not) commit the duplicate, then confirm
    // there is still exactly one row — the INSERT IGNORE dedup held.
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    let (status, body) = post_json(&app, "/listMessages", json!({
        "messageBox": "inbox"
    })).await;
    assert_eq!(status, StatusCode::OK);
    let messages = body["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 1, "duplicate messageId must not write a second row");
}

/// Gating-order invariant: a blocked recipient (fee = -1) is rejected with 403
/// BEFORE the push/persist loop runs, so NOTHING is persisted (or broadcast).
/// `listMessages` for that recipient must be empty.
///
/// Auth is fixed to TEST_KEY, so we model recipient == sender == TEST_KEY and
/// have TEST_KEY block TEST_KEY on `inbox`. That trips the same blocked-recipient
/// 403 path, and TEST_KEY (the authenticated lister) can then verify its inbox
/// stayed empty.
#[tokio::test]
async fn test_blocked_recipient_not_persisted_or_broadcast() {
    let pool = fresh_pool().await;

    let pk = PrivateKey::from_hex(&"a".repeat(64)).unwrap();
    let wallet = Arc::new(SdkProtoWallet::new(pk));
    let funded_wallet = test_funded_wallet().await;
    let (_sio_layer, io) = socketioxide::SocketIo::new_layer();
    io.ns("/", |_: socketioxide::extract::SocketRef| {});
    let ws_broadcast = crate::ws::WsBroadcast::new(io, "a".repeat(64), pool.clone());

    let state = AppState {
        db: pool.clone(),
        config: Arc::new(test_config()),
        wallet,
        funded_wallet,
        ws: ws_broadcast,
    };
    let app = Router::new()
        .route("/sendMessage", post(crate::handlers::send_message::send_message))
        .route("/listMessages", post(crate::handlers::list_messages::list_messages))
        .layer(axum::middleware::from_fn(auth_middleware))
        .with_state(state);

    // TEST_KEY (recipient) blocks TEST_KEY (sender) on 'inbox' → fee -1.
    queries::set_message_permission(&pool, TEST_KEY, Some(TEST_KEY), "inbox", -1)
        .await
        .unwrap();

    let (status, body) = post_json(&app, "/sendMessage", json!({
        "message": {
            "recipient": TEST_KEY,
            "messageBox": "inbox",
            "messageId": "blocked-mid-1",
            "body": "should never persist"
        }
    })).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "ERR_DELIVERY_BLOCKED");

    // A rejected send must NOT have persisted anything. Wait a worker tick to
    // rule out a late async write, then assert the inbox is empty.
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    let (status, body) = post_json(&app, "/listMessages", json!({
        "messageBox": "inbox"
    })).await;
    assert_eq!(status, StatusCode::OK);
    let messages = body["messages"].as_array().unwrap();
    assert!(
        messages.is_empty(),
        "blocked send must persist nothing, found: {messages:?}"
    );
}

/// Concurrent load: fire N distinct-messageId sends in parallel and confirm the
/// single-consumer worker commits EXACTLY N distinct rows (no loss, no dup).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_sends_all_persist_exactly_once() {
    const N: usize = 30;
    let app = setup_app().await;

    let mut handles = Vec::with_capacity(N);
    for i in 0..N {
        let app = app.clone();
        handles.push(tokio::spawn(async move {
            let mid = format!("concurrent-mid-{i}");
            let (status, _) = post_json(&app, "/sendMessage", json!({
                "message": {
                    "recipient": TEST_KEY,
                    "messageBox": "inbox",
                    "messageId": mid,
                    "body": format!("concurrent body {i}")
                }
            })).await;
            assert_eq!(status, StatusCode::OK);
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // All N must land. list_messages_until polls until the worker drains.
    let body = list_messages_until(&app, "inbox", N).await;
    let messages = body["messages"].as_array().unwrap();

    let distinct: std::collections::HashSet<&str> = messages
        .iter()
        .filter_map(|m| m["messageId"].as_str())
        .collect();
    assert_eq!(
        messages.len(),
        N,
        "expected exactly {N} rows (no loss/dup), got {}",
        messages.len()
    );
    assert_eq!(
        distinct.len(),
        N,
        "expected {N} distinct messageIds, got {}",
        distinct.len()
    );
}

