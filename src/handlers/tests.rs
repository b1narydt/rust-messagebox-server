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

    // Send same messageId again
    let (status, body) = send_valid_message(&app, "dup-msg-1").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "ERR_DUPLICATE_MESSAGE");
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

    // Now list messages for the authenticated user (TEST_KEY)
    let (status, body) = post_json(&app, "/listMessages", json!({
        "messageBox": "inbox"
    })).await;
    assert_eq!(status, StatusCode::OK);
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

/// Duplicate messageId to same recipient returns 400 ERR_DUPLICATE_MESSAGE.
#[tokio::test]
async fn test_send_message_duplicate_same_recipient_is_rejected() {
    let app = setup_app().await;

    let (status, _) = send_valid_message(&app, "dup-parity-1").await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send_valid_message(&app, "dup-parity-1").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "ERR_DUPLICATE_MESSAGE");
}

