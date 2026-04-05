use axum::{
    body::Body,
    http::{Request, StatusCode, Method},
    Router,
    routing::{get, post},
};
use tower::ServiceExt; // for oneshot
use http_body_util::BodyExt; // for collect
use serde_json::{json, Value};
use std::sync::Arc;

use crate::db;
use crate::config::Config;
use crate::handlers::helpers::{AppState, AuthIdentity};

const TEST_KEY: &str = "028d37b941208cd6b8a4c28288eda5f2f16c2b3ab0fcb6d13c18b47fe37b971fc1";
const RECIPIENT_KEY: &str = "0350b59e3efb8e37ba1ba2bde37c24e2bed89346ef3dc46d780e2b99f3efe50d1c";

fn test_config() -> Config {
    Config {
        node_env: "development".to_string(),
        port: 8080,
        server_private_key: "a".repeat(64),
        routing_prefix: String::new(),
        db_driver: "sqlite3".to_string(),
        db_source: ":memory:".to_string(),
        bsv_network: "testnet".to_string(),
        enable_websockets: false,
        wallet_storage_url: None,
        firebase_project_id: None,
        firebase_service_account_json: None,
        firebase_service_account_path: None,
    }
}

fn setup_app() -> Router {
    let pool = db::new(":memory:").unwrap();
    db::migrate(&pool).unwrap();

    let pk = bsv::primitives::private_key::PrivateKey::from_hex(&"a".repeat(64)).unwrap();
    let wallet = Arc::new(bsv::wallet::proto_wallet::ProtoWallet::new(pk));

    // Create a minimal WsBroadcast for tests (Socket.IO is not exercised
    // but needs a default namespace registered to avoid panics on broadcast)
    let (_sio_layer, io) = socketioxide::SocketIo::new_layer();
    io.ns("/", |_: socketioxide::extract::SocketRef| {});
    let ws_broadcast = crate::ws::WsBroadcast::new(io, "a".repeat(64), pool.clone());

    let state = AppState {
        db: pool,
        config: Arc::new(test_config()),
        wallet,
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
    let app = setup_app();
    let (status, body) = post_json(&app, "/sendMessage", json!({})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "ERR_MESSAGE_REQUIRED");
}

#[tokio::test]
async fn test_send_message_missing_recipient() {
    let app = setup_app();
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
    let app = setup_app();
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
    let app = setup_app();
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
    let app = setup_app();
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
    let app = setup_app();
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
    let app = setup_app();
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
    let app = setup_app();
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
    let app = setup_app();
    let (status, _) = send_valid_message(&app, "dup-msg-1").await;
    assert_eq!(status, StatusCode::OK);

    // Send same messageId again
    let (status, body) = send_valid_message(&app, "dup-msg-1").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "ERR_DUPLICATE_MESSAGE");
}

#[tokio::test]
async fn test_send_message_json_body() {
    let app = setup_app();
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
    let app = setup_app();
    let (status, body) = post_json(&app, "/listMessages", json!({})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "ERR_MESSAGEBOX_REQUIRED");
}

#[tokio::test]
async fn test_list_messages_empty_box() {
    let app = setup_app();
    let (status, body) = post_json(&app, "/listMessages", json!({
        "messageBox": "inbox"
    })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "success");
    assert!(body["messages"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_list_messages_with_messages() {
    let app = setup_app();

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
    let app = setup_app();
    let (status, body) = post_json(&app, "/acknowledgeMessage", json!({})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "ERR_MESSAGE_ID_REQUIRED");
}

#[tokio::test]
async fn test_acknowledge_invalid_ids_format() {
    let app = setup_app();
    let (status, body) = post_json(&app, "/acknowledgeMessage", json!({
        "messageIds": [123, 456]
    })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "ERR_INVALID_MESSAGE_ID");
}

#[tokio::test]
async fn test_acknowledge_not_found() {
    let app = setup_app();
    let (status, body) = post_json(&app, "/acknowledgeMessage", json!({
        "messageIds": ["nonexistent-id"]
    })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "ERR_INVALID_ACKNOWLEDGMENT");
}

#[tokio::test]
async fn test_acknowledge_success() {
    let app = setup_app();

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
    let app = setup_app();
    let (status, body) = post_json(&app, "/registerDevice", json!({})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "ERR_INVALID_FCM_TOKEN");
}

#[tokio::test]
async fn test_register_device_invalid_platform() {
    let app = setup_app();
    let (status, body) = post_json(&app, "/registerDevice", json!({
        "fcmToken": "valid-token-123",
        "platform": "windows"
    })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "ERR_INVALID_PLATFORM");
}

#[tokio::test]
async fn test_register_device_success() {
    let app = setup_app();
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
    let app = setup_app();
    let (status, body) = get_path(&app, "/devices").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "success");
    assert!(body["devices"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_list_devices_with_registered() {
    let app = setup_app();

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
    let app = setup_app();
    let (status, body) = post_json(&app, "/permissions/set", json!({
        "messageBox": "inbox",
        "recipientFee": 50
    })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "success");
}

#[tokio::test]
async fn test_get_permission_not_found() {
    let app = setup_app();
    let (status, body) = get_path(&app, "/permissions/get?messageBox=inbox").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "success");
    assert!(body["permission"].is_null());
}

#[tokio::test]
async fn test_get_permission_after_set() {
    let app = setup_app();

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
    let app = setup_app();

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
    let app = setup_app();
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
    let app = setup_app();
    let (status, body) = get_path(&app, "/permissions/quote").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "ERR_MISSING_PARAMETERS");
}

// ===========================================================================
// auth tests
// ===========================================================================

#[tokio::test]
async fn test_no_auth_returns_401() {
    let app = setup_app();

    // Test POST endpoints without auth
    let (status, body) = post_json_no_auth(&app, "/sendMessage", json!({
        "message": {"body": "test"}
    })).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["code"], "ERR_AUTH_REQUIRED");

    // Test another POST endpoint
    let app = setup_app();
    let (status, _) = post_json_no_auth(&app, "/listMessages", json!({
        "messageBox": "inbox"
    })).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Test GET endpoint without auth
    let app = setup_app();
    let (status, _) = get_path_no_auth(&app, "/devices").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Test permissions GET without auth
    let app = setup_app();
    let (status, _) = get_path_no_auth(&app, "/permissions/get?messageBox=inbox").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}
