use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde_json::Value;
use tracing::error;

use crate::db::queries;
use crate::handlers::helpers::{error_response, AppState, AuthIdentity};
use crate::handlers::response_types::{
    DeviceOut, ListDevicesResponse, RegisterDeviceResponse,
};

/// POST /registerDevice
pub async fn register_device(
    State(state): State<AppState>,
    auth: AuthIdentity,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let identity_key = auth.0;

    // ── Parse fcmToken ────────────────────────────────────────────────
    let fcm_token = match body.get("fcmToken").and_then(|v| v.as_str()) {
        Some(s) if !s.trim().is_empty() => s.trim().to_string(),
        _ => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "ERR_INVALID_FCM_TOKEN",
                "fcmToken is required and must be a non-empty string.",
            )
            .into_response();
        }
    };

    // ── Parse optional deviceId ───────────────────────────────────────
    let device_id = body
        .get("deviceId")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // ── Parse and validate optional platform ──────────────────────────
    let platform = body
        .get("platform")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    if let Some(ref p) = platform {
        if !matches!(p.as_str(), "ios" | "android" | "web") {
            return error_response(
                StatusCode::BAD_REQUEST,
                "ERR_INVALID_PLATFORM",
                "platform must be one of: ios, android, web",
            )
            .into_response();
        }
    }

    // ── Register device ───────────────────────────────────────────────
    let id = match queries::register_device(
        &state.db,
        &identity_key,
        &fcm_token,
        device_id.as_deref(),
        platform.as_deref(),
    )
    .await
    {
        Ok(id) => id,
        Err(e) => {
            error!("failed to register device: {e}");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ERR_DATABASE_ERROR",
                "Failed to register device.",
            )
            .into_response();
        }
    };

    (
        StatusCode::OK,
        Json(RegisterDeviceResponse {
            status: "success".to_string(),
            message: "Device registered successfully for push notifications".to_string(),
            device_id: id,
        }),
    )
        .into_response()
}

/// GET /devices
pub async fn list_devices(
    State(state): State<AppState>,
    auth: AuthIdentity,
) -> impl IntoResponse {
    let identity_key = auth.0;

    let devices = match queries::list_devices(&state.db, &identity_key).await {
        Ok(d) => d,
        Err(e) => {
            error!("failed to list devices: {e}");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ERR_DATABASE_ERROR",
                "Failed to retrieve devices.",
            )
            .into_response();
        }
    };

    let out: Vec<DeviceOut> = devices
        .into_iter()
        .map(|d| {
            // Mask the FCM token: show only last 10 chars.
            let masked_token = if d.fcm_token.len() > 10 {
                format!("...{}", &d.fcm_token[d.fcm_token.len() - 10..])
            } else {
                d.fcm_token
            };

            DeviceOut {
                id: d.id,
                device_id: d.device_id,
                platform: d.platform,
                fcm_token: masked_token,
                active: d.active,
                created_at: d.created_at,
                updated_at: d.updated_at,
                last_used: d.last_used.unwrap_or_default(),
            }
        })
        .collect();

    (
        StatusCode::OK,
        Json(ListDevicesResponse {
            status: "success".to_string(),
            devices: out,
        }),
    )
        .into_response()
}
