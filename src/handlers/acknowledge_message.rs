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
use crate::handlers::response_types::SuccessResponse;

pub async fn acknowledge_message(
    State(state): State<AppState>,
    auth: AuthIdentity,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let identity_key = auth.0;

    // ── Parse messageIds ──────────────────────────────────────────────
    let message_ids: Vec<String> = match body.get("messageIds") {
        Some(Value::Array(arr)) if !arr.is_empty() => {
            let mut ids = Vec::with_capacity(arr.len());
            for v in arr {
                match v.as_str() {
                    Some(s) => ids.push(s.to_string()),
                    None => {
                        return error_response(
                            StatusCode::BAD_REQUEST,
                            "ERR_INVALID_MESSAGE_ID",
                            "Message IDs must be formatted as an array of strings!",
                        )
                        .into_response();
                    }
                }
            }
            ids
        }
        _ => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "ERR_MESSAGE_ID_REQUIRED",
                "Please provide the ID of the message(s) to acknowledge!",
            )
            .into_response();
        }
    };

    // Validate each ID is non-empty.
    for id in &message_ids {
        if id.trim().is_empty() {
            return error_response(
                StatusCode::BAD_REQUEST,
                "ERR_INVALID_MESSAGE_ID",
                "Message IDs must be formatted as an array of strings!",
            )
            .into_response();
        }
    }

    // ── Acknowledge ───────────────────────────────────────────────────
    let deleted = match queries::acknowledge_messages(&state.db, &identity_key, &message_ids) {
        Ok(n) => n,
        Err(e) => {
            error!("failed to acknowledge messages: {e}");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ERR_INTERNAL_ERROR",
                "An internal error has occurred while acknowledging the message",
            )
            .into_response();
        }
    };

    if deleted == 0 {
        return error_response(
            StatusCode::BAD_REQUEST,
            "ERR_INVALID_ACKNOWLEDGMENT",
            "Message not found!",
        )
        .into_response();
    }

    (
        StatusCode::OK,
        Json(SuccessResponse {
            status: "success".to_string(),
        }),
    )
        .into_response()
}
