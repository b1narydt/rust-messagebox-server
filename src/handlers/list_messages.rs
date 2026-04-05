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
use crate::handlers::response_types::{ListMessagesResponse, MessageOut};

pub async fn list_messages(
    State(state): State<AppState>,
    auth: AuthIdentity,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let identity_key = auth.0;

    // ── Parse messageBox ──────────────────────────────────────────────
    let message_box = match body.get("messageBox").and_then(|v| v.as_str()) {
        Some(s) if !s.trim().is_empty() => s.trim().to_string(),
        _ => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "ERR_MESSAGEBOX_REQUIRED",
                "Please provide the name of a valid MessageBox!",
            )
            .into_response();
        }
    };

    // ── Look up message box ───────────────────────────────────────────
    let mb_id = match queries::get_message_box_id(&state.db, &identity_key, &message_box) {
        Ok(Some(id)) => id,
        Ok(None) => {
            // Box does not exist yet — return empty list, not an error.
            return (
                StatusCode::OK,
                Json(ListMessagesResponse {
                    status: "success".to_string(),
                    messages: vec![],
                }),
            )
                .into_response();
        }
        Err(e) => {
            error!("failed to get messageBox: {e}");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ERR_INTERNAL_ERROR",
                "An internal error has occurred while listing messages.",
            )
            .into_response();
        }
    };

    // ── Fetch messages ────────────────────────────────────────────────
    let rows = match queries::list_messages(&state.db, &identity_key, mb_id) {
        Ok(r) => r,
        Err(e) => {
            error!("failed to list messages: {e}");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ERR_INTERNAL_ERROR",
                "An internal error has occurred while listing messages.",
            )
            .into_response();
        }
    };

    let messages: Vec<MessageOut> = rows
        .into_iter()
        .map(|m| MessageOut {
            message_id: m.message_id,
            body: m.body,
            sender: m.sender,
            created_at: m.created_at,
            updated_at: m.updated_at,
        })
        .collect();

    (
        StatusCode::OK,
        Json(ListMessagesResponse {
            status: "success".to_string(),
            messages,
        }),
    )
        .into_response()
}
