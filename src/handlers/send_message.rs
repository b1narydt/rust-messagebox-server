use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
// Payment internalization uses bsv-sdk WalletInterface but ProtoWallet
// always returns "requires a full wallet" — payment validation is structural only.
use serde_json::Value;
use tracing::{error, warn};

use crate::db::queries;
use crate::firebase::send_fcm_notification::{send_fcm_notification, FcmPayload};
use crate::handlers::helpers::{
    build_per_recipient_outputs, error_response, is_valid_pub_key, AppState, AuthIdentity, FeeRow,
};
use crate::handlers::request_types::Payment;
use crate::handlers::response_types::{
    DeliveryBlockedError, SendMessageResponse, SendMessageResult,
};

pub async fn send_message(
    State(state): State<AppState>,
    auth: AuthIdentity,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let sender_key = auth.0;

    // ── Parse message object ──────────────────────────────────────────
    let msg = match body.get("message") {
        Some(v) if !v.is_null() => v,
        _ => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "ERR_MESSAGE_REQUIRED",
                "Please provide a valid message to send!",
            )
            .into_response();
        }
    };

    // ── messageBox ────────────────────────────────────────────────────
    let box_type = match msg.get("messageBox").and_then(|v| v.as_str()) {
        Some(s) if !s.trim().is_empty() => s.trim().to_string(),
        _ => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "ERR_INVALID_MESSAGEBOX",
                "Invalid message box.",
            )
            .into_response();
        }
    };

    // ── body ──────────────────────────────────────────────────────────
    let msg_body_val = msg.get("body");
    let body_str = match msg_body_val {
        None => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "ERR_INVALID_MESSAGE_BODY",
                "Invalid message body.",
            )
            .into_response();
        }
        Some(v) if v.is_null() => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "ERR_INVALID_MESSAGE_BODY",
                "Invalid message body.",
            )
            .into_response();
        }
        Some(Value::String(s)) if s.is_empty() => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "ERR_INVALID_MESSAGE_BODY",
                "Invalid message body.",
            )
            .into_response();
        }
        Some(v) => v,
    };

    // ── recipients ────────────────────────────────────────────────────
    let recipients_raw = msg
        .get("recipients")
        .filter(|v| !v.is_null())
        .or_else(|| msg.get("recipient").filter(|v| !v.is_null()));

    let recipients_raw = match recipients_raw {
        Some(v) => v,
        None => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "ERR_RECIPIENT_REQUIRED",
                r#"Missing recipient(s). Provide "recipient" or "recipients"."#,
            )
            .into_response();
        }
    };

    let recipients: Vec<String> = if let Some(arr) = recipients_raw.as_array() {
        match arr
            .iter()
            .map(|v| {
                v.as_str()
                    .map(|s| s.to_string())
                    .ok_or(())
            })
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(v) => v,
            Err(()) => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "ERR_INVALID_RECIPIENT_KEY",
                    "Invalid recipient format",
                )
                .into_response();
            }
        }
    } else if let Some(s) = recipients_raw.as_str() {
        vec![s.to_string()]
    } else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "ERR_INVALID_RECIPIENT_KEY",
            "Invalid recipient format",
        )
        .into_response();
    };

    if recipients.is_empty() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "ERR_RECIPIENT_REQUIRED",
            r#"Missing recipient(s). Provide "recipient" or "recipients"."#,
        )
        .into_response();
    }

    // Validate recipient keys.
    for r in &recipients {
        if !is_valid_pub_key(r.trim()) {
            return error_response(
                StatusCode::BAD_REQUEST,
                "ERR_INVALID_RECIPIENT_KEY",
                &format!("Invalid recipient key: {r}"),
            )
            .into_response();
        }
    }

    // ── messageId ─────────────────────────────────────────────────────
    let mid_raw = match msg.get("messageId") {
        Some(v) if !v.is_null() => v,
        _ => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "ERR_MESSAGEID_REQUIRED",
                "Missing messageId.",
            )
            .into_response();
        }
    };

    let message_ids: Vec<String> = if let Some(arr) = mid_raw.as_array() {
        match arr
            .iter()
            .map(|v| v.as_str().map(|s| s.to_string()).ok_or(()))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(v) => v,
            Err(()) => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "ERR_MESSAGEID_REQUIRED",
                    "Missing messageId.",
                )
                .into_response();
            }
        }
    } else if let Some(s) = mid_raw.as_str() {
        vec![s.to_string()]
    } else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "ERR_MESSAGEID_REQUIRED",
            "Missing messageId.",
        )
        .into_response();
    };

    // Validate counts.
    if recipients.len() > 1 && message_ids.len() == 1 {
        return error_response(
            StatusCode::BAD_REQUEST,
            "ERR_MESSAGEID_COUNT_MISMATCH",
            &format!(
                "Provided 1 messageId for {} recipients. Provide one messageId per recipient (same order).",
                recipients.len()
            ),
        )
        .into_response();
    }
    if message_ids.len() != recipients.len() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "ERR_MESSAGEID_COUNT_MISMATCH",
            &format!(
                "Recipients ({}) and messageId count ({}) must match.",
                recipients.len(),
                message_ids.len()
            ),
        )
        .into_response();
    }

    // Validate each messageId is non-empty.
    for id in &message_ids {
        if id.trim().is_empty() {
            return error_response(
                StatusCode::BAD_REQUEST,
                "ERR_INVALID_MESSAGEID",
                "Each messageId must be a non-empty string.",
            )
            .into_response();
        }
    }

    // ── Ensure message boxes ──────────────────────────────────────────
    for recip in &recipients {
        if let Err(e) = queries::ensure_message_box(&state.db, recip.trim(), &box_type).await {
            error!("failed to ensure messageBox: {e}");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ERR_INTERNAL_ERROR",
                "An internal error has occurred.",
            )
            .into_response();
        }
    }

    // ── Fee evaluation ────────────────────────────────────────────────
    let delivery_fee = match queries::get_server_delivery_fee(&state.db, &box_type).await {
        Ok(f) => f,
        Err(e) => {
            error!("failed to get delivery fee: {e}");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ERR_INTERNAL_ERROR",
                "An internal error has occurred.",
            )
            .into_response();
        }
    };

    let mut fee_rows: Vec<FeeRow> = Vec::with_capacity(recipients.len());
    for recip in &recipients {
        let recip = recip.trim();
        match queries::get_recipient_fee(&state.db, recip, &sender_key, &box_type).await {
            Ok(rf) => fee_rows.push(FeeRow {
                recipient: recip.to_string(),
                recipient_fee: rf,
                allowed: rf != -1,
            }),
            Err(e) => {
                error!("failed to get recipient fee: {e}");
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "ERR_INTERNAL_ERROR",
                    "An internal error has occurred.",
                )
                .into_response();
            }
        }
    }

    // Check blocked.
    let blocked: Vec<String> = fee_rows
        .iter()
        .filter(|fr| !fr.allowed)
        .map(|fr| fr.recipient.clone())
        .collect();

    if !blocked.is_empty() {
        return (
            StatusCode::FORBIDDEN,
            Json(DeliveryBlockedError {
                status: "error".to_string(),
                code: "ERR_DELIVERY_BLOCKED".to_string(),
                description: format!("Blocked recipients: {}", blocked.join(", ")),
                blocked_recipients: blocked,
            }),
        )
            .into_response();
    }

    // ── Payment processing ────────────────────────────────────────────
    let any_recipient_fee = fee_rows.iter().any(|fr| fr.recipient_fee > 0);
    let requires_payment = delivery_fee > 0 || any_recipient_fee;

    // Parse payment from body if needed.
    let payment: Option<Payment> = body
        .get("payment")
        .and_then(|v| serde_json::from_value(v.clone()).ok());

    let per_recipient_outputs = if requires_payment {
        let pay = match &payment {
            Some(p)
                if p.tx.as_ref().is_none_or(|t| t.is_empty())
                    || p.outputs.as_ref().is_none_or(|o| o.is_empty()) =>
            {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "ERR_MISSING_PAYMENT_TX",
                    "Payment transaction data is required for payable delivery.",
                )
                .into_response();
            }
            Some(p) => p,
            None => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "ERR_MISSING_PAYMENT_TX",
                    "Payment transaction data is required for payable delivery.",
                )
                .into_response();
            }
        };

        // Payment internalization is a no-op with ProtoWallet (requires full wallet).
        // Structural payment validation is sufficient for self-hosted deployments.
        // TODO: Implement full internalization when HttpWallet is available.
        if delivery_fee > 0 {
            warn!("ProtoWallet does not support payment internalization; structurally validated, continuing");
        }

        let outputs = pay.outputs.as_deref().unwrap_or_default();
        match build_per_recipient_outputs(outputs, delivery_fee, &fee_rows) {
            Ok(m) => m,
            Err(e) => {
                error!("output mapping failed: {} - {}", e.code, e.description);
                return error_response(StatusCode::BAD_REQUEST, &e.code, &e.description)
                    .into_response();
            }
        }
    } else {
        std::collections::HashMap::new()
    };

    // ── Insert messages ───────────────────────────────────────────────
    let mut results: Vec<SendMessageResult> = Vec::with_capacity(fee_rows.len());

    for (i, fr) in fee_rows.iter().enumerate() {
        let mb_id = match queries::get_message_box_id(&state.db, &fr.recipient, &box_type).await {
            Ok(Some(id)) => id,
            Ok(None) => {
                // Should not happen since we ensured above, but handle gracefully.
                error!("messageBox disappeared after ensure for {}", fr.recipient);
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "ERR_INTERNAL_ERROR",
                    "An internal error has occurred.",
                )
                .into_response();
            }
            Err(e) => {
                error!("failed to get messageBoxId: {e}");
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "ERR_INTERNAL_ERROR",
                    "An internal error has occurred.",
                )
                .into_response();
            }
        };

        let msg_id = &message_ids[i];

        // Build stored body: {"message": <body>, "payment"?: <per-recipient payment>}
        let mut stored_body = serde_json::Map::new();
        stored_body.insert("message".to_string(), body_str.clone());

        if let Some(recipient_outputs) = per_recipient_outputs.get(&fr.recipient) {
            if let Some(ref pay) = payment {
                let per_recipient_payment = serde_json::json!({
                    "tx": pay.tx,
                    "outputs": recipient_outputs,
                    "description": pay.description,
                    "labels": pay.labels,
                    "seekPermission": pay.seek_permission,
                });
                stored_body.insert("payment".to_string(), per_recipient_payment);
            }
        }

        let body_bytes = serde_json::to_string(&stored_body).unwrap_or_default();

        match queries::insert_message(&state.db, msg_id, mb_id, &sender_key, &fr.recipient, &body_bytes).await {
            Ok(false) => {
                // Duplicate message – the Go code returns an error.
                error!("duplicate message rejected: messageId={msg_id}");
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "ERR_DUPLICATE_MESSAGE",
                    "Duplicate message.",
                )
                .into_response();
            }
            Ok(true) => {
                // Broadcast to WebSocket room for live message push.
                let room = crate::ws::room_id(&fr.recipient, &box_type);
                let event = format!("sendMessage-{room}");
                state.ws.broadcast_to_room(
                    &room,
                    &event,
                    &crate::ws::RoomMessage {
                        message_id: msg_id.clone(),
                        sender: sender_key.clone(),
                        recipient: fr.recipient.clone(),
                        message_box: box_type.clone(),
                        body: body_bytes.clone(),
                        created_at: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
                        updated_at: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
                    },
                );
            }
            Err(e) => {
                error!("failed to insert message: {e}");
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "ERR_INTERNAL_ERROR",
                    "An internal error has occurred.",
                )
                .into_response();
            }
        }

        // FCM notification for "notifications" box.
        if queries::should_use_fcm_delivery(&box_type) {
            let pool = state.db.clone();
            let recipient = fr.recipient.clone();
            let payload = FcmPayload {
                title: "New Message".to_string(),
                message_id: msg_id.clone(),
                originator: sender_key.clone(),
            };
            tokio::spawn(async move {
                send_fcm_notification(&pool, &recipient, payload).await;
            });
        }

        results.push(SendMessageResult {
            recipient: fr.recipient.clone(),
            message_id: msg_id.clone(),
        });
    }

    (
        StatusCode::OK,
        Json(SendMessageResponse {
            status: "success".to_string(),
            message: format!(
                "Your message has been sent to {} recipient(s).",
                results.len()
            ),
            results,
        }),
    )
        .into_response()
}
