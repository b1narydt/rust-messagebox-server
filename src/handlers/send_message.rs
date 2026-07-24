use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use bsv::primitives::public_key::PublicKey;
use bsv::wallet::interfaces::{InternalizeActionArgs, InternalizeOutput, Payment as SdkPayment};
use serde_json::Value;
use tracing::{debug, error, warn};

use crate::db::queries;
use crate::firebase::send_fcm_notification::{send_fcm_notification, FcmPayload};
use crate::handlers::helpers::{
    build_per_recipient_outputs, error_response, is_valid_pub_key, AppState, AuthIdentity, FeeRow,
};
use crate::handlers::request_types::Payment;
use crate::handlers::response_types::{
    DeliveryBlockedError, SendMessageResponse, SendMessageResult,
};

#[tracing::instrument(
    name = "http_send_message",
    skip_all,
    fields(sender = %auth.0, message_box = tracing::field::Empty, recipients = tracing::field::Empty)
)]
pub async fn send_message(
    State(state): State<AppState>,
    auth: AuthIdentity,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    // In-flight marker for graceful drain: an authenticated client's HTTP
    // send counts as in-flight work — drain waits for it (bounded), and
    // admission control never gates this route.
    let _send_guard = state.ws.ops().begin_send();

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

    tracing::Span::current().record("message_box", tracing::field::display(&box_type));

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
        // H10 (TS parity): a whitespace-only string body is rejected exactly
        // like an empty one (TS checks `.trim() === ''`).
        Some(Value::String(s)) if s.trim().is_empty() => {
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
            .map(|v| v.as_str().map(|s| s.to_string()).ok_or(()))
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

    tracing::Span::current().record("recipients", recipients.len());

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
    // @bsv wire-parity compat surface, NOT an enforced ACL: the WS sendMessage
    // path bypasses this entirely. `inbox`/`payment_inbox` seed at fee 0, but
    // `notifications` seeds at 10 sats (TS parity), so this route DOES gate on
    // payment for that box. See the module doc on handlers::permissions.
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
        // Missing payment (or payment without transaction bytes) → the TS
        // ERR_MISSING_PAYMENT_TX. Empty outputs are handled per-case below so
        // the delivery-fee case can return the distinct TS code (H4).
        let pay = match &payment {
            Some(p) if p.tx.as_ref().is_some_and(|t| !t.is_empty()) => p,
            _ => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "ERR_MISSING_PAYMENT_TX",
                    "Payment transaction data is required for payable delivery.",
                )
                .into_response();
            }
        };

        // H4 (TS parity): a delivery fee is due but the payment carries no
        // outputs at all → 400 ERR_MISSING_DELIVERY_OUTPUT (previously
        // collapsed into ERR_MISSING_PAYMENT_TX).
        if delivery_fee > 0 && pay.outputs.as_ref().is_none_or(|o| o.is_empty()) {
            return error_response(
                StatusCode::BAD_REQUEST,
                "ERR_MISSING_DELIVERY_OUTPUT",
                "Payment is missing the server delivery-fee output.",
            )
            .into_response();
        }

        // Internalize the server's delivery-fee output so the payment is credited
        // to the server's wallet at the remote storage backend.
        if delivery_fee > 0 {
            // The server's delivery-fee output is always the first output (index 0).
            let server_output = match pay.outputs.as_deref().and_then(|o| o.first()) {
                Some(o) => o,
                None => {
                    return error_response(
                        StatusCode::BAD_REQUEST,
                        "ERR_MISSING_DELIVERY_OUTPUT",
                        "Payment is missing the server delivery-fee output.",
                    )
                    .into_response();
                }
            };

            // Extract derivation parameters from the payment remittance on the
            // server's output. The sender must have included these for the server
            // to verify and claim the output.
            let remittance = match &server_output.payment_remittance {
                Some(r) => r,
                None => {
                    return error_response(
                        StatusCode::BAD_REQUEST,
                        "ERR_INVALID_PAYMENT",
                        "Server delivery-fee output is missing payment remittance.",
                    )
                    .into_response();
                }
            };

            let derivation_prefix = match &remittance.derivation_prefix {
                Some(p) if !p.is_empty() => p.as_bytes().to_vec(),
                _ => {
                    return error_response(
                        StatusCode::BAD_REQUEST,
                        "ERR_INVALID_PAYMENT",
                        "Server delivery-fee output is missing derivationPrefix.",
                    )
                    .into_response();
                }
            };

            let derivation_suffix = match &remittance.derivation_suffix {
                Some(s) if !s.is_empty() => s.as_bytes().to_vec(),
                _ => {
                    return error_response(
                        StatusCode::BAD_REQUEST,
                        "ERR_INVALID_PAYMENT",
                        "Server delivery-fee output is missing derivationSuffix.",
                    )
                    .into_response();
                }
            };

            let sender_pub_key = match &remittance.sender_identity_key {
                Some(k) if !k.is_empty() => match PublicKey::from_string(k) {
                    Ok(pk) => pk,
                    Err(_) => {
                        return error_response(
                            StatusCode::BAD_REQUEST,
                            "ERR_INVALID_PAYMENT",
                            "senderIdentityKey in payment remittance is not a valid public key.",
                        )
                        .into_response();
                    }
                },
                _ => {
                    return error_response(
                        StatusCode::BAD_REQUEST,
                        "ERR_INVALID_PAYMENT",
                        "Server delivery-fee output is missing senderIdentityKey.",
                    )
                    .into_response();
                }
            };

            let tx_bytes = pay.tx.clone().unwrap_or_default();

            let internalize_args = InternalizeActionArgs {
                tx: tx_bytes,
                outputs: vec![InternalizeOutput::WalletPayment {
                    output_index: server_output.output_index,
                    payment: SdkPayment {
                        derivation_prefix,
                        derivation_suffix,
                        sender_identity_key: sender_pub_key,
                    },
                }],
                description: format!("Delivery fee payment for message box: {box_type}"),
                labels: vec![],
                seek_permission: Some(false).into(),
            };

            // H3 (TS parity): internalize NOT ACCEPTED → 400
            // ERR_INSUFFICIENT_PAYMENT; internalize EXCEPTION → 500
            // ERR_INTERNALIZE_FAILED. (Previously both collapsed into a
            // Rust-only 500 ERR_INTERNALIZATION_FAILED.)
            match state
                .funded_wallet
                .internalize_action(internalize_args, None)
                .await
            {
                Ok(result) => {
                    if !result.accepted {
                        error!("internalize_action rejected delivery fee payment");
                        return error_response(
                            StatusCode::BAD_REQUEST,
                            "ERR_INSUFFICIENT_PAYMENT",
                            "The delivery fee payment was not accepted.",
                        )
                        .into_response();
                    }
                }
                Err(e) => {
                    error!("internalize_action failed: {e}");
                    return error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "ERR_INTERNALIZE_FAILED",
                        "The server failed to process the delivery fee payment.",
                    )
                    .into_response();
                }
            }
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

    // ── Push live, then persist asynchronously ────────────────────────
    //
    // All gating checks (recipient validation, messageBox ensure, fee/permission
    // lookup, blocked-recipient rejection, and payment internalization) have
    // already run synchronously ABOVE — a message that should be rejected for
    // policy/fee reasons never reaches this loop. From here we PUSH-LIVE-FIRST
    // and PERSIST-ASYNC: broadcast to the connected recipient immediately
    // (off the DB hot path) and hand the durable INSERT to the background
    // persist worker. Durability is preserved by the worker (bounded retry,
    // ERROR on permanent failure, inline fallback under backpressure — never a
    // silent drop), so listMessages still serves offline / re-polling recipients.
    //
    // Dedup: the previous synchronous ERR_DUPLICATE_MESSAGE check is gone because
    // the INSERT now happens after the response is built. The unique messageId
    // constraint (INSERT IGNORE in the worker) still prevents a second row, and
    // the client treats a duplicate as idempotent success — so a retried send is
    // safe. The worker logs duplicates at debug.
    let mut results: Vec<SendMessageResult> = Vec::with_capacity(fee_rows.len());

    for (i, fr) in fee_rows.iter().enumerate() {
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

        // Push-live-first: broadcast to the connected recipient before the DB write.
        let room = crate::ws::room_id(&fr.recipient, &box_type);
        let event = format!("sendMessage-{room}");
        let now = chrono::Utc::now()
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
            .to_string();
        state
            .ws
            .broadcast_to_room(
                &room,
                &event,
                &crate::ws::RoomMessage {
                    message_id: msg_id.clone(),
                    sender: sender_key.clone(),
                    recipient: fr.recipient.clone(),
                    message_box: box_type.clone(),
                    body: body_bytes.clone(),
                    created_at: now.clone(),
                    updated_at: now,
                },
            )
            .await;

        // Persist-async: durable INSERT off the hot path (see crate::persist).
        // This path builds a richer stored body ({"message", "payment"?}), so it
        // uses `with_stored_body` — the body is persisted VERBATIM (no extra
        // `{"message": ...}` wrap, which would double-wrap it).
        let enqueued = state
            .ws
            .persist_async(crate::persist::PersistJob::with_stored_body(
                msg_id.clone(),
                fr.recipient.clone(),
                box_type.clone(),
                sender_key.clone(),
                body_bytes,
            ))
            .await;

        // Observe fast-path bypass / dead-letter so it isn't silent. `Queued`
        // is the happy path; everything else means an inline write (DB
        // backpressure) or a dead-letter to disk.
        match enqueued {
            crate::persist::Enqueued::Queued => {}
            crate::persist::Enqueued::InlineOk => debug!(
                msg_id = %msg_id, recipient = %fr.recipient,
                "persist: fast path bypassed — persisted inline (DB backpressure), row durably written"
            ),
            crate::persist::Enqueued::InlineDeadLettered => warn!(
                msg_id = %msg_id, recipient = %fr.recipient,
                "persist: inline write exhausted transient retries — dead-lettered to disk; row NOT yet in MySQL"
            ),
            crate::persist::Enqueued::DeadLettered => error!(
                msg_id = %msg_id, recipient = %fr.recipient,
                "persist: inline write hit a permanent error — dead-lettered to disk; row NOT in MySQL"
            ),
        }

        // FCM push for the `notifications` box (§4.3): after the send,
        // best-effort, never failing the response. Fire-and-forget, but the
        // result summary is inspected so a total delivery failure is
        // observable here — message durability does not depend on FCM.
        if queries::should_use_fcm_delivery(&box_type) {
            let pool = state.db.clone();
            let recipient = fr.recipient.clone();
            let msg_id_for_fcm = msg_id.clone();
            let payload = FcmPayload {
                title: "New Message".to_string(),
                message_id: msg_id.clone(),
                originator: sender_key.clone(),
            };
            tokio::spawn(async move {
                let result = send_fcm_notification(&pool, &recipient, payload).await;
                if !result.success {
                    warn!(
                        msg_id = %msg_id_for_fcm,
                        recipient = %recipient,
                        error = result.error.as_deref().unwrap_or("unknown"),
                        "FCM notification not delivered (best-effort; live push + persistence are unaffected)"
                    );
                }
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
