use std::collections::HashMap;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::Value;
use tracing::error;

use crate::db::queries;
use crate::handlers::helpers::{error_response, is_valid_pub_key, AppState, AuthIdentity};
use crate::handlers::response_types::{
    GetPermissionResponse, ListPermissionsResponse, PermissionDetail, PermissionDetailList,
    QuoteEntry, QuoteMultiResponse, QuoteSingle, QuoteSingleResponse, QuoteTotals,
    SetPermissionResponse,
};

// ───────────────────────────────────────────────────────────────────────────
// POST /permissions/set
// ───────────────────────────────────────────────────────────────────────────

pub async fn set_permission(
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
                "ERR_INVALID_REQUEST",
                "messageBox (string) and recipientFee (number) are required. sender (string) is optional for box-wide settings.",
            )
            .into_response();
        }
    };

    // ── Parse recipientFee ────────────────────────────────────────────
    let fee = match body.get("recipientFee") {
        Some(v) => match v.as_i64() {
            Some(n) => n,
            None => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "ERR_INVALID_REQUEST",
                    "messageBox (string) and recipientFee (number) are required. sender (string) is optional for box-wide settings.",
                )
                .into_response();
            }
        },
        None => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "ERR_INVALID_REQUEST",
                "messageBox (string) and recipientFee (number) are required. sender (string) is optional for box-wide settings.",
            )
            .into_response();
        }
    };

    // ── Parse optional sender ─────────────────────────────────────────
    let sender = body
        .get("sender")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    if let Some(ref s) = sender {
        if !is_valid_pub_key(s) {
            return error_response(
                StatusCode::BAD_REQUEST,
                "ERR_INVALID_PUBLIC_KEY",
                "Invalid sender public key format.",
            )
            .into_response();
        }
    }

    // ── Persist permission ────────────────────────────────────────────
    if let Err(e) = queries::set_message_permission(
        &state.db,
        &identity_key,
        sender.as_deref(),
        &message_box,
        fee,
    )
    .await
    {
        error!("failed to set permission: {e}");
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "ERR_DATABASE_ERROR",
            "Failed to update message permission.",
        )
        .into_response();
    }

    // ── Build description matching Go output ──────────────────────────
    let is_box_wide = sender.is_none();
    let (sender_text, action_text) = if is_box_wide {
        ("all senders".to_string(), "Box-wide default for")
    } else {
        (sender.as_deref().unwrap().to_string(), "Messages from")
    };

    let description = match fee {
        -1 => {
            if is_box_wide {
                format!("{action_text} {sender_text} to {message_box} is now blocked.")
            } else {
                format!("{action_text} {sender_text} to {message_box} are now blocked.")
            }
        }
        0 => {
            if is_box_wide {
                format!("{action_text} {sender_text} to {message_box} is now always allowed.")
            } else {
                format!("{action_text} {sender_text} to {message_box} are now always allowed.")
            }
        }
        _ => {
            if is_box_wide {
                format!("{action_text} {sender_text} to {message_box} now requires {fee} satoshis.")
            } else {
                format!("{action_text} {sender_text} to {message_box} now require {fee} satoshis.")
            }
        }
    };

    (
        StatusCode::OK,
        Json(SetPermissionResponse {
            status: "success".to_string(),
            description,
        }),
    )
        .into_response()
}

// ───────────────────────────────────────────────────────────────────────────
// GET /permissions/get
// ───────────────────────────────────────────────────────────────────────────

pub async fn get_permission(
    State(state): State<AppState>,
    auth: AuthIdentity,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let identity_key = auth.0;

    // ── Parse messageBox ──────────────────────────────────────────────
    let message_box = match params.get("messageBox").filter(|s| !s.is_empty()) {
        Some(s) => s.clone(),
        None => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "ERR_MISSING_PARAMETERS",
                "messageBox parameter is required.",
            )
            .into_response();
        }
    };

    // ── Parse optional sender ─────────────────────────────────────────
    let sender = params
        .get("sender")
        .filter(|s| !s.is_empty())
        .cloned();

    if let Some(ref s) = sender {
        if !is_valid_pub_key(s) {
            return error_response(
                StatusCode::BAD_REQUEST,
                "ERR_INVALID_PUBLIC_KEY",
                "Invalid sender public key format.",
            )
            .into_response();
        }
    }

    // ── Query permission ──────────────────────────────────────────────
    let perm = match queries::get_permission(
        &state.db,
        &identity_key,
        sender.as_deref(),
        &message_box,
    )
    .await
    {
        Ok(p) => p,
        Err(e) => {
            error!("failed to get permission: {e}");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ERR_INTERNAL_ERROR",
                "An internal error has occurred.",
            )
            .into_response();
        }
    };

    match perm {
        Some(p) => {
            let status = match p.recipient_fee {
                -1 => "blocked",
                0 => "always_allow",
                _ => "payment_required",
            };

            let desc = if sender.is_some() {
                format!(
                    "Permission setting found for sender {} to {}.",
                    sender.as_deref().unwrap(),
                    message_box
                )
            } else {
                format!("Box-wide permission setting found for {}.", message_box)
            };

            (
                StatusCode::OK,
                Json(GetPermissionResponse {
                    status: "success".to_string(),
                    description: desc,
                    permission: Some(PermissionDetail {
                        sender: p.sender,
                        message_box: p.message_box,
                        recipient_fee: p.recipient_fee,
                        status: status.to_string(),
                        created_at: p.created_at,
                        updated_at: p.updated_at,
                    }),
                }),
            )
                .into_response()
        }
        None => {
            let desc = if sender.is_some() {
                format!(
                    "No permission setting found for sender {} to {}.",
                    sender.as_deref().unwrap(),
                    message_box
                )
            } else {
                format!("No box-wide permission setting found for {}.", message_box)
            };

            (
                StatusCode::OK,
                Json(GetPermissionResponse {
                    status: "success".to_string(),
                    description: desc,
                    permission: None,
                }),
            )
                .into_response()
        }
    }
}

// ───────────────────────────────────────────────────────────────────────────
// GET /permissions/list
// ───────────────────────────────────────────────────────────────────────────

pub async fn list_permissions(
    State(state): State<AppState>,
    auth: AuthIdentity,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let identity_key = auth.0;

    let message_box = params
        .get("messageBox")
        .filter(|s| !s.is_empty())
        .cloned();

    // ── Parse limit ───────────────────────────────────────────────────
    let limit: i64 = match params.get("limit") {
        Some(s) => match s.parse::<i64>() {
            Ok(v) if (1..=1000).contains(&v) => v,
            _ => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "ERR_INVALID_LIMIT",
                    "Limit must be a number between 1 and 1000",
                )
                .into_response();
            }
        },
        None => 100,
    };

    // ── Parse offset ──────────────────────────────────────────────────
    let offset: i64 = match params.get("offset") {
        Some(s) => match s.parse::<i64>() {
            Ok(v) if v >= 0 => v,
            _ => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "ERR_INVALID_OFFSET",
                    "Offset must be a non-negative number",
                )
                .into_response();
            }
        },
        None => 0,
    };

    // ── Parse sort order ──────────────────────────────────────────────
    let order = match params.get("createdAtOrder").map(|s| s.as_str()) {
        Some("asc") => "asc",
        _ => "desc",
    };

    // ── Query ─────────────────────────────────────────────────────────
    let (perms, total) = match queries::list_permissions(
        &state.db,
        &identity_key,
        message_box.as_deref(),
        limit,
        offset,
        order,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            error!("failed to list permissions: {e}");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ERR_LIST_PERMISSIONS_FAILED",
                "Failed to list permissions",
            )
            .into_response();
        }
    };

    let out: Vec<PermissionDetailList> = perms
        .into_iter()
        .map(|p| PermissionDetailList {
            sender: p.sender,
            message_box: p.message_box,
            recipient_fee: p.recipient_fee,
            created_at: p.created_at,
            updated_at: p.updated_at,
        })
        .collect();

    (
        StatusCode::OK,
        Json(ListPermissionsResponse {
            status: "success".to_string(),
            permissions: out,
            total_count: total,
        }),
    )
        .into_response()
}

// ───────────────────────────────────────────────────────────────────────────
// GET /permissions/quote
// ───────────────────────────────────────────────────────────────────────────

/// The quote endpoint accepts repeated `recipient` query parameters. axum's
/// `Query<HashMap>` collapses duplicates, so we use the raw query string.
pub async fn get_quote(
    State(state): State<AppState>,
    auth: AuthIdentity,
    raw_query: axum::extract::RawQuery,
) -> Response {
    let sender_key = auth.0;

    // Parse query string manually to support repeated `recipient` params.
    let query_string = raw_query.0.unwrap_or_default();
    let pairs: Vec<(String, String)> = form_urlencoded::parse(query_string.as_bytes())
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

    let message_box: Option<String> = pairs
        .iter()
        .find(|(k, _)| k == "messageBox")
        .map(|(_, v)| v.clone())
        .filter(|s| !s.is_empty());

    let recipients: Vec<String> = pairs
        .iter()
        .filter(|(k, _)| k == "recipient")
        .map(|(_, v)| v.clone())
        .collect();

    if message_box.is_none() || recipients.is_empty() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "ERR_MISSING_PARAMETERS",
            "recipient and messageBox parameters are required.",
        )
        .into_response();
    }
    let message_box = message_box.unwrap();

    // Validate recipient keys.
    let invalid_indexes: Vec<usize> = recipients
        .iter()
        .enumerate()
        .filter(|(_, r)| !is_valid_pub_key(r.trim()))
        .map(|(i, _)| i)
        .collect();

    if !invalid_indexes.is_empty() {
        let idx_strs: Vec<String> = invalid_indexes.iter().map(|i| i.to_string()).collect();
        return error_response(
            StatusCode::BAD_REQUEST,
            "ERR_INVALID_PUBLIC_KEY",
            &format!(
                "Invalid recipient public key at index(es): {}.",
                idx_strs.join(", ")
            ),
        )
        .into_response();
    }

    // ── Get delivery fee ──────────────────────────────────────────────
    let delivery_fee = match queries::get_server_delivery_fee(&state.db, &message_box).await {
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

    // ── Single recipient: legacy response shape ───────────────────────
    if recipients.len() == 1 {
        let recipient_fee = match queries::get_recipient_fee(
            &state.db,
            &recipients[0],
            &sender_key,
            &message_box,
        )
        .await
        {
            Ok(f) => f,
            Err(e) => {
                error!("failed to get recipient fee: {e}");
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "ERR_INTERNAL_ERROR",
                    "An internal error has occurred.",
                )
                .into_response();
            }
        };

        return (
            StatusCode::OK,
            Json(QuoteSingleResponse {
                status: "success".to_string(),
                description: "Message delivery quote generated.".to_string(),
                quote: QuoteSingle {
                    delivery_fee,
                    recipient_fee,
                },
            }),
        )
            .into_response();
    }

    // ── Multiple recipients ───────────────────────────────────────────
    let mut quotes: Vec<QuoteEntry> = Vec::with_capacity(recipients.len());
    let mut blocked_recipients: Vec<String> = Vec::new();
    let mut total_recipient_fees: i64 = 0;
    let mut total_delivery_fees: i64 = 0;

    for rec in &recipients {
        let rf = match queries::get_recipient_fee(&state.db, rec, &sender_key, &message_box).await {
            Ok(f) => f,
            Err(e) => {
                error!("failed to get recipient fee: {e}");
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "ERR_INTERNAL_ERROR",
                    "An internal error has occurred.",
                )
                .into_response();
            }
        };

        let status = match rf {
            -1 => {
                blocked_recipients.push(rec.clone());
                "blocked"
            }
            0 => "always_allow",
            _ => {
                total_recipient_fees += rf;
                "payment_required"
            }
        };

        total_delivery_fees += delivery_fee;

        quotes.push(QuoteEntry {
            recipient: rec.clone(),
            message_box: message_box.clone(),
            delivery_fee,
            recipient_fee: rf,
            status: status.to_string(),
        });
    }

    (
        StatusCode::OK,
        Json(QuoteMultiResponse {
            status: "success".to_string(),
            description: format!(
                "Message delivery quotes generated for {} recipients.",
                recipients.len()
            ),
            quotes_by_recipient: quotes,
            totals: QuoteTotals {
                delivery_fees: total_delivery_fees,
                recipient_fees: total_recipient_fees,
                total_for_payable_recipients: total_delivery_fees + total_recipient_fees,
            },
            blocked_recipients,
        }),
    )
        .into_response()
}
