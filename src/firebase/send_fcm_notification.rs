//! FCM v1 delivery — parity audit §4.3.
//!
//! Trigger contract: every stored message whose box is exactly
//! `notifications` (see `queries::should_use_fcm_delivery`), after the send,
//! best-effort — FCM failure never fails the send. The visible notification
//! body is the messageId (the content is E2E-encrypted; the ID lets the app
//! fetch it), matching TS `sendFCMNotification` byte-for-byte.

use crate::db::DbPool;
use serde_json::json;
use std::time::Duration;

/// Max concurrent in-flight FCM HTTP requests per notification fan-out.
const MAX_CONCURRENT_FCM_SENDS: usize = 16;

/// Payload describing the push notification to send via FCM.
#[derive(Clone, Debug)]
pub struct FcmPayload {
    /// Visible notification title — the TS server always sends 'New Message'.
    pub title: String,
    pub message_id: String,
    /// Sender identity key (TS passes 'unknown' when absent; the Rust send
    /// path always has the verified/authenticated sender, so it passes it).
    pub originator: String,
}

/// Result of attempting to send an FCM notification.
pub struct SendFcmNotificationResult {
    pub success: bool,
    pub error: Option<String>,
}

/// Send a push notification to all active devices registered for `recipient`.
///
/// Devices are notified in parallel. Tokens that FCM reports as permanently
/// invalid (`NOT_FOUND` / `UNREGISTERED`) are automatically deactivated. On
/// success the device's `last_used` timestamp is updated (§4.3 token
/// lifecycle). Returns success if at least one device was notified.
pub async fn send_fcm_notification(
    pool: &DbPool,
    recipient: &str,
    payload: FcmPayload,
) -> SendFcmNotificationResult {
    let (project_id, access_token, http) = match super::get_valid_token().await {
        Some(tuple) => tuple,
        None => {
            return SendFcmNotificationResult {
                success: false,
                error: Some("Firebase is not enabled".to_owned()),
            };
        }
    };

    // Look up all active devices for the recipient.
    let devices = match crate::db::queries::list_active_devices(pool, recipient).await {
        Ok(d) => d,
        Err(e) => {
            tracing::error!("Failed to list active devices for recipient: {e}");
            return SendFcmNotificationResult {
                success: false,
                error: Some(format!("database error: {e}")),
            };
        }
    };

    if devices.is_empty() {
        tracing::debug!(
            "No active devices found for recipient {}",
            truncate_key(recipient)
        );
        return SendFcmNotificationResult {
            success: false,
            error: Some("no active devices for recipient".to_owned()),
        };
    }

    let url = format!("https://fcm.googleapis.com/v1/projects/{project_id}/messages:send");

    // Spawn one task per device, but cap concurrent in-flight FCM requests: a
    // recipient with hundreds of devices (times a burst of notifications sends)
    // must not open an unbounded number of sockets to FCM at once.
    let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_FCM_SENDS));
    let mut handles = Vec::with_capacity(devices.len());

    for device in devices {
        let url = url.clone();
        let payload = payload.clone();
        let pool = pool.clone();
        let http = http.clone();
        let access_token = access_token.clone();
        let token_tail = truncate_token(&device.fcm_token);
        let sem = sem.clone();

        let handle = tokio::spawn(async move {
            // Held for the request's lifetime; bounds concurrency to the cap.
            let _permit = sem.acquire_owned().await.expect("FCM semaphore");
            let body = build_fcm_body(&device.fcm_token, &payload);

            let result = tokio::time::timeout(
                Duration::from_secs(5),
                http.post(&url)
                    .bearer_auth(&access_token)
                    .json(&body)
                    .send(),
            )
            .await;

            let resp = match result {
                Ok(Ok(r)) => r,
                Ok(Err(e)) => {
                    tracing::error!("FCM request failed for device ...{}: {e}", token_tail);
                    return false;
                }
                Err(_) => {
                    tracing::error!("FCM request timed out for device ...{}", token_tail);
                    return false;
                }
            };

            let status = resp.status();
            if status.is_success() {
                tracing::debug!("FCM sent successfully to device ...{}", token_tail);
                if let Err(e) = crate::db::queries::update_device_last_used(&pool, device.id).await
                {
                    tracing::warn!("Failed to update last_used for device {}: {e}", device.id);
                }
                return true;
            }

            // Parse the error body to decide whether to deactivate the token.
            let body_text = resp.text().await.unwrap_or_default();
            tracing::warn!(
                "FCM returned {} for device ...{}: {}",
                status,
                token_tail,
                body_text
            );

            if should_deactivate(status, &body_text) {
                tracing::warn!(
                    "Deactivating device ...{} due to invalid FCM token",
                    token_tail
                );
                if let Err(e) = crate::db::queries::deactivate_device(&pool, device.id).await {
                    tracing::error!("Failed to deactivate device {}: {e}", device.id);
                }
            }

            false
        });

        handles.push(handle);
    }

    // Await all tasks and check if at least one succeeded.
    let mut any_success = false;
    for handle in handles {
        match handle.await {
            Ok(true) => any_success = true,
            Ok(false) => {}
            Err(e) => {
                tracing::error!("FCM send task panicked: {e}");
            }
        }
    }

    if any_success {
        SendFcmNotificationResult {
            success: true,
            error: None,
        }
    } else {
        SendFcmNotificationResult {
            success: false,
            error: Some("failed to deliver to any device".to_owned()),
        }
    }
}

/// Build the FCM v1 API request body (§4.3 — mirrors TS `sendFCMNotification`).
///
/// The visible body is the RAW messageId (TS behavior; the earlier Rust
/// prefixed `"New message: "` — a cosmetic drift, dropped for strict parity).
/// FCM ignores inapplicable platform blocks, so android + apns are always
/// both included — same as TS.
fn build_fcm_body(token: &str, payload: &FcmPayload) -> serde_json::Value {
    json!({
        "message": {
            "token": token,
            "notification": {
                "title": payload.title,
                "body": payload.message_id
            },
            "android": {
                "priority": "high",
                "data": {
                    "messageId": payload.message_id,
                    "originator": payload.originator
                }
            },
            "apns": {
                "headers": {
                    "apns-push-type": "alert",
                    "apns-priority": "10"
                },
                "payload": {
                    "aps": {
                        "mutable-content": 1,
                        "alert": {
                            "title": payload.title,
                            "body": payload.message_id
                        }
                    },
                    "messageId": payload.message_id,
                    "originator": payload.originator
                }
            }
        }
    })
}

/// `true` if the FCM error response indicates the token is permanently
/// invalid and should be deactivated (§4.3: v1 API `NOT_FOUND` /
/// `UNREGISTERED`, plus the legacy SDK strings
/// `registration-token-not-registered` / `invalid-registration-token`).
/// Decide whether an FCM error means the token is permanently invalid and the
/// device row should be deactivated.
///
/// The old logic uppercased the whole body and substring-matched `NOT_FOUND` /
/// `UNREGISTERED` on ANY non-2xx response, so an unrelated error whose text
/// merely contained those words (a 5xx, a details string, a wrapped upstream
/// error) would deactivate a still-valid token. The FCM v1 contract is precise:
/// a permanently-invalid token is reported as **HTTP 404** with
/// `error.status == "NOT_FOUND"` and/or `error.details[].errorCode ==
/// "UNREGISTERED"`. We deactivate only on that, never on 5xx/UNAVAILABLE/
/// INTERNAL/auth/quota errors. The narrow lowercase legacy-SDK token codes are
/// kept as an unambiguous fallback (they are token-specific by construction).
///
/// This mirrors the TS reference (`registration-token-not-registered` /
/// `invalid-registration-token`) and the Go reference (`IsUnregistered` /
/// `IsInvalidArgument`), which both classify narrowly rather than by substring.
fn should_deactivate(status: reqwest::StatusCode, response_body: &str) -> bool {
    // Unambiguous legacy-SDK token codes: safe to honor without a status gate.
    if response_body.contains("registration-token-not-registered")
        || response_body.contains("invalid-registration-token")
    {
        return true;
    }

    // FCM v1: only a 404 can mean "token no longer registered".
    if status != reqwest::StatusCode::NOT_FOUND {
        return false;
    }
    let Ok(v) = serde_json::from_str::<serde_json::Value>(response_body) else {
        // A 404 with an unparseable body: treat as invalid-token (the v1 API
        // only 404s for an unknown token target).
        return true;
    };
    if v["error"]["status"].as_str() == Some("NOT_FOUND") {
        return true;
    }
    v["error"]["details"]
        .as_array()
        .is_some_and(|details| {
            details
                .iter()
                .any(|d| d["errorCode"].as_str() == Some("UNREGISTERED"))
        })
}

/// Show only the last 10 characters of an FCM token for log safety.
///
/// Counts by `char`, not by byte: a byte-index slice at `len() - 10` panics when
/// it lands inside a multi-byte character, and FCM tokens are arbitrary utf8mb4.
fn truncate_token(token: &str) -> String {
    let char_count = token.chars().count();
    if char_count <= 10 {
        token.to_owned()
    } else {
        token.chars().skip(char_count - 10).collect()
    }
}

/// Truncate an identity key for logs.
fn truncate_key(key: &str) -> String {
    if key.len() <= 12 {
        key.to_owned()
    } else {
        format!("{}...", &key[..12])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// §4.3: the FCM v1 body contract — title 'New Message', visible body is
    /// the RAW messageId, android high-priority data block, apns alert block
    /// with mutable-content, originator = sender key in both data blocks.
    #[test]
    fn fcm_body_matches_ts_contract() {
        let body = build_fcm_body(
            "tok-1",
            &FcmPayload {
                title: "New Message".into(),
                message_id: "msg-42".into(),
                originator: "02abc".into(),
            },
        );
        let m = &body["message"];
        assert_eq!(m["token"], "tok-1");
        assert_eq!(m["notification"]["title"], "New Message");
        assert_eq!(
            m["notification"]["body"], "msg-42",
            "body must be the RAW messageId (no 'New message: ' prefix)"
        );
        assert_eq!(m["android"]["priority"], "high");
        assert_eq!(m["android"]["data"]["messageId"], "msg-42");
        assert_eq!(m["android"]["data"]["originator"], "02abc");
        assert_eq!(m["apns"]["headers"]["apns-push-type"], "alert");
        assert_eq!(m["apns"]["headers"]["apns-priority"], "10");
        assert_eq!(m["apns"]["payload"]["aps"]["mutable-content"], 1);
        assert_eq!(m["apns"]["payload"]["aps"]["alert"]["title"], "New Message");
        assert_eq!(m["apns"]["payload"]["aps"]["alert"]["body"], "msg-42");
        assert_eq!(m["apns"]["payload"]["messageId"], "msg-42");
        assert_eq!(m["apns"]["payload"]["originator"], "02abc");
    }

    /// §4.3 token lifecycle: only a 404 + a real invalid-token signal
    /// deactivates; transient errors and body-substring coincidences do not.
    #[test]
    fn deactivation_signals() {
        use reqwest::StatusCode;
        let nf = StatusCode::NOT_FOUND;

        // 404 + structured invalid-token signals → deactivate.
        assert!(should_deactivate(nf, r#"{"error":{"status":"NOT_FOUND"}}"#));
        assert!(should_deactivate(
            nf,
            r#"{"error":{"status":"NOT_FOUND","details":[{"errorCode":"UNREGISTERED"}]}}"#
        ));
        // Legacy-SDK token codes: honored regardless of status.
        assert!(should_deactivate(nf, "registration-token-not-registered"));
        assert!(should_deactivate(
            StatusCode::BAD_REQUEST,
            "invalid-registration-token"
        ));

        // Transient / unrelated errors must NOT deactivate.
        assert!(!should_deactivate(
            StatusCode::SERVICE_UNAVAILABLE,
            r#"{"error":{"status":"UNAVAILABLE"}}"#
        ));
        assert!(!should_deactivate(
            StatusCode::INTERNAL_SERVER_ERROR,
            r#"{"error":{"status":"INTERNAL"}}"#
        ));
        // The old false-positive: a non-404 body that merely mentions the words.
        assert!(!should_deactivate(
            StatusCode::INTERNAL_SERVER_ERROR,
            r#"{"error":{"status":"INTERNAL","message":"upstream NOT_FOUND while reading config"}}"#
        ));
        // A 404 whose body does not carry a token signal still deactivates
        // (the v1 API only 404s for an unknown token target).
        assert!(should_deactivate(nf, "Not Found"));
    }

    #[test]
    fn token_truncation_is_log_safe() {
        assert_eq!(truncate_token("abcdefghijKLMNOPQRST"), "KLMNOPQRST");
        assert_eq!(truncate_token("short"), "short");
    }
}
