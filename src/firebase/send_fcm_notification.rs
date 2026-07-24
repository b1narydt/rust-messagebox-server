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

    // Spawn one task per device, collect join handles.
    let mut handles = Vec::with_capacity(devices.len());

    for device in devices {
        let url = url.clone();
        let payload = payload.clone();
        let pool = pool.clone();
        let http = http.clone();
        let access_token = access_token.clone();
        let token_tail = truncate_token(&device.fcm_token);

        let handle = tokio::spawn(async move {
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

            if should_deactivate(&body_text) {
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
fn should_deactivate(response_body: &str) -> bool {
    let upper = response_body.to_uppercase();
    upper.contains("NOT_FOUND")
        || upper.contains("UNREGISTERED")
        || upper.contains("REGISTRATION-TOKEN-NOT-REGISTERED")
        || upper.contains("INVALID-REGISTRATION-TOKEN")
}

/// Show only the last 10 characters of an FCM token for log safety.
fn truncate_token(token: &str) -> String {
    if token.len() <= 10 {
        token.to_owned()
    } else {
        token[token.len() - 10..].to_owned()
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

    /// §4.3 token lifecycle: v1 invalid-token signals deactivate; transient
    /// errors do not.
    #[test]
    fn deactivation_signals() {
        assert!(should_deactivate(r#"{"error":{"status":"NOT_FOUND"}}"#));
        assert!(should_deactivate(
            r#"{"error":{"details":[{"errorCode":"UNREGISTERED"}]}}"#
        ));
        assert!(should_deactivate("registration-token-not-registered"));
        assert!(should_deactivate("invalid-registration-token"));
        assert!(!should_deactivate(r#"{"error":{"status":"UNAVAILABLE"}}"#));
        assert!(!should_deactivate(r#"{"error":{"status":"INTERNAL"}}"#));
    }

    #[test]
    fn token_truncation_is_log_safe() {
        assert_eq!(truncate_token("abcdefghijKLMNOPQRST"), "KLMNOPQRST");
        assert_eq!(truncate_token("short"), "short");
    }
}
