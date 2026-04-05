use crate::db::DbPool;
use serde_json::json;
use std::time::Duration;

/// Payload describing the push notification to send via FCM.
#[derive(Clone, Debug)]
pub struct FcmPayload {
    pub title: String,
    pub message_id: String,
    pub originator: String,
}

/// Result of attempting to send an FCM notification.
pub struct SendFcmNotificationResult {
    pub success: bool,
    pub error: Option<String>,
}

/// Send a push notification to all active devices registered for `recipient`.
///
/// Devices are notified in parallel. Tokens that FCM reports as invalid
/// (`NOT_FOUND` / `UNREGISTERED`) are automatically deactivated. On success the
/// device's `last_used` timestamp is updated.
///
/// Returns success if at least one device was notified.
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
    let devices = match crate::db::queries::list_active_devices(pool, recipient) {
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
        tracing::debug!("No active devices found for recipient {}", truncate_key(recipient));
        return SendFcmNotificationResult {
            success: false,
            error: Some("no active devices for recipient".to_owned()),
        };
    }

    let url = format!(
        "https://fcm.googleapis.com/v1/projects/{}/messages:send",
        project_id
    );

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
            let body = build_fcm_body(&device.fcm_token, &payload, device.platform.as_deref());

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
                if let Err(e) = crate::db::queries::update_device_last_used(&pool, device.id) {
                    tracing::warn!("Failed to update last_used for device {}: {e}", device.id);
                }
                return true;
            }

            // Try to parse error body to decide whether to deactivate the token.
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
                if let Err(e) = crate::db::queries::deactivate_device(&pool, device.id) {
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

/// Build the FCM v1 API request body with platform-specific overrides.
fn build_fcm_body(
    token: &str,
    payload: &FcmPayload,
    platform: Option<&str>,
) -> serde_json::Value {
    let body_text = format!("New message: {}", payload.message_id);

    let message = json!({
        "message": {
            "token": token,
            "notification": {
                "title": payload.title,
                "body": body_text
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
                            "body": body_text
                        }
                    },
                    "messageId": payload.message_id,
                    "originator": payload.originator
                }
            }
        }
    });

    // For a purely Android device we could strip the apns block and vice
    // versa, but FCM already ignores inapplicable platform blocks, so
    // including both is correct and simpler.
    let _ = platform; // acknowledge parameter; no stripping needed

    message
}

/// Return `true` if the FCM error response indicates the token is permanently
/// invalid and should be deactivated.
fn should_deactivate(response_body: &str) -> bool {
    let upper = response_body.to_uppercase();
    upper.contains("NOT_FOUND") || upper.contains("UNREGISTERED")
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
