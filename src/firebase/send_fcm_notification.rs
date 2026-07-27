//! FCM v1 delivery — parity audit §4.3.
//!
//! Trigger contract: every stored message whose box is exactly
//! `notifications` (see `queries::should_use_fcm_delivery`), after the send,
//! best-effort — FCM failure never fails the send. The visible notification
//! body is the messageId (the content is E2E-encrypted; the ID lets the app
//! fetch it), matching TS `sendFCMNotification` byte-for-byte.

use crate::db::DbPool;
use serde_json::json;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

/// Max concurrent in-flight FCM HTTP requests per notification fan-out.
const MAX_CONCURRENT_FCM_SENDS: usize = 16;

/// Deactivation decisions taken since the last successful FCM send.
///
/// Second line of defense behind [`should_deactivate`]'s narrow gate. Losing a
/// device row is irreversible from the server's side (the handset must
/// re-register), so a long unbroken run of "deactivate" with not one success in
/// between is treated as systemic — bad credentials, wrong project, or an FCM
/// contract change — rather than as that many genuinely dead handsets. Past the
/// trip count we stop writing and shout, leaving the rows intact for an
/// operator to inspect. Stale-but-present rows are recoverable; a wiped fleet
/// is not.
static CONSECUTIVE_DEACTIVATIONS: AtomicU32 = AtomicU32::new(0);

/// Consecutive deactivations tolerated before the breaker opens.
///
/// Set far above any plausible legitimate run. The count is process-global and
/// resets only on a successful send, so a single recipient holding many dead
/// handsets can rack up strikes with nothing to clear them — too low a value
/// would trip on healthy cleanup and then block deactivation server-wide.
const DEACTIVATION_BREAKER_TRIP: u32 = 500;

/// Claim permission to deactivate one device, advancing the breaker.
///
/// Returns `false` once the trip count is exceeded. The ERROR fires only on the
/// transition so a sustained outage doesn't flood the log.
fn claim_deactivation() -> bool {
    // `saturating_add` on the returned value: a plain `+ 1` would panic in debug
    // builds at u32::MAX, and in release would wrap to 0 and silently re-close
    // the breaker. Unreachable in practice, but the breaker exists precisely for
    // the case where our reasoning about reachability was wrong.
    let n = CONSECUTIVE_DEACTIVATIONS
        .fetch_add(1, Ordering::Relaxed)
        .saturating_add(1);
    if n <= DEACTIVATION_BREAKER_TRIP {
        return true;
    }
    if n == DEACTIVATION_BREAKER_TRIP + 1 {
        tracing::error!(
            consecutive = n,
            "FCM deactivation breaker OPEN: {DEACTIVATION_BREAKER_TRIP} consecutive tokens were \
             reported permanently invalid with no successful send in between. Refusing further \
             deactivations and leaving the rows active. A run this long is more consistent with \
             a systemic fault (credentials, project, or an FCM contract change) than with that \
             many dead handsets — verify push is working before clearing it; any successful \
             send resets the breaker."
        );
    }
    false
}

/// Reset the breaker — a delivery proving the credentials and project are good.
fn note_successful_send() {
    CONSECUTIVE_DEACTIVATIONS.store(0, Ordering::Relaxed);
}

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
                note_successful_send();
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

            if should_deactivate(status, &body_text) && claim_deactivation() {
                tracing::warn!(
                    "Deactivating device ...{} — FCM reported the token UNREGISTERED",
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
///
/// ## Why a bare 404 / `status: NOT_FOUND` is NOT enough
///
/// Deactivation is irreversible from the server's side — the user's handset has
/// to re-register before it can be reached again — so the signal must identify
/// THE TOKEN, not merely the request. `POST /v1/projects/{project_id}/messages:send`
/// also 404s with `error.status == "NOT_FOUND"` when the *project path* is wrong
/// (a typo'd `FIREBASE_PROJECT_ID`, a revoked project, an API path change). That
/// response is identical for every device, so honoring it would deactivate the
/// entire fleet on the first notification burst after a misconfigured deploy.
/// The only token-specific signal in the v1 contract is
/// `error.details[].errorCode == "UNREGISTERED"`, so that (or the equally
/// token-specific legacy codes) is what we require. A bare 404 is treated as
/// transient: nothing is deactivated and the send simply fails.
fn should_deactivate(status: reqwest::StatusCode, response_body: &str) -> bool {
    // Unambiguous legacy-SDK token codes: safe to honor without a status gate.
    if response_body.contains("registration-token-not-registered")
        || response_body.contains("invalid-registration-token")
    {
        return true;
    }

    // A malformed token is reported as 400 INVALID_ARGUMENT, not 404.
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(response_body) {
        if is_invalid_token_argument(status, &v) {
            return true;
        }
    }

    // FCM v1: only a 404 can mean "token no longer registered".
    if status != reqwest::StatusCode::NOT_FOUND {
        return false;
    }
    let Ok(v) = serde_json::from_str::<serde_json::Value>(response_body) else {
        // A 404 whose body we cannot parse is not attributable to the token.
        return false;
    };
    v["error"]["details"].as_array().is_some_and(|details| {
        details
            .iter()
            .any(|d| d["errorCode"].as_str() == Some("UNREGISTERED"))
    })
}

/// `INVALID_ARGUMENT` on a 400 means the token itself is malformed.
///
/// Google documents deleting the token on `UNREGISTERED` **or** on
/// `INVALID_ARGUMENT` when the request payload is known-good. That caveat is
/// what makes this safe here: [`build_fcm_body`] is entirely server-generated
/// and fixed-shape, so the only caller-varying field in the request is the
/// token — if FCM calls the argument invalid, the token is the argument.
///
/// This matters because deactivation is the ONLY cleanup path that exists:
/// nothing deletes device rows, there is no TTL and no sweeper, and a rotated
/// token inserts a new row while leaving the old one active. Gating solely on
/// `UNREGISTERED` would leave malformed tokens accumulating forever.
fn is_invalid_token_argument(status: reqwest::StatusCode, v: &serde_json::Value) -> bool {
    status == reqwest::StatusCode::BAD_REQUEST
        && v["error"]["status"].as_str() == Some("INVALID_ARGUMENT")
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

        // The one token-specific v1 signal → deactivate.
        assert!(should_deactivate(
            nf,
            r#"{"error":{"status":"NOT_FOUND","details":[{"errorCode":"UNREGISTERED"}]}}"#
        ));
        // A malformed token: 400 INVALID_ARGUMENT. Safe to honor because the
        // request body is server-generated, so the token is the only argument.
        assert!(should_deactivate(
            StatusCode::BAD_REQUEST,
            r#"{"error":{"code":400,"status":"INVALID_ARGUMENT","message":"The registration token is not a valid FCM registration token"}}"#
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
        // A 403 from a revoked/misconfigured project must never look like a
        // token problem — that is the fleet-wipe path.
        assert!(!should_deactivate(
            StatusCode::FORBIDDEN,
            r#"{"error":{"code":403,"status":"PERMISSION_DENIED"}}"#
        ));
        // INVALID_ARGUMENT is only honored on a 400.
        assert!(!should_deactivate(
            StatusCode::INTERNAL_SERVER_ERROR,
            r#"{"error":{"status":"INVALID_ARGUMENT"}}"#
        ));
        // The old false-positive: a non-404 body that merely mentions the words.
        assert!(!should_deactivate(
            StatusCode::INTERNAL_SERVER_ERROR,
            r#"{"error":{"status":"INTERNAL","message":"upstream NOT_FOUND while reading config"}}"#
        ));
        // A 404 that does not name THIS token must NOT deactivate: the wrong
        // FIREBASE_PROJECT_ID returns exactly this for every device, so honoring
        // it would wipe the fleet on the first notification burst.
        assert!(!should_deactivate(
            nf,
            r#"{"error":{"code":404,"message":"Requested entity was not found.","status":"NOT_FOUND"}}"#
        ));
        // Same for a 404 whose body we cannot parse at all.
        assert!(!should_deactivate(nf, "Not Found"));
    }

    /// The breaker stops an unbounded run of deactivations even when each one
    /// carries a well-formed UNREGISTERED signal — the systemic-failure case
    /// that the per-response gate cannot distinguish on its own.
    #[test]
    fn deactivation_breaker_opens_after_a_long_unbroken_run() {
        note_successful_send(); // start from a known-closed breaker

        for i in 1..=DEACTIVATION_BREAKER_TRIP {
            assert!(
                claim_deactivation(),
                "claim {i} within budget must be allowed"
            );
        }
        assert!(
            !claim_deactivation(),
            "the claim past the trip count must be refused"
        );
        assert!(!claim_deactivation(), "and it must stay refused");

        // A single successful delivery proves the credentials/project are fine
        // and re-arms normal cleanup.
        note_successful_send();
        assert!(
            claim_deactivation(),
            "a successful send must reset the breaker"
        );
        note_successful_send();
    }

    #[test]
    fn token_truncation_is_log_safe() {
        assert_eq!(truncate_token("abcdefghijKLMNOPQRST"), "KLMNOPQRST");
        assert_eq!(truncate_token("short"), "short");
    }
}
