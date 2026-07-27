//! Firebase Cloud Messaging (FCM v1) client — the push-notification half of
//! the TS-parity notification system (parity audit §4.3, rows H9/N3/N4).
//!
//! Enablement is EXPLICIT (TS parity + fail-safe): `main()` calls
//! [`initialize`] only when `ENABLE_FIREBASE=true`, and initialization
//! requires `FIREBASE_PROJECT_ID` plus one credential source
//! (`FIREBASE_SERVICE_ACCOUNT_JSON` or `FIREBASE_SERVICE_ACCOUNT_PATH`).
//! Initialization failure is non-fatal — the server runs without push.
//!
//! ## E2 guard — this module handles key material
//!
//! The service-account JSON (and the RSA private key inside it) must NEVER be
//! logged, `Debug`-printed, or echoed into an error message. Upstream TS
//! itself logs the first 100 chars of the service-account JSON at init — do
//! NOT reproduce that. Nothing in this module formats the JSON or the parsed
//! key; error paths carry only parser/HTTP error text.

pub mod send_fcm_notification;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use reqwest::Client;
use rsa::pkcs8::DecodePrivateKey;
use rsa::{Pkcs1v15Sign, RsaPrivateKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, OnceCell, RwLock};

/// Refresh this many seconds before the token actually expires. Also the window
/// during which a concurrent refresh is invisible to readers: they keep using
/// the current token, which stays valid for this whole period.
const TOKEN_REFRESH_SKEW_SECS: i64 = 300;

/// Bound on the OAuth2 token exchange. `reqwest` defaults to NO timeout, and
/// this call runs under the refresh lock — an unbounded hang against
/// `oauth2.googleapis.com` would stall every later refresh behind it.
const TOKEN_EXCHANGE_TIMEOUT: Duration = Duration::from_secs(10);

/// Bound on an FCM send. Covers the whole request *including reading the body*;
/// the send path's own `tokio::time::timeout` wraps only `send()`, leaving the
/// body read unbounded while holding a concurrency permit.
const FCM_HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// After a failed exchange, suppress further attempts for this long.
///
/// Single-flighting alone collapses N exchanges into one only when the refresh
/// SUCCEEDS: on failure the expiry never advances, so the next caller is still
/// stale, takes the lock, and spends another [`TOKEN_EXCHANGE_TIMEOUT`]. During
/// a Google OAuth outage that serializes every notification behind its own
/// timeout — a burst arriving faster than one per timeout becomes an
/// ever-growing queue of spawned tasks. The cooldown makes a failed refresh
/// cheap for everyone who arrives during it.
const REFRESH_FAILURE_COOLDOWN_SECS: i64 = 30;

// ---------------------------------------------------------------------------
// Shared state with token refresh support
// ---------------------------------------------------------------------------

struct FirebaseState {
    project_id: String,
    service_account_json: String, // kept for re-signing on refresh — never logged (E2)
    access_token: String,
    token_expires_at: i64, // unix timestamp
    /// Unix timestamp of the last failed token exchange (0 = none since the
    /// last success). Gates [`REFRESH_FAILURE_COOLDOWN_SECS`].
    last_refresh_failure_at: i64,
    http_client: Client,
}

static FIREBASE_STATE: OnceCell<Arc<RwLock<Option<FirebaseState>>>> = OnceCell::const_new();

/// Elects the single task that performs a token exchange.
///
/// Deliberately separate from the state `RwLock`, and acquired with `try_lock`:
/// the network round-trip runs with no state lock held, and a caller that loses
/// the election does not queue — it uses the current token, which is still
/// valid throughout the refresh window. Holding the state write lock across the
/// exchange instead would queue every sender behind an untimed HTTP call
/// (tokio's `RwLock` is write-preferring), wedging push process-wide.
static REFRESH_LOCK: OnceCell<Mutex<()>> = OnceCell::const_new();

async fn refresh_lock() -> &'static Mutex<()> {
    REFRESH_LOCK.get_or_init(|| async { Mutex::new(()) }).await
}

/// Initialize the Firebase client. Returns `Some(())` on success, `None` if
/// Firebase could not be configured (non-fatal — the server runs without
/// push notifications). Only call when the operator explicitly enabled
/// Firebase (`ENABLE_FIREBASE=true`).
pub async fn initialize(
    project_id: Option<&str>,
    service_account_json: Option<&str>,
    service_account_path: Option<&str>,
) -> Option<()> {
    let project_id = match project_id {
        Some(id) if !id.is_empty() => id.to_owned(),
        _ => {
            tracing::warn!(
                "ENABLE_FIREBASE=true but FIREBASE_PROJECT_ID is not set; push notifications disabled"
            );
            FIREBASE_STATE
                .get_or_init(|| async { Arc::new(RwLock::new(None)) })
                .await;
            return None;
        }
    };

    // Resolve service account JSON: the provided string, or read from file.
    let sa_json = if let Some(json) = service_account_json {
        Some(json.to_owned())
    } else if let Some(path) = service_account_path {
        match tokio::fs::read_to_string(path).await {
            Ok(contents) => Some(contents),
            Err(e) => {
                tracing::warn!(
                    "Failed to read service account file at {}: {}; push notifications disabled",
                    path,
                    e
                );
                None
            }
        }
    } else {
        None
    };

    let sa_json = match sa_json {
        Some(j) => j,
        None => {
            tracing::warn!("No Firebase service account credentials; push notifications disabled");
            FIREBASE_STATE
                .get_or_init(|| async { Arc::new(RwLock::new(None)) })
                .await;
            return None;
        }
    };

    let (access_token, expires_at) = match obtain_access_token(&sa_json).await {
        Ok(pair) => pair,
        Err(e) => {
            tracing::warn!(
                "Failed to obtain Firebase access token: {}; push notifications disabled",
                e
            );
            FIREBASE_STATE
                .get_or_init(|| async { Arc::new(RwLock::new(None)) })
                .await;
            return None;
        }
    };

    let state = FirebaseState {
        project_id,
        service_account_json: sa_json,
        access_token,
        token_expires_at: expires_at,
        last_refresh_failure_at: 0,
        http_client: Client::builder()
            .timeout(FCM_HTTP_TIMEOUT)
            .build()
            .unwrap_or_else(|e| {
                tracing::warn!("Failed to build FCM client with timeout ({e}); using default");
                Client::new()
            }),
    };

    FIREBASE_STATE
        .get_or_init(|| async { Arc::new(RwLock::new(Some(state))) })
        .await;

    tracing::info!("Firebase initialized — FCM push notifications enabled");
    Some(())
}

/// Clone out (project_id, access_token, http_client) iff the token is not yet
/// inside the refresh window. `None` also covers "Firebase not configured".
async fn token_if_fresh(
    state_lock: &Arc<RwLock<Option<FirebaseState>>>,
) -> Option<(String, String, Client)> {
    let guard = state_lock.read().await;
    let state = guard.as_ref()?;
    let now = chrono::Utc::now().timestamp();
    (now < state.token_expires_at - TOKEN_REFRESH_SKEW_SECS).then(|| {
        (
            state.project_id.clone(),
            state.access_token.clone(),
            state.http_client.clone(),
        )
    })
}

/// Clone out the credentials iff the token has not actually expired yet —
/// ignoring the refresh skew. This is what a caller should use while some other
/// task is mid-refresh: the token is still accepted by FCM, just due for
/// renewal.
async fn token_if_unexpired(
    state_lock: &Arc<RwLock<Option<FirebaseState>>>,
) -> Option<(String, String, Client)> {
    let guard = state_lock.read().await;
    let state = guard.as_ref()?;
    (chrono::Utc::now().timestamp() < state.token_expires_at).then(|| {
        (
            state.project_id.clone(),
            state.access_token.clone(),
            state.http_client.clone(),
        )
    })
}

/// Return a valid (project_id, access_token, http_client) tuple, refreshing
/// the OAuth2 token if it is about to expire (within [`TOKEN_REFRESH_SKEW_SECS`]).
/// Returns `None` if Firebase is not configured.
///
/// No state lock is held across the network round-trip — see [`REFRESH_LOCK`] —
/// and a caller only ever waits on the refresh when it has nothing usable.
pub async fn get_valid_token() -> Option<(String, String, Client)> {
    let state_lock = FIREBASE_STATE.get()?.clone();

    // Fast path: the current token is good for a while yet.
    if let Some(creds) = token_if_fresh(&state_lock).await {
        return Some(creds);
    }

    // We are inside the skew window. Exactly one task performs the exchange;
    // everyone else keeps using the current token, which FCM still accepts for
    // up to TOKEN_REFRESH_SKEW_SECS — that head start is the entire point of
    // refreshing early. Queueing here instead would serialize every sender
    // behind one network round-trip, which is the bug this window prevents.
    let lock = refresh_lock().await;
    let _refresh_guard = match lock.try_lock() {
        Ok(guard) => guard,
        Err(_) => {
            if let Some(creds) = token_if_unexpired(&state_lock).await {
                return Some(creds);
            }
            // Truly expired and someone else is already refreshing: we have
            // nothing to send with, so wait for their result.
            let guard = lock.lock().await;
            if let Some(creds) = token_if_fresh(&state_lock).await {
                return Some(creds);
            }
            guard
        }
    };

    // Re-check: the winner of a race for this lock may have just refreshed.
    if let Some(creds) = token_if_fresh(&state_lock).await {
        return Some(creds);
    }

    // Snapshot the credential and RELEASE the lock before going to the network.
    // Also back off if the provider just failed us, rather than spending
    // another TOKEN_EXCHANGE_TIMEOUT per caller against something that is down.
    let service_account_json = {
        let guard = state_lock.read().await;
        let state = guard.as_ref()?;
        let now = chrono::Utc::now().timestamp();
        if now - state.last_refresh_failure_at < REFRESH_FAILURE_COOLDOWN_SECS
            && now < state.token_expires_at
        {
            return Some((
                state.project_id.clone(),
                state.access_token.clone(),
                state.http_client.clone(),
            ));
        }
        state.service_account_json.clone()
    };

    tracing::debug!("Refreshing Firebase OAuth2 access token");
    let refreshed = obtain_access_token(&service_account_json).await;

    // Re-acquire only to publish the result: no I/O happens under this guard.
    let mut guard = state_lock.write().await;
    let state = guard.as_mut()?;
    match refreshed {
        Ok((new_token, new_expires)) => {
            state.access_token = new_token;
            state.token_expires_at = new_expires;
            state.last_refresh_failure_at = 0;
        }
        Err(e) => {
            // Keep the stale token: it has up to TOKEN_REFRESH_SKEW_SECS of
            // life left, so a transient exchange failure need not fail sends.
            state.last_refresh_failure_at = chrono::Utc::now().timestamp();
            tracing::error!("Failed to refresh Firebase access token: {e}");
        }
    }
    Some((
        state.project_id.clone(),
        state.access_token.clone(),
        state.http_client.clone(),
    ))
}

// ---------------------------------------------------------------------------
// OAuth2 token exchange
// ---------------------------------------------------------------------------

// NOTE (E2): no Debug derive — this struct holds the RSA private key.
#[derive(Deserialize)]
struct ServiceAccount {
    private_key: String,
    client_email: String,
    token_uri: String,
}

#[derive(Serialize)]
struct JwtClaims {
    iss: String,
    scope: String,
    aud: String,
    iat: i64,
    exp: i64,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    /// Lifetime in seconds as reported by Google. Currently always 3600, but
    /// trusting the response rather than a constant matters: if a shorter-lived
    /// token were ever issued, a hardcoded hour would leave every push failing
    /// with a stale token until the guess ran out.
    expires_in: Option<i64>,
}

/// Build a self-signed JWT, exchange it at Google's token endpoint, and return
/// the resulting OAuth2 access token together with its expiry timestamp.
async fn obtain_access_token(service_account_json: &str) -> Result<(String, i64), String> {
    let sa: ServiceAccount = serde_json::from_str(service_account_json)
        .map_err(|e| format!("invalid service account JSON: {e}"))?;

    let now = chrono::Utc::now().timestamp();
    let claims = JwtClaims {
        iss: sa.client_email.clone(),
        scope: "https://www.googleapis.com/auth/firebase.messaging".to_owned(),
        aud: sa.token_uri.clone(),
        iat: now,
        exp: now + 3600,
    };

    let jwt = build_rs256_jwt(&claims, &sa.private_key)?;

    let client = Client::builder()
        .timeout(TOKEN_EXCHANGE_TIMEOUT)
        .build()
        .map_err(|e| format!("failed to build token-exchange client: {e}"))?;
    let resp = client
        .post(&sa.token_uri)
        .form(&[
            ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
            ("assertion", &jwt),
        ])
        .send()
        .await
        .map_err(|e| format!("token exchange request failed: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("token exchange returned {status}: {body}"));
    }

    let token_resp: TokenResponse = resp
        .json()
        .await
        .map_err(|e| format!("failed to parse token response: {e}"))?;

    // Record absolute expiry from what the server actually reported.
    let expires_at = now + token_resp.expires_in.unwrap_or(3600);
    Ok((token_resp.access_token, expires_at))
}

/// Construct an RS256-signed JWT from the given claims and PEM-encoded private
/// key. Uses the `rsa` crate for signing — no external CLI tools needed.
fn build_rs256_jwt(claims: &JwtClaims, pem_key: &str) -> Result<String, String> {
    // -- header
    let header = r#"{"alg":"RS256","typ":"JWT"}"#;
    let header_b64 = URL_SAFE_NO_PAD.encode(header.as_bytes());

    // -- payload
    let payload = serde_json::to_string(claims)
        .map_err(|e| format!("failed to serialise JWT claims: {e}"))?;
    let payload_b64 = URL_SAFE_NO_PAD.encode(payload.as_bytes());

    let signing_input = format!("{header_b64}.{payload_b64}");

    // -- sign with RSA PKCS#1 v1.5 / SHA-256
    let sign_fn = rsa_sign_pkcs1v15_sha256(pem_key)?;
    let signature = sign_fn(signing_input.as_bytes())?;
    let sig_b64 = URL_SAFE_NO_PAD.encode(&signature);

    Ok(format!("{signing_input}.{sig_b64}"))
}

/// Parse a PEM-encoded PKCS#8 RSA private key and return a closure that
/// produces PKCS#1 v1.5 SHA-256 signatures. Errors carry only the parser's
/// message — never the key text (E2).
fn rsa_sign_pkcs1v15_sha256(
    pem_key: &str,
) -> Result<impl Fn(&[u8]) -> Result<Vec<u8>, String>, String> {
    let private_key = RsaPrivateKey::from_pkcs8_pem(pem_key)
        .map_err(|e| format!("failed to parse private key PEM: {e}"))?;

    Ok(move |message: &[u8]| {
        let mut hasher = Sha256::new();
        hasher.update(message);
        let hash = hasher.finalize();
        private_key
            .sign(Pkcs1v15Sign::new::<Sha256>(), &hash)
            .map_err(|e| format!("RSA signing failed: {e}"))
    })
}

#[cfg(test)]
mod tests {
    use super::rsa_sign_pkcs1v15_sha256;

    /// E2: a bad key must produce an error that does NOT echo the key text.
    #[test]
    fn key_parse_error_never_echoes_key_material() {
        let fake_key = "-----BEGIN PRIVATE KEY-----\nSUPERSECRETBYTES\n-----END PRIVATE KEY-----";
        let err = rsa_sign_pkcs1v15_sha256(fake_key)
            .err()
            .expect("fake key must not parse");
        assert!(
            !err.contains("SUPERSECRETBYTES"),
            "E2: error text echoed key material: {err}"
        );
    }
}
