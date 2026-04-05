pub mod send_fcm_notification;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use reqwest::Client;
use rsa::pkcs8::DecodePrivateKey;
use rsa::{Pkcs1v15Sign, RsaPrivateKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::{OnceCell, RwLock};

// ---------------------------------------------------------------------------
// Shared state with token refresh support
// ---------------------------------------------------------------------------

struct FirebaseState {
    project_id: String,
    service_account_json: String, // kept for re-signing on refresh
    access_token: String,
    token_expires_at: i64, // unix timestamp
    http_client: Client,
}

static FIREBASE_STATE: OnceCell<Arc<RwLock<Option<FirebaseState>>>> = OnceCell::const_new();

/// Initialize the Firebase client. Returns `Some(())` on success, `None` if
/// Firebase could not be configured (which is non-fatal — the server runs
/// without push notifications).
pub async fn initialize(
    project_id: Option<&str>,
    service_account_json: Option<&str>,
    service_account_path: Option<&str>,
) -> Option<()> {
    let project_id = match project_id {
        Some(id) if !id.is_empty() => id.to_owned(),
        _ => {
            tracing::warn!("Firebase project_id not provided; push notifications disabled");
            FIREBASE_STATE
                .get_or_init(|| async { Arc::new(RwLock::new(None)) })
                .await;
            return None;
        }
    };

    // Resolve service account JSON: use the provided string, or read from file.
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
        http_client: Client::new(),
    };

    FIREBASE_STATE
        .get_or_init(|| async { Arc::new(RwLock::new(Some(state))) })
        .await;

    tracing::debug!("Firebase initialized successfully");
    Some(())
}

/// Whether Firebase push notifications are enabled.
pub fn is_enabled() -> bool {
    match FIREBASE_STATE.get() {
        Some(lock) => lock.try_read().map(|g| g.is_some()).unwrap_or(false),
        None => false,
    }
}

/// Return a valid (project_id, access_token, http_client) tuple, refreshing
/// the OAuth2 token if it is about to expire (within 5 minutes).
/// Returns `None` if Firebase is not configured.
pub async fn get_valid_token() -> Option<(String, String, Client)> {
    let state_lock = FIREBASE_STATE.get()?.clone();

    // Fast path: read lock, check if token is still valid.
    {
        let guard = state_lock.read().await;
        let state = guard.as_ref()?;
        let now = chrono::Utc::now().timestamp();
        if now < state.token_expires_at - 300 {
            return Some((
                state.project_id.clone(),
                state.access_token.clone(),
                state.http_client.clone(),
            ));
        }
    }

    // Slow path: token expiring soon, take write lock and refresh.
    let mut guard = state_lock.write().await;
    let state = guard.as_mut()?;

    // Double-check: another task may have refreshed while we waited.
    let now = chrono::Utc::now().timestamp();
    if now < state.token_expires_at - 300 {
        return Some((
            state.project_id.clone(),
            state.access_token.clone(),
            state.http_client.clone(),
        ));
    }

    tracing::debug!("Refreshing Firebase OAuth2 access token");
    match obtain_access_token(&state.service_account_json).await {
        Ok((new_token, new_expires)) => {
            state.access_token = new_token;
            state.token_expires_at = new_expires;
            Some((
                state.project_id.clone(),
                state.access_token.clone(),
                state.http_client.clone(),
            ))
        }
        Err(e) => {
            tracing::error!("Failed to refresh Firebase access token: {e}");
            // Return the stale token — it might still work for a few minutes.
            Some((
                state.project_id.clone(),
                state.access_token.clone(),
                state.http_client.clone(),
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// OAuth2 token exchange
// ---------------------------------------------------------------------------

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

    let client = Client::new();
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

    // Google tokens are valid for 3600s; record absolute expiry.
    let expires_at = now + 3600;
    Ok((token_resp.access_token, expires_at))
}

/// Construct an RS256-signed JWT from the given claims and PEM-encoded private
/// key. Uses the `rsa` crate for signing — no external CLI tools needed.
fn build_rs256_jwt(claims: &JwtClaims, pem_key: &str) -> Result<String, String> {
    // -- header
    let header = r#"{"alg":"RS256","typ":"JWT"}"#;
    let header_b64 = URL_SAFE_NO_PAD.encode(header.as_bytes());

    // -- payload
    let payload =
        serde_json::to_string(claims).map_err(|e| format!("failed to serialise JWT claims: {e}"))?;
    let payload_b64 = URL_SAFE_NO_PAD.encode(payload.as_bytes());

    let signing_input = format!("{header_b64}.{payload_b64}");

    // -- sign with RSA PKCS#1 v1.5 / SHA-256
    let sign_fn = rsa_sign_pkcs1v15_sha256(pem_key)?;
    let signature = sign_fn(signing_input.as_bytes())?;
    let sig_b64 = URL_SAFE_NO_PAD.encode(&signature);

    Ok(format!("{signing_input}.{sig_b64}"))
}

/// Parse a PEM-encoded PKCS#8 RSA private key and return a closure that
/// produces PKCS#1 v1.5 SHA-256 signatures.
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
