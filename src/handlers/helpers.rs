use std::sync::Arc;

use axum::{
    extract::FromRequestParts,
    http::{request::Parts, StatusCode},
    Json,
};
use bsv::wallet::proto_wallet::ProtoWallet as SdkProtoWallet;

use crate::config::Config;
use crate::db::DbPool;
use crate::handlers::request_types::PaymentOutput;
use crate::handlers::response_types::ErrorResponse;
use crate::ws::WsBroadcast;

/// Shared application state passed to all handlers via axum's State extractor.
#[derive(Clone)]
pub struct AppState {
    pub db: DbPool,
    pub config: Arc<Config>,
    pub wallet: Arc<SdkProtoWallet>,
    pub ws: WsBroadcast,
}

/// Extractor that pulls the authenticated identity key from request extensions.
///
/// Bridges the new `bsv_auth_axum_middleware::Authenticated` extractor to the
/// existing handler signatures. The middleware inserts `Authenticated { identity_key }`
/// into extensions; this extractor reads it.
#[derive(Clone)]
pub struct AuthIdentity(pub String);

impl<S: Send + Sync> FromRequestParts<S> for AuthIdentity {
    type Rejection = (StatusCode, Json<ErrorResponse>);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // Try the new middleware's Authenticated extractor first
        if let Some(auth) = parts.extensions.get::<bsv_auth_axum_middleware::Authenticated>() {
            return Ok(AuthIdentity(auth.identity_key.clone()));
        }
        // Fallback: check for legacy AuthIdentity (e.g., from internal routes)
        if let Some(id) = parts.extensions.get::<AuthIdentity>() {
            return Ok(id.clone());
        }
        Err(error_response(
            StatusCode::UNAUTHORIZED,
            "ERR_AUTH_REQUIRED",
            "Authentication required",
        ))
    }
}

/// Validates that `key` is a compressed secp256k1 public key encoded as hex.
///
/// A valid key is exactly 66 hex characters and starts with `02` or `03`.
pub fn is_valid_pub_key(key: &str) -> bool {
    if key.len() != 66 {
        return false;
    }
    if !key.starts_with("02") && !key.starts_with("03") {
        return false;
    }
    key.chars().all(|c| c.is_ascii_hexdigit())
}

/// Convenience helper that builds a typed error tuple suitable for returning
/// from any axum handler.
pub fn error_response(
    status: StatusCode,
    code: &str,
    description: &str,
) -> (StatusCode, Json<ErrorResponse>) {
    (
        status,
        Json(ErrorResponse {
            status: "error".to_string(),
            code: code.to_string(),
            description: description.to_string(),
        }),
    )
}

/// Fee information for a single recipient, used during send-message payment
/// processing.
pub struct FeeRow {
    pub recipient: String,
    pub recipient_fee: i64,
    pub allowed: bool,
}

/// Error returned when payment outputs cannot be mapped to recipients.
#[derive(Debug)]
pub struct OutputMappingError {
    pub code: String,
    pub description: String,
}

impl std::fmt::Display for OutputMappingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description)
    }
}

impl std::error::Error for OutputMappingError {}

/// Extracts `recipientIdentityKey` from a payment output's `customInstructions`
/// field (checking both payment and insertion remittance).
fn extract_recipient_key(out: &PaymentOutput) -> Option<String> {
    let raw = out
        .payment_remittance
        .as_ref()
        .and_then(|pr| pr.custom_instructions.as_ref())
        .or_else(|| {
            out.insertion_remittance
                .as_ref()
                .and_then(|ir| ir.custom_instructions.as_ref())
        });

    let raw = raw?;

    #[derive(serde::Deserialize)]
    struct Instr {
        #[serde(rename = "recipientIdentityKey")]
        recipient_identity_key: Option<String>,
    }

    let instr: Instr = serde_json::from_value(raw.clone()).ok()?;
    instr
        .recipient_identity_key
        .filter(|k| !k.is_empty())
}

/// Maps payment outputs to recipients based on `customInstructions` tagging or,
/// failing that, positional assignment.
///
/// The first output is skipped when a non-zero server delivery fee is present
/// (it belongs to the server).
pub fn build_per_recipient_outputs(
    outputs: &[PaymentOutput],
    delivery_fee: i64,
    fee_rows: &[FeeRow],
) -> Result<std::collections::HashMap<String, Vec<PaymentOutput>>, OutputMappingError> {
    use std::collections::{HashMap, HashSet};

    let mut per_recipient: HashMap<String, Vec<PaymentOutput>> = HashMap::new();

    let start_idx = if delivery_fee > 0 && !outputs.is_empty() {
        1
    } else {
        0
    };
    let recipient_side = &outputs[start_idx..];

    // Collect recipients that actually require payment.
    let fee_recipients: Vec<&str> = fee_rows
        .iter()
        .filter(|fr| fr.recipient_fee > 0)
        .map(|fr| fr.recipient.as_str())
        .collect();

    if fee_recipients.is_empty() {
        return Ok(per_recipient);
    }

    // Try explicit mapping via customInstructions.recipientIdentityKey.
    let mut by_key: HashMap<String, Vec<PaymentOutput>> = HashMap::new();
    let mut used_indexes: HashSet<u32> = HashSet::new();

    for out in recipient_side {
        if let Some(key) = extract_recipient_key(out) {
            used_indexes.insert(out.output_index);
            by_key.entry(key).or_default().push(out.clone());
        }
    }

    if by_key.is_empty() {
        // Positional fallback.
        if recipient_side.len() < fee_recipients.len() {
            return Err(OutputMappingError {
                code: "ERR_INSUFFICIENT_OUTPUTS".to_string(),
                description: format!(
                    "Expected at least {} recipient output(s) but received {}",
                    fee_recipients.len(),
                    recipient_side.len()
                ),
            });
        }
        for (i, r) in fee_recipients.iter().enumerate() {
            if i < recipient_side.len() {
                per_recipient
                    .entry(r.to_string())
                    .or_default()
                    .push(recipient_side[i].clone());
            }
        }
    } else {
        // Use tagged outputs.
        for r in &fee_recipients {
            if let Some(tagged) = by_key.get(*r) {
                per_recipient.insert(r.to_string(), tagged.clone());
            }
        }

        // Allocate remaining untagged outputs positionally to unmapped recipients.
        let unmapped: Vec<&str> = fee_recipients
            .iter()
            .filter(|r| !per_recipient.contains_key(**r))
            .copied()
            .collect();

        if !unmapped.is_empty() {
            let remaining: Vec<&PaymentOutput> = recipient_side
                .iter()
                .filter(|o| !used_indexes.contains(&o.output_index))
                .collect();

            if remaining.len() < unmapped.len() {
                return Err(OutputMappingError {
                    code: "ERR_INSUFFICIENT_OUTPUTS".to_string(),
                    description: format!(
                        "Expected at least {} additional recipient output(s) but only {} remain",
                        unmapped.len(),
                        remaining.len()
                    ),
                });
            }
            for (i, r) in unmapped.iter().enumerate() {
                per_recipient
                    .entry(r.to_string())
                    .or_default()
                    .push(remaining[i].clone());
            }
        }
    }

    Ok(per_recipient)
}
