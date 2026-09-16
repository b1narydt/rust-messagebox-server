//! Authenticated, neutral relay-presence probe.
//!
//! This route answers only whether an identity currently has a local,
//! authenticated Socket.IO membership in its MPC presence box. It deliberately
//! reads no vault, roster, certificate, or message state: the relay terminates
//! the socket and is the sole authority for that narrow fact.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};

use crate::handlers::helpers::{error_response, is_valid_pub_key, AppState, AuthIdentity};
use crate::handlers::response_types::PresenceResponse;

/// `GET /presence/{identity}` — report whether `identity` is dialled into this
/// relay's MPC lane.
///
/// The enclosing router requires BRC-103/104 authentication, but the caller
/// need not be the queried identity: this is a neutral liveness primitive, not
/// a vault-membership or authorization decision. The result is local to this
/// relay instance; Model-B deployments need sticky routing or a distributed
/// presence registry before they can claim a cluster-wide answer.
pub async fn presence(
    State(state): State<AppState>,
    _caller: AuthIdentity,
    Path(identity): Path<String>,
) -> impl IntoResponse {
    if !is_valid_pub_key(&identity) {
        return error_response(
            StatusCode::BAD_REQUEST,
            "ERR_INVALID_PUBLIC_KEY",
            "identity must be a compressed secp256k1 public key encoded as hex.",
        )
        .into_response();
    }

    // BRC-103 identities and room ids are emitted as lowercase DER hex. Accept
    // an uppercase URL spelling without turning it into a false absence.
    let canonical_identity = identity.to_ascii_lowercase();
    (
        StatusCode::OK,
        Json(PresenceResponse {
            identity: canonical_identity.clone(),
            connected: state.ws.is_mpc_identity_present(&canonical_identity),
        }),
    )
        .into_response()
}
