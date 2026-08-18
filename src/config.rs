use std::collections::HashSet;
use std::env;
use std::fmt;

use serde::Deserialize;

fn default_mysql_port() -> u16 {
    3306
}

#[derive(Debug, Deserialize)]
struct KnexDbConnection {
    host: String,
    #[serde(default = "default_mysql_port")]
    port: u16,
    user: String,
    password: String,
    database: String,
}

/// Default ceiling on one relayed `mpcEnvelope` body, overridable with
/// `MPC_ENVELOPE_MAX_BODY_BYTES`. Matches rust-mpc's own
/// `DEFAULT_HUB_MAX_PAYLOAD_BYTES` (`crates/core/src/config.rs:426`), because
/// both ends of the same hop must agree on what is transportable.
///
/// This is NOT the mailbox lane's `MAX_MESSAGE_BODY_BYTES`, and deliberately
/// so. The ceremony lane has a STRUCTURAL maximum an order of
/// magnitude above a chat message: rust-mpc's protocol driver caps a round's
/// plaintext at 256 KiB, and BRC-78 sealing (`IV(32) || ct || tag(16)`) plus
/// base64 (4/3) expand that to ~341 KiB on the wire — see `LARGEST_CEREMONY_BODY`
/// at `rust-mpc/crates/transport/src/hub_limits.rs:566-570`. The heaviest rounds
/// are CGGMP24 aux-info and key-refresh (3071-bit Paillier modulus plus the
/// fixed-M Π^prm / Π^mod proofs); roster size adds envelopes, not bytes. A
/// 128 KiB-class body cap would therefore break DKG and key-refresh outright.
/// 8 MiB leaves ~24x headroom, absorbs a `SecurityLevel192` bump, and still sits
/// under the WebSocket layer's own frame limits.
pub const DEFAULT_MPC_ENVELOPE_MAX_BODY_BYTES: usize = 8 * 1024 * 1024;

/// Configuration of the transient MPC relay lane — the `mpcEnvelope` verb.
///
/// Scoped to that verb ALONE. `sendMessage`, `joinRoom`, `leaveRoom` and every
/// HTTP mailbox route keep their existing behaviour and never consult this,
/// exactly as rust-mpc keeps its wallet-lane and ceremony-lane allowlists from
/// consulting each other (`crates/transport/src/wallet_lane_wire.rs:82-85`).
#[derive(Clone, Debug)]
pub struct MpcRelayConfig {
    /// `MPC_PEER_IDENTITIES` — comma-separated identity keys allowed to route
    /// ceremony traffic, canonicalized by [`canon_identity`].
    ///
    /// FAIL-CLOSED: an empty set refuses every `mpcEnvelope`, which is what an
    /// unset or blank variable produces. The lane is off until an operator
    /// names the peers, and [`Self::is_enabled`] lets the boot path say so once
    /// rather than leaving an operator to discover it from dropped rounds.
    pub peer_identities: HashSet<String>,
    /// `MPC_ENVELOPE_MAX_BODY_BYTES` — see
    /// [`DEFAULT_MPC_ENVELOPE_MAX_BODY_BYTES`] for the derivation.
    pub max_body_bytes: usize,
}

impl Default for MpcRelayConfig {
    /// The lane off (no peers) at the derived payload ceiling — the shape a
    /// deployment that never sets `MPC_PEER_IDENTITIES` runs with.
    fn default() -> Self {
        Self {
            peer_identities: HashSet::new(),
            max_body_bytes: DEFAULT_MPC_ENVELOPE_MAX_BODY_BYTES,
        }
    }
}

impl MpcRelayConfig {
    /// `false` when no peer identity is configured — every `mpcEnvelope` is
    /// refused.
    pub fn is_enabled(&self) -> bool {
        !self.peer_identities.is_empty()
    }
}

/// Default ceiling on one relayed wallet-lane `frame`, overridable with
/// `MPC_WALLET_FRAME_MAX_BYTES`.
///
/// This REUSES [`DEFAULT_MPC_ENVELOPE_MAX_BODY_BYTES`] rather than deriving a
/// wallet-specific figure, and the reason is worth stating: the wallet lane has
/// no structural maximum of its own to derive one FROM. A `frame` carries an
/// arbitrary BRC-100 call — a `createAction` with a large transaction, a
/// `listOutputs` page — so its size is set by application traffic, not by a
/// protocol's fixed round shape the way a CGGMP24 round is. The two bounds that
/// do exist are both ceilings, not derivations: the receiving box refuses any
/// nested frame over 16 MiB before it parses it (`MAX_WALLET_FRAME_BYTES`,
/// `rust-mpc/bins/enterprise-wallet/src/wallet_lane.rs:114`, checked at :773),
/// and rust-mpc pins its hub payload limit under the WebSocket layer's own
/// 16 MiB frame limit (`crates/transport/src/hub_limits.rs:622-626`). 8 MiB sits
/// below both, so nothing this relay forwards is something the box would then
/// refuse for size, and it is a number this codebase already carries rather than
/// one invented here.
///
/// It is a SEPARATE knob from the ceremony lane's all the same: the two lanes
/// carry unrelated traffic, and an operator raising the ceiling for a heavy
/// BRC-100 workload must not thereby raise what a ceremony peer may push.
pub const DEFAULT_WALLET_FRAME_MAX_BYTES: usize = DEFAULT_MPC_ENVELOPE_MAX_BODY_BYTES;

/// Configuration of the wallet-RPC relay lane — the `walletCall` /
/// `walletReply` verbs.
///
/// Deliberately a SEPARATE struct from [`MpcRelayConfig`], because the two
/// allowlists are separate populations. rust-mpc admits a wallet frame from the
/// union of its ceremony roster and this app allowlist, and admits an
/// `mpcEnvelope` from the roster ALONE — so an app named here can never route
/// ceremony traffic, which the wire contract calls being "scoped to the wallet
/// verbs by construction" (`crates/transport/src/wallet_lane_wire.rs:82-85`).
/// Keeping the app list out of `MpcRelayConfig` makes that separation
/// structural here too: [`crate::ws::admit_mpc_envelope`] has no field to
/// consult even if a future edit wanted one.
#[derive(Clone, Debug)]
pub struct WalletRelayConfig {
    /// `MPC_WALLET_APP_IDENTITIES` — comma-separated identity keys admitted for
    /// the wallet verbs ONLY, canonicalized by [`canon_identity`]. Mirrors
    /// rust-mpc's `WALLET_APP_IDENTITIES_ENV`
    /// (`bins/coordinator/src/mpc_hub.rs:113`) so one operator runbook covers
    /// both hubs.
    ///
    /// FAIL-CLOSED: an empty set admits no app. It is not the whole admission
    /// rule, though — a box or cosigner sends its `walletReply` under its own
    /// roster identity, so [`MpcRelayConfig::peer_identities`] is admitted here
    /// too (the union rule). With BOTH empty the wallet lane carries nothing.
    ///
    /// TRANSITIONAL, exactly as it is upstream: the production front door is the
    /// BRC-73 grant-proposal pairing flow, where admission derives from a
    /// permission token minted at introduction rather than from a static env
    /// list (`app-authorization-known-counterparty.md` §5).
    pub app_identities: HashSet<String>,
    /// `MPC_WALLET_FRAME_MAX_BYTES` — see [`DEFAULT_WALLET_FRAME_MAX_BYTES`].
    pub max_frame_bytes: usize,
}

impl Default for WalletRelayConfig {
    /// No app admitted, at the reused payload ceiling — the shape a deployment
    /// that never sets `MPC_WALLET_APP_IDENTITIES` runs with. Roster peers can
    /// still exchange wallet frames under this default; only non-roster apps are
    /// shut out.
    fn default() -> Self {
        Self {
            app_identities: HashSet::new(),
            max_frame_bytes: DEFAULT_WALLET_FRAME_MAX_BYTES,
        }
    }
}

impl WalletRelayConfig {
    /// `false` when NEITHER population can send a wallet frame: no app is
    /// allowlisted AND no ceremony peer is configured, so the union admission
    /// rule admits nobody and every `walletCall`/`walletReply` is refused.
    ///
    /// Takes the ceremony config because the wallet lane's admission is that
    /// union, not this struct alone — asking `WalletRelayConfig` on its own
    /// whether the lane is live would give the wrong answer for the common
    /// enterprise shape, where a box and a cosigner exchange wallet frames under
    /// their roster identities and no separate app is named at all.
    pub fn is_enabled(&self, mpc: &MpcRelayConfig) -> bool {
        !self.app_identities.is_empty() || !mpc.peer_identities.is_empty()
    }
}

/// Canonical form of an identity key for allowlist comparison: trimmed and
/// lowercased. Mirrors the fallback arm of the coordinator's `canon_identity`
/// (`rust-mpc/bins/coordinator/src/handlers.rs:3557`) — its primary arm parses
/// the key through `mpc_core::IdentityKey` and re-emits lowercase compressed
/// hex, which for a well-formed key is the same string this produces. The relay
/// does not link the MPC stack, so it canonicalizes textually.
pub fn canon_identity(key: &str) -> String {
    key.trim().to_lowercase()
}

#[derive(Clone)]
pub struct Config {
    pub node_env: String,
    pub port: u16,
    pub server_private_key: String,
    pub routing_prefix: String,
    pub db_source: String,
    pub db_max_connections: u32,
    pub bsv_network: String,
    pub wallet_storage_url: String,
    /// `REDIS_URL` — unset/empty → Model A (single instance, in-process
    /// routing, the default); set → Model B (Redis pub/sub backplane for
    /// cross-instance live push; safe to run N replicas behind a sticky LB).
    pub redis_url: Option<String>,
    /// `MAX_CONNECTIONS` — per-instance WebSocket connection ceiling for
    /// admission control (design D3). `0` (the default) = unlimited. Past the
    /// ceiling, NEW connections get 503 + Retry-After (Model B: the LB sheds
    /// to another instance); in-flight sessions are never affected.
    pub max_connections: usize,
    /// `DRAIN_TIMEOUT_SECS` — per-phase bound on the SIGTERM graceful drain
    /// (in-flight send quiesce, persist-queue flush). Default 30.
    pub drain_timeout_secs: u64,
    /// The transient MPC relay lane (`mpcEnvelope`): who may route ceremony
    /// traffic and how large one envelope may be. Off unless
    /// `MPC_PEER_IDENTITIES` names at least one peer.
    pub mpc_relay: MpcRelayConfig,
    /// The wallet-RPC relay lane (`walletCall`/`walletReply`): which non-roster
    /// apps may reach a box's BRC-100 surface, and how large one frame may be.
    /// Admission is the union of this list and [`Self::mpc_relay`]'s peers.
    pub wallet_relay: WalletRelayConfig,
    /// `MESSAGEBOX_PARITY_FEES=true` — restore the reference (TS/Go/CF) fee
    /// economics: seed the `notifications` box at delivery fee 10 and use a
    /// recipient smart-default of 10 for it. Default `false` = free delivery
    /// for every box (owner decision — a deliberate deviation from the
    /// reference, which is pay-to-deliver). `MESSAGEBOX_FEES` overrides still
    /// win per box. Free by default, byte-parity on demand.
    pub parity_fees: bool,
    /// Parsed from `MESSAGEBOX_FEES=chat=10,priority=100` — applied at boot.
    pub message_box_fees: Vec<(String, i64)>,
    /// Parse warnings from `MESSAGEBOX_FEES` — emitted after the logger is up.
    pub message_box_fees_warnings: Vec<String>,
    /// `ENABLE_FIREBASE=true` — explicit opt-in for FCM push notifications
    /// (TS parity §4.3: TS gates on this flag BEFORE looking at the project
    /// id; the earlier Rust auto-enabled on project-id presence, which was a
    /// fail-open drift).
    pub enable_firebase: bool,
    /// `FIREBASE_PROJECT_ID` — required when Firebase is enabled.
    pub firebase_project_id: Option<String>,
    /// `FIREBASE_SERVICE_ACCOUNT_JSON` — the service-account key material.
    /// SECRET: never logged, never in `Debug` output (E2 must not return).
    pub firebase_service_account_json: Option<String>,
    /// `FIREBASE_SERVICE_ACCOUNT_PATH` — file alternative to the inline JSON.
    pub firebase_service_account_path: Option<String>,
}

impl Config {
    pub fn load() -> Result<Self, String> {
        let node_env = env::var("NODE_ENV").unwrap_or_else(|_| "development".to_string());

        let default_port: u16 = if node_env == "production" { 3000 } else { 8080 };
        let port = env::var("PORT")
            .or_else(|_| env::var("HTTP_PORT"))
            .ok()
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(default_port);

        let server_private_key = env::var("SERVER_PRIVATE_KEY").unwrap_or_default();
        if server_private_key.is_empty() {
            return Err("SERVER_PRIVATE_KEY is required".to_string());
        }
        if server_private_key.len() != 64 {
            return Err("SERVER_PRIVATE_KEY must be a 64-character hex string".to_string());
        }
        // Validate it is valid hex.
        hex::decode(&server_private_key)
            .map_err(|e| format!("SERVER_PRIVATE_KEY is not valid hex: {e}"))?;

        let routing_prefix = env::var("ROUTING_PREFIX").unwrap_or_default();

        // Resolve the DB connection URL.
        //   1. DATABASE_URL (Railway convention) — verbatim
        //   2. KNEX_DB_CONNECTION (team/TS Knex convention) — JSON, build URL
        //   3. DB_SOURCE — verbatim (backwards compat)
        //   4. Default local-dev URL
        let db_source = if let Ok(url) = env::var("DATABASE_URL") {
            url
        } else if let Ok(raw) = env::var("KNEX_DB_CONNECTION") {
            let knex: KnexDbConnection = serde_json::from_str(&raw)
                .map_err(|e| format!("KNEX_DB_CONNECTION is not valid JSON: {e}"))?;
            let encoded_pw: String =
                form_urlencoded::byte_serialize(knex.password.as_bytes()).collect();
            let encoded_user: String =
                form_urlencoded::byte_serialize(knex.user.as_bytes()).collect();
            format!(
                "mysql://{}:{}@{}:{}/{}",
                encoded_user, encoded_pw, knex.host, knex.port, knex.database
            )
        } else if let Ok(url) = env::var("DB_SOURCE") {
            url
        } else {
            "mysql://root:root@127.0.0.1:3306/messagebox".to_string()
        };

        // Default 50 (was 10): under concurrent-session load the old default of 10
        // was the measured throughput cliff — 100+ WS clients polling listMessages
        // + sending starved the 10-slot sqlx pool and acquires timed out, collapsing
        // delivery from 100% to ~15%. MySQL allows 151 by default, so 50 is safe
        // headroom; raise further via DB_MAX_CONNECTIONS for high concurrency.
        let db_max_connections = env::var("DB_MAX_CONNECTIONS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(50);
        let bsv_network = env::var("BSV_NETWORK").unwrap_or_else(|_| "mainnet".to_string());

        let wallet_storage_url = env::var("WALLET_STORAGE_URL")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "https://storage.babbage.systems".to_string());

        // Model A/B toggle — see the field doc. Whitespace-only counts as unset.
        let redis_url = env::var("REDIS_URL").ok().filter(|s| !s.trim().is_empty());

        // Admission-control ceiling + drain bound (Phase 3 / D3) — field docs.
        let max_connections = env::var("MAX_CONNECTIONS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);
        let drain_timeout_secs = env::var("DRAIN_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(30);

        // MPC relay lane. The allowlist is fail-closed on an unset or blank
        // variable, and the payload ceiling only takes a positive override —
        // a `0` or unparseable value would otherwise refuse every round.
        let mpc_relay = MpcRelayConfig {
            peer_identities: parse_identity_allowlist(
                &env::var("MPC_PEER_IDENTITIES").unwrap_or_default(),
            ),
            max_body_bytes: env::var("MPC_ENVELOPE_MAX_BODY_BYTES")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_MPC_ENVELOPE_MAX_BODY_BYTES),
        };

        // Wallet-RPC lane. Same fail-closed parse on its OWN allowlist: an app
        // named here reaches the wallet verbs and nothing else, and the ceremony
        // lane never reads this list.
        let wallet_relay = WalletRelayConfig {
            app_identities: parse_identity_allowlist(
                &env::var("MPC_WALLET_APP_IDENTITIES").unwrap_or_default(),
            ),
            max_frame_bytes: env::var("MPC_WALLET_FRAME_MAX_BYTES")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_WALLET_FRAME_MAX_BYTES),
        };

        // Parse MESSAGEBOX_FEES=chat=10,priority=100
        // Format: comma-separated box_name=satoshis pairs. Whitespace is trimmed.
        // Malformed or negative entries are collected as warnings and emitted
        // after the logger is initialised in main(); the server still boots.
        let (message_box_fees, message_box_fees_warnings) =
            parse_message_box_fees(&env::var("MESSAGEBOX_FEES").unwrap_or_default());

        // Free delivery by default; opt into reference (TS/Go/CF) pay-to-deliver
        // economics for `notifications` with MESSAGEBOX_PARITY_FEES=true.
        let parity_fees = env::var("MESSAGEBOX_PARITY_FEES")
            .map(|v| v == "true")
            .unwrap_or(false);

        // Firebase (§4.3): explicit ENABLE_FIREBASE=true, then project id +
        // one of the credential sources. Resolution happens in main().
        let enable_firebase = env::var("ENABLE_FIREBASE")
            .map(|v| v == "true")
            .unwrap_or(false);
        let firebase_project_id = env::var("FIREBASE_PROJECT_ID")
            .ok()
            .filter(|s| !s.is_empty());
        let firebase_service_account_json = env::var("FIREBASE_SERVICE_ACCOUNT_JSON")
            .ok()
            .filter(|s| !s.is_empty());
        let firebase_service_account_path = env::var("FIREBASE_SERVICE_ACCOUNT_PATH")
            .ok()
            .filter(|s| !s.is_empty());

        Ok(Config {
            node_env,
            port,
            server_private_key,
            routing_prefix,
            db_source,
            db_max_connections,
            bsv_network,
            wallet_storage_url,
            redis_url,
            max_connections,
            drain_timeout_secs,
            mpc_relay,
            wallet_relay,
            parity_fees,
            message_box_fees,
            message_box_fees_warnings,
            enable_firebase,
            firebase_project_id,
            firebase_service_account_json,
            firebase_service_account_path,
        })
    }

    pub fn is_development(&self) -> bool {
        self.node_env != "production"
    }
}

/// Parse `MESSAGEBOX_FEES` value into `(box_name, delivery_fee)` pairs.
///
/// Accepted format: `chat=10,priority=100 , notifications = 5`
/// - Whitespace around names/values is trimmed.
/// - Empty tokens (e.g. from a trailing comma) are silently skipped.
/// - Entries without exactly one `=` separator → warning string returned, skipped.
/// - Values that are not valid `i64` → warning string returned, skipped.
/// - Negative values → warning string returned, skipped.
///
/// Returns `(valid_pairs, warnings)`. Warnings must be emitted by the caller
/// after the tracing subscriber has been installed.
fn parse_message_box_fees(raw: &str) -> (Vec<(String, i64)>, Vec<String>) {
    if raw.trim().is_empty() {
        return (Vec::new(), Vec::new());
    }
    let mut out = Vec::new();
    let mut warnings = Vec::new();
    for token in raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        // Split on the first '=' only; a box name should never contain '='.
        let parts: Vec<&str> = token.splitn(2, '=').collect();
        if parts.len() != 2 {
            warnings.push(format!(
                "MESSAGEBOX_FEES: malformed entry {token:?} (expected box_name=satoshis) — skipped"
            ));
            continue;
        }
        let box_name = parts[0].trim();
        let fee_str = parts[1].trim();
        if box_name.is_empty() {
            warnings.push(format!(
                "MESSAGEBOX_FEES: entry {token:?} has an empty box name — skipped"
            ));
            continue;
        }
        let fee: i64 = match fee_str.parse() {
            Ok(v) => v,
            Err(_) => {
                warnings.push(format!(
                    "MESSAGEBOX_FEES: entry {token:?} has non-integer fee {fee_str:?} — skipped"
                ));
                continue;
            }
        };
        if fee < 0 {
            warnings.push(format!(
                "MESSAGEBOX_FEES: entry {token:?} has negative fee {fee} — skipped"
            ));
            continue;
        }
        out.push((box_name.to_string(), fee));
    }
    (out, warnings)
}

/// Parse one relay allowlist variable — `MPC_PEER_IDENTITIES` for the ceremony
/// lane, `MPC_WALLET_APP_IDENTITIES` for the wallet lane.
///
/// Accepted format: `02aa…,03bb…` — comma-separated identity keys, whitespace
/// around each trimmed, empty tokens (a trailing comma) skipped. Every entry is
/// canonicalized with [`canon_identity`] so an operator pasting mixed-case hex
/// still matches the verified socket identity.
///
/// The two lanes share this PARSER while keeping separate SETS: the format is
/// identical, and the isolation that matters is which set each admission check
/// reads, not how the strings were split.
///
/// A blank or absent value yields an EMPTY set, and an empty set admits nobody —
/// both lanes are opt-in, never open by default. Unlike
/// [`parse_message_box_fees`] there is nothing to warn about per entry: any
/// string an operator writes here is either a key that will match a verified
/// socket or one that never will, and refusing to route for an identity that
/// never connects is the intended outcome either way.
fn parse_identity_allowlist(raw: &str) -> HashSet<String> {
    raw.split(',')
        .map(canon_identity)
        .filter(|id| !id.is_empty())
        .collect()
}

/// Redact the password portion of a `mysql://user:pass@host:port/db` URL.
/// Leaves non-credentialed URLs unchanged. Best-effort; if parsing fails,
/// returns the input as-is.
fn redact_db_url(url: &str) -> String {
    let scheme_sep = match url.find("://") {
        Some(i) => i + 3,
        None => return url.to_string(),
    };
    let rest = &url[scheme_sep..];
    // Find the '@' separating userinfo from host. Only present when creds exist.
    let at = match rest.find('@') {
        Some(i) => i,
        None => return url.to_string(),
    };
    let userinfo = &rest[..at];
    let after = &rest[at..]; // starts with '@'
                             // Split userinfo into user / password at first ':'.
    let redacted_userinfo = match userinfo.find(':') {
        Some(ci) => format!("{}:***", &userinfo[..ci]),
        None => userinfo.to_string(),
    };
    format!("{}{}{}", &url[..scheme_sep], redacted_userinfo, after)
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("node_env", &self.node_env)
            .field("port", &self.port)
            .field("server_private_key", &"***redacted***")
            .field("routing_prefix", &self.routing_prefix)
            .field("db_source", &redact_db_url(&self.db_source))
            .field("db_max_connections", &self.db_max_connections)
            .field("bsv_network", &self.bsv_network)
            .field("wallet_storage_url", &self.wallet_storage_url)
            .field("redis_url", &self.redis_url.as_deref().map(redact_db_url))
            .field("max_connections", &self.max_connections)
            .field("drain_timeout_secs", &self.drain_timeout_secs)
            .field("mpc_relay", &self.mpc_relay)
            .field("wallet_relay", &self.wallet_relay)
            .field("parity_fees", &self.parity_fees)
            .field("message_box_fees", &self.message_box_fees)
            // message_box_fees_warnings are transient — omitted from Debug output.
            .field("enable_firebase", &self.enable_firebase)
            .field("firebase_project_id", &self.firebase_project_id)
            // E2 guard: the service-account JSON is key material and must
            // NEVER appear in Debug output or logs — only its presence.
            .field(
                "firebase_service_account_json",
                &self
                    .firebase_service_account_json
                    .as_ref()
                    .map(|_| "***redacted***"),
            )
            .field(
                "firebase_service_account_path",
                &self.firebase_service_account_path,
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// E2 must not return: the Firebase service-account key material never
    /// appears in the config's Debug output (which is what boot logging and
    /// error reports print).
    #[test]
    fn debug_never_leaks_firebase_service_account_json() {
        let secret = r#"{"private_key":"-----BEGIN PRIVATE KEY-----SECRETMATERIAL"}"#;
        let config = Config {
            node_env: "production".into(),
            port: 3000,
            server_private_key: "b".repeat(64),
            routing_prefix: String::new(),
            db_source: "mysql://user:dbpass@host/db".into(),
            db_max_connections: 10,
            bsv_network: "mainnet".into(),
            wallet_storage_url: "https://storage.example".into(),
            redis_url: None,
            max_connections: 0,
            drain_timeout_secs: 30,
            mpc_relay: MpcRelayConfig::default(),
            wallet_relay: WalletRelayConfig::default(),
            parity_fees: false,
            message_box_fees: Vec::new(),
            message_box_fees_warnings: Vec::new(),
            enable_firebase: true,
            firebase_project_id: Some("proj-1".into()),
            firebase_service_account_json: Some(secret.into()),
            firebase_service_account_path: None,
        };
        let out = format!("{config:?}");
        assert!(!out.contains("SECRETMATERIAL"), "E2: key material leaked");
        assert!(!out.contains("PRIVATE KEY"), "E2: key material leaked");
        assert!(out.contains("***redacted***"));
        // The pre-existing redactions still hold.
        assert!(!out.contains(&"b".repeat(64)), "server key leaked");
        assert!(!out.contains("dbpass"), "db password leaked");
    }

    /// An unset or blank `MPC_PEER_IDENTITIES` leaves the ceremony lane OFF.
    /// Fail-closed is the whole point: a server that silently relayed MPC
    /// traffic for anyone would be an unintended open ceremony hub.
    #[test]
    fn unset_peer_identities_leaves_the_mpc_lane_disabled() {
        for raw in ["", "   ", ",", " , ,"] {
            let relay = MpcRelayConfig {
                peer_identities: parse_identity_allowlist(raw),
                ..Default::default()
            };
            assert!(!relay.is_enabled(), "{raw:?} must not enable the lane");
        }
    }

    /// Entries are trimmed, lowercased and de-duplicated, so an operator pasting
    /// keys out of a dashboard still gets a set that matches verified socket
    /// identities.
    #[test]
    fn peer_identities_are_canonicalized() {
        let a = "02AABBCCDDEEFF00112233445566778899AABBCCDDEEFF00112233445566778899";
        let b = "03bbccddeeff00112233445566778899aabbccddeeff001122334455667788990a";
        let parsed = parse_identity_allowlist(&format!(" {a} , {b},{}, ", a.to_lowercase()));
        assert_eq!(parsed.len(), 2);
        assert!(parsed.contains(&a.to_lowercase()));
        assert!(parsed.contains(b));
    }

    /// The ceremony lane's ceiling is an order of magnitude above the mailbox
    /// lane's, because a CGGMP24 key-refresh round structurally cannot fit under
    /// a chat-sized cap. Pinned so a future "let's unify the limits" edit fails
    /// here rather than on a live DKG.
    #[test]
    fn mpc_envelope_ceiling_clears_the_largest_ceremony_body() {
        // `(256 KiB + 48) * 4/3` rounded up, plus the JSON wrapper — the same
        // derivation as rust-mpc's LARGEST_CEREMONY_BODY.
        let largest_ceremony_body = ((256usize * 1024 + 48) * 4).div_ceil(3) + 32;
        assert!(
            DEFAULT_MPC_ENVELOPE_MAX_BODY_BYTES > largest_ceremony_body * 20,
            "the default must keep generous headroom over the structural maximum"
        );
    }

    /// With neither allowlist configured the wallet lane is OFF: no app is
    /// named and no roster peer exists, so the union admits nobody. A server
    /// that relayed BRC-100 calls for anyone would be an open front door onto
    /// every box dialed into it.
    #[test]
    fn unset_allowlists_leave_the_wallet_lane_disabled() {
        let wallet = WalletRelayConfig::default();
        assert!(!wallet.is_enabled(&MpcRelayConfig::default()));
    }

    /// Either population alone turns the lane on, because admission is their
    /// union: an app-only deployment serves a box that is named as a ceremony
    /// peer, and a roster-only deployment carries the box's own `walletReply`s
    /// with no third-party app in the picture.
    #[test]
    fn either_population_enables_the_wallet_lane() {
        let app = "02aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899";
        let apps_only = WalletRelayConfig {
            app_identities: parse_identity_allowlist(app),
            ..Default::default()
        };
        assert!(apps_only.is_enabled(&MpcRelayConfig::default()));

        let roster_only = MpcRelayConfig {
            peer_identities: parse_identity_allowlist(app),
            ..Default::default()
        };
        assert!(WalletRelayConfig::default().is_enabled(&roster_only));
    }
}
