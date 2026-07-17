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

        // Parse MESSAGEBOX_FEES=chat=10,priority=100
        // Format: comma-separated box_name=satoshis pairs. Whitespace is trimmed.
        // Malformed or negative entries are collected as warnings and emitted
        // after the logger is initialised in main(); the server still boots.
        let (message_box_fees, message_box_fees_warnings) =
            parse_message_box_fees(&env::var("MESSAGEBOX_FEES").unwrap_or_default());

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
}
