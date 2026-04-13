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
    pub enable_websockets: bool,
    pub wallet_storage_url: String,
    pub firebase_project_id: Option<String>,
    pub firebase_service_account_json: Option<String>,
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

        let server_private_key =
            env::var("SERVER_PRIVATE_KEY").unwrap_or_default();
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

        let db_max_connections = env::var("DB_MAX_CONNECTIONS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(10);
        let bsv_network = env::var("BSV_NETWORK").unwrap_or_else(|_| "mainnet".to_string());

        let enable_websockets = env::var("ENABLE_WEBSOCKETS")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        let wallet_storage_url = env::var("WALLET_STORAGE_URL")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "https://storage.babbage.systems".to_string());
        let firebase_project_id =
            env::var("FIREBASE_PROJECT_ID").ok().filter(|s| !s.is_empty());
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
            enable_websockets,
            wallet_storage_url,
            firebase_project_id,
            firebase_service_account_json,
            firebase_service_account_path,
        })
    }

    pub fn is_development(&self) -> bool {
        self.node_env != "production"
    }

    /// True when running on Railway (detected via `RAILWAY_ENVIRONMENT`).
    pub fn is_railway(&self) -> bool {
        std::env::var("RAILWAY_ENVIRONMENT").is_ok()
    }
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
            .field("enable_websockets", &self.enable_websockets)
            .field("wallet_storage_url", &self.wallet_storage_url)
            .field("firebase_project_id", &self.firebase_project_id)
            .field(
                "firebase_service_account_json",
                &self.firebase_service_account_json,
            )
            .field(
                "firebase_service_account_path",
                &self.firebase_service_account_path,
            )
            .finish()
    }
}
