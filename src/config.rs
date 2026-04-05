use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub node_env: String,
    pub port: u16,
    pub server_private_key: String,
    pub routing_prefix: String,
    pub db_driver: String,
    pub db_source: String,
    pub bsv_network: String,
    pub enable_websockets: bool,
    pub wallet_storage_url: Option<String>,
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
        let db_driver = env::var("DB_DRIVER").unwrap_or_else(|_| "sqlite3".to_string());
        let db_source = env::var("DB_SOURCE").unwrap_or_else(|_| "messagebox.db".to_string());
        let bsv_network = env::var("BSV_NETWORK").unwrap_or_else(|_| "mainnet".to_string());

        let enable_websockets = env::var("ENABLE_WEBSOCKETS")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        let wallet_storage_url = env::var("WALLET_STORAGE_URL").ok().filter(|s| !s.is_empty());
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
            db_driver,
            db_source,
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
}
