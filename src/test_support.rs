#![cfg(test)]
//! Shared test infrastructure: a single MySQL container per test binary,
//! and a `fresh_pool()` helper that creates a per-test database so tests
//! are fully isolated.

use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use sqlx::{ConnectOptions, Executor, MySqlPool};
use std::str::FromStr;
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::mysql::Mysql;
use tokio::sync::OnceCell;

use crate::db;

// Public identity-key constants so test modules share them.
pub const TEST_KEY: &str = "028d37b941208cd6b8a4c28288eda5f2f16c2b3ab0fcb6d13c18b47fe37b971fc1";
pub const RECIPIENT_KEY: &str = "0350b59e3efb8e37ba1ba2bde37c24e2bed89346ef3dc46d780e2b99f3efe50d1c";

/// Lazily-initialised shared container for the current test binary.
/// Tuple: (container handle to keep alive, base URL `mysql://root@host:port`).
static CONTAINER: OnceCell<(ContainerAsync<Mysql>, String)> = OnceCell::const_new();

async fn base_url() -> &'static str {
    let (_container, url) = CONTAINER
        .get_or_init(|| async {
            let container = Mysql::default()
                .start()
                .await
                .expect("failed to start MySQL testcontainer");
            let port = container
                .get_host_port_ipv4(3306)
                .await
                .expect("failed to get host port for MySQL testcontainer");
            let url = format!("mysql://root@127.0.0.1:{port}");
            (container, url)
        })
        .await;
    url.as_str()
}

/// Create a fresh per-test database and return a pool bound to it. Runs
/// migrations and initialises the delivery-fee cache (idempotent).
pub async fn fresh_pool() -> MySqlPool {
    let base = base_url().await;
    let db_name = format!("test_{}", uuid::Uuid::new_v4().simple());

    // Connect to the root (no-DB) URL and create the fresh database.
    let root_opts = MySqlConnectOptions::from_str(base)
        .expect("valid base URL")
        .disable_statement_logging();
    let mut root_conn = root_opts
        .connect()
        .await
        .expect("connect to MySQL root for db creation");
    root_conn
        .execute(format!("CREATE DATABASE `{db_name}`").as_str())
        .await
        .expect("create per-test database");
    drop(root_conn);

    // Now connect the actual pool to that database.
    let pool_url = format!("{base}/{db_name}");
    let opts = MySqlConnectOptions::from_str(&pool_url).expect("valid per-test URL");
    let pool = MySqlPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(opts)
        .await
        .expect("connect per-test MySQL pool");

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("run migrations");

    // OnceLock-backed: first call seeds, subsequent calls are a no-op Err
    // which is fine — the already-seeded values are identical.
    let _ = db::queries::init_delivery_fee_cache(&pool).await;

    pool
}
