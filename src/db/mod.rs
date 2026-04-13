use std::str::FromStr;
use std::time::Duration;

use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use sqlx::MySqlPool;

pub mod queries;

#[cfg(test)]
mod tests;

pub type DbPool = MySqlPool;

pub async fn new(database_url: &str, max_connections: u32) -> Result<DbPool, sqlx::Error> {
    let opts = MySqlConnectOptions::from_str(database_url)?;
    MySqlPoolOptions::new()
        .max_connections(max_connections.max(1))
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(opts)
        .await
}

pub async fn migrate(pool: &DbPool) -> Result<(), sqlx::migrate::MigrateError> {
    sqlx::migrate!("./migrations").run(pool).await
}
