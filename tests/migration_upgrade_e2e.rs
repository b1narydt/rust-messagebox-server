//! In-place upgrade e2e (the scenario that previously crash-looped prod).
//!
//! A database migrated by the ORIGINAL 4-migration chain must forward-migrate
//! cleanly — no `VersionMissing` crash-loop — and converge to the squashed
//! schema, with existing data intact. This drives the real `sqlx` migrator
//! against a faked "already applied the originals" state (checksums read from
//! the embedded set, so they validate), then asserts the forward migration
//! (`20260718000000`) both applies and converges. Needs Docker (MySQL).

use std::time::Duration;

use messagebox_server::db::{self, DbPool, MIGRATOR};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::mysql::Mysql;

/// The single additive forward migration; everything below it is an "original".
const FORWARD_VERSION: i64 = 20260718000000;

/// A fresh, EMPTY database (no migrations applied).
async fn raw_pool() -> DbPool {
    use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
    use sqlx::{ConnectOptions, Executor};
    use std::str::FromStr;

    let container = Mysql::default()
        .start()
        .await
        .expect("start MySQL testcontainer (Docker required)");
    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("mysql port");
    // Leak the container handle so it outlives the pool for the test's duration.
    Box::leak(Box::new(container));

    let base = format!("mysql://root@127.0.0.1:{port}");
    let db_name = "legacy_upgrade";
    let mut root = MySqlConnectOptions::from_str(&base)
        .expect("base")
        .disable_statement_logging()
        .connect()
        .await
        .expect("connect root");
    root.execute(format!("CREATE DATABASE `{db_name}`").as_str())
        .await
        .expect("create db");
    drop(root);

    MySqlPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&format!("{base}/{db_name}"))
        .await
        .expect("connect pool")
}

#[tokio::test]
async fn old_chain_db_upgrades_in_place() {
    let pool = raw_pool().await;

    // 1. Reproduce a legacy DB: run the ORIGINAL migrations' SQL directly, and
    //    record them in `_sqlx_migrations` with the checksums the embedded set
    //    carries — exactly what a DB migrated by the old build would hold.
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS _sqlx_migrations (\
         version BIGINT PRIMARY KEY, description TEXT NOT NULL, \
         installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, \
         success BOOLEAN NOT NULL, checksum BLOB NOT NULL, execution_time BIGINT NOT NULL);",
    )
    .execute(&pool)
    .await
    .expect("create _sqlx_migrations");

    for m in MIGRATOR.iter().filter(|m| m.version < FORWARD_VERSION) {
        sqlx::raw_sql(&m.sql)
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("apply original migration {}: {e}", m.version));
        sqlx::query(
            "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) \
             VALUES (?, ?, TRUE, ?, 0)",
        )
        .bind(m.version)
        .bind(&*m.description)
        .bind(&*m.checksum)
        .execute(&pool)
        .await
        .expect("seed applied-migration row");
    }

    // Legacy `messages` has no surrogate PK yet; seed rows the ALTER must keep.
    sqlx::query(
        "INSERT INTO messages (messageId, sender, recipient, body) \
         VALUES ('legacy-1','02aa','03bb','{}'), ('legacy-2','02aa','03bb','{}')",
    )
    .execute(&pool)
    .await
    .expect("seed legacy messages");

    // Sanity: the legacy fee set still has the experiment boxes pre-upgrade.
    let pre: Vec<String> =
        sqlx::query_scalar("SELECT message_box FROM server_fees ORDER BY message_box")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert!(
        pre.contains(&"chat".to_string()) && pre.contains(&"payment_requests".to_string()),
        "legacy DB should carry the experiment fee rows before upgrade: {pre:?}"
    );

    // 2. THE upgrade: the real migrator. Must NOT return VersionMissing, and
    //    must apply only the forward migration.
    db::migrate(&pool)
        .await
        .expect("in-place forward migration must succeed (no VersionMissing crash-loop)");

    // 3a. messages gained the surrogate PK, and the legacy rows survived.
    let pk_cols: Vec<String> = sqlx::query_scalar(
        "SELECT column_name FROM information_schema.key_column_usage \
         WHERE table_schema = DATABASE() AND table_name = 'messages' \
         AND constraint_name = 'PRIMARY' ORDER BY ordinal_position",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(pk_cols, vec!["id".to_string()], "surrogate PK added");

    let (msg_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM messages")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(msg_count, 2, "legacy message rows survive the ALTER");

    let (unique_msgid,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM information_schema.table_constraints \
         WHERE table_schema = DATABASE() AND table_name = 'messages' \
         AND constraint_type = 'UNIQUE' AND constraint_name = 'uq_messages_messageid'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(unique_msgid, 1, "messageId stays UNIQUE (dedup preserved)");

    // 3b. Fee seed converged to the 3 reference boxes (experiment rows removed).
    let post: Vec<String> =
        sqlx::query_scalar("SELECT message_box FROM server_fees ORDER BY message_box")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        post,
        vec![
            "inbox".to_string(),
            "notifications".to_string(),
            "payment_inbox".to_string()
        ],
        "fee seed converges to the reference set (chat/payment_requests gone)"
    );
}
