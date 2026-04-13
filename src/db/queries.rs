use std::collections::HashMap;
use std::sync::OnceLock;

use chrono::{DateTime, SecondsFormat, Utc};
use sqlx::{QueryBuilder, Row};

use super::DbPool;

static DELIVERY_FEE_CACHE: OnceLock<HashMap<String, i64>> = OnceLock::new();

/// Upsert a `server_fees` row. Creates the row if it does not exist; overwrites
/// `delivery_fee` if it does. Must be called before `init_delivery_fee_cache`
/// so the in-memory cache reflects the operator-supplied value.
pub async fn upsert_server_fee(
    pool: &DbPool,
    message_box: &str,
    delivery_fee: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO server_fees (message_box, delivery_fee) \
         VALUES (?, ?) \
         ON DUPLICATE KEY UPDATE delivery_fee = VALUES(delivery_fee)",
    )
    .bind(message_box)
    .bind(delivery_fee)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn init_delivery_fee_cache(pool: &DbPool) -> Result<(), sqlx::Error> {
    let rows: Vec<(String, i64)> =
        sqlx::query_as("SELECT message_box, delivery_fee FROM server_fees")
            .fetch_all(pool)
            .await?;
    let map: HashMap<String, i64> = rows.into_iter().collect();
    let _ = DELIVERY_FEE_CACHE.set(map);
    Ok(())
}

fn fmt_ts(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::Millis, true)
}

// ---------------------------------------------------------------------------
// Row types
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct MessageRow {
    pub message_id: String,
    pub body: String,
    pub sender: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug)]
pub struct PermissionRow {
    pub id: i64,
    pub recipient: String,
    pub sender: Option<String>,
    pub message_box: String,
    pub recipient_fee: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug)]
pub struct DeviceRow {
    pub id: i64,
    pub identity_key: String,
    pub fcm_token: String,
    pub device_id: Option<String>,
    pub platform: Option<String>,
    pub last_used: Option<String>,
    pub active: bool,
    pub created_at: String,
    pub updated_at: String,
}

// ---------------------------------------------------------------------------
// Message-box helpers
// ---------------------------------------------------------------------------

pub async fn ensure_message_box(
    pool: &DbPool,
    identity_key: &str,
    box_type: &str,
) -> Result<i64, sqlx::Error> {
    sqlx::query("INSERT IGNORE INTO messageBox (type, identityKey) VALUES (?, ?)")
        .bind(box_type)
        .bind(identity_key)
        .execute(pool)
        .await?;

    let id: i64 = sqlx::query_scalar::<_, i64>(
        "SELECT CAST(messageBoxId AS SIGNED) FROM messageBox WHERE type = ? AND identityKey = ?",
    )
    .bind(box_type)
    .bind(identity_key)
    .fetch_one(pool)
    .await?;
    Ok(id)
}

pub async fn get_message_box_id(
    pool: &DbPool,
    identity_key: &str,
    box_type: &str,
) -> Result<Option<i64>, sqlx::Error> {
    sqlx::query_scalar::<_, i64>(
        "SELECT CAST(messageBoxId AS SIGNED) FROM messageBox WHERE type = ? AND identityKey = ?",
    )
    .bind(box_type)
    .bind(identity_key)
    .fetch_optional(pool)
    .await
}

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

/// Insert a message. Returns Ok(true) on success, Ok(false) if duplicate.
pub async fn insert_message(
    pool: &DbPool,
    message_id: &str,
    message_box_id: i64,
    sender: &str,
    recipient: &str,
    body: &str,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        "INSERT IGNORE INTO messages (messageId, messageBoxId, sender, recipient, body) \
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(message_id)
    .bind(message_box_id)
    .bind(sender)
    .bind(recipient)
    .bind(body)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

pub async fn list_messages(
    pool: &DbPool,
    recipient: &str,
    message_box_id: i64,
) -> Result<Vec<MessageRow>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT messageId, body, sender, created_at, updated_at \
         FROM messages WHERE recipient = ? AND messageBoxId = ? \
         ORDER BY created_at ASC",
    )
    .bind(recipient)
    .bind(message_box_id)
    .fetch_all(pool)
    .await?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(MessageRow {
            message_id: row.try_get("messageId")?,
            body: row.try_get("body")?,
            sender: row.try_get("sender")?,
            created_at: fmt_ts(row.try_get::<DateTime<Utc>, _>("created_at")?),
            updated_at: fmt_ts(row.try_get::<DateTime<Utc>, _>("updated_at")?),
        });
    }
    Ok(out)
}

/// Delete acknowledged messages. Returns the number of rows deleted.
pub async fn acknowledge_messages(
    pool: &DbPool,
    recipient: &str,
    message_ids: &[String],
) -> Result<u64, sqlx::Error> {
    if message_ids.is_empty() {
        return Ok(0);
    }
    let mut qb: QueryBuilder<sqlx::MySql> =
        QueryBuilder::new("DELETE FROM messages WHERE recipient = ");
    qb.push_bind(recipient);
    qb.push(" AND messageId IN (");
    let mut sep = qb.separated(", ");
    for id in message_ids {
        sep.push_bind(id);
    }
    qb.push(")");
    let result = qb.build().execute(pool).await?;
    Ok(result.rows_affected())
}

// ---------------------------------------------------------------------------
// Fees & permissions
// ---------------------------------------------------------------------------

pub async fn get_server_delivery_fee(
    pool: &DbPool,
    message_box: &str,
) -> Result<i64, sqlx::Error> {
    if let Some(cache) = DELIVERY_FEE_CACHE.get() {
        return Ok(cache.get(message_box).copied().unwrap_or(0));
    }

    let fee: Option<i64> =
        sqlx::query_scalar("SELECT delivery_fee FROM server_fees WHERE message_box = ?")
            .bind(message_box)
            .fetch_optional(pool)
            .await?;
    Ok(fee.unwrap_or(0))
}

/// Smart default fee: notifications=10, everything else=0.
fn smart_default_fee(message_box: &str) -> i64 {
    if message_box == "notifications" {
        10
    } else {
        0
    }
}

/// Hierarchical recipient-fee lookup (single-query):
/// 1. Sender-specific permission (non-NULL sender wins)
/// 2. Box-wide default (sender IS NULL)
/// 3. Auto-create box-wide default with smart_default_fee
pub async fn get_recipient_fee(
    pool: &DbPool,
    recipient: &str,
    sender: &str,
    message_box: &str,
) -> Result<i64, sqlx::Error> {
    // Single query: prefer sender-specific over NULL sender.
    // MySQL places NULL last on DESC, so the non-NULL row wins with LIMIT 1.
    let result: Option<i64> = sqlx::query_scalar(
        "SELECT recipient_fee FROM message_permissions \
         WHERE recipient = ? AND message_box = ? \
         AND (sender = ? OR sender IS NULL) \
         ORDER BY sender DESC \
         LIMIT 1",
    )
    .bind(recipient)
    .bind(message_box)
    .bind(sender)
    .fetch_optional(pool)
    .await?;

    if let Some(fee) = result {
        return Ok(fee);
    }

    // Nothing found anywhere: auto-create the box-wide default.
    let default_fee = smart_default_fee(message_box);
    sqlx::query(
        "INSERT IGNORE INTO message_permissions (recipient, sender, message_box, recipient_fee) \
         VALUES (?, NULL, ?, ?)",
    )
    .bind(recipient)
    .bind(message_box)
    .bind(default_fee)
    .execute(pool)
    .await?;

    Ok(default_fee)
}

/// Upsert a permission. Returns Ok(true) on success.
pub async fn set_message_permission(
    pool: &DbPool,
    recipient: &str,
    sender: Option<&str>,
    message_box: &str,
    recipient_fee: i64,
) -> Result<bool, sqlx::Error> {
    match sender {
        Some(s) => {
            // Non-null sender: UNIQUE(recipient, sender, message_box) matches on
            // concrete values, so ON DUPLICATE KEY UPDATE works directly.
            sqlx::query(
                "INSERT INTO message_permissions (recipient, sender, message_box, recipient_fee) \
                 VALUES (?, ?, ?, ?) \
                 ON DUPLICATE KEY UPDATE \
                   recipient_fee = VALUES(recipient_fee), \
                   updated_at = CURRENT_TIMESTAMP",
            )
            .bind(recipient)
            .bind(s)
            .bind(message_box)
            .bind(recipient_fee)
            .execute(pool)
            .await?;
        }
        None => {
            // MySQL treats NULL as distinct inside UNIQUE constraints, so
            // ON DUPLICATE KEY UPDATE will never match a NULL-sender row.
            // Do SELECT-then-INSERT/UPDATE explicitly.
            let exists: Option<i64> = sqlx::query_scalar(
                "SELECT CAST(id AS SIGNED) AS id FROM message_permissions \
                 WHERE recipient = ? AND sender IS NULL AND message_box = ? LIMIT 1",
            )
            .bind(recipient)
            .bind(message_box)
            .fetch_optional(pool)
            .await?;

            if exists.is_some() {
                sqlx::query(
                    "UPDATE message_permissions \
                     SET recipient_fee = ?, updated_at = CURRENT_TIMESTAMP \
                     WHERE recipient = ? AND sender IS NULL AND message_box = ?",
                )
                .bind(recipient_fee)
                .bind(recipient)
                .bind(message_box)
                .execute(pool)
                .await?;
            } else {
                sqlx::query(
                    "INSERT INTO message_permissions (recipient, sender, message_box, recipient_fee) \
                     VALUES (?, NULL, ?, ?)",
                )
                .bind(recipient)
                .bind(message_box)
                .bind(recipient_fee)
                .execute(pool)
                .await?;
            }
        }
    }

    Ok(true)
}

pub async fn get_permission(
    pool: &DbPool,
    recipient: &str,
    sender: Option<&str>,
    message_box: &str,
) -> Result<Option<PermissionRow>, sqlx::Error> {
    let row_opt = match sender {
        Some(s) => {
            sqlx::query(
                "SELECT CAST(id AS SIGNED) AS id, recipient, sender, message_box, recipient_fee, created_at, updated_at \
                 FROM message_permissions \
                 WHERE recipient = ? AND sender = ? AND message_box = ?",
            )
            .bind(recipient)
            .bind(s)
            .bind(message_box)
            .fetch_optional(pool)
            .await?
        }
        None => {
            sqlx::query(
                "SELECT CAST(id AS SIGNED) AS id, recipient, sender, message_box, recipient_fee, created_at, updated_at \
                 FROM message_permissions \
                 WHERE recipient = ? AND sender IS NULL AND message_box = ?",
            )
            .bind(recipient)
            .bind(message_box)
            .fetch_optional(pool)
            .await?
        }
    };

    match row_opt {
        Some(row) => Ok(Some(PermissionRow {
            id: row.try_get::<i64, _>("id")?,
            recipient: row.try_get("recipient")?,
            sender: row.try_get("sender")?,
            message_box: row.try_get("message_box")?,
            recipient_fee: row.try_get("recipient_fee")?,
            created_at: fmt_ts(row.try_get::<DateTime<Utc>, _>("created_at")?),
            updated_at: fmt_ts(row.try_get::<DateTime<Utc>, _>("updated_at")?),
        })),
        None => Ok(None),
    }
}

pub async fn list_permissions(
    pool: &DbPool,
    recipient: &str,
    message_box: Option<&str>,
    limit: i64,
    offset: i64,
    order: &str,
) -> Result<(Vec<PermissionRow>, i64), sqlx::Error> {
    let order_dir = if order.eq_ignore_ascii_case("desc") {
        "DESC"
    } else {
        "ASC"
    };

    // Total count
    let total_count: i64 = match message_box {
        Some(mb) => sqlx::query_scalar(
            "SELECT COUNT(*) FROM message_permissions WHERE recipient = ? AND message_box = ?",
        )
        .bind(recipient)
        .bind(mb)
        .fetch_one(pool)
        .await?,
        None => sqlx::query_scalar(
            "SELECT COUNT(*) FROM message_permissions WHERE recipient = ?",
        )
        .bind(recipient)
        .fetch_one(pool)
        .await?,
    };

    // Rows. Order: box asc, NULL-sender first, then sender asc, then created_at by order_dir.
    let select_sql = match message_box {
        Some(_) => format!(
            "SELECT CAST(id AS SIGNED) AS id, recipient, sender, message_box, recipient_fee, created_at, updated_at \
             FROM message_permissions \
             WHERE recipient = ? AND message_box = ? \
             ORDER BY message_box ASC, \
                      CASE WHEN sender IS NULL THEN 0 ELSE 1 END, \
                      sender ASC, \
                      created_at {order_dir} \
             LIMIT ? OFFSET ?"
        ),
        None => format!(
            "SELECT CAST(id AS SIGNED) AS id, recipient, sender, message_box, recipient_fee, created_at, updated_at \
             FROM message_permissions \
             WHERE recipient = ? \
             ORDER BY message_box ASC, \
                      CASE WHEN sender IS NULL THEN 0 ELSE 1 END, \
                      sender ASC, \
                      created_at {order_dir} \
             LIMIT ? OFFSET ?"
        ),
    };

    let rows = match message_box {
        Some(mb) => sqlx::query(&select_sql)
            .bind(recipient)
            .bind(mb)
            .bind(limit)
            .bind(offset)
            .fetch_all(pool)
            .await?,
        None => sqlx::query(&select_sql)
            .bind(recipient)
            .bind(limit)
            .bind(offset)
            .fetch_all(pool)
            .await?,
    };

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(PermissionRow {
            id: row.try_get::<i64, _>("id")?,
            recipient: row.try_get("recipient")?,
            sender: row.try_get("sender")?,
            message_box: row.try_get("message_box")?,
            recipient_fee: row.try_get("recipient_fee")?,
            created_at: fmt_ts(row.try_get::<DateTime<Utc>, _>("created_at")?),
            updated_at: fmt_ts(row.try_get::<DateTime<Utc>, _>("updated_at")?),
        });
    }

    Ok((out, total_count))
}

// ---------------------------------------------------------------------------
// Device registrations
// ---------------------------------------------------------------------------

pub async fn register_device(
    pool: &DbPool,
    identity_key: &str,
    fcm_token: &str,
    device_id: Option<&str>,
    platform: Option<&str>,
) -> Result<i64, sqlx::Error> {
    let result = sqlx::query(
        "INSERT INTO device_registrations \
           (identity_key, fcm_token, device_id, platform, last_used, active) \
         VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, 1) \
         ON DUPLICATE KEY UPDATE \
           identity_key = VALUES(identity_key), \
           device_id = VALUES(device_id), \
           platform = VALUES(platform), \
           last_used = CURRENT_TIMESTAMP, \
           active = 1, \
           updated_at = CURRENT_TIMESTAMP",
    )
    .bind(identity_key)
    .bind(fcm_token)
    .bind(device_id)
    .bind(platform)
    .execute(pool)
    .await?;

    // On an UPDATE (upsert hit an existing row) last_insert_id is the old row's id;
    // on INSERT it is the new id. For the existing-row case, look it up.
    let id = result.last_insert_id() as i64;
    if id > 0 {
        return Ok(id);
    }
    let found: i64 =
        sqlx::query_scalar("SELECT CAST(id AS SIGNED) FROM device_registrations WHERE fcm_token = ?")
            .bind(fcm_token)
            .fetch_one(pool)
            .await?;
    Ok(found)
}

pub async fn list_devices(
    pool: &DbPool,
    identity_key: &str,
) -> Result<Vec<DeviceRow>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT CAST(id AS SIGNED) AS id, identity_key, fcm_token, device_id, platform, \
                last_used, active, created_at, updated_at \
         FROM device_registrations WHERE identity_key = ? ORDER BY updated_at DESC",
    )
    .bind(identity_key)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(row_to_device).collect()
}

pub async fn list_active_devices(
    pool: &DbPool,
    identity_key: &str,
) -> Result<Vec<DeviceRow>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT CAST(id AS SIGNED) AS id, identity_key, fcm_token, device_id, platform, \
                last_used, active, created_at, updated_at \
         FROM device_registrations WHERE identity_key = ? AND active = 1 \
         ORDER BY updated_at DESC",
    )
    .bind(identity_key)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(row_to_device).collect()
}

fn row_to_device(row: sqlx::mysql::MySqlRow) -> Result<DeviceRow, sqlx::Error> {
    Ok(DeviceRow {
        id: row.try_get::<i64, _>("id")?,
        identity_key: row.try_get("identity_key")?,
        fcm_token: row.try_get("fcm_token")?,
        device_id: row.try_get("device_id")?,
        platform: row.try_get("platform")?,
        last_used: row
            .try_get::<Option<DateTime<Utc>>, _>("last_used")?
            .map(fmt_ts),
        active: row.try_get::<bool, _>("active")?,
        created_at: fmt_ts(row.try_get::<DateTime<Utc>, _>("created_at")?),
        updated_at: fmt_ts(row.try_get::<DateTime<Utc>, _>("updated_at")?),
    })
}

pub async fn update_device_last_used(pool: &DbPool, device_id: i64) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE device_registrations \
         SET last_used = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP \
         WHERE id = ?",
    )
    .bind(device_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn deactivate_device(pool: &DbPool, device_id: i64) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE device_registrations \
         SET active = 0, updated_at = CURRENT_TIMESTAMP \
         WHERE id = ?",
    )
    .bind(device_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub fn should_use_fcm_delivery(message_box: &str) -> bool {
    message_box == "notifications"
}
