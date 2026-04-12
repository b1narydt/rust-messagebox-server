use rusqlite::params;
use rusqlite::OptionalExtension;
use std::collections::HashMap;
use std::sync::OnceLock;

use super::DbPool;

static DELIVERY_FEE_CACHE: OnceLock<HashMap<String, i64>> = OnceLock::new();

pub fn init_delivery_fee_cache(pool: &DbPool) {
    let conn = pool.get().expect("failed to get connection for delivery fee cache init");
    let mut stmt = conn
        .prepare("SELECT message_box, delivery_fee FROM server_fees")
        .unwrap();
    let mut map = HashMap::new();
    let mut rows = stmt.query([]).unwrap();
    while let Some(row) = rows.next().unwrap() {
        let mb: String = row.get(0).unwrap();
        let fee: i64 = row.get(1).unwrap();
        map.insert(mb, fee);
    }
    let _ = DELIVERY_FEE_CACHE.set(map);
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

pub fn ensure_message_box(
    pool: &DbPool,
    identity_key: &str,
    box_type: &str,
) -> Result<i64, rusqlite::Error> {
    let conn = pool.get().map_err(|e| rusqlite::Error::InvalidParameterName(format!("{e}")))?;
    conn.execute(
        "INSERT OR IGNORE INTO messageBox (type, identityKey) VALUES (?1, ?2)",
        params![box_type, identity_key],
    )?;
    let id: i64 = conn.query_row(
        "SELECT messageBoxId FROM messageBox WHERE type = ?1 AND identityKey = ?2",
        params![box_type, identity_key],
        |row| row.get(0),
    )?;
    Ok(id)
}

pub fn get_message_box_id(
    pool: &DbPool,
    identity_key: &str,
    box_type: &str,
) -> Result<Option<i64>, rusqlite::Error> {
    let conn = pool.get().map_err(|e| rusqlite::Error::InvalidParameterName(format!("{e}")))?;
    let mut stmt = conn.prepare(
        "SELECT messageBoxId FROM messageBox WHERE type = ?1 AND identityKey = ?2",
    )?;
    let mut rows = stmt.query(params![box_type, identity_key])?;
    match rows.next()? {
        Some(row) => Ok(Some(row.get(0)?)),
        None => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

/// Insert a message. Returns Ok(true) on success, Ok(false) if duplicate.
pub fn insert_message(
    pool: &DbPool,
    message_id: &str,
    message_box_id: i64,
    sender: &str,
    recipient: &str,
    body: &str,
) -> Result<bool, rusqlite::Error> {
    let conn = pool.get().map_err(|e| rusqlite::Error::InvalidParameterName(format!("{e}")))?;
    let affected = conn.execute(
        "INSERT OR IGNORE INTO messages (messageId, messageBoxId, sender, recipient, body) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![message_id, message_box_id, sender, recipient, body],
    )?;
    Ok(affected > 0)
}

pub fn list_messages(
    pool: &DbPool,
    recipient: &str,
    message_box_id: i64,
) -> Result<Vec<MessageRow>, rusqlite::Error> {
    let conn = pool.get().map_err(|e| rusqlite::Error::InvalidParameterName(format!("{e}")))?;
    let mut stmt = conn.prepare(
        "SELECT messageId, body, sender, created_at, updated_at \
         FROM messages WHERE recipient = ?1 AND messageBoxId = ?2 \
         ORDER BY created_at ASC",
    )?;
    let rows = stmt.query_map(params![recipient, message_box_id], |row| {
        Ok(MessageRow {
            message_id: row.get(0)?,
            body: row.get(1)?,
            sender: row.get(2)?,
            created_at: row.get(3)?,
            updated_at: row.get(4)?,
        })
    })?;
    rows.collect()
}

/// Delete acknowledged messages. Returns the number of rows deleted.
pub fn acknowledge_messages(
    pool: &DbPool,
    recipient: &str,
    message_ids: &[String],
) -> Result<usize, rusqlite::Error> {
    if message_ids.is_empty() {
        return Ok(0);
    }
    let conn = pool.get().map_err(|e| rusqlite::Error::InvalidParameterName(format!("{e}")))?;
    let placeholders: Vec<String> = (0..message_ids.len()).map(|i| format!("?{}", i + 2)).collect();
    let sql = format!(
        "DELETE FROM messages WHERE recipient = ?1 AND messageId IN ({})",
        placeholders.join(", ")
    );
    let mut stmt = conn.prepare(&sql)?;

    let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
    param_values.push(Box::new(recipient.to_string()));
    for id in message_ids {
        param_values.push(Box::new(id.clone()));
    }
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = param_values.iter().map(|p| p.as_ref()).collect();
    let count = stmt.execute(param_refs.as_slice())?;
    Ok(count)
}

// ---------------------------------------------------------------------------
// Fees & permissions
// ---------------------------------------------------------------------------

pub fn get_server_delivery_fee(
    pool: &DbPool,
    message_box: &str,
) -> Result<i64, rusqlite::Error> {
    // Check static cache first (populated at startup)
    if let Some(cache) = DELIVERY_FEE_CACHE.get() {
        if let Some(&fee) = cache.get(message_box) {
            return Ok(fee);
        }
        // Key not in cache means no row in server_fees => default 0
        return Ok(0);
    }

    // Fallback to DB query (cache not yet initialised)
    let conn = pool.get().map_err(|e| rusqlite::Error::InvalidParameterName(format!("{e}")))?;
    let mut stmt =
        conn.prepare("SELECT delivery_fee FROM server_fees WHERE message_box = ?1")?;
    let mut rows = stmt.query(params![message_box])?;
    match rows.next()? {
        Some(row) => Ok(row.get(0)?),
        None => Ok(0),
    }
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
pub fn get_recipient_fee(
    pool: &DbPool,
    recipient: &str,
    sender: &str,
    message_box: &str,
) -> Result<i64, rusqlite::Error> {
    let conn = pool.get().map_err(|e| rusqlite::Error::InvalidParameterName(format!("{e}")))?;

    // Single query: try sender-specific first, then box-wide default
    let result: Option<i64> = conn
        .prepare(
            "SELECT recipient_fee FROM message_permissions \
             WHERE recipient = ?1 AND message_box = ?3 \
             AND (sender = ?2 OR sender IS NULL) \
             ORDER BY sender DESC \
             LIMIT 1",
        )?
        .query_row(params![recipient, sender, message_box], |row| row.get(0))
        .optional()?;

    if let Some(fee) = result {
        return Ok(fee);
    }

    // Auto-create box-wide default (only if nothing found at all)
    let default_fee = smart_default_fee(message_box);
    conn.execute(
        "INSERT OR IGNORE INTO message_permissions (recipient, sender, message_box, recipient_fee) \
         VALUES (?1, NULL, ?2, ?3)",
        params![recipient, message_box, default_fee],
    )?;

    Ok(default_fee)
}

/// Upsert a permission. Returns Ok(true) on success.
pub fn set_message_permission(
    pool: &DbPool,
    recipient: &str,
    sender: Option<&str>,
    message_box: &str,
    recipient_fee: i64,
) -> Result<bool, rusqlite::Error> {
    let conn = pool.get().map_err(|e| rusqlite::Error::InvalidParameterName(format!("{e}")))?;

    match sender {
        Some(s) => {
            // Non-null sender: use ON CONFLICT.
            conn.execute(
                "INSERT INTO message_permissions (recipient, sender, message_box, recipient_fee) \
                 VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT(recipient, sender, message_box) DO UPDATE SET \
                 recipient_fee = excluded.recipient_fee, \
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')",
                params![recipient, s, message_box, recipient_fee],
            )?;
        }
        None => {
            // NULL sender: SQLite treats NULLs as distinct in UNIQUE constraints,
            // so we check existence manually.
            let exists: bool = conn
                .prepare(
                    "SELECT COUNT(*) FROM message_permissions \
                     WHERE recipient = ?1 AND sender IS NULL AND message_box = ?2",
                )?
                .query_row(params![recipient, message_box], |row| {
                    Ok(row.get::<_, i64>(0)? > 0)
                })?;

            if exists {
                conn.execute(
                    "UPDATE message_permissions \
                     SET recipient_fee = ?1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                     WHERE recipient = ?2 AND sender IS NULL AND message_box = ?3",
                    params![recipient_fee, recipient, message_box],
                )?;
            } else {
                conn.execute(
                    "INSERT INTO message_permissions (recipient, sender, message_box, recipient_fee) \
                     VALUES (?1, NULL, ?2, ?3)",
                    params![recipient, message_box, recipient_fee],
                )?;
            }
        }
    }

    Ok(true)
}

pub fn get_permission(
    pool: &DbPool,
    recipient: &str,
    sender: Option<&str>,
    message_box: &str,
) -> Result<Option<PermissionRow>, rusqlite::Error> {
    let conn = pool.get().map_err(|e| rusqlite::Error::InvalidParameterName(format!("{e}")))?;
    let (sql, result) = match sender {
        Some(s) => {
            let mut stmt = conn.prepare(
                "SELECT id, recipient, sender, message_box, recipient_fee, created_at, updated_at \
                 FROM message_permissions \
                 WHERE recipient = ?1 AND sender = ?2 AND message_box = ?3",
            )?;
            let mut rows = stmt.query(params![recipient, s, message_box])?;
            let row = rows.next()?;
            match row {
                Some(r) => (true, Some(read_permission_row(r)?)),
                None => (false, None),
            }
        }
        None => {
            let mut stmt = conn.prepare(
                "SELECT id, recipient, sender, message_box, recipient_fee, created_at, updated_at \
                 FROM message_permissions \
                 WHERE recipient = ?1 AND sender IS NULL AND message_box = ?2",
            )?;
            let mut rows = stmt.query(params![recipient, message_box])?;
            let row = rows.next()?;
            match row {
                Some(r) => (true, Some(read_permission_row(r)?)),
                None => (false, None),
            }
        }
    };

    if sql { Ok(result) } else { Ok(None) }
}

fn read_permission_row(row: &rusqlite::Row<'_>) -> Result<PermissionRow, rusqlite::Error> {
    Ok(PermissionRow {
        id: row.get(0)?,
        recipient: row.get(1)?,
        sender: row.get(2)?,
        message_box: row.get(3)?,
        recipient_fee: row.get(4)?,
        created_at: row.get(5)?,
        updated_at: row.get(6)?,
    })
}

pub fn list_permissions(
    pool: &DbPool,
    recipient: &str,
    message_box: Option<&str>,
    limit: i64,
    offset: i64,
    order: &str,
) -> Result<(Vec<PermissionRow>, i64), rusqlite::Error> {
    let conn = pool.get().map_err(|e| rusqlite::Error::InvalidParameterName(format!("{e}")))?;

    let order_dir = if order.eq_ignore_ascii_case("desc") {
        "DESC"
    } else {
        "ASC"
    };

    let (where_clause, count_params, query_params): (
        String,
        Vec<Box<dyn rusqlite::types::ToSql>>,
        Vec<Box<dyn rusqlite::types::ToSql>>,
    ) = match message_box {
        Some(mb) => (
            "WHERE recipient = ?1 AND message_box = ?2".to_string(),
            vec![
                Box::new(recipient.to_string()) as Box<dyn rusqlite::types::ToSql>,
                Box::new(mb.to_string()),
            ],
            vec![
                Box::new(recipient.to_string()) as Box<dyn rusqlite::types::ToSql>,
                Box::new(mb.to_string()),
                Box::new(limit),
                Box::new(offset),
            ],
        ),
        None => (
            "WHERE recipient = ?1".to_string(),
            vec![Box::new(recipient.to_string()) as Box<dyn rusqlite::types::ToSql>],
            vec![
                Box::new(recipient.to_string()) as Box<dyn rusqlite::types::ToSql>,
                Box::new(limit),
                Box::new(offset),
            ],
        ),
    };

    // Total count
    let count_sql = format!("SELECT COUNT(*) FROM message_permissions {where_clause}");
    let count_refs: Vec<&dyn rusqlite::types::ToSql> =
        count_params.iter().map(|p| p.as_ref()).collect();
    let total_count: i64 =
        conn.prepare(&count_sql)?.query_row(count_refs.as_slice(), |row| row.get(0))?;

    // Build order-by clause.
    let order_by = format!(
        "ORDER BY message_box ASC, CASE WHEN sender IS NULL THEN 0 ELSE 1 END, sender ASC, created_at {order_dir}"
    );

    let (limit_placeholder, offset_placeholder) = if message_box.is_some() {
        ("?3", "?4")
    } else {
        ("?2", "?3")
    };

    let select_sql = format!(
        "SELECT id, recipient, sender, message_box, recipient_fee, created_at, updated_at \
         FROM message_permissions {where_clause} {order_by} LIMIT {limit_placeholder} OFFSET {offset_placeholder}"
    );

    let query_refs: Vec<&dyn rusqlite::types::ToSql> =
        query_params.iter().map(|p| p.as_ref()).collect();
    let mut stmt = conn.prepare(&select_sql)?;
    let rows = stmt.query_map(query_refs.as_slice(), |row| read_permission_row(row))?;
    let permissions: Vec<PermissionRow> = rows.collect::<Result<_, _>>()?;

    Ok((permissions, total_count))
}

// ---------------------------------------------------------------------------
// Device registrations
// ---------------------------------------------------------------------------

pub fn register_device(
    pool: &DbPool,
    identity_key: &str,
    fcm_token: &str,
    device_id: Option<&str>,
    platform: Option<&str>,
) -> Result<i64, rusqlite::Error> {
    let conn = pool.get().map_err(|e| rusqlite::Error::InvalidParameterName(format!("{e}")))?;
    conn.execute(
        "INSERT INTO device_registrations (identity_key, fcm_token, device_id, platform, last_used, active) \
         VALUES (?1, ?2, ?3, ?4, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1) \
         ON CONFLICT(fcm_token) DO UPDATE SET \
         identity_key = excluded.identity_key, \
         device_id = excluded.device_id, \
         platform = excluded.platform, \
         last_used = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), \
         active = 1, \
         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')",
        params![identity_key, fcm_token, device_id, platform],
    )?;
    Ok(conn.last_insert_rowid())
}

pub fn list_devices(
    pool: &DbPool,
    identity_key: &str,
) -> Result<Vec<DeviceRow>, rusqlite::Error> {
    let conn = pool.get().map_err(|e| rusqlite::Error::InvalidParameterName(format!("{e}")))?;
    let mut stmt = conn.prepare(
        "SELECT id, identity_key, fcm_token, device_id, platform, last_used, active, created_at, updated_at \
         FROM device_registrations WHERE identity_key = ?1 ORDER BY updated_at DESC",
    )?;
    let rows = stmt.query_map(params![identity_key], |row| read_device_row(row))?;
    rows.collect()
}

pub fn list_active_devices(
    pool: &DbPool,
    identity_key: &str,
) -> Result<Vec<DeviceRow>, rusqlite::Error> {
    let conn = pool.get().map_err(|e| rusqlite::Error::InvalidParameterName(format!("{e}")))?;
    let mut stmt = conn.prepare(
        "SELECT id, identity_key, fcm_token, device_id, platform, last_used, active, created_at, updated_at \
         FROM device_registrations WHERE identity_key = ?1 AND active = 1 ORDER BY updated_at DESC",
    )?;
    let rows = stmt.query_map(params![identity_key], |row| read_device_row(row))?;
    rows.collect()
}

fn read_device_row(row: &rusqlite::Row<'_>) -> Result<DeviceRow, rusqlite::Error> {
    Ok(DeviceRow {
        id: row.get(0)?,
        identity_key: row.get(1)?,
        fcm_token: row.get(2)?,
        device_id: row.get(3)?,
        platform: row.get(4)?,
        last_used: row.get(5)?,
        active: row.get(6)?,
        created_at: row.get(7)?,
        updated_at: row.get(8)?,
    })
}

pub fn update_device_last_used(pool: &DbPool, device_id: i64) -> Result<(), rusqlite::Error> {
    let conn = pool.get().map_err(|e| rusqlite::Error::InvalidParameterName(format!("{e}")))?;
    conn.execute(
        "UPDATE device_registrations SET last_used = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?1",
        params![device_id],
    )?;
    Ok(())
}

pub fn deactivate_device(pool: &DbPool, device_id: i64) -> Result<(), rusqlite::Error> {
    let conn = pool.get().map_err(|e| rusqlite::Error::InvalidParameterName(format!("{e}")))?;
    conn.execute(
        "UPDATE device_registrations SET active = 0, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?1",
        params![device_id],
    )?;
    Ok(())
}

pub fn should_use_fcm_delivery(message_box: &str) -> bool {
    message_box == "notifications"
}
