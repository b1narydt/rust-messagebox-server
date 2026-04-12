pub mod queries;

#[cfg(test)]
mod tests;

pub type DbPool = r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>;

pub fn new(db_source: &str) -> Result<DbPool, Box<dyn std::error::Error>> {
    let manager = r2d2_sqlite::SqliteConnectionManager::file(db_source)
        .with_init(|conn| {
            conn.execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;",
            )?;
            Ok(())
        });
    let pool = r2d2::Pool::builder().max_size(8).build(manager)?;
    Ok(pool)
}

pub fn migrate(pool: &DbPool) -> Result<(), Box<dyn std::error::Error>> {
    let conn = pool.get()?;

    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS messageBox (
            messageBoxId INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            updated_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            type TEXT NOT NULL,
            identityKey TEXT NOT NULL,
            UNIQUE(type, identityKey)
        );

        CREATE TABLE IF NOT EXISTS messages (
            messageId TEXT UNIQUE NOT NULL,
            created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            updated_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            messageBoxId INTEGER REFERENCES messageBox(messageBoxId) ON DELETE CASCADE,
            sender TEXT NOT NULL,
            recipient TEXT NOT NULL,
            body TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS message_permissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            updated_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            recipient TEXT NOT NULL,
            sender TEXT,
            message_box TEXT NOT NULL,
            recipient_fee INTEGER NOT NULL,
            UNIQUE(recipient, sender, message_box)
        );

        CREATE INDEX IF NOT EXISTS idx_permissions_recipient
            ON message_permissions(recipient);
        CREATE INDEX IF NOT EXISTS idx_permissions_recipient_box
            ON message_permissions(recipient, message_box);
        CREATE INDEX IF NOT EXISTS idx_permissions_box
            ON message_permissions(message_box);
        CREATE INDEX IF NOT EXISTS idx_permissions_sender
            ON message_permissions(sender);

        CREATE TABLE IF NOT EXISTS server_fees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            updated_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            message_box TEXT NOT NULL UNIQUE,
            delivery_fee INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS device_registrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            updated_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            identity_key TEXT NOT NULL,
            fcm_token TEXT NOT NULL UNIQUE,
            device_id TEXT,
            platform TEXT,
            last_used TEXT,
            active BOOLEAN DEFAULT 1
        );

        CREATE INDEX IF NOT EXISTS idx_devices_identity_key
            ON device_registrations(identity_key);
        CREATE INDEX IF NOT EXISTS idx_devices_identity_active
            ON device_registrations(identity_key, active);

        CREATE INDEX IF NOT EXISTS idx_messages_recipient_box
            ON messages(recipient, messageBoxId, created_at);
        CREATE INDEX IF NOT EXISTS idx_messages_box_id
            ON messages(messageBoxId);

        CREATE INDEX IF NOT EXISTS idx_messagebox_identity_type
            ON messageBox(identityKey, type);
        CREATE INDEX IF NOT EXISTS idx_devices_identity_active_updated
            ON device_registrations(identity_key, active, updated_at DESC);
        ",
    )?;

    // Insert default server fees if they don't already exist.
    conn.execute(
        "INSERT OR IGNORE INTO server_fees (message_box, delivery_fee) VALUES (?1, ?2)",
        rusqlite::params!["notifications", 10],
    )?;
    conn.execute(
        "INSERT OR IGNORE INTO server_fees (message_box, delivery_fee) VALUES (?1, ?2)",
        rusqlite::params!["inbox", 0],
    )?;
    conn.execute(
        "INSERT OR IGNORE INTO server_fees (message_box, delivery_fee) VALUES (?1, ?2)",
        rusqlite::params!["payment_inbox", 0],
    )?;

    Ok(())
}
