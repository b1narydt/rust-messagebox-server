-- Initial schema mirroring TS message-box-server final state (post all 5 Knex migrations).
-- Idempotent via IF NOT EXISTS so this migration is safe on a fresh DB or on a DB
-- previously migrated by the TS Knex chain.

CREATE TABLE IF NOT EXISTS messageBox (
  messageBoxId INT UNSIGNED NOT NULL AUTO_INCREMENT,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  type VARCHAR(255) NOT NULL,
  identityKey VARCHAR(255) NOT NULL,
  PRIMARY KEY (messageBoxId),
  UNIQUE KEY uq_messagebox_type_identity (type, identityKey)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- messages has no PRIMARY KEY: the 2024-03-05 TS migration dropped the original
-- INT AUTO_INCREMENT PK and re-added messageId as VARCHAR UNIQUE. Parity requires
-- we do not add a surrogate PK here.
CREATE TABLE IF NOT EXISTS messages (
  messageId VARCHAR(255) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  messageBoxId INT UNSIGNED,
  sender VARCHAR(255) NOT NULL,
  recipient VARCHAR(255) NOT NULL,
  body LONGTEXT NOT NULL,
  UNIQUE KEY uq_messages_messageid (messageId),
  KEY idx_messages_recipient_box_created (recipient, messageBoxId, created_at),
  KEY idx_messages_box (messageBoxId),
  CONSTRAINT fk_messages_messagebox
    FOREIGN KEY (messageBoxId) REFERENCES messageBox (messageBoxId) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS message_permissions (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  recipient VARCHAR(255) NOT NULL,
  sender VARCHAR(255) NULL,
  message_box VARCHAR(255) NOT NULL,
  recipient_fee INT NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_permissions_recipient_sender_box (recipient, sender, message_box),
  KEY idx_permissions_recipient (recipient),
  KEY idx_permissions_recipient_box (recipient, message_box),
  KEY idx_permissions_box (message_box),
  KEY idx_permissions_sender (sender)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS server_fees (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  message_box VARCHAR(255) NOT NULL,
  delivery_fee INT NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_server_fees_box (message_box)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Seed default delivery fees. INSERT IGNORE so re-running against a populated DB
-- (e.g. one already seeded by the TS migration) is a no-op.
INSERT IGNORE INTO server_fees (message_box, delivery_fee) VALUES
  ('notifications', 10),
  ('inbox', 0),
  ('payment_inbox', 0);

CREATE TABLE IF NOT EXISTS device_registrations (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  identity_key VARCHAR(255) NOT NULL,
  fcm_token VARCHAR(500) NOT NULL,
  device_id VARCHAR(255) NULL,
  platform VARCHAR(50) NULL,
  last_used TIMESTAMP NULL,
  active TINYINT(1) NOT NULL DEFAULT 1,
  PRIMARY KEY (id),
  UNIQUE KEY uq_devices_fcm_token (fcm_token),
  KEY idx_devices_identity (identity_key),
  KEY idx_devices_identity_active (identity_key, active),
  KEY idx_devices_last_used (last_used)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
