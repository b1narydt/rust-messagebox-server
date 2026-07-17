-- Fresh-deploy baseline (squash of the previous 4 migrations — design decision D2).
-- No prod data exists, so this is the single migration a new deployment runs.
--
-- Schema tracks the TS message-box-server final state (post all 5 Knex
-- migrations) with one deliberate divergence, per design decision D1:
-- `messages` gets a REAL PRIMARY KEY. The TS 2024-03-05 migration dropped the
-- messages PK and left only `messageId` UNIQUE — an InnoDB anti-pattern (hidden
-- row id; no stable row identity for replication/online DDL). We restore a
-- surrogate AUTO_INCREMENT PK and keep `messageId` UNIQUE as the dedup
-- constraint (`INSERT IGNORE` semantics unchanged). DB schema is not part of
-- the @bsv wire contract, so this does not affect TS parity (audit row D2†).
--
-- `device_registrations` is intentionally ABSENT here: W1 cut the broken
-- devices+FCM subsystem; the Phase-5 parity rebuild re-adds the table to the
-- exact TS DDL (parity audit §4 / row D5).

CREATE TABLE messageBox (
  messageBoxId INT UNSIGNED NOT NULL AUTO_INCREMENT,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  type VARCHAR(255) NOT NULL,
  identityKey VARCHAR(255) NOT NULL,
  PRIMARY KEY (messageBoxId),
  UNIQUE KEY uq_messagebox_type_identity (type, identityKey)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE messages (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  messageId VARCHAR(255) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  messageBoxId INT UNSIGNED,
  sender VARCHAR(255) NOT NULL,
  recipient VARCHAR(255) NOT NULL,
  body LONGTEXT NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_messages_messageid (messageId),
  KEY idx_messages_recipient_box_created (recipient, messageBoxId, created_at),
  KEY idx_messages_box (messageBoxId),
  CONSTRAINT fk_messages_messagebox
    FOREIGN KEY (messageBoxId) REFERENCES messageBox (messageBoxId) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE message_permissions (
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

CREATE TABLE server_fees (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  message_box VARCHAR(255) NOT NULL,
  delivery_fee INT NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_server_fees_box (message_box)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Delivery fees default to 0: operators opt into fees per box via the
-- MESSAGEBOX_FEES env var (upserted at boot after migrations). This carries
-- forward the pre-squash end state (the zero_default_fees migration). Note
-- the TS server seeds notifications=10; whether the out-of-box default flips
-- back to 10 for behavioral parity is a Phase-5 decision (parity audit D3
-- follow-on — "finalize at Phase-5 build time"). The smart-default RECIPIENT
-- fee for `notifications` (=10) lives in code and already matches TS.
INSERT INTO server_fees (message_box, delivery_fee) VALUES
  ('notifications', 0),
  ('inbox', 0),
  ('payment_inbox', 0);
