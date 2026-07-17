-- Phase-5 parity rebuild (parity audit §4 / rows D5, H13, H14, H9): re-add the
-- device_registrations table with the EXACT TS DDL. The Phase-1/4 baseline
-- squash deliberately dropped the previous (broken-subsystem) table; this
-- migration restores it for the rebuilt notification/device system.
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
