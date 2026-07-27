-- Forward migration replacing the squashed 20260717000000_baseline_schema.sql.
--
-- The squash (D1/D2) collapsed the original 4-migration chain into one baseline
-- and, as a side effect, made in-place upgrades impossible: sqlx keys
-- _sqlx_migrations by (version, checksum), so deleting the 4 original files left
-- any already-migrated DB with 4 applied versions that no longer exist in the
-- source -> Migrator::run returns VersionMissing -> boot crash loop. The TS
-- (knex) and Go (idempotent DDL) references both upgrade in place; this restores
-- that property by keeping the 4 originals byte-identical (they validate as
-- already-applied no-ops) and folding the only two schema deltas the baseline
-- introduced into this single additive migration:
--
--   1. messages gets the surrogate BIGINT AUTO_INCREMENT primary key (D1). The
--      original chain left messages with no PK (a TS artifact: the 2024-03-05
--      migration dropped the PK and kept only messageId UNIQUE — an InnoDB
--      anti-pattern). messageId stays UNIQUE, so INSERT IGNORE dedup is
--      unchanged; not wire-visible (audit D2-dagger).
--   2. The PeerPay experiment fee rows (chat, payment_requests) are removed so
--      the seed matches the TS/Go reference set exactly: notifications, inbox,
--      payment_inbox. Both default to 0 and boxes auto-create on first send, so
--      this is cosmetic convergence, not a behavior change.
--
-- Fresh deploys run the full chain and land here; existing deploys run only this
-- migration. Both converge to the same schema.

ALTER TABLE messages
  ADD COLUMN id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT FIRST,
  ADD PRIMARY KEY (id);

DELETE FROM server_fees WHERE message_box IN ('chat', 'payment_requests');
