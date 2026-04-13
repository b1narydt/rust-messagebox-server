-- All delivery fees default to 0. Operators set non-zero fees per
-- messageBox via the MESSAGEBOX_FEES env var, which is upserted into
-- this table at boot time after migrations run.
--
-- This deliberately overwrites the seeded `notifications=10` and the
-- prior `chat=10` from the Phase 1A migration. Anyone wanting those
-- fees back can set MESSAGEBOX_FEES=notifications=10,chat=10 (or any
-- subset) on their deployment.
UPDATE server_fees SET delivery_fee = 0;
