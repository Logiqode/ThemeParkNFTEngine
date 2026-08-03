ALTER TABLE ticket_vouchers DROP COLUMN IF EXISTS participant_id;
DROP TABLE IF EXISTS pending_mints;
DROP TABLE IF EXISTS participants;
DROP TYPE IF EXISTS wallet_state;
