-- Schema v4 — Week 6 mint attribution (W6-A).
-- Adds participant_id to mint_logs so a dependent's NFT minted into the
-- guardian custodial wallet is attributed to the dependent participant
-- (Rev 3 R31/R32), while user_id remains the guardian/account that triggered
-- the mint. Kept NULL for legacy mint_logs rows.
ALTER TABLE mint_logs ADD COLUMN IF NOT EXISTS participant_id BIGINT REFERENCES participants(id);
CREATE INDEX IF NOT EXISTS idx_mint_logs_participant_date ON mint_logs(participant_id, mint_date);
