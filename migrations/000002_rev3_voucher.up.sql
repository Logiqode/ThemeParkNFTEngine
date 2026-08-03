-- Schema v2 — Rev 3 Family Voucher & Participant Model (R26-R35)
-- Adds participants + pending_mints (durable attribution ledger) and links
-- ticket_vouchers to the participant they are delegated to.

DO $$ BEGIN CREATE TYPE wallet_state AS ENUM ('NONE','OWN_NON_CUSTODIAL','CUSTODIAL_PROXY'); EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- participants: a person (R26), distinct from a login/wallet. Dependents (no
-- account) carry a guardian_id (self-reference to the family head) and a
-- custodial-proxy wallet (R35 - a dedicated family/guardian Sui address, keys
-- server-side/encrypted, never commingled with the guardian's own wallet).
CREATE TABLE IF NOT EXISTS participants (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    account_email VARCHAR(255),
    guardian_id BIGINT REFERENCES participants(id),
    wallet_state wallet_state NOT NULL DEFAULT 'NONE',
    custodial_wallet_address VARCHAR(128),
    keys_enc TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_participants_guardian ON participants(guardian_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_participants_account_email ON participants(account_email) WHERE account_email IS NOT NULL;

-- pending_mints: durable attribution ledger (R32), NOT a cache. Keyed to a
-- participant, durably retains (ride_ids, scanned_ats, mint_date). Never tied
-- to the ephemeral Redis 48h cache or the disposable wristband (end of Day+1);
-- rebuildable at any time from scan_events (the true durable source) and kept
-- by default (deletable on request, GDPR/R34).
CREATE TABLE IF NOT EXISTS pending_mints (
    id BIGSERIAL PRIMARY KEY,
    participant_id BIGINT NOT NULL REFERENCES participants(id),
    ride_ids JSONB NOT NULL,
    scanned_ats JSONB NOT NULL,
    mint_date DATE NOT NULL,
    wallet_state wallet_state NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (participant_id, mint_date)
);
CREATE INDEX IF NOT EXISTS idx_pending_mints_participant ON pending_mints(participant_id);

-- Link each voucher to the participant it is delegated to (R27/R28). UNCLAIMED
-- vouchers have a NULL participant until the buyer allocates them (account or
-- dependent mode).
ALTER TABLE ticket_vouchers ADD COLUMN IF NOT EXISTS participant_id BIGINT REFERENCES participants(id);
