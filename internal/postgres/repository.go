package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/Logiqode/ThemeParkNFT/internal/models"
)

type Repository struct {
	db *sqlx.DB
}

func NewRepository(db *sqlx.DB) *Repository { return &Repository{db: db} }

func (r *Repository) CreateUser(ctx context.Context, email string) (*models.User, error) {
	var u models.User
	err := r.db.GetContext(ctx, &u, `INSERT INTO users (email) VALUES ($1) ON CONFLICT (email) DO UPDATE SET updated_at=now() RETURNING *`, email)
	if err != nil {
		return nil, fmt.Errorf("create user: %w", err)
	}
	return &u, nil
}

func (r *Repository) GetUserByEmail(ctx context.Context, email string) (*models.User, error) {
	var u models.User
	err := r.db.GetContext(ctx, &u, `SELECT * FROM users WHERE email=$1`, email)
	return &u, err
}

func (r *Repository) UpdateSuiAccount(ctx context.Context, userID int64, suiAddress, encKey, encProof string) error {
	_, err := r.db.ExecContext(ctx, `UPDATE users SET sui_address=$1, ephemeral_key=$2, zklogin_proof=$3, updated_at=now() WHERE id=$4`, suiAddress, encKey, encProof, userID)
	return err
}

func (r *Repository) CreateTicket(ctx context.Context, ticketID string, userID *int64, purchaserID *int64) error {
	_, err := r.db.ExecContext(ctx, `INSERT INTO tickets (ticket_id, user_id, status, purchased_by) VALUES ($1,$2,$3,$4)`, ticketID, userID, models.StatusActive, purchaserID)
	return err
}

func (r *Repository) CreateVouchers(ctx context.Context, purchaserID int64, quantity int) ([]string, error) {
	ids := make([]string, 0, quantity)
	for i := 0; i < quantity; i++ {
		vid := fmt.Sprintf("voucher-%d-%d", purchaserID, i)
		_, err := r.db.ExecContext(ctx, `INSERT INTO ticket_vouchers (voucher_id, purchaser_id, status) VALUES ($1,$2,$3)`, vid, purchaserID, models.StatusUnclaimed)
		if err != nil {
			return ids, fmt.Errorf("create voucher %s: %w", vid, err)
		}
		ids = append(ids, vid)
	}
	return ids, nil
}

func (r *Repository) ClaimVoucher(ctx context.Context, voucherID string, ownerID int64) error {
	res, err := r.db.ExecContext(ctx, `UPDATE ticket_vouchers SET owner_id=$1, status=$2, updated_at=now() WHERE voucher_id=$3 AND status='UNCLAIMED'`, ownerID, models.StatusClaimed, voucherID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("voucher %s not found or already claimed", voucherID)
	}
	return nil
}

// RecordMint writes a CONFIRMED mint_logs row, idempotent on
// (user_id, ride_id, mint_date). participantID is optional (W6-A): it attributes
// the mint to a participant (e.g. a dependent minted into the guardian wallet).
// The UNIQUE constraint is on (user_id, ride_id, mint_date); re-running the same
// mint returns the existing digest (M6.3 idempotency).
func (r *Repository) RecordMint(ctx context.Context, userID int64, participantID *int64, rideID, date, txDigest string, gasUsed int64) error {
	_, err := r.db.ExecContext(ctx, `INSERT INTO mint_logs (user_id, participant_id, ride_id, mint_date, tx_digest, status, gas_used) VALUES ($1,$2,$3,$4,$5,'CONFIRMED',$6) ON CONFLICT (user_id, ride_id, mint_date) DO UPDATE SET tx_digest=$5, status='CONFIRMED', gas_used=$6, updated_at=now()`, userID, participantID, rideID, date, txDigest, gasUsed)
	return err
}

// RecordMintForParticipant records a CONFIRMED mint_logs row attributed to a
// participant (W6-A): it resolves the owning account user (guardian walk-up for
// dependents) and writes the row with participant_id set, idempotent on
// (user_id, ride_id, mint_date). mint_logs.user_id is NOT NULL FK → every mint
// must be attributable to an owning account (in this model always the guardian
// who purchased the family's tickets), so a missing owner is an error, not a
// silent zero.
func (r *Repository) RecordMintForParticipant(ctx context.Context, participantID int64, rideID, date, txDigest string) error {
	ownerID, err := r.OwnerUserIDForParticipant(ctx, participantID)
	if err != nil {
		return err
	}
	if ownerID == 0 {
		return fmt.Errorf("record mint: no owning account user for participant %d", participantID)
	}
	ptr := &participantID
	return r.RecordMint(ctx, ownerID, ptr, rideID, date, txDigest, 0)
}

// ---- Rev 3: participant & durable attribution ledger (R26-R35) ----

// CreateParticipant inserts a participant. For account-linked participants pass
// an accountEmail; for dependents pass a guardianID (and the pre-provisioned
// custodial wallet address is set afterwards via SetParticipantWallet).
func (r *Repository) CreateParticipant(ctx context.Context, name string, accountEmail *string, guardianID *int64) (*models.Participant, error) {
	var p models.Participant
	err := r.db.GetContext(ctx, &p,
		`INSERT INTO participants (name, account_email, guardian_id)
		 VALUES ($1, $2, $3) RETURNING *`, name, accountEmail, guardianID)
	if err != nil {
		return nil, fmt.Errorf("create participant: %w", err)
	}
	return &p, nil
}

// GetParticipant returns a participant by id, or (nil, nil) if not found.
func (r *Repository) GetParticipant(ctx context.Context, id int64) (*models.Participant, error) {
	var p models.Participant
	err := r.db.GetContext(ctx, &p, `SELECT * FROM participants WHERE id=$1`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get participant: %w", err)
	}
	return &p, nil
}

// GetParticipantByEmail finds the participant linked to an account email, or
// (nil, nil) if none exists yet (R28 account mode).
func (r *Repository) GetParticipantByEmail(ctx context.Context, email string) (*models.Participant, error) {
	var p models.Participant
	err := r.db.GetContext(ctx, &p, `SELECT * FROM participants WHERE account_email=$1`, email)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get participant by email: %w", err)
	}
	return &p, nil
}

// ListParticipants returns every participant (used by the off-chain end-of-day
// mint-resolution driver, M4.12).
func (r *Repository) ListParticipants(ctx context.Context) ([]models.Participant, error) {
	var ps []models.Participant
	if err := r.db.SelectContext(ctx, &ps, `SELECT * FROM participants ORDER BY id`); err != nil {
		return nil, fmt.Errorf("list participants: %w", err)
	}
	return ps, nil
}

// SetParticipantWallet updates a participant's wallet state + address (R30:
// own wallet attached later, or dependent custodial wallet provisioned).
func (r *Repository) SetParticipantWallet(ctx context.Context, id int64, state models.ParticipantWalletState, addr string) error {
	_, err := r.db.ExecContext(ctx, `UPDATE participants SET wallet_state=$1, custodial_wallet_address=$2, updated_at=now() WHERE id=$3`, state, addr, id)
	if err != nil {
		return fmt.Errorf("set participant wallet: %w", err)
	}
	return nil
}

// OwnerUserIDForParticipant resolves the users.id of the account that owns a
// participant (for mint_logs attribution, W6-A). For an account-linked
// participant this is the user with participant.account_email; for a dependent
// it walks up the guardian chain to the family head's account_email, then maps
// that email to users.id. Returns (0, nil) if no owning user exists yet (a
// dependent whose guardian has no account — mint not yet attributable).
func (r *Repository) OwnerUserIDForParticipant(ctx context.Context, participantID int64) (int64, error) {
	// Recursive CTE walks the guardian self-ref up to an account-bearing head.
	var userID sql.NullInt64
	err := r.db.QueryRowContext(ctx, `
		WITH RECURSIVE chain AS (
			SELECT id, account_email, guardian_id FROM participants WHERE id=$1
			UNION ALL
			SELECT p.id, p.account_email, p.guardian_id FROM participants p
			JOIN chain c ON p.id = c.guardian_id
		)
		SELECT u.id FROM chain c JOIN users u ON u.email = c.account_email
		WHERE c.account_email IS NOT NULL LIMIT 1`, participantID).Scan(&userID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("resolve owner user for participant %d: %w", participantID, err)
	}
	if !userID.Valid {
		return 0, nil
	}
	return userID.Int64, nil
}

// DelegateVoucher allocates a voucher to a participant (R27/R28) by linking
// participant_id. Fails if the voucher is not still UNCLAIMED (already
// delegated/claimed).
func (r *Repository) DelegateVoucher(ctx context.Context, voucherID string, participantID int64) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE ticket_vouchers SET participant_id=$1, updated_at=now()
		 WHERE voucher_id=$2 AND status='UNCLAIMED' AND participant_id IS NULL`, participantID, voucherID)
	if err != nil {
		return fmt.Errorf("delegate voucher: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("voucher %s: %w", voucherID, models.ErrAlreadyAllocated)
	}
	return nil
}

// UpsertPendingMint writes (or replaces) the durable attribution-ledger row for
// a participant + mint date (R32). Idempotent on (participant_id, mint_date).
// ride_ids and scanned_ats are persisted as JSONB (durable, RFC3339 timestamps).
func (r *Repository) UpsertPendingMint(ctx context.Context, pm models.PendingMint) error {
	if pm.RideIDs == nil {
		pm.RideIDs = []string{}
	}
	if pm.ScannedAts == nil {
		pm.ScannedAts = []time.Time{}
	}
	rides, err := json.Marshal(pm.RideIDs)
	if err != nil {
		return fmt.Errorf("marshal ride_ids: %w", err)
	}
	ats, err := json.Marshal(pm.ScannedAts)
	if err != nil {
		return fmt.Errorf("marshal scanned_ats: %w", err)
	}
	_, err = r.db.ExecContext(ctx,
		`INSERT INTO pending_mints (participant_id, ride_ids, scanned_ats, mint_date, wallet_state)
		 VALUES ($1, $2::jsonb, $3::jsonb, $4, $5)
		 ON CONFLICT (participant_id, mint_date) DO UPDATE
		   SET ride_ids=EXCLUDED.ride_ids, scanned_ats=EXCLUDED.scanned_ats,
		       wallet_state=EXCLUDED.wallet_state`,
		pm.ParticipantID, string(rides), string(ats), pm.MintDate, string(pm.WalletState))
	if err != nil {
		return fmt.Errorf("upsert pending mint: %w", err)
	}
	return nil
}

// GetPendingMint returns the durable attribution row for a participant on a
// date, or (nil, nil) if none. Used to prove M4.10 (row outlives Redis/wristband).
func (r *Repository) GetPendingMint(ctx context.Context, participantID int64, mintDate string) (*models.PendingMint, error) {
	var (
		id          int64
		rideRaw     []byte
		atsRaw      []byte
		mdate       string
		walletState string
		createdAt   time.Time
	)
	err := r.db.QueryRowContext(ctx,
		`SELECT id, ride_ids, scanned_ats, mint_date::text, wallet_state, created_at
		 FROM pending_mints WHERE participant_id=$1 AND mint_date=$2`,
		participantID, mintDate).
		Scan(&id, &rideRaw, &atsRaw, &mdate, &walletState, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get pending mint: %w", err)
	}
	// ride_ids is JSONB → raw []byte; decode into []string.
	var rides []string
	if len(rideRaw) > 0 {
		if err := json.Unmarshal(rideRaw, &rides); err != nil {
			return nil, fmt.Errorf("unmarshal ride_ids: %w", err)
		}
	}
	// scanned_ats is JSONB (RFC3339 timestamps) → decode into []time.Time.
	var ats []time.Time
	if len(atsRaw) > 0 {
		if err := json.Unmarshal(atsRaw, &ats); err != nil {
			return nil, fmt.Errorf("unmarshal scanned_ats: %w", err)
		}
	}
	if rides == nil {
		rides = []string{}
	}
	if ats == nil {
		ats = []time.Time{}
	}
	return &models.PendingMint{
		ID:            id,
		ParticipantID: participantID,
		RideIDs:       rides,
		ScannedAts:    ats,
		MintDate:      mdate,
		WalletState:   models.ParticipantWalletState(walletState),
		CreatedAt:     createdAt,
	}, nil
}

// ---- Outbox (M4.3): parked Redis-aggregation intents ----

// InsertOutbox parks a failed Redis SADD so it can be replayed later (idempotent
// on trace_id — a re-processed event never double-parks). Used when PG insert
// succeeded but Redis aggregation failed, guaranteeing no loss.
func (r *Repository) InsertOutbox(ctx context.Context, traceID, userEmail, rideID string, scannedAt time.Time) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO scan_events_outbox (trace_id, user_email, ride_id, scanned_at)
		 VALUES ($1,$2,$3,$4) ON CONFLICT (trace_id) DO NOTHING`,
		traceID, userEmail, rideID, scannedAt)
	if err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}
	return nil
}

// ListPendingOutbox returns up to `limit` PENDING outbox rows oldest-first.
func (r *Repository) ListPendingOutbox(ctx context.Context, limit int) ([]models.OutboxRow, error) {
	var rows []models.OutboxRow
	if err := r.db.SelectContext(ctx, &rows,
		`SELECT * FROM scan_events_outbox WHERE status='PENDING' ORDER BY created_at LIMIT $1`, limit); err != nil {
		return nil, fmt.Errorf("list pending outbox: %w", err)
	}
	return rows, nil
}

// BumpOutboxAttempts increments the retry counter for a parked row.
func (r *Repository) BumpOutboxAttempts(ctx context.Context, traceID string) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE scan_events_outbox SET attempts=attempts+1, updated_at=now() WHERE trace_id=$1`, traceID)
	if err != nil {
		return fmt.Errorf("bump outbox attempts: %w", err)
	}
	return nil
}

// DeleteOutbox removes a parked row once its Redis SADD has succeeded.
func (r *Repository) DeleteOutbox(ctx context.Context, traceID string) error {
	if _, err := r.db.ExecContext(ctx, `DELETE FROM scan_events_outbox WHERE trace_id=$1`, traceID); err != nil {
		return fmt.Errorf("delete outbox: %w", err)
	}
	return nil
}
