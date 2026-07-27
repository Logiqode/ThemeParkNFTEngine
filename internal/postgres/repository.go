package postgres

import (
	"context"
	"fmt"

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

func (r *Repository) RecordMint(ctx context.Context, userID int64, rideID, date, txDigest string, gasUsed int64) error {
	_, err := r.db.ExecContext(ctx, `INSERT INTO mint_logs (user_id, ride_id, mint_date, tx_digest, status, gas_used) VALUES ($1,$2,$3,$4,'CONFIRMED',$5) ON CONFLICT (user_id, ride_id, mint_date) DO UPDATE SET tx_digest=$4, status='CONFIRMED', gas_used=$5, updated_at=now()`, userID, rideID, date, txDigest, gasUsed)
	return err
}
