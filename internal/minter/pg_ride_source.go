package minter

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/Logiqode/ThemeParkNFT/internal/models"
)

// ScanEventRideSource reads a participant's distinct rides for a date from the
// durable `scan_events` table. Account-linked participants resolve via
// users.email; dependents (no account) resolve through the voucher→participant
// link (ticket_vouchers.participant_id). Using scan_events makes this the
// M4.10 "rebuild from the true durable source" path (independent of the
// ephemeral Redis daily cache).
type ScanEventRideSource struct {
	db *sqlx.DB
}

// NewScanEventRideSource builds the durable scan_events ride source.
func NewScanEventRideSource(db *sqlx.DB) *ScanEventRideSource {
	return &ScanEventRideSource{db: db}
}

// RidesForParticipant returns distinct ride_ids + scan times for a participant
// on `date` (YYYY-MM-DD), or empty slices when the participant has none.
func (s *ScanEventRideSource) RidesForParticipant(ctx context.Context, p *models.Participant, date string) ([]string, []time.Time, error) {
	if p.AccountEmail != nil && *p.AccountEmail != "" {
		return s.byEmail(ctx, *p.AccountEmail, date)
	}
	// Dependent (no account) → rides whose ticket was delegated to this
	// participant (ticket_vouchers.participant_id).
	return s.byParticipantID(ctx, p.ID, date)
}

func (s *ScanEventRideSource) byEmail(ctx context.Context, email, date string) ([]string, []time.Time, error) {
	var rideIDs []string
	var ats []time.Time
	rows, err := s.db.QueryContext(ctx,
		`SELECT DISTINCT se.ride_id, se.scanned_at
		 FROM scan_events se
		 JOIN users u ON u.id = se.user_id
		 WHERE u.email = $1 AND se.scanned_at::date = $2::date
		 ORDER BY se.scanned_at`, email, date)
	if err != nil {
		return nil, nil, fmt.Errorf("query scan_events by email: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var ride string
		var at time.Time
		if err := rows.Scan(&ride, &at); err != nil {
			return nil, nil, fmt.Errorf("scan ride row: %w", err)
		}
		rideIDs = append(rideIDs, ride)
		ats = append(ats, at)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate scan_events: %w", err)
	}
	return rideIDs, ats, nil
}

func (s *ScanEventRideSource) byParticipantID(ctx context.Context, participantID int64, date string) ([]string, []time.Time, error) {
	var rideIDs []string
	var ats []time.Time
	rows, err := s.db.QueryContext(ctx,
		`SELECT DISTINCT se.ride_id, se.scanned_at
		 FROM scan_events se
		 JOIN ticket_vouchers tv ON tv.voucher_id = se.ticket_id AND tv.participant_id = $1
		 WHERE se.scanned_at::date = $2::date
		 ORDER BY se.scanned_at`, participantID, date)
	if err != nil {
		return nil, nil, fmt.Errorf("query scan_events by participant: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var ride string
		var at time.Time
		if err := rows.Scan(&ride, &at); err != nil {
			return nil, nil, fmt.Errorf("scan ride row: %w", err)
		}
		rideIDs = append(rideIDs, ride)
		ats = append(ats, at)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate scan_events: %w", err)
	}
	return rideIDs, ats, nil
}
