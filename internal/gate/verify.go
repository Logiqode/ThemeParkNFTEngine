package gate

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/Logiqode/ThemeParkNFT/internal/models"
)

type Verifier struct {
	db *sqlx.DB
}

func NewVerifier(db *sqlx.DB) *Verifier {
	return &Verifier{db: db}
}

// VerifyTicket performs an atomic ticket verification using SELECT FOR UPDATE.
// State machine: ACTIVE → PENDING_ENTRY → USED (or back to ACTIVE on failure)
func (v *Verifier) VerifyTicket(ctx context.Context, ticketID string) (*models.GateVerifyResponse, error) {
	tx, err := v.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback() // safe to call even after commit

	// SELECT ... FOR UPDATE locks the row for the duration of the transaction.
	var ticket models.Ticket
	err = tx.GetContext(ctx, &ticket,
		`SELECT id, ticket_id, user_id, status FROM tickets WHERE ticket_id = $1 FOR UPDATE`, ticketID)
	if err != nil {
		return &models.GateVerifyResponse{Allowed: false, Reason: "ticket not found"}, nil
	}

	if ticket.Status == models.StatusUsed {
		return &models.GateVerifyResponse{Allowed: false, Reason: "ticket already used"}, nil
	}
	if ticket.Status == models.StatusExpired {
		return &models.GateVerifyResponse{Allowed: false, Reason: "ticket expired"}, nil
	}

	// Transition to PENDING_ENTRY
	_, err = tx.ExecContext(ctx,
		`UPDATE tickets SET status = $1, updated_at = $2 WHERE ticket_id = $3`,
		models.StatusPendingEntry, time.Now(), ticketID)
	if err != nil {
		return &models.GateVerifyResponse{Allowed: false, Reason: "failed to update ticket"}, err
	}

	// Commit marks the ticket as PENDING_ENTRY
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	userID := "unknown"
	if ticket.UserID != nil {
		userID = fmt.Sprintf("%d", *ticket.UserID)
	}

	return &models.GateVerifyResponse{
		Allowed: true,
		UserID:  userID,
	}, nil
}

// ConfirmEntry completes the turnstile entry, marking ticket as USED.
func (v *Verifier) ConfirmEntry(ctx context.Context, ticketID string) error {
	_, err := v.db.ExecContext(ctx,
		`UPDATE tickets SET status = $1, updated_at = $2 WHERE ticket_id = $3`,
		models.StatusUsed, time.Now(), ticketID)
	return err
}

// RejectEntry reverts PENDING_ENTRY back to ACTIVE (e.g., turnstile denied).
func (v *Verifier) RejectEntry(ctx context.Context, ticketID string) error {
	_, err := v.db.ExecContext(ctx,
		`UPDATE tickets SET status = $1, updated_at = $2 WHERE ticket_id = $3`,
		models.StatusActive, time.Now(), ticketID)
	return err
}
