// Package gate implements the wristband binding state machine (Rev 1 / R9-R25).
//
// This replaces the old "turnstile" verifier with the business-aligned model:
//
//	staff scans the visitor's account QR  → bind wristband NFC id → ticket BINDING
//	staff NFC-scans the wristband        → transaction check      → ticket BOUND
//	admin on faulty NFC                  → reset (unbind/re-bind) → ticket CLAIMED
//
// The binding itself is ephemeral and lives in Redis (R19) — never in Postgres
// long-term — and is keyed by wristband with a reverse index by ticket_id. All
// binding writes are atomic (Lua double-SETNX) so concurrent binds of the same
// wristband or ticket can never double-win (M3.5). The NFC transaction check is
// performed through the mockable auth.TxnCheckPerformer interface (R16) so
// benchmarks and CI never spam the Sui testnet.
package gate

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/Logiqode/ThemeParkNFT/internal/auth"
	"github.com/Logiqode/ThemeParkNFT/internal/config"
	"github.com/Logiqode/ThemeParkNFT/internal/models"
	redisClient "github.com/Logiqode/ThemeParkNFT/internal/redis"
)

// BindRequest is the payload for POST /api/wristband/bind (R18, R21). The QR
// fields are the visitor's scanned account QR; the wristband_uid is the NFC id
// read by the staff scanner.
type BindRequest struct {
	TicketID     string `json:"ticket_id" validate:"required"`
	QRUUID       string `json:"qr_uuid" validate:"required"`
	QRTimestamp  int64  `json:"qr_timestamp" validate:"required"`
	QRSignature  string `json:"qr_signature" validate:"required"`
	WristbandUID string `json:"wristband_uid" validate:"required"`
}

// NFCCheckRequest is the payload for POST /api/wristband/nfc-check.
type NFCCheckRequest struct {
	WristbandUID string `json:"wristband_uid" validate:"required"`
}

// ResetRequest is the payload for POST /api/wristband/reset (faulty-NFC admin override).
type ResetRequest struct {
	WristbandUID string `json:"wristband_uid" validate:"required"`
}

// WristbandResponse is the output of the gate wristband endpoints. UserEmail is
// internal-only (R11) and is never written to the blockchain or IPFS.
type WristbandResponse struct {
	WristbandUID string `json:"wristband_uid"`
	TicketID     string `json:"ticket_id"`
	Status       string `json:"status"`
	UserEmail    string `json:"user_email,omitempty"`
	Allowed      *bool  `json:"allowed,omitempty"`
	Reason       string `json:"reason,omitempty"`
	Source       string `json:"source,omitempty"` // "grace_window" on faithful replay
}

// nfcDecision is the structured value cached in the wristband grace window so a
// cached decision is replayed faithfully (R24) — a cached "denied" is NEVER
// replayed as "allowed" (this is the D1 security fix).
type nfcDecision struct {
	Allowed bool   `json:"allowed"`
	Reason  string `json:"reason"`
}

// Sentinel errors returned by BindingService so HTTP handlers can map them onto
// appropriate status codes (400 vs 409 vs 404 vs 500).
var (
	ErrQRInvalid     = errors.New("invalid or expired QR code")
	ErrQRReplay      = errors.New("QR code already used")
	ErrTicketInvalid = errors.New("ticket not found or not bindable")
	ErrNoBinding     = errors.New("wristband not bound")
	ErrNotBound      = errors.New("wristband not BOUND (active)")
	ErrAlreadyBound  = errors.New("wristband or ticket already bound")
)

// BindingService coordinates the wristband binding lifecycle across Redis (the
// source of truth for the ephemeral link, R19) and Postgres (ticket ownership
// and status reflection, R13).
type BindingService struct {
	db    *sqlx.DB
	redis *redisClient.Client
	cfg   config.GateConfig
	perf  auth.TxnCheckPerformer
}

// NewBindingService builds the gate binding service.
func NewBindingService(db *sqlx.DB, r *redisClient.Client, cfg config.GateConfig, perf auth.TxnCheckPerformer) *BindingService {
	return &BindingService{db: db, redis: r, cfg: cfg, perf: perf}
}

// Bind performs the first staff scan: verify the visitor's account QR (bound to
// the ticket, R18), enforce one-time use (R21), check the ticket is bindable in
// Postgres, and create the ephemeral wristband↔ticket link in Redis (R19).
func (s *BindingService) Bind(ctx context.Context, req BindRequest) (*WristbandResponse, error) {
	// 1. Verify the QR signature and 30s rotation window, with the ticket_id
	//    bound inside the HMAC (R18) — a token cannot be re-targeted.
	token := &QRToken{
		TicketID:  req.TicketID,
		UUID:      req.QRUUID,
		Timestamp: req.QRTimestamp,
		Signature: req.QRSignature,
	}
	if ok, err := VerifyQROTP(s.cfg, token); err != nil || !ok {
		return nil, ErrQRInvalid
	}

	// 2. Resolve the ticket owner email from Postgres (R19 consults PG only for
	//    ownership/status). A valid-but-unbindable ticket is rejected before we
	//    consume the one-time QR.
	userEmail, err := s.ticketOwnerEmail(ctx, req.TicketID)
	if err != nil {
		return nil, err
	}

	// 3. One-time use (R21): consume the QR; a replay (even inside the 30s
	//    window) is rejected.
	first, err := s.redis.MarkQRUsed(ctx, req.QRUUID)
	if err != nil {
		return nil, fmt.Errorf("mark qr used: %w", err)
	}
	if !first {
		return nil, ErrQRReplay
	}

	// 4. Create the ephemeral binding (atomic double-SETNX, R19).
	if err := s.redis.BindWristband(ctx, req.WristbandUID, req.TicketID, userEmail); err != nil {
		if errors.Is(err, redisClient.ErrWristbandAlreadyBound) || errors.Is(err, redisClient.ErrTicketAlreadyBound) {
			return nil, ErrAlreadyBound
		}
		return nil, fmt.Errorf("bind wristband: %w", err)
	}

	// 5. Reflect the Rev 1 state machine in PG (R13): ticket → PENDING_ENTRY.
	_ = s.setTicketStatus(ctx, req.TicketID, models.StatusPendingEntry) // best-effort

	return &WristbandResponse{
		WristbandUID: req.WristbandUID,
		TicketID:     req.TicketID,
		Status:       string(models.BindingStatusBinding),
		UserEmail:    userEmail,
	}, nil
}

// NFCCheck performs the second staff scan: run the transaction check (R16) and,
// on success, promote the binding BINDING → BOUND and the ticket → ACTIVE.
// A repeated scan within the grace window faithfully replays the cached decision
// (R24) keyed by wristband — NOT the original always-allowed bug (D1).
func (s *BindingService) NFCCheck(ctx context.Context, req NFCCheckRequest) (*WristbandResponse, error) {
	// Faithful grace-window replay (R24): keyed by wristband, preserves the
	// cached decision exactly.
	if cached, err := s.redis.GetWristbandGrace(ctx, req.WristbandUID); err != nil {
		return nil, fmt.Errorf("grace lookup: %w", err)
	} else if cached != "" {
		var d nfcDecision
		if err := json.Unmarshal([]byte(cached), &d); err == nil {
			return &WristbandResponse{
				WristbandUID: req.WristbandUID,
				Allowed:      &d.Allowed,
				Reason:       d.Reason,
				Source:       "grace_window",
			}, nil
		}
		// A corrupt cache entry is ignored and re-evaluated below.
	}

	b, err := s.redis.GetBinding(ctx, req.WristbandUID)
	if err != nil {
		return nil, fmt.Errorf("get binding: %w", err)
	}
	if b == nil {
		return nil, ErrNoBinding
	}

	// Run the transaction check. accountRef is the internal email key (R11) —
	// pseudonymous, never written on-chain.
	allowed := true
	reason := ""
	if err := s.perf.CheckTxnCapability(ctx, b.UserEmail); err != nil {
		allowed = false
		reason = err.Error()
	}

	status := models.BindingStatusBinding
	if allowed {
		if err := s.redis.SetBindingStatus(ctx, b.WristbandUID, models.BindingStatusBound); err != nil {
			return nil, fmt.Errorf("promote binding: %w", err)
		}
		status = models.BindingStatusBound
		_ = s.setTicketStatus(ctx, b.TicketID, models.StatusActive) // best-effort (R13)
	}

	// Cache the decision for a faithful grace replay (R24 / D1).
	if raw, err := json.Marshal(nfcDecision{Allowed: allowed, Reason: reason}); err == nil {
		_ = s.redis.SetWristbandGrace(ctx, req.WristbandUID, string(raw)) // best-effort
	}

	return &WristbandResponse{
		WristbandUID: b.WristbandUID,
		TicketID:     b.TicketID,
		Status:       string(status),
		Allowed:      &allowed,
		Reason:       reason,
	}, nil
}

// Reset performs the admin undo/overwrite on faulty NFC (R9/R13): unbind the
// wristband, remove the reverse index, drop any cached grace decision, and push
// the ticket back to CLAIMED so a fresh wristband can be bound.
func (s *BindingService) Reset(ctx context.Context, req ResetRequest) (*WristbandResponse, error) {
	b, err := s.redis.GetBinding(ctx, req.WristbandUID)
	if err != nil {
		return nil, fmt.Errorf("get binding: %w", err)
	}
	if b == nil {
		return nil, ErrNoBinding
	}

	if err := s.redis.DeleteBinding(ctx, req.WristbandUID); err != nil {
		return nil, fmt.Errorf("delete binding: %w", err)
	}
	_ = s.redis.ClearWristbandGrace(ctx, req.WristbandUID)       // stale decision must not replay
	_ = s.setTicketStatus(ctx, b.TicketID, models.StatusClaimed) // re-bindable (R13)

	return &WristbandResponse{
		WristbandUID: b.WristbandUID,
		TicketID:     b.TicketID,
		Status:       string(models.StatusClaimed),
	}, nil
}

// ticketOwnerEmail resolves the ticket's owning user email, rejecting tickets
// that are already USED/EXPIRED (R13). Returns ErrTicketInvalid if missing.
func (s *BindingService) ticketOwnerEmail(ctx context.Context, ticketID string) (string, error) {
	var email string
	err := s.db.GetContext(ctx, &email,
		`SELECT u.email FROM tickets t
		   JOIN users u ON u.id = t.user_id
		  WHERE t.ticket_id = $1
		    AND t.status NOT IN ('USED','EXPIRED')`, ticketID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrTicketInvalid
		}
		return "", fmt.Errorf("resolve ticket owner: %w", err)
	}
	return email, nil
}

// setTicketStatus reflects the binding lifecycle back onto the Postgres ticket
// state machine (R13). Used best-effort; Redis remains the source of truth.
func (s *BindingService) setTicketStatus(ctx context.Context, ticketID string, status models.TicketStatus) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE tickets SET status = $1, updated_at = now() WHERE ticket_id = $2`, status, ticketID)
	return err
}
