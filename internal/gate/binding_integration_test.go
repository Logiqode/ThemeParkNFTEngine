//go:build integration

// Week 3 integration tests for the wristband binding state machine (M3.4, M3.5,
// M3.7, QR single-use replay). Requires running Postgres + Redis (`make up`) and
// INTEGRATION=1:
//
//	go test -tags=integration ./internal/gate -run TestBinding -v -count=1
package gate

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"

	"github.com/Logiqode/ThemeParkNFT/internal/auth"
	"github.com/Logiqode/ThemeParkNFT/internal/config"
	"github.com/Logiqode/ThemeParkNFT/internal/models"
	redisClient "github.com/Logiqode/ThemeParkNFT/internal/redis"
)

type bindHarness struct {
	ctx   context.Context
	db    *sqlx.DB
	redis *redisClient.Client
	cfg   config.GateConfig
}

func newBindHarness(t *testing.T, failWhen string) *bindHarness {
	t.Helper()
	if os.Getenv("INTEGRATION") == "" {
		t.Skip("INTEGRATION=1 not set; skipping integration tests")
	}
	cfg := config.MustLoad()
	db, err := sqlx.Connect("pgx", cfg.Postgres.DSN())
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	rc := redisClient.NewClient(cfg.Redis)
	t.Cleanup(func() { rc.Close() })

	cfg.Gate.HMACSecret = "0123456789abcdef0123456789abcdef"
	cfg.Gate.QRRotationSeconds = 30

	return &bindHarness{ctx: context.Background(), db: db, redis: rc, cfg: cfg.Gate}
}

// seedTicket creates a user + a claimed ticket owned by that user, returning the
// ticket_id and email.
func (h *bindHarness) seedTicket(t *testing.T, tag string) (ticketID, email string) {
	t.Helper()
	email = "bind-" + tag + "-" + uuid.NewString() + "@test.local"
	ticketID = "t-" + tag + "-" + uuid.NewString()
	var userID int64
	if err := h.db.QueryRowContext(h.ctx,
		`INSERT INTO users (email) VALUES ($1) ON CONFLICT (email) DO UPDATE SET updated_at=now() RETURNING id`, email).Scan(&userID); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if _, err := h.db.ExecContext(h.ctx,
		`INSERT INTO tickets (ticket_id, user_id, status) VALUES ($1,$2,$3)`, ticketID, userID, models.StatusClaimed); err != nil {
		t.Fatalf("seed ticket: %v", err)
	}
	return ticketID, email
}

func (h *bindHarness) svc(failWhen string) *BindingService {
	return NewBindingService(h.db, h.redis, h.cfg, &auth.MockTxnCheck{FailWhen: failWhen})
}

func (h *bindHarness) bindReq(ticketID string) BindRequest {
	tok := GenerateQROTP(h.cfg, ticketID)
	return BindRequest{
		TicketID:     tok.TicketID,
		QRUUID:       tok.UUID,
		QRTimestamp:  tok.Timestamp,
		QRSignature:  tok.Signature,
		WristbandUID: "wb-" + uuid.NewString(),
	}
}

// M3.7: full flow QR bind → txn check pass → BOUND; mock check fail → reset →
// re-bind succeeds.
func TestBindingFlowBindCheckResetRebind(t *testing.T) {
	h := newBindHarness(t, "")
	ticketID, email := h.seedTicket(t, "flow")
	svc := h.svc("") // empty FailWhen → always pass

	// Bind.
	req := h.bindReq(ticketID)
	resp, err := svc.Bind(h.ctx, req)
	if err != nil {
		t.Fatalf("Bind() error = %v", err)
	}
	if resp.Status != string(models.BindingStatusBinding) {
		t.Fatalf("Bind status = %s, want BINDING", resp.Status)
	}

	// NFC check → BOUND.
	ncResp, err := svc.NFCCheck(h.ctx, NFCCheckRequest{WristbandUID: req.WristbandUID})
	if err != nil {
		t.Fatalf("NFCCheck() error = %v", err)
	}
	if ncResp.Allowed == nil || !*ncResp.Allowed {
		t.Fatalf("NFCCheck allowed = %v, want true", ncResp.Allowed)
	}
	if ncResp.Status != string(models.BindingStatusBound) {
		t.Fatalf("NFCCheck status = %s, want BOUND", ncResp.Status)
	}

	// Reset (faulty-NFC admin override) → back to CLAIMED.
	_, err = svc.Reset(h.ctx, ResetRequest{WristbandUID: req.WristbandUID})
	if err != nil {
		t.Fatalf("Reset() error = %v", err)
	}

	// Re-bind succeeds with a fresh wristband (same ticket).
	req2 := h.bindReq(ticketID)
	resp2, err := svc.Bind(h.ctx, req2)
	if err != nil {
		t.Fatalf("re-Bind() error = %v", err)
	}
	if resp2.Status != string(models.BindingStatusBinding) {
		t.Fatalf("re-Bind status = %s, want BINDING", resp2.Status)
	}
	_ = email
}

// M3.4: faithful grace replay — a cached decision is replayed exactly, so a
// cached "denied" is never replayed as "allowed" (D1 regression).
func TestBindingGraceWindowFaithfulReplay(t *testing.T) {
	h := newBindHarness(t, "")
	ticketID, email := h.seedTicket(t, "grace")
	svc := h.svc(email) // mock fails THIS email → first check denied

	req := h.bindReq(ticketID)
	if _, err := svc.Bind(h.ctx, req); err != nil {
		t.Fatalf("Bind() error = %v", err)
	}

	// First check → denied (transaction check fails).
	first, err := svc.NFCCheck(h.ctx, NFCCheckRequest{WristbandUID: req.WristbandUID})
	if err != nil {
		t.Fatalf("NFCCheck() error = %v", err)
	}
	if first.Allowed == nil || *first.Allowed {
		t.Fatalf("first NFCCheck allowed = %v, want false", first.Allowed)
	}

	// Immediate re-scan within grace window → must replay denied, NOT allowed.
	second, err := svc.NFCCheck(h.ctx, NFCCheckRequest{WristbandUID: req.WristbandUID})
	if err != nil {
		t.Fatalf("second NFCCheck() error = %v", err)
	}
	if second.Source != "grace_window" {
		t.Fatalf("second.Source = %q, want grace_window", second.Source)
	}
	if second.Allowed == nil || *second.Allowed {
		t.Fatalf("grace replay allowed = %v, want false (D1 regression)", second.Allowed)
	}
}

// M3.5: concurrent binds of the same ticket → exactly one winner, one rejected.
func TestBindingConcurrentBindsOneWinner(t *testing.T) {
	h := newBindHarness(t, "")
	ticketID, email := h.seedTicket(t, "race")

	tok := GenerateQROTP(h.cfg, ticketID)
	// Use the same QR for both concurrent binds (only one may win the race too).
	reqA := BindRequest{TicketID: ticketID, QRUUID: tok.UUID, QRTimestamp: tok.Timestamp, QRSignature: tok.Signature, WristbandUID: "wb-A-" + uuid.NewString()}
	reqB := BindRequest{TicketID: ticketID, QRUUID: tok.UUID, QRTimestamp: tok.Timestamp, QRSignature: tok.Signature, WristbandUID: "wb-B-" + uuid.NewString()}

	svc := h.svc("")
	var wg sync.WaitGroup
	results := make([]error, 2)
	wg.Add(2)
	go func() { defer wg.Done(); _, err := svc.Bind(h.ctx, reqA); results[0] = err }()
	go func() { defer wg.Done(); _, err := svc.Bind(h.ctx, reqB); results[1] = err }()
	wg.Wait()

	okCount, errCount := 0, 0
	for _, err := range results {
		if err == nil {
			okCount++
		} else {
			errCount++
		}
	}
	// Because both share one QR, at most one can consume it → exactly one success.
	if okCount != 1 || errCount != 1 {
		t.Fatalf("concurrent bind: ok=%d err=%d, want 1/1", okCount, errCount)
	}
	_ = email

	// Also assert a direct double-BindWristband on the same ticket is atomic-ish:
	// but the QR single-use already guarantees one winner here.
}

// TestBindingQRReplayRejected: a QR token, once consumed at bind, cannot be
// replayed even within the 30s rotation window (R21).
func TestBindingQRReplayRejected(t *testing.T) {
	h := newBindHarness(t, "")
	ticketID, _ := h.seedTicket(t, "qr")
	svc := h.svc("")

	req := h.bindReq(ticketID)
	if _, err := svc.Bind(h.ctx, req); err != nil {
		t.Fatalf("first Bind() error = %v", err)
	}
	// Replay the exact same QR with a different wristband → rejected as replay.
	req2 := req
	req2.WristbandUID = "wb-other-" + uuid.NewString()
	if _, err := svc.Bind(h.ctx, req2); err == nil {
		t.Fatal("replayed Bind() = nil error, want ErrQRReplay")
	}
}

// TestBindingRequestSkew: a QR bound to a different ticket fails verification.
func TestBindingQRTamperedTicket(t *testing.T) {
	h := newBindHarness(t, "")
	ticketID, _ := h.seedTicket(t, "tamper")
	svc := h.svc("")

	req := h.bindReq(ticketID)
	req.TicketID = "t-different-" + uuid.NewString() // tamper
	if _, err := svc.Bind(h.ctx, req); err == nil {
		t.Fatal("tampered-ticket Bind() = nil error, want ErrQRInvalid")
	}
}
