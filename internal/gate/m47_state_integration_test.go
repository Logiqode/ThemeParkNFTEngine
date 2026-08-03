//go:build integration

// M4.7: full Rev 1/R13 ticket state machine — claim → QR bind (PENDING_ENTRY) →
// NFC txn check (ACTIVE) → ride scan (USED) — verified live against Postgres +
// Redis.
//
//	go test -tags=integration ./internal/gate -run TestM47 -v -count=1
package gate

import (
	"context"
	"testing"

	"github.com/Logiqode/ThemeParkNFT/internal/models"
)

// mockPublisher is a no-op ScanEventPublisher for the ride-scan path.
type mockPublisher struct{}

func (mockPublisher) PublishScanEvent(context.Context, *models.ScanEvent) error { return nil }

func m47TicketStatus(t *testing.T, h *bindHarness, ticketID string) models.TicketStatus {
	t.Helper()
	var s string
	if err := h.db.Get(&s, `SELECT status FROM tickets WHERE ticket_id=$1`, ticketID); err != nil {
		t.Fatalf("read ticket status: %v", err)
	}
	return models.TicketStatus(s)
}

func TestM47TicketStateTransitions(t *testing.T) {
	h := newBindHarness(t, "")

	// CLAIMED (seedTicket inserts status=CLAIMED).
	ticketID, _ := h.seedTicket(t, "m47")
	if st := m47TicketStatus(t, h, ticketID); st != models.StatusClaimed {
		t.Fatalf("start: expected CLAIMED, got %s", st)
	}

	svc := h.svc("") // transaction check always passes

	// 1. claim → QR bind → PENDING_ENTRY (R13 "BINDING").
	req := h.bindReq(ticketID)
	if _, err := svc.Bind(h.ctx, req); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if st := m47TicketStatus(t, h, ticketID); st != models.StatusPendingEntry {
		t.Fatalf("after bind: expected PENDING_ENTRY, got %s", st)
	}

	// 2. NFC transaction check → ACTIVE (R13 "BOUND").
	if _, err := svc.NFCCheck(h.ctx, NFCCheckRequest{WristbandUID: req.WristbandUID}); err != nil {
		t.Fatalf("nfc-check: %v", err)
	}
	if st := m47TicketStatus(t, h, ticketID); st != models.StatusActive {
		t.Fatalf("after nfc-check: expected ACTIVE, got %s", st)
	}

	// 3. ride scan → USED (M4.7).
	scanSvc := NewRideScanService(h.redis, mockPublisher{}, h.db)
	if _, err := scanSvc.Scan(h.ctx, RideScanRequest{WristbandUID: req.WristbandUID, RideID: "ride-1"}); err != nil {
		t.Fatalf("ride scan: %v", err)
	}
	if st := m47TicketStatus(t, h, ticketID); st != models.StatusUsed {
		t.Fatalf("after ride scan: expected USED, got %s", st)
	}
}
