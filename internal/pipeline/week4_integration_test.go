//go:build integration

// Week 4 ingestion-pipeline integration tests (M4.1 persistence, M4.2 Redis
// aggregation, M4.3 Redis-down outbox no-loss, M4.4 E2E 10k/15% dups) against
// real Postgres + Redis.
//
//	go test -tags=integration ./internal/pipeline -run TestW4 -v -count=1
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"

	"github.com/Logiqode/ThemeParkNFT/internal/config"
	"github.com/Logiqode/ThemeParkNFT/internal/models"
	"github.com/Logiqode/ThemeParkNFT/internal/postgres"
	redisClient "github.com/Logiqode/ThemeParkNFT/internal/redis"
)

type w4h struct {
	ctx  context.Context
	db   *sqlx.DB
	rc   *redisClient.Client
	repo *postgres.Repository
}

func newW4h(t *testing.T) *w4h {
	t.Helper()
	if os.Getenv("INTEGRATION") == "" {
		t.Skip("INTEGRATION=1 not set; skipping integration tests")
	}
	db, err := sqlx.Connect("pgx", config.MustLoad().Postgres.DSN())
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	rc := redisClient.NewClient(config.MustLoad().Redis)
	t.Cleanup(func() { rc.Close() })
	return &w4h{ctx: context.Background(), db: db, rc: rc, repo: postgres.NewRepository(db)}
}

func (h *w4h) seedUser(t *testing.T, tag string) (int64, string) {
	t.Helper()
	email := tag + "-" + fmt.Sprint(time.Now().UnixNano()) + "@test.local"
	u, err := h.repo.CreateUser(h.ctx, email)
	if err != nil {
		t.Fatalf("seed user: %v", err)
	}
	return u.ID, email
}

func (h *w4h) countScans(userID int64) int {
	var n int
	if err := h.db.Get(&n, `SELECT COUNT(*) FROM scan_events WHERE user_id=$1`, userID); err != nil {
		return -1
	}
	return n
}

func (h *w4h) event(email, trace, ride string, ts time.Time) *models.ScanEvent {
	return &models.ScanEvent{
		UserID: email, RideID: ride, TraceID: trace, TicketID: "", Timestamp: ts.UnixMilli(),
	}
}

func (h *w4h) handleMany(hnd *ScanHandler, evs []*models.ScanEvent) error {
	for _, e := range evs {
		if err := hnd.Handle(h.ctx, e); err != nil {
			return fmt.Errorf("handle %s: %w", e.TraceID, err)
		}
	}
	return nil
}

// failAgg wraps a healthy redis client but errors on the aggregation SADD,
// simulating Redis going down mid-pipeline (dedup still works).
type failAgg struct {
	base *redisClient.Client
}

func (f failAgg) IsDuplicate(ctx context.Context, trace string) (bool, error) { return f.base.IsDuplicate(ctx, trace) }
func (f failAgg) ClearDedup(ctx context.Context, trace string) error          { return f.base.ClearDedup(ctx, trace) }
func (f failAgg) GetUserRides(ctx context.Context, user, date string) ([]string, error) {
	return f.base.GetUserRides(ctx, user, date)
}
func (failAgg) AddRideToUserSet(ctx context.Context, _, _, _ string) error {
	return errors.New("redis down (simulated aggregate failure)")
}

// M4.1: 5k valid events → PG row count == 5k, no dups.
func TestW4Persistence5k(t *testing.T) {
	h := newW4h(t)
	userID, email := h.seedUser(t, "w41")
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	run := time.Now().UnixNano()
	hnd := NewScanHandler(h.db, h.rc)

	events := make([]*models.ScanEvent, 0, 5000)
	for i := 0; i < 5000; i++ {
		events = append(events, h.event(email, fmt.Sprintf("w41-%d-trace-%d", run, i), fmt.Sprintf("ride-%d", i%5), now.Add(time.Duration(i)*time.Millisecond)))
	}
	if err := h.handleMany(hnd, events); err != nil {
		t.Fatalf("M4.1: %v", err)
	}
	if got := h.countScans(userID); got != 5000 {
		t.Fatalf("M4.1: expected 5000 rows, got %d", got)
	}
	// Re-submit half → all deduped, no dups.
	dup := []*models.ScanEvent{events[0], events[1], events[2500], events[4999]}
	if err := h.handleMany(hnd, dup); err != nil {
		t.Fatalf("M4.1 re-submit: %v", err)
	}
	if got := h.countScans(userID); got != 5000 {
		t.Fatalf("M4.1: dups leaked, expected still 5000, got %d", got)
	}
}

// M4.2: user 3 distinct rides → Redis set cardinality == 3; dup ride → unchanged.
func TestW4Aggregation(t *testing.T) {
	h := newW4h(t)
	_, email := h.seedUser(t, "w42")
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	date := now.Format("2006-01-02")
	run := time.Now().UnixNano()
	hnd := NewScanHandler(h.db, h.rc)

	for i, ride := range []string{"r1", "r2", "r3"} {
		if err := hnd.Handle(h.ctx, h.event(email, fmt.Sprintf("w42-%d-trace-%d", run, i), ride, now)); err != nil {
			t.Fatalf("M4.2: %v", err)
		}
	}
	rides, err := h.rc.GetUserRides(h.ctx, email, date)
	if err != nil {
		t.Fatalf("M4.2: %v", err)
	}
	if len(rides) != 3 {
		t.Fatalf("M4.2: expected 3 distinct rides, got %v", rides)
	}
	// Same ride again (fresh scan) → set unchanged.
	if err := hnd.Handle(h.ctx, h.event(email, fmt.Sprintf("w42-%d-dup-r1", run), "r1", now.Add(time.Minute))); err != nil {
		t.Fatalf("M4.2 dup: %v", err)
	}
	rides, _ = h.rc.GetUserRides(h.ctx, email, date)
	if len(rides) != 3 {
		t.Fatalf("M4.2: dup ride changed cardinality, got %v", rides)
	}
}

// M4.3: Redis down for the aggregate step → PG insert succeeds, intent parked in
// outbox; drain replays into Redis and clears the row → no loss.
func TestW4OutboxNoLoss(t *testing.T) {
	h := newW4h(t)
	_, email := h.seedUser(t, "w43")
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	trace := fmt.Sprintf("w43-%d-1", time.Now().UnixNano())

	// Handler whose aggregation step fails (Redis down), outbox armed.
	var outbox OutboxWriter = h.repo
	hnd := NewScanHandler(h.db, failAgg{base: h.rc}).WithOutbox(outbox)
	if err := hnd.Handle(h.ctx, h.event(email, trace, "r-purple", now)); err != nil {
		t.Fatalf("M4.3: handle should commit (nil error) with outbox parked, got %v", err)
	}

	// PG insert committed.
	if n := h.countScansForTrace(trace); n != 1 {
		t.Fatalf("M4.3: expected PG row, got %d", n)
	}
	// Outbox row parked.
	pending, err := h.repo.ListPendingOutbox(h.ctx, 100)
	if err != nil {
		t.Fatalf("M4.3: list outbox: %v", err)
	}
	if len(pending) != 1 || pending[0].TraceID != trace {
		t.Fatalf("M4.3: expected 1 outbox row for %s, got %+v", trace, pending)
	}
	// Redis does NOT yet have the ride (aggregate failed).
	date := now.Format("2006-01-02")
	rides, _ := h.rc.GetUserRides(h.ctx, email, date)
	if len(rides) != 0 {
		t.Fatalf("M4.3: redis should be empty before drain, got %v", rides)
	}

	// Drain with a healthy handler → SADD lands, outbox cleared.
	healthy := NewScanHandler(h.db, h.rc).WithOutbox(outbox)
	drained, err := healthy.DrainOutbox(h.ctx, h.repo, 100)
	if err != nil {
		t.Fatalf("M4.3: drain: %v", err)
	}
	if drained != 1 {
		t.Fatalf("M4.3: expected to drain 1, got %d", drained)
	}
	rides, _ = h.rc.GetUserRides(h.ctx, email, date)
	if len(rides) != 1 {
		t.Fatalf("M4.3: redis should have the ride after drain, got %v", rides)
	}
	if rest, _ := h.repo.ListPendingOutbox(h.ctx, 100); len(rest) != 0 {
		t.Fatalf("M4.3: outbox should be empty after drain, got %v", rest)
	}
}

func (h *w4h) countScansForTrace(trace string) int {
	var n int
	if err := h.db.Get(&n, `SELECT COUNT(*) FROM scan_events WHERE trace_id=$1`, trace); err != nil {
		return -1
	}
	return n
}

// M4.4: E2E — 10k events, 15% dups → PG == 8.5k unique, Redis sets correct.
func TestW4E2ELoadgenDups(t *testing.T) {
	h := newW4h(t)
	userID, email := h.seedUser(t, "w44")
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	date := now.Format("2006-01-02")
	run := time.Now().UnixNano()
	hnd := NewScanHandler(h.db, h.rc)

	var events []*models.ScanEvent
	const uniq = 8500
	for i := 0; i < uniq; i++ {
		events = append(events, h.event(email, fmt.Sprintf("w44-%d-u-%d", run, i), fmt.Sprintf("ride-%d", i%10), now.Add(time.Duration(i)*time.Microsecond)))
	}
	// 1500 duplicates (15% of 10k total) replaying existing trace ids.
	for i := 0; i < 1500; i++ {
		events = append(events, events[i])
	}
	if len(events) != 10000 {
		t.Fatalf("M4.4: expected 10000 events, got %d", len(events))
	}
	if err := h.handleMany(hnd, events); err != nil {
		t.Fatalf("M4.4: %v", err)
	}
	if got := h.countScans(userID); got != uniq {
		t.Fatalf("M4.4: expected %d unique rows (10k, 15%% dups), got %d", uniq, got)
	}
	// Redis set reflects only the 10 distinct rides actually ridden.
	rides, err := h.rc.GetUserRides(h.ctx, email, date)
	if err != nil {
		t.Fatalf("M4.4: %v", err)
	}
	if len(rides) != 10 {
		t.Fatalf("M4.4: expected 10 distinct rides in redis set, got %v", rides)
	}
}
