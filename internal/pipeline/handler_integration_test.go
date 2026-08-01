//go:build integration

// Integration smoke tests for the shared scan-event pipeline handler (R4/M1.2).
// These tests require a running Postgres + Redis (use `make up`):
//
//	go test -tags=integration ./internal/pipeline -v -count=1
//
// They exercise the exact production logic: Redis dedup → Postgres insert
// (idempotent on trace_id) → Redis daily set aggregation.
package pipeline

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/Logiqode/ThemeParkNFT/internal/config"
	"github.com/Logiqode/ThemeParkNFT/internal/models"
	redisClient "github.com/Logiqode/ThemeParkNFT/internal/redis"
)

func integrationHandler(t *testing.T) *ScanHandler {
	t.Helper()

	// Require explicit opt-in even with the integration tag to avoid accidental
	// runs against unexpected databases. Set INTEGRATION=1 to enable.
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

	// Ensure a known user exists for the FK resolution (email-based identity, R11).
	_, err = db.ExecContext(context.Background(),
		`INSERT INTO users (email) VALUES ($1) ON CONFLICT (email) DO UPDATE SET updated_at=now()`,
		"integration@test.local")
	if err != nil {
		t.Fatalf("seed user: %v", err)
	}

	return NewScanHandler(db, rc)
}

func TestIntegrationSingleEventPersists(t *testing.T) {
	h := integrationHandler(t)
	ctx := context.Background()
	trace := "it-single-" + time.Now().Format("150405.000000000")

	event := &models.ScanEvent{
		UserID:    "integration@test.local",
		RideID:    "ride-001",
		Timestamp: time.Now().UnixMilli(),
		TraceID:   trace,
	}
	if err := h.Handle(ctx, event); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Assert Postgres row exists.
	var count int
	if err := h.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM scan_events WHERE trace_id=$1`, trace).Scan(&count); err != nil {
		t.Fatalf("query count: %v", err)
	}
	if count != 1 {
		t.Errorf("scan_events count = %d, want 1", count)
	}

	// Assert Redis daily set contains the ride.
	rides, err := h.redis.GetUserRides(ctx, "integration@test.local", time.Now().Format("2006-01-02"))
	if err != nil {
		t.Fatalf("GetUserRides: %v", err)
	}
	found := false
	for _, r := range rides {
		if r == "ride-001" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("rides = %v, missing ride-001", rides)
	}
}

func TestIntegrationDuplicateTraceDropped(t *testing.T) {
	h := integrationHandler(t)
	ctx := context.Background()
	trace := "it-dup-" + time.Now().Format("150405.000000000")

	event := &models.ScanEvent{
		UserID:    "integration@test.local",
		RideID:    "ride-002",
		Timestamp: time.Now().UnixMilli(),
		TraceID:   trace,
	}
	if err := h.Handle(ctx, event); err != nil {
		t.Fatalf("Handle() 1st = %v", err)
	}
	// Second delivery of the same trace_id must be dropped by the dedup shield.
	if err := h.Handle(ctx, event); err != nil {
		t.Fatalf("Handle() 2nd = %v", err)
	}

	var count int
	if err := h.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM scan_events WHERE trace_id=$1`, trace).Scan(&count); err != nil {
		t.Fatalf("query count: %v", err)
	}
	if count != 1 {
		t.Errorf("scan_events count = %d, want 1 (dedup)", count)
	}
}

func TestIntegrationMultipleRidesAggregated(t *testing.T) {
	h := integrationHandler(t)
	ctx := context.Background()
	ts := time.Now().UnixMilli()

	for i, ride := range []string{"ride-003", "ride-004", "ride-005"} {
		event := &models.ScanEvent{
			UserID:    "integration@test.local",
			RideID:    ride,
			Timestamp: ts + int64(i),
			TraceID:   "it-agg-" + time.Now().Format("150405.000000000") + ride,
		}
		if err := h.Handle(ctx, event); err != nil {
			t.Fatalf("Handle(%s) = %v", ride, err)
		}
	}

	rides, err := h.redis.GetUserRides(ctx, "integration@test.local", time.Now().Format("2006-01-02"))
	if err != nil {
		t.Fatalf("GetUserRides: %v", err)
	}
	seen := map[string]bool{}
	for _, r := range rides {
		seen[r] = true
	}
	for _, want := range []string{"ride-003", "ride-004", "ride-005"} {
		if !seen[want] {
			t.Errorf("rides missing %s (got %v)", want, rides)
		}
	}
}

</write_to_file>
<task_progress>
- [x] R8: Memory bank
- [x] R17a: QR route rename
- [x] R5: Kafka default
- [x] R1: Migrations tooling + Makefile
- [x] R7: Compose + make healthy
- [x] R2: Strict readiness wired in all 4 services
- [x] R3: config.Validate + gate + minter
- [x] R16: internal/auth/TxnCheckPerformer + MockTxnCheck
- [x] R4: Unit tests + pipeline handler + consumer refactor
- [x] R4: Integration smoke test created
- [ ] R4: Add models import to integration test (check compile)
- [ ] R4: CI Kafka service
- [ ] Verify build + tests
- [ ] Update implementation_plan + memory-bank + README
</task_progress>
</write_to_file>