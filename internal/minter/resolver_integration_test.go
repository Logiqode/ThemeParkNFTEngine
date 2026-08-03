//go:build integration

// Integration test for the off-chain end-of-day mint-resolution driver (M4.12,
// R30/R32) against real Postgres. Proves M4.10 end-to-end via the durable
// scan_events source: an unresolved adult with rides on a date gets a durable
// pending_mints attribution row (rebuildable from scan_events, independent of
// Redis/wristband lifetime). NO on-chain activity.
//
// Requires `make up` + `make migrate-up`, then:
//
//	go test -tags=integration ./internal/minter -v -count=1
package minter

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"

	"github.com/Logiqode/ThemeParkNFT/internal/config"
	"github.com/Logiqode/ThemeParkNFT/internal/models"
	"github.com/Logiqode/ThemeParkNFT/internal/postgres"
	voucherService "github.com/Logiqode/ThemeParkNFT/internal/voucher"
)

func TestIntegrationResolveDayWritesDurablePending(t *testing.T) {
	if os.Getenv("INTEGRATION") == "" {
		t.Skip("INTEGRATION=1 not set; skipping integration tests")
	}
	dsn := config.MustLoad().Postgres.DSN()
	db, err := sqlx.Connect("pgx", dsn)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	ctx := context.Background()
	repo := postgres.NewRepository(db)
	run := time.Now().Format("20060102150405.000000000")
	const date = "2026-08-03"
	email := "minter-rd-" + run + "@test.local"

	// Account-linked adult participant with NO wallet (wallet_state NONE) →
	// should resolve to durable pending.
	user, err := repo.CreateUser(ctx, email)
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	participant, err := repo.CreateParticipant(ctx, "Unresolved Adult", &email, nil)
	if err != nil {
		t.Fatalf("create participant: %v", err)
	}

	// Seed durable scan_events on the target date (3 distinct rides).
	base := time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC)
	for i, ride := range []string{"r1", "r2", "r3"} {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO scan_events (trace_id, user_id, ride_id, scanned_at) VALUES ($1,$2,$3,$4)`,
			fmt.Sprintf("it-minter-%s-%d", run, i), user.ID, ride, base.Add(time.Duration(i)*time.Hour)); err != nil {
			t.Fatalf("seed scan_event: %v", err)
		}
	}

	// Run the off-chain day resolution.
	d := NewDayResolver(repo, NewScanEventRideSource(db), voucherService.NewService(repo))
	resolutions, err := d.ResolveDay(ctx, date)
	if err != nil {
		t.Fatalf("resolve day: %v", err)
	}

	var found bool
	for _, res := range resolutions {
		if res.ParticipantID == participant.ID {
			found = true
			if res.Outcome != OutcomePending {
				t.Fatalf("expected pending outcome for unresolved adult, got %+v", res)
			}
		}
	}
	if !found {
		t.Fatalf("no resolution returned for participant %d", participant.ID)
	}

	// M4.10: durable pending_mints row persisted (outlives Redis/wristband).
	pm, err := repo.GetPendingMint(ctx, participant.ID, date)
	if err != nil {
		t.Fatalf("get pending mint: %v", err)
	}
	if pm == nil {
		t.Fatalf("expected durable pending_mints row for participant %d on %s", participant.ID, date)
	}
	if len(pm.RideIDs) != 3 {
		t.Fatalf("expected 3 ride ids round-tripped, got %v", pm.RideIDs)
	}
	if len(pm.ScannedAts) != 3 {
		t.Fatalf("expected 3 scanned times round-tripped, got %d", len(pm.ScannedAts))
	}

	// Idempotency: re-running the pass refreshes, never duplicates.
	if _, err := d.ResolveDay(ctx, date); err != nil {
		t.Fatalf("re-run resolve day: %v", err)
	}
	pm2, err := repo.GetPendingMint(ctx, participant.ID, date)
	if err != nil || pm2 == nil {
		t.Fatalf("expected row after re-run, got %v err=%v", pm2, err)
	}
	if len(pm2.RideIDs) != 3 {
		t.Fatalf("idempotency broken: ride ids changed to %v", pm2.RideIDs)
	}
}

// TestIntegrationResolveDayDependentIncluded verifies the dependent
// ride-attribution path: a dependent's rides (ticket_vouchers.participant_id
// → scan_events.ticket_id) are picked up by the end-of-day driver and resolve
// to the custodial wallet (mint-ready, R31).
func TestIntegrationResolveDayDependentIncluded(t *testing.T) {
	if os.Getenv("INTEGRATION") == "" {
		t.Skip("INTEGRATION=1 not set; skipping integration tests")
	}
	dsn := config.MustLoad().Postgres.DSN()
	db, err := sqlx.Connect("pgx", dsn)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	ctx := context.Background()
	repo := postgres.NewRepository(db)
	run := time.Now().Format("20060102150405.000000000")
	const date = "2026-08-03"
	guardianEmail := "minter-guardian-" + run + "@test.local"

	// Guardian + dependent (no account).
	guardian, err := repo.CreateParticipant(ctx, "Guardian", &guardianEmail, nil)
	if err != nil {
		t.Fatalf("create guardian: %v", err)
	}
	dependent, err := repo.CreateParticipant(ctx, "Kid", nil, &guardian.ID)
	if err != nil {
		t.Fatalf("create dependent: %v", err)
	}
	if err := repo.SetParticipantWallet(ctx, dependent.ID, models.ParticipantWalletCustodial, "0xguardian-custodial-dep"); err != nil {
		t.Fatalf("set custodial wallet: %v", err)
	}

	// A users row is required by scan_events.user_id FK (proxy account for the scan).
	proxy, err := repo.CreateUser(ctx, "minter-proxy-"+run+"@test.local")
	if err != nil {
		t.Fatalf("create proxy user: %v", err)
	}
	// The dependent's ticket (voucher) that produced a ride scan.
	voucherID := "dv-" + run
	if _, err := db.ExecContext(ctx,
		`INSERT INTO ticket_vouchers (voucher_id, purchaser_id, participant_id, status) VALUES ($1,$2,$3,'CLAIMED')`,
		voucherID, proxy.ID, dependent.ID); err != nil {
		t.Fatalf("seed dependent voucher: %v", err)
	}
	// The ride scan carries ticket_id == the dependent's voucher id.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO scan_events (trace_id, user_id, ticket_id, ride_id, scanned_at) VALUES ($1,$2,$3,$4,$5)`,
		"it-minter-dep-"+run, proxy.ID, voucherID, "r-kid-slide",
		time.Date(2026, 8, 3, 11, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("seed dependent scan: %v", err)
	}

	d := NewDayResolver(repo, NewScanEventRideSource(db), voucherService.NewService(repo))
	resolutions, err := d.ResolveDay(ctx, date)
	if err != nil {
		t.Fatalf("resolve day: %v", err)
	}
	var found bool
	for _, res := range resolutions {
		if res.ParticipantID == dependent.ID {
			found = true
			if res.Outcome != OutcomeMintReady {
				t.Fatalf("dependent should be mint_ready (custodial), got %+v", res)
			}
			if len(res.RideIDs) != 1 || res.RideIDs[0] != "r-kid-slide" {
				t.Fatalf("dependent ride attribution wrong: %v", res.RideIDs)
			}
		}
	}
	if !found {
		t.Fatalf("dependent participant not resolved — dependent ride-attribution missing")
	}
}
