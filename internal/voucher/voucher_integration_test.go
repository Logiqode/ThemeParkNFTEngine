//go:build integration

// Integration tests for the Rev 3 family voucher & participant model (R26-R35).
// Require a running Postgres with migration 0002 applied (use `make up` then
// `make migrate-up`):
//
//	go test -tags=integration ./internal/voucher -v -count=1
//
// Exercises the real *postgres.Repository through the voucher service:
//   - M4.9  dependent (no account) → custodial-proxy wallet, no pending mint
//   - M4.10 durable pending_mints attribution ledger (row outlives Redis/wristband)
//   - R33   claim-custody flips a dependent to their own non-custodial wallet
package voucher

import (
	"context"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"

	"github.com/Logiqode/ThemeParkNFT/internal/config"
	"github.com/Logiqode/ThemeParkNFT/internal/models"
	"github.com/Logiqode/ThemeParkNFT/internal/postgres"
)

func integrationSVCSession(t *testing.T) (*Service, *sqlx.DB) {
	t.Helper()
	if os.Getenv("INTEGRATION") == "" {
		t.Skip("INTEGRATION=1 not set; skipping integration tests")
	}
	dsn := config.MustLoad().Postgres.DSN()
	db, err := sqlx.Connect("pgx", dsn)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return NewService(postgres.NewRepository(db)), db
}

func TestIntegrationDependentDelegationAndMintResolution(t *testing.T) {
	svc, db := integrationSVCSession(t)
	ctx := context.Background()
	run := time.Now().Format("20060102150405.000000000")

	// Create a guardian (family head) + delegate a voucher to them.
	guardianEmail := "guardian-" + run + "@test.local"
	gd, err := svc.Delegate(ctx, models.DelegateRequest{
		VoucherID: voucherFor(t, db, run, "g"), Mode: models.DelegationAccount, AccountEmail: guardianEmail,
	})
	if err != nil {
		t.Fatalf("delegate guardian: %v", err)
	}

	// M4.9: delegate to a child dependent — custodial wallet, never pending.
	dep, err := svc.Delegate(ctx, models.DelegateRequest{
		VoucherID: voucherFor(t, db, run, "k"), Mode: models.DelegationDependent, Name: "Kid", GuardianID: gd.ParticipantID,
	})
	if err != nil {
		t.Fatalf("delegate dependent: %v", err)
	}
	if dep.Pending {
		t.Fatal("M4.9: dependent must not be pending (mint straight to custodial)")
	}
	if dep.WalletState != models.ParticipantWalletCustodial || dep.WalletAddress == "" {
		t.Fatalf("M4.9: expected custodial wallet, got %+v", dep)
	}

	// The voucher row must now be linked to the participant (R27/R28).
	var participantID int64
	if err := db.GetContext(ctx, &participantID,
		`SELECT participant_id FROM ticket_vouchers WHERE voucher_id=$1`, dep.VoucherID); err != nil {
		t.Fatalf("read voucher link: %v", err)
	}
	if participantID != dep.ParticipantID {
		t.Fatalf("voucher not linked to participant: got %d want %d", participantID, dep.ParticipantID)
	}

	// M4.10: the guardian (adult) has no wallet yet → durable pending ledger row.
	if err := svc.RecordPendingMint(ctx, models.RecordPendingMintRequest{
		ParticipantID: gd.ParticipantID,
		MintDate:      "2026-08-03",
		RideIDs:       []string{"r1", "r2", "r3"},
		ScannedAts:    []time.Time{time.Now().Add(-2 * time.Hour), time.Now().Add(-time.Hour)},
	}); err != nil {
		t.Fatalf("record pending mint: %v", err)
	}
	pm, err := svc.GetPendingMint(ctx, gd.ParticipantID, "2026-08-03")
	if err != nil || pm == nil {
		t.Fatalf("M4.10: expected durable pending row, got %v err=%v", pm, err)
	}
	if len(pm.RideIDs) != 3 {
		t.Fatalf("M4.10: ride_ids round-trip failed, got %v", pm.RideIDs)
	}
	if len(pm.ScannedAts) != 2 {
		t.Fatalf("M4.10: scanned_ats round-trip failed, got %d", len(pm.ScannedAts))
	}

	// R33: dependent later links their own Google account → custody transferred.
	kid, err := svc.ClaimCustody(ctx, dep.ParticipantID, "0xkid-own-wallet")
	if err != nil {
		t.Fatalf("claim custody: %v", err)
	}
	if kid.WalletState != models.ParticipantWalletOwn {
		t.Fatalf("R33: expected OWN after claim custody, got %s", kid.WalletState)
	}
}

// voucherFor creates an UNCLAIMED voucher under a fresh purchaser and returns
// its id, wired to the unique run suffix.
func voucherFor(t *testing.T, db *sqlx.DB, run, tag string) string {
	t.Helper()
	ctx := context.Background()
	var purchaserID int64
	if err := db.GetContext(ctx, &purchaserID,
		`INSERT INTO users (email) VALUES ($1) ON CONFLICT (email) DO UPDATE SET updated_at=now() RETURNING id`,
		"purchaser-"+run+"-"+tag+"@test.local"); err != nil {
		t.Fatalf("seed purchaser: %v", err)
	}
	vid := "it-voucher-" + run + "-" + tag
	if _, err := db.ExecContext(ctx,
		`INSERT INTO ticket_vouchers (voucher_id, purchaser_id, status) VALUES ($1,$2,'UNCLAIMED')`,
		vid, purchaserID); err != nil {
		t.Fatalf("insert voucher: %v", err)
	}
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), `DELETE FROM ticket_vouchers WHERE voucher_id=$1`, vid) })
	return vid
}
