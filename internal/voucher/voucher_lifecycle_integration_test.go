//go:build integration

// Voucher lifecycle integration tests (M4.5, M4.8) against real Postgres.
//
//	go test -tags=integration ./internal/voucher -run TestIntegration -v -count=1
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

func voucherIntegrationRepo(t *testing.T) (*postgres.Repository, *sqlx.DB) {
	t.Helper()
	if os.Getenv("INTEGRATION") == "" {
		t.Skip("INTEGRATION=1 not set; skipping integration tests")
	}
	db, err := sqlx.Connect("pgx", config.MustLoad().Postgres.DSN())
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return postgres.NewRepository(db), db
}

func voucherStatus(t *testing.T, db *sqlx.DB, voucherID string) models.TicketStatus {
	t.Helper()
	var s string
	if err := db.Get(&s, `SELECT status FROM ticket_vouchers WHERE voucher_id=$1`, voucherID); err != nil {
		t.Fatalf("read status: %v", err)
	}
	return models.TicketStatus(s)
}

// M4.5: purchase 8 → all 8 UNCLAIMED → claim one → CLAIMED.
func TestIntegrationVoucherPurchaseAndClaim(t *testing.T) {
	repo, db := voucherIntegrationRepo(t)
	ctx := context.Background()
	run := time.Now().Format("20060102150405.000000000")

	dad, err := repo.CreateUser(ctx, "dad-"+run+"@test.local")
	if err != nil {
		t.Fatalf("create dad: %v", err)
	}
	ids, err := repo.CreateVouchers(ctx, dad.ID, 8)
	if err != nil {
		t.Fatalf("purchase 8: %v", err)
	}
	if len(ids) != 8 {
		t.Fatalf("expected 8 vouchers, got %d", len(ids))
	}
	for _, id := range ids {
		if st := voucherStatus(t, db, id); st != models.StatusUnclaimed {
			t.Fatalf("M4.5: voucher %s should be UNCLAIMED, got %s", id, st)
		}
	}

	// Claim exactly one.
	wife, err := repo.CreateUser(ctx, "wife-"+run+"@test.local")
	if err != nil {
		t.Fatalf("create wife: %v", err)
	}
	if err := repo.ClaimVoucher(ctx, ids[0], wife.ID); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if st := voucherStatus(t, db, ids[0]); st != models.StatusClaimed {
		t.Fatalf("M4.5: claimed voucher should be CLAIMED, got %s", st)
	}
	// The other 7 stay unclaimed.
	for _, id := range ids[1:] {
		if st := voucherStatus(t, db, id); st != models.StatusUnclaimed {
			t.Fatalf("M4.5: voucher %s should still be UNCLAIMED, got %s", id, st)
		}
	}
}

// M4.8 (off-chain): dad buys 4 → delegates 1 to wife (account mode) → claim →
// own non-custodial wallet attached → wallet resolution → mint lands in her wallet.
func TestIntegrationAccountDelegationResolvesToOwnWallet(t *testing.T) {
	repo, _ := voucherIntegrationRepo(t)
	svc := NewService(repo)
	ctx := context.Background()
	run := time.Now().Format("20060102150405.000000000")

	dadEmail := "daddel-" + run + "@test.local"
	wifeEmail := "wifedel-" + run + "@test.local"

	// Dad (purchaser) buys 4.
	dad, err := repo.CreateUser(ctx, dadEmail)
	if err != nil {
		t.Fatalf("create dad: %v", err)
	}
	ids, err := repo.CreateVouchers(ctx, dad.ID, 4)
	if err != nil {
		t.Fatalf("purchase 4: %v", err)
	}

	// Dad delegates one voucher to wife (account-linked).
	res, err := svc.Delegate(ctx, models.DelegateRequest{
		VoucherID: ids[0], Mode: models.DelegationAccount, AccountEmail: wifeEmail,
	})
	if err != nil {
		t.Fatalf("delegate: %v", err)
	}
	if res.ParticipantID == 0 {
		t.Fatal("expected a participant for wife")
	}
	// Wife claims the delegated voucher via the magic-link flow, which
	// JIT-registers her account (creates the users row) before claiming.
	wifeUser, err := repo.CreateUser(ctx, wifeEmail)
	if err != nil {
		t.Fatalf("create wife user: %v", err)
	}
	if err := repo.ClaimVoucher(ctx, ids[0], wifeUser.ID); err != nil {
		t.Fatalf("claim: %v", err)
	}

	// Week 6 attaches wife's own non-custodial wallet; here we simulate it and
	// assert mint resolution points at HER wallet (off-chain half of M4.8).
	if err := repo.SetParticipantWallet(ctx, res.ParticipantID, models.ParticipantWalletOwn, "0xwife-own"); err != nil {
		t.Fatalf("attach own wallet: %v", err)
	}
	wifeParticipant, err := repo.GetParticipant(ctx, res.ParticipantID)
	if err != nil || wifeParticipant == nil {
		t.Fatalf("get wife participant: %v (err=%v)", wifeParticipant, err)
	}
	mr := svc.ResolveMintWallet(wifeParticipant)
	if mr.Pending {
		t.Fatal("M4.8: own wallet present → must not be pending")
	}
	if mr.WalletAddress != "0xwife-own" || mr.WalletState != models.ParticipantWalletOwn {
		t.Fatalf("M4.8: mint should resolve to wife's own wallet, got %+v", mr)
	}
}
