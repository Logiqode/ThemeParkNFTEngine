package demo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Logiqode/ThemeParkNFT/internal/models"
	"github.com/Logiqode/ThemeParkNFT/internal/sui"
)

// isNoRows reports whether err is sql.ErrNoRows.
func isNoRows(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

const (
	guardianEmailFmt = "guardian-%04d@themepark.local"
	dependentNameFmt = "dependent-%04d"
	rideFmt          = "ride-%03d"
)

// scenarioDate returns the deterministic calendar date each scenario's rides
// are seeded under. Using distinct dates isolates a scenario's mint set from
// other scenarios' additive data (no reset), while guardians/dependents remain
// shared participants across scenarios (2c reuses 2b's guardians).
func scenarioDate(s Scenario) string {
	switch s {
	case ScenarioProbe:
		return "2026-08-13"
	case ScenarioGuardianMint:
		return "2026-08-14"
	case ScenarioDependentMint:
		return "2026-08-15"
	case ScenarioMixedMint:
		return "2026-08-16"
	default:
		return "2026-08-13"
	}
}

// guardianEmail builds a deterministic guardian email for 1-based index n.
func guardianEmail(n int) string {
	return fmt.Sprintf(guardianEmailFmt, n)
}

// dependentName builds a deterministic dependent name for 1-based index n.
func dependentName(n int) string {
	return fmt.Sprintf(dependentNameFmt, n)
}

// rideID builds a deterministic ride id for 1-based index n.
func rideID(n int) string {
	return fmt.Sprintf(rideFmt, n)
}

// guardian is a seeded guardian participant + its owning user + derived wallet.
type guardian struct {
	userID   int64
	particip *models.Participant
	wallet   string
}

// seedGuardian get-or-creates a guardian user + account-linked participant with
// its own non-custodial deterministic wallet. Idempotent (guardians are shared
// across scenarios).
func (o *Orchestrator) seedGuardian(ctx context.Context, email string) (*guardian, error) {
	user, err := o.repo.CreateUser(ctx, email)
	if err != nil {
		return nil, fmt.Errorf("create user %s: %w", email, err)
	}

	p, err := o.repo.GetParticipantByEmail(ctx, email)
	if err != nil {
		return nil, err
	}
	if p == nil {
		p, err = o.repo.CreateParticipant(ctx, email, &email, nil)
		if err != nil {
			return nil, err
		}
	}

	_, addr, err := sui.DeterministicWallet(email, o.walletSecret)
	if err != nil {
		return nil, err
	}
	if err := o.repo.SetParticipantWallet(ctx, p.ID, models.ParticipantWalletOwn, addr); err != nil {
		return nil, err
	}
	if err := o.repo.UpdateSuiAccount(ctx, user.ID, addr, "", ""); err != nil {
		return nil, err
	}

	return &guardian{userID: user.ID, particip: p, wallet: addr}, nil
}

// seedDependent get-or-creates a dependent participant under a guardian, with a
// custodial-proxy wallet set to the guardian's derived address (R31/R35: the
// dependent's NFT mints into the guardian wallet). Returns the dependent.
func (o *Orchestrator) seedDependent(ctx context.Context, g *guardian, name string) (*models.Participant, error) {
	p, err := o.findDependent(ctx, name, g.particip.ID)
	if err != nil {
		return nil, err
	}
	if p == nil {
		guardianID := g.particip.ID
		p, err = o.repo.CreateParticipant(ctx, name, nil, &guardianID)
		if err != nil {
			return nil, err
		}
	}
	if err := o.repo.SetParticipantWallet(ctx, p.ID, models.ParticipantWalletCustodial, g.wallet); err != nil {
		return nil, err
	}
	return p, nil
}

// findDependent looks up a dependent participant by (name, guardian_id).
func (o *Orchestrator) findDependent(ctx context.Context, name string, guardianID int64) (*models.Participant, error) {
	var p models.Participant
	err := o.db.GetContext(ctx, &p,
		`SELECT * FROM participants WHERE name=$1 AND guardian_id=$2 LIMIT 1`, name, guardianID)
	if err != nil {
		if isNoRows(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("find dependent %s: %w", name, err)
	}
	return &p, nil
}

// seedRideAccount inserts an account-linked ride (guardian owns the scan via
// users.email). Idempotent per trace_id.
func (o *Orchestrator) seedRideAccount(ctx context.Context, guardian *guardian, date, ride, trace string) error {
	scannedAt := dateScannedAt(date, ride)
	_, err := o.db.ExecContext(ctx,
		`INSERT INTO scan_events (trace_id, user_id, ticket_id, ride_id, scanned_at)
		 VALUES ($1, $2, NULL, $3, $4) ON CONFLICT (trace_id) DO NOTHING`,
		trace, guardian.userID, ride, scannedAt)
	return err
}

// seedRideDependent inserts a dependent ride linked via a voucher (ticket_id).
// The scan is attributed to the dependent participant through
// ticket_vouchers.participant_id (what ScanEventRideSource.byParticipantID
// joins on). Idempotent.
func (o *Orchestrator) seedRideDependent(ctx context.Context, g *guardian, p *models.Participant, date, ride, trace string) error {
	voucherID := fmt.Sprintf("demo-voucher-%d", p.ID)
	_, err := o.db.ExecContext(ctx,
		`INSERT INTO ticket_vouchers (voucher_id, purchaser_id, status, participant_id)
		 VALUES ($1, $2, 'UNCLAIMED', $3) ON CONFLICT (voucher_id) DO NOTHING`,
		voucherID, g.userID, p.ID)
	if err != nil {
		return fmt.Errorf("insert dependent voucher: %w", err)
	}

	scannedAt := dateScannedAt(date, ride)
	_, err = o.db.ExecContext(ctx,
		`INSERT INTO scan_events (trace_id, user_id, ticket_id, ride_id, scanned_at)
		 VALUES ($1, NULL, $2, $3, $4) ON CONFLICT (trace_id) DO NOTHING`,
		trace, voucherID, ride, scannedAt)
	if err != nil {
		return fmt.Errorf("insert dependent scan event: %w", err)
	}
	return nil
}

// dateScannedAt builds a stable scanned_at timestamp on the scenario date.
func dateScannedAt(date, ride string) time.Time {
	base := date + "T10:00:00Z"
	t, err := time.Parse(time.RFC3339, base)
	if err != nil {
		t = time.Now().UTC()
	}
	// Stagger by ride index so multi-ride ordering is stable.
	switch ride {
	case rideID(2):
		t = t.Add(10 * time.Minute)
	case rideID(3):
		t = t.Add(20 * time.Minute)
	case rideID(4):
		t = t.Add(30 * time.Minute)
	case rideID(5):
		t = t.Add(40 * time.Minute)
	}
	return t
}
