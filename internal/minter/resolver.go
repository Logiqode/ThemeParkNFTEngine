// Package minter implements the off-chain end-of-day mint-resolution pass
// (Week 4, M4.12, R30/R31/R32). It iterates participants with rides for a date,
// resolves each participant's wallet, writes a durable `pending_mints` row for
// unresolved adults, and reports participants that are mint-ready.
//
// IMPORTANT: this package performs NO on-chain activity. It resolves wallets
// and persists the durable attribution ledger only; actual Sui tx submission
// (mints, custody transfers) is Week 6.
package minter

import (
	"context"
	"fmt"
	"time"

	"github.com/Logiqode/ThemeParkNFT/internal/models"
)

// ParticipantLister provides the participant set for a resolution pass.
type ParticipantLister interface {
	ListParticipants(ctx context.Context) ([]models.Participant, error)
}

// RideSource returns the distinct rides a participant took on a given date
// (durable source = scan_events) plus their scan times.
type RideSource interface {
	RidesForParticipant(ctx context.Context, p *models.Participant, date string) (rides []string, scannedAts []time.Time, err error)
}

// WalletResolver resolves a participant's wallet and can persist a durable
// pending_mints row. Satisfied by *voucher.Service.
type WalletResolver interface {
	ResolveMintWallet(p *models.Participant) models.MintResolution
	RecordPendingMint(ctx context.Context, req models.RecordPendingMintRequest) error
}

// DayResolver runs a single end-of-day resolution pass for one mint date.
type DayResolver struct {
	participants ParticipantLister
	rides        RideSource
	wallets      WalletResolver
}

// NewDayResolver builds the off-chain minter day resolver.
func NewDayResolver(participants ParticipantLister, rides RideSource, wallets WalletResolver) *DayResolver {
	return &DayResolver{participants: participants, rides: rides, wallets: wallets}
}

// Resolution is the result of resolving one participant's mint for a date.
type Resolution struct {
	ParticipantID int64                       `json:"participant_id"`
	MintDate      string                      `json:"mint_date"`
	RideIDs       []string                    `json:"ride_ids"`
	WalletState   models.ParticipantWalletState `json:"wallet_state"`
	WalletAddress string                      `json:"wallet_address,omitempty"`
	// Outcome is "pending" (durable pending_mints row written — no wallet yet)
	// or "mint_ready" (wallet resolved; tx submitted in Week 6).
	Outcome string `json:"outcome"`
}

// Outcome enum.
const (
	OutcomePending   = "pending"   // no wallet → durable pending_mints written (R32)
	OutcomeMintReady = "mint_ready" // wallet resolved → mint in Week 6
)

// ResolveDay processes every participant with rides on `date` (YYYY-MM-DD):
// resolve wallet; if pending, write/refresh the durable pending_mints ledger row.
// Idempotent — re-running a date only refreshes ledger rows, never mints.
func (d *DayResolver) ResolveDay(ctx context.Context, date string) ([]Resolution, error) {
	ps, err := d.participants.ListParticipants(ctx)
	if err != nil {
		return nil, fmt.Errorf("list participants: %w", err)
	}

	resolutions := make([]Resolution, 0, len(ps))
	for i := range ps {
		p := &ps[i]
		rides, ats, err := d.rides.RidesForParticipant(ctx, p, date)
		if err != nil {
			return resolutions, fmt.Errorf("rides for participant %d: %w", p.ID, err)
		}
		if len(rides) == 0 {
			continue // no attendance that day
		}

		res := d.wallets.ResolveMintWallet(p)
		outcome := OutcomeMintReady
		if res.Pending {
			// Unresolved adult → durable attribution ledger (R32). Actual mint
			// happens later once a wallet is attached (Week 6).
			if err := d.wallets.RecordPendingMint(ctx, models.RecordPendingMintRequest{
				ParticipantID: p.ID,
				MintDate:      date,
				RideIDs:       rides,
				ScannedAts:    ats,
			}); err != nil {
				return resolutions, fmt.Errorf("record pending mint for participant %d: %w", p.ID, err)
			}
			outcome = OutcomePending
		}

		resolutions = append(resolutions, Resolution{
			ParticipantID: p.ID,
			MintDate:      date,
			RideIDs:       rides,
			WalletState:   res.WalletState,
			WalletAddress: res.WalletAddress,
			Outcome:       outcome,
		})
	}
	return resolutions, nil
}
