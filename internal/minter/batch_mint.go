package minter

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"

	"github.com/Logiqode/ThemeParkNFT/internal/sui"
)

// BatchMint executes the on-chain half of Week 6 (M6.2/M6.3/M6.6): for a set of
// mint-ready resolutions it submits batch mints to the resolved recipient
// wallets (own non-custodial OR guardian custodial wallet for dependents) and
// records idempotent mint_logs. It depends on a Minter + a MetadataProvider so
// it is fully testable against a fake Sui client (no testnet in unit tests).
type BatchMint struct {
	client   sui.Minter
	metadata MetadataProvider
	records  MintRecorder
}

// MetadataProvider supplies per-ride name + metadata URL for a mint (backed by
// storage.CIDCache in production; a stub in tests).
type MetadataProvider interface {
	GetOrPin(ctx context.Context, rideID, date string) (MetadataAssets, error)
}

// MetadataAssets is the pinned per-ride metadata (Option A, CIDCache).
type MetadataAssets struct {
	MetadataURI string
}

// MintRecorder persists a confirmed mint_logs row (idempotent).
type MintRecorder interface {
	RecordMintForParticipant(ctx context.Context, participantID int64, rideID, date, txDigest string) error
}

// NewBatchMint builds the batch-mint executor.
func NewBatchMint(client sui.Minter, metadata MetadataProvider, records MintRecorder) *BatchMint {
	return &BatchMint{client: client, metadata: metadata, records: records}
}

// MintResolutionResult is the per-participant outcome of a run.
type MintResolutionResult struct {
	ParticipantID int64    `json:"participant_id"`
	WalletAddress string   `json:"wallet_address"`
	RideIDs       []string `json:"ride_ids"`
	TxDigest      string   `json:"tx_digest"`
	Minted        bool     `json:"minted"`
	Error         string   `json:"error,omitempty"`
}

// MintLive is an input to the batch mint: a resolved mint-ready participant.
type MintLive struct {
	ParticipantID int64
	WalletAddress string // resolved recipient (own or guardian custodial)
	RideIDs       []string
	Date          string // YYYY-MM-DD
}

// Run mints every provided live participant in a single pass. It is idempotent
// at the mint_logs level (M6.3): re-running the same participant+date returns
// the prior digest (via ON CONFLICT) and issues no duplicate tx. Errors are
// per-participant; a failure on one does not abort the rest.
func (b *BatchMint) Run(ctx context.Context, lives []MintLive) []MintResolutionResult {
	results := make([]MintResolutionResult, 0, len(lives))
	for _, l := range lives {
		res := b.mintOne(ctx, l)
		results = append(results, res)
	}
	return results
}

func (b *BatchMint) mintOne(ctx context.Context, l MintLive) MintResolutionResult {
	res := MintResolutionResult{
		ParticipantID: l.ParticipantID,
		WalletAddress: l.WalletAddress,
		RideIDs:       l.RideIDs,
	}

	names := make([]string, len(l.RideIDs))
	urls := make([]string, len(l.RideIDs))
	for i, rideID := range l.RideIDs {
		assets, err := b.metadata.GetOrPin(ctx, rideID, l.Date)
		if err != nil {
			msg := fmt.Sprintf("pin metadata %s: %v", rideID, err)
			res.Error = msg
			return res
		}
		names[i] = rideName(rideID)
		urls[i] = assets.MetadataURI
	}

	digest, err := b.client.MintBatchAttendance(ctx, l.WalletAddress, l.RideIDs, l.Date, names, urls)
	if err != nil {
		res.Error = fmt.Sprintf("mint: %v", err)
		log.Error().Err(err).Int64("participant", l.ParticipantID).Msg("batch mint failed")
		return res
	}
	res.TxDigest = digest
	res.Minted = true

	// Idempotent attribution (M6.3): ON CONFLICT returns prior digest, no dup tx.
	for _, rideID := range l.RideIDs {
		if err := b.records.RecordMintForParticipant(ctx, l.ParticipantID, rideID, l.Date, digest); err != nil {
			log.Error().Err(err).Int64("participant", l.ParticipantID).Str("ride", rideID).Msg("record mint failed")
			res.Error = fmt.Sprintf("record mint %s: %v", rideID, err)
			return res
		}
	}
	return res
}

// rideName returns a readable display name for a ride id (fallback: the id).
func rideName(rideID string) string {
	if rideID == "" {
		return "Ride"
	}
	return "Ride " + rideID
}
