package minter

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Logiqode/ThemeParkNFT/internal/models"
)

type fakeParticipantLister struct{ ps []models.Participant }

func (f *fakeParticipantLister) ListParticipants(context.Context) ([]models.Participant, error) { return f.ps, nil }

type ridesInfo struct {
	rides []string
	ats   []time.Time
}

type fakeRideSource struct{ byID map[int64]ridesInfo }

func (f *fakeRideSource) RidesForParticipant(_ context.Context, p *models.Participant, _ string) ([]string, []time.Time, error) {
	info, ok := f.byID[p.ID]
	if !ok {
		return nil, nil, nil
	}
	return info.rides, info.ats, nil
}

type fakeWalletResolver struct {
	pending map[string][]string
}

func (f *fakeWalletResolver) ResolveMintWallet(p *models.Participant) models.MintResolution {
	switch p.WalletState {
	case models.ParticipantWalletCustodial:
		if p.CustodialWalletAddress != nil {
			return models.MintResolution{WalletState: models.ParticipantWalletCustodial, WalletAddress: *p.CustodialWalletAddress}
		}
		return models.MintResolution{WalletState: models.ParticipantWalletCustodial}
	case models.ParticipantWalletOwn:
		if p.CustodialWalletAddress != nil {
			return models.MintResolution{WalletState: models.ParticipantWalletOwn, WalletAddress: *p.CustodialWalletAddress}
		}
		return models.MintResolution{WalletState: models.ParticipantWalletOwn, Pending: true}
	default:
		return models.MintResolution{WalletState: models.ParticipantWalletNone, Pending: true}
	}
}

func (f *fakeWalletResolver) RecordPendingMint(_ context.Context, req models.RecordPendingMintRequest) error {
	f.pending[fmt.Sprintf("%d|%s", req.ParticipantID, req.MintDate)] = req.RideIDs
	return nil
}

func TestResolveDayWritesPendingForUnresolvedAdult(t *testing.T) {
	const date = "2026-08-03"
	wallets := &fakeWalletResolver{pending: map[string][]string{}}
	d := NewDayResolver(
		&fakeParticipantLister{ps: []models.Participant{
			{ID: 1, Name: "Adult", AccountEmail: ptr("adult@x.com"), WalletState: models.ParticipantWalletNone},
		}},
		&fakeRideSource{byID: map[int64]ridesInfo{
			1: {rides: []string{"r1", "r2"}, ats: []time.Time{time.Now()}},
		}},
		wallets,
	)

	res, err := d.ResolveDay(context.Background(), date)
	if err != nil {
		t.Fatalf("resolve day: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("expected 1 resolution, got %d", len(res))
	}
	if res[0].Outcome != OutcomePending {
		t.Fatalf("expected pending for unresolved adult, got %+v", res[0])
	}
	// Durable ledger written with the participant's ride ids (R32/M4.10).
	rides, ok := wallets.pending["1|"+date]
	if !ok {
		t.Fatal("expected pending_mints row recorded for participant 1")
	}
	if len(rides) != 2 || rides[0] != "r1" || rides[1] != "r2" {
		t.Fatalf("unexpected pending rides: %v", rides)
	}
}

func TestResolveDayDependentIsMintReady(t *testing.T) {
	const date = "2026-08-03"
	wallets := &fakeWalletResolver{pending: map[string][]string{}}
	cast := "0xguardian-custodial-9"
	d := NewDayResolver(
		&fakeParticipantLister{ps: []models.Participant{
			{ID: 9, Name: "Kid", GuardianID: &[]int64{1}[0], WalletState: models.ParticipantWalletCustodial, CustodialWalletAddress: &cast},
		}},
		&fakeRideSource{byID: map[int64]ridesInfo{
			9: {rides: []string{"r9"}},
		}},
		wallets,
	)

	res, err := d.ResolveDay(context.Background(), date)
	if err != nil {
		t.Fatalf("resolve day: %v", err)
	}
	if len(res) != 1 || res[0].Outcome != OutcomeMintReady {
		t.Fatalf("expected dependent to be mint_ready, got %+v", res)
	}
	if res[0].WalletAddress != cast {
		t.Fatalf("expected custodial wallet address, got %q", res[0].WalletAddress)
	}
	// Dependent is NOT pending → no pending_mints row (R31).
	if len(wallets.pending) != 0 {
		t.Fatalf("dependent must not write pending_mints, got %v", wallets.pending)
	}
}

func TestResolveDaySkipsParticipantsWithNoRides(t *testing.T) {
	d := NewDayResolver(
		&fakeParticipantLister{ps: []models.Participant{
			{ID: 1, Name: "A", AccountEmail: ptr("a@x.com")},
			{ID: 2, Name: "B", AccountEmail: ptr("b@x.com")},
		}},
		&fakeRideSource{byID: map[int64]ridesInfo{
			1: {rides: []string{"r1"}}, // 2 has no rides this day
		}},
		&fakeWalletResolver{pending: map[string][]string{}},
	)
	res, err := d.ResolveDay(context.Background(), "2026-08-03")
	if err != nil {
		t.Fatalf("resolve day: %v", err)
	}
	if len(res) != 1 || res[0].ParticipantID != 1 {
		t.Fatalf("expected only participant 1 to resolve, got %+v", res)
	}
}

func ptr(s string) *string { return &s }
