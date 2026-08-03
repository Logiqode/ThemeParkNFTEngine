package voucher

import (
	"context"
	"errors"
	"testing"

	"github.com/Logiqode/ThemeParkNFT/internal/models"
)

// fakeRepo is an in-memory implementation of Repo for testing the service's
// domain logic without Postgres.
type fakeRepo struct {
	participants map[int64]*models.Participant
	nextID       int64
	allocated    map[string]bool // voucherID -> delegated
	pending      map[string]*models.PendingMint
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		participants: map[int64]*models.Participant{},
		nextID:       1,
		allocated:    map[string]bool{},
		pending:      map[string]*models.PendingMint{},
	}
}

func (f *fakeRepo) GetParticipant(_ context.Context, id int64) (*models.Participant, error) {
	return f.participants[id], nil
}

func (f *fakeRepo) GetParticipantByEmail(_ context.Context, email string) (*models.Participant, error) {
	for _, p := range f.participants {
		if p.AccountEmail != nil && *p.AccountEmail == email {
			return p, nil
		}
	}
	return nil, nil
}

func (f *fakeRepo) CreateParticipant(_ context.Context, name string, accountEmail *string, guardianID *int64) (*models.Participant, error) {
	p := &models.Participant{ID: f.nextID, Name: name, AccountEmail: accountEmail, GuardianID: guardianID, WalletState: models.ParticipantWalletNone}
	f.nextID++
	f.participants[p.ID] = p
	return p, nil
}

func (f *fakeRepo) SetParticipantWallet(_ context.Context, id int64, state models.ParticipantWalletState, addr string) error {
	p := f.participants[id]
	if p == nil {
		return models.ErrAlreadyAllocated // arbitrary; not hit in valid flows
	}
	p.WalletState = state
	p.CustodialWalletAddress = &addr
	return nil
}

func (f *fakeRepo) DelegateVoucher(_ context.Context, voucherID string, participantID int64) error {
	if f.allocated[voucherID] {
		return models.ErrAlreadyAllocated
	}
	f.allocated[voucherID] = true
	return nil
}

func (f *fakeRepo) UpsertPendingMint(_ context.Context, pm models.PendingMint) error {
	f.pending[pmKey(pm.ParticipantID, pm.MintDate)] = &pm
	return nil
}

func (f *fakeRepo) GetPendingMint(_ context.Context, participantID int64, mintDate string) (*models.PendingMint, error) {
	return f.pending[pmKey(participantID, mintDate)], nil
}

func pmKey(pid int64, date string) string { return string(rune(pid)) + "|" + date }

func TestDelegateAccountCreatesParticipantAndResolvesPending(t *testing.T) {
	s := NewService(newFakeRepo())
	res, err := s.Delegate(context.Background(), models.DelegateRequest{
		VoucherID:    "v1",
		Mode:         models.DelegationAccount,
		AccountEmail: "wife@example.com",
	})
	if err != nil {
		t.Fatalf("delegate: %v", err)
	}
	if res.ParticipantID == 0 {
		t.Fatal("expected a participant id")
	}
	// Account-linked adults have no wallet until they onboard (R30) → pending.
	if !res.Pending {
		t.Fatalf("expected pending for account without wallet, got %+v", res)
	}
	if res.WalletState != models.ParticipantWalletNone {
		t.Fatalf("expected NONE wallet state, got %s", res.WalletState)
	}
}

func TestDelegateAccountReusesExistingParticipant(t *testing.T) {
	repo := newFakeRepo()
	email := "dad@example.com"
	own := "0xdad-own"
	// Pre-create the dad participant (the family head) with an own wallet.
	dad := &models.Participant{ID: 100, Name: "Dad", AccountEmail: &email, WalletState: models.ParticipantWalletOwn, CustodialWalletAddress: &own}
	repo.participants[100] = dad
	s := NewService(repo)

	res, err := s.Delegate(context.Background(), models.DelegateRequest{
		VoucherID: "v5", Mode: models.DelegationAccount, AccountEmail: email,
	})
	if err != nil {
		t.Fatalf("delegate: %v", err)
	}
	if res.ParticipantID != 100 {
		t.Fatalf("expected existing participant 100 reused, got %d", res.ParticipantID)
	}
	if res.Pending {
		t.Fatal("expected non-pending when OWN wallet attached")
	}
}

func TestDelegateDependentMintsIntoCustodialImmediately(t *testing.T) {
	repo := newFakeRepo()
	// Guardian (family head) exists.
	repo.participants[1] = &models.Participant{ID: 1, Name: "Dad", WalletState: models.ParticipantWalletOwn}
	s := NewService(repo)

	res, err := s.Delegate(context.Background(), models.DelegateRequest{
		VoucherID: "v-kid", Mode: models.DelegationDependent, Name: "Kid", GuardianID: 1,
	})
	if err != nil {
		t.Fatalf("delegate dependent: %v", err)
	}
	// Dependent has a custodial wallet → mints immediately, never pending (R31).
	if res.Pending {
		t.Fatal("dependent must not be pending — R31 mints into custodial wallet")
	}
	if res.WalletState != models.ParticipantWalletCustodial {
		t.Fatalf("expected CUSTODIAL_PROXY, got %s", res.WalletState)
	}
	if res.WalletAddress == "" {
		t.Fatal("dependent should have a custodial wallet address (R35)")
	}
}

func TestDelegateValidationErrors(t *testing.T) {
	repo := newFakeRepo()
	repo.participants[1] = &models.Participant{ID: 1, Name: "Dad"}
	s := NewService(repo)

	cases := []struct {
		name string
		req  models.DelegateRequest
	}{
		{"account without email", models.DelegateRequest{VoucherID: "v", Mode: models.DelegationAccount}},
		{"dependent without guardian", models.DelegateRequest{VoucherID: "v", Mode: models.DelegationDependent, Name: "Kid"}},
		{"dependent unknown guardian", models.DelegateRequest{VoucherID: "v", Mode: models.DelegationDependent, Name: "Kid", GuardianID: 999}},
		{"unknown mode", models.DelegateRequest{VoucherID: "v", Mode: "bogus", AccountEmail: "x@y.z"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := s.Delegate(context.Background(), tc.req); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestDelegateAlreadyAllocated(t *testing.T) {
	repo := newFakeRepo()
	repo.allocated["v-used"] = true
	s := NewService(repo)
	_, err := s.Delegate(context.Background(), models.DelegateRequest{
		VoucherID: "v-used", Mode: models.DelegationAccount, AccountEmail: "a@b.c",
	})
	if !errors.Is(err, ErrVoucherAllocated) {
		t.Fatalf("expected ErrVoucherAllocated, got %v", err)
	}
}

func TestResolveMintWalletMatrix(t *testing.T) {
	s := NewService(newFakeRepo())
	own := "0xown"
	cast := "0xguardian-custodial-7"
	cases := []struct {
		name    string
		p       *models.Participant
		want    models.ParticipantWalletState
		wantAcc string
		pending bool
	}{
		{"account no wallet → pending", &models.Participant{WalletState: models.ParticipantWalletNone}, models.ParticipantWalletNone, "", true},
		{"own wallet attached → mint to own", &models.Participant{WalletState: models.ParticipantWalletOwn, CustodialWalletAddress: &own}, models.ParticipantWalletOwn, own, false},
		{"own state but no address → pending", &models.Participant{WalletState: models.ParticipantWalletOwn}, models.ParticipantWalletOwn, "", true},
		{"dependent → mint to custodial", &models.Participant{WalletState: models.ParticipantWalletCustodial, CustodialWalletAddress: &cast}, models.ParticipantWalletCustodial, cast, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := s.ResolveMintWallet(tc.p)
			if got.WalletState != tc.want || got.WalletAddress != tc.wantAcc || got.Pending != tc.pending {
				t.Fatalf("got %+v, want state=%s addr=%s pending=%v", got, tc.want, tc.wantAcc, tc.pending)
			}
		})
	}
}

func TestRecordPendingMintPersistsDurableRow(t *testing.T) {
	repo := newFakeRepo()
	repo.participants[7] = &models.Participant{ID: 7, Name: "Adult", WalletState: models.ParticipantWalletNone}
	s := NewService(repo)
	if err := s.RecordPendingMint(context.Background(), models.RecordPendingMintRequest{
		ParticipantID: 7, MintDate: "2026-08-03", RideIDs: []string{"r1", "r2"},
	}); err != nil {
		t.Fatalf("record pending: %v", err)
	}
	pm, err := s.GetPendingMint(context.Background(), 7, "2026-08-03")
	if err != nil || pm == nil {
		t.Fatalf("expected durable pending row, got %v err=%v", pm, err)
	}
	if len(pm.RideIDs) != 2 {
		t.Fatalf("expected 2 ride ids, got %v", pm.RideIDs)
	}
}

func TestClaimCustodyFlipsDependentToOwn(t *testing.T) {
	repo := newFakeRepo()
	cast := "0xguardian-custodial-9"
	repo.participants[9] = &models.Participant{
		ID: 9, Name: "Kid", WalletState: models.ParticipantWalletCustodial, CustodialWalletAddress: &cast,
	}
	s := NewService(repo)
	p, err := s.ClaimCustody(context.Background(), 9, "0xkid-own")
	if err != nil {
		t.Fatalf("claim custody: %v", err)
	}
	if p.WalletState != models.ParticipantWalletOwn {
		t.Fatalf("expected OWN after claim custody, got %s", p.WalletState)
	}
	if p.CustodialWalletAddress == nil || *p.CustodialWalletAddress != "0xkid-own" {
		t.Fatalf("expected own wallet recorded, got %v", p.CustodialWalletAddress)
	}
}
