// Package voucher implements the Rev 3 family voucher & participant model
// (R26–R35): voucher delegation to participants (account-linked or dependent),
// mint-time wallet resolution, and the durable pending-mint attribution ledger.
package voucher

import (
	"context"
	"errors"
	"fmt"

	"github.com/Logiqode/ThemeParkNFT/internal/models"
)

var (
	// ErrVoucherAllocated is returned when a voucher is already delegated/claimed.
	ErrVoucherAllocated = errors.New("voucher already allocated")
	// ErrInvalidDelegation is returned when a delegation request is malformed
	// (e.g. account mode without an email, or dependent mode without a guardian).
	ErrInvalidDelegation = errors.New("invalid delegation")
	// ErrParticipantNotFound is returned when a referenced guardian/participant
	// does not exist.
	ErrParticipantNotFound = errors.New("participant not found")
)

// Repo is the persistence surface the voucher service depends on. Backed by
// *postgres.Repository in production; a stub in unit tests.
type Repo interface {
	GetParticipant(ctx context.Context, id int64) (*models.Participant, error)
	GetParticipantByEmail(ctx context.Context, email string) (*models.Participant, error)
	CreateParticipant(ctx context.Context, name string, accountEmail *string, guardianID *int64) (*models.Participant, error)
	SetParticipantWallet(ctx context.Context, id int64, state models.ParticipantWalletState, addr string) error
	DelegateVoucher(ctx context.Context, voucherID string, participantID int64) error
	UpsertPendingMint(ctx context.Context, pm models.PendingMint) error
	GetPendingMint(ctx context.Context, participantID int64, mintDate string) (*models.PendingMint, error)
}

// Service orchestrates Rev 3 delegate / mint-resolution / pending-ledger flows.
type Service struct {
	repo Repo
}

// NewService builds the voucher service.
func NewService(repo Repo) *Service { return &Service{repo: repo} }

// Delegate allocates a voucher to a participant (R27/R28). Two modes:
//
//	account:   link to an existing-or-new participant with an account email.
//	           Wallet stays NONE until the account attaches an own non-custodial
//	           wallet later (eventually-linked, R30).
//	dependent: create a participant under a guardian with a custodial-proxy
//	           wallet (R35) so its NFT mints immediately into the family wallet.
//
// It returns the participant the voucher was allocated to and whether minting
// for that participant currently resolves to a durable pending (R32).
func (s *Service) Delegate(ctx context.Context, req models.DelegateRequest) (*models.DelegateResponse, error) {
	var (
		p   *models.Participant
		err error
	)
	switch req.Mode {
	case models.DelegationAccount:
		if req.AccountEmail == "" {
			return nil, fmt.Errorf("%w: account_email required for account mode", ErrInvalidDelegation)
		}
		p, err = s.repo.GetParticipantByEmail(ctx, req.AccountEmail)
		if err != nil {
			return nil, err
		}
		if p == nil {
			email := req.AccountEmail
			p, err = s.repo.CreateParticipant(ctx, req.AccountEmail, &email, nil)
			if err != nil {
				return nil, err
			}
		}
	case models.DelegationDependent:
		if req.GuardianID == 0 {
			return nil, fmt.Errorf("%w: guardian_id required for dependent mode", ErrInvalidDelegation)
		}
		guardian, err := s.repo.GetParticipant(ctx, req.GuardianID)
		if err != nil {
			return nil, err
		}
		if guardian == nil {
			return nil, fmt.Errorf("%w: guardian %d", ErrParticipantNotFound, req.GuardianID)
		}
		p, err = s.repo.CreateParticipant(ctx, req.Name, nil, &req.GuardianID)
		if err != nil {
			return nil, err
		}
		// Provision a dedicated custodial-proxy wallet for the dependent (R35).
		addr := custodialWalletFor(p.ID)
		if err := s.repo.SetParticipantWallet(ctx, p.ID, models.ParticipantWalletCustodial, addr); err != nil {
			return nil, err
		}
		p.CustodialWalletAddress = &addr
		p.WalletState = models.ParticipantWalletCustodial
	default:
		return nil, fmt.Errorf("%w: unknown mode %q", ErrInvalidDelegation, req.Mode)
	}

	if err := s.repo.DelegateVoucher(ctx, req.VoucherID, p.ID); err != nil {
		if errors.Is(err, models.ErrAlreadyAllocated) {
			return nil, ErrVoucherAllocated
		}
		return nil, err
	}

	res := s.ResolveMintWallet(p)
	return &models.DelegateResponse{
		ParticipantID: p.ID,
		VoucherID:     req.VoucherID,
		Mode:          req.Mode,
		WalletState:   res.WalletState,
		WalletAddress: res.WalletAddress,
		Pending:       res.Pending,
	}, nil
}

// ResolveMintWallet implements R30/R31 with no I/O: it inspects the
// participant's wallet state.
//
//	OWN_NON_CUSTODIAL + address  → mint to that wallet (not pending)
//	CUSTODIAL_PROXY + address    → dependent mint into custodial wallet (not
//	                               pending — R31: dependents never sit in a
//	                               forever-pending state)
//	otherwise (NONE, account w/o wallet) → durable pending (R32)
func (s *Service) ResolveMintWallet(p *models.Participant) models.MintResolution {
	switch p.WalletState {
	case models.ParticipantWalletOwn:
		if p.CustodialWalletAddress != nil && *p.CustodialWalletAddress != "" {
			return models.MintResolution{WalletState: models.ParticipantWalletOwn, WalletAddress: *p.CustodialWalletAddress, Pending: false}
		}
		// Own wallet state but no address yet — treat as pending (R30 case 3).
		return models.MintResolution{WalletState: models.ParticipantWalletOwn, Pending: true}
	case models.ParticipantWalletCustodial:
		if p.CustodialWalletAddress != nil && *p.CustodialWalletAddress != "" {
			return models.MintResolution{WalletState: models.ParticipantWalletCustodial, WalletAddress: *p.CustodialWalletAddress, Pending: false}
		}
		// Dependent with no wallet provisioned (shouldn't happen at delegation)
		// still resolves non-pending but with no address → service-layer error.
		return models.MintResolution{WalletState: models.ParticipantWalletCustodial, Pending: false}
	default: // NONE
		return models.MintResolution{WalletState: models.ParticipantWalletNone, Pending: true}
	}
}

// RecordPendingMint durably persists a participant's ride set for a mint date
// into pending_mints (R32). Idempotent per (participant, mint_date). The row is
// the attribution ledger and outlives Redis/wristband lifetimes.
func (s *Service) RecordPendingMint(ctx context.Context, req models.RecordPendingMintRequest) error {
	p, err := s.repo.GetParticipant(ctx, req.ParticipantID)
	if err != nil {
		return err
	}
	if p == nil {
		return fmt.Errorf("%w: %d", ErrParticipantNotFound, req.ParticipantID)
	}
	res := s.ResolveMintWallet(p)
	pm := models.PendingMint{
		ParticipantID: req.ParticipantID,
		RideIDs:       req.RideIDs,
		ScannedAts:    req.ScannedAts,
		MintDate:      req.MintDate,
		WalletState:   res.WalletState,
	}
	return s.repo.UpsertPendingMint(ctx, pm)
}

// GetPendingMint returns the durable attribution row for a participant/date.
func (s *Service) GetPendingMint(ctx context.Context, participantID int64, mintDate string) (*models.PendingMint, error) {
	return s.repo.GetPendingMint(ctx, participantID, mintDate)
}

// ClaimCustody implements the off-chain half of R33: once a dependent links
// their own account + non-custodial wallet (Week 6 does the real Sui object
// transfer), flip the participant to OWN_NON_CUSTODIAL and record the new
// wallet. On-chain NFT object transfer is stubbed (Week 6).
func (s *Service) ClaimCustody(ctx context.Context, participantID int64, ownWallet string) (*models.Participant, error) {
	p, err := s.repo.GetParticipant(ctx, participantID)
	if err != nil {
		return nil, err
	}
	if p == nil {
		return nil, fmt.Errorf("%w: %d", ErrParticipantNotFound, participantID)
	}
	if p.WalletState != models.ParticipantWalletCustodial {
		return nil, fmt.Errorf("participant %d is not a dependent (state=%s)", participantID, p.WalletState)
	}
	if err := s.repo.SetParticipantWallet(ctx, participantID, models.ParticipantWalletOwn, ownWallet); err != nil {
		return nil, err
	}
	return s.repo.GetParticipant(ctx, participantID)
}

// custodialWalletFor derives a deterministic placeholder dedicated custodial
// wallet address for a dependent (R35). Week 6 replaces this with real key
// generation + server-side encryption; the format is kept stable here so the
// data model and resolution logic are fully exercisable now.
func custodialWalletFor(participantID int64) string {
	return fmt.Sprintf("0xguardian-custodial-%d", participantID)
}
