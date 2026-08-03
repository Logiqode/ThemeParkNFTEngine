package models

import (
	"errors"
	"time"
)

// ErrAlreadyAllocated is returned when a voucher is already delegated/claimed.
// Defined here (shared) so both the postgres repository (source of the
// condition) and the voucher service (domain mapping) can reference it without
// a cross-package dependency.
var ErrAlreadyAllocated = errors.New("voucher already allocated")

type ScanEvent struct {
	UserID    string `json:"user_id"    validate:"required"`
	RideID    string `json:"ride_id"    validate:"required"`
	Timestamp int64  `json:"timestamp"  validate:"required"`
	TraceID   string `json:"trace_id"   validate:"required"`
	// TicketID is the optional ticket that drove the ride scan (R23/D6). It is
	// backward-compatible (omitempty) so old producers/consumers still round-trip.
	TicketID string `json:"ticket_id,omitempty"`
}

// BindingStatus is the state of a wristband↔ticket binding (R19/R13), stored in
// Redis (ephemeral, TTL = end of day+1). See Rev 1 ticket state machine.
type BindingStatus string

const (
	// BindingStatusBinding = ticket PENDING_ENTRY ("BINDING"), first staff scan done.
	BindingStatusBinding BindingStatus = "BINDING"
	// BindingStatusBound = ticket ACTIVE ("BOUND"), NFC transaction check passed.
	BindingStatusBound BindingStatus = "BOUND"
)

// WristbandBinding is the Redis-resident temporary link between a wristband NFC
// id, a ticket, and the visitor account (email, R11). Stored under
// `bind:wristband:{uid}` → value + reverse `bind:ticket:{ticket_id}` → uid.
type WristbandBinding struct {
	WristbandUID string        `json:"wristband_uid"`
	TicketID     string        `json:"ticket_id"`
	UserEmail    string        `json:"user_email"`
	Status       BindingStatus `json:"status"`
	BoundAt      int64         `json:"bound_at"`
}

type TicketStatus string

const (
	StatusUnclaimed    TicketStatus = "UNCLAIMED"
	StatusClaimed      TicketStatus = "CLAIMED"
	StatusActive       TicketStatus = "ACTIVE"
	StatusPendingEntry TicketStatus = "PENDING_ENTRY"
	StatusUsed         TicketStatus = "USED"
	StatusExpired      TicketStatus = "EXPIRED"
)

type MintStatus string

const (
	MintPending   MintStatus = "PENDING"
	MintSubmitted MintStatus = "SUBMITTED"
	MintConfirmed MintStatus = "CONFIRMED"
	MintFailed    MintStatus = "FAILED"
)

type User struct {
	ID           int64     `db:"id"`
	Email        string    `db:"email"`
	SuiAddress   *string   `db:"sui_address"`
	EphemeralKey *string   `db:"ephemeral_key"`
	ZkloginProof *string   `db:"zklogin_proof"`
	CreatedAt    time.Time `db:"created_at"`
	UpdatedAt    time.Time `db:"updated_at"`
}

type Ticket struct {
	ID          int64        `db:"id"`
	TicketID    string       `db:"ticket_id"`
	UserID      *int64       `db:"user_id"`
	Status      TicketStatus `db:"status"`
	PurchasedBy *int64       `db:"purchased_by"`
	CreatedAt   time.Time    `db:"created_at"`
	UpdatedAt   time.Time    `db:"updated_at"`
}

type TicketVoucher struct {
	ID          int64        `db:"id"`
	VoucherID   string       `db:"voucher_id"`
	PurchaserID int64        `db:"purchaser_id"`
	OwnerID     *int64       `db:"owner_id"`
	// ParticipantID links this voucher to the participant it is delegated to
	// (R27/R28). NULL until the buyer allocates it (account or dependent mode).
	ParticipantID *int64      `db:"participant_id"`
	Status        TicketStatus `db:"status"`
	ClaimToken    *string      `db:"claim_token"`
	ExpiresAt     *time.Time   `db:"expires_at"`
	CreatedAt     time.Time    `db:"created_at"`
	UpdatedAt     time.Time    `db:"updated_at"`
}

type Ride struct {
	ID        int64     `db:"id"`
	RideID    string    `db:"ride_id"`
	Name      string    `db:"name"`
	CreatedAt time.Time `db:"created_at"`
}

type ScanRecord struct {
	ID        int64     `db:"id"`
	TraceID   string    `db:"trace_id"`
	UserID    *int64    `db:"user_id"`
	TicketID  *string   `db:"ticket_id"`
	RideID    string    `db:"ride_id"`
	ScannedAt time.Time `db:"scanned_at"`
	CreatedAt time.Time `db:"created_at"`
}

type MintLog struct {
	ID        int64      `db:"id"`
	UserID    int64      `db:"user_id"`
	RideID    string     `db:"ride_id"`
	MintDate  string     `db:"mint_date"`
	TxDigest  *string    `db:"tx_digest"`
	Status    MintStatus `db:"status"`
	GasUsed   *int64     `db:"gas_used"`
	Error     *string    `db:"error"`
	CreatedAt time.Time  `db:"created_at"`
	UpdatedAt time.Time  `db:"updated_at"`
}

type GateVerifyRequest struct {
	TicketID string `json:"ticket_id" validate:"required"`
	RideID   string `json:"ride_id"   validate:"required"`
	TraceID  string `json:"trace_id"  validate:"required"`
}

type GateVerifyResponse struct {
	Allowed   bool   `json:"allowed"`
	Reason    string `json:"reason,omitempty"`
	QRPayload string `json:"qr_payload,omitempty"`
	UserID    string `json:"user_id,omitempty"`
}

type QRTokenResponse struct {
	Payload   string `json:"payload"`
	ExpiresAt int64  `json:"expires_at"`
}

type PurchaseRequest struct {
	PurchaserID string `json:"purchaser_id" validate:"required"`
	Quantity    int    `json:"quantity"     validate:"required,min=1,max=50"`
}

type PurchaseResponse struct {
	VoucherIDs []string `json:"voucher_ids"`
}

type ShareRequest struct {
	VoucherID string `json:"voucher_id" validate:"required"`
	Recipient string `json:"recipient"  validate:"required,email"`
}

type ShareResponse struct {
	MagicLink string `json:"magic_link"`
}

type ClaimResponse struct {
	Status   string `json:"status"`
	UserID   string `json:"user_id,omitempty"`
	TicketID string `json:"ticket_id,omitempty"`
}

type MintDailyRequest struct {
	UserID string `json:"user_id" validate:"required"`
	Date   string `json:"date"    validate:"required"`
}

type MintDailyResponse struct {
	TxDigest string   `json:"tx_digest,omitempty"`
	RideIDs  []string `json:"ride_ids"`
	Status   string   `json:"status"`
}

type GoogleAuthRequest struct {
	Token string `json:"token" validate:"required"`
}

type AuthResponse struct {
	UserID     string `json:"user_id"`
	SuiAddress string `json:"sui_address"`
}

// ---- Rev 3: Family Voucher & Participant Model (R26-R35) ----

// ParticipantWalletState is how a participant's attendance NFT resolves to a
// wallet (R30/R31/R35).
type ParticipantWalletState string

const (
	// ParticipantWalletNone — no wallet attached yet (adult not onboarded);
	// mints resolve to a durable pending_mints row (R32).
	ParticipantWalletNone ParticipantWalletState = "NONE"
	// ParticipantWalletOwn — participant's own non-custodial zkLogin wallet.
	ParticipantWalletOwn ParticipantWalletState = "OWN_NON_CUSTODIAL"
	// ParticipantWalletCustodial — custodial-proxy wallet held for a dependent
	// (age/<usable-account> edge cases, R28). Dedicated per guardian (R35).
	ParticipantWalletCustodial ParticipantWalletState = "CUSTODIAL_PROXY"
)

// Participant is a person (R26), distinct from an account/wallet. The gate's
// wristband bind targets a participant. Dependents have a GuardianID (self-ref
// to the family head) and a custodial-proxy wallet (R35).
type Participant struct {
	ID                     int64                  `db:"id"`
	Name                   string                 `db:"name"`
	AccountEmail           *string                `db:"account_email"`
	GuardianID             *int64                 `db:"guardian_id"`
	WalletState            ParticipantWalletState `db:"wallet_state"`
	CustodialWalletAddress *string                `db:"custodial_wallet_address"`
	KeysEnc                *string                `db:"keys_enc"`
	CreatedAt              time.Time              `db:"created_at"`
	UpdatedAt              time.Time              `db:"updated_at"`
}

// PendingMint is a durable attribution-ledger row (R32), keyed to a participant
// and mint date. Never tied to Redis TTL / wristband lifetime; rebuildable from
// scan_events. Kept by default, deletable on request (GDPR/R34).
type PendingMint struct {
	ID            int64                  `db:"id"`
	ParticipantID int64                  `db:"participant_id"`
	RideIDs       []string               `db:"ride_ids"`   // jsonb
	ScannedAts    []time.Time            `db:"scanned_ats"` // jsonb (RFC3339)
	MintDate      string                 `db:"mint_date"`   // DATE (YYYY-MM-DD)
	WalletState   ParticipantWalletState `db:"wallet_state"`
	CreatedAt     time.Time              `db:"created_at"`
}

// DelegationMode selects how a voucher is allocated to a participant (R28).
type DelegationMode string

const (
	// DelegationAccount links the voucher to an email with a Google zkLogin
	// account → own non-custodial wallet (eventually-linked, R30).
	DelegationAccount DelegationMode = "account"
	// DelegationDependent allocates to a person with no account (child/infant/
	// elderly) under a guardian → custodial-proxy wallet (R28/R35).
	DelegationDependent DelegationMode = "dependent"
)

// DelegateRequest is POST /api/vouchers/delegate (R27/R28).
type DelegateRequest struct {
	VoucherID string         `json:"voucher_id" validate:"required"`
	Mode      DelegationMode `json:"mode" validate:"required,oneof=account dependent"`
	// Account mode: the email to link (own non-custodial, eventually-linked).
	AccountEmail string `json:"account_email" validate:"omitempty,email"`
	// Dependent mode: the dependent's name + the guardian participant id.
	Name       string `json:"name"`
	GuardianID int64  `json:"guardian_id"`
}

// DelegateResponse returns the participant the voucher was allocated to.
type DelegateResponse struct {
	ParticipantID int64                  `json:"participant_id"`
	VoucherID     string                 `json:"voucher_id"`
	Mode          DelegationMode         `json:"mode"`
	WalletState   ParticipantWalletState `json:"wallet_state"`
	WalletAddress string                 `json:"wallet_address,omitempty"`
	Pending       bool                   `json:"pending"`
}

// MintResolution is the result of resolving a participant's wallet at mint time
// (R30/R31).
type MintResolution struct {
	WalletState   ParticipantWalletState `json:"wallet_state"`
	WalletAddress string                 `json:"wallet_address,omitempty"`
	// Pending is true when the participant has no wallet yet → caller must
	// persist a durable pending_mints row (R32) instead of minting.
	Pending bool `json:"pending"`
}

// RecordPendingMintRequest is the payload for writing a durable pending_mints
// row (R32) for a participant's ride set on a given date.
type RecordPendingMintRequest struct {
	ParticipantID int64       `json:"participant_id" validate:"required"`
	MintDate      string      `json:"mint_date" validate:"required"`
	RideIDs       []string    `json:"ride_ids"`
	ScannedAts    []time.Time `json:"scanned_ats"`
}
