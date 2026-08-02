package models

import "time"

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
	Status      TicketStatus `db:"status"`
	ClaimToken  *string      `db:"claim_token"`
	ExpiresAt   *time.Time   `db:"expires_at"`
	CreatedAt   time.Time    `db:"created_at"`
	UpdatedAt   time.Time    `db:"updated_at"`
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
