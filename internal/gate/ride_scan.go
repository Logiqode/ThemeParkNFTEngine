package gate

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"

	"github.com/Logiqode/ThemeParkNFT/internal/models"
	redisClient "github.com/Logiqode/ThemeParkNFT/internal/redis"
)

// ScanEventPublisher is the Kafka write boundary for the ride-scan path (D8).
// Defined as an interface so tests can assert the produced event without a real
// broker; internal/kafka.Producer satisfies it in production.
type ScanEventPublisher interface {
	PublishScanEvent(ctx context.Context, event *models.ScanEvent) error
}

// RideScanRequest is the payload for POST /api/rides/scan (R23): a wristband NFC
// scan at a ride during the visit.
type RideScanRequest struct {
	WristbandUID string `json:"wristband_uid" validate:"required"`
	RideID       string `json:"ride_id" validate:"required"`
}

// RideScanResponse reports the freshly-generated trace_id back to the caller so
// the simulated web flow can correlate the event end-to-end.
type RideScanResponse struct {
	TraceID string `json:"trace_id"`
	RideID  string `json:"ride_id"`
	Status  string `json:"status"`
}

// RideScanService converts a confirmed ride NFC scan into a ScanEvent published
// to the ride-scans topic. Only a wristband whose binding is BOUND (transaction
// check passed) may produce ride scans (R22 — scans never change binding state).
// On a successful scan the ticket is redeemed to USED (R13, M4.7).
type RideScanService struct {
	redis     *redisClient.Client
	publisher ScanEventPublisher
	db        *sqlx.DB
}

// NewRideScanService builds the ride-scan service.
func NewRideScanService(r *redisClient.Client, p ScanEventPublisher, db *sqlx.DB) *RideScanService {
	return &RideScanService{redis: r, publisher: p, db: db}
}

// Scan validates the wristband is BOUND, builds a fresh ScanEvent, and publishes
// it to Kafka. user_id is the internal email key (R11, never on-chain); ticket_id
// is carried for D6 (the consumer now persists it instead of an empty string).
func (s *RideScanService) Scan(ctx context.Context, req RideScanRequest) (*RideScanResponse, error) {
	b, err := s.redis.GetBinding(ctx, req.WristbandUID)
	if err != nil {
		return nil, fmt.Errorf("get binding: %w", err)
	}
	if b == nil {
		return nil, ErrNoBinding
	}
	if b.Status != models.BindingStatusBound {
		return nil, ErrNotBound
	}

	event := &models.ScanEvent{
		UserID:    b.UserEmail,
		RideID:    req.RideID,
		Timestamp: time.Now().UnixMilli(),
		TraceID:   uuid.NewString(), // fresh trace_id per ride scan
		TicketID:  b.TicketID,       // R23 / D6/D8
	}
	if err := s.publisher.PublishScanEvent(ctx, event); err != nil {
		return nil, fmt.Errorf("publish ride scan: %w", err)
	}

	// M4.7 / R13: a successful ride scan redeems the ticket (ACTIVE → USED).
	// Best-effort — Redis binding remains the source of truth for gate admission.
	_ = s.markTicketUsed(ctx, b.TicketID)

	return &RideScanResponse{TraceID: event.TraceID, RideID: req.RideID, Status: "recorded"}, nil
}

// markTicketUsed advances the ticket state machine to USED (R13) after a
// confirmed ride scan. Only a fully-activated ticket is redeemed; expired or
// already-used tickets are left untouched.
func (s *RideScanService) markTicketUsed(ctx context.Context, ticketID string) error {
	if s.db == nil {
		return nil
	}
	_, err := s.db.ExecContext(ctx,
		`UPDATE tickets SET status=$1, updated_at=now()
		  WHERE ticket_id=$2 AND status='ACTIVE'`, models.StatusUsed, ticketID)
	if err != nil {
		return fmt.Errorf("mark ticket used: %w", err)
	}
	return nil
}
