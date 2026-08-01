// Package pipeline contains the reusable Kafka consumer message handler:
// dedup (Redis) → persist (Postgres) → aggregate (Redis). Extracted from
// cmd/consumer so integration tests exercise the exact production logic.
package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"

	"github.com/Logiqode/ThemeParkNFT/internal/models"
	redisClient "github.com/Logiqode/ThemeParkNFT/internal/redis"
)

// ScanHandler implements internal/kafka.MessageHandler: deduplicate by trace_id,
// persist to Postgres scan_events, and aggregate into the user's daily Redis set.
type ScanHandler struct {
	db    *sqlx.DB
	redis *redisClient.Client
}

// NewScanHandler builds the production scan-event handler shared by cmd/consumer
// and integration tests.
func NewScanHandler(db *sqlx.DB, redis *redisClient.Client) *ScanHandler {
	return &ScanHandler{db: db, redis: redis}
}

// Handle processes a single ScanEvent (R11: user_id is the internal email key).
func (h *ScanHandler) Handle(ctx context.Context, event *models.ScanEvent) error {
	// Deduplication shield.
	isDup, err := h.redis.IsDuplicate(ctx, event.TraceID)
	if err != nil {
		return err
	}
	if isDup {
		log.Debug().Str("trace_id", event.TraceID).Msg("duplicate dropped")
		return nil
	}

	// Persist to Postgres (idempotent via trace_id UNIQUE).
	_, err = h.db.ExecContext(ctx,
		`INSERT INTO scan_events (trace_id, user_id, ticket_id, ride_id, scanned_at)
		 VALUES ($1, (SELECT id FROM users WHERE email = $2 LIMIT 1), $3, $4, $5)
		 ON CONFLICT (trace_id) DO NOTHING`,
		event.TraceID, event.UserID, "", event.RideID, time.UnixMilli(event.Timestamp))
	if err != nil {
		return fmt.Errorf("pg insert: %w", err)
	}

	// Aggregate into the user's daily Redis set (TTL 48h).
	date := time.UnixMilli(event.Timestamp).Format("2006-01-02")
	if err := h.redis.AddRideToUserSet(ctx, event.UserID, event.RideID, date); err != nil {
		return fmt.Errorf("redis aggregate: %w", err)
	}

	log.Debug().Str("trace_id", event.TraceID).Str("user_id", event.UserID).Str("ride_id", event.RideID).Msg("scan processed")
	return nil
}