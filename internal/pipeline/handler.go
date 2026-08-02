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
//
// Reliability contract (R25/D5): once the SETNX dedup marker is claimed, any
// transient downstream failure (PG insert or Redis aggregate) clears the marker
// before returning an error, so the consumer's retry re-processes the trace_id
// instead of dropping it. This preserves effectively-once alongside zero loss.
func (h *ScanHandler) Handle(ctx context.Context, event *models.ScanEvent) (err error) {
	// Deduplication shield (SETNX → trace_id marker with TTL).
	isDup, err := h.redis.IsDuplicate(ctx, event.TraceID)
	if err != nil {
		return err
	}
	if isDup {
		log.Debug().Str("trace_id", event.TraceID).Msg("duplicate dropped")
		return nil
	}

	// Compensation (R25): on any error after the marker was claimed, drop the
	// dedup key so a retry reprocesses rather than being filtered as a duplicate.
	defer func() {
		if err != nil {
			if cerr := h.redis.ClearDedup(ctx, event.TraceID); cerr != nil {
				log.Error().Err(cerr).Str("trace_id", event.TraceID).Msg("failed to clear dedup on error")
			}
		}
	}()

	// Persist to Postgres (idempotent via trace_id UNIQUE). ticket_id is now the
	// real value from the producer (D6, R23) instead of a hardcoded "".
	_, err = h.db.ExecContext(ctx,
		`INSERT INTO scan_events (trace_id, user_id, ticket_id, ride_id, scanned_at)
		 VALUES ($1, (SELECT id FROM users WHERE email = $2 LIMIT 1), $3, $4, $5)
		 ON CONFLICT (trace_id) DO NOTHING`,
		event.TraceID, event.UserID, event.TicketID, event.RideID, time.UnixMilli(event.Timestamp))
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
