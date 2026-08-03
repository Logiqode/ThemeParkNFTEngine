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
)

// scanRedis is the Redis surface the pipeline handler needs. *redisClient.Client
// satisfies it; tests inject fakes (e.g. to break the aggregation step for M4.3).
type scanRedis interface {
	IsDuplicate(ctx context.Context, traceID string) (bool, error)
	ClearDedup(ctx context.Context, traceID string) error
	AddRideToUserSet(ctx context.Context, userID, rideID, date string) error
	GetUserRides(ctx context.Context, userID, date string) ([]string, error)
}

// OutboxWriter parks a failed Redis aggregation (M4.3). After the scan_events
// row is durably committed, a Redis SADD failure is written here and replayed
// by a drain worker, so no ride count is lost when Redis is down.
type OutboxWriter interface {
	InsertOutbox(ctx context.Context, traceID, userEmail, rideID string, scannedAt time.Time) error
}

// ScanHandler implements internal/kafka.MessageHandler: deduplicate by trace_id,
// persist to Postgres scan_events, and aggregate into the user's daily Redis set.
type ScanHandler struct {
	db     *sqlx.DB
	redis  scanRedis
	outbox OutboxWriter // nil = legacy behavior (clear dedup + retry)
}

// NewScanHandler builds the production scan-event handler shared by cmd/consumer
// and integration tests.
func NewScanHandler(db *sqlx.DB, redis scanRedis) *ScanHandler {
	return &ScanHandler{db: db, redis: redis}
}

// WithOutbox attaches an outbox writer (M4.3). Once set, a Redis aggregation
// failure parks the intent instead of failing the message, and returns the
// handler for chaining.
func (h *ScanHandler) WithOutbox(o OutboxWriter) *ScanHandler {
	h.outbox = o
	return h
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
	//
	// M4.3 outbox: when armed, a Redis failure here parks the intent instead of
	// failing the message. PG is already durably committed, so the consumer
	// commits and the drain worker replays the SADD — no loss and no duplicate
	// PG insert (dedup marker is not cleared because we didn't return an error).
	date := time.UnixMilli(event.Timestamp).Format("2006-01-02")
	if err := h.redis.AddRideToUserSet(ctx, event.UserID, event.RideID, date); err != nil {
		if h.outbox != nil {
			if werr := h.outbox.InsertOutbox(ctx, event.TraceID, event.UserID, event.RideID, time.UnixMilli(event.Timestamp)); werr != nil {
				return fmt.Errorf("redis aggregate + outbox: %w (redis err: %v)", werr, err)
			}
			log.Warn().Str("trace_id", event.TraceID).Str("ride_id", event.RideID).Err(err).Msg("redis aggregate failed; parked in outbox")
			return nil
		}
		return fmt.Errorf("redis aggregate: %w", err)
	}

	log.Debug().Str("trace_id", event.TraceID).Str("user_id", event.UserID).Str("ride_id", event.RideID).Msg("scan processed")
	return nil
}

// OutboxStore is the persistence surface the drain worker replays from.
type OutboxStore interface {
	ListPendingOutbox(ctx context.Context, limit int) ([]models.OutboxRow, error)
	BumpOutboxAttempts(ctx context.Context, traceID string) error
	DeleteOutbox(ctx context.Context, traceID string) error
}

// DrainOutbox replays parked Redis aggregations (M4.3). For each PENDING row it
// retries the SADD on the user's daily set keyed by the row's scanned date; on
// success the row is deleted, on failure its attempt counter is bumped so the
// next pass retries. Returns the number drained. The caller loops this on a
// ticker (see cmd/consumer).
func (h *ScanHandler) DrainOutbox(ctx context.Context, store OutboxStore, batch int) (int, error) {
	rows, err := store.ListPendingOutbox(ctx, batch)
	if err != nil {
		return 0, err
	}
	drained := 0
	for _, row := range rows {
		date := row.ScannedAt.Format("2006-01-02")
		if err := h.redis.AddRideToUserSet(ctx, row.UserEmail, row.RideID, date); err != nil {
			_ = store.BumpOutboxAttempts(ctx, row.TraceID)
			log.Warn().Str("trace_id", row.TraceID).Err(err).Msg("outbox drain: redis still unavailable, will retry")
			continue
		}
		if err := store.DeleteOutbox(ctx, row.TraceID); err != nil {
			return drained, err
		}
		drained++
	}
	if drained > 0 {
		log.Info().Int("drained", drained).Msg("outbox drain complete")
	}
	return drained, nil
}
