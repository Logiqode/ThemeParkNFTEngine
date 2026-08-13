package demo

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"
)

// ResetResult reports what a full-stack reset truncated/flushed.
type ResetResult struct {
	TruncatedTables []string `json:"truncated_tables"`
	RedisFlushed    bool     `json:"redis_flushed"`
}

// Reset wipes all demo-affecting data: the application tables, Redis, and the
// demo's Kafka topic is left intact (topics are durable infra; the consumer
// dedups by trace_id so stale messages are harmless). It is the data-layer
// equivalent of `make reset` without tearing down the containers.
func (o *Orchestrator) Reset(ctx context.Context) (*ResetResult, error) {
	tables := []string{
		"scan_events",
		"scan_events_outbox",
		"mint_logs",
		"pending_mints",
		"ticket_vouchers",
		"participants",
		"tickets",
		"users",
	}
	// Truncate in FK-safe order.
	for _, t := range tables {
		stmt := fmt.Sprintf("TRUNCATE TABLE %s RESTART IDENTITY CASCADE", t)
		if _, err := o.db.ExecContext(ctx, stmt); err != nil {
			return nil, fmt.Errorf("truncate %s: %w", t, err)
		}
	}

	if err := o.redis.FlushDB(ctx); err != nil {
		return nil, fmt.Errorf("redis flushdb: %w", err)
	}

	log.Info().Strs("tables", tables).Msg("demo reset complete")
	return &ResetResult{TruncatedTables: tables, RedisFlushed: true}, nil
}
