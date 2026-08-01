//go:build integration

// M2.1 CI integration test: real Kafka produce → consume round-trip.
// Requires a running Kafka broker (compose or CI service) and INTEGRATION=1.
//
//	go test -tags=integration ./internal/kafka -run TestIntegrationDelivery -count=1
package kafka

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/Logiqode/ThemeParkNFT/internal/config"
	"github.com/Logiqode/ThemeParkNFT/internal/models"
)

// TestIntegrationDelivery produces N events via the real sync producer and
// consumes them from earliest offset (fresh group), asserting every trace_id
// appears exactly once. This is the M2.1 CI gate.
func TestIntegrationDelivery(t *testing.T) {
	if os.Getenv("INTEGRATION") == "" {
		t.Skip("INTEGRATION=1 not set; skipping integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg := config.MustLoad()
	producer := NewProducer(cfg.Kafka)
	defer func() { _ = producer.Close() }()

	const n = 1000
	events := make([]*models.ScanEvent, n)
	for i := 0; i < n; i++ {
		events[i] = &models.ScanEvent{
			UserID:    "ci-user-" + string(rune('0'+i/100%10)) + string(rune('0'+i/10%10)) + string(rune('0'+i%10)) + "@bench.local",
			RideID:    "ride-" + string(rune('0'+i%10)),
			Timestamp: time.Now().UnixMilli(),
			TraceID:   uuid.NewString(),
		}
	}

	// Sync batch write: nil error => all 1000 delivered (acks=all).
	if err := producer.PublishBatch(ctx, events); err != nil {
		t.Fatalf("PublishBatch(1000) error = %v", err)
	}

	// Consume from earliest offset with a unique group.
	groupID := "it-delivery-" + uuid.NewString()
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Kafka.BrokerList(),
		GroupID:     groupID,
		Topic:       cfg.Kafka.TopicRideScans,
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.FirstOffset,
		MaxAttempts: 3,
	})
	defer func() { _ = r.Close() }()

	want := make(map[string]bool, n)
	for _, e := range events {
		want[e.TraceID] = true
	}

	seen := make(map[string]bool, n)
	var dups int
	deadline := time.Now().Add(30 * time.Second)
	for len(seen) < n && time.Now().Before(deadline) {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			continue
		}
		var ev models.ScanEvent
		if err := json.Unmarshal(msg.Value, &ev); err != nil || ev.TraceID == "" {
			continue
		}
		if !want[ev.TraceID] {
			continue // old benchmark events on the topic
		}
		if seen[ev.TraceID] {
			dups++
		} else {
			seen[ev.TraceID] = true
		}
	}

	if len(seen) != n {
		t.Errorf("observed %d of %d unique events", len(seen), n)
	}
	if dups != 0 {
		t.Errorf("delivery duplicates = %d, want 0", dups)
	}
}