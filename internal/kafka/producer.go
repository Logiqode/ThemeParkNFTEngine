package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"

	"github.com/Logiqode/ThemeParkNFT/internal/config"
	"github.com/Logiqode/ThemeParkNFT/internal/models"
)

// Producer publishes ScanEvents to the ride-scans Kafka topic.
//
// Delivery semantics (Q1 decision — effectively-once on the consumer side):
//   - segmentio/kafka-go does NOT implement the Java client's idempotent
//     producer protocol (producer PID/epoch + sequence numbers). There is no
//     way to set `enable.idempotence=true` with this library.
//   - Instead we enforce effectively-once via the combination of:
//     1. RequiredAcks=RequireAll (leader + all in-sync replicas ack)
//     2. MaxAttempts (kafka-go retries transient broker errors before returning)
//     3. Consumer-side idempotency: every ScanEvent carries a unique `trace_id`
//        used as the Kafka message key AND the Redis SETNX dedup key
//        (internal/pipeline.ScanHandler). At-least-once Kafka delivery +
//        exactly-once processing of each trace_id = effectively-once end-to-end.
//   - Async mode (KAFKA_PRODUCER_ASYNC=true) is available for the future
//     Week 3 gate producer path where fire-and-forget throughput is preferred;
//     async errors are surfaced via Writer.Stats().Errors (kafka-go has no
//     error channel). The load generator / benchmark harness keeps Async=false
//     so that every batch write returns with a definitive success/error and the
//     benchmark manifest records only actually-delivered trace_ids.
type Producer struct {
	writer  *kafka.Writer
	brokers []string
	async   bool
}

// NewProducer creates a new Kafka producer with acks=all + retries.
// Pass cfg.ProducerAsync=true to enable the async writer mode.
func NewProducer(cfg config.KafkaConfig) *Producer {
	brokers := cfg.BrokerList()
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        cfg.TopicRideScans,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  3,
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		Async:        cfg.ProducerAsync,
	}
	return &Producer{writer: w, brokers: brokers, async: cfg.ProducerAsync}
}

// buildMessages converts ScanEvents to kafka.Messages. Pure and broker-free so
// unit tests can verify the wire format (key = trace_id, header = traceparent,
// value = JSON ScanEvent) without a running broker.
func buildMessages(events []*models.ScanEvent) ([]kafka.Message, error) {
	msgs := make([]kafka.Message, 0, len(events))
	for i, event := range events {
		val, err := json.Marshal(event)
		if err != nil {
			return nil, fmt.Errorf("marshal event %d: %w", i, err)
		}
		msgs = append(msgs, kafka.Message{
			Key:   []byte(event.TraceID),
			Value: val,
			Headers: []kafka.Header{
				{Key: "traceparent", Value: []byte(event.TraceID)},
			},
		})
	}
	return msgs, nil
}

// PublishScanEvent marshals a ScanEvent to JSON and writes it to Kafka.
func (p *Producer) PublishScanEvent(ctx context.Context, event *models.ScanEvent) error {
	msgs, err := buildMessages([]*models.ScanEvent{event})
	if err != nil {
		return err
	}
	if err := p.writer.WriteMessages(ctx, msgs...); err != nil {
		log.Error().Err(err).Str("trace_id", event.TraceID).Msg("kafka write failed")
		return fmt.Errorf("write messages: %w", err)
	}
	log.Debug().Str("trace_id", event.TraceID).Str("user_id", event.UserID).Str("ride_id", event.RideID).Msg("event produced")
	return nil
}

// PublishBatch sends multiple ScanEvents in a batch for throughput.
// In sync mode (default) the write returns only after the broker acks all
// messages, so a nil error means every event in the batch was delivered.
func (p *Producer) PublishBatch(ctx context.Context, events []*models.ScanEvent) error {
	msgs, err := buildMessages(events)
	if err != nil {
		return err
	}
	if err := p.writer.WriteMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("write batch: %w", err)
	}
	log.Debug().Int("count", len(events)).Msg("batch produced")
	return nil
}

// Ping verifies Kafka connectivity by dialing each configured broker via the
// Kafka protocol (API-version handshake). Used as a readiness check (R2).
// It does NOT write to the topic, so no test messages pollute the pipeline.
func (p *Producer) Ping(ctx context.Context) error {
	_ = ctx // kafka.Dial performs its own handshake with a short timeout.
	for _, addr := range p.brokers {
		conn, err := kafka.Dial("tcp", addr)
		if err != nil {
			return fmt.Errorf("kafka ping broker %s: %w", addr, err)
		}
		_ = conn.Close()
	}
	return nil
}

// Close flushes and closes the producer. In async mode this blocks until the
// writer has flushed buffered messages, satisfying the graceful-shutdown
// requirement (M2.4: SIGTERM mid-batch → all in-flight flushed, no data loss).
func (p *Producer) Close() error {
	log.Info().Bool("async", p.async).Msg("kafka producer shutting down, flushing...")
	return p.writer.Close()
}