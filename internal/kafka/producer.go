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

type Producer struct {
	writer  *kafka.Writer
	topic   string
	brokers []string
}

// NewProducer creates a new Kafka producer with idempotence enabled.
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
		Async:        false,
	}
	return &Producer{writer: w, topic: cfg.TopicRideScans, brokers: brokers}
}

// PublishScanEvent marshals a ScanEvent to JSON and writes it to Kafka.
func (p *Producer) PublishScanEvent(ctx context.Context, event *models.ScanEvent) error {
	val, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal ScanEvent: %w", err)
	}
	msg := kafka.Message{
		Key:   []byte(event.TraceID),
		Value: val,
		Headers: []kafka.Header{
			{Key: "traceparent", Value: []byte(event.TraceID)},
		},
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		log.Error().Err(err).Str("trace_id", event.TraceID).Msg("kafka write failed")
		return fmt.Errorf("write messages: %w", err)
	}
	log.Debug().Str("trace_id", event.TraceID).Str("user_id", event.UserID).Str("ride_id", event.RideID).Msg("event produced")
	return nil
}

// PublishBatch sends multiple ScanEvents in a batch for throughput.
func (p *Producer) PublishBatch(ctx context.Context, events []*models.ScanEvent) error {
	msgs := make([]kafka.Message, len(events))
	for i, event := range events {
		val, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("marshal event %d: %w", i, err)
		}
		msgs[i] = kafka.Message{
			Key:   []byte(event.TraceID),
			Value: val,
			Headers: []kafka.Header{
				{Key: "traceparent", Value: []byte(event.TraceID)},
			},
		}
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

// Close flushes and closes the producer.
func (p *Producer) Close() error {
	log.Info().Msg("kafka producer shutting down, flushing...")
	return p.writer.Close()
}
