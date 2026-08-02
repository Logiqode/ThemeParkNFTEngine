package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"

	"github.com/Logiqode/ThemeParkNFT/internal/config"
	"github.com/Logiqode/ThemeParkNFT/internal/models"
)

// MessageHandler processes a single ScanEvent.
type MessageHandler func(ctx context.Context, event *models.ScanEvent) error

// ErrPoison marks a message that cannot be parsed (malformed JSON / missing
// trace_id). Poison messages skip retries and go straight to the DLQ (M3.3).
var ErrPoison = errors.New("poison message: cannot parse")

type Consumer struct {
	reader     *kafka.Reader
	handler    MessageHandler
	dlq        *kafka.Writer
	dlqTopic   string
	brokers    []string
	workers    int
	maxRetries int
	backoff    time.Duration
	wg         sync.WaitGroup
}

// NewConsumer creates a Kafka consumer with manual-commit reliability (R14/D5):
//
//   - CommitInterval is intentionally UNSET (0) — the reader never auto-commits.
//     Offsets advance only via CommitMessages AFTER a message is successfully
//     handled (or routed to the DLQ), so a handler failure can never silently
//     lose a message (it is retried, then DLQed).
//   - Transient handler failures retry MaxRetries times with exponential backoff
//     (base ConsumerBackoffMS), then the raw message is written to the DLQ.
//   - Unparseable (poison) messages go instantly to the DLQ with an error header
//     and the consumer keeps processing (M3.3).
func NewConsumer(cfg config.KafkaConfig, groupID string, handler MessageHandler, workers int) *Consumer {
	brokers := cfg.BrokerList()
	maxRetries := cfg.ConsumerMaxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}
	backoffMS := cfg.ConsumerBackoffMS
	if backoffMS <= 0 {
		backoffMS = 500
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     groupID,
		Topic:       cfg.TopicRideScans,
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.FirstOffset,
		MaxAttempts: 5,
		// CommitInterval left at 0 → manual commit via CommitMessages only.
	})
	dlq := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        cfg.TopicDLQ,
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  3,
	}
	return &Consumer{
		reader: r, handler: handler, dlq: dlq, dlqTopic: cfg.TopicDLQ,
		brokers: brokers, workers: workers,
		maxRetries: maxRetries, backoff: time.Duration(backoffMS) * time.Millisecond,
	}
}

// Ping verifies Kafka connectivity by dialing each configured broker via the
// Kafka protocol (API-version handshake). Used as a readiness check (R2).
func (c *Consumer) Ping(ctx context.Context) error {
	_ = ctx
	for _, addr := range c.brokers {
		conn, err := kafka.Dial("tcp", addr)
		if err != nil {
			return fmt.Errorf("kafka ping broker %s: %w", addr, err)
		}
		_ = conn.Close()
	}
	return nil
}

// Close closes the underlying Kafka reader. Safe to call on an unused reader
// (e.g. a readiness-only consumer instance).
func (c *Consumer) Close() error {
	_ = c.dlq.Close()
	return c.reader.Close()
}

// Run starts consuming messages with a worker pool and manual-commit semantics.
func (c *Consumer) Run(ctx context.Context) error {
	log.Info().Int("workers", c.workers).Int("max_retries", c.maxRetries).Msg("kafka consumer starting")
	msgCh := make(chan kafka.Message, c.workers*2)

	for i := 0; i < c.workers; i++ {
		c.wg.Add(1)
		go func(workerID int) {
			defer c.wg.Done()
			for msg := range msgCh {
				if err := c.processAndCommit(ctx, msg, workerID); err != nil {
					// Non-nil here means either DLQ/commit failure (reported above)
					// or context cancellation. Message is left uncommitted so it is
					// reprocessed on restart (zero silent loss, D5).
					log.Error().Err(err).Int("worker", workerID).Int64("offset", msg.Offset).Msg("uncommitted (will reprocess)")
				}
			}
		}(i)
	}

	for {
		select {
		case <-ctx.Done():
			close(msgCh)
			c.wg.Wait()
			log.Info().Msg("consumer workers drained")
			return c.reader.Close()
		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					close(msgCh)
					c.wg.Wait()
					return c.reader.Close()
				}
				log.Error().Err(err).Msg("fetch message failed, retrying...")
				time.Sleep(time.Second)
				continue
			}
			msgCh <- msg
		}
	}
}

// processAndCommit applies the reliable-ingestion policy to a single message:
// poison → DLQ → commit; transient failure → retry ×N → DLQ → commit; success →
// commit. The offset is never advanced unless the message is finalised.
func (c *Consumer) processAndCommit(ctx context.Context, msg kafka.Message, workerID int) error {
	event, err := c.parseMessage(&msg)
	if err != nil {
		// Poison: route to DLQ immediately (M3.3), then commit and continue.
		if derr := c.writeDLQ(ctx, msg, err); derr != nil {
			return derr
		}
		if cerr := c.commit(ctx, msg); cerr != nil {
			return cerr
		}
		log.Warn().Err(err).Int("worker", workerID).Int64("offset", msg.Offset).Msg("poison message routed to DLQ")
		return nil
	}

	var lastErr error
	for attempt := 0; ; attempt++ {
		if herr := c.handler(ctx, event); herr != nil {
			lastErr = herr
			if attempt < c.maxRetries {
				log.Warn().Err(herr).Int("worker", workerID).Int("attempt", attempt).Str("trace_id", event.TraceID).Msg("handler failed, retrying")
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(c.backoffFor(attempt)):
				}
				continue
			}
			// Retries exhausted → DLQ (no silent loss, D5), then commit.
			if derr := c.writeDLQ(ctx, msg, lastErr); derr != nil {
				return derr
			}
			return c.commit(ctx, msg)
		}
		// Success → commit the offset.
		log.Debug().Int("worker", workerID).Str("trace_id", event.TraceID).Msg("handler ok, committing")
		return c.commit(ctx, msg)
	}
}

// commit advances the consumer-group offset for a fully-handled message.
func (c *Consumer) commit(ctx context.Context, msg kafka.Message) error {
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		return fmt.Errorf("commit offset: %w", err)
	}
	return nil
}

// backoffFor returns the exponential backoff for the given attempt index
// (attempt 0 → base, 1 → ×2, 2 → ×4, ...).
func (c *Consumer) backoffFor(attempt int) time.Duration {
	d := c.backoff
	for i := 0; i < attempt; i++ {
		d *= 2
	}
	return d
}

// writeDLQ writes a failed/poison message to the dead-letter topic with error
// headers so operators (and replay tooling) can inspect the cause.
func (c *Consumer) writeDLQ(ctx context.Context, msg kafka.Message, cause error) error {
	m := kafka.Message{
		Key:   msg.Key,
		Value: msg.Value,
		Headers: append(msg.Headers,
			kafka.Header{Key: "dlq.reason", Value: []byte(cause.Error())},
			kafka.Header{Key: "dlq.time", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		),
	}
	if err := c.dlq.WriteMessages(ctx, m); err != nil {
		return fmt.Errorf("dlq write to %s: %w", c.dlqTopic, err)
	}
	log.Warn().Str("dlq.reason", cause.Error()).Msg("message routed to DLQ")
	return nil
}

func (c *Consumer) parseMessage(msg *kafka.Message) (*models.ScanEvent, error) {
	var event models.ScanEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return nil, fmt.Errorf("unmarshal event at offset %d: %w", msg.Offset, err)
	}
	if event.TraceID == "" {
		return nil, fmt.Errorf("missing trace_id at offset %d", msg.Offset)
	}
	return &event, nil
}
