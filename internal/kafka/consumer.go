package kafka

import (
	"context"
	"encoding/json"
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

type Consumer struct {
	reader  *kafka.Reader
	handler MessageHandler
	wg      sync.WaitGroup
	workers int
	brokers []string
}

func NewConsumer(cfg config.KafkaConfig, groupID string, handler MessageHandler, workers int) *Consumer {
	brokers := cfg.BrokerList()
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          cfg.TopicRideScans,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset,
		MaxAttempts:    5,
	})
	return &Consumer{reader: r, handler: handler, workers: workers, brokers: brokers}
}

// Ping verifies Kafka connectivity by dialing each configured broker via the
// Kafka protocol (API-version handshake). Used as a readiness check (R2).
func (c *Consumer) Ping(ctx context.Context) error {
	_ = ctx // kafka.Dial performs its own handshake with a short timeout.
	for _, addr := range c.brokers {
		conn, err := kafka.Dial("tcp", addr)
		if err != nil {
			return fmt.Errorf("kafka ping broker %s: %w", addr, err)
		}
		conn.Close()
	}
	return nil
}

// Close closes the underlying Kafka reader. Safe to call on an unused reader
// (e.g. a readiness-only consumer instance).
func (c *Consumer) Close() error {
	return c.reader.Close()
}

// Run starts consuming messages with a worker pool.
func (c *Consumer) Run(ctx context.Context) error {
	log.Info().Int("workers", c.workers).Msg("kafka consumer starting")
	msgCh := make(chan kafka.Message, c.workers*2)

	for i := 0; i < c.workers; i++ {
		c.wg.Add(1)
		go func(workerID int) {
			defer c.wg.Done()
			for msg := range msgCh {
				event, err := c.parseMessage(&msg)
				if err != nil {
					log.Error().Err(err).Int("worker", workerID).Int64("offset", msg.Offset).Msg("parse failed, skipping")
					continue
				}
				if err := c.handler(ctx, event); err != nil {
					log.Error().Err(err).Int("worker", workerID).Str("trace_id", event.TraceID).Msg("handler failed")
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
