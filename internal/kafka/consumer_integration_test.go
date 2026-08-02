//go:build integration

// Week 3 integration tests for Kafka consumer reliability (R14/D5): M3.3
// poison→DLQ + continue, and M3.6 concurrent worker-pool no cross-worker
// duplicates. Requires a running Kafka broker (`make up`) and INTEGRATION=1:
//
//	go test -tags=integration ./internal/kafka -run TestIntegrationConsumer -v -count=1
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/Logiqode/ThemeParkNFT/internal/config"
	"github.com/Logiqode/ThemeParkNFT/internal/models"
)

// produceRaw writes raw kafka.Messages to a topic (bypasses the ScanEvent
// producer so we can inject malformed payloads for the poison test).
func produceRaw(t *testing.T, cfg config.KafkaConfig, topic string, msgs []kafka.Message) {
	t.Helper()
	w := &kafka.Writer{
		Addr:         kafka.TCP(cfg.BrokerList()...),
		Topic:        topic,
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  3,
	}
	defer func() { _ = w.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := w.WriteMessages(ctx, msgs...); err != nil {
		t.Fatalf("produce %d messages to %s: %v", len(msgs), topic, err)
	}
}

// cfgFor builds a config pointing at real brokers, with a real DLQ topic, and a
// fast retry loop so tests don't hang on backoff.
func cfgFor() config.KafkaConfig {
	cfg := config.MustLoad()
	ck := cfg.Kafka
	ck.ConsumerMaxRetries = 2
	ck.ConsumerBackoffMS = 50
	return ck
}

// createTopic ensures a topic exists and waits until it is fully available
// (leader elected) so an immediately-following produce does not race metadata
// propagation.
func createTopic(t *testing.T, cfg config.KafkaConfig, topic string, partitions int) {
	t.Helper()
	conn, err := kafka.Dial("tcp", cfg.BrokerList()[0])
	if err != nil {
		t.Fatalf("dial broker for topic creation: %v", err)
	}
	err = conn.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: partitions, ReplicationFactor: 1})
	_ = conn.Close()
	if err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}

	// Wait until the topic is actually writable (all partition leaders elected).
	// A fresh topic can still return "Unknown Topic Or Partition" on produce for
	// a short window after CreateTopics, so probe with a throwaway message.
	probeWriter := &kafka.Writer{
		Addr:         kafka.TCP(cfg.BrokerList()...),
		Topic:        topic,
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  1,
	}
	defer func() { _ = probeWriter.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		err := probeWriter.WriteMessages(ctx, kafka.Message{Key: []byte("__probe__"), Value: []byte("__probe__")})
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("topic %s did not become writable in time: %v", topic, err)
		}
		time.Sleep(150 * time.Millisecond)
	}
}

// isolatedKafka returns a config pointing at freshly-created, per-run topics so
// tests never read pre-existing benchmark/legacy messages from the shared
// ride-scans topic.
func isolatedKafka(t *testing.T, run string) config.KafkaConfig {
	t.Helper()
	c := cfgFor()
	suffix := run + "-" + uuid.NewString()
	c.TopicRideScans = "it-scans-" + suffix
	c.TopicDLQ = "it-dlq-" + suffix
	createTopic(t, c, c.TopicRideScans, 6)
	createTopic(t, c, c.TopicDLQ, 3)
	return c
}

// waitFor polls a predicate until it is true or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, what string, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// readDLQPoll scans the DLQ topic from earliest (fresh group) waiting for a
// message whose value equals the expected poison payload.
func readDLQPoll(t *testing.T, cfg config.KafkaConfig, expect string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.BrokerList(),
		GroupID:     "it-dlq-" + uuid.NewString(),
		Topic:       cfg.TopicDLQ,
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.FirstOffset,
		MaxAttempts: 3,
	})
	defer func() { _ = r.Close() }()
	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				t.Fatalf("poison message %q not found in DLQ", expect)
			}
			continue
		}
		if string(msg.Value) == expect {
			return
		}
	}
}

// M3.3: a malformed (poison) message is routed to the DLQ and the consumer keeps
// processing subsequent valid messages. Only this run's marker-tagged events are
// counted, so pre-existing topic contents (earlier benchmark runs) don't interfere.
func TestIntegrationConsumerPoisonToDLQAndContinue(t *testing.T) {
	if os.Getenv("INTEGRATION") == "" {
		t.Skip("INTEGRATION=1 not set; skipping integration tests")
	}
	ck := isolatedKafka(t, "poison")

	marker := "poison-" + uuid.NewString()
	poison := "not-json{{not valid-" + uuid.NewString() // unique per run

	var mu sync.Mutex
	processed := 0
	handler := func(ctx context.Context, e *models.ScanEvent) error {
		if strings.HasPrefix(e.UserID, marker) {
			mu.Lock()
			processed++
			mu.Unlock()
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer := NewConsumer(ck, "it-poison-"+uuid.NewString(), handler, 2)
	go func() { _ = consumer.Run(ctx) }()
	defer func() { _ = consumer.Close() }()

	const validN = 20
	msgs := make([]kafka.Message, 0, 1+validN)
	msgs = append(msgs, kafka.Message{Key: []byte("poison"), Value: []byte(poison)})
	for i := 0; i < validN; i++ {
		ev := models.ScanEvent{UserID: marker + "-user", RideID: "ride-1", Timestamp: time.Now().UnixMilli(), TraceID: uuid.NewString()}
		b, _ := json.Marshal(ev)
		msgs = append(msgs, kafka.Message{Key: []byte(ev.TraceID), Value: b})
	}
	produceRaw(t, ck, ck.TopicRideScans, msgs)

	waitFor(t, 30*time.Second, "consumer to process all valid marker events", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return processed >= validN
	})
	mu.Lock()
	got := processed
	mu.Unlock()
	if got != validN {
		t.Fatalf("processed = %d, want %d (poison must not stop processing)", got, validN)
	}
	readDLQPoll(t, ck, poison)
}

// M3.6: a 2,000-event stream through a 10-worker pool processes every distinct
// trace_id exactly once (no cross-worker duplicates). Only this run's marker
// events are counted.
func TestIntegrationConsumerConcurrentPoolNoDups(t *testing.T) {
	if os.Getenv("INTEGRATION") == "" {
		t.Skip("INTEGRATION=1 not set; skipping integration tests")
	}
	ck := isolatedKafka(t, "pool")

	marker := "pool-" + uuid.NewString()
	var mu sync.Mutex
	seen := map[string]int{}
	var dups int
	var processed int
	handler := func(ctx context.Context, e *models.ScanEvent) error {
		if !strings.HasPrefix(e.UserID, marker) {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		processed++
		if seen[e.TraceID] > 0 {
			dups++
		}
		seen[e.TraceID]++
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer := NewConsumer(ck, "it-pool-"+uuid.NewString(), handler, 10)
	go func() { _ = consumer.Run(ctx) }()
	defer func() { _ = consumer.Close() }()

	const n = 2000
	msgs := make([]kafka.Message, n)
	for i := 0; i < n; i++ {
		ev := models.ScanEvent{UserID: fmt.Sprintf("%s-%04d", marker, i), RideID: "ride-x", Timestamp: time.Now().UnixMilli(), TraceID: uuid.NewString()}
		b, _ := json.Marshal(ev)
		msgs[i] = kafka.Message{Key: []byte(ev.TraceID), Value: b}
	}
	produceRaw(t, ck, ck.TopicRideScans, msgs)

	waitFor(t, 45*time.Second, "consumer to process all 2000 marker events", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return processed >= n
	})
	mu.Lock()
	p, d := processed, dups
	mu.Unlock()
	if p != n {
		t.Fatalf("processed = %d, want %d", p, n)
	}
	if d != 0 {
		t.Fatalf("cross-worker duplicates = %d, want 0", d)
	}
}
