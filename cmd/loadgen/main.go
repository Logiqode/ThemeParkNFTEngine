package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/Logiqode/ThemeParkNFT/internal/config"
	internalKafka "github.com/Logiqode/ThemeParkNFT/internal/kafka"
	"github.com/Logiqode/ThemeParkNFT/internal/models"
)

// summary is the machine-readable result emitted by loadgen. Shell benchmark
// scripts parse it (jq) to assert delivery metrics against the R15 99.9% target.
type summary struct {
	TotalProduced int64   `json:"total_produced"`
	TotalErrors   int64   `json:"total_errors"`
	Duplicates    int64   `json:"duplicates"`
	AvgRate       float64 `json:"avg_rate"`
	ElapsedSec    float64 `json:"elapsed_sec"`
	P99LatencyMS  float64 `json:"p99_latency_ms"`
}

func main() {
	rate := flag.Int("rate", 1000, "events per second")
	duration := flag.Duration("duration", 10*time.Second, "test duration (0 = run until SIGTERM)")
	maxEvents := flag.Int64("max-events", 0, "stop after producing this many events (0 = unlimited)")
	users := flag.Int("users", 500, "unique user count")
	dupPct := flag.Float64("dup-pct", 0.0, "duplicate percentage (0-100)")
	rides := flag.Int("rides", 10, "distinct ride types")
	concurrency := flag.Int("concurrency", 4, "goroutine pool")
	manifestPath := flag.String("manifest", "manifest.jsonl", "path to write JSONL of delivered trace_ids")
	summaryPath := flag.String("summary", "summary.json", "path to write machine-readable summary")
	emailDomain := flag.String("email-domain", "bench.local", "email domain for generated users (R11 internal identity)")
	flag.Parse()

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).
		Level(zerolog.InfoLevel)

	cfg := config.MustLoad()
	log.Info().
		Str("kafka", cfg.Kafka.Brokers).
		Int("rate", *rate).
		Dur("duration", *duration).
		Int("users", *users).
		Float64("dup_pct", *dupPct).
		Int("concurrency", *concurrency).
		Msg("load generator starting")

	producer := internalKafka.NewProducer(cfg.Kafka)

	// R11: user identity is the internal email key (never emitted on-chain).
	// Consumer resolves scan_events.user_id via users.email.
	userIDs := make([]string, *users)
	for i := 0; i < *users; i++ {
		userIDs[i] = fmt.Sprintf("user-%04d@%s", i+1, *emailDomain)
	}

	rideIDs := make([]string, *rides)
	for i := 0; i < *rides; i++ {
		rideIDs[i] = fmt.Sprintf("ride-%03d", i+1)
	}

	type eventPair struct {
		event *models.ScanEvent
		isDup bool
	}
	log.Info().Int64("max_events", *maxEvents).Msg("load generator configured")
	// Sending channel: generator -> producers. Closed exactly once by the
	// generator goroutine (the only closer), so producers drain and stop.
	// When --rate=0 (max-events mode) use a sane fixed buffer so the generator
	// is never starved waiting on an unbuffered channel.
	eventChBuf := *rate * 2
	if eventChBuf == 0 {
		eventChBuf = 4096
	}
	eventCh := make(chan eventPair, eventChBuf)
	// Success channel: producers -> manifest writer (delivered trace_ids only).
	producedCh := make(chan []string, *rate*2)

	// --- Manifest writer (JSONL: {"trace_id": "..."} per delivered event) ---
	manifestFile, err := os.Create(*manifestPath)
	if err != nil {
		log.Fatal().Err(err).Str("path", *manifestPath).Msg("open manifest failed")
	}
	var wgManifest sync.WaitGroup
	wgManifest.Add(1)
	go func() {
		defer wgManifest.Done()
		w := bufio.NewWriter(manifestFile)
		enc := json.NewEncoder(w)
		defer func() {
			_ = w.Flush()
			_ = manifestFile.Close()
		}()
		for batch := range producedCh {
			for _, tid := range batch {
				_ = enc.Encode(map[string]string{"trace_id": tid})
			}
		}
	}()

	// --- Counters + latency tracking (p99 produce latency) ---
	var produced atomic.Int64
	// emitted counts events handed to the producer pool BEFORE batching/acking.
	// It drives --max-events so exactly N events are enqueued; producers drain
	// the channel before exit, so acked count matches N (barring broker errors).
	var emitted atomic.Int64
	var duplicates atomic.Int64
	var errors atomic.Int64
	var latencyMu sync.Mutex
	var latencies []time.Duration

	ctx := context.Background()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}
	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// --- Producer pool ---
	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			batch := make([]*models.ScanEvent, 0, 100)
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()

			flush := func() {
				if len(batch) == 0 {
					return
				}
				start := time.Now()
				if err := producer.PublishBatch(context.Background(), batch); err != nil {
					log.Error().Err(err).Int("worker", workerID).Msg("batch publish failed")
					errors.Add(int64(len(batch)))
				} else {
					ids := make([]string, len(batch))
					for j, e := range batch {
						ids[j] = e.TraceID
					}
					producedCh <- ids
					produced.Add(int64(len(batch)))
					latencyMu.Lock()
					latencies = append(latencies, time.Since(start))
					latencyMu.Unlock()
				}
				batch = batch[:0]
			}

			for {
				select {
				case ep, ok := <-eventCh:
					if !ok {
						flush()
						return
					}
					batch = append(batch, ep.event)
					if ep.isDup {
						duplicates.Add(1)
					}
					if len(batch) >= 100 {
						flush()
					}
				case <-ticker.C:
					flush()
				}
			}
		}(i)
	}

	// --- Event generator (single goroutine; the sole closer of eventCh) ---
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	dupsMu := sync.Mutex{}
	dupsGenerated := make(map[string]bool)
	genDone := make(chan struct{})

	go func() {
		defer close(eventCh)
		defer close(genDone)
		var interval time.Duration
		if *rate > 0 {
			interval = time.Second / time.Duration(*rate)
		}
		for {
			select {
			case <-ctx.Done():
				log.Info().Msg("duration elapsed, draining producers...")
				return
			case <-sigCtx.Done():
				log.Info().Msg("SIGINT/SIGTERM received, draining producers...")
				return
			default:
			}

			// max-events mode: stop once the requested count has been enqueued.
			if *maxEvents > 0 && emitted.Load() >= *maxEvents {
				log.Info().
					Int64("produced", produced.Load()).
					Int64("max_events", *maxEvents).
					Msg("max events reached, draining producers...")
				return
			}

			uidx := rng.Intn(len(userIDs))
			ridx := rng.Intn(len(rideIDs))
			traceID := uuid.NewString()
			isDup := false
			if *dupPct > 0 && rng.Float64()*100 < *dupPct {
				dupsMu.Lock()
				for k := range dupsGenerated {
					traceID = k
					break
				}
				dupsMu.Unlock()
				isDup = true
			}
			if !isDup {
				dupsMu.Lock()
				dupsGenerated[traceID] = true
				dupsMu.Unlock()
			}

			event := &models.ScanEvent{
				UserID:    userIDs[uidx],
				RideID:    rideIDs[ridx],
				Timestamp: time.Now().UnixMilli(),
				TraceID:   traceID,
			}
			emitted.Add(1)
			eventCh <- eventPair{event: event, isDup: isDup}

			select {
			case <-time.After(interval):
			case <-ctx.Done():
				return
			case <-sigCtx.Done():
				return
			}
		}
	}()

	start := time.Now()
	// Periodic progress log (5s) for long benchmark runs.
	progressDone := make(chan struct{})
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-progressDone:
				return
			case <-t.C:
				elapsed := time.Since(start)
				log.Info().
					Int64("produced", produced.Load()).
					Int64("duplicates", duplicates.Load()).
					Int64("errors", errors.Load()).
					Float64("rate", float64(produced.Load())/elapsed.Seconds()).
					Msg("progress")
			}
		}
	}()

	// Wait for the generator to finish: either duration timeout, termination
	// signal, or --max-events reached. The generator is the sole closer of
	// eventCh; the producer pool drains the remainder and flushes before
	// returning.
	select {
	case <-ctx.Done():
	case <-sigCtx.Done():
	case <-genDone:
	}

	wg.Wait()
	close(progressDone)
	close(producedCh)
	wgManifest.Wait()

	_ = producer.Close()
	elapsed := time.Since(start)

	latencyMu.Lock()
	p99Then := percentile(latencies, 0.99) // float64 nanoseconds
	latencyMu.Unlock()

	s := summary{
		TotalProduced: produced.Load(),
		TotalErrors:   errors.Load(),
		Duplicates:    duplicates.Load(),
		AvgRate:       float64(produced.Load()) / elapsed.Seconds(),
		ElapsedSec:    elapsed.Seconds(),
		P99LatencyMS:  p99Then / float64(time.Millisecond),
	}

	raw, _ := json.Marshal(s)
	if *summaryPath != "" {
		if err := os.WriteFile(*summaryPath, raw, 0o644); err != nil {
			log.Error().Err(err).Msg("write summary failed")
		}
	}
	fmt.Println(string(raw))
	log.Info().Msg("load generator complete")
}

// percentile returns the p-th percentile (0..1) of an unsorted duration slice.
// Returns 0 when the slice is empty.
func percentile(ds []time.Duration, p float64) float64 {
	if len(ds) == 0 {
		return 0
	}
	vals := make([]float64, len(ds))
	for i, d := range ds {
		vals[i] = float64(d)
	}
	// Simple deterministic selection: sort a copy.
	insertionSort(vals)
	idx := int(p * float64(len(vals)-1))
	return vals[idx]
}

func insertionSort(vals []float64) {
	for i := 1; i < len(vals); i++ {
		for j := i; j > 0 && vals[j] < vals[j-1]; j-- {
			vals[j], vals[j-1] = vals[j-1], vals[j]
		}
	}
}