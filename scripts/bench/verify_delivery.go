// Command verify_delivery measures Kafka end-to-end delivery against a loadgen
// manifest (R15). Consumes ride-scans from the earliest offset, intersects
// observed trace_ids with the manifest, and reports:
//
//	success_rate          = observed unique trace_ids / manifest unique trace_ids  (target >= 99.9%)
//	delivery_duplicates   = observations BEYOND the benchmark's intended count     (target 0)
//	recovery_sec          = wall time to observe all manifest uniques
//
// Benchmark semantics: loadgen intentionally re-sends some trace_ids
// (-dup-pct 10-15%) to model duplicate redundancy. The verifier counts a
// delivery duplicate only when a trace_id is observed MORE times than the
// manifest says it was produced. Exit code is non-zero when the target is not
// met. Shell benchmark scripts parse the JSON output.
//
// Usage:
//
//	go run ./scripts/bench/verify_delivery.go \
//	  --manifest manifest.jsonl --brokers localhost:29092 --topic ride-scans
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
)

type manifestEntry struct {
	TraceID string `json:"trace_id"`
}

type result struct {
	ManifestUnique     int64   `json:"manifest_unique"`
	ManifestLines      int64   `json:"manifest_lines"`
	ObservedUnique     int64   `json:"observed_unique"`
	DeliveryDuplicates int64   `json:"delivery_duplicates"`
	SuccessRate        float64 `json:"success_rate"`
	RecoverySec        float64 `json:"recovery_sec"`
	Pass               bool    `json:"pass"`
}

func main() {
	manifestPath := flag.String("manifest", "manifest.jsonl", "loadgen manifest JSONL")
	brokers := flag.String("brokers", "localhost:29092", "comma-separated Kafka brokers")
	topic := flag.String("topic", "ride-scans", "Kafka topic to verify")
	targetPct := flag.Float64("target", 0.999, "minimum success fraction (0.999 = 99.9%)")
	dupAllowed := flag.Int64("allow-dups", 0, "maximum tolerated duplicate deliveries")
	timeout := flag.Duration("timeout", 60*time.Second, "max wait to observe all manifest uniques")
	flag.Parse()

	// expected[trace_id] = how many times loadgen intentionally produced this
	// trace_id (benchmark injects 10-15% dup lines). Delivery duplicates are
	// only counted when a trace_id is observed MORE than its expected count.
	expected := make(map[string]int64)
	seen := make(map[string]bool)
	var manifestUniques, manifestLines int64
	file, err := os.Open(*manifestPath)
	if err != nil {
		fatal("open manifest: %v", err)
	}
	sc := bufio.NewScanner(file)
	sc.Buffer(make([]byte, 0, 4096), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var e manifestEntry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			fatal("parse manifest line %q: %v", line, err)
		}
		manifestLines++
		expected[e.TraceID]++
		if !seen[e.TraceID] {
			seen[e.TraceID] = true
			manifestUniques++
		}
	}
	if err := sc.Err(); err != nil {
		fatal("scan manifest: %v", err)
	}
	_ = file.Close()

	if manifestUniques == 0 {
		fatal("manifest contains no unique trace_ids")
	}
	log.Info().Int64("manifest_lines", manifestLines).Int64("manifest_unique", manifestUniques).Msg("verifier starting")

	// Fresh group ensures this run starts from earliest offset.
	groupID := "verify-" + fmt.Sprintf("%d", time.Now().UnixNano())
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     strings.Split(*brokers, ","),
		GroupID:     groupID,
		Topic:       *topic,
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.FirstOffset,
		MaxAttempts: 3,
	})
	defer func() { _ = r.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	var observed atomic.Int64
	var duplicates atomic.Int64
	var lastObserved atomic.Int64
	start := time.Now()
	observedCount := make(map[string]int64)

	for time.Since(start) < *timeout {
		if ctx.Err() != nil {
			break
		}
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			continue
		}
		var ev struct {
			TraceID string `json:"trace_id"`
		}
		if err := json.Unmarshal(msg.Value, &ev); err != nil || ev.TraceID == "" {
			continue
		}
		if !seen[ev.TraceID] {
			continue // not part of this benchmark's manifest
		}
		observedCount[ev.TraceID]++
		if observedCount[ev.TraceID] == 1 {
			observed.Add(1)
			lastObserved.Store(time.Now().UnixMilli())
		}
		if observedCount[ev.TraceID] > expected[ev.TraceID] {
			// Extra copy beyond the benchmark's intended (sampled) count.
			duplicates.Add(1)
		}
		if int(observed.Load()) == int(manifestUniques) {
			break // all manifest uniques delivered
		}
	}

	recovery := time.Since(start).Seconds()
	if lastObserved.Load() > 0 {
		recovery = float64(lastObserved.Load()-start.UnixMilli()) / 1000.0
	}

	successRate := float64(observed.Load()) / float64(manifestUniques)
	pass := successRate >= *targetPct && duplicates.Load() <= *dupAllowed

	res := result{
		ManifestUnique:     manifestUniques,
		ManifestLines:      manifestLines,
		ObservedUnique:     observed.Load(),
		DeliveryDuplicates: duplicates.Load(),
		SuccessRate:        successRate,
		RecoverySec:        recovery,
		Pass:               pass,
	}
	raw, _ := json.Marshal(res)
	fmt.Println(string(raw))

	if !pass {
		log.Error().Float64("success_rate", successRate).Float64("target", *targetPct).
			Int64("duplicates", duplicates.Load()).Msg("DELIVERY VERIFICATION FAILED")
		os.Exit(1)
	}
	log.Info().Float64("success_rate", successRate).Float64("recovery_sec", recovery).
		Int64("observed_unique", observed.Load()).Msg("delivery verification passed")
}

func fatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}