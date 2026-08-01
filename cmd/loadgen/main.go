package main

import (
	"context"
	"flag"
	"fmt"
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

func main() {
	rate := flag.Int("rate", 1000, "events per second")
	duration := flag.Duration("duration", 10*time.Second, "test duration")
	users := flag.Int("users", 500, "unique user count")
	dupPct := flag.Float64("dup-pct", 0.0, "duplicate percentage (0-100)")
	rides := flag.Int("rides", 10, "distinct ride types")
	concurrency := flag.Int("concurrency", 4, "goroutine pool")
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
	defer func() { _ = producer.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var produced atomic.Int64
	var duplicates atomic.Int64
	var errors atomic.Int64

	rideIDs := make([]string, *rides)
	for i := 0; i < *rides; i++ {
		rideIDs[i] = fmt.Sprintf("ride-%03d", i+1)
	}

	userIDs := make([]string, *users)
	for i := 0; i < *users; i++ {
		userIDs[i] = uuid.New().String()
	}

	type eventPair struct {
		event     *models.ScanEvent
		isDup     bool
	}
	eventCh := make(chan eventPair, *rate*2)

	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch := make([]*models.ScanEvent, 0, 100)
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case ep, ok := <-eventCh:
					if !ok {
						if len(batch) > 0 {
							if err := producer.PublishBatch(context.Background(), batch); err != nil {
								log.Error().Err(err).Msg("batch publish failed")
								errors.Add(int64(len(batch)))
							} else {
								produced.Add(int64(len(batch)))
							}
						}
						return
					}
					batch = append(batch, ep.event)
					if ep.isDup {
						duplicates.Add(1)
					}
					if len(batch) >= 100 {
						if err := producer.PublishBatch(context.Background(), batch); err != nil {
							log.Error().Err(err).Msg("batch publish failed")
							errors.Add(int64(len(batch)))
						} else {
							produced.Add(int64(len(batch)))
						}
						batch = batch[:0]
					}
				case <-ticker.C:
					if len(batch) > 0 {
						if err := producer.PublishBatch(context.Background(), batch); err != nil {
							log.Error().Err(err).Msg("batch ticker publish failed")
							errors.Add(int64(len(batch)))
						} else {
							produced.Add(int64(len(batch)))
						}
						batch = batch[:0]
					}
				}
			}
		}()
	}

	interval := time.Second / time.Duration(*rate)
	dupsGenerated := make(map[string]bool)
	ticker := time.NewTicker(5 * time.Second)
	start := time.Now()

	log.Info().Msg("generating events...")
	go func() {
		for {
			select {
			case <-ctx.Done():
				close(eventCh)
				return
			case <-sigCtx.Done():
				log.Info().Msg("SIGINT/SIGTERM received, flushing...")
				close(eventCh)
				return
			default:
				uidx := int(time.Now().UnixNano()) % len(userIDs)
				ridx := int(time.Now().UnixNano()) % len(rideIDs)
				traceID := uuid.New().String()
				isDup := false
				if *dupPct > 0 && float64(time.Now().UnixNano()%100)/100.0 < *dupPct/100.0 {
					if len(dupsGenerated) > 0 {
						for k := range dupsGenerated {
							traceID = k
							break
						}
						isDup = true
					}
				}
				if !isDup {
					dupsGenerated[traceID] = true
				}
				event := &models.ScanEvent{
					UserID:    userIDs[uidx],
					RideID:    rideIDs[ridx],
					Timestamp: time.Now().UnixMilli(),
					TraceID:   traceID,
				}
				eventCh <- eventPair{event: event, isDup: isDup}
				time.Sleep(interval)
			}
		}
	}()

	for {
		select {
		case <-ticker.C:
			elapsed := time.Since(start)
			rate := float64(produced.Load()) / elapsed.Seconds()
			log.Info().
				Int64("produced", produced.Load()).
				Int64("duplicates", duplicates.Load()).
				Int64("errors", errors.Load()).
				Float64("rate", rate).
				Msg("progress")
		case <-ctx.Done():
			log.Info().Msg("context done, waiting for publishers to flush...")
			wg.Wait()
			elapsed := time.Since(start)
			total := produced.Load()
			log.Info().
				Int64("total_produced", total).
				Int64("duplicates", duplicates.Load()).
				Int64("errors", errors.Load()).
				Float64("avg_rate", float64(total)/elapsed.Seconds()).
				Float64("elapsed_sec", elapsed.Seconds()).
				Msg("load generator complete")
			return
		case <-sigCtx.Done():
			log.Info().Msg("SIGINT/SIGTERM, flushing...")
			cancel()
			wg.Wait()
			return
		}
	}
}
