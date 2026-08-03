package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/Logiqode/ThemeParkNFT/internal/config"
	"github.com/Logiqode/ThemeParkNFT/internal/health"
	internalKafka "github.com/Logiqode/ThemeParkNFT/internal/kafka"
	"github.com/Logiqode/ThemeParkNFT/internal/models"
	"github.com/Logiqode/ThemeParkNFT/internal/pipeline"
	"github.com/Logiqode/ThemeParkNFT/internal/postgres"
	redisClient "github.com/Logiqode/ThemeParkNFT/internal/redis"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.MustLoad()
	log.Info().Str("config", cfg.String()).Msg("consumer starting")

	db, err := sqlx.Connect("pgx", cfg.Postgres.DSN())
	if err != nil {
		log.Fatal().Err(err).Msg("postgres connect failed")
	}
	defer func() { _ = db.Close() }()

	redis := redisClient.NewClient(cfg.Redis)
	defer func() { _ = redis.Close() }()

	// Strict readiness checks (R2): Consumer is ready when Kafka brokers,
	// Redis, and Postgres are all reachable.
	checker := health.NewHealthChecker("consumer")
	checker.AddCheck(func(ctx context.Context) error { return redis.Ping(ctx) })
	checker.AddCheck(func(ctx context.Context) error { return db.PingContext(ctx) })

	// Kafka readiness is verified via the consumer's own broker dial.
	consumer0 := internalKafka.NewConsumer(cfg.Kafka, cfg.Kafka.ConsumerGroup, func(ctx context.Context, event *models.ScanEvent) error { return nil }, 1)
	checker.AddCheck(func(ctx context.Context) error { return consumer0.Ping(ctx) })

	// Startup grace: wait up to 20s for dependencies before consuming.
	if err := health.WaitForChecks(ctx, checker, 20*time.Second); err != nil {
		log.Fatal().Err(err).Msg("consumer startup: dependencies not ready")
	}
	// The readiness-only consumer is no longer needed; close its reader to
	// avoid leaking a Kafka group + broker connection.
	if err := consumer0.Close(); err != nil {
		log.Warn().Err(err).Msg("closing readiness consumer failed (non-fatal)")
	}

	// Consumer health server on :8081 (headless Kafka worker — health only).
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", checker.HealthzHandler())
	healthMux.HandleFunc("/readyz", checker.ReadyzHandler())
	healthSrv := &http.Server{Addr: ":8081", Handler: healthMux}
	go func() { log.Info().Msg("consumer health API listening on :8081"); _ = healthSrv.ListenAndServe() }()
	defer func() { _ = healthSrv.Close() }()

	// Shared production handler: dedup → persist → aggregate (internal/pipeline).
	// It is outbox-armed (M4.3): a Redis aggregation failure parks the intent in
	// scan_events_outbox instead of losing the count.
	repo := postgres.NewRepository(db)
	scanHandler := pipeline.NewScanHandler(db, redis).WithOutbox(repo)

	// M4.3 outbox drain worker: replay parked Redis aggregations until they land.
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := scanHandler.DrainOutbox(ctx, repo, 100); err != nil {
					log.Warn().Err(err).Msg("outbox drain pass failed (will retry)")
				}
			}
		}
	}()

	consumer := internalKafka.NewConsumer(cfg.Kafka, cfg.Kafka.ConsumerGroup, scanHandler.Handle, 10)
	if err := consumer.Run(ctx); err != nil {
		log.Fatal().Err(err).Msg("consumer failed")
	}
}
