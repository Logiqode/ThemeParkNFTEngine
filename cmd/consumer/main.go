package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/Logiqode/ThemeParkNFT/internal/config"
	"github.com/Logiqode/ThemeParkNFT/internal/health"
	internalKafka "github.com/Logiqode/ThemeParkNFT/internal/kafka"
	"github.com/Logiqode/ThemeParkNFT/internal/models"
	"github.com/Logiqode/ThemeParkNFT/internal/pipeline"
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
	defer db.Close()

	redis := redisClient.NewClient(cfg.Redis)
	defer redis.Close()

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
	defer healthSrv.Close()

	// Shared production handler: dedup → persist → aggregate (internal/pipeline).
	scanHandler := pipeline.NewScanHandler(db, redis)

	consumer := internalKafka.NewConsumer(cfg.Kafka, cfg.Kafka.ConsumerGroup, scanHandler.Handle, 10)
	if err := consumer.Run(ctx); err != nil {
		log.Fatal().Err(err).Msg("consumer failed")
	}
}
