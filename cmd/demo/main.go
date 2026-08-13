package main

import (
	"context"
	"fmt"
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
	demoSvc "github.com/Logiqode/ThemeParkNFT/internal/demo"
	kafkapkg "github.com/Logiqode/ThemeParkNFT/internal/kafka"
	"github.com/Logiqode/ThemeParkNFT/internal/minter"
	redispkg "github.com/Logiqode/ThemeParkNFT/internal/redis"
	"github.com/Logiqode/ThemeParkNFT/internal/storage"
	"github.com/Logiqode/ThemeParkNFT/internal/sui"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.MustLoad()
	// Fail-fast on the secrets the demo actually mints with (R3). SUI package
	// + gas pool mnemonic are required for real on-chain transactions; a missing
	// Pinata JWT degrades to metadata-URI faults at mint time (logged by Pinata).
	if err := cfg.Validate("SUI_PACKAGE_ID", "SUI_MINTCAP_ID", "SUI_GAS_POOL_MNEMONIC"); err != nil {
		log.Fatal().Err(err).Msg("demo startup: missing required environment")
	}
	log.Info().Msg("demo orchestrator starting")

	db, err := sqlx.Connect("pgx", cfg.Postgres.DSN())
	if err != nil {
		log.Fatal().Err(err).Msg("postgres connect failed")
	}
	defer func() { _ = db.Close() }()

	redis := redispkg.NewClient(cfg.Redis)
	defer func() { _ = redis.Close() }()

	producer := kafkapkg.NewProducer(cfg.Kafka)
	defer func() { _ = producer.Close() }()

	suiClient, err := sui.NewClient(cfg.Sui)
	if err != nil {
		log.Fatal().Err(err).Msg("sui client init failed")
	}

	pinata := storage.NewPinataClient(cfg.Pinata.APIKey, cfg.Pinata.APISecret, cfg.Pinata.JWT, cfg.Pinata.Gateway)
	cidCache := storage.NewCIDCache(pinata)
	meta := minter.NewCIDMetadataProvider(cidCache)

	orch, err := demoSvc.New(db, redis, producer, suiClient, suiClient, meta, *cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("demo orchestrator build failed")
	}

	if err := orch.WaitReady(ctx, 20*time.Second); err != nil {
		log.Fatal().Err(err).Msg("demo startup: dependencies not ready")
	}

	mux := http.NewServeMux()
	orch.Register(mux)

	addr := fmt.Sprintf(":%d", cfg.Demo.Port)
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		log.Info().Str("addr", addr).Msg("demo API listening")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("demo server failed")
		}
	}()

	<-ctx.Done()
	_ = srv.Shutdown(context.Background())
}
