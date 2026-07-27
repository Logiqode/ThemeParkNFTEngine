package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/Logiqode/ThemeParkNFT/internal/config"
	internalKafka "github.com/Logiqode/ThemeParkNFT/internal/kafka"
	"github.com/Logiqode/ThemeParkNFT/internal/models"
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

	handler := func(ctx context.Context, event *models.ScanEvent) error {
		// Deduplication shield
		isDup, err := redis.IsDuplicate(ctx, event.TraceID)
		if err != nil {
			return err
		}
		if isDup {
			log.Debug().Str("trace_id", event.TraceID).Msg("duplicate dropped")
			return nil
		}

		// Persist to Postgres (idempotent via trace_id UNIQUE)
		_, err = db.ExecContext(ctx,
			`INSERT INTO scan_events (trace_id, user_id, ticket_id, ride_id, scanned_at)
			 VALUES ($1, (SELECT id FROM users WHERE email = $2 LIMIT 1), $3, $4, $5)
			 ON CONFLICT (trace_id) DO NOTHING`,
			event.TraceID, event.UserID, "", event.RideID, time.UnixMilli(event.Timestamp))
		if err != nil {
			log.Error().Err(err).Str("trace_id", event.TraceID).Msg("pg insert failed")
			return err
		}

		// Aggregate into Redis Set (daily)
		date := time.UnixMilli(event.Timestamp).Format("2006-01-02")
		if err := redis.AddRideToUserSet(ctx, event.UserID, event.RideID, date); err != nil {
			log.Error().Err(err).Str("user_id", event.UserID).Str("ride_id", event.RideID).Msg("redis aggregation failed")
			return err
		}

		log.Debug().Str("trace_id", event.TraceID).Str("user_id", event.UserID).Str("ride_id", event.RideID).Msg("scan processed")
		return nil
	}

	consumer := internalKafka.NewConsumer(cfg.Kafka, cfg.Kafka.ConsumerGroup, handler, 10)
	if err := consumer.Run(ctx); err != nil {
		log.Fatal().Err(err).Msg("consumer failed")
	}
}
