package main

import (
	"context"
	"encoding/json"
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
	"github.com/Logiqode/ThemeParkNFT/internal/gate"
	"github.com/Logiqode/ThemeParkNFT/internal/health"
	internalKafka "github.com/Logiqode/ThemeParkNFT/internal/kafka"
	redisClient "github.com/Logiqode/ThemeParkNFT/internal/redis"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.MustLoad()
	if err := cfg.Validate("HMAC_SECRET"); err != nil {
		log.Fatal().Err(err).Msg("gate startup: missing required environment")
	}
	log.Info().Str("config", cfg.String()).Msg("gate starting")

	db, err := sqlx.Connect("pgx", cfg.Postgres.DSN())
	if err != nil {
		log.Fatal().Err(err).Msg("postgres connect failed")
	}
	defer db.Close()

	redis := redisClient.NewClient(cfg.Redis)
	defer redis.Close()

	producer := internalKafka.NewProducer(cfg.Kafka)
	defer producer.Close()

	verifier := gate.NewVerifier(db)

	// Strict readiness checks (R2): Gate is ready when Postgres, Redis, and
	// Kafka bootstrap brokers are all reachable.
	checker := health.NewHealthChecker("gate")
	checker.AddCheck(func(ctx context.Context) error { return db.PingContext(ctx) })
	checker.AddCheck(func(ctx context.Context) error { return redis.Ping(ctx) })
	checker.AddCheck(func(ctx context.Context) error { return producer.Ping(ctx) })

	// Startup grace: wait up to 20s for dependencies before serving traffic.
	if err := health.WaitForChecks(ctx, checker, 20*time.Second); err != nil {
		log.Fatal().Err(err).Msg("gate startup: dependencies not ready")
	}

	mux := http.NewServeMux()

	// Health check
	mux.HandleFunc("/healthz", checker.HealthzHandler())
	mux.HandleFunc("/readyz", checker.ReadyzHandler())

	// POST /api/gate/verify — atomic ticket verification with 5s grace window
	mux.HandleFunc("/api/gate/verify", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			TicketID string `json:"ticket_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}

		// Check Redis grace window (5s retry cache)
		if cached, _ := redis.GetGraceWindow(r.Context(), req.TicketID); cached != "" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{"allowed": true, "source": "grace_window"})
			return
		}

		resp, err := verifier.VerifyTicket(r.Context(), req.TicketID)
		if err != nil {
			log.Error().Err(err).Str("ticket_id", req.TicketID).Msg("verify failed")
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		// Cache the result in the grace window
		resultStr := "denied"
		if resp.Allowed {
			resultStr = "allowed"
		}
		_ = redis.SetGraceWindow(r.Context(), req.TicketID, resultStr)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	// GET /api/wristband/scan-visitor-qr-token — generate HMAC-signed QR payload.
	// Business meaning: gate staff requests a one-time QR token (30s rotation) for the visitor to present.
	mux.HandleFunc("/api/wristband/scan-visitor-qr-token", func(w http.ResponseWriter, r *http.Request) {
		token := gate.GenerateQROTP(cfg.Gate)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(token)
	})

	// POST /api/gate/confirm — turnstile confirms entry
	mux.HandleFunc("/api/gate/confirm", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct{ TicketID string `json:"ticket_id"` }
		json.NewDecoder(r.Body).Decode(&req)
		if err := verifier.ConfirmEntry(r.Context(), req.TicketID); err != nil {
			http.Error(w, "confirm failed", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Addr: ":8080", Handler: mux}
	go func() { log.Info().Msg("gate API listening on :8080"); _ = srv.ListenAndServe() }()

	<-ctx.Done()
	log.Info().Msg("gate shutting down")
	srv.Shutdown(context.Background())
}
