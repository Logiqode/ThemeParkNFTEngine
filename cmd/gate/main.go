package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/Logiqode/ThemeParkNFT/internal/auth"
	"github.com/Logiqode/ThemeParkNFT/internal/config"
	"github.com/Logiqode/ThemeParkNFT/internal/gate"
	"github.com/Logiqode/ThemeParkNFT/internal/health"
	internalKafka "github.com/Logiqode/ThemeParkNFT/internal/kafka"
	redisClient "github.com/Logiqode/ThemeParkNFT/internal/redis"
)

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

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
	defer func() { _ = db.Close() }()

	redis := redisClient.NewClient(cfg.Redis)
	defer func() { _ = redis.Close() }()

	producer := internalKafka.NewProducer(cfg.Kafka)
	defer func() { _ = producer.Close() }()

	// NFC transaction check (R16): mock now (no testnet spam); real zkLogin in W6.
	perf := &auth.MockTxnCheck{FailWhen: cfg.Gate.TxnCheckFailWhen}
	bindingSvc := gate.NewBindingService(db, redis, cfg.Gate, perf)
	rideScanSvc := gate.NewRideScanService(redis, producer, db)

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

	// GET /api/wristband/scan-visitor-qr-token?ticket_id=...
	// Business meaning: gate staff requests a one-time HMAC QR token (30s rotation,
	// ticket bound into the signature, R18) for the visitor to present.
	mux.HandleFunc("/api/wristband/scan-visitor-qr-token", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		ticketID := r.URL.Query().Get("ticket_id")
		if ticketID == "" {
			writeErr(w, http.StatusBadRequest, "ticket_id query param required")
			return
		}
		token := gate.GenerateQROTP(cfg.Gate, ticketID)
		writeJSON(w, http.StatusOK, token)
	})

	// POST /api/wristband/bind — first staff scan: bind wristband NFC id to the
	// visitor's ticket via the scanned account QR (R9/R18/R21). Ticket → BINDING.
	mux.HandleFunc("/api/wristband/bind", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var req gate.BindRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid json")
			return
		}
		resp, err := bindingSvc.Bind(r.Context(), req)
		switch {
		case errors.Is(err, gate.ErrQRInvalid):
			writeErr(w, http.StatusBadRequest, "invalid or expired QR")
		case errors.Is(err, gate.ErrQRReplay):
			writeErr(w, http.StatusConflict, "QR already used (replay rejected)")
		case errors.Is(err, gate.ErrTicketInvalid):
			writeErr(w, http.StatusNotFound, "ticket not found or not bindable")
		case errors.Is(err, gate.ErrAlreadyBound):
			writeErr(w, http.StatusConflict, "wristband or ticket already bound")
		case err != nil:
			log.Error().Err(err).Msg("bind failed")
			writeErr(w, http.StatusInternalServerError, "internal error")
		default:
			writeJSON(w, http.StatusOK, resp)
		}
	})

	// POST /api/wristband/nfc-check — second staff scan: run the transaction check
	// (R16). Success → BINDING → BOUND (ticket ACTIVE). Faithful grace replay (R24).
	mux.HandleFunc("/api/wristband/nfc-check", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var req gate.NFCCheckRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid json")
			return
		}
		resp, err := bindingSvc.NFCCheck(r.Context(), req)
		switch {
		case errors.Is(err, gate.ErrNoBinding):
			writeErr(w, http.StatusNotFound, "wristband not bound")
		case err != nil:
			log.Error().Err(err).Msg("nfc-check failed")
			writeErr(w, http.StatusInternalServerError, "internal error")
		default:
			// A denied check is still a valid 200 response carrying allowed:false.
			writeJSON(w, http.StatusOK, resp)
		}
	})

	// POST /api/wristband/reset — admin undo/overwrite on faulty NFC (R9/R13):
	// unbind, ticket → CLAIMED so a fresh wristband can be bound.
	mux.HandleFunc("/api/wristband/reset", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var req gate.ResetRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid json")
			return
		}
		resp, err := bindingSvc.Reset(r.Context(), req)
		switch {
		case errors.Is(err, gate.ErrNoBinding):
			writeErr(w, http.StatusNotFound, "wristband not bound")
		case err != nil:
			log.Error().Err(err).Msg("reset failed")
			writeErr(w, http.StatusInternalServerError, "internal error")
		default:
			writeJSON(w, http.StatusOK, resp)
		}
	})

	// POST /api/rides/scan — ride staff NFC scan during visit (R23): publish a
	// ScanEvent to Kafka with a fresh trace_id. Only BOUND wristbands may scan.
	mux.HandleFunc("/api/rides/scan", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var req gate.RideScanRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid json")
			return
		}
		resp, err := rideScanSvc.Scan(r.Context(), req)
		switch {
		case errors.Is(err, gate.ErrNoBinding):
			writeErr(w, http.StatusNotFound, "wristband not bound")
		case errors.Is(err, gate.ErrNotBound):
			writeErr(w, http.StatusForbidden, "wristband not BOUND (active)")
		case err != nil:
			log.Error().Err(err).Msg("ride scan failed")
			writeErr(w, http.StatusInternalServerError, "internal error")
		default:
			writeJSON(w, http.StatusOK, resp)
		}
	})

	srv := &http.Server{Addr: ":8080", Handler: mux}
	go func() { log.Info().Msg("gate API listening on :8080"); _ = srv.ListenAndServe() }()

	<-ctx.Done()
	log.Info().Msg("gate shutting down")
	_ = srv.Shutdown(context.Background())
}
