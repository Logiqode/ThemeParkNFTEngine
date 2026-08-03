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

	"github.com/jmoiron/sqlx"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/Logiqode/ThemeParkNFT/internal/config"
	"github.com/Logiqode/ThemeParkNFT/internal/health"
	"github.com/Logiqode/ThemeParkNFT/internal/models"
	"github.com/Logiqode/ThemeParkNFT/internal/postgres"
	redisClient "github.com/Logiqode/ThemeParkNFT/internal/redis"
	voucherService "github.com/Logiqode/ThemeParkNFT/internal/voucher"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.MustLoad()
	log.Info().Msg("voucher service starting")

	db, err := sqlx.Connect("pgx", cfg.Postgres.DSN())
	if err != nil {
		log.Fatal().Err(err).Msg("postgres connect failed")
	}
	defer func() { _ = db.Close() }()

	redis := redisClient.NewClient(cfg.Redis)
	defer func() { _ = redis.Close() }()
	repo := postgres.NewRepository(db)
	vsvc := voucherService.NewService(repo)

	// Strict readiness checks (R2): Voucher is ready when Postgres and Redis
	// are both reachable.
	checker := health.NewHealthChecker("voucher")
	checker.AddCheck(func(ctx context.Context) error { return db.PingContext(ctx) })
	checker.AddCheck(func(ctx context.Context) error { return redis.Ping(ctx) })

	// Startup grace: wait up to 20s for dependencies before serving traffic.
	if err := health.WaitForChecks(ctx, checker, 20*time.Second); err != nil {
		log.Fatal().Err(err).Msg("voucher startup: dependencies not ready")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", checker.HealthzHandler())
	mux.HandleFunc("/readyz", checker.ReadyzHandler())

	// POST /api/vouchers/purchase
	mux.HandleFunc("/api/vouchers/purchase", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			PurchaserEmail string `json:"purchaser_email"`
			Quantity       int    `json:"quantity"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		user, err := repo.CreateUser(r.Context(), req.PurchaserEmail)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		ids, err := repo.CreateVouchers(r.Context(), user.ID, req.Quantity)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"voucher_ids": ids})
	})

	// POST /api/vouchers/share — generate magic link (JWT, M4.6)
	mux.HandleFunc("/api/vouchers/share", func(w http.ResponseWriter, r *http.Request) {
		var req struct{ VoucherID string `json:"voucher_id"` }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		tokenStr, err := voucherService.SignMagicLink(cfg.Gate.HMACSecret, req.VoucherID, 24*time.Hour)
		if err != nil {
			http.Error(w, "link signing failed", http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"magic_link": "https://themepark.app/claim?token=" + tokenStr})
	})

	// GET /api/vouchers/claim?token=...&email=... — JIT claim (M4.5/M4.6)
	mux.HandleFunc("/api/vouchers/claim", func(w http.ResponseWriter, r *http.Request) {
		tokenStr := r.URL.Query().Get("token")
		voucherID, err := voucherService.VerifyMagicLink(cfg.Gate.HMACSecret, tokenStr)
		if err != nil {
			// Invalid and expired links are both rejected (M4.6), without
			// leaking which — same 401 either way.
			http.Error(w, "invalid or expired magic link", http.StatusUnauthorized)
			return
		}
		email := r.URL.Query().Get("email")
		if email == "" {
			http.Error(w, "email required", http.StatusBadRequest)
			return
		}
		user, err := repo.CreateUser(r.Context(), email)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := repo.ClaimVoucher(r.Context(), voucherID, user.ID); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "claimed", "user_id": email})
	})

	// POST /api/vouchers/delegate — allocate a voucher to a participant
	// (Rev 3, R27/R28): account mode links an email (own non-custodial,
	// eventually-linked R30), dependent mode creates a child participant under
	// a guardian with a custodial-proxy wallet (R35).
	mux.HandleFunc("/api/vouchers/delegate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req models.DelegateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		res, err := vsvc.Delegate(r.Context(), req)
		if err != nil {
			switch {
			case errors.Is(err, voucherService.ErrInvalidDelegation),
				errors.Is(err, voucherService.ErrParticipantNotFound):
				http.Error(w, err.Error(), http.StatusBadRequest)
			case errors.Is(err, voucherService.ErrVoucherAllocated):
				http.Error(w, err.Error(), http.StatusConflict)
			default:
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		_ = json.NewEncoder(w).Encode(res)
	})

	// POST /api/vouchers/pending — persist a durable pending_mints attribution
	// row for a participant + date (Rev 3, R32). End-of-day job calls this for
	// adults who have no wallet yet; the row outlives Redis/wristband lifetime.
	mux.HandleFunc("/api/vouchers/pending", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req models.RecordPendingMintRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if err := vsvc.RecordPendingMint(r.Context(), req); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "recorded"})
	})

	// POST /api/mint/claim-custody — (Rev 3, R33) off-chain custody transfer:
	// once a dependent links their own account + non-custodial wallet, flip the
	// participant to OWN_NON_CUSTODIAL. Real Sui object transfer is Week 6.
	mux.HandleFunc("/api/mint/claim-custody", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			ParticipantID int64  `json:"participant_id"`
			OwnWallet     string `json:"own_wallet"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		p, err := vsvc.ClaimCustody(r.Context(), req.ParticipantID, req.OwnWallet)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(p)
	})

	srv := &http.Server{Addr: ":8084", Handler: mux}
	go func() { log.Info().Msg("voucher API listening on :8084"); _ = srv.ListenAndServe() }()
	<-ctx.Done()
	_ = srv.Shutdown(context.Background())
}
