package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jmoiron/sqlx"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/Logiqode/ThemeParkNFT/internal/config"
	"github.com/Logiqode/ThemeParkNFT/internal/health"
	"github.com/Logiqode/ThemeParkNFT/internal/postgres"
	redisClient "github.com/Logiqode/ThemeParkNFT/internal/redis"
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
	defer db.Close()

	redis := redisClient.NewClient(cfg.Redis)
	defer redis.Close()
	repo := postgres.NewRepository(db)

	mux := http.NewServeMux()
	checker := health.NewHealthChecker("voucher")
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
		json.NewDecoder(r.Body).Decode(&req)
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
		json.NewEncoder(w).Encode(map[string]interface{}{"voucher_ids": ids})
	})

	// POST /api/vouchers/share — generate magic link (JWT)
	mux.HandleFunc("/api/vouchers/share", func(w http.ResponseWriter, r *http.Request) {
		var req struct{ VoucherID string `json:"voucher_id"` }
		json.NewDecoder(r.Body).Decode(&req)
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"voucher_id": req.VoucherID,
			"exp":        time.Now().Add(24 * time.Hour).Unix(),
		})
		tokenStr, _ := token.SignedString([]byte(cfg.Gate.HMACSecret))
		json.NewEncoder(w).Encode(map[string]string{"magic_link": "https://themepark.app/claim?token=" + tokenStr})
	})

	// GET /api/vouchers/claim?token=... — JIT claim
	mux.HandleFunc("/api/vouchers/claim", func(w http.ResponseWriter, r *http.Request) {
		tokenStr := r.URL.Query().Get("token")
		token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) { return []byte(cfg.Gate.HMACSecret), nil })
		if err != nil || !token.Valid {
			http.Error(w, "invalid or expired magic link", http.StatusUnauthorized)
			return
		}
		claims, _ := token.Claims.(jwt.MapClaims)
		voucherID, _ := claims["voucher_id"].(string)
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
		json.NewEncoder(w).Encode(map[string]string{"status": "claimed", "user_id": email})
	})

	srv := &http.Server{Addr: ":8084", Handler: mux}
	go func() { log.Info().Msg("voucher API listening on :8084"); _ = srv.ListenAndServe() }()
	<-ctx.Done()
	srv.Shutdown(context.Background())
}
