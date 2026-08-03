package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	minterService "github.com/Logiqode/ThemeParkNFT/internal/minter"
	"github.com/Logiqode/ThemeParkNFT/internal/postgres"
	redisClient "github.com/Logiqode/ThemeParkNFT/internal/redis"
	"github.com/Logiqode/ThemeParkNFT/internal/storage"
	suiClient "github.com/Logiqode/ThemeParkNFT/internal/sui"
	voucherService "github.com/Logiqode/ThemeParkNFT/internal/voucher"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.MustLoad()
	if err := cfg.Validate("SUI_PACKAGE_ID", "SUI_MINTCAP_ID"); err != nil {
		log.Fatal().Err(err).Msg("minter startup: missing required environment")
	}
	log.Info().Msg("minter service starting")

	db, err := sqlx.Connect("pgx", cfg.Postgres.DSN())
	if err != nil {
		log.Fatal().Err(err).Msg("postgres connect failed")
	}
	defer func() { _ = db.Close() }()

	redis := redisClient.NewClient(cfg.Redis)
	defer func() { _ = redis.Close() }()
	repo := postgres.NewRepository(db)
	sui, err := suiClient.NewClient(cfg.Sui)
	if err != nil {
		log.Fatal().Err(err).Msg("sui client init failed")
	}

	// Initialize IPFS pinning (Pinata) and CID cache.
	// Implements Option A: artwork + metadata pinned once per ride, reused for all NFTs.
	pinata := storage.NewPinataClient(cfg.Pinata.APIKey, cfg.Pinata.APISecret, cfg.Pinata.Gateway)
	cidCache := storage.NewCIDCache(pinata)

	if cfg.Pinata.APIKey == "" {
		log.Warn().Msg("PINATA_API_KEY not set — IPFS pinning disabled, NFTs will use placeholder URLs")
	}

	// Strict readiness checks (R2): Minter is ready when Postgres, Redis, and
	// the Sui RPC endpoint are all reachable.
	checker := health.NewHealthChecker("minter")
	checker.AddCheck(func(ctx context.Context) error { return db.PingContext(ctx) })
	checker.AddCheck(func(ctx context.Context) error { return redis.Ping(ctx) })
	checker.AddCheck(func(ctx context.Context) error { return sui.Ping(ctx) })

	// Startup grace: wait up to 20s for dependencies before serving traffic.
	if err := health.WaitForChecks(ctx, checker, 20*time.Second); err != nil {
		log.Fatal().Err(err).Msg("minter startup: dependencies not ready")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", checker.HealthzHandler())
	mux.HandleFunc("/readyz", checker.ReadyzHandler())

	// POST /mint/daily — batch mint NFTs for a user's daily rides.
	// Flow: Redis rides → ensure IPFS assets pinned (cached per ride) → batch mint with metadata URLs.
	mux.HandleFunc("/mint/daily", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Email string `json:"email"`
			Date  string `json:"date"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if req.Email == "" || req.Date == "" {
			http.Error(w, "email and date required", http.StatusBadRequest)
			return
		}

		user, err := repo.GetUserByEmail(r.Context(), req.Email)
		if err != nil || user.SuiAddress == nil {
			http.Error(w, "user not found or no Sui address (register via POST /api/auth/google first)", http.StatusBadRequest)
			return
		}

		rideIDs, err := redis.GetUserRides(r.Context(), req.Email, req.Date)
		if err != nil || len(rideIDs) == 0 {
			http.Error(w, "no rides found for this date", http.StatusNotFound)
			return
		}

		// Ensure artwork + metadata are pinned to IPFS for each ride.
		// CIDCache implements Option A: pin once per ride, reuse forever.
		names := make([]string, len(rideIDs))
		metadataURLs := make([]string, len(rideIDs))

		for i, rideID := range rideIDs {
			assets, err := cidCache.GetOrPin(r.Context(), rideID, req.Date)
			if err != nil {
				log.Error().Err(err).Str("ride_id", rideID).Msg("failed to pin ride assets")
				http.Error(w, fmt.Sprintf("IPFS pinning failed for %s: %v", rideID, err), http.StatusInternalServerError)
				return
			}
			names[i] = storage.RideName(rideID)
			metadataURLs[i] = assets.MetadataURI
		}

		log.Info().
			Str("email", req.Email).
			Str("date", req.Date).
			Int("ride_count", len(rideIDs)).
			Int("cid_cache_size", cidCache.Size()).
			Msg("batch minting with IPFS metadata")

		txDigest, err := sui.MintBatchAttendance(r.Context(), *user.SuiAddress, rideIDs, req.Date, names, metadataURLs)
		if err != nil {
			log.Error().Err(err).Str("user_id", req.Email).Msg("mint failed")
			http.Error(w, "mint failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		for _, rideID := range rideIDs {
			_ = repo.RecordMint(r.Context(), user.ID, rideID, req.Date, txDigest, 0)
		}

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"tx_digest":     txDigest,
			"ride_ids":      rideIDs,
			"metadata_urls": metadataURLs,
			"status":        "confirmed",
		})
	})

	// POST /mint/resolve-day — OFF-CHAIN end-of-day mint-resolution pass
	// (Week 4, M4.12, R30/R31/R32). Iterates participants-with-rides for a date,
	// resolves each wallet, and writes a durable pending_mints row for adults
	// with no wallet. NO on-chain activity — actual tx submission is Week 6.
	mux.HandleFunc("/mint/resolve-day", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Date string `json:"date" validate:"required"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if req.Date == "" {
			http.Error(w, "date required (YYYY-MM-DD)", http.StatusBadRequest)
			return
		}
		resolver := minterService.NewDayResolver(
			repo,
			minterService.NewScanEventRideSource(db),
			voucherService.NewService(repo),
		)
		resolutions, err := resolver.ResolveDay(r.Context(), req.Date)
		if err != nil {
			http.Error(w, "resolve-day failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"date":        req.Date,
			"resolutions": resolutions,
		})
	})

	// POST /api/auth/google — zkLogin: exchange Google JWT for Sui address
	mux.HandleFunc("/api/auth/google", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct{ Token string `json:"token"` }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}

		suiAddr, ek, proof, err := sui.DeriveSuiAddressFromJWT(r.Context(), req.Token)
		if err != nil {
			http.Error(w, "zkLogin failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		// Extract email from JWT (simplified — in prod, verify the JWT properly)
		email := "user@example.com" // placeholder: parse from JWT claims
		user, err := repo.CreateUser(r.Context(), email)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = repo.UpdateSuiAccount(r.Context(), user.ID, suiAddr, ek, proof)

		_ = json.NewEncoder(w).Encode(map[string]string{
			"user_id":     fmt.Sprintf("%d", user.ID),
			"sui_address": suiAddr,
		})
	})

	srv := &http.Server{Addr: ":8083", Handler: mux}
	go func() { log.Info().Msg("minter API listening on :8083"); _ = srv.ListenAndServe() }()
	<-ctx.Done()
	_ = srv.Shutdown(context.Background())
}
