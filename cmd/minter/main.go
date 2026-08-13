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

	"github.com/Logiqode/ThemeParkNFT/internal/auth"
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
	// The gas-pool signer is required to mint (D4: derived from mnemonic).
	if cfg.Sui.GasPoolMnemonic == "" {
		log.Warn().Msg("SUI_GAS_POOL_MNEMONIC empty — mints will fail; deterministic derivation still available for address mapping")
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

	var sui suiClient.Minter
	suiClientReal, suiErr := suiClient.NewClient(cfg.Sui)
	if suiErr != nil {
		log.Warn().Err(suiErr).Msg("sui client init deferred (mint endpoints will 503); deterministic address mapping still available")
	} else {
		sui = suiClientReal
	}

	// IPFS pinning (Option A, CIDCache) + deterministic metadata adapter.
	pinata := storage.NewPinataClient(cfg.Pinata.APIKey, cfg.Pinata.APISecret, cfg.Pinata.JWT, cfg.Pinata.Gateway)
	cidCache := storage.NewCIDCache(pinata)
	meta := minterService.NewCIDMetadataProvider(cidCache)

	// Deterministic wallet secret (W6-B): HMAC email->wallet seed.
	detSecret := cfg.Auth.DeterministicWalletSecret
	if detSecret == "" {
		detSecret = cfg.Auth.EncryptionKey
	}
	googleVerifier := auth.NewGoogleTokenVerifier(cfg.Auth.GoogleOAuthClientID)

	checker := health.NewHealthChecker("minter")
	checker.AddCheck(func(ctx context.Context) error { return db.PingContext(ctx) })
	checker.AddCheck(func(ctx context.Context) error { return redis.Ping(ctx) })
	if sui != nil {
		checker.AddCheck(func(ctx context.Context) error { return sui.Ping(ctx) })
	}

	if err := health.WaitForChecks(ctx, checker, 20*time.Second); err != nil {
		log.Fatal().Err(err).Msg("minter startup: dependencies not ready")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", checker.HealthzHandler())
	mux.HandleFunc("/readyz", checker.ReadyzHandler())

	// POST /api/auth/google — Week 6 (W6-B): real Google JWT parse (D2) +
	// deterministic custodial wallet derivation. Same email + same secret => same
	// Sui address, so mock emails reliably receive real NFTs in benchmarks.
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

		payload, err := googleVerifier.VerifyToken(r.Context(), req.Token)
		if err != nil {
			http.Error(w, "google token invalid: "+err.Error(), http.StatusUnauthorized)
			return
		}
		if payload.Email == "" {
			http.Error(w, "google token missing email claim", http.StatusUnauthorized)
			return
		}

		// Deterministic custodial wallet from the verified email (W6-B primary).
		_, suiAddr, err := suiClient.DeterministicWallet(payload.Email, detSecret)
		if err != nil {
			http.Error(w, "wallet derivation failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		user, err := repo.CreateUser(r.Context(), payload.Email)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// Persist the derived address. Ephemeral/zk proof fields left empty in
		// deterministic mode (custodial key is server-derived, not stored).
		if err := repo.UpdateSuiAccount(r.Context(), user.ID, suiAddr, "", ""); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{
			"user_id":     fmt.Sprintf("%d", user.ID),
			"email":       payload.Email,
			"sui_address": suiAddr,
		})
	})

	// POST /mint/resolve-day — OFF-CHAIN resolution (M4.12, unchanged).
	mux.HandleFunc("/mint/resolve-day", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct{ Date string `json:"date"` }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if req.Date == "" {
			http.Error(w, "date required (YYYY-MM-DD)", http.StatusBadRequest)
			return
		}
		resolver := minterService.NewDayResolver(repo, minterService.NewScanEventRideSource(db), voucherService.NewService(repo))
		resolutions, err := resolver.ResolveDay(r.Context(), req.Date)
		if err != nil {
			http.Error(w, "resolve-day failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"date": req.Date, "resolutions": resolutions})
	})

	// POST /mint/run-day — Week 6 (M6.2/M6.3/M6.6): resolves the day, then mints
	// every mint-ready participant (incl. dependents -> guardian custodial
	// wallet, M6.6) via the real Sui client, recording idempotent mint_logs.
	mux.HandleFunc("/mint/run-day", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if sui == nil {
			http.Error(w, "sui client not initialized (check SUI_GAS_POOL_MNEMONIC)", http.StatusServiceUnavailable)
			return
		}
		var req struct{ Date string `json:"date"` }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if req.Date == "" {
			http.Error(w, "date required (YYYY-MM-DD)", http.StatusBadRequest)
			return
		}

		resolver := minterService.NewDayResolver(repo, minterService.NewScanEventRideSource(db), voucherService.NewService(repo))
		resolutions, err := resolver.ResolveDay(r.Context(), req.Date)
		if err != nil {
			http.Error(w, "resolve-day failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		// Build the mint-ready live set.
		var lives []minterService.MintLive
		for _, res := range resolutions {
			if res.Outcome != minterService.OutcomeMintReady || res.WalletAddress == "" {
				continue
			}
			lives = append(lives, minterService.MintLive{
				ParticipantID: res.ParticipantID,
				WalletAddress: res.WalletAddress,
				RideIDs:       res.RideIDs,
				Date:          req.Date,
			})
		}

		batch := minterService.NewBatchMint(sui, meta, repo)
		results := batch.Run(r.Context(), lives)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"date":    req.Date,
			"results": results,
			"details": resolutions,
		})
	})

	// POST /mint/claim-custody — Week 6 (M6.7 / R33): real Sui object transfer
	// of a dependent's NFT from the guardian custodial wallet to a newly-linked
	// own wallet. nft_object_ids are the AttendanceNFT objects held in the
	// custodial wallet to transfer.
	mux.HandleFunc("/mint/claim-custody", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if sui == nil {
			http.Error(w, "sui client not initialized (check SUI_GAS_POOL_MNEMONIC)", http.StatusServiceUnavailable)
			return
		}
		var req struct {
			ParticipantID int64    `json:"participant_id"`
			OwnWallet     string   `json:"own_wallet"`
			NFTObjectIDs  []string `json:"nft_object_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if req.ParticipantID == 0 || req.OwnWallet == "" || len(req.NFTObjectIDs) == 0 {
			http.Error(w, "participant_id, own_wallet, nft_object_ids required", http.StatusBadRequest)
			return
		}

		// 1) On-chain: transfer each NFT object to the dependent's own wallet.
		digests := make([]string, 0, len(req.NFTObjectIDs))
		for _, objID := range req.NFTObjectIDs {
			d, err := sui.TransferNFT(r.Context(), objID, req.OwnWallet)
			if err != nil {
				http.Error(w, fmt.Sprintf("custody transfer of %s failed: %v", objID, err), http.StatusInternalServerError)
				return
			}
			digests = append(digests, d)
		}

		// 2) Off-chain: flip participant to own wallet + attribute.
		v := voucherService.NewService(repo)
		p, err := v.ClaimCustody(r.Context(), req.ParticipantID, req.OwnWallet)
		if err != nil {
			http.Error(w, "claim-custody state update failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"participant_id": p.ID,
			"own_wallet":     p.CustodialWalletAddress,
			"tx_digests":     digests,
			"status":         "transferred",
		})
	})

	srv := &http.Server{Addr: ":8083", Handler: mux}
	go func() { log.Info().Msg("minter API listening on :8083"); _ = srv.ListenAndServe() }()
	<-ctx.Done()
	_ = srv.Shutdown(context.Background())
}
