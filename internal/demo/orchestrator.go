package demo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/Logiqode/ThemeParkNFT/internal/config"
	"github.com/Logiqode/ThemeParkNFT/internal/health"
	kafkapkg "github.com/Logiqode/ThemeParkNFT/internal/kafka"
	"github.com/Logiqode/ThemeParkNFT/internal/minter"
	"github.com/Logiqode/ThemeParkNFT/internal/postgres"
	redispkg "github.com/Logiqode/ThemeParkNFT/internal/redis"
	"github.com/Logiqode/ThemeParkNFT/internal/sui"
	"github.com/Logiqode/ThemeParkNFT/internal/voucher"
)

// Explorer URL templates (Suiscan testnet, R-demo config).
const (
	suiscanTxURL     = "https://suiscan.xyz/testnet/tx/"
	suiscanObjectURL = "https://suiscan.xyz/testnet/object/"
)

// Orchestrator is the demo server's dependency bundle. It exposes the run/reset/
// wallet/health endpoints consumed by the React frontend.
// namedCheck pairs a readiness Checker with a human-readable service name so
// the demo health endpoint can report per-service status.
type namedCheck struct {
	name  string
	check func(context.Context) error
}

type Orchestrator struct {
	db      *sqlx.DB
	repo    *postgres.Repository
	redis   *redispkg.Client
	kafka   *kafkapkg.Producer
	sui     sui.Minter
	reader  sui.Reader
	meta    minter.MetadataProvider
	voucher *voucher.Service

	walletSecret    string
	probeAmountMist string
	checker         *health.HealthChecker
	checks          []namedCheck
}

// New builds the demo orchestrator from already-constructed dependencies.
func New(
	db *sqlx.DB,
	redis *redispkg.Client,
	kafka *kafkapkg.Producer,
	suiClient sui.Minter,
	reader sui.Reader,
	meta minter.MetadataProvider,
	cfg config.Config,
) (*Orchestrator, error) {
	if suiClient == nil {
		return nil, fmt.Errorf("sui client required")
	}
	if reader == nil {
		return nil, fmt.Errorf("sui reader required")
	}
	repo := postgres.NewRepository(db)
	secret := cfg.Auth.DeterministicWalletSecret
	if secret == "" {
		secret = cfg.Auth.EncryptionKey
	}
	probe := cfg.Demo.ProbeAmountMist
	if probe == "" {
		probe = "1000000"
	}

	checks := []namedCheck{
		{name: "postgres", check: func(ctx context.Context) error { return db.PingContext(ctx) }},
		{name: "redis", check: func(ctx context.Context) error { return redis.Ping(ctx) }},
		{name: "kafka", check: func(ctx context.Context) error { return kafka.Ping(ctx) }},
		{name: "sui", check: func(ctx context.Context) error { return suiClient.Ping(ctx) }},
	}

	checker := health.NewHealthChecker("demo")
	for _, c := range checks {
		checker.AddCheck(c.check)
	}

	return &Orchestrator{
		db:              db,
		repo:            repo,
		redis:           redis,
		kafka:           kafka,
		sui:             suiClient,
		reader:          reader,
		meta:            meta,
		voucher:         voucher.NewService(repo),
		walletSecret:    secret,
		probeAmountMist: probe,
		checker:         checker,
		checks:          checks,
	}, nil
}

// Register routes onto the root mux.
func (o *Orchestrator) Register(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", o.checker.HealthzHandler())
	mux.HandleFunc("/readyz", o.checker.ReadyzHandler())
	mux.HandleFunc("/api/demo/health", o.handleHealth)
	mux.HandleFunc("/api/demo/seed", o.handleSeed)
	mux.HandleFunc("/api/demo/run", o.handleRun)
	mux.HandleFunc("/api/demo/reset", o.handleReset)
	mux.HandleFunc("/api/demo/wallet", o.handleWallet)
}

// WaitReady blocks until dependencies are ready (mirrors R2 startup grace).
func (o *Orchestrator) WaitReady(ctx context.Context, timeout time.Duration) error {
	return health.WaitForChecks(ctx, o.checker, timeout)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
