// Package health provides HTTP health check handlers for all services.
package health

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Status struct {
	Status    string `json:"status"`
	Service   string `json:"service"`
	Timestamp int64  `json:"timestamp"`
}

type Checker func(context.Context) error

type HealthChecker struct {
	service   string
	checks    []Checker
	startTime time.Time
}

func NewHealthChecker(service string) *HealthChecker {
	return &HealthChecker{
		service:   service,
		startTime: time.Now(),
	}
}

// AddCheck registers a readiness dependency check (Postgres, Redis, Kafka, Sui...).
func (h *HealthChecker) AddCheck(c Checker) {
	h.checks = append(h.checks, c)
}

func writeStatus(w http.ResponseWriter, code int, status string, service string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(Status{
		Status:    status,
		Service:   service,
		Timestamp: time.Now().UnixMilli(),
	})
}

// HealthzHandler is a pure liveness probe: the process is up and serving HTTP.
// Dependency health is reported via /readyz (strict readiness per R2).
func (h *HealthChecker) HealthzHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeStatus(w, http.StatusOK, "healthy", h.service)
	}
}

// ReadyzHandler is a strict readiness probe (R2): returns 503 "not_ready" until
// all registered dependency checks pass, then 200 "ready".
func (h *HealthChecker) ReadyzHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		for _, check := range h.checks {
			if err := check(ctx); err != nil {
				writeStatus(w, http.StatusServiceUnavailable, "not_ready", h.service)
				return
			}
		}
		writeStatus(w, http.StatusOK, "ready", h.service)
	}
}

func StartHealthServer(addr string, checker *HealthChecker) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", checker.HealthzHandler())
	mux.HandleFunc("/readyz", checker.ReadyzHandler())
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()
	return srv
}

// WaitForChecks polls all registered readiness checks until they pass or the
// timeout elapses. Used at startup to give backing services (Postgres, Redis,
// Kafka) time to become ready before the process begins serving (R2 grace).
func WaitForChecks(ctx context.Context, checker *HealthChecker, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("readiness checks did not pass within %s", timeout)
		}

		allPass := true
		checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		for _, check := range checker.checks {
			if err := check(checkCtx); err != nil {
				allPass = false
				break
			}
		}
		cancel()

		if allPass {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}
