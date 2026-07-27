// Package health provides HTTP health check handlers for all services.
package health

import (
	"context"
	"encoding/json"
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

func (h *HealthChecker) AddCheck(c Checker) {
	h.checks = append(h.checks, c)
}

func (h *HealthChecker) HealthzHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		for _, check := range h.checks {
			if err := check(ctx); err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				json.NewEncoder(w).Encode(Status{
					Status:    "unhealthy",
					Service:   h.service,
					Timestamp: time.Now().UnixMilli(),
				})
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(Status{
			Status:    "healthy",
			Service:   h.service,
			Timestamp: time.Now().UnixMilli(),
		})
	}
}

func (h *HealthChecker) ReadyzHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(Status{
			Status:    "ready",
			Service:   h.service,
			Timestamp: time.Now().UnixMilli(),
		})
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
