package demo

import (
	"encoding/json"
	"net/http"
	"time"
)

// handleHealth aggregates readiness of PG/Redis/Kafka/minter/Sui RPC into a
// single machine-readable response consumed by the frontend health gate.
func (o *Orchestrator) handleHealth(w http.ResponseWriter, r *http.Request) {
	type svc struct {
		Healthy bool   `json:"healthy"`
		Error   string `json:"error,omitempty"`
	}
	healthy := true
	services := map[string]svc{}
	for _, c := range o.checks {
		err := c.check(r.Context())
		s := svc{Healthy: err == nil}
		if err != nil {
			s.Error = err.Error()
			healthy = false
		}
		services[c.name] = s
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"healthy":  healthy,
		"services": services,
		"ts":       time.Now().UTC().Format(time.RFC3339),
	})
}

// handleSeed handles POST /api/demo/seed {scenario}.
func (o *Orchestrator) handleSeed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Scenario Scenario `json:"scenario"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json")
		return
	}
	res, err := o.Seed(r.Context(), req.Scenario)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleRun handles POST /api/demo/run {scenario}.
func (o *Orchestrator) handleRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Scenario Scenario `json:"scenario"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json")
		return
	}
	res, err := o.Run(r.Context(), req.Scenario)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleReset handles POST /api/demo/reset (full-stack reset).
func (o *Orchestrator) handleReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	res, err := o.Reset(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleWallet handles GET /api/demo/wallet?address=0x….
func (o *Orchestrator) handleWallet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	address := r.URL.Query().Get("address")
	if address == "" {
		writeErr(w, http.StatusBadRequest, "address query parameter required")
		return
	}
	res, err := o.Wallet(r.Context(), address)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}
