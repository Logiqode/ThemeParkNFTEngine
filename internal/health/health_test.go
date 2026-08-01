package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHealthzIsLiveness(t *testing.T) {
	// Even with failing dependency checks, healthz must return 200 (pure liveness).
	checker := NewHealthChecker("svc")
	checker.AddCheck(func(ctx context.Context) error { return errors.New("dep down") })

	rr := httptest.NewRecorder()
	checker.HealthzHandler()(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rr.Code != http.StatusOK {
		t.Errorf("HealthzHandler() = %d, want 200 (liveness ignores deps)", rr.Code)
	}
	var s Status
	if err := json.Unmarshal(rr.Body.Bytes(), &s); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if s.Status != "healthy" {
		t.Errorf("Status = %q, want healthy", s.Status)
	}
}

func TestReadyzIsStrict(t *testing.T) {
	fail := NewHealthChecker("svc")
	fail.AddCheck(func(ctx context.Context) error { return errors.New("redis down") })
	rr := httptest.NewRecorder()
	fail.ReadyzHandler()(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("ReadyzHandler(fail) = %d, want 503 (strict R2)", rr.Code)
	}

	pass := NewHealthChecker("svc")
	pass.AddCheck(func(ctx context.Context) error { return nil })
	rr2 := httptest.NewRecorder()
	pass.ReadyzHandler()(rr2, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr2.Code != http.StatusOK {
		t.Errorf("ReadyzHandler(pass) = %d, want 200", rr2.Code)
	}
}

func TestWaitForChecksTimeout(t *testing.T) {
	checker := NewHealthChecker("svc")
	checker.AddCheck(func(ctx context.Context) error { return errors.New("never ready") })
	if err := WaitForChecks(context.Background(), checker, 50*time.Millisecond); err == nil {
		t.Error("WaitForChecks() = nil, want timeout error")
	}
}

func TestWaitForChecksPassesImmediately(t *testing.T) {
	checker := NewHealthChecker("svc")
	checker.AddCheck(func(ctx context.Context) error { return nil })
	if err := WaitForChecks(context.Background(), checker, time.Second); err != nil {
		t.Errorf("WaitForChecks() = %v, want nil", err)
	}
}