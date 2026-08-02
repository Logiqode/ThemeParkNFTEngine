package redis

import (
	"testing"
	"time"
)

// TestBindingExpiry ensures the disposable wristband link (R19) expires at the
// end of Day+1 — always in the future, less than the end of Day+2.
func TestBindingExpiry(t *testing.T) {
	got := bindingExpiry()
	now := time.Now()

	if !got.After(now) {
		t.Fatalf("bindingExpiry() = %v, not in the future", got)
	}

	// Must be before the start of day+2 (i.e. within tomorrow's close).
	dayAfterTomorrowStart := time.Date(now.Year(), now.Month(), now.Day()+2, 0, 0, 0, 0, now.Location())
	if !got.Before(dayAfterTomorrowStart) {
		t.Fatalf("bindingExpiry() = %v, should be before start of day+2 %v", got, dayAfterTomorrowStart)
	}

	// Must be no earlier than tomorrow's start (always ≥ ~24h away).
	tomorrowStart := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
	if got.Before(tomorrowStart) {
		t.Fatalf("bindingExpiry() = %v, unexpectedly before start of day+1", got)
	}
}
