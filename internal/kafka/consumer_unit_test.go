package kafka

import (
	"testing"
	"time"
)

// TestBackoffFor verifies the exponential backoff progression (0.5s → 1s → 2s).
func TestBackoffFor(t *testing.T) {
	c := &Consumer{backoff: 500 * time.Millisecond}
	want := []time.Duration{
		500 * time.Millisecond,
		1000 * time.Millisecond,
		2000 * time.Millisecond,
		4000 * time.Millisecond,
	}
	for i, w := range want {
		if got := c.backoffFor(i); got != w {
			t.Errorf("backoffFor(%d) = %v, want %v", i, got, w)
		}
	}
}
