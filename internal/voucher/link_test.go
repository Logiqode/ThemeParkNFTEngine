package voucher

import (
	"errors"
	"testing"
	"time"
)

func TestMagicLinkRoundTrip(t *testing.T) {
	tok, err := SignMagicLink("s3cret", "v-abc", time.Hour)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	voucherID, err := VerifyMagicLink("s3cret", tok)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if voucherID != "v-abc" {
		t.Fatalf("expected v-abc, got %q", voucherID)
	}
}

func TestMagicLinkExpiredIsRejected(t *testing.T) {
	// Negative TTL → exp in the past → must be rejected as expired (M4.6).
	tok, err := SignMagicLink("s3cret", "v-old", -time.Minute)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	if _, err := VerifyMagicLink("s3cret", tok); !errors.Is(err, ErrLinkExpired) {
		t.Fatalf("expected ErrLinkExpired, got %v", err)
	}
}

func TestMagicLinkInvalidIsRejected(t *testing.T) {
	valid, err := SignMagicLink("s3cret", "v-1", time.Hour)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	cases := []struct {
		name   string
		secret string
		token  string
	}{
		{"wrong secret", "other-secret", valid},
		{"garbage token", "s3cret", "not.a.jwt"},
		{"empty token", "s3cret", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := VerifyMagicLink(tc.secret, tc.token); !errors.Is(err, ErrInvalidLink) {
				t.Fatalf("expected ErrInvalidLink, got %v", err)
			}
		})
	}
}
