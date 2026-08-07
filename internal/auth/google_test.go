package auth

import (
	"context"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// signGoogleLike builds a syntactically-valid JWT with the given issuer.
// Signature is not cryptographically validated in deterministic mode.
func signGoogleLike(t *testing.T, iss string, claims map[string]interface{}, exp time.Time) string {
	t.Helper()
	m := jwt.MapClaims{
		"iss":   iss,
		"sub":   "1234567890",
		"email": "user@example.com",
		"aud":   []interface{}{"test-client-id"},
		"exp":   float64(exp.Unix()),
	}
	for k, v := range claims {
		m[k] = v
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, m)
	s, err := tok.SignedString([]byte("test-secret"))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return s
}

func TestVerifyTokenValid(t *testing.T) {
	tok := signGoogleLike(t, GoogleIssuer, nil, time.Now().Add(time.Hour))
	v := NewGoogleTokenVerifier("")
	p, err := v.VerifyToken(context.Background(), tok)
	if err != nil {
		t.Fatalf("VerifyToken: %v", err)
	}
	if p.Email != "user@example.com" {
		t.Fatalf("email = %q", p.Email)
	}
}

func TestVerifyTokenWrongIssuer(t *testing.T) {
	tok := signGoogleLike(t, "https://evil.example.com", nil, time.Now().Add(time.Hour))
	v := NewGoogleTokenVerifier("")
	if _, err := v.VerifyToken(context.Background(), tok); err == nil {
		t.Fatal("expected error for wrong issuer")
	}
}

func TestVerifyTokenExpired(t *testing.T) {
	tok := signGoogleLike(t, GoogleIssuer, nil, time.Now().Add(-time.Hour))
	v := NewGoogleTokenVerifier("")
	if _, err := v.VerifyToken(context.Background(), tok); err == nil {
		t.Fatal("expected error for expired token")
	}
}

func TestVerifyTokenEmpty(t *testing.T) {
	v := NewGoogleTokenVerifier("")
	if _, err := v.VerifyToken(context.Background(), ""); err == nil {
		t.Fatal("expected error for empty token")
	}
}

func TestVerifyTokenAudiencePinWhenConfigured(t *testing.T) {
	tok := signGoogleLike(t, GoogleIssuer, map[string]interface{}{"aud": []interface{}{"other-client"}}, time.Now().Add(time.Hour))
	v := NewGoogleTokenVerifier("test-client-id")
	if _, err := v.VerifyToken(context.Background(), tok); err == nil {
		t.Fatal("expected audience mismatch error")
	}

	tokOK := signGoogleLike(t, GoogleIssuer, nil, time.Now().Add(time.Hour))
	if _, err := v.VerifyToken(context.Background(), tokOK); err != nil {
		t.Fatalf("audience-match token rejected: %v", err)
	}
}

func TestVerifyTokenMissingEmailSub(t *testing.T) {
	tok := signGoogleLike(t, GoogleIssuer, map[string]interface{}{"email": "", "sub": ""}, time.Now().Add(time.Hour))
	v := NewGoogleTokenVerifier("")
	if _, err := v.VerifyToken(context.Background(), tok); err == nil {
		t.Fatal("expected error when email and sub both missing")
	}
}
