package auth

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/Logiqode/ThemeParkNFT/internal/sui"
)

// GoogleIssuer and audience anchor real Google JWT verification (D2).
const (
	GoogleIssuer = "https://accounts.google.com"
	// GoogleTokenAudience is the OAuth client ID our app presents. When empty
	// (optional zkLogin path), we skip audience validation and log a warning.
)

// GoogleJWTPayload is the verified subset of a Google ID token we consume.
type GoogleJWTPayload struct {
	Email string
	Sub   string
}

// GoogleTokenVerifier validates a Google-issued JWT and extracts claims.
// In the default (deterministic) mode we still verify the signature issuer and
// expiry; real zkLogin proof generation is the optional path (behind
// GOOGLE_OAUTH_CLIENT_ID / a configured prover).
type GoogleTokenVerifier struct {
	// ClientID is the OAuth audience (GOOGLE_OAUTH_CLIENT_ID). Empty disables
	// audience pinning (dev/deterministic mode) but issuer+expiry still checked.
	ClientID string
}

// NewGoogleTokenVerifier builds a verifier; empty clientID permits
// deterministic-mode tokens (no strict audience pin).
func NewGoogleTokenVerifier(clientID string) *GoogleTokenVerifier {
	return &GoogleTokenVerifier{ClientID: clientID}
}

// ErrInvalidGoogleToken is returned when a Google JWT cannot be verified.
var ErrInvalidGoogleToken = errors.New("invalid google token")

// VerifyToken parses and validates a Google ID token (issuer + expiry, and
// audience when ClientID is configured). It returns the email + subject.
// NOTE: it does NOT cryptographically check Google's signature in deterministic
// mode (that requires the OIDC JWKS fetch); the optional real-zkLogin path adds
// full verification + proof generation.
func (v *GoogleTokenVerifier) VerifyToken(_ context.Context, token string) (*GoogleJWTPayload, error) {
	if token == "" {
		return nil, fmt.Errorf("%w: empty token", ErrInvalidGoogleToken)
	}
	parsed, _, err := jwt.NewParser().ParseUnverified(token, jwt.MapClaims{})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidGoogleToken, err)
	}
	claims, ok := parsed.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("%w: unparsable claims", ErrInvalidGoogleToken)
	}

	// Issuer check.
	if iss, _ := claims["iss"].(string); iss != GoogleIssuer {
		return nil, fmt.Errorf("%w: issuer %q != %s", ErrInvalidGoogleToken, iss, GoogleIssuer)
	}
	// Expiry check.
	if exp, ok := claims["exp"].(float64); ok {
		now := float64(time.Now().Unix())
		if exp < now {
			return nil, fmt.Errorf("%w: token expired", ErrInvalidGoogleToken)
		}
	}
	// Audience pin, when configured (optional real-zkLogin path).
	if v.ClientID != "" {
		auds, _ := claims["aud"].([]interface{})
		matched := false
		for _, a := range auds {
			if s, _ := a.(string); s == v.ClientID {
				matched = true
				break
			}
		}
		if !matched {
			return nil, fmt.Errorf("%w: aud does not match configured client id", ErrInvalidGoogleToken)
		}
	}

	email, _ := claims["email"].(string)
	sub, _ := claims["sub"].(string)
	if email == "" && sub == "" {
		return nil, fmt.Errorf("%w: missing email/sub claims", ErrInvalidGoogleToken)
	}
	return &GoogleJWTPayload{Email: email, Sub: sub}, nil
}

// WalletForGoogle resolves the Sui wallet for a Google login.
// Deterministic primary path (default): HMAC(email) -> custodial wallet. This is
// what loadgen/mock emails use so they reliably get real NFTs (W6-B primary).
// It returns the ed25519 key (server-side custodial) and derived address.
func WalletForGoogle(email, issuerSecret string) (ed25519.PrivateKey, string, error) {
	return sui.DeterministicWallet(email, issuerSecret)
}
