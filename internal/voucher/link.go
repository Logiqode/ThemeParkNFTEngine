package voucher

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Link signing/verification for voucher magic links (Q6/R13, share & claim).
// Extracted from cmd/voucher so the expiry/invalid handling is unit-testable
// (M4.6) and shared by production and tests.

var (
	// ErrInvalidLink is returned when a magic-link token is missing, malformed,
	// or signed incorrectly.
	ErrInvalidLink = errors.New("invalid magic link")
	// ErrLinkExpired is returned when the magic-link token is past its expiry.
	ErrLinkExpired = errors.New("magic link expired")
)

// SignMagicLink signs a JWT carrying the voucher_id with the given HMAC secret
// and a TTL. A zero ttl defaults to 24h; a negative ttl produces an
// already-expired link (used to test M4.6 rejection).
func SignMagicLink(secret, voucherID string, ttl time.Duration) (string, error) {
	if ttl == 0 {
		ttl = 24 * time.Hour // default claim behaviour; a negative ttl intentionally yields an already-expired link
	}
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"voucher_id": voucherID,
		"exp":        time.Now().Add(ttl).Unix(),
	})
	return t.SignedString([]byte(secret))
}

// VerifyMagicLink parses and validates a magic-link token, returning the embedded
// voucher_id. Returns ErrInvalidLink for a missing/badly-signed token and
// ErrLinkExpired when the token is valid but past its expiry (M4.6).
func VerifyMagicLink(secret, token string) (string, error) {
	if token == "" {
		return "", ErrInvalidLink
	}
	parsed, err := jwt.Parse(token, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return []byte(secret), nil
	})
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return "", ErrLinkExpired
		}
		return "", ErrInvalidLink
	}
	if !parsed.Valid {
		return "", ErrInvalidLink
	}
	claims, ok := parsed.Claims.(jwt.MapClaims)
	if !ok {
		return "", ErrInvalidLink
	}
	// Explicitly enforce expiry (M4.6) — jwt v5's MapClaims does not always
	// reject an expired token at parse time, so we check it ourselves.
	if exp, err := claims.GetExpirationTime(); err == nil && exp != nil && !exp.After(time.Now()) {
		return "", ErrLinkExpired
	}
	voucherID, _ := claims["voucher_id"].(string)
	if voucherID == "" {
		return "", ErrInvalidLink
	}
	return voucherID, nil
}
