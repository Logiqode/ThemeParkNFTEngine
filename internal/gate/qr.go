package gate

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/Logiqode/ThemeParkNFT/internal/config"
)

// QRToken represents a rotating HMAC-signed QR payload.
type QRToken struct {
	UUID      string `json:"uuid"`
	Timestamp int64  `json:"timestamp"`
	Signature string `json:"signature"`
}

// GenerateQROTP generates a new HMAC-signed QR payload with a 30s expiry.
func GenerateQROTP(cfg config.GateConfig) *QRToken {
	ts := time.Now().Unix()
	uid := uuid.New().String()
	payload := fmt.Sprintf("%s|%d", uid, ts)
	sig := hmacSHA256([]byte(cfg.HMACSecret), []byte(payload))
	return &QRToken{UUID: uid, Timestamp: ts, Signature: sig}
}

// VerifyQROTP validates the HMAC signature and checks the 30s rotation window.
func VerifyQROTP(cfg config.GateConfig, token *QRToken) (bool, error) {
	payload := fmt.Sprintf("%s|%d", token.UUID, token.Timestamp)
	expected := hmacSHA256([]byte(cfg.HMACSecret), []byte(payload))
	if !hmac.Equal([]byte(expected), []byte(token.Signature)) {
		return false, fmt.Errorf("invalid HMAC signature")
	}
	age := time.Now().Unix() - token.Timestamp
	if age < 0 {
		age = -age
	}
	if age > int64(cfg.QRRotationSeconds) {
		return false, fmt.Errorf("QR code expired (%ds old, max %ds)", age, cfg.QRRotationSeconds)
	}
	return true, nil
}

func hmacSHA256(key, data []byte) string {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return hex.EncodeToString(mac.Sum(nil))
}
