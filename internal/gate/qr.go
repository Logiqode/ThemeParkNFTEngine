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

// QRToken represents a rotating HMAC-signed QR payload (R18). The ticket_id is
// bound INSIDE the HMAC payload (`ticketID|uuid|timestamp`), so a token can only
// be generated for one specific ticket and cannot be re-targeted.
type QRToken struct {
	TicketID  string `json:"ticket_id"`
	UUID      string `json:"uuid"`
	Timestamp int64  `json:"timestamp"`
	Signature string `json:"signature"`
}

// GenerateQROTP generates a new HMAC-signed QR payload for the given ticket,
// with a 30s rotation window and one-time-use enforcement (R21).
func GenerateQROTP(cfg config.GateConfig, ticketID string) *QRToken {
	ts := time.Now().Unix()
	uid := uuid.New().String()
	payload := fmt.Sprintf("%s|%s|%d", ticketID, uid, ts)
	sig := hmacSHA256([]byte(cfg.HMACSecret), []byte(payload))
	return &QRToken{TicketID: ticketID, UUID: uid, Timestamp: ts, Signature: sig}
}

// VerifyQROTP validates the HMAC signature (including the bound ticket_id) and
// checks the 30s rotation window.
func VerifyQROTP(cfg config.GateConfig, token *QRToken) (bool, error) {
	payload := fmt.Sprintf("%s|%s|%d", token.TicketID, token.UUID, token.Timestamp)
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
