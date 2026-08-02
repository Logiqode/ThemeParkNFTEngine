package gate

import (
	"testing"
	"time"

	"github.com/Logiqode/ThemeParkNFT/internal/config"
)

const testHMACSecret = "0123456789abcdef0123456789abcdef" // 32 bytes

func TestQROTPRoundTrip(t *testing.T) {
	cfg := config.GateConfig{HMACSecret: testHMACSecret, QRRotationSeconds: 30}
	token := GenerateQROTP(cfg, "ticket-001")
	if token.TicketID != "ticket-001" || token.UUID == "" || token.Timestamp == 0 || token.Signature == "" {
		t.Fatalf("GenerateQROTP returned incomplete token: %+v", token)
	}
	ok, err := VerifyQROTP(cfg, token)
	if err != nil || !ok {
		t.Fatalf("VerifyQROTP() = %v, %v; want true, nil", ok, err)
	}
}

func TestQROTPTamperedSignature(t *testing.T) {
	cfg := config.GateConfig{HMACSecret: testHMACSecret, QRRotationSeconds: 30}
	token := GenerateQROTP(cfg, "ticket-001")
	token.Signature = "deadbeef"
	if ok, _ := VerifyQROTP(cfg, token); ok {
		t.Error("VerifyQROTP(tampered) = true, want false")
	}
}

func TestQROTPTamperedTicketID(t *testing.T) {
	cfg := config.GateConfig{HMACSecret: testHMACSecret, QRRotationSeconds: 30}
	token := GenerateQROTP(cfg, "ticket-001")
	token.TicketID = "ticket-999" // re-target attempt (R18) must fail verification
	if ok, _ := VerifyQROTP(cfg, token); ok {
		t.Error("VerifyQROTP(tampered ticket_id) = true, want false")
	}
}

func TestQROTPExpired(t *testing.T) {
	cfg := config.GateConfig{HMACSecret: testHMACSecret, QRRotationSeconds: 30}
	token := GenerateQROTP(cfg, "ticket-001")
	token.Timestamp = time.Now().Unix() - 31
	if ok, _ := VerifyQROTP(cfg, token); ok {
		t.Error("VerifyQROTP(expired) = true, want false")
	}
}

func TestQROTPWrongKey(t *testing.T) {
	cfg := config.GateConfig{HMACSecret: testHMACSecret, QRRotationSeconds: 30}
	other := config.GateConfig{HMACSecret: "fedcba9876543210fedcba9876543210", QRRotationSeconds: 30}
	token := GenerateQROTP(cfg, "ticket-001")
	if ok, _ := VerifyQROTP(other, token); ok {
		t.Error("VerifyQROTP(wrong key) = true, want false")
	}
}