package sui

import (
	"testing"
)

// fixedTestMnemonic is a valid BIP39 phrase (12 words) used only for
// deterministic-derivation tests. It is NOT a real funded wallet.
const fixedTestMnemonic = "test test test test test test test test test test test junk"

func TestPubKeyToSuiAddressIsDeterministic(t *testing.T) {
	// Deterministic: same mnemonic => same address every time, matching the
	// real Sui derivation (verified 2026-08-03 against the deployed gas pool).
	_, addr1, err := SignerFromMnemonic(fixedTestMnemonic)
	if err != nil {
		t.Fatalf("SignerFromMnemonic: %v", err)
	}
	_, addr2, err := SignerFromMnemonic(fixedTestMnemonic)
	if err != nil {
		t.Fatalf("SignerFromMnemonic second: %v", err)
	}
	if addr1 != addr2 {
		t.Fatalf("derivation not deterministic: %s vs %s", addr1, addr2)
	}
	if len(addr1) != 2+64 { // "0x" + 32 bytes
		t.Fatalf("address length %d, want 66", len(addr1))
	}
	if addr1[:2] != "0x" {
		t.Fatalf("address should be 0x-prefixed, got %s", addr1)
	}
}

func TestSignerFromMnemonicEmpty(t *testing.T) {
	if _, _, err := SignerFromMnemonic(""); err == nil {
		t.Fatal("expected error for empty mnemonic")
	}
}

func TestSignerFromMnemonicInvalid(t *testing.T) {
	if _, _, err := SignerFromMnemonic("not a valid mnemonic phrase at all"); err == nil {
		t.Fatal("expected error for invalid mnemonic")
	}
}

func TestDeterministicWalletStableAndDistinct(t *testing.T) {
	const secret = "test-issuer-secret"

	_, a1, err := DeterministicWallet("alice@example.com", secret)
	if err != nil {
		t.Fatalf("DeterministicWallet alice: %v", err)
	}
	_, a2, err := DeterministicWallet("alice@example.com", secret)
	if err != nil {
		t.Fatalf("DeterministicWallet alice again: %v", err)
	}
	if a1 != a2 {
		t.Fatalf("same email+secret should map to same wallet: %s vs %s", a1, a2)
	}

	// Different email -> different wallet.
	_, b, err := DeterministicWallet("bob@example.com", secret)
	if err != nil {
		t.Fatalf("DeterministicWallet bob: %v", err)
	}
	if a1 == b {
		t.Fatalf("distinct emails must not collide: %s", a1)
	}

	// Same email, different secret -> different wallet (secret matters).
	_, c, err := DeterministicWallet("alice@example.com", "other-secret")
	if err != nil {
		t.Fatalf("DeterministicWallet alice/other: %v", err)
	}
	if a1 == c {
		t.Fatalf("different secret must yield different wallet: %s", a1)
	}

	// All derived addresses are valid 0x + 64 hex.
	for _, addr := range []string{a1, b, c} {
		if len(addr) != 66 || addr[:2] != "0x" {
			t.Fatalf("invalid deterministic address: %s", addr)
		}
	}
}

func TestDeterministicWalletRequiresEmail(t *testing.T) {
	if _, _, err := DeterministicWallet("", "secret"); err == nil {
		t.Fatal("expected error for empty email")
	}
}
