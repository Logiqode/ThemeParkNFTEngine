package sui

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"golang.org/x/crypto/blake2b"

	"github.com/block-vision/sui-go-sdk/signer"
	"github.com/tyler-smith/go-bip39"
)

// SUI_HARDENED_DERIVATION_PATH is the BIP44-style path Sui uses to derive
// an account key from a mnemonic seed (hardened ed25519). Matches the Sui CLI
// `sui keytool` default and the deployed gas-pool wallet.
const SUI_HARDENED_DERIVATION_PATH = "m/44'/784'/0'/0'/0'"

// ed25519 scheme flag used in Sui's address = blake2b(0x00 || pubkey) scheme.
const suiEd25519Scheme byte = 0x00

// PubKeyToSuiAddress derives a real Sui address from an ed25519 public key
// (fixes D3): address = "0x" + blake2b-256(0x00 || pubkey) (32 bytes -> 64 hex).
// Verified 2026-08-03 against the deployed gas-pool wallet.
func PubKeyToSuiAddress(pubKey ed25519.PublicKey) string {
	tmp := make([]byte, len(pubKey)+1)
	tmp[0] = suiEd25519Scheme
	copy(tmp[1:], pubKey)
	h := blake2b.Sum256(tmp)
	return "0x" + hex.EncodeToString(h[:])
}

// SignerFromMnemonic derives the gas-pool signer (ed25519 private key) and its
// Sui address from a BIP39 mnemonic (fixes D4 — no more fragile ~/.sui keystore
// file loading). Empty mnemonic returns an error so callers fail fast.
func SignerFromMnemonic(mnemonic string) (ed25519.PrivateKey, string, error) {
	if mnemonic == "" {
		return nil, "", fmt.Errorf("SUI_GAS_POOL_MNEMONIC not set — cannot derive gas-pool signer")
	}
	seed, err := bip39.NewSeedWithErrorChecking(mnemonic, "")
	if err != nil {
		return nil, "", fmt.Errorf("invalid gas-pool mnemonic: %w", err)
	}
	key, err := signer.DeriveForPath(SUI_HARDENED_DERIVATION_PATH, seed)
	if err != nil {
		return nil, "", fmt.Errorf("derive gas-pool key: %w", err)
	}
	priv := ed25519.NewKeyFromSeed(key.Key)
	addr := PubKeyToSuiAddress(priv.Public().(ed25519.PublicKey))
	return priv, addr, nil
}

// DeterministicWallet derives a deterministic custodial Sui wallet for an email
// (W6-B primary path). The seed = HMAC-SHA256(email, issuerSecret) -> ed25519
// keypair -> real Sui address. Same email + same secret => same wallet, every
// run, so mock/loadgen emails reliably receive real NFTs (reproducible tests
// without live OAuth). Keys are held server-side (custodial); mint gas comes
// from the shared pool.
func DeterministicWallet(email, issuerSecret string) (ed25519.PrivateKey, string, error) {
	if email == "" {
		return nil, "", fmt.Errorf("email required for deterministic wallet")
	}
	mac := hmac.New(sha256.New, []byte(issuerSecret))
	_, _ = mac.Write([]byte(email))
	seed := mac.Sum(nil) // 32 bytes -> valid ed25519 seed
	priv := ed25519.NewKeyFromSeed(seed)
	addr := PubKeyToSuiAddress(priv.Public().(ed25519.PublicKey))
	return priv, addr, nil
}
