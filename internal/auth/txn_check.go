// Package auth defines the transaction-check abstraction used by the gate's
// NFC wristband binding flow (R16).
//
// The gate's second staff scan (POST /api/wristband/nfc-check) must confirm that
// the visitor's theme-park account can push real-world "NFC scan → NFT Proof of
// Visit" transactions from their zkLogin wallet bound to Google. This package
// exposes a mockable interface so Week 1–2 tests and benchmarks never touch the
// Sui testnet:
//
//   - MockTxnCheck — always-pass (or configurable failure) for local dev, CI, benchmarks.
//   - Real zkLogin implementation arrives in Week 6 (replaces stub Sui derivation).
//
// The interface also enforces the right-to-be-forgotten contract (R11): the
// check operates on a pseudonymous account reference (e.g. email hash or DB id),
// never on raw PII.
package auth

import (
	"context"
	"errors"
)

// ErrTxnCheckFailed indicates the account's wallet cannot currently perform
// on-chain transactions (e.g. zkLogin credentials absent, gas pool unfunded,
// JWT revoked, or the NFC chip is faulty).
var ErrTxnCheckFailed = errors.New("transaction check failed")

// TxnCheckPerformer answers the question: "can this account's zkLogin wallet
// push real-world NFC-scan → NFT Proof of Visit transactions right now?"
type TxnCheckPerformer interface {
	// CheckTxnCapability returns nil if the account can transact, or
	// ErrTxnCheckFailed (wrapped with a reason) if it cannot.
	CheckTxnCapability(ctx context.Context, accountRef string) error
}

// MockTxnCheck is a test/demo implementation (R16): it always passes unless
// configured to fail. No blockchain interaction occurs, so benchmarks and CI
// never spam the testnet.
type MockTxnCheck struct {
	// FailWhen is an optional identifier that, when matched against
	// accountRef, makes the check return ErrTxnCheckFailed. Empty = never fail.
	FailWhen string
}

// CheckTxnCapability implements TxnCheckPerformer.
func (m *MockTxnCheck) CheckTxnCapability(ctx context.Context, accountRef string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if m.FailWhen != "" && m.FailWhen == accountRef {
		return ErrTxnCheckFailed
	}
	return nil
}