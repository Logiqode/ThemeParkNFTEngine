package auth

import (
	"context"
	"errors"
	"testing"
)

func TestMockTxnCheckAlwaysPasses(t *testing.T) {
	m := &MockTxnCheck{}
	if err := m.CheckTxnCapability(context.Background(), "user-a"); err != nil {
		t.Errorf("CheckTxnCapability() = %v, want nil (R16 default pass)", err)
	}
}

func TestMockTxnCheckCanFailForTarget(t *testing.T) {
	m := &MockTxnCheck{FailWhen: "faulty-nfc"}
	if err := m.CheckTxnCapability(context.Background(), "faulty-nfc"); !errors.Is(err, ErrTxnCheckFailed) {
		t.Errorf("CheckTxnCapability(faulty-nfc) = %v, want ErrTxnCheckFailed", err)
	}
	// Other accounts still pass.
	if err := m.CheckTxnCapability(context.Background(), "user-b"); err != nil {
		t.Errorf("CheckTxnCapability(user-b) = %v, want nil", err)
	}
}

func TestMockTxnCheckRespectsContext(t *testing.T) {
	m := &MockTxnCheck{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.CheckTxnCapability(ctx, "user-a"); err == nil {
		t.Error("CheckTxnCapability(cancelled ctx) = nil, want error")
	}
}