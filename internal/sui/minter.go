package sui

import (
	"context"
)

// Minter is the Sui-facing surface the batch-mint driver depends on. It
// abstracts the real client so the off-chain driver (internal/minter) and the
// minter HTTP endpoints can be unit-integration tested against a fake without
// touching the testnet (R16 mock-interface philosophy, extended to Week 6).
type Minter interface {
	// MintBatchAttendance submits attendance::mint_batch to the given recipient
	// (own non-custodial OR guardian custodial wallet), returning the tx digest.
	MintBatchAttendance(ctx context.Context, recipient string, rideIDs []string, date string, names, metadataURLs []string) (txDigest string, err error)

	// TransferNFT performs the R33/M6.7 custody object transfer of one NFT from
	// the current owner (custodial wallet) to a new recipient, returning the
	// tx digest.
	TransferNFT(ctx context.Context, nftObjectID, toAddress string) (txDigest string, err error)

	// Ping verifies Sui RPC connectivity (readiness, R2).
	Ping(ctx context.Context) error
}

// compile-time check that *Client satisfies Minter.
var _ Minter = (*Client)(nil)
