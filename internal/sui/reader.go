package sui

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/block-vision/sui-go-sdk/models"
	"github.com/rs/zerolog/log"
)

// attendanceNFTTypeSuffix is the Move struct-type suffix of an
// attendance_nft::attendance::AttendanceNFT object. The full type is built from
// SUI_PACKAGE_ID at runtime (see nftStructType): "0x…::attendance::AttendanceNFT".
const attendanceNFTTypeSuffix = "::attendance::AttendanceNFT"

// NFTInfo is the on-chain identity of an attendance NFT object. It deliberately
// carries no PII (R34) — only the object id, its Move type, owner, and version.
// Ride/date/name attribution is joined off-chain from Postgres (mint_logs) by
// the demo orchestrator, never read from Move object content.
type NFTInfo struct {
	ObjectID string `json:"object_id"`
	Type     string `json:"type"`
	Owner    string `json:"owner"`
	Version  string `json:"version"`
}

// TxStatus is the resolved on-chain status of a transaction digest.
type TxStatus struct {
	Digest string `json:"digest"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
	// CountNFTsCreated is the number of AttendanceNFT objects created by this
	// transaction (parsed from objectChanges).
	CountNFTsCreated int `json:"nfts_created"`
	// Recipients is the set of distinct address-owners of created AttendanceNFTs.
	Recipients []string `json:"recipients,omitempty"`
}

// Reader is the Sui query/probe surface the demo orchestrator depends on, kept
// separate from Minter so production mint fakes/tests are unaffected. The real
// *Client satisfies it.
type Reader interface {
	// TransferSuiProbe sponsors a tiny SUI transfer from the gas pool to the
	// recipient, proving the derived address is live on-chain. Returns the digest.
	TransferSuiProbe(ctx context.Context, recipient, amountMist string) (string, error)
	// TransactionStatus resolves a tx digest's execution status + created NFTs.
	TransactionStatus(ctx context.Context, digest string) (TxStatus, error)
	// OwnedNFTs returns the AttendanceNFT objects owned by an address.
	OwnedNFTs(ctx context.Context, address string) ([]NFTInfo, error)
	// BalanceMist returns the gas-pool SUI balance in MIST (dashboard low-funds
	// flag, protecting against gas-pool depletion across repeated sweeps).
	BalanceMist(ctx context.Context) (string, error)
}

// compile-time check that *Client satisfies Reader.
var _ Reader = (*Client)(nil)

// nftStructType returns the fully-qualified AttendanceNFT struct type for the
// deployed package, e.g. "0x78c9...::attendance::AttendanceNFT".
func (c *Client) nftStructType() string {
	return c.packageID + attendanceNFTTypeSuffix
}

// TransferSuiProbe implements Reader: a sponsored SUI transfer from the gas
// pool to recipient (proof-of-life probe, 2a). It uses unsafe_transferSui where
// the pool's own coin object also serves as the gas object, then signs+executes.
func (c *Client) TransferSuiProbe(ctx context.Context, recipient, amountMist string) (string, error) {
	if recipient == "" {
		return "", fmt.Errorf("recipient address required for probe transfer")
	}
	if amountMist == "" {
		amountMist = "1000000"
	}
	log.Info().Str("signer", c.signerAddress).Str("to", recipient).Str("amount_mist", amountMist).Msg("sui probe transfer")

	// gas pool's first spendable SUI coin also serves as the gas object.
	coins, err := c.sdkClient.SuiXGetCoins(ctx, models.SuiXGetCoinsRequest{
		Owner:    c.signerAddress,
		CoinType: "0x2::sui::SUI",
		Limit:    1,
	})
	if err != nil {
		return "", fmt.Errorf("get gas-pool coin: %w", err)
	}
	if len(coins.Data) == 0 {
		return "", fmt.Errorf("gas pool has no SUI coin to transfer")
	}

	for attempt := 0; attempt < 5; attempt++ {
		if err := c.acquire(ctx); err != nil {
			return "", err
		}
		txnMeta, err := c.sdkClient.TransferSui(ctx, models.TransferSuiRequest{
			Signer:      c.signerAddress,
			SuiObjectId: coins.Data[0].CoinObjectId,
			GasBudget:   c.gasBudget,
			Recipient:   recipient,
			Amount:      amountMist,
		})
		c.release()
		if err != nil {
			if isThrottled(err) {
				if !backoff(ctx, attempt) {
					return "", ctx.Err()
				}
				continue
			}
			return "", fmt.Errorf("transferSui failed (attempt %d): %w", attempt+1, err)
		}

		if err := c.acquire(ctx); err != nil {
			return "", err
		}
		resp, err := c.sdkClient.SignAndExecuteTransactionBlock(ctx, models.SignAndExecuteTransactionBlockRequest{
			TxnMetaData: txnMeta,
			PriKey:      c.signerPriKey,
			Options: models.SuiTransactionBlockOptions{
				ShowEffects:       true,
				ShowObjectChanges: true,
			},
			RequestType: "WaitForLocalExecution",
		})
		c.release()
		if err != nil {
			if isThrottled(err) {
				if !backoff(ctx, attempt) {
					return "", ctx.Err()
				}
				continue
			}
			return "", fmt.Errorf("signAndExecute probe failed (attempt %d): %w", attempt+1, err)
		}
		return resp.Digest, nil
	}
	return "", fmt.Errorf("probe transfer failed after 5 attempts")
}

// TransactionStatus implements Reader: resolves a digest's status + created
// AttendanceNFT owners, used to verify mint success on-chain (2b/2c/2d and the
// 2a probe).
func (c *Client) TransactionStatus(ctx context.Context, digest string) (TxStatus, error) {
	resp, err := c.sdkClient.SuiGetTransactionBlock(ctx, models.SuiGetTransactionBlockRequest{
		Digest: digest,
		Options: models.SuiTransactionBlockOptions{
			ShowEffects:       true,
			ShowObjectChanges: true,
		},
	})
	if err != nil {
		return TxStatus{}, fmt.Errorf("get transaction block %s: %w", digest, err)
	}

	ts := TxStatus{
		Digest: digest,
		Status: resp.Effects.Status.Status,
		Error:  resp.Effects.Status.Error,
	}
	seen := map[string]struct{}{}
	for _, change := range resp.ObjectChanges {
		if change.Type != "created" {
			continue
		}
		if change.ObjectType != c.nftStructType() {
			continue
		}
		ts.CountNFTsCreated++
		owner := change.GetObjectChangeAddressOwner()
		if owner == "" {
			continue
		}
		if _, ok := seen[owner]; ok {
			continue
		}
		seen[owner] = struct{}{}
		ts.Recipients = append(ts.Recipients, owner)
	}
	return ts, nil
}

// OwnedNFTs implements Reader: returns the AttendanceNFT objects owned by an
// address, filtered by the deployed package.
func (c *Client) OwnedNFTs(ctx context.Context, address string) ([]NFTInfo, error) {
	var (
		out    []NFTInfo
		cursor interface{}
	)
	for {
		resp, err := c.sdkClient.SuiXGetOwnedObjects(ctx, models.SuiXGetOwnedObjectsRequest{
			Address: address,
			Query: models.SuiObjectResponseQuery{
				Filter: models.ObjectFilterByPackage{Package: c.packageID},
				Options: models.SuiObjectDataOptions{
					ShowType:  true,
					ShowOwner: true,
				},
			},
			Cursor: cursor,
			Limit:  50,
		})
		if err != nil {
			return nil, fmt.Errorf("get owned objects for %s: %w", address, err)
		}
		out = append(out, c.parseNFTs(resp.Data)...)
		if !resp.HasNextPage {
			break
		}
		cursor = resp.NextCursor
	}
	return out, nil
}

// parseNFTs converts SuiObjectResponse entries into minimal NFTInfo, skipping
// non-NFT (non-AttendanceNFT) objects.
func (c *Client) parseNFTs(objects []models.SuiObjectResponse) []NFTInfo {
	out := make([]NFTInfo, 0, len(objects))
	for _, obj := range objects {
		if obj.Data == nil {
			continue
		}
		if obj.Data.Type != c.nftStructType() {
			continue
		}
		info := NFTInfo{
			ObjectID: obj.Data.ObjectId,
			Type:     obj.Data.Type,
			Version:  obj.Data.Version,
		}
		if obj.Data.Owner != nil {
			if b, err := json.Marshal(obj.Data.Owner); err == nil {
				var owner models.ObjectOwner
				if json.Unmarshal(b, &owner) == nil {
					info.Owner = owner.AddressOwner
					if info.Owner == "" {
						info.Owner = owner.ObjectOwner
					}
				}
			}
		}
		out = append(out, info)
	}
	return out
}

// BalanceMist implements Reader: the gas pool's SUI balance in MIST.
func (c *Client) BalanceMist(ctx context.Context) (string, error) {
	resp, err := c.sdkClient.SuiXGetBalance(ctx, models.SuiXGetBalanceRequest{
		Owner:    c.signerAddress,
		CoinType: "0x2::sui::SUI",
	})
	if err != nil {
		return "", fmt.Errorf("get gas-pool balance: %w", err)
	}
	return resp.TotalBalance, nil
}
