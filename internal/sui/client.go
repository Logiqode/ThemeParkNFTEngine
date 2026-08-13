package sui

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/block-vision/sui-go-sdk/models"
	suiSDK "github.com/block-vision/sui-go-sdk/sui"
	"github.com/rs/zerolog/log"

	"github.com/Logiqode/ThemeParkNFT/internal/config"
)

// Client wraps the Sui Go SDK for custodial mint operations.
// Gas is sponsored by a custodial gas-pool wallet derived from
// SUI_GAS_POOL_MNEMONIC (D4 fixed: no more fragile ~/.sui keystore file load).
type Client struct {
	rpcURL     string
	packageID  string
	mintCapID  string // MintCap object ID (required for mint calls)

	signerAddress string
	signerPriKey  ed25519.PrivateKey

	gasBudget  string
	concurrency chan struct{} // RPC concurrency semaphore (W6-C)
	sdkClient suiSDK.ISuiAPI
}

// NewClient builds a Sui client whose signer is the custodial gas-pool wallet
// derived from cfg.GasPoolMnemonic. Returns an error if the mnemonic is absent
// or invalid (fail-fast per R3).
func NewClient(cfg config.SuiConfig) (*Client, error) {
	sdk := suiSDK.NewSuiClient(cfg.RPCURL)

	priKey, address, err := SignerFromMnemonic(cfg.GasPoolMnemonic)
	if err != nil {
		return nil, err
	}

	if cfg.RPCMaxConcurrency < 1 {
		cfg.RPCMaxConcurrency = 1
	}

	log.Info().
		Str("signer", address).
		Str("package", cfg.PackageID).
		Str("mint_cap", cfg.MintCapID).
		Int("rpc_max_concurrency", cfg.RPCMaxConcurrency).
		Msg("sui client initialized (mnemonic gas pool)")

	return &Client{
		rpcURL:     cfg.RPCURL,
		packageID:  cfg.PackageID,
		mintCapID:  cfg.MintCapID,
		signerAddress: address,
		signerPriKey:  priKey,
		gasBudget:     cfg.GasBudget,
		concurrency:   make(chan struct{}, cfg.RPCMaxConcurrency),
		sdkClient:     sdk,
	}, nil
}

// acquire / release implement the RPC concurrency semaphore (W6-C) so N mints
// never exceed SUI_RPC_MAX_CONCURRENCY in-flight calls to the testnet.
func (c *Client) acquire(ctx context.Context) error {
	select {
	case c.concurrency <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) release() { <-c.concurrency }

// Ping verifies Sui RPC connectivity with a lightweight chain-identifier call.
// Used as a readiness check (R2).
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.sdkClient.SuiCall(ctx, "sui_getChainIdentifier")
	if err != nil {
		return fmt.Errorf("sui ping: %w", err)
	}
	return nil
}

// MintBatchAttendance submits a real batch mint MoveCall to the Sui blockchain,
// signing with the gas-pool key, throttled by the RPC concurrency semaphore,
// and retrying on HTTP 429. Calls attendance::mint_batch.
func (c *Client) MintBatchAttendance(ctx context.Context, suiAddress string, rideIDs []string, date string, names, metadataURLs []string) (txDigest string, err error) {
	if c.packageID == "" {
		return "", fmt.Errorf("SUI_PACKAGE_ID not configured")
	}
	if c.mintCapID == "" {
		return "", fmt.Errorf("SUI_MINTCAP_ID not configured")
	}
	if len(rideIDs) == 0 {
		return "", fmt.Errorf("no ride_ids to mint")
	}

	log.Info().
		Str("signer", c.signerAddress).
		Str("recipient", suiAddress).
		Int("ride_count", len(rideIDs)).
		Str("date", date).
		Msg("minting batch attendance NFTs (MoveCall)")

	args := []interface{}{
		c.mintCapID,  // MintCap object ID
		suiAddress,   // recipient
		encodeBytesVec(rideIDs),     // vector<vector<u8>> ride_ids
		toDateHex(date),             // u64 date (hex string, JSON-RPC pure)
		encodeBytesVec(names),       // vector<vector<u8>> names
		encodeBytesVec(metadataURLs), // vector<vector<u8>> metadata_urls
	}

	moveReq := models.MoveCallRequest{
		Signer:          c.signerAddress,
		PackageObjectId: c.packageID,
		Module:          "attendance",
		Function:        "mint_batch",
		TypeArguments:   []interface{}{}, // non-nil so it marshals as [] not null
		Arguments:       args,
		GasBudget:       c.gasBudget,
	}

	for attempt := 0; attempt < 5; attempt++ {
		if err := c.acquire(ctx); err != nil {
			return "", err
		}
		txnMeta, err := c.sdkClient.MoveCall(ctx, moveReq)
		c.release()
		if err != nil {
			if isThrottled(err) {
				if !backoff(ctx, attempt) {
					return "", ctx.Err()
				}
				continue
			}
			return "", fmt.Errorf("moveCall failed (attempt %d): %w", attempt+1, err)
		}

		if err := c.acquire(ctx); err != nil {
			return "", err
		}
		resp, err := c.sdkClient.SignAndExecuteTransactionBlock(ctx, models.SignAndExecuteTransactionBlockRequest{
			TxnMetaData: txnMeta,
			PriKey:      c.signerPriKey,
			Options: models.SuiTransactionBlockOptions{
				ShowInput:         true,
				ShowEffects:       true,
				ShowEvents:        true,
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
			return "", fmt.Errorf("signAndExecute failed (attempt %d): %w", attempt+1, err)
		}

		digest := resp.Digest
		log.Info().
			Str("tx_digest", digest).
			Str("status", resp.Effects.Status.Status).
			Msg("mint transaction executed")
		return digest, nil
	}
	return "", fmt.Errorf("mint batch failed after 5 attempts")
}

// TransferNFT performs the R33/M6.7 custody object transfer: moves an
// AttendanceNFT object from the custodial wallet (signer) to a dependent's own
// non-custodial wallet. It uses the Sui `transfer` (or transfer_object)
// primitive; throttled + retried like mints.
func (c *Client) TransferNFT(ctx context.Context, nftObjectID, toAddress string) (txDigest string, err error) {
	if nftObjectID == "" || toAddress == "" {
		return "", fmt.Errorf("nft_object_id and to_address required for custody transfer")
	}
	log.Info().
		Str("signer", c.signerAddress).
		Str("nft_object", nftObjectID).
		Str("to", toAddress).
		Msg("transferring custody of attendance NFT")

	for attempt := 0; attempt < 5; attempt++ {
		if err := c.acquire(ctx); err != nil {
			return "", err
		}
		txnMeta, err := c.sdkClient.TransferObject(ctx, models.TransferObjectRequest{
			Signer:     c.signerAddress,
			ObjectId:   nftObjectID,
			Recipient:  toAddress,
			GasBudget:  c.gasBudget,
		})
		c.release()
		if err != nil {
			if isThrottled(err) {
				if !backoff(ctx, attempt) {
					return "", ctx.Err()
				}
				continue
			}
			return "", fmt.Errorf("transferObject failed (attempt %d): %w", attempt+1, err)
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
			return "", fmt.Errorf("signAndExecute transfer failed (attempt %d): %w", attempt+1, err)
		}
		log.Info().Str("tx_digest", resp.Digest).Msg("custody transfer executed")
		return resp.Digest, nil
	}
	return "", fmt.Errorf("custody transfer failed after 5 attempts")
}

// ── Internal helpers ──

// encodeBytesVec hex-encodes a string slice as a raw hex address array to satisfy
// Sui's JSON-RPC pure-value encoding for `vector<vector<u8>>`. Each element is
// rendered as a 0x-prefixed hex string (matching how the Sui CLI / SDK serialize
// byte-vector pure arguments), which the modern fullnode accepts via
// `unsafe_moveCall`. Passing plain Go strings/numbers fails with
// "invalid type: null, expected a sequence".
func encodeBytesVec(in []string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = "0x" + hex.EncodeToString([]byte(s))
	}
	return out
}

// toDateHex converts a date string (YYYYMMDD or YYYY-MM-DD) to a 0x-hex u64
// string, the format Sui's JSON-RPC requires for a `u64` pure argument.
func toDateHex(date string) string {
	d := toDateU64(strings.ReplaceAll(date, "-", ""))
	return "0x" + strconv.FormatUint(d, 16)
}

// toDateU64 converts a date string (YYYYMMDD or YYYY-MM-DD) to a u64.
func toDateU64(date string) uint64 {
	date = strings.ReplaceAll(date, "-", "")
	var d uint64
	if _, err := fmt.Sscanf(date, "%d", &d); err != nil {
		return 0
	}
	return d
}

// isThrottled reports whether an SDK error is an HTTP 429 (rate limit) so the
// caller retries with exponential backoff.
func isThrottled(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "429") || strings.Contains(s, "Too Many") || strings.Contains(s, "rate limit")
}

// backoff sleeps 2^attempt seconds, honoring context cancellation.
func backoff(ctx context.Context, attempt int) bool {
	d := time.Duration(1<<uint(attempt)) * time.Second
	log.Warn().Int("attempt", attempt+1).Dur("backoff", d).Msg("RPC throttled, retrying")
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}
