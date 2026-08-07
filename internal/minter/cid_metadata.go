package minter

import (
	"context"
	"fmt"

	"github.com/Logiqode/ThemeParkNFT/internal/storage"
)

// cidMetadataProvider adapts storage.CIDCache to the minter.MetadataProvider
// interface so BatchMint stays decoupled from the concrete IPFS/Pinata client.
type cidMetadataProvider struct {
	cache *storage.CIDCache
}

// NewCIDMetadataProvider wraps a storage.CIDCache as a MetadataProvider.
func NewCIDMetadataProvider(cache *storage.CIDCache) MetadataProvider {
	return &cidMetadataProvider{cache: cache}
}

// GetOrPin returns pinata-pinned metadata URI for a ride/date (Option A CIDCache).
func (p *cidMetadataProvider) GetOrPin(ctx context.Context, rideID, date string) (MetadataAssets, error) {
	if p.cache == nil {
		return MetadataAssets{}, fmt.Errorf("CID cache not configured")
	}
	assets, err := p.cache.GetOrPin(ctx, rideID, date)
	if err != nil {
		return MetadataAssets{}, err
	}
	return MetadataAssets{MetadataURI: assets.MetadataURI}, nil
}
