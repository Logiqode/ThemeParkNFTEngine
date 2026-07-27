package storage

import (
	"context"
	"fmt"
	"sync"

	"github.com/rs/zerolog/log"
)

// CIDCache implements Option A: artwork and metadata are pinned to IPFS once
// per ride type and reused across all NFTs. This keeps pin count under the
// Pinata free-tier limit (500 pins) regardless of how many NFTs are minted.
type CIDCache struct {
	mu       sync.RWMutex
	cache    map[string]*RideAssets // key: rideID
	pinata   *PinataClient
}

// RideAssets holds the IPFS CIDs and URIs for a single ride's artwork and metadata.
type RideAssets struct {
	ArtworkCID    string // raw IPFS CID for the SVG artwork
	ArtworkURI    string // ipfs:// URI for the artwork
	MetadataCID   string // raw IPFS CID for the metadata JSON
	MetadataURI   string // ipfs:// URI for the metadata
}

// NewCIDCache creates a CID cache backed by a Pinata client.
func NewCIDCache(pinata *PinataClient) *CIDCache {
	return &CIDCache{
		cache:  make(map[string]*RideAssets),
		pinata: pinata,
	}
}

// GetOrPin ensures artwork and metadata for a ride are pinned to IPFS.
// On first call for a ride, it generates the SVG, pins it, builds metadata,
// pins metadata, and caches both CIDs. Subsequent calls return cached CIDs.
// This means only ~20 pins total for 10 ride types, regardless of NFT count.
func (c *CIDCache) GetOrPin(ctx context.Context, rideID, date string) (*RideAssets, error) {
	// Fast path: check cache under read lock
	c.mu.RLock()
	if assets, ok := c.cache[rideID]; ok {
		c.mu.RUnlock()
		log.Debug().Str("ride_id", rideID).Msg("CID cache hit")
		return assets, nil
	}
	c.mu.RUnlock()

	// Slow path: generate and pin under write lock
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock
	if assets, ok := c.cache[rideID]; ok {
		return assets, nil
	}

	log.Info().Str("ride_id", rideID).Msg("pinning ride assets to IPFS (first time)")

	// 1. Generate SVG artwork
	svg := GenerateRideSVG(rideID, date)
	filename := fmt.Sprintf("%s-%s.svg", rideID, date)

	// 2. Pin artwork to IPFS
	artCID, artURI, err := c.pinata.PinFile(ctx, filename, svg)
	if err != nil {
		return nil, fmt.Errorf("pin artwork for %s: %w", rideID, err)
	}

	// 3. Build metadata JSON (references the pinned artwork)
	metadata := BuildMetadata(rideID, date, artURI)

	// 4. Pin metadata to IPFS
	metaCID, metaURI, err := c.pinata.PinJSON(ctx, fmt.Sprintf("%s-metadata-%s", rideID, date), metadata)
	if err != nil {
		return nil, fmt.Errorf("pin metadata for %s: %w", rideID, err)
	}

	assets := &RideAssets{
		ArtworkCID:  artCID,
		ArtworkURI:  artURI,
		MetadataCID: metaCID,
		MetadataURI: metaURI,
	}

	c.cache[rideID] = assets
	log.Info().
		Str("ride_id", rideID).
		Str("artwork_cid", artCID).
		Str("metadata_cid", metaCID).
		Msg("ride assets pinned and cached")

	return assets, nil
}

// Get returns cached assets for a ride without pinning. Returns nil if not cached.
func (c *CIDCache) Get(rideID string) *RideAssets {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cache[rideID]
}

// Size returns the number of cached ride entries.
func (c *CIDCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cache)
}