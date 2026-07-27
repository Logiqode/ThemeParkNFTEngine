package storage

import (
	"fmt"
	"strings"
)

// NFTMetadata is the standard NFT metadata JSON schema used by marketplaces and explorers.
type NFTMetadata struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Image       string      `json:"image"`
	Attributes  []Attribute `json:"attributes"`
}

// Attribute is a key-value trait for NFT metadata.
type Attribute struct {
	TraitType string `json:"trait_type"`
	Value     string `json:"value"`
}

// Known ride names for generating human-readable metadata.
var rideNames = map[string]string{
	"ride-001": "Space Mountain",
	"ride-002": "Pirates of the Caribbean",
	"ride-003": "Haunted Mansion",
	"ride-004": "Big Thunder Mountain",
	"ride-005": "Splash Mountain",
	"ride-006": "Indiana Jones Adventure",
	"ride-007": "It's a Small World",
	"ride-008": "Star Wars: Rise of the Resistance",
	"ride-009": "Avatar Flight of Passage",
	"ride-010": "Expedition Everest",
}

// RideName returns a human-readable name for a ride ID, or the ride ID itself if unknown.
func RideName(rideID string) string {
	if name, ok := rideNames[rideID]; ok {
		return name
	}
	// Capitalize for unknown ride IDs
	return strings.Title(strings.ReplaceAll(rideID, "-", " "))
}

// BuildMetadata creates a standard NFT metadata JSON payload for a specific ride and date.
// imageURL should be an ipfs:// URI pointing to the ride's artwork.
func BuildMetadata(rideID, date string, imageURL string) NFTMetadata {
		name := RideName(rideID)
	return NFTMetadata{
		Name:        fmt.Sprintf("%s — %s", name, displayDate(date)),
		Description: fmt.Sprintf("Attendance NFT for completing %s on %s", name, displayDate(date)),
		Image:       imageURL,
		Attributes: []Attribute{
			{TraitType: "Ride", Value: name},
			{TraitType: "Ride ID", Value: rideID},
			{TraitType: "Date", Value: displayDate(date)},
			{TraitType: "Rarity", Value: "Common"},
		},
	}
}

// displayDate formats a YYYY-MM-DD or YYYYMMDD string for display.
func displayDate(date string) string {
	date = strings.ReplaceAll(date, "-", "")
	if len(date) == 8 {
		return fmt.Sprintf("%s-%s-%s", date[0:4], date[4:6], date[6:8])
	}
	return date
}