package storage

import (
	"fmt"
	"math/rand"
)

// rideColors maps ride IDs to a distinctive theme color.
var rideColors = map[string]string{
	"ride-001": "#1a1a2e", // Space Mountain — deep space blue
	"ride-002": "#3d1c02", // Pirates — dark rum brown
	"ride-003": "#2d1b4e", // Haunted Mansion — ghostly purple
	"ride-004": "#8b4513", // Big Thunder — saddle brown
	"ride-005": "#1e90ff", // Splash Mountain — dodger blue
	"ride-006": "#b8860b", // Indiana Jones — dark goldenrod
	"ride-007": "#ff69b4", // Small World — hot pink
	"ride-008": "#ff4500", // Star Wars — orange red
	"ride-009": "#00ced1", // Avatar — dark turquoise
	"ride-010": "#228b22", // Everest — forest green
}

// GenerateRideSVG produces an SVG badge image for a given ride and date.
// This is a placeholder until real artwork is available.
func GenerateRideSVG(rideID, date string) []byte {
	name := RideName(rideID)
	color := rideColors[rideID]
	if color == "" {
		// Generate a deterministic color for unknown ride IDs
		color = fmt.Sprintf("#%06x", rand.New(rand.NewSource(int64(hash(rideID)))).Intn(0xFFFFFF))
	}
	displayDt := displayDate(date)

	svg := fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" width="512" height="512" viewBox="0 0 512 512">
  <defs>
    <linearGradient id="bg" x1="0%%" y1="0%%" x2="100%%" y2="100%%">
      <stop offset="0%%" style="stop-color:%s;stop-opacity:1"/>
      <stop offset="100%%" style="stop-color:#000000;stop-opacity:1"/>
    </linearGradient>
  </defs>
  <rect width="512" height="512" rx="32" fill="url(#bg)"/>
  <text x="256" y="160" text-anchor="middle" fill="white" font-family="Arial, sans-serif" font-size="28" font-weight="bold">🎢 ThemeParkNFT</text>
  <text x="256" y="240" text-anchor="middle" fill="white" font-family="Arial, sans-serif" font-size="36" font-weight="bold">%s</text>
  <text x="256" y="300" text-anchor="middle" fill="#cccccc" font-family="Arial, sans-serif" font-size="22">%s</text>
  <text x="256" y="380" text-anchor="middle" fill="#999999" font-family="Arial, sans-serif" font-size="16">Attendance NFT</text>
  <text x="256" y="440" text-anchor="middle" fill="#666666" font-family="Arial, sans-serif" font-size="14">Verified on Sui Blockchain</text>
</svg>`, color, name, displayDt)

	return []byte(svg)
}

// hash returns a simple hash of a string for deterministic color generation.
func hash(s string) int {
	h := 0
	for _, c := range s {
		h = h*31 + int(c)
	}
	if h < 0 {
		h = -h
	}
	return h
}