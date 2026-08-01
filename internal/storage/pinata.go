// Package storage handles IPFS pinning via Pinata.
// Implements Option A: artwork and metadata are pinned once per ride type
// and reused across all NFTs, keeping pin count under the free-tier limit.
package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
)

// PinataClient uploads and pins content to IPFS via Pinata's API.
type PinataClient struct {
	apiKey    string
	apiSecret string
	gateway   string
	client    *http.Client
}

// PinataResponse is the JSON response from Pinata's pinFileToIPFS and pinJSONToIPFS endpoints.
type PinataResponse struct {
	IpfsHash  string `json:"IpfsHash"`
	PinSize   int64  `json:"PinSize"`
	Timestamp string `json:"Timestamp"`
}

// NewPinataClient creates a Pinata client. apiKey and apiSecret come from
// the Pinata developer dashboard (https://app.pinata.cloud/developers/api-keys).
// gateway is the IPFS gateway URL (default: https://gateway.pinata.cloud).
func NewPinataClient(apiKey, apiSecret, gateway string) *PinataClient {
	if gateway == "" {
		gateway = "https://gateway.pinata.cloud"
	}
	return &PinataClient{
		apiKey:    apiKey,
		apiSecret: apiSecret,
		gateway:   gateway,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// PinFile uploads a file (image, SVG, etc.) to IPFS via Pinata.
// Returns the IPFS CID (e.g., "QmX7Y...") and the full ipfs:// URI.
func (p *PinataClient) PinFile(ctx context.Context, filename string, content []byte) (cid string, ipfsURI string, err error) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	part, err := writer.CreateFormFile("file", filename)
	if err != nil {
		return "", "", fmt.Errorf("create form file: %w", err)
	}
	if _, err := part.Write(content); err != nil {
		return "", "", fmt.Errorf("write file content: %w", err)
	}
	if err := writer.Close(); err != nil {
		return "", "", fmt.Errorf("close multipart writer: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.pinata.cloud/pinning/pinFileToIPFS", &body)
	if err != nil {
		return "", "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("pinata_api_key", p.apiKey)
	req.Header.Set("pinata_secret_api_key", p.apiSecret)

	log.Debug().Str("filename", filename).Int("size", len(content)).Msg("pinning file to IPFS")

	resp, err := p.client.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("pinata pinFile request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return "", "", fmt.Errorf("pinata pinFile returned %d: %s", resp.StatusCode, string(respBody))
	}

	var pinResp PinataResponse
	if err := json.NewDecoder(resp.Body).Decode(&pinResp); err != nil {
		return "", "", fmt.Errorf("decode pinata response: %w", err)
	}

	ipfsURI = fmt.Sprintf("ipfs://%s", pinResp.IpfsHash)
	log.Info().Str("cid", pinResp.IpfsHash).Str("uri", ipfsURI).Msg("file pinned to IPFS")
	return pinResp.IpfsHash, ipfsURI, nil
}

// PinJSON uploads a JSON object to IPFS via Pinata and returns the CID + ipfs:// URI.
func (p *PinataClient) PinJSON(ctx context.Context, name string, data interface{}) (cid string, ipfsURI string, err error) {
	type pinataJSONBody struct {
		PinataContent  interface{} `json:"pinataContent"`
		PinataMetadata struct {
			Name string `json:"name"`
		} `json:"pinataMetadata"`
	}

	pinBody := pinataJSONBody{PinataContent: data}
	pinBody.PinataMetadata.Name = name

	bodyBytes, err := json.Marshal(pinBody)
	if err != nil {
		return "", "", fmt.Errorf("marshal pinata body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.pinata.cloud/pinning/pinJSONToIPFS", bytes.NewReader(bodyBytes))
	if err != nil {
		return "", "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("pinata_api_key", p.apiKey)
	req.Header.Set("pinata_secret_api_key", p.apiSecret)

	log.Debug().Str("name", name).Msg("pinning JSON to IPFS")

	resp, err := p.client.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("pinata pinJSON request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return "", "", fmt.Errorf("pinata pinJSON returned %d: %s", resp.StatusCode, string(respBody))
	}

	var pinResp PinataResponse
	if err := json.NewDecoder(resp.Body).Decode(&pinResp); err != nil {
		return "", "", fmt.Errorf("decode pinata response: %w", err)
	}

	ipfsURI = fmt.Sprintf("ipfs://%s", pinResp.IpfsHash)
	log.Info().Str("cid", pinResp.IpfsHash).Str("uri", ipfsURI).Msg("JSON pinned to IPFS")
	return pinResp.IpfsHash, ipfsURI, nil
}

// GatewayURL returns the HTTP gateway URL for a given IPFS URI.
func (p *PinataClient) GatewayURL(ipfsURI string) string {
	if len(ipfsURI) > 7 && ipfsURI[:7] == "ipfs://" {
		return p.gateway + "/ipfs/" + ipfsURI[7:]
	}
	return ipfsURI
}