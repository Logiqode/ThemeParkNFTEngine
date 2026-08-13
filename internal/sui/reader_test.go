package sui

import (
	"testing"

	"github.com/block-vision/sui-go-sdk/models"
)

func TestNFTStructType(t *testing.T) {
	c := &Client{packageID: "0xabc"}
	got := c.nftStructType()
	want := "0xabc::attendance::AttendanceNFT"
	if got != want {
		t.Fatalf("nftStructType() = %q, want %q", got, want)
	}
}

func TestParseNFTsFiltersNonNFTs(t *testing.T) {
	c := &Client{packageID: "0xabc"}
	nftType := "0xabc::attendance::AttendanceNFT"

	ownerBytes, _ := ownerJSON("0xdead")
	objects := []models.SuiObjectResponse{
		{Data: &models.SuiObjectData{ObjectId: "0x1", Type: "0x2::coin::Coin<0x2::sui::SUI>"}},
		{Data: &models.SuiObjectData{ObjectId: "0x2", Type: nftType, Owner: ownerBytes}},
		{Data: nil},
	}

	out := c.parseNFTs(objects)
	if len(out) != 1 {
		t.Fatalf("expected 1 NFT, got %d", len(out))
	}
	if out[0].ObjectID != "0x2" {
		t.Fatalf("wrong object id: %s", out[0].ObjectID)
	}
	if out[0].Owner != "0xdead" {
		t.Fatalf("wrong owner: %s", out[0].Owner)
	}
}

func ownerJSON(addr string) (interface{}, error) {
	return map[string]string{"AddressOwner": addr}, nil
}
