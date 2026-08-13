package demo

import (
	"context"
	"database/sql"
	"fmt"
)

// Wallet returns the enriched wallet view for an address: on-chain owned NFT
// objects (from Sui) plus off-chain attribution (guardian own vs each
// dependent) joined from Postgres. Because a guardian and their dependents
// share the same on-chain address, attribution is only resolvable off-chain
// (R34) and is therefore returned as a separate grouped list rather than a
// false object-level mapping.
func (o *Orchestrator) Wallet(ctx context.Context, address string) (*WalletView, error) {
	if address == "" {
		return nil, fmt.Errorf("address query parameter required")
	}

	nfTs, err := o.reader.OwnedNFTs(ctx, address)
	if err != nil {
		return nil, err
	}
	balance, _ := o.reader.BalanceMist(ctx)

	view := &WalletView{
		Address:     address,
		BalanceMist: balance,
		NFTObjects:  make([]WalletNFTObject, 0, len(nfTs)),
		Attribution: []WalletAttribution{},
	}
	for _, n := range nfTs {
		view.NFTObjects = append(view.NFTObjects, WalletNFTObject{
			ObjectID: n.ObjectID, Type: n.Type, Owner: n.Owner, Version: n.Version,
		})
	}

	attribution, guardianName, hasDependents, err := o.attribution(ctx, address)
	if err != nil {
		return nil, err
	}
	view.Attribution = attribution
	view.GuardianName = guardianName
	view.HasDependents = hasDependents
	return view, nil
}

// attribution groups Postgres mint_logs (ride/date/participant) for a wallet.
func (o *Orchestrator) attribution(ctx context.Context, address string) ([]WalletAttribution, string, bool, error) {
	// Resolve the guardian (own account) this wallet belongs to.
	var guardianID int64
	var guardianName string
	err := o.db.QueryRowContext(ctx,
		`SELECT p.id, p.name
		 FROM participants p
		 JOIN users u ON u.email = p.account_email
		 WHERE u.sui_address = $1
		 ORDER BY p.id LIMIT 1`, address).Scan(&guardianID, &guardianName)
	if err != nil && err != sql.ErrNoRows {
		return nil, "", false, fmt.Errorf("resolve guardian by wallet: %w", err)
	}

	rows, err := o.db.QueryContext(ctx, `
		SELECT
			p.name AS participant,
			p.wallet_state::text AS state,
			ml.mint_date::text AS mint_date,
			ml.ride_id AS ride_id
		FROM mint_logs ml
		JOIN participants p ON p.id = ml.participant_id
		WHERE (p.id = $1 OR p.guardian_id = $1) AND ml.tx_digest IS NOT NULL
		ORDER BY ml.mint_date, p.name, ml.ride_id`, guardianID)
	if err != nil {
		return nil, "", false, fmt.Errorf("query attribution: %w", err)
	}
	defer func() { _ = rows.Close() }()

	grouped := map[string]*WalletAttribution{}
	order := []string{}
	hasDependents := false
	for rows.Next() {
		var name, state, mintDate, rideID string
		if err := rows.Scan(&name, &state, &mintDate, &rideID); err != nil {
			return nil, "", false, fmt.Errorf("scan attribution: %w", err)
		}
		section := "guardian"
		if state == "CUSTODIAL_PROXY" {
			section = name // dependent section keyed by name
			hasDependents = true
		}
		key := section + "|" + name + "|" + mintDate
		if _, ok := grouped[key]; !ok {
			grouped[key] = &WalletAttribution{
				Section: section, Participant: name, MintDate: mintDate, RideIDs: []string{},
			}
			order = append(order, key)
		}
		grouped[key].RideIDs = append(grouped[key].RideIDs, rideID)
	}
	if err := rows.Err(); err != nil {
		return nil, "", false, fmt.Errorf("iterate attribution: %w", err)
	}

	out := make([]WalletAttribution, 0, len(order))
	for _, key := range order {
		out = append(out, *grouped[key])
	}
	return out, guardianName, hasDependents, nil
}
