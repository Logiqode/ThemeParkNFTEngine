// Package demo implements the frontend-test-runner orchestrator: a small Go
// service that seeds deterministic guardians/dependents, injects ride data
// through the real pipeline, runs exact-count on-chain scenarios, and returns
// structured flow events for the React frontend to visualise.
package demo

// Scenario is one of the demo's four reproducible on-chain tests.
type Scenario string

const (
	// ScenarioProbe is 2a — NFC binding + wallet probe (10 sponsored SUI transfers).
	ScenarioProbe Scenario = "2a"
	// ScenarioGuardianMint is 2b — batch mint for 10 guardians (own wallets).
	ScenarioGuardianMint Scenario = "2b"
	// ScenarioDependentMint is 2c — batch mint for 10 dependents (guardian wallets).
	ScenarioDependentMint Scenario = "2c"
	// ScenarioMixedMint is 2d — 5 guardian + 5 dependent mints, randomized order.
	ScenarioMixedMint Scenario = "2d"
)

// AllScenarios lists every runnable scenario in presentation order.
var AllScenarios = []Scenario{ScenarioProbe, ScenarioGuardianMint, ScenarioDependentMint, ScenarioMixedMint}

// ScenarioDeps is the number of guardians/dependents each scenario seeds.
type ScenarioDeps struct {
	Guardians  int
	Dependents int
	ProbeTx    int // how many wallet probes the scenario fires
}

// depsFor returns the participant counts each scenario needs.
func depsFor(s Scenario) ScenarioDeps {
	switch s {
	case ScenarioProbe:
		// 9 guardians (8 standalone + 1 guardian with 1 dependent) →
		// 8×1 own probe + 1×[own + dependent] probe = 10 probe txs total.
		// The guardian-with-dependent gets 2 transactions pushed (its own
		// probe + the dependent's probe, which lands in the guardian wallet).
		return ScenarioDeps{Guardians: 9, Dependents: 1, ProbeTx: 10}
	case ScenarioGuardianMint:
		return ScenarioDeps{Guardians: 10, Dependents: 0, ProbeTx: 0}
	case ScenarioDependentMint:
		// 10 dependents under (reused) guardians; guardians come from 2b.
		return ScenarioDeps{Guardians: 0, Dependents: 10, ProbeTx: 0}
	case ScenarioMixedMint:
		return ScenarioDeps{Guardians: 5, Dependents: 5, ProbeTx: 0}
	default:
		return ScenarioDeps{}
	}
}

// FlowEvent is a single ordered step in a scenario run, tagged by kind so the
// frontend can colour-code on-chain vs off-chain vs mock steps, and link
// on-chain steps to Suiscan.
type FlowEvent struct {
	Step        int    `json:"step"`
	Label       string `json:"label"`
	Kind        string `json:"kind"` // "offchain" | "onchain" | "mock"
	Detail      string `json:"detail"`
	TxDigest    string `json:"txDigest,omitempty"`
	ExplorerURL string `json:"explorerUrl,omitempty"`
	Participant string `json:"participant,omitempty"`
	Wallet      string `json:"wallet,omitempty"`
}

// TxnCard is a per-transaction summary for the transaction table.
type TxnCard struct {
	Participant string `json:"participant"`
	Kind        string `json:"kind"` // "probe" | "probe-dependent" | "mint-guardian" | "mint-dependent"
	Recipient   string `json:"recipient"`
	TxDigest    string `json:"tx_digest"`
	ExplorerURL string `json:"explorer_url"`
	Status      string `json:"status"`
	NFTsCreated int    `json:"nfts_created,omitempty"`
}

// RunResult is the structured response of POST /api/demo/run.
type RunResult struct {
	Scenario     Scenario    `json:"scenario"`
	Date         string      `json:"date"`
	Steps        []FlowEvent `json:"steps"`
	Transactions []TxnCard   `json:"transactions"`
	Totals       RunTotals   `json:"totals"`
}

// RunTotals summarises a scenario's on-chain outcome.
type RunTotals struct {
	Transactions int `json:"transactions"`
	Succeeded    int `json:"succeeded"`
	Failed       int `json:"failed"`
	NFTsCreated  int `json:"nfts_created"`
}

// WalletAttribution is an off-chain ride/participant grouping for a wallet.
// Because dependents and their guardian share the same on-chain address, the
// object-level mapping is intentionally joined off-chain (R34) — the chain
// holds only the NFT object; Postgres holds who it was minted for.
type WalletAttribution struct {
	Section     string   `json:"section"` // "guardian" | dependent name
	Participant string   `json:"participant"`
	MintDate    string   `json:"mint_date"`
	RideIDs     []string `json:"ride_ids"`
}

// WalletView is the response of GET /api/demo/wallet.
type WalletView struct {
	Address       string              `json:"address"`
	BalanceMist   string              `json:"balance_mist"`
	HasDependents bool                `json:"has_dependents"`
	GuardianName  string              `json:"guardian_name,omitempty"`
	NFTObjects    []WalletNFTObject   `json:"nft_objects"`
	Attribution   []WalletAttribution `json:"attribution"`
}

// WalletNFTObject is the on-chain identity of one owned NFT object.
type WalletNFTObject struct {
	ObjectID string `json:"object_id"`
	Type     string `json:"type"`
	Owner    string `json:"owner"`
	Version  string `json:"version"`
}
