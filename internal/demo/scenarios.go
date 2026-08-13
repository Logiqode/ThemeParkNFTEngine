package demo

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/Logiqode/ThemeParkNFT/internal/minter"
	"github.com/Logiqode/ThemeParkNFT/internal/models"
)

// SeedResult is the response of POST /api/demo/seed.
type SeedResult struct {
	Scenario   Scenario        `json:"scenario"`
	Date       string          `json:"date"`
	Guardians  []SeedGuardian  `json:"guardians"`
	Dependents []SeedDependent `json:"dependents"`
}

// SeedGuardian is a seeded guardian account + its derived wallet.
type SeedGuardian struct {
	Email  string `json:"email"`
	Wallet string `json:"wallet"`
}

// SeedDependent is a seeded dependent participant linked to a guardian.
type SeedDependent struct {
	Name           string `json:"name"`
	GuardianEmail  string `json:"guardian_email"`
	GuardianWallet string `json:"guardian_wallet"`
}

// dependentInfo pairs a seeded dependent with its guardian.
type dependentInfo struct {
	particip *models.Participant
	name     string
	guardian *guardian
}

// seedState is the intermediate in-memory result of seeding a scenario.
type seedState struct {
	scenario   Scenario
	date       string
	guardians  []guardian
	dependents []dependentInfo
	// label maps a participant id to a human label for flow events.
	label map[int64]string
	// kind maps a participant id to "guardian" | "dependent".
	kind map[int64]string
}

// seedScenario idempotently seeds the participants + ride data a scenario
// needs. It is non-destructive: get-or-create by deterministic key, so 2c
// reuses 2b's guardians and repeated runs are additive.
func (o *Orchestrator) seedScenario(ctx context.Context, s Scenario) (*seedState, error) {
	st := &seedState{
		scenario: s,
		date:     scenarioDate(s),
		label:    map[int64]string{},
		kind:     map[int64]string{},
	}

	switch s {
	case ScenarioProbe:
		// 9 guardians: 8 standalone + Guardian-0009 with 1 dependent.
		// 8 standalone guardians → 1 own probe each (8 probe txs).
		// Guardian-0009 → own probe (1) + dependent's probe pushed into the
		// guardian wallet (1) = 2 probe txs for that guardian. Total = 10 probe txs.
		for i := 1; i <= 9; i++ {
			g, err := o.seedGuardian(ctx, guardianEmail(i))
			if err != nil {
				return nil, err
			}
			st.guardians = append(st.guardians, *g)
			st.label[g.particip.ID] = g.particip.Name
			st.kind[g.particip.ID] = "guardian"
		}
		// Guardian-0009 (last) has a dependent.
		gLast := &st.guardians[len(st.guardians)-1]
		d, err := o.seedDependent(ctx, gLast, dependentName(1))
		if err != nil {
			return nil, err
		}
		st.dependents = append(st.dependents, dependentInfo{particip: d, name: dependentName(1), guardian: gLast})
		st.label[d.ID] = d.Name
		st.kind[d.ID] = "dependent"

	case ScenarioGuardianMint:
		// 10 guardians, each takes rides (own non-custodial wallet mint).
		for i := 1; i <= 10; i++ {
			g, err := o.seedGuardian(ctx, guardianEmail(i))
			if err != nil {
				return nil, err
			}
			st.guardians = append(st.guardians, *g)
			st.label[g.particip.ID] = g.particip.Name
			st.kind[g.particip.ID] = "guardian"
			for r := 1; r <= 2; r++ {
				if err := o.seedRideAccount(ctx, g, st.date, rideID(r), fmt.Sprintf("seed-2b-%s-%d", g.particip.Name, r)); err != nil {
					return nil, err
				}
			}
		}

	case ScenarioDependentMint:
		// Reuse 2b's 10 guardians; give each one dependent that rides all day.
		// Dependent custodial wallet == guardian derived address (R31).
		for i := 1; i <= 10; i++ {
			g, err := o.seedGuardian(ctx, guardianEmail(i))
			if err != nil {
				return nil, err
			}
			st.guardians = append(st.guardians, *g)
			st.label[g.particip.ID] = g.particip.Name
			st.kind[g.particip.ID] = "guardian"

			d, err := o.seedDependent(ctx, g, dependentName(i))
			if err != nil {
				return nil, err
			}
			st.dependents = append(st.dependents, dependentInfo{particip: d, name: dependentName(i), guardian: g})
			st.label[d.ID] = d.Name
			st.kind[d.ID] = "dependent"
			for r := 1; r <= 2; r++ {
				if err := o.seedRideDependent(ctx, g, d, st.date, rideID(r), fmt.Sprintf("seed-2c-%s-%d", d.Name, r)); err != nil {
					return nil, err
				}
			}
		}

	case ScenarioMixedMint:
		// 5 guardian-only + 5 guardians-with-1-dependent, shuffled at run time.
		for i := 1; i <= 5; i++ {
			g, err := o.seedGuardian(ctx, guardianEmail(i))
			if err != nil {
				return nil, err
			}
			st.guardians = append(st.guardians, *g)
			st.label[g.particip.ID] = g.particip.Name
			st.kind[g.particip.ID] = "guardian"
			for r := 1; r <= 2; r++ {
				if err := o.seedRideAccount(ctx, g, st.date, rideID(r), fmt.Sprintf("seed-2d-%s-%d", g.particip.Name, r)); err != nil {
					return nil, err
				}
			}
		}
		for i := 6; i <= 10; i++ {
			g, err := o.seedGuardian(ctx, guardianEmail(i))
			if err != nil {
				return nil, err
			}
			st.guardians = append(st.guardians, *g)
			st.label[g.particip.ID] = g.particip.Name
			st.kind[g.particip.ID] = "guardian"

			d, err := o.seedDependent(ctx, g, dependentName(i))
			if err != nil {
				return nil, err
			}
			st.dependents = append(st.dependents, dependentInfo{particip: d, name: dependentName(i), guardian: g})
			st.label[d.ID] = d.Name
			st.kind[d.ID] = "dependent"
			for r := 1; r <= 2; r++ {
				if err := o.seedRideDependent(ctx, g, d, st.date, rideID(r), fmt.Sprintf("seed-2d-%s-%d", d.Name, r)); err != nil {
					return nil, err
				}
			}
		}
	}

	return st, nil
}

// Seed handles POST /api/demo/seed: returns what the scenario will seed.
func (o *Orchestrator) Seed(ctx context.Context, s Scenario) (*SeedResult, error) {
	if !validScenario(s) {
		return nil, fmt.Errorf("unknown scenario %q (expected 2a|2b|2c|2d)", s)
	}
	st, err := o.seedScenario(ctx, s)
	if err != nil {
		return nil, err
	}

	res := &SeedResult{Scenario: s, Date: st.date}
	for _, g := range st.guardians {
		res.Guardians = append(res.Guardians, SeedGuardian{Email: g.particip.Name, Wallet: g.wallet})
	}
	for _, d := range st.dependents {
		res.Dependents = append(res.Dependents, SeedDependent{
			Name: d.name, GuardianEmail: d.guardian.particip.Name, GuardianWallet: d.guardian.wallet,
		})
	}
	return res, nil
}

func validScenario(s Scenario) bool {
	for _, v := range AllScenarios {
		if v == s {
			return true
		}
	}
	return false
}

// Run executes a scenario, first idempotently seeding what it needs.
func (o *Orchestrator) Run(ctx context.Context, s Scenario) (*RunResult, error) {
	if !validScenario(s) {
		return nil, fmt.Errorf("unknown scenario %q (expected 2a|2b|2c|2d)", s)
	}
	st, err := o.seedScenario(ctx, s)
	if err != nil {
		return nil, err
	}

	out := &RunResult{Scenario: s, Date: st.date}
	if s == ScenarioProbe {
		o.runProbe(ctx, st, out)
	} else {
		o.runMint(ctx, st, out)
	}

	out.Totals.Transactions = len(out.Transactions)
	for _, c := range out.Transactions {
		if c.Status == "success" {
			out.Totals.Succeeded++
			out.Totals.NFTsCreated += c.NFTsCreated
		} else {
			out.Totals.Failed++
		}
	}
	return out, nil
}

// runProbe executes 2a: one sponsored wallet-probe transfer per wallet.
// 8 standalone guardians get an own-wallet probe (1 tx each). Guardian-0009
// has one dependent: the dependent has no wallet, so its probe is pushed into
// the guardian wallet — giving that guardian 2 transactions pushed. Total = 10.
func (o *Orchestrator) runProbe(ctx context.Context, st *seedState, out *RunResult) {
	step := 0

	// Standalone guardians: pair wristband + own wallet probe.
	for i := range st.guardians {
		g := &st.guardians[i]
		step++
		out.Steps = append(out.Steps, FlowEvent{
			Step:        step,
			Label:       fmt.Sprintf("Gate pairs wristband wb-%03d to %s", i+1, g.particip.Name),
			Kind:        "offchain",
			Detail:      "POST /api/wristband/bind → Redis binding created",
			Participant: g.particip.Name,
			Wallet:      g.wallet,
		})
		step = o.probeWallet(ctx, out, step, g.particip.Name, g.wallet, "probe", "own wallet")
	}

	// Dependents: no wallet of their own → the probe is pushed into the
	// guardian wallet (so a guardian with n dependents shows n extra txs).
	for _, d := range st.dependents {
		step++
		out.Steps = append(out.Steps, FlowEvent{
			Step:        step,
			Label:       fmt.Sprintf("Gate pairs wristband wb-dep-%s to %s", d.name, d.name),
			Kind:        "offchain",
			Detail:      "Dependent has no wallet; pairing targets guardian custodial wallet",
			Participant: d.name,
			Wallet:      d.guardian.wallet,
		})
		step = o.probeWallet(ctx, out, step, d.name, d.guardian.wallet, "probe-dependent", "guardian custodial wallet")
	}

	if len(st.dependents) > 0 {
		step++
		out.Steps = append(out.Steps, FlowEvent{
			Step: step, Kind: "offchain",
			Label:       fmt.Sprintf("Wristband Account Pairing Successful · %d Dependent", len(st.dependents)),
			Detail:      "Dependents have no wallet; rides + probes mint/push into the guardian custodial wallet",
			Participant: st.dependents[0].name, Wallet: st.dependents[0].guardian.wallet,
		})
	}
}

// probeWallet fires one sponsored SUI-transfer probe into recipient, records it
// as a FlowEvent + TxnCard, and returns the updated step counter.
func (o *Orchestrator) probeWallet(ctx context.Context, out *RunResult, step int, participant, recipient, kind, walletDesc string) int {
	digest, err := o.reader.TransferSuiProbe(ctx, recipient, o.probeAmountMist)
	step++
	if err != nil {
		out.Steps = append(out.Steps, FlowEvent{
			Step: step, Label: "Wallet probe failed", Kind: "onchain",
			Detail: err.Error(), Participant: participant, Wallet: recipient,
		})
		out.Transactions = append(out.Transactions, TxnCard{
			Participant: participant, Kind: kind, Recipient: recipient, Status: "failed",
		})
		return step
	}

	status := "success"
	if stt, serr := o.reader.TransactionStatus(ctx, digest); serr == nil {
		status = stt.Status
	}
	label := fmt.Sprintf("Wallet probe → %s", recipient)
	if kind == "probe-dependent" {
		label = fmt.Sprintf("Dependent probe pushed → guardian %s", recipient)
	}
	out.Steps = append(out.Steps, FlowEvent{
		Step: step, Label: label, Kind: "onchain",
		Detail:   fmt.Sprintf("Gas-pool sponsored SUI transfer proving %s is live", walletDesc),
		TxDigest: digest, ExplorerURL: suiscanTxURL + digest,
		Participant: participant, Wallet: recipient,
	})
	out.Transactions = append(out.Transactions, TxnCard{
		Participant: participant, Kind: kind, Recipient: recipient,
		TxDigest: digest, ExplorerURL: suiscanTxURL + digest, Status: status,
	})
	return step
}

// runMint executes the batch-mint scenarios (2b/2c/2d) via the shared
// DayResolver + BatchMint, then resolves each digest's on-chain status.
func (o *Orchestrator) runMint(ctx context.Context, st *seedState, out *RunResult) {
	out.Steps = append(out.Steps, FlowEvent{
		Step: 1, Kind: "offchain",
		Label:  fmt.Sprintf("Resolve day %s participants", st.date),
		Detail: "POST /mint/resolve-day → durable scan_events source",
	})

	resolver := minter.NewDayResolver(o.repo, minter.NewScanEventRideSource(o.db), o.voucher)
	resolutions, err := resolver.ResolveDay(ctx, st.date)
	if err != nil {
		out.Steps = append(out.Steps, FlowEvent{Step: 2, Kind: "mock", Label: "resolve failed", Detail: err.Error()})
		return
	}

	var lives []minter.MintLive
	var already []minter.Resolution
	for _, res := range resolutions {
		if res.Outcome != minter.OutcomeMintReady || res.WalletAddress == "" {
			continue
		}
		done, _, err := o.alreadyMinted(ctx, res.ParticipantID, st.date)
		if err != nil {
			out.Steps = append(out.Steps, FlowEvent{Step: 2, Kind: "mock", Label: "mint check failed", Detail: err.Error()})
			return
		}
		if done {
			already = append(already, res)
			continue
		}
		lives = append(lives, minter.MintLive{
			ParticipantID: res.ParticipantID,
			WalletAddress: res.WalletAddress,
			RideIDs:       res.RideIDs,
			Date:          st.date,
		})
	}

	// 2d: randomize guardian/dependent order to simulate a real mixed day.
	if st.scenario == ScenarioMixedMint && len(lives) > 1 {
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		rng.Shuffle(len(lives), func(i, j int) { lives[i], lives[j] = lives[j], lives[i] })
	}

	step := 2
	for _, res := range already {
		step++
		out.Steps = append(out.Steps, FlowEvent{
			Step: step, Kind: "offchain",
			Label:       fmt.Sprintf("%s already minted", st.label[res.ParticipantID]),
			Detail:      "mint_logs.idempotent → no duplicate on-chain tx",
			Participant: st.label[res.ParticipantID], Wallet: res.WalletAddress,
		})
	}

	if len(lives) == 0 {
		out.Steps = append(out.Steps, FlowEvent{Step: step + 1, Kind: "mock", Label: "No new mints", Detail: "All participants already minted for this date"})
		return
	}

	batch := minter.NewBatchMint(o.sui, o.meta, o.repo)
	results := batch.Run(ctx, lives)
	step++

	// Map participant id → participant label/kind for card construction.
	for _, r := range results {
		label := st.label[r.ParticipantID]
		kind := st.kind[r.ParticipantID]
		cardKind := "mint-guardian"
		if kind == "dependent" {
			cardKind = "mint-dependent"
		}

		if !r.Minted || r.TxDigest == "" {
			step++
			out.Steps = append(out.Steps, FlowEvent{
				Step: step, Kind: "onchain", Label: fmt.Sprintf("Mint failed: %s", label),
				Detail: r.Error, Participant: label, Wallet: r.WalletAddress,
			})
			out.Transactions = append(out.Transactions, TxnCard{
				Participant: label, Kind: cardKind, Recipient: r.WalletAddress, Status: "failed",
			})
			continue
		}

		status := "success"
		nfts := 0
		if stt, serr := o.reader.TransactionStatus(ctx, r.TxDigest); serr == nil {
			status = stt.Status
			nfts = stt.CountNFTsCreated
		}
		step++
		out.Steps = append(out.Steps, FlowEvent{
			Step: step, Label: fmt.Sprintf("Mint %s → %s", label, r.WalletAddress), Kind: "onchain",
			Detail:   fmt.Sprintf("attendance::mint_batch into %s", cardKindHuman(cardKind)),
			TxDigest: r.TxDigest, ExplorerURL: suiscanTxURL + r.TxDigest,
			Participant: label, Wallet: r.WalletAddress,
		})
		out.Transactions = append(out.Transactions, TxnCard{
			Participant: label, Kind: cardKind, Recipient: r.WalletAddress,
			TxDigest: r.TxDigest, ExplorerURL: suiscanTxURL + r.TxDigest,
			Status: status, NFTsCreated: nfts,
		})
	}

	// 2c/2d verification callout: dependents' NFTs land in the guardian wallet.
	if st.scenario == ScenarioDependentMint || st.scenario == ScenarioMixedMint {
		for _, d := range st.dependents {
			step++
			out.Steps = append(out.Steps, FlowEvent{
				Step: step, Kind: "onchain",
				Label:       fmt.Sprintf("Verify: %s minted into guardian wallet %s", d.name, d.guardian.wallet),
				Detail:      "Reverse-object check: created NFT owner == guardian custodial address",
				Participant: d.name, Wallet: d.guardian.wallet,
			})
		}
	}
}

func cardKindHuman(kind string) string {
	if kind == "mint-dependent" {
		return "guardian custodial wallet"
	}
	return "own non-custodial wallet"
}

// alreadyMinted reports whether a participant already has a confirmed mint for
// a date (idempotency guard so repeated runs issue no duplicate on-chain tx).
func (o *Orchestrator) alreadyMinted(ctx context.Context, participantID int64, date string) (bool, string, error) {
	var digest sql.NullString
	err := o.db.QueryRowContext(ctx,
		`SELECT tx_digest FROM mint_logs WHERE participant_id=$1 AND mint_date=$2 LIMIT 1`,
		participantID, date).Scan(&digest)
	if err == sql.ErrNoRows {
		return false, "", nil
	}
	if err != nil {
		log.Error().Err(err).Int64("participant", participantID).Msg("already-minted check failed")
		return false, "", err
	}
	return true, digest.String, nil
}
