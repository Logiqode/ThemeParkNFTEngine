# Test-Runner Frontend + Backend Orchestrator — Build Plan

> **For Hermes:** subagent-driven-development to implement task-by-task after approval.
> **Goal:** A React (Vite) frontend that lets a non-Web3 visitor *watch* the theme-park → on-chain flow in action: run 4 scenarios that each fire **exactly 10 real Sui-testnet transactions**, see the flow of events per tx (with real on-chain vs off-chain steps visually distinguished), open tx hashes on Suiscan, view wallet objects, and reset/health-check the stack.
> **Architecture:** A small Go demo-orchestrator endpoint seeds deterministic guardians/dependents, injects ride data, runs exact-count mints/tests, and returns structured results (digests + ordered flow events). The React app consumes that API + the existing gate/minter/voucher services and renders it. Real on-chain txs and off-chain/mock steps are tagged differently end-to-end.
> **Tech Stack:** React 18 + Vite (JS/TS), `react-flow` (event-flow viz), CSS (dark theme). Go stdlib `net/http` for the orchestrator (same stack as repo). Suiscan testnet links.

---

## Current state (verified)

- **Backend services:** gate `:8080` (`/api/wristband/bind|nfc-check`, `/api/rides/scan`), minter `:8083` (`/api/auth/google`, `/mint/run-day`, `/mint/resolve-day`, `/mint/claim-custody`), voucher `:8084` (`/api/vouchers/*`).
- **Deterministic wallet:** `sui.DeterministicWallet(email, issuerSecret)` → HMAC-SHA256(email) → ed25519 → real blake2b Sui address. **`DETERMINISTIC_WALLET_SECRET` is NOT set in `.env`** → falls back to `ENCRYPTION_KEY` (32 chars). Same email ⇒ same wallet (reproducible).
- **Minting proven live:** `/mint/run-day` fired 10 real testnet txs, all `success`, 41 NFTs (2026-08-13). Pinata via `PINATA_JWT`; SDK `unsafe_moveCall` arg encoding fixed (hex pure args + non-nil `TypeArguments`).
- **No seeder exists** for test emails/rides; **no orchestrator endpoint**; **no frontend**.
- **Gas pool** at `0x3cd9956f...` is the signer for all mint txs (sponsors gas). Derived wallets have **no SUI** of their own.

---

## Design decisions / open questions to confirm

1. **2a "wallet probe" transaction.** Derived wallets receive NFTs but have no SUI. For "the wallet does 1 test tx the staff can see went through," the natural real on-chain probe (sponsored) is a tiny **SUI transfer from the gas pool → the derived wallet** (creates a coin owned by it = proof the address is live). For a dependent (no own wallet), the probe token is sent to the **guardian** wallet. → confirm this is the intended "test transaction."
2. **2d exact split.** Requirement "Batch Mint NFt Guardian + Dependent (5 each)" interpreted as **5 guardian-only wallets + 5 guardians that each have 1 dependent** = **10 mint txs** (5 mints land in guardian own wallets, 5 land in the same/etc. guardian wallets for dependents). Confirm.
3. **Where orchestration lives.** Recommended: a new lightweight Go service `cmd/demo` (port `:8090`) exposing the run/reset/health/wallet APIs, so exact-count isolation is server-side and honest. Confirm you want a new Go binary vs. extending `minter`.
4. **Reset scope/scenario isolation.** Each scenario writes to the same PG/Redis/Kafka. "Reset" = truncate app tables + flush Redis + (optionally) drop/recreate Kafka topics. Recommend each scenario run also start from a clean seed to avoid cross-scenario contamination.
5. **Language:** TypeScript vs JS — default **TypeScript** for a portfolio-grade app. Confirm.

---

## Backend work packages

### B1 — `cmd/demo` orchestrator service (port :8090)
- **Files:** add `cmd/demo/main.go`, `internal/demo/orchestrator.go`, `internal/demo/seed.go`, `internal/demo/scenarios.go`, `internal/demo/wallet.go`, `internal/demo/reset.go`.
- Reuses: `internal/config`, `internal/postgres.Repository`, `internal/redis`, `internal/sui.Client` (Minter), `internal/minter.NewDayResolver`, `internal/minter.NewBatchMint`, `internal/voucher.Service`.
- Endpoints:
  - `GET  /api/demo/health` → aggregate readiness of PG, Redis, Kafka, minter, Sui RPC → `{healthy, services:{...}}`.
  - `POST /api/demo/seed` `{scenario}` → deterministically create guardians/dependents/vouchers/rides/scan-events; idempotent (reset + reseed). Returns seeded emails + wallet addrs.
  - `POST /api/demo/run` `{scenario}` → run one of 2a/2b/2c/2d, return `{scenario, steps:[FlowEvent], transactions:[{participant, kind, recipient, tx_digest, explorer_url, status}], totals}`.
  - `POST /api/demo/reset` → truncate `scan_events, mint_logs, pending_mints, participants, ticket_vouchers, tickets, users`, Redis `FLUSHDB`, (option) recreate Kafka topics.
  - `GET  /api/demo/wallet?address=0x…` → owned NFT objects + rides + attribution (guardian vs which dependent).
- **FlowEvent shape** (the crux of frontend visualization):
  ```json
  {
    "step": 1, "label": "Gate pairs wristband wb-001 to guardian@themepark.local",
    "kind": "offchain" | "onchain" | "mock",
    "detail": "POST /api/wristband/bind → Redis binding created",
    "txDigest": "…",            // present only when kind=onchain
    "explorerUrl": "https://suiscan.xyz…",
    "participant": "guardian-0001",
    "wallet": "0x…"
  }
  ```

### B2 — Seeder (deterministic emails + rides) — `internal/demo/seed.go`
- Fixed email scheme: `guardian-0001@themepark.local …`, `dependent-0001@themepark.local …`, rides `ride-001…005`.
- Creates participants: guardians `OWN_NON_CUSTODIAL` w/ `DeterministicWallet(email)`; dependents `CUSTODIAL_PROXY` linked to a guardian.
- Inserts `scan_events` (via consumer path or direct durable insert — prefer injecting via Kafka → consumer to exercise the real pipeline) + build Redis ride-sets. `date` configurable (default today).

### B3 — Scenario runners — `internal/demo/scenarios.go`
- **2a · NFC binding test (10 probe txs):** mix of standalone + guardian-with-dependents such that total "wallet probe" txs = 10. Each probe = real sponsored SUI transfer gas pool → recipient (own wallet or guardian for dependents). Emit an event "Wristband Account Pairing Successful · n Dependent" (n = dependents). Every step tagged offchain (bind) vs onchain (probe tx).
- **2b · Batch Mint Guardian only (10 mint txs):** 10 guardians, each 1+ ride → `/mint/run-day` → 10 digests (owns own wallet). *(exactly the live run we did.)*
- **2c · Batch Mint Dependent only (10 mint txs):** 10 dependents (under guardians) → each mint lands in its **guardian custodial wallet**. Verify created-object owner == guardian address (reverse object check), expose in the transaction card for the user to verify.
- **2d · Batch Mint Guardian + Dependent (5 each = 10 mint txs):** 5 guardian-only + 5 guardians-with-1-dependent → 10 mint txs; each dependent's mint lands in the guardian wallet; guardian's own in own wallet. Distinguish attribution.

### B4 — Wallet viewer query — `internal/demo/wallet.go`
- Given an address: query Sui `sui_getOwnedObjects` (via block-vision SDK) for `AttendanceNFT`s; join PG `mint_logs`/`participants` for attribution (guardian's own vs each dependent's NFTs/rides). Since dependents share the guardian address, present grouped sections: **Guardian's own NFTs & rides** vs **Dependent N's NFTs & rides** (attributed via `participant_id`).

### B5 — Reset — `internal/demo/reset.go`
- Execute the same semantics as `make reset` at the data layer (truncate + flush + recreate topics) via SQL/Redis/Kafka admin.

### Backend Makefile/Dockerfile
- Add `cmd/demo` to `make build`, `make test`, and a `Dockerfile.demo` (mirror existing distroless multi-stage). `.env.example` gains nothing new (reuses existing secrets) unless `DEMO_PORT` added.

---

## Frontend work packages (React + Vite, in `frontend/`)

### F1 — Scaffold
- `frontend/` via Vite (react-ts). Layout: top nav (Dashboard / Tests / Wallet / Reset), dark theme, responsive.
- Deps: `react`, `react-dom`, `react-router-dom`, `@xyflow/react` (react-flow), `@tanstack/react-query` (server state), CSS variables (no heavy UI lib by default).

### F2 — Health gate (block tests until healthy)
- On load, call `GET /api/demo/health`. Show a health panel; **disable all test-run buttons** unless every required service is `healthy`. Poll every ~10s. Show which service is down.

### F3 — Dashboard
- Cards for the 4 scenarios with a run button; each card shows last-run summary (count, success, digests). "Run" only when healthy.

### F4 — Scenario runner view (per scenario)
- On run: call `POST /api/demo/run {scenario}`; render:
  - **Live step list / react-flow** of the returned `steps`, color-coded: 🟢 offchain, 🔵 mock, 🔴/🔗 onchain (with a ⛓ icon + `txDigest` + **Suiscan link**).
  - **Transaction table:** participant · kind · recipient wallet · tx digest (clickable → `https://suiscan.xyz/testnet/tx/<digest>`) · status.
  - For **2c/2d**: explicit "Verify: minted into guardian wallet `0x…`" callout (from the reverse-object check the backend returns).
  - For **2a**: show "Wristband Account Pairing Successful · n Dependent" + probe txs.

### F5 — Wallet viewer
- Input address → `GET /api/demo/wallet?address=…`. Render owned **AttendanceNFT** objects (name/ride/date/IPFS metadata URI), grouped **Guardian's own** vs **each dependent's** (driven by backend attribution). Clickable object IDs → Suiscan object page. Human-readable object cards (no raw Move types confusion).
- Explicit note when a wallet has dependents (because they share the guardian address).

### F6 — Reset panel
- `POST /api/demo/reset` with a two-step JSON confirm; show what got truncated/flushed; then re-check health.

### F7 — Suiscan links / config
- `src/config.ts`: `SUI_TESTNET_EXPLORER = "https://suiscan.xyz/testnet"`, API base (default `http://localhost:8090`), service ports. Easy env overrides via Vite `import.meta.env`.

---

## Files likely to change

**Backend (new):**
- `cmd/demo/main.go`, `internal/demo/{orchestrator,seed,scenarios,wallet,reset}.go`
- `scripts/build_demo.sh` (or extend Makefile), `deployments/Dockerfile.demo`
- `Makefile` (add `demo` build + `make demo` run target + `make reset-demo`)

**Backend (modify):**
- `Makefile` (add demo to `build`/`test`)
- possibly `internal/config/config.go` (`DEMO_PORT`), `.env.example` (optional `DEMO_PORT`)

**Frontend (new, all under `frontend/`):**
- `package.json`, `vite.config.ts`, `index.html`, `tsconfig.json`
- `src/main.tsx`, `src/App.tsx`, `src/config.ts`
- `src/api/demo.ts` (typed client to :8090 + services)
- `src/pages/{Dashboard,ScenarioRunner,WalletViewer,Reset}.tsx`
- `src/components/FlowVisualizer.tsx`, `TransactionTable.tsx`, `HealthGate.tsx`, `WalletObjects.tsx`, `ScenarioCard.tsx`
- `src/styles.css` (+ theme tokens)
- `frontend/README.md` (how to run: `npm i && npm run dev`, backend must be up + `.env` set)

---

## Validation

- **Backend:** `make build` (incl. demo), `make test` (new `internal/demo` unit tests for seeder determinism + scenario tx-count arithmetic + wallet attribution), `golangci-lint run ./...` → 0 issues.
- **Live:** `POST /api/demo/seed {2a..2d}` then `POST /api/demo/run` → assert each scenario returns **exactly 10** transactions, `tx_digest` present, `status` from `sui_getTransactionBlock` == success, `explorer_url` resolvable; wallet viewer returns grouped objects for a guardian-with-dependents address.
- **Frontend:** `npm run build` (type-check + bundle). Manual: health gate blocks run when a service is down; scenario renders steps + digests + suiscan links; palette distinguishes onchain/offchain/mock; reset wipes and re-checks health.

---

## Risks / trade-offs / open questions

- **Gas budget:** 4 scenarios × 10 txs = 40 on-chain txs per full sweep; repeated runs consume pool SUI. Recommend a soft per-scenario "expected cost" note + optional confirmation, and route 2a probes as minimal transfers to keep cost low. Confirm acceptable.
- **Pool balance:** gas pool ~3.98 SUI. 40 txs at testnet gas costs is small, but repeated sweeps could deplete it — may need a faucet-top-up. Flag on dashboard if `sui_getCoins` shows low.
- **Cross-scenario contamination:** each run seeds from a clean DB slice; decide whether `run` auto-resets or requires explicit reset (recommend: auto-seed, non-destructive, so repeated runs are additive unless user hits Reset).
- **2a probe semantics** — see decision #1.
- **Orchestrator host/port:8080 vs 8090** — confirm to avoid clashing with gate.

---

*Plan drafted 2026-08-13 for verification. No code written yet in this turn.*
