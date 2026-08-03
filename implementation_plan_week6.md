# Week 6 — Execution Plan: Sui SDK, zkLogin Custodial & Batch Mint Pipeline

> **Drafted:** 2026-08-03 · **Status:** PLAN — no execution yet
> **Canonical source:** `implementation_plan.md` (Week 6 section) is the source of truth for
> goals/milestones. This file is the *execution breakdown* — ordered, testable work packages
> grounded in the current code, with the defects to close and the verification each must meet.
> Concurrency: Week 7 (Observability) touches `internal/telemetry` but that can wait; Week 6
> is sequential and self-contained on the on-chain path.

---

## 0. Context / current on-chain code state (verified 2026-08-03)

| Area | File | Current state |
|---|---|---|
| Sui client | `internal/sui/client.go` | `MintBatchAttendance` MoveCall + 429 backoff **already real**. But: key loaded from **Sui CLI keystore file** (`~/.sui/.../sui.keystore`, D4), address derived via **fake `0x%x` of first 20 bytes** (D3), `DeriveSuiAddressFromJWT` is a **stub** (`0xzk_...`, D2). `SUI_RPC_MAX_CONCURRENCY` config exists but **no semaphore applied**. |
| zkLogin readiness | `internal/auth/txn_check.go` | **Mock only** (`MockTxnCheck`); real R16 impl needed. |
| Minter HTTP | `cmd/minter/main.go` | `/mint/daily` reads **Redis** rides + requires `user.SuiAddress` (own wallet only — **no dependent/guardian path**); `/api/auth/google` **hardcodes `user@example.com`** (D2); `/mint/resolve-day` (off-chain DayResolver, M4.12) exists. |
| Day resolver | `internal/minter/resolver.go` + `pg_ride_source.go` | Off-chain resolution + durable `pending_mints` **done (M4.12)**. Outputs `mint_ready` vs `pending`. |
| Mint ledger | `internal/postgres` `RecordMint` | `mint_logs` UNIQUE(**user_id**, ride_id, mint_date) + `ON CONFLICT` idempotent. **Gap:** keyed by `user_id`, not participant/wallet — needs reconciling for dependent mints into a guardian wallet (attribution). |
| IPFS metadata | `internal/storage` (Pinata + CIDCache) | **Done (Option A)** — pin once per ride, reuse CID. Good for M6 metadata URLs. |
| Move contract | `move/attendance_nft` | Deployed testnet (`0x78c9...`). **No contract changes needed** (Week 5 audit). `recipient` = plain address → works for guardian custodial mint; NFT `key,store` → transferable for M6.7. |
| Config | `internal/config/config.go` `SuiConfig` | `SUI_GAS_POOL_MNEMONIC`, `SUI_RPC_MAX_CONCURRENCY`, `ENCRYPTION_KEY`, `GOOGLE_OAUTH_CLIENT_*` all present but **partly unused**. |

**Key structural gap to resolve first (W6-A):** the on-chain mint target is a *wallet address*,
but the business key is a *participant*. Week 4 resolved "participant → own | custodial | pending".
Week 6 must drive the **mint-ready** resolutions (incl. dependents → **guardian custodial wallet**)
through `MintBatchAttendance`, and record attribution correctly. `mint_logs` keyed by `user_id`
needs a participant/wallet attribution path (either extend schema or map participant→owner user).

---

## Work Packages (ordered — each ends at a runnable/verifiable state)

### W6-A — Mint attribution & driver wiring (data layer, no real tx yet)
**Goal:** make `/mint/daily` consume the **durable DayResolver output** (not Redis) and mint the
right recipients (own wallet OR guardian custodial wallet for dependents), idempotently.
- [ ] Extend `DayResolver`/`verify` path so each `mint_ready` resolution carries the exact
      `recipient` Sui address (own non-custodial for account-linked; guardian **custodial** wallet
      for dependents — R31).
- [ ] Decide `mint_logs` attribution: add `participant_id` (nullable) + keep `user_id` (guardian)
      so a dependent's NFT minted into the guardian wallet is attributed to the dependent.
      Migration `0004_mint_attribution.up/down.sql` (+ `make migrate-up`; verify version).
- [ ] Wire `cmd/minter` `POST /mint/daily` to iterate DayResolver resolutions instead of
      `Redis.GetUserRides`; `pending` outcomes stay in the ledger (recheck when wallet attached).
- **Verify:** `go build ./...`; new unit test for attribution mapping; integration test against
      compose PG asserting guardian-custodial recipient + dependent attribution row.

### W6-B — Real zkLogin (fix D2 + D3) & custodial wallet creation
**Goal:** replace the stubs — derive a real Sui zkLogin address from a Google JWT.
- [ ] **D2:** parse + `jwt`-verify Google token (issuer `https://accounts.google.com`, aud = client
      id) — replace hardcoded `user@example.com` in `/api/auth/google`; extract `email` + `sub`.
- [ ] **D3:** real Sui address derivation = **blake2b-256(0x00 ‖ pubkey)** on the zkLogin ephemeral
      key (use `golang.org/x/crypto/blake2b`, already in `go.mod`). Fix `pubKeyToSuiAddress`.
- [ ] zkLogin address = f(mnemonic seed, jwt, …) per Sui spec; store ephemeral key + user mapping
      in PG `users` **encrypted at rest** with `ENCRYPTION_KEY` (AES-GCM; verify round-trip).
- [ ] Real `internal/auth` `TxnCheckPerformer` (R16): JWT valid + wallet derived + gas pool funded.
- **Verify:** unit tests for JWT parse + address round-trip (same JWT ⇒ same address); a mocked
      / tiny real `sui_getObjectsOwnedByAddress` (bounded, no spam) showing a derived address.

### W6-C — Gas pool sponsorship (fix D4) + RPC concurrency
**Goal:** stop loading the signer from the fragile SLUI CLI keystore file.
- [ ] **D4:** derive the gas-pool signer from `SUI_GAS_POOL_MNEMONIC` (BIP39 → ed25519 seed), not
      `loadKeyFromKeystore()`. Keep keystore fallback only for local demo; prefer mnemonic.
- [ ] Apply `SUI_RPC_MAX_CONCURRENCY` as a **semaphore** around `MoveCall`/`SignAndExecute` +
      existing 429 backoff (already present).
- **Verify:** unit test that mnemonic ⇒ deterministic signer address; semaphore limits concurrent
      RPC calls (count via test hook).

### W6-D — Batch mint E2E (M6.2 / M6.3), incl. dependent→guardian (M6.6)
**Goal:** submit real (testnet) `attendance::mint_batch` against resolved wallets, idempotently.
- [ ] `POST /mint/daily` (or `/mint/run-day`) → for each mint-ready resolution: gather ride
      metadata (CIDCache), call `MintBatchAttendance(recipient, rideIDs, date, names, urls)`,
      record `mint_logs`, capture `tx_digest`.
- [ ] **M6.6 (dependent):** resolution whose wallet_state = CUSTODIAL_PROXY → recipient = guardian
      **custodial** wallet; validate a child's mint lands in the family wallet (on-chain).
- [ ] **M6.3 idempotency:** re-run → `ON CONFLICT` returns existing digests, **no duplicate mint**
      (verify digest count stable; no second tx).
- [ ] **M6.4 429:** force throttle → exponential backoff succeeds, no partial-state corruption.
- **Verify:** M6.2 (3 rides → 1 batch tx → 3 NFTs, digests recorded), M6.3, M6.4, M6.6. Testnet,
      **bounded, minimal txs**; report **actual tx digests** + reverse-object check.

### W6-E — Custody transfer real impl (R33 → M6.7)
**Goal:** transfer a dependent's NFT from guardians custodial wallet to their own non-custodial
wallet once they link Google.
- [ ] `POST /mint/claim-custody` real impl: Sui **NFT object transfer** (Sui `transfer`/object
      transfer) from custodial wallet → dependent's own address; update off-chain attribution
      (participant wallet_state NONE → OWN_NON_CUSTODIAL + `mint_logs` owner). Week 4 built the
      off-chain state-flip only.
- [ ] Idempotency: only claim NFTs actually in the custodial wallet; skip already-transferred.
- **Verify:** M6.7 on-chain — real object transfer digest; ownership check on the new address;
      attribution updated in PG.

### W6-F — CI / coverage / zkLogin-backed voucher JIT claim (M6.5)
**Goal:** make it green and mock-safe in CI, tie into voucher JIT claim.
- [ ] **M6.5 zkLogin:** Google JWT → derived address, tx signed server-side, NFT lands in user's
      address; **testnet, minimal txs; mock Sui RPC in CI**.
- [ ] **Voucher JIT integration (R9/R13):** voucher claim triggers real zkLogin → custodial wallet
      → background mint (Week 4 built the off-chain link; wire the real wallet creation).
- [ ] `ci-build.yml` builds `minter` image; mock Sui RPC for unit tests.
- [ ] **Coverage:** add `internal/sui` + real `internal/auth` unit tests (currently 0%) → start
      closing the 38%→70% gate (burndown toward Week 6/7 target).
- **Verify:** `make test`, `make lint` (0 issues), `make build` (all 6 bins), CI gate green.

---

## Verification standard (per user)
- **DONE** only when there's a verifiable real effect: a **tx digest**, an **object-ownership
  read-back**, or an **executed integration test** — never a stub report.
- Off-chain vs on-chain must stay explicit: e.g. W6-A is plumbing (unit/integration-verified);
  W6-D/E are the on-chain "done" milestones and require actual digests.
- `mint_logs`/`pending_mints` rows and `scan_events` are durable — verify by row count / query.

---

## Open questions to confirm before starting execution
1. **`mint_logs` attribution:** OK to add nullable `participant_id` (migration 0004) so
   dependent→guardian mints attribute to the dependent? (Alternative: key by recipient address.)
2. **zkLogin prover:** do we use Sui's remote zkLogin prover service (requires JWT issuer proof)
   or generate proofs locally (heavier)? This affects W6-B scope/schedule.
3. **Schedule of keeping the Sui testnet faucet funded** for gas-pool txs during M6 test loops —
   need a funded `SUI_GAS_POOL_MNEMONIC` on the user's machine for the real-mint milestones.

## Definition of done for Week 6
- [ ] Real zkLogin address derivation (D2/D3) + encrypted ephemeral-key storage.
- [ ] Gas-pool signer from mnemonic (D4) + RPC concurrency semaphore.
- [ ] `/mint/daily` mint-ready driver incl. dependent→guardian (M6.6); idempotent (M6.3);
      429-resilient (M6.4).
- [ ] Real custody object transfer (M6.7).
- [ ] zkLogin-backed voucher JIT claim (R9/R13) + M6.5 on testnet, bounded.
- [ ] Coverage gate partially closed (sui/auth tests added); CI green (W6-F).
- [ ] Plan + memory-bank checkboxes honest (off-chain vs on-chain clearly marked).
