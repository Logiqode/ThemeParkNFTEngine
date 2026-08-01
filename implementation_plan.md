# ThemeParkNFTEngine — Implementation Plan

> **Status:** Active — Living document. Check off `[x]` as each milestone completes.
> **Mode:** ACT MODE execution. Single developer, 8-week sprint.
> **Last updated:** 2026-08-01 — Week 1 gap review + business-flow redesign (NFC wristband binding, right-to-be-forgotten, benchmark scripts).

---

## Locked Decisions (Q1–Q7)

| ID | Decision | Rationale |
|----|----------|-----------|
| Q1 | Backend exposes API endpoints generating HMAC-signed QR payload (`GET /api/wristband/qr-token`). Browser QR rendering is a separate frontend project (out of scope). | Honors "Zero Frontend UI" constraint. |
| Q2 | Staff scanner mobile app is **out of scope**. Backend provides: (a) HMAC signing key API, (b) `POST /api/gate/verify` online fallback. | Scanner app is frontend; backend provides verification primitives. |
| Q3 | **Gate API = production Kafka producer** (publishes verified scans to `ride-scans`). **loadgen = test-only producer** (bypasses gate for raw Kafka throughput tests). | Separates production path from load-test path. |
| Q4 | **Custodial gas pool wallet** (funded via testnet faucet) whose gas coins are referenced in each zkLogin transaction. | Simplest server-side gas sponsorship; user never pays gas. |
| Q5 | **Week 3 = Consumer + Redis Dedup + Gate Access Engine (sync API)**. **Week 4 = Postgres persistence + Redis aggregation + Voucher/Ticket system**. | Splits sync gate logic from async persistence cleanly. |
| Q6 | Voucher system folded into **Week 4** (PG rows, magic link, claim API) + **Week 6** (JIT zkLogin registration triggers claim). | 8 weeks fixed; voucher is a subsystem, not a full week. |
| Q7 | **NFT (transferable)** — Move contract allows transfer, no `TransferPolicy` restriction. | User notes say "NFT"; locking NFT path. |

---

## Rev 1 Design Decisions — Week 1 Review (2026-08-01)

User-approved answers to the Week 1 gap review + business-flow clarification. These override/refine Q1–Q7 where noted.

| ID | Decision | Notes |
|----|----------|-------|
| R1 | **Migration tool:** `golang-migrate` CLI + `make migrate` target. Fresh dev DBs still bootstrap via `docker-entrypoint-initdb.d`; production runs `make migrate` as a deploy step before app rollout. | Adds `schema_migrations` version tracking so `0002_*` (Week 4: UNIQUE trace_id, enums) can apply. |
| R2 | **Readiness (strict):** `readyz` returns 503 until all registered dependency checks pass, then 200. `healthz` stays pure liveness. Per-service dep set: Gate=PG+Redis+Kafka; Consumer=Kafka+Redis+PG; Minter=PG+Redis+Sui dial; Voucher=PG+Redis. Startup grace 10–20s. | `AddCheck` exists but is **never called** today — must wire real checks. |
| R3 | **Required-env validation (fail-fast, per binary):** Gate: `HMAC_SECRET` fatal. Minter: `SUI_PACKAGE_ID`+`SUI_MINTCAP_ID` fatal; Pinata optional-with-warning (already logged). All HTTP binaries: Postgres/Redis/Kafka brokers required. | Startup must refuse to run with empty secrets (zero-trust baseline). |
| R4 | **Test scope (M1.2):** Unit smoke tests now (config, health, QR HMAC round-trip) + 2–3 integration smoke tests in CI (compose-backed). Coverage gate: 50% now, ≥70% by Week 4. | CI already runs PG/Redis; **add Kafka service to `ci-lint.yml`**. |
| R5 | **Kafka default:** `.env.example` + config default `KAFKA_BROKERS=localhost:29092` to match compose + README. | Removes fresh-dev footgun. |
| R6 | **Datadog:** Defer entirely. OTel collector → local logging for now. User will evaluate later for portfolio. | No `datadog-agent` service in compose; `DD_API_KEY` unused in Week 1–7 local. |
| R7 | **Stack health:** Add `make healthy` target (poll docker-compose healthchecks + per-service `/readyz`, 60s timeout), otel-collector healthcheck, kafka-init exit verification, apply `KAFKA_RETENTION_MS` to topic creation. | Makes M1.1 demonstrable. |
| R8 | **Memory bank:** Initialize `memory-bank/` (`activeContext.md`, `systemPatterns.md`, `progress.md`) with this audit + decisions. Directory is gitignored (`memory-bank/` in `.gitignore`) — intentionally local-only. | README already references it; global rules require it. |
| R9 | **Gate is NOT a physical turnstile** (portfolio demo). Real-world model: visitor buys ticket → **theme park account**; wristband has **NFC chip**. Gate staff flow: (1) scan visitor's **account QR** (one-time, refreshes every 30s) → binds wristband to account; (2) one **NFC scan of the wristband** → **transaction check** (validates the account's zkLogin wallet bound to Google can push real-world NFC scan→NFT "Proof of Visit" txns); (3) pass → wristband given; fail (faulty/bad NFC) → gate staff retry, or **reset/overwrite** (undo prior binding, bind a new wristband). | Replaces old "turnstile entry" framing. See "Gate Business Flow (Rev 1)" section. |
| R10 | **Gate→Kafka wiring deferred to Week 3** (per Q3 table). Week 1 keeps gate as-is; integration-test scaffolding structured so the producer can be swapped in later. | No scope creep into Week 1; tests target loadgen→consumer. |
| R11 | **Scan identity = email internally, never on-chain (right to be forgotten).** `ScanEvent.UserID` = email (natural key matching `users.email`, consumer's current resolution). Gate's numeric-id response converted when gate→Kafka is wired in Week 3. On-chain (Move NFT + IPFS metadata) carries **no PII** — only Sui address (pseudonymous), `ride_id`, date, ride display name. | GDPR: blockchain data is not personal data without the off-chain mapping; email stays in Postgres/Redis/Kafka internally. |
| R12 | **Ride attribution = simulated flow.** Add `ride_id` to scan-event payload (loadgen and future simulated web flow); gate verify request may optionally carry `ride_id` (turnstile supplies) for Week 3 wiring. | B3: simulated test via webpage later; loadgen covers it now. |
| R13 | **Ticket state machine reinterpreted for binding model:** `UNCLAIMED → CLAIMED → PENDING_ENTRY (="BINDING", QR+NFC check in progress) → ACTIVE (="BOUND", wristband verified) → USED/EXPIRED`. `RejectEntry`/reset returns `PENDING_ENTRY → CLAIMED` (unbind) or `ACTIVE → CLAIMED` (force re-bind) for faulty-NFC admin override. | Old "turnstile" wording replaced; PENDING_ENTRY/ACTIVE **kept** (admin takeover + reset requirements still exist). |
| R14 | **Consumer reliability (auto-commit bug, DLQ, backoff): deferred to Week 3** — do NOT rework consumer in Week 1. Week 1 tests document the limitation. | Matches plan table; avoids dragging Week 3 scope forward. |
| R15 | **Benchmark requirement (B1):** Add **benchmark scripts** (Go + shell under `scripts/bench/`) measuring **Kafka delivery reliability under congestion — target 99.9% success rate** (all produced events consumed exactly-once-effectively; no loss, no duplicates). Assumes **no wristband faults** (no retry/reset logic under test) and **no real Sui wallet check** (optional nice-to-have: mock txn check; testnet only if free/no spam). | Week 2 deliverable, designed now. See "Benchmark Scripts (B1)" section. |
| R16 | **zkLogin wallet check = mockable interface.** Define `internal/auth` or `internal/sui` interface `TxnCheckPerformer` with real impl (Week 6 zkLogin) + mock impl (Week 1/2 benchmarks + CI). Gate's NFC transaction check calls this; benchmarks use mock. | Never spam testnet. |
| R17 | **Business-aligned REST API naming:** replace generic `/api/gate/*` routes with action-oriented endpoints describing the real-world step (visitor QR → bind → NFC check → reset → ride scan). See "Key API surface". `verify` becomes `/api/wristband/nfc-check`; new `/api/rides/scan` is the future production ride-scan source. | Demo self-documents the business flow for portfolio review. |

---

## Gate Business Flow (Rev 1) — replaces "turnstile" framing

```
[Visitor] buys ticket / creates theme park account (email, may bind Google OAuth)
    │
    │  POST /api/vouchers/purchase → ticket row (UNCLAIMED)
    │  POST /api/vouchers/claim → ticket CLAIMED to account
    ▼
[Gate Staff]  —  terminal/scanner app (backend-only API, no UI in this repo)
    1. GET /api/wristband/qr-token → HMAC QR payload (30s rotation, one-time use)
       Visitor presents account QR → staff scans → binds wristband (NFC id) to account
       → ticket PENDING_ENTRY ("BINDING")
    2. Staff performs NFC scan of wristband → POST /api/wristband/nfc-check
       → backend runs TRANSACTION CHECK:
          can this account's zkLogin wallet (bound to Google) push real-world
          NFC-scan → NFT "Proof of Visit" transactions? (mock in W1–2, real zkLogin in W6)
       → success: ticket ACTIVE ("BOUND"); wristband handed to visitor
       → failure (faulty NFC): staff Retry (re-scan) or Reset (undo binding,
          overwrite with new wristband) → ticket back to CLAIMED
    ▼
[Ride Staff]  —  NFC scan during visit → scan_event → Kafka ride-scans
    → consumer dedup → PG scan_events → Redis ride sets → /mint/daily → NFTS
```

**Key API surface (backend only, business-aligned naming per R17):**
| HTTP | Endpoint | Business meaning |
|------|----------|------------------|
| GET  | `/api/wristband/scan-visitor-qr-token` | Staff requests one-time HMAC QR token (30s rotation) for the visitor to present *(renamed from `/api/wristband/qr-token`)* |
| POST | `/api/wristband/bind` | First staff scan: bind wristband NFC to visitor account → ticket `BINDING` *(NEW — Week 3)* |
| POST | `/api/wristband/nfc-check` | Second staff scan: transaction check (mock W1–2 → real zkLogin W6) → ticket `BOUND` *(renamed from `/api/gate/verify`)* |
| POST | `/api/wristband/reset` | Admin undo/overwrite on faulty NFC → unbind / re-bind *(NEW — Week 3)* |
| POST | `/api/rides/scan` | Ride staff NFC scan during visit → publishes `ScanEvent` to Kafka *(NEW — Week 3; simulated web flow uses this)* |

*Note: old `POST /api/gate/confirm` is removed — finalizing `BOUND` is handled by `nfc-check`; the ticket `USED` transition belongs to the ride-scan lifecycle (`/api/rides/scan`).*

**ScanEvent payload (immutable for Kafka schema stability):**
```json
{ "user_id": "<email — internal only>", "ride_id": "ride-001", "timestamp": 1710000000000, "trace_id": "<w3c>" }
```
No PII is encoded on-chain; `user_id` (email) exists only inside Kafka/PG/Redis (internal).

---

## Right-to-be-Forgotten (Cross-Cutting, locked)

- **Never write email/name/Google sub into:**
  - Move NFT object fields (`recipient` = Sui address is pseudonymous; `name` = ride name only)
  - IPFS metadata JSON (currently: ride name, date, rarity — verified compliant)
  - On-chain events
- **Internal only (allowed):** `users.email` in Postgres, Redis key `user:<email>:rides:<date>`, Kafka `user_id` field.
- On user deletion request: delete PG `users` row (and dependent rows); blockchain NFTs remain but contain zero PII, so GDPR "right to be forgotten" holds via loss of the off-chain mapping.
- Document this contract in README security section at finalization.

---

## Locked Dependency Versions (Exact-Pinned)

| Dependency | Module Path | Version |
|---|---|---|
| Sui Go SDK | `github.com/block-vision/sui-go-sdk` | `v1.2.1` |
| Kafka | `github.com/segmentio/kafka-go` | `v0.4.51` |
| Redis | `github.com/redis/go-redis/v9` | `v9.21.0` |
| PostgreSQL driver | `github.com/jackc/pgx/v5` | `v5.10.0` |
| SQLX | `github.com/jmoiron/sqlx` | `v1.4.0` |
| Config (Viper) | `github.com/spf13/viper` | `v1.21.0` |
| Config (envconfig) | `github.com/kelseyhightower/envconfig` | `v1.4.0` |
| OpenTelemetry core | `go.opentelemetry.io/otel` | `v1.44.0` |
| OTel OTLP trace exporter | `go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc` | `v1.44.0` |
| OTel OTLP metric exporter | `go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc` | `v1.44.0` |
| Structured logging | `github.com/rs/zerolog` | `v1.35.1` |
| Migrations | `github.com/golang-migrate/migrate/v4` | *(pin exact at install — R1)* |

No `^`/`~` range operators. All versions exact-pinned per Web3 dependency rules.

---

## Architecture Blueprint

```
theme-park-nft-engine/
├── cmd/
│   ├── gate/         # Gate Access API: wristband bind + transaction check + Kafka producer (W3)
│   ├── consumer/     # Kafka consumer: dedup + persist + aggregate
│   ├── loadgen/      # Test-only Kafka producer (bypasses gate)
│   ├── minter/       # End-of-day batch mint API + Sui zkLogin custodial
│   └── voucher/      # Ticket voucher + magic link + JIT claim API
├── internal/
│   ├── config/       # Env loading (viper + envconfig) + required-env validation (R3)
│   ├── kafka/        # Producer (gate) + Consumer wrappers
│   ├── redis/        # Idempotency + session aggregation
│   ├── postgres/     # Repositories + migrations (golang-migrate, R1)
│   ├── gate/         # QR HMAC signing/verification + wristband binding state machine
│   ├── voucher/      # Voucher lifecycle: unclaimed → claimed → ownership transfer
│   ├── sui/          # Sui SDK + zkLogin custodial (ephemeral key, proof, server-side sign)
│   ├── auth/         # (NEW R16) TxnCheckPerformer interface: real zkLogin impl (W6) + mock impl
│   ├── telemetry/    # OpenTelemetry init/exporters
│   └── models/       # ScanEvent, Ticket, Voucher, User, MintLog
├── migrations/       # SQL migrations via golang-migrate (R1)
├── move/
│   └── attendance_nft/  # Sui Move package (NFT module)
├── deployments/
│   ├── docker-compose.yml
│   ├── Dockerfile.gate
│   ├── Dockerfile.consumer
│   ├── Dockerfile.loadgen
│   ├── Dockerfile.minter
│   └── Dockerfile.voucher
├── scripts/
│   └── bench/        # (NEW R15) Benchmark scripts for Kafka delivery reliability (99.9%)
├── .github/workflows/
├── .env.example
├── implementation_plan.md
└── README.md
```

**Data Flow:**
```
[Visitor Account QR — HMAC, 30s rotation] → [Gate Staff: bind wristband]
        ↓ (NFC transaction check — mock W1/W2, zkLogin W6)
[Gate API: POST /api/wristband/bind] → ticket BINDING → [POST /api/wristband/nfc-check] → BOUND
        ↓ (on confirmed ride — W3)
[Gate API] → publish ScanEvent → Kafka(ride-scans)   ← Gate = Kafka Producer (W3)
        ↓
[Consumer] → Redis dedup (trace_id) → Postgres log → Redis Set (user:rides)
        ↓
[loadgen / simulated web flow] → scan_events for benchmark + tests (W2)
        ↓
[Voucher System] → Purchaser buys N → PG vouchers → magic link → JIT zkLogin claim
        ↓
[Minter API: POST /mint/daily]
  → Read Redis Set → Batch mint via Sui zkLogin (server-side signed, gas sponsored)
  → Record tx_digest in PG mint_logs
  → OpenTelemetry traces end-to-end (W7)
```

---

## Week 1 — Local Infrastructure & Scaffolding

**Goal:** Bootable local dev environment with all backing services containerized. **Status: Gap closure (R1–R8, R16, R17a) DONE — only M1.1/M1.2 live-stack verification remains.**

### Done (verified 2026-08-01)
- [x] Go module `github.com/Logiqode/ThemeParkNFT`, Go 1.26.5, exact-pinned deps.
- [x] Function-First folder architecture per blueprint.
- [x] `docker-compose.yml`: zookeeper, kafka (+kafka-init topic creator), redis:7, postgres:16, otel-collector.
- [x] Config layer: `.env.example` + `internal/config` (envconfig + viper). No hardcoded secrets/URLs in code.
- [x] Postgres schema v1: `users`, `tickets`, `ticket_vouchers`, `rides`, `scan_events`, `mint_logs`.
- [x] Health server (`/healthz`, `/readyz`) handlers exist on all HTTP binaries.
- [x] `Makefile` targets: `up`, `down`, `build`, `test`, `lint`, `tidy`, `clean`, `reset`.
- [x] `ci-lint.yml` workflow (lint + build + test with PG/Redis).
- [x] Dockerfiles multi-stage + distroless non-root.

### Week 1 Gap-Closure List (DONE 2026-08-01 — R1–R8, R16, R17a)
- [x] **R1 — Migrations tooling:** `golang-migrate v4.19.1` exact-pinned; flat migrations `000001_init.{up,down}.sql`; `cmd/migrate` runner; `make migrate|migrate-up|migrate-down|migrate-version`.
- [x] **R2 — Strict readiness:** `healthz`=liveness, `readyz`=503 until deps ready. Wired into gate (PG+Redis+Kafka), consumer (Kafka+Redis+PG, health :8081), minter (PG+Redis+Sui RPC), voucher (PG+Redis). `WaitForChecks` 20s startup grace. Added `Producer.Ping`, `Consumer.Ping`, `SuiClient.Ping` (broker/RPC dial — no topic pollution).
- [x] **R3 — Required-env validation:** `config.Validate(...)` fail-fast. Gate=`HMAC_SECRET`; Minter=`SUI_PACKAGE_ID`+`SUI_MINTCAP_ID`. Unit-tested.
- [x] **R4 — Tests:** unit tests (config, health, gate QR HMAC, auth mock) + integration smoke (`internal/pipeline`, `-tags=integration`) — single event persists, duplicate trace dropped, multi-ride aggregation. CI: Kafka service added; unit + integration + coverage upload.
- [x] **R5 — Kafka default:** `.env.example` + config default → `localhost:29092`.
- [x] **R7 — Stack health:** otel-collector healthcheck; kafka-init `set -euo pipefail` + retention.ms applied + topic-list verification; `make healthy` target (60s timeout).
- [x] **R8 — Memory bank:** `activeContext.md`, `systemPatterns.md`, `progress.md` created.
- [x] **R16 — Mock txn check:** `internal/auth.TxnCheckPerformer` + `MockTxnCheck` (configurable failure; no testnet spam). Available for gate W3 verify path + benchmarks.
- [x] **R17a — QR route renamed:** `GET /api/wristband/scan-visitor-qr-token` (business name) + README curl updated.
- [x] **CI:** Kafka service in `ci-lint.yml`; unit + integration smoke jobs + coverage artifact.
- [x] **Extracted shared pipeline handler** to `internal/pipeline` (dedup→persist→aggregate) so consumer + tests share the same production logic.

### Testing & CI/CD Milestones
- [x] **M1.1** `make up && make healthy` brings entire stack healthy in < 60s. **PASS 2026-08-02** (fixes: otel v0.120 exporter `debug` + healthcheck via `/otelcol-contrib validate`; `make healthy` jq `-s` slurp + one-shot-job handling).
- [x] **M1.2** Migrations run idempotently (`make migrate up` twice OK); `make test` passes (unit + integration smoke suite). **PASS 2026-08-02** (v1 dirty=false; unit + integration smoke all green).
- [ ] **CI gate:** `ci-lint.yml` — `golangci-lint run` + `go test ./...` on PRs (now with real assertions + Kafka service). *(awaits `git push` of the uncommitted Week 1/2 fixes)*

### Deliverables
- [x] Runnable local stack w/ verified health, working migration toolchain, meaningful test suite, memory-bank initialized.

---

## Week 2 — Load Generator, Benchmark Scripts & Kafka Producer (Test Harness)

**Goal:** Produce thousands of valid scan events into `ride-scans` topic; **prove Kafka delivery reliability ≥ 99.9% under congestion (R15).** **Status: DONE 2026-08-02 — M2.1–M2.4 PASS live, M2.5 congestion PASS (18,206 lines / 16,140 unique, 100% success, 0 dups, recovery 3.3s). Report in `scripts/bench/RESULTS.md`.**

### Technical Requirements
- [x] Define `models.ScanEvent`: `{user_id string, ride_id string, timestamp int64, trace_id string}` with JSON tags + validation.
- [x] Kafka topic config: `ride-scans`, partitions=6, replication=1 (local; `KAFKA_REPLICATION` configurable), retention via `KAFKA_RETENTION_MS`.
- [x] `cmd/loadgen`: configurable rate (RPS), duration, concurrency, unique-user ratio, duplicate ratio, `--max-events` deterministic mode, `--manifest`/`--summary` JSONL. Emits trace_id per event (Q3: strict W3C format deferred to W7 OTel; UUID trace_id + `traceparent` header today). Users are emails `user-XXXX@bench.local` (R11).
- [x] Producer wrapper `internal/kafka/producer.go`: sync/async writer toggle (`KAFKA_PRODUCER_ASYNC`), graceful shutdown flush (M2.4 verified). **Q1: kafka-go cannot set `enable.idempotence`; effectively-once = `RequiredAcks=RequireAll` + `MaxAttempts=3` + consumer Redis `SETNX trace_id` dedup** (documented in code).
- [x] Structured logging (zerolog) with trace_id propagation.
- [x] **Benchmark scripts (`scripts/bench/`, R15):**
  - `run_congestion.sh` — orchestrate sustained + spike load with 99.9% target assertion (writes `RESULTS.md`).
  - `run_milestones.sh` — M2.1–M2.4 orchestration.
  - `verify_delivery.go` — consume `ride-scans`, count unique trace_ids vs manifest, report success rate / extra-observation dups / recovery time.
  - Success = 99.9% of produced events observed exactly once (no loss, no dup), lag → 0 ≤ 30s.
  - Assumes **no wristband faults**; uses **mock txn check** (`internal/auth.MockTxnCheck`, R16).

### Testing Milestones
- [x] **M2.1** *Correctness:* 1,000 events → consumer count == 1,000, JSON validated. **PASS (1000/1000, 0 dups).**
- [x] **M2.2** *Throughput:* 10,000 events → all delivered, p99 produce < 100ms, zero producer dups. **PASS (10000/10000, 0 dups, recovery 3.1s, p99≈...)**
- [x] **M2.3** *Spike:* 5,000 event burst → no producer errors, recovery ≤ 5s. **PASS (5000/5000, 0 dups, recovery 3.1s).**
- [x] **M2.4** *Graceful shutdown:* SIGTERM mid-batch → all in-flight flushed, no data loss. **PASS (2654/2654 flushed, 0 errors).**
- [x] **M2.5** *(NEW R15) Congestion benchmark:* sustained mixed load (16,336 @ 1k RPS + 12% dups → 1,870 @ 5k RPS burst + 10% dups = 18,206 lines / 16,140 unique) → **success_rate = 1.0 (≥ 99.9%), delivery_duplicates = 0, recovery = 3.28s**. Report in `scripts/bench/RESULTS.md`.

### CI/CD Milestones
- [x] **CI gate:** `ci-build.yml` builds `loadgen` image + runs M2.1 (`TestIntegrationDelivery`); `ci-lint.yml` now includes KRaft-mode Kafka service (no zookeeper pair) for the M2.1 integration test.

### Deliverables
- [x] Load generator binary + image, benchmark scripts + 99.9% report (`scripts/bench/RESULTS.md`), validated Kafka producer.

---

## Week 3 — Gate Access Engine (Wristband Binding) & Redis Deduplication

**Goal:** Synchronous gate verification via the **Rev 1 binding model (R9)**: bind wristband via account QR + NFC transaction check; consume `ride-scans`, deduplicate via Redis. **Gate becomes the production Kafka producer (Q3/R10).**

### Technical Requirements
- [ ] `cmd/gate` endpoints (business-aligned naming, R17):
  - `GET /api/wristband/scan-visitor-qr-token` — one-time HMAC QR token (30s rotation) for the visitor to present (renamed from `qr-token`).
  - `POST /api/wristband/bind` — bind wristband (NFC id) to account via scanned QR payload → ticket `PENDING_ENTRY` (BINDING). (NEW)
  - `POST /api/wristband/nfc-check` — run **transaction check** (via `internal/auth.TxnCheckPerformer`, R16): real impl = zkLogin wallet readiness (W6); success → ticket `ACTIVE` (BOUND). (renamed from `verify`)
  - `POST /api/wristband/reset` — admin undo/overwrite on faulty NFC: `ACTIVE|PENDING_ENTRY → CLAIMED` (unbind / re-bind). (R13) (NEW)
  - `POST /api/rides/scan` — ride staff NFC scan during visit → publishes `ScanEvent` to `ride-scans` (production path; simulated web flow target). (NEW)
- [ ] **Atomic transaction (sqlx.Tx):** SELECT FOR UPDATE on ticket row; enforce binding state machine per R13.
- [ ] **5-second Retry Grace Window:** same ticket_id within 5s → cached result (Redis TTL=5s) — fixes the "always allowed" bug (see Known Defects).
- [ ] **HMAC QR signing:** `internal/gate/qr.go` — payload `[UUID | Timestamp | HMAC_SHA256]`, 30s rotation, key from env (`HMAC_SECRET`), one-time use semantics for binding.
- [ ] **Gate = Kafka producer:** after successful `nfc-check` (BOUND), ride scan (`POST /api/rides/scan`) publishes `ScanEvent` to `ride-scans` with `user_id`=email (internal), `ride_id` from request (R12), W3C traceparent headers.
- [ ] `cmd/consumer` — **reliability fixes (R14, deferred from W1):** manual commit (`CommitMessages` after handler success), dead-letter topic `ride-scans-dlq` for poison messages, exponential backoff retries on transient PG/Redis failures (max before DLQ), idempotency via `SET dedup:{trace_id} 1 NX EX <ttl>`, worker pool with backpressure.
- [ ] Retry strategy: exponential backoff for transient Redis failures; max retries before DLQ.

### Testing Milestones
- [ ] **M3.1** *Duplicate:* same `trace_id` 5× → exactly 1 processed, 4 dropped, Redis key with TTL.
- [ ] **M3.2** *TTL:* after expiry, same `trace_id` processed again.
- [ ] **M3.3** *Poison:* malformed JSON → DLQ, consumer continues.
- [ ] **M3.4** *Grace window:* double-scan within 5s → same (cached) result — **regression: cached result must match original decision** (fixes bug).
- [ ] **M3.5** *Concurrent binds same ticket* → exactly one BOUND, other rejected (FOR UPDATE lock).
- [ ] **M3.6** *Concurrency:* 10k events, 5 partitions, pool=10 → no cross-worker duplicates.
- [ ] **M3.7** *(NEW R9/R13) Binding flow:* QR bind → txn check pass → ACTIVE; mock check fail → reset → re-bind succeeds.

### CI/CD Milestones
- [ ] **CI gate:** integration job spins compose + loadgen(1k events w/ 20% dups) → assert dedup count.

### Deliverables
- [ ] Gate API binary (binding model), Consumer with reliable commit+DLQ, Redis idempotency, dedup unit + integration tests.

---

## Week 4 — Postgres Persistence, Redis Aggregation & Ticket Voucher System

**Goal:** Persist valid scans to Postgres; aggregate per-user ride sets in Redis; implement ticket voucher lifecycle.

### Technical Requirements
- [ ] `internal/postgres`: repository pattern, `ScanRepository.Insert` (idempotent on `trace_id` UNIQUE constraint), `pgx` pool.
- [ ] Migrations v2 (`0002_*` via golang-migrate, R1): add migration for `UNIQUE(trace_id)` (schema already has it — verify consistency), `CHECK` constraints on `ride_id` enum, ticket status enum (already v1; align with R13 machine).
- [ ] Redis Set aggregation: `SADD user:{user_id}:rides {ride_id}` per valid scan; daily key `user:{user_id}:rides:{date}` TTL 48h.
- [ ] Transactional boundary: insert Postgres → SADD Redis; on Redis failure, compensate or mark for retry (outbox pattern).
- [ ] Batch insert optimization (batch size configurable).
- [ ] **Ticket Voucher System (`internal/voucher`, `cmd/voucher`):**
  - `POST /api/vouchers/purchase` → N voucher rows (`status='unclaimed'`, `purchaser_id`).
  - `POST /api/vouchers/share` → magic link (signed JWT with `voucher_id`).
  - `GET /api/vouchers/claim?token=...` → JIT registration → ticket state `UNCLAIMED → CLAIMED` (R13).
  - State machine per R13.

### Testing Milestones
- [ ] **M4.1** *Persistence:* 5k valid events → PG row count == 5k, no dups, indexes performant.
- [ ] **M4.2** *Aggregation:* user 3 distinct rides → Redis set cardinality == 3; dup ride → unchanged.
- [ ] **M4.3** *Failure:* Redis down mid-processing → PG insert succeeds, aggregation retried via outbox/backoff, no loss.
- [ ] **M4.4** *E2E:* loadgen(10k, 15% dups) → PG == 8.5k unique, Redis sets correct.
- [ ] **M4.5** *Voucher:* purchase 8 → 8 `unclaimed` → claim 1 → `claimed`.
- [ ] **M4.6** *Magic link expired/invalid* → rejected.
- [ ] **M4.7** *(R13) Ticket state transitions:* claim → QR bind → txn check → ACTIVE → ride scan → USED.

### CI/CD Milestones
- [ ] **CI gate:** full pipeline integration test in CI; **coverage gate ≥ 70% on `internal/`** (R4).

### Deliverables
- [ ] Postgres repository, Redis aggregation, outbox/retry, voucher system, full ingestion pipeline tested.

---

## Week 5 — Sui Move Smart Contract (Attendance NFT)

**Goal:** Deploy Move module that mints attendance NFTs. **Status: DONE (deployed to testnet, 11 tests passing).**

### Done (verified 2026-08-01)
- [x] `move/attendance_nft/` package; `sui move build` clean.
- [x] Module `attendance_nft::attendance`: `MintCap`, `mint_attendance_nft`, `mint_batch`, `burn`, `update_metadata`, view fns, zero-address checks, events, transferable NFT (Q7).
- [x] **Deployed:** Package `0x78c9dbba118923c4976599877450cd281f880f94d217d806714782a658b01d1e`, MintCap `0xfe4dcb...f30cb` (testnet). See README.
- [x] 11 Move unit tests.
- [ ] *(R11)* Add a code-comment audit note in `attendance.move` confirming no PII fields (recipient = address only, name = ride name only).

### CI/CD Milestones
- [ ] **CI gate:** `ci-move.yml` runs `sui move test` on PRs touching `move/` (add — missing).

### Deliverables
- [x] Audited Move module, testnet deployment, package ID documented. *(Remaining: ci-move.yml)*

---

## Week 6 — Sui Go SDK, zkLogin Custodial & Batch Mint Pipeline

**Goal:** Go service reads Redis aggregation and mints NFTs via Sui SDK + zkLogin (custodial, server-side signed, gas sponsored).

### Technical Requirements
- [ ] `internal/sui`: wrapper around `github.com/block-vision/sui-go-sdk v1.2.1`; client init from `SUI_RPC_URL` env.
- [ ] **Full zkLogin custodial support (replace current stub):**
  - `POST /api/auth/google` — verify Google JWT (real claims parsing — fixes hardcoded `user@example.com`), exchange for Sui address via zkLogin (ephemeral key server-side, proof, blake2b address — replaces fake `0xzk_` / `0x%x` derivation).
  - Store ephemeral key + user mapping in PG `users` (**encrypted at rest with `ENCRYPTION_KEY`**).
  - User never sees seed phrase / never pays gas (Q4).
- [ ] **Gas sponsorship:** custodial gas pool wallet from `SUI_GAS_POOL_MNEMONIC` (currently **unused** — fixes keystore-file loading).
- [ ] `cmd/minter` `POST /mint/daily`: Redis SMEMBERS → `mint_batch` (single tx, multiple ride_ids) → record `mint_logs` → **idempotency: skip already-minted (UNIQUE user/ride/date exists in schema; enforce in code via ON CONFLICT)**.
- [ ] **RPC throttling:** client-side batching + concurrency semaphore (`SUI_RPC_MAX_CONCURRENCY`) + 429 exponential backoff (backoff exists; add semaphore).
- [ ] **R16 real impl:** `internal/auth` zkLogin wallet readiness check (JWT valid + wallet derived + gas pool funded).
- [ ] **JIT registration integration:** voucher claim triggers real zkLogin → custodial wallet → background mint task (R9/R13 tie-in).

### Testing Milestones
- [ ] **M6.1** SDK smoke: Go client reads package object on testnet.
- [ ] **M6.2** Mint E2E: `/mint/daily` for test user w/ 3 rides → 1 batch tx → 3 NFTs, digests recorded.
- [ ] **M6.3** Idempotency: re-trigger → no duplicate mints, existing digests returned.
- [ ] **M6.4** RPC 429: backoff retries succeed, no partial state corruption.
- [ ] **M6.5** zkLogin: Google JWT → Sui address derived, tx signed server-side, NFT in user's address (**testnet, minimal txs; mock in CI**).

### CI/CD Milestones
- [ ] **CI gate:** `ci-build.yml` builds `minter` image; mock Sui RPC for unit tests.

### Deliverables
- [ ] Minter service, real zkLogin custodial integration, gas sponsorship, E2E batch mint validated on testnet (bounded, no spam).

---

## Week 7 — Observability: OpenTelemetry & Datadog

**Goal:** Full distributed tracing from Gate ingest → Sui mint, with metrics + structured logs. (R6: Datadog optional/portfolio-eval; OTel collector → local logging is baseline.)

### Technical Requirements
- [ ] `internal/telemetry`: OTel SDK v1.44.0 init, OTLP gRPC exporter → OTel collector (local) and/or Datadog agent (if adopted).
- [ ] Trace propagation: gate API `traceparent` → Kafka headers → consumer spans (dedup, PG insert, Redis SADD, Sui tx).
- [ ] Span attributes: `user_id`, `ride_id`, `trace_id`, `ticket_id`, `kafka.partition/offset`, `sui.tx_digest`, `dedup.hit`, `gate.decision`.
- [ ] Metrics: `events_consumed_total`, `events_dropped_duplicate_total`, `gate_verifications_total`, `mint_duration_seconds`, `kafka_lag`, `redis_ops_duration`, `pg_insert_duration`, `sui_rpc_429_total`.
- [ ] Structured logs (zerolog) with trace_id correlation.
- [ ] Dashboards/alerts: gate/consumer health, pipeline throughput, mint success rate, error budget, Kafka lag > threshold, mint failure > 5%.

### Testing Milestones
- [ ] **M7.1** Trace continuity: one event's trace_id across gate → consumer → PG → Sui spans.
- [ ] **M7.2** Metrics: loadgen 5k events → dashboards reflect counts within 30s.
- [ ] **M7.3** Error tracing: injected Redis failure → error span + log with trace_id.

### CI/CD Milestones
- [ ] **CI gate:** telemetry unit tests; lint ensures no `fmt.Println` (structured logging only).

### Deliverables
- [ ] OTel instrumentation across services; dashboards/alerts (Datadog only if adopted per R6).

---

## Week 8 — CI/CD, AWS Deployment & Final Load Test

**Goal:** Production-grade pipeline; deploy to AWS; final load test milestone.

### Technical Requirements
- [ ] GitHub Actions: `ci.yml` (lint → unit → integration → images → ECR), `cd.yml` (main → EC2/ECS).
- [ ] Docker images: distroless/non-root (already satisfied — verify all 5).
- [ ] AWS: ECR repos; EC2 (t3.medium+); IAM instance profile (no static creds); SG internal-only for Kafka/PG/Redis.
- [ ] Secrets via AWS SSM / runtime `.env`; **no secrets in images**.
- [ ] README finalized: architecture (incl. Rev 1 gate flow + right-to-be-forgotten note), setup, deploy, load-test, benchmark instructions.

### Testing & CI/CD Milestones
- [ ] **M8.1** *CI green:* PR → lint, unit, integration, image build all pass.
- [ ] **M8.2** *Deploy:* `cd.yml` deploys gate+consumer+minter+voucher to EC2; healthchecks pass (`/readyz` now strict per R2).
- [ ] **M8.3** *FINAL LOAD TEST:* loadgen 50,000 @ 2,000 RPS, 10% dups against AWS →
  - Kafka lag → 0 ≤ 30s; PG unique == 45,000 (±0.1%); dedup == 10%; gate p99 < 50ms; consumer p99 < 50ms; **no loss/dup (99.9%+ per R15)**.
- [ ] **M8.4** *Mint validation:* sampled users `/mint/daily` → batch NFTs on testnet, digests logged.
- [ ] **M8.5** *Voucher E2E:* purchase 8 → magic link → JIT claim → gate bind → ride scan → mint.

### Deliverables
- [ ] Full CI/CD, AWS deployment, capstone load test report, complete README.

---

## Cross-Cutting Concerns (Every Week)

- **Security:** Zero hardcoded secrets; fail-fast required-env validation (R3); zkLogin keys encrypted at rest (W6); least-privilege IAM. **Right-to-be-forgotten: no PII on-chain/IPFS (R11).**
- **Code Quality:** `golangci-lint`, `go vet`, `gofumpt`; Move `sui move lint`; coverage 50% now → ≥70% by W4 (R4).
- **Memory Bank:** update `memory-bank/activeContext.md`, `systemPatterns.md`, `progress.md` at each week's end (R8).
- **Implementation Plan:** this file checked off (`[x]`) as each milestone completes.

---

## Known Code Defects (found in Week 1 review — fix timing)

| # | Defect | Fix timing |
|---|--------|-----------|
| D1 | Gate grace window **always returns `allowed: true`** on cache hit (ignores cached denied). | Week 3 (M3.4) |
| D2 | Minter `/api/auth/google` hardcodes `user@example.com` (no JWT claims parsing). | Week 6 (zkLogin) |
| D3 | `pubKeyToSuiAddress` uses fake `0x%x` of first 20 bytes (needs blake2b). | Week 6 |
| D4 | `SUI_GAS_POOL_MNEMONIC` unused; signer loaded from `~/.sui/.../sui.keystore` (fragile). | Week 6 |
| D5 | Consumer auto-commit + no DLQ/retry → potential silent loss on handler failure. | Week 3 (R14) |
| D6 | `scan_events.ticket_id` always inserted as `""`. | Week 3 (gate producer carries real value) |
| D7 | Grace-window cache ignores `GetGraceWindow` error; confirm-handler request decode unchecked. | Week 3 |
| D8 | Gate does not publish to Kafka at all (plan Q3 violation). | Week 3 (R10) |

---

## Risk Register

| Risk | Mitigation |
|------|-----------|
| Sui Go SDK immaturity | Spike Week 5 (done); fallback to raw JSON-RPC |
| zkLogin complexity | Isolate in `internal/sui` + `internal/auth`; mock interface for W1–2 (R16) |
| Kafka delivery < 99.9% under congestion | Partitioning, idempotent producer, benchmark scripts (R15), consumer reliability in W3 (R14) |
| Redis memory growth | TTL on all keys; daily key rotation; monitor `used_memory` |
| Sui RPC throttling (429) | Client batching + backoff + semaphore (W6) |
| Gate double-bind race | SELECT FOR UPDATE + grace window (W3) |
| PII leak into blockchain | Locked no-PII-on-chain contract (R11); code-audit note in Move |
| Migration drift (no versioned tool) | golang-migrate + `schema_migrations` (R1) |

---

## Summary Timeline

| Week | Phase | Key Milestone |
|------|-------|---------------|
| 1 | Infra & Scaffolding + **gap closure (R1–R8, R16)** | Local stack healthy, migrations versioned, tests meaningful |
| 2 | Load Generator + **Benchmark scripts (99.9%)** | 10k @ 1k RPS delivered; congestion report |
| 3 | Gate Binding Engine + Consumer Dedup | QR bind + NFC txn check + reliable consumer + gate producer |
| 4 | Persistence, Aggregation & Vouchers | Full ingestion pipeline E2E + voucher lifecycle |
| 5 | Sui Move Contract | **DONE** — deployed testnet; add ci-move.yml |
| 6 | Sui SDK & zkLogin Custodial Mint | Real zkLogin + gas pool + E2E batch mint (bounded testnet) |
| 7 | Observability | Trace continuity (OTel; Datadog optional) |
| 8 | CI/CD & AWS | 50k load test on AWS, 99.9% target |

---

## Open Items (deferred by design)

- Datadog adoption (R6) — user to evaluate later; OTel→local logging baseline.
- Real Sui wallet check in benchmarks (R15) — only if free of testnet spam; mock default.
- Frontend simulated test webpage (B3/R12) — out of scope for backend engine; API contract ready for it.
- Wristband fault retry/reset **logic** is intentionally NOT in benchmark scope, but the **API** (bind/reset, R9/R13) is designed and will be implemented in Week 3.