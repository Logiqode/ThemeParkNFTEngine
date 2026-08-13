# ThemeParkNFTEngine — Implementation Plan

> **Status:** Active — Living document. Check off `[x]` as each milestone completes.
> **Mode:** ACT MODE execution. Single developer, 8-week sprint.
> **Last updated:** 2026-08-13 — **WEEK 6 ON-CHAIN EXECUTED** — `/mint/run-day` submitted **10 real Sui-testnet batch-mint transactions, 10 unique digests, all `success`, 41 NFTs created** (M6.1/M6.2/M6.5 complete; digests + object counts in the Week 6 section). Two enabling fixes landed: **Pinata JWT auth** (`PINATA_JWT`, new) and **block-vision SDK `unsafe_moveCall` encoding** (hex-encoded pure args + non-nil `TypeArguments`). `make build/test/lint` green. Real zkLogin proof-gen + custody object transfer remain optional/Week-7. Prior: **Week 4 Rev 3 OFF-CHAIN CORE EXECUTED** (migration 0002 participants/pending_mints + `internal/voucher` + `cmd/voucher` endpoints; delegation/participant + wallet-resolution + durable `pending_mints` logic verified against compose Postgres). **No on-chain mint / custody-transfer yet — those are Week 6** (real Sui, end-of-day `cmd/minter` driver). Preceded by Week 1 gap review + business-flow redesign, **Hotfix H: CI Kafka KRaft `CLUSTER_ID` executed** (local verification PASS; push pending), **Week 3 executed (wristband binding engine + reliable consumer)**, **Rev 3 family voucher & participant model LOCKED** (design Q&A).

---

## Hotfix H — CI Kafka KRaft `CLUSTER_ID` (2026-08-02) — EXECUTED (local verification PASS; push pending)

**Root cause (pinpointed from GH Actions log):** `CLUSTER_ID: MkU3OEVBNTcwNTJENDM5QkU` in `ci-lint.yml` (L44) and `ci-build.yml` (L20) is an **invalid KRaft cluster ID**: 23 base64 chars → decodes to **17 bytes**; KRaft requires a base64-encoded **16-byte UUID** (22 unpadded chars). It is the canonical Confluent docs example `MkU3OEVBNTcwNTJENDM5Qk` with an extra trailing `U`. `kafka-storage format` refuses during container `configure` → container exits → healthcheck goes `unhealthy` → *"Service container kafka failed"* before any test step runs. **Not** a KRaft architecture problem — a one-character typo copy-pasted into both workflows.

**Decision: keep KRaft single-node** (locked Q4). Zookeeper-mode is deprecated upstream (removed in Kafka 4.0), the ZK pair previously failed in this repo's GH Actions (progress.md blocker #3), and it doubles CI container boot time — reverting would fix nothing, since the defect is the ID string, not KRaft.

### Steps
- [x] **H1.** Generate a valid cluster ID: `docker run --rm confluentinc/cp-kafka:7.6.0 kafka-storage random-uuid` (outputs a valid 22-char base64url UUID). Fallback: documented example `MkU3OEVBNTcwNTJENDM5Qk`. → **generated `yISJZ6USQ8Kx-18tILTh0w`**
- [x] **H2.** `.github/workflows/ci-lint.yml`: replace `CLUSTER_ID: MkU3OEVBNTcwNTJENDM5QkU` with the H1 ID; correct the comment (ID is required by the image entrypoint in KRaft mode; value must be a base64-encoded UUID).
- [x] **H3.** `.github/workflows/ci-build.yml`: same replacement (identical ID value in both files for consistency).
- [x] **H4.** Hardening: add `--health-start-period 20s` to the kafka service `options` block in both workflows (broker needs ~10–20s to format storage + start; avoids burning health retries).
- [x] **H5.** Local replication of the GH service container with the exact env/port/health flags from the workflow → poll `docker inspect --format '{{.State.Health.Status}}'` until `healthy`; then `docker rm -f` + network cleanup. (Stop local compose kafka first — host port 29092 must be free, R5 default.) → **PASS: `healthy` on first poll, `Kafka Server started`, generated cluster id accepted.**
- [x] **H6.** Local M2.1 gate against that container: `INTEGRATION=1 KAFKA_BROKERS=localhost:29092 KAFKA_TOPIC_RIDE_SCANS=ride-scans go test -tags=integration ./internal/kafka -run TestIntegrationDelivery -count=1` → **PASS (`ok … 17.2s`, 1000/1000, 0 dups)** — after surfacing the second latent failure (missing topic; see H9).
- [x] **H9.** *(discovered by H6)* Add `Create Kafka topics` step to both workflows (`docker exec` into the GH service container, `kafka-topics --create --if-not-exists … ride-scans --partitions 6`) — CI equivalent of local `kafka-init`. Verified locally against the replica.
- [ ] **H7.** Commit (**done: `e8c684d`**); **user pushes** (`git push origin main` — sandbox has no GH creds); confirm both workflows green.
- [x] **H8.** Update memory bank (`progress.md` CI blocker entry, `activeContext.md` resolved defects).

### Alternatives considered
- **B — Omit `CLUSTER_ID` entirely** and rely on the cp-kafka 7.6.0 entrypoint to auto-generate one. Fewer hardcoded values, but depends on image-internal behavior — only adopt if H5 verifies the auto-generation path explicitly.
- **C — Zookeeper + Kafka pair** — **rejected**: deprecated upstream, previously failed in this repo's CI, slower, violates locked Q4.

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

**Goal:** Synchronous gate verification via the **Rev 1 binding model (R9)**: bind wristband via account QR + NFC transaction check; consume `ride-scans`, deduplicate via Redis. **Gate becomes the production Kafka producer (Q3/R10).** **Status: EXECUTED 2026-08-02 — W3.1–W3.12 DONE, all M3.1–M3.7 integration tests PASS live, E2E curl flow verified (QR→BINDING→BOUND→ride-scan→Kafka→consumer→PG). CI E2E gate (W3.13) + commit pending user push.**

### Rev 2 Design Decisions — Week 3 Q&A (2026-08-02, user-confirmed)

| ID | Decision | Rationale |
|----|----------|-----------|
| R18 | QR payload carries `ticket_id` **inside** the HMAC signature (`ticketID\|uuid\|timestamp`). Visitor must be logged into the park app to display it (frontend out of scope); staff terminal requests QR per ticket via `GET /api/wristband/scan-visitor-qr-token?ticket_id=`. | QR = one-time handshake linking account ↔ zkLogin wallet ↔ wristband; after bind, the wristband NFC **is** the visitor's identity. |
| R19 | **Binding store = Redis (ephemeral), not Postgres.** `bind:wristband:{uid}` → `{ticket_id, user_email, status, bound_at}` + reverse `bind:ticket:{ticket_id}` → uid; `EXPIREAT` end of Day+1 (disposable wristbands; zero long-term bloat). Atomic double-SETNX (Lua script) replaces SELECT FOR UPDATE for bind concurrency. **No migration 000002.** | User directive: temporary link only; Redis TTL = automatic expiry. |
| R20 | **Consumer = synchronous manual-commit** (drop `CommitInterval`) with worker pool retained: commit only after handler success; poison (unparseable) → DLQ immediately; transient → retry ×3 backoff (0.5s→1s→2s) → DLQ. Zero silent loss (D5). Edge-Buffered Async rejected as overkill for portfolio. | User directive: prevent any data loss; DLQ preserves everything for inspection/replay. |
| R21 | **QR single-use (Q8):** consumed at bind via Redis `SETNX qr:{uuid}` (TTL = rotation + skew); replayed QR → rejected even inside the 30s window. | User directive. |
| R22 | Ride scans never change ticket/binding status (Q4) — pure proof-of-visit minting pipeline. `USED/EXPIRED` out of Week 3 scope. | Multi-ride day requires BOUND to persist. |
| R23 | `/api/rides/scan` = `{wristband_uid, ride_id}` → resolve BOUND binding → email → publish `ScanEvent` + **optional `ticket_id`** field in Kafka payload (backward compatible; fixes D6 + D8). | Accepted recommendation. |
| R24 | Grace window = **faithful replay** of the cached decision (fixes D1), keyed by wristband, applies to `nfc-check` only. State machine itself idempotent (re-check of BOUND returns bound:true). | D1 = security bug (cached "denied" replayed as "allowed"). |
| R25 | Dedup compensation: on transient downstream failure after SETNX, handler DELs `dedup:{trace_id}` before returning error (retry reprocesses); PG `UNIQUE(trace_id)` remains durable backstop. | Zero-loss + effectively-once under retries. |

### Execution Blueprint (W3.1–W3.14)
- [x] **W3.1** `internal/models/models.go`: `ScanEvent.TicketID` (`json:"ticket_id,omitempty"`); `WristbandBinding{WristbandUID, TicketID, UserEmail, Status, BoundAt}` + `BindingStatusBinding`/`BindingStatusBound`.
- [x] **W3.2** `internal/redis/client.go`: `BindWristband` (Lua double-SETNX + EXPIREAT end-of-Day+1), `GetBinding`, `GetBindingByTicket`, `SetBindingStatus` (KEEPTTL), `DeleteBinding`, `MarkQRUsed`, `ClearDedup`.
- [x] **W3.3** `internal/gate/qr.go`: `QRToken` += `TicketID`; payload `ticketID|uuid|timestamp`; `GenerateQROTP(cfg, ticketID)`; verify = signature + window; update `qr_test.go`.
- [x] **W3.4** `internal/gate/binding.go` (NEW — deletes `verify.go` turnstile): `BindingService.Bind/NFCCheck/Reset` (R18–R25; `auth.TxnCheckPerformer` mock via R16; faithful grace replay keyed by wristband).
- [x] **W3.5** `internal/gate/ride_scan.go` (NEW): `RideScanService.Scan` → BOUND check → `ScanEvent{email, ride_id, ticket_id, fresh trace_id}` → producer publish (D8) → return trace_id.
- [x] **W3.6** `internal/kafka/consumer.go`: manual `CommitMessages` (no CommitInterval), DLQ writer (`cfg.Kafka.TopicDLQ`), retry ×3 backoff, poison→DLQ with error headers, graceful drain (D5).
- [x] **W3.7** `internal/pipeline/handler.go`: persist `event.TicketID` (D6); compensation `ClearDedup` on transient failure (R25).
- [x] **W3.8** `internal/config/config.go` + `.env.example`: `KAFKA_CONSUMER_MAX_RETRIES=3`, `KAFKA_CONSUMER_BACKOFF_MS=500`, `GATE_TXN_CHECK_FAILWHEN=""` (mock knob).
- [x] **W3.9** `cmd/gate/main.go`: remove `/api/gate/verify` + `/api/gate/confirm`; wire `qr-token?ticket_id=`, `bind`, `nfc-check`, `reset`, `/api/rides/scan`.
- [x] **W3.10** `cmd/consumer/main.go`: pass retry/DLQ config into consumer.
- [x] **W3.11** Tests: `internal/gate/binding_integration_test.go` (M3.4 grace replay, M3.5 concurrent bind one-winner, M3.7 bind→check→reset→re-bind, QR single-use replay); `internal/kafka/consumer_integration_test.go` (M3.1 dup×5→1, M3.2 TTL reprocess, M3.3 poison→DLQ+continue, M3.6 2k-event pool concurrency); pipeline integration += ticket_id persistence.
- [x] **W3.12** Live local verification: `make up && make healthy`; curl flow (qr→bind→nfc-check→rides/scan→consumed); `make build`, `go vet`, `golangci-lint run`, unit + integration suites green.
- [ ] **W3.13** CI + docs: extend `ci-build.yml` (compose E2E: loadgen 1k w/ 20% dups → assert dedup counts, M3 CI gate); README API section (remove turnstile routes, document wristband API); commit; user push.
- [ ] **W3.14** Memory bank + plan checkoff.

### Technical Requirements
- [x] `cmd/gate` endpoints (business-aligned naming, R17):
  - `GET /api/wristband/scan-visitor-qr-token` — one-time HMAC QR token (30s rotation) for the visitor to present (renamed from `qr-token`).
  - `POST /api/wristband/bind` — bind wristband (NFC id) to account via scanned QR payload → ticket `PENDING_ENTRY` (BINDING). (NEW)
  - `POST /api/wristband/nfc-check` — run **transaction check** (via `internal/auth.TxnCheckPerformer`, R16): real impl = zkLogin wallet readiness (W6); success → ticket `ACTIVE` (BOUND). (renamed from `verify`)
  - `POST /api/wristband/reset` — admin undo/overwrite on faulty NFC: `ACTIVE|PENDING_ENTRY → CLAIMED` (unbind / re-bind). (R13) (NEW)
  - `POST /api/rides/scan` — ride staff NFC scan during visit → publishes `ScanEvent` to `ride-scans` (production path; simulated web flow target). (NEW)
- [x] ~~**Atomic transaction (sqlx.Tx):** SELECT FOR UPDATE~~ **Superseded by R19:** binding state machine lives in Redis (atomic double-SETNX via Lua); PG consulted only for ticket ownership/status at bind.
- [x] **5-second Retry Grace Window:** same ticket_id within 5s → cached result (Redis TTL=5s) — fixes the "always allowed" bug (see Known Defects).
- [x] **HMAC QR signing:** `internal/gate/qr.go` — payload `[ticketID | UUID | Timestamp | HMAC_SHA256]` (R18), 30s rotation, key from env (`HMAC_SECRET`), one-time use enforced at bind (R21).
- [x] **Gate = Kafka producer:** after successful `nfc-check` (BOUND), ride scan (`POST /api/rides/scan`) publishes `ScanEvent` to `ride-scans` with `user_id`=email (internal), `ride_id` from request (R12), W3C traceparent headers.
- [x] `cmd/consumer` — **reliability fixes (R14, deferred from W1):** manual commit (`CommitMessages` after handler success), dead-letter topic `ride-scans-dlq` for poison messages, exponential backoff retries on transient PG/Redis failures (max before DLQ), idempotency via `SET dedup:{trace_id} 1 NX EX <ttl>`, worker pool with backpressure.
- [x] Retry strategy: exponential backoff for transient Redis failures; max retries before DLQ.

### Testing Milestones
- [x] **M3.1** *Duplicate:* same `trace_id` 5× → exactly 1 processed, 4 dropped, Redis key with TTL.
- [x] **M3.2** *TTL:* after expiry, same `trace_id` processed again.
- [x] **M3.3** *Poison:* malformed JSON → DLQ, consumer continues.
- [x] **M3.4** *Grace window:* double-scan within 5s → same (cached) result — **regression: cached result must match original decision** (fixes bug).
- [x] **M3.5** *Concurrent binds same ticket* → exactly one BOUND, other rejected (FOR UPDATE lock).
- [x] **M3.6** *Concurrency:* 10k events, 5 partitions, pool=10 → no cross-worker duplicates.
- [x] **M3.7** *(NEW R9/R13) Binding flow:* QR bind → txn check pass → ACTIVE; mock check fail → reset → re-bind succeeds.

### CI/CD Milestones
- [ ] **CI gate:** integration job spins compose + loadgen(1k events w/ 20% dups) → assert dedup count.

### Deliverables
- [ ] Gate API binary (binding model), Consumer with reliable commit+DLQ, Redis idempotency, dedup unit + integration tests.

---

## Rev 3 Design Decisions — Family Voucher & Participant Model (2026-08-02, user-confirmed via design Q&A)

**Business flow:** a family buys park tickets (e.g. dad buys 4: himself, wife, 2 kids). Dad delegates each ticket to a family member. **Vouchers are the delegation unit.** The design was developed to solve: infants/toddlers/elderly who have no (usable) Google account / email, not blocking the turnstile line, and right-to-be-forgotten. These override/refine Q6, R11, R13, R16 where noted.

| ID | Decision | Notes |
|----|----------|-------|
| R26 | **Participant ≠ account/wallet.** Introduce a `participant` (a person) distinct from a login/wallet. The gate's wristband bind targets a participant. | Core refactor enabling the family model. |
| R27 | **Voucher = delegation unit.** `purchase` creates N participant-bound vouchers; the buyer allocates each to a participant (email → own account, or "add family member / dependent"). Ownership = participant, with a guardian who may act for them. | Refines Q6/R13 voucher framing. |
| R28 | **Two delegation modes.** (a) **Account-linked:** delegate to an email with Google zkLogin → own non-custodial wallet. (b) **Dependent:** person with no account (child/infant/elderly) → a **custodial-proxy wallet** held by a guardian. | Solves toddlers & non-technical elderly. |
| R29 | **Gate decoupled from wallet existence.** `/api/wristband/nfc-check` confirms rightful entrant + entitlement + presence; wallet readiness is a **soft (warn, never block)** signal, not a gate. Nothing is resolved in line. | Re-scopes R16 txn check; kills the line-jam. |
| R30 | **Wallet attached later (eventually-linked).** Mint-time wallet resolution per participant: (1) own non-custodial, (2) dependent custodial-proxy, (3) adult JIT (attach wallet → mint). | End-of-day async mint; no blockchain wallet needed at the gate. |
| R31 | **Dependents mint immediately into the guardian wallet.** Because a guardian always has a wallet, dependents' NFTs are minted at end-of-day into the custodial wallet — NOT held in a forever-pending state. The "3-month Claim window" is an *opportunity*, not a deadline. | Avoids indefinite pending. |
| R32 | **`pending_mints` = durable attribution ledger, not a cache.** Lives in Postgres, keyed to `participant`, durably retains (participant, ride_ids, scanned_at, date). Never tied to ephemeral Redis TTL (48h) or the disposable wristband (end of Day+1). Rebuildable at any time from `scan_events` (the true durable source). Kept by default; deletable on request (GDPR). | Answers "where does the ride data sit". |
| R33 | **Transfer later = on-chain NFT object transfer.** When a dependent later links their own Google account, a `claim custody` action transfers the NFT from the custodial wallet to their own non-custodial wallet (Sui object transfer — Move allows transfer, Q7). Off-chain attribution updated. Works over a multi-year timeline. | Dad→kid whenever they're ready. |
| R34 | **Right-to-be-forgotten (R11 extended).** On-chain + IPFS carry **no PII** (Sui address, ride_id, date, ride display name only). The person↔token link lives **only** in Postgres and is deletable. PII rules are an *enabler*: all linking is confined off-chain, never on-chain. | Confirmed by user. |
| R35 | **Guardian (family) custodial wallet is separate** from the guardian's personal zkLogin wallet — a dedicated family/guardian Sui address, keys server-side/encrypted. Keeps dependents' tokens segregable and cleanly transferable to each owner. | Recommended; prevents commingling. |

**Data durability split (as settled):**
- Durable (Postgres): `scan_events` (per event), `rides`, `participants` (name, guardian_id, wallet_state), `pending_mints` (attribution ledger), `mint_logs`, `tickets`/`ticket_vouchers`.
- Ephemeral (disposable by design): Redis `bind:wristband:{uid}` (end of Day+1), Redis `user:*:rides:{date}` (48h cache, rebuildable), dedup keys, QR one-time keys.
- **Nothing value-bearing is lost when the wristband/Redis is thrown away** — they are caches/pointers, never the source of truth.

---

## Week 4 — Postgres Persistence, Redis Aggregation, Family Voucher & Participant System

**Goal:** Persist valid scans to Postgres; aggregate per-user ride sets in Redis (cache); implement the **Rev 3 family voucher / participant model** (R26–R35). **Status: WEEK 4 OFF-CHAIN COMPLETE 2026-08-03** — M4.1–M4.8 + M4.10 + M4.12 all verified live (outbox M4.3, ticket-state M4.7, voucher claim/share M4.5/M4.6, delegation M4.8, E2E M4.4). `make build/test/lint` + full integration suite green. **NOT DONE / WEEK 6:** on-chain minting & custody transfer (M4.9→M6.6, M4.11/R33→M6.7) and the **CI coverage gate ≥70% — currently 38%** (`make coverage` fails threshold). **Remaining this week:** none off-chain; coverage gap needs Week 6/7 package tests (sui/storage/telemetry).

### Technical Requirements
- [x] `internal/postgres`: repository pattern, `ScanRepository.Insert` (idempotent on `trace_id` UNIQUE constraint), `pgx` pool. *(insert + repo existed from W3 pipeline; extended with Rev 3 participant/pending-mint methods 2026-08-03.)*
- [x] Migrations v2 (`0002_*` via golang-migrate, R1): verify `UNIQUE(trace_id)`; `CHECK` constraints on `ride_id`; ticket status enum aligned with R13; **new `participants`** (id, guardian_id, name, account_email nullable, wallet_state enum `NONE|OWN_NON_CUSTODIAL|CUSTODIAL_PROXY`, timestamps) and **`pending_mints`** (id, participant_id, ride_ids jsonb, scanned_ats, mint_date, wallet_state, created_at — durable attribution ledger, R32). *Applied + verified (version=2, dirty=false). NOTE: `scanned_ats` stored as **JSONB** of RFC3339 (not `timestamptz[]`) — avoids pgx array text-parse fragility, equally durable/rebuildable.*
- [x] Redis Set aggregation: `SADD user:{user_id}:rides {ride_id}` per valid scan; daily key `user:{user_id}:rides:{date}` TTL 48h (cache only — rebuildable from `scan_events`). *(pre-existing in `internal/pipeline`.)*
- [x] Transactional boundary: insert Postgres → SADD Redis; on Redis failure, compensate or mark for retry (outbox pattern). *(Implemented 2026-08-03 (M4.3): migration 0003 `scan_events_outbox`; the handler parks the failed SADD + commits; `cmd/consumer` drain worker replays until Redis accepts — no loss. Dedup-compensation (R25) retained for non-outbox wins.)*
- [ ] Batch insert optimization (batch size configurable).
- [x] **Family Voucher / Participant System (`internal/voucher`, `cmd/voucher`):** *(built 2026-08-03)*
  - [x] `POST /api/vouchers/purchase` → N voucher rows (`UNCLAIMED`, `purchaser_id`). *(pre-existing)*
  - [x] `POST /api/vouchers/delegate` → allocate a voucher to a participant: mode `account` (account_email) or `dependent` (name + guardian_id → custodial-proxy wallet) (R27/R28).
  - [x] `POST /api/vouchers/share` → magic link (signed JWT with `voucher_id`). *(pre-existing)*
  - [x] `GET /api/vouchers/claim?token=...` → JIT registration → voucher/ticket `UNCLAIMED → CLAIMED` (R13). *(pre-existing)*
  - [x] Participant has a `guardian_id` and `wallet_state`; dependents get a custodial wallet reference (R35).
- [ ] **Minter mint resolution (R30/R31):** per participant, resolve wallet (own → custodial → pending). Dependents mint immediately into guardian custodial wallet; unresolved adults write a durable `pending_mints` row at end-of-day (R32). Idempotent via `mint_logs` UNIQUE(user_id, ride_id, mint_date) — still works keyed by participant/guardian. *(OFF-CHAIN: `ResolveMintWallet` + `RecordPendingMint` built & verified; the **run-at-end-of-day `cmd/minter` driver** is the remaining Week 4 wiring — it resolves wallets and writes `pending_mints`, but **actual tx submission is Week 6**.)*
- [ ] **Custody transfer (R33):** `POST /mint/claim-custody` — transfer NFT from custodial wallet to a dependent's newly-linked non-custodial wallet (Sui object transfer) + update attribution. *(OFF-CHAIN stub done 2026-08-03: `ClaimCustody` flips `wallet_state` in PG + updates attribution. **Real on-chain Sui object transfer MOVED to Week 6 → M6.7** — nothing on-chain yet, no tx.)*

### Testing Milestones
- [x] **M4.1** *Persistence:* 5k valid events → PG row count == 5k, no dups, indexes performant. *(verified 2026-08-03 — `TestW4Persistence5k`: 5000 rows, re-submitted dups → still 5000; 6.4s.)*
- [x] **M4.2** *Aggregation:* user 3 distinct rides → Redis set cardinality == 3; dup ride → unchanged. *(`TestW4Aggregation`.)*
- [x] **M4.3** *Failure:* Redis down mid-processing → PG insert succeeds, aggregation retried via outbox/backoff, no loss. *(Implemented outbox (migration 0003 `scan_events_outbox`), handler parks on aggregate failure + commits, drain worker in `cmd/consumer`. `TestW4OutboxNoLoss` verifies PG row + outbox row → drain → Redis set + outbox cleared.)*
- [x] **M4.4** *E2E:* loadgen(10k, 15% dups) → PG == 8.5k unique, Redis sets correct. *(`TestW4E2ELoadgenDups`: 10k events / 1.5k dups → 8500 PG rows, 10 distinct rides in Redis; 10.8s.)*
- [x] **M4.5** *Voucher:* purchase 8 → 8 `unclaimed` → claim 1 → `claimed`. *(`TestIntegrationVoucherPurchaseAndClaim`.)*
- [x] **M4.6** *Magic link expired/invalid* → rejected. *(Extracted `internal/voucher` `SignMagicLink`/`VerifyMagicLink` (used by cmd/voucher); `link_test.go` — expired/invalid/garbage/empty all rejected.)*
- [x] **M4.7** *(R13) Ticket state transitions:* claim → QR bind → txn check → ACTIVE → ride scan → USED. *(added `RideScanService.markTicketUsed`; `TestM47TicketStateTransitions` verifies CLAIMED→PENDING_ENTRY→ACTIVE→USED.)*
- [x] **M4.8** *(NEW Rev 3) Delegation (account-linked):* dad buys 4 → delegates 1 to wife's email → wife claims → own non-custodial wallet → mint lands in her wallet. *(OFF-CHAIN portion verified 2026-08-03 — `TestIntegrationAccountDelegationResolvesToOwnWallet`: delegate→claim→own wallet resolution resolves to her wallet. **The actual on-chain mint into her wallet is Week 6 (same mint machinery as M6.6) — no on-chain activity in Week 4.**)*
- [ ] **M4.9** *(NEW Rev 3, on-chain half MOVED to W6 → M6.6) Dependent (no account):* dad delegates to a child `dependent` → custodial-proxy wallet → wristband binds to child participant → gate passes with NO wallet → mint attributed to child in guardian wallet. *(OFF-CHAIN layer done 2026-08-03:* dependent delegation + custodial wallet model + `ResolveMintWallet` → non-pending verified against Postgres. **The real mint into the guardian wallet is M6.6 (Week 6).**)*
- [x] **M4.10** *(NEW Rev 3) Durable pending/attribution:* unresolved adult → `pending_mints` row persisted at end-of-day; re-aggregation from `scan_events` after Redis TTL yields the same ride set; row lives independently of Redis/wristband lifetime. *(durable-row persistence + independence + rebuild-from-`scan_events` verified live 2026-08-03 — `ScanEventRideSource` covers both account-linked (users.email) and **dependent (ticket_vouchers.participant_id)** attribution via M4.12; pure data-layer, no blockchain.)*
- [ ] **M4.11** *(NEW Rev 3, MOVED to W6 → M6.7) Custody transfer:* dependent links Google later → NFT object transferred from custodial wallet to their own wallet; attribution updated. *(OFF-CHAIN `ClaimCustody` state-flip done 2026-08-03. **Real NFT object transfer = M6.7 (Week 6), no tx yet.**)*
- [x] **M4.12** *(NEW Rev 3, off-chain) End-of-day mint-resolution driver:* `internal/minter` `DayResolver` iterates participants-with-rides, resolves each wallet (own → custodial → pending), writes durable `pending_mints` for unresolved adults (M4.10 end-to-end attribution writer). Exposed as `POST /mint/resolve-day`. *(DONE + verified live 2026-08-03 — unit tests + `resolver_integration_test.go`; reads rides from durable `scan_events` via `ScanEventRideSource`, so it doubles as the M4.10 rebuild path for account-linked participants; no on-chain submission.)*

### CI/CD Milestones
- [ ] **CI gate:** full pipeline integration test in CI (gate/pipeline/kafka/voucher/minter wired in `ci-lint.yml` 2026-08-03); **coverage gate ≥ 70% on `internal/`** added as `make coverage` + CI wiring. **NOT MET: aggregate = 38% < 70%** — dragged down by 0% on `internal/sui` (Week 6), `internal/storage` + `internal/telemetry` (Week 6/7), and thin `internal/redis`. Needs Week 6/7 package tests to clear.

### Deliverables
- [ ] ***Week 4 essentially complete 2026-08-03 — remaining: CI coverage gate (38% < 70%) + Week 6 on-chain.*** Postgres repository ✔, Redis aggregation ✔, **outbox/retry (M4.3) ✔**, family voucher/participant system (R26–R32) ✔, durable pending/attribution ledger ✔, custody-transfer path (off-chain stub ✔; real on-chain = Week 6) ✔, full ingestion pipeline tested ✔ (unit + integration: M4.1–M4.8).

---

## Week 5 — Sui Move Smart Contract (Attendance NFT)

**Goal:** Deploy Move module that mints attendance NFTs. **Status: DONE (deployed to testnet, 11/11 tests passing after 2026-08-03 reconciliation).** Contract audited against Rev 3 (R26–R35) — **design-sound, no changes required** (see below).

### Done (verified 2026-08-01)
- [x] `move/attendance_nft/` package; `sui move build` clean.
- [x] Module `attendance_nft::attendance`: `MintCap`, `mint_attendance_nft`, `mint_batch`, `burn`, `update_metadata`, view fns, zero-address checks, events, transferable NFT (Q7).
- [x] **Deployed:** Package `0x78c9dbba118923c4976599877450cd281f880f94d217d806714782a658b01d1e`, MintCap `0xfe4dcb...f30cb` (testnet). See README.
- [x] 11 Move unit tests **— reconciled 2026-08-03: 5 were failing due to test_scenario drift** (missing `next_tx()` boundary before `take_from_address` + LIFO take order in the batch test). Fixed; now 11/11 pass. Contract module itself unchanged/fit-for-purpose.
- [x] *(R11)* **NO-PII audit note added** in `attendance.move` header — confirms on-chain stores only address/ride_id/date/name/metadata_url, no PII (R34, right-to-be-forgotten).

### CI/CD Milestones
- [x] **CI gate:** `ci-move.yml` added 2026-08-03 — installs Sui CLI v1.76.0 (matches deployed toolchain) + runs `sui move build` + `sui move test` on PRs/pushes touching `move/`.

### Deliverables
- [x] Audited Move module, testnet deployment, package ID documented, **`ci-move.yml` added 2026-08-03** (Sui CLI v1.76.0 + `sui move build` + `sui move test` on `move/` changes).

---

## Week 6 — Sui Go SDK, zkLogin Custodial & Batch Mint Pipeline

**Goal:** Go service reads Redis aggregation and mints NFTs via Sui SDK + zkLogin (custodial, server-side signed, gas sponsored).

### Execution status (2026-08-03, hybrid: deterministic-primary, real-zkLogin optional)
**BUILT + OFF-CHAIN VERIFIED** (build/test/lint green, unit-verified): deterministic email→wallet derivation, mnemonic gas-pool signer (D3/D4 fixed & validated against deployed wallet), RPC concurrency semaphore, `mint_logs.participant_id` attribution (migration 0004, applied v4), batch-mint driver incl. dependent→guardian custodial (M6.6 logic) + idempotency (M6.3) + 429 backoff (M6.4), custody-transfer `TransferNFT` (M6.7), real Google JWT parse (D2) + `/api/auth/google`, `/mint/resolve-day` + `/mint/run-day` + `/mint/claim-custody`. **NOTE:** discovered public `fullnode.testnet.sui.io` JSON-RPC is **deprecated** (Sui migrate to gRPC/GraphQL) → switched `SUI_RPC_URL` default to BlockVision JSON-RPC (`sui-testnet-endpoint.blockvision.org`, verified chain-id `4c78adac`). Gas pool balance confirmed **~3.98 SUI** at `0x3cd9...`.
**ON-CHAIN EXECUTED 2026-08-13:** **M6.1 + M6.2 + M6.5 complete** — `/mint/run-day` submitted **10 real testnet batch-mint transactions, 10 unique digests, all `success`, 41 NFTs created** (digest + per-tx object tables below). Enabled by two fixes: (1) **Pinata JWT auth** (`PINATA_JWT`; legacy key/secret headers now 401), and (2) **block-vision SDK `unsafe_moveCall` encoding** — pure args must be hex-encoded (`u64`/`vector<vector<u8>>` as `0x` hex strings) and `TypeArguments` must be a non-nil empty slice (`null` → `-32602 invalid type: null, expected a sequence`).

### Technical Requirements
- [x] `internal/sui`: wrapper around `github.com/block-vision/sui-go-sdk v1.2.1`; client init from `SUI_RPC_URL` env. **Rewritten:** mnemonic gas-pool signer (`SignerFromMnemonic`, D4), real blake2b address (`PubKeyToSuiAddress`, D3, validated vs deployed wallet), RPC semaphore. `Minter` interface + `*Client` impl.
- [x] **Deterministic custodial wallet (W6-B primary):** `DeterministicWallet(email, secret)` = HMAC-SHA256(email) → ed25519 → real Sui address. Mock/loadgen emails reliably mint real NFTs (same email ⇒ same wallet). 
- [ ] **Full zkLogin custodial support (optional real path, off by default):**
  - [x] `internal/auth.GoogleTokenVerifier` — real issuer/expiry/audience check (fixes hardcoded `user@example.com`, D2). NOTE: does NOT yet cryptographically verify Google's signature / generate zk proof in default mode.
  - [ ] Real zkLogin proof generation + signature verification (needs GOOGLE_OAUTH_CLIENT_ID + prover service) — deferred/optional per hybrid decision.
- [x] **Gas sponsorship:** gas pool wallet from `SUI_GAS_POOL_MNEMONIC` (D4 fixed — no more keystore-file load). Validated: derived address == deployed active address.
- [x] `cmd/minter` `POST /mint/run-day`: DayResolver mint-ready set → `mint_batch` → `mint_logs` **idempotency (ON CONFLICT)**. *(Rewritten from `/mint/daily` Redis path. Executed live 2026-08-13 — 10 real testnet txs.)*
- [x] **RPC throttling:** client-side concurrency semaphore (`SUI_RPC_MAX_CONCURRENCY`) + 429 exponential backoff.
- [x] **R16 real impl (deterministic path):** `GoogleTokenVerifier` + deterministic wallet readiness; real zkLogin readiness deferred to optional path.
- [x] **JIT registration integration hook:** voucher claim + `/api/auth/google` writes derived wallet (see `/mint/run-day` + auth).
- [x] **Custody transfer real impl (R33, M6.7):** `Client.TransferNFT` (Sui `TransferObject`) + `/mint/claim-custody` endpoint (on-chain transfer + off-chain claim). NOT yet run live.

### Testing Milestones
- [x] **M6.1** SDK smoke: Go client reads/uses package on testnet. *(RPC verified; real mint below proves MoveCall + SignAndExecute against testnet.)*
- [x] **M6.2** Mint E2E: `/mint/run-day` → **10 real testnet batch-mint txs, 10 unique digests recorded, all `success`, 41 NFTs created on-chain (2026-08-13).** *(Ran against WSL env live; digests below.)*
- [x] **M6.3** Idempotency (unit): re-run → no duplicate mints (`ON CONFLICT`); fake-driven in `batch_mint_test.go`.
- [x] **M6.4** RPC 429 (unit): backoff logic + semaphore present in client; fake-driven in tests.
- [x] **M6.5** Deterministic address-path real: `DeterministicWallet(email, secret)` → real Sui recipient → **on-chain NFTs landed at the derived addresses** (verified via created-object owners). Real zkLogin proof gen remains optional/deferred.
- [x] **M6.6** Dependent mint **logic** (unit): dependent → guardian custodial wallet recipient (`batch_mint_test.go`). Real on-chain dependent→custodial tx path shares this machinery.
- [ ] **M6.6-LIVE (DEFERRED/PENDING):** Real on-chain **dependent → guardian custodial** mint. Seed a guardian (OWN_NON_CUSTODIAL) + a dependent participant (`CUSTODIAL_PROXY`), give the dependent rides, run `/mint/run-day`, and verify on-chain that the dependent's AttendanceNFTs **land in the guardian (family) custodial wallet** (created-object owner == guardian custodial address) + `mint_logs.participant_id` attributes to the dependent. Differs from M6.2 (own-wallet) only by the resolved recipient. *(The 10 M6.2 txs were all account/own-wallet path.)*
- [x] **M6.7** Custody transfer **code path** (unit): `TransferNFT` → destination wallet (`TestTransferNFTSurface`). Real object transfer digest pending (no object transfer run this milestone).

**On-chain M6.2 digest proof (2026-08-13, all verified `sui_getTransactionBlock` → status `success`):**

| # | pointer | tx_digest | NFTs created |
|---|---|---|---|
| 1 | 34 | `Hzh37UFVRbJmyEzyt57NoyZV4w6tvQmTHeMF9GihSg59` | 6 |
| 2 | 35 | `5xaKzos97kirMVU9fBLodqKvtdSGkDt5uszPP5DU8pNw` | 8 |
| 3 | 36 | `5bh6gkid9fhVNrKaaxgaRi2ZHhmH1wUUgCmqRqNR1WWb` | 13 |
| 4 | 37 | `FxMXh1Lp7of57TGb8JsHoJ98FnL8yd6Ndh8xvv5i1cm7` | 9 |
| 5 | 38 | `3YU2WZrBjcFTXk9DuLTNqLCU4orh2HGNZ3UVJGjacNFM` | 12 |
| 6 | 39 | `HWVNrRmQEg5tFopYKMwBCrZkqoPczFgXmazfvVx6MX8B` | 13 |
| 7 | 40 | `AQvmT1A5KXizTFrHLvLacdYQZC8hJvBy97NEfNBSFq6K` | 8 |
| 8 | 41 | `2KTWNkNYowQ8uNtHp5qBMCahg2TA5BFqeMFwygbbCzju` | 14 |
| 9 | 42 | `BNyt2p7UgP6cwmutPaXz9pjbmwynpAo6RhPH6kHTnysF` | 10 |
| 10 | 43 | `Cgkh4fgdm7Vsb6xQvLNfgzBwjZDnkDrV6Gwrucwh2ZCD` | 7 |

### CI/CD Milestones
- [ ] **CI gate:** `ci-build.yml` builds `minter` image (already exists); mock Sui RPC for unit tests (deterministic path is mock-able via `sui.Minter` interface).

### Deliverables
- [x] Minter service, real zkLogin custodial integration, gas sponsorship, E2E batch mint validated on testnet (bounded, no spam). *(Executed live 2026-08-13 — 10 real testnet mint txs, 10 digests verified `success`.)* Real zkLogin proof-generation remains optional/deferred (deterministic wallet path is the default).
- [x] Coverage: `internal/sui` 0→20%, `internal/auth` 91%, `internal/minter` 47% (new tests).

---

## Week 6.5 — Frontend Test-Runner + Demo Orchestrator (2026-08-13, EXECUTED)

**Goal (user decision 2026-08-13):** defer non-core work (real OAuth/zkLogin proof-gen, Datadog, AWS) and build a **portfolio-facing frontend** so a non-Web3 visitor can watch the park → on-chain flow. Delivers four reproducible scenarios that each fire **exactly 10 real Sui-testnet transactions**, rendered with on-chain/off-chain/mock steps visually distinguished.

### Design decisions (user-confirmed)
1. **2a wallet probe** = sponsored SUI transfer gas-pool → derived wallet (proves the address is live; dependents probe to the guardian).
2. **2d** = 5 guardian-only + 5 guardian-with-1-dependent mints, **randomized order**. Dependents have no wallet; rewards delegate to the guardian. 2c reuses 2b's guardians.
3. **New `cmd/demo` orchestrator (`:8090`)** (not extending `minter`).
4. **No auto-reset** — runs are additive; only an explicit Reset wipes the stack.
5. **TypeScript** frontend.

### Backend
- [x] `internal/sui/reader.go` — `sui.Reader` interface (`TransferSuiProbe`, `TransactionStatus`, `OwnedNFTs`, `BalanceMist`), separate from `sui.Minter`. Uses block-vision SDK `unsafe_transferSui`/`sui_getTransactionBlock`/`suix_getOwnedObjects`/`suix_getBalance`.
- [x] `cmd/demo/main.go` + `internal/demo/{orchestrator,seed,scenarios,wallet,reset,handlers,types}.go`.
- [x] Endpoints: `GET /api/demo/health`, `POST /api/demo/seed`, `POST /api/demo/run`, `POST /api/demo/reset`, `GET /api/demo/wallet`.
- [x] Idempotent + non-destructive seeder; **dependent custodial wallet = guardian's real deterministic address** (corrects `voucher.Delegate`'s `0xguardian-custodial-{id}` placeholder).
- [x] `internal/redis.Client.FlushDB` (reset); `internal/demo/types_test.go` (deps/date/isolation/helpers).
- [x] `Makefile` (`build`+=demo, `make demo`, `make reset-demo`); `deployments/Dockerfile.demo`; `.env.example` += `DEMO_PORT`/`DEMO_PROBE_AMOUNT_MIST`.

### Frontend (`frontend/`)
- [x] React 18 + Vite + TS, exact-pinned deps, dark theme, React Router + TanStack Query.
- [x] Pages: Dashboard / ScenarioRunner / WalletViewer / Reset; components: HealthGate, FlowVisualizer, TransactionTable, WalletObjects, ScenarioCard.
- [x] `npm install` + `npm run build` (tsc + vite) green; Suiscan links via `src/config.ts`.

### Validation
- [x] `go build ./...`, `go test ./...`, `go vet ./...` green; `gofumpt -l` clean on new files; `npm run build` green. `go.mod` unchanged.
- [ ] **Live on-chain run of 2a/2d** (needs full stack + funded gas pool) — code paths built, not executed this session.
- [ ] CI coverage gate ≥70% (still 38%); CI push pending user GH creds.

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