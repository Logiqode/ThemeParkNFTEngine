# ThemeParkNFTEngine — Implementation Plan

> **Status:** Active — Living document. Check off `[x]` as each milestone completes.
> **Mode:** ACT MODE execution. Single developer, 8-week sprint.

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

No `^`/`~` range operators. All versions exact-pinned per Web3 dependency rules.

---

## Architecture Blueprint

```
theme-park-nft-engine/
├── cmd/
│   ├── gate/         # Gate Access API: POST /api/gate/verify (sync, Kafka producer)
│   ├── consumer/    # Kafka consumer: dedup + persist + aggregate
│   ├── loadgen/     # Test-only Kafka producer (bypasses gate)
│   ├── minter/      # End-of-day batch mint API + Sui zkLogin custodial
│   └── voucher/     # Ticket voucher + magic link + JIT claim API
├── internal/
│   ├── config/      # Env loading (viper + envconfig)
│   ├── kafka/       # Producer (gate) + Consumer wrappers
│   ├── redis/       # Idempotency + session aggregation
│   ├── postgres/    # Repositories + migrations
│   ├── gate/        # QR HMAC signing/verification + turnstile state machine
│   ├── voucher/     # Voucher lifecycle: unclaimed → claimed → ownership transfer
│   ├── sui/         # Sui SDK + zkLogin custodial (ephemeral key, proof, server-side sign)
│   ├── telemetry/   # OpenTelemetry init/exporters
│   └── models/      # ScanEvent, Ticket, Voucher, User, MintLog
├── migrations/      # SQL migrations
├── move/
│   └── attendance_nft/  # Sui Move package (NFT module)
├── deployments/
│   ├── docker-compose.yml
│   ├── Dockerfile.gate
│   ├── Dockerfile.consumer
│   ├── Dockerfile.loadgen
│   ├── Dockerfile.minter
│   └── Dockerfile.voucher
├── .github/workflows/
├── scripts/
├── .env.example
├── implementation_plan.md
└── README.md
```

**Data Flow:**
```
[Digital Wristband API] → HMAC QR token (30s rotation)
        ↓
[Staff Scanner — offline HMAC verify]  (out of scope, external)
        ↓ (online fallback)
[Gate API: POST /api/gate/verify]
  → sqlx.Tx: SELECT FOR UPDATE → ACTIVE? → PENDING_ENTRY → USED
  → 5s retry grace window
  → Publish verified scan to Kafka(ride-scans)  ← Gate = Kafka Producer
        ↓
[Consumer] → Redis dedup (trace_id) → Postgres log → Redis Set (user:rides)
        ↓
[Voucher System] → Purchaser buys N → PG vouchers → magic link → JIT zkLogin claim
        ↓
[Minter API: POST /mint/daily]
  → Read Redis Set → Batch mint via Sui zkLogin (server-side signed, gas sponsored)
  → Record tx_digest in PG mint_logs
  → OpenTelemetry traces end-to-end → Datadog
```

---

## Week 1 — Local Infrastructure & Scaffolding

**Goal:** Bootable local dev environment with all backing services containerized.

### Technical Requirements
- [ ] Initialize Go module (`go mod init github.com/Logiqode/ThemeParkNFT`), Go 1.26.5+.
- [ ] Enforce Function-First folder architecture per blueprint.
- [ ] `docker-compose.yml` with services: `zookeeper`, `kafka`, `redis:7`, `postgres:16`, `otel-collector`, `datadog-agent`.
- [ ] Config layer: `.env.example` + `internal/config` using `envconfig` + `viper`. **No hardcoded URLs/RPCs/secrets.**
- [ ] Postgres schema v1: `users`, `tickets`, `ticket_vouchers`, `rides`, `scan_events`, `mint_logs`.
- [ ] Health server (`/healthz`, `/readyz`) for each Go binary.
- [ ] `Makefile` targets: `up`, `down`, `migrate`, `build`, `test`.

### Testing & CI/CD Milestones
- [ ] **M1.1** `docker compose up` brings entire stack healthy in < 60s.
- [ ] **M1.2** Migrations run idempotently; `make test` passes (smoke unit tests for config + health).
- [ ] **CI gate:** GitHub Actions workflow `ci-lint.yml` — `golangci-lint run` + `go test ./...` on PRs.

### Deliverables
- [ ] Runnable local stack, schema v1, health endpoints, lint+test CI.

---

## Week 2 — Load Generator & Kafka Producer (Test Harness)

**Goal:** Produce thousands of valid scan events into `ride-scans` topic.

### Technical Requirements
- [ ] Define `models.ScanEvent`: `{user_id string, ride_id string, timestamp int64, trace_id string}` with JSON tags + validation.
- [ ] Kafka topic config: `ride-scans`, partitions=6, replication=3 (local: 1), retention configurable via env.
- [ ] `cmd/loadgen`: configurable rate (RPS), duration, concurrency, unique-user ratio, duplicate ratio. Emits OpenTelemetry trace_id per event (W3C Trace Context).
- [ ] Producer wrapper `internal/kafka/producer.go`: sync/async producer, graceful shutdown (flush on SIGTERM), idempotent producer enabled (`enable.idempotence=true`).
- [ ] Structured logging (zerolog) with trace_id propagation.

### Testing Milestones (Load Generator vs. Kafka)
- [ ] **M2.1** *Correctness test:* 1,000 events produced → `kafka-console-consumer` count == 1,000, JSON schema validated.
- [ ] **M2.2** *Throughput test:* 10,000 events @ 1,000 RPS → all delivered, p99 produce latency < 100ms, zero duplicates from producer.
- [ ] **M2.3** *Spike test:* burst 5,000 events in 1s → no producer errors, Kafka lag returns to 0 within 5s.
- [ ] **M2.4** *Graceful shutdown:* SIGTERM mid-batch → all in-flight messages flushed, no data loss.

### CI/CD Milestones
- [ ] **CI gate:** `ci-build.yml` builds `loadgen` Docker image, runs M2.1 as integration test in compose.

### Deliverables
- [ ] Load generator binary + image, validated Kafka producer, throughput baseline documented in README.

---

## Week 3 — Gate Access Engine & Redis Deduplication

**Goal:** Synchronous gate verification with atomic transactions; consume `ride-scans`, deduplicate via Redis.

### Technical Requirements
- [ ] `cmd/gate`: `POST /api/gate/verify` — **synchronous** ticket verification.
- [ ] **Atomic transaction (sqlx.Tx):**
  1. `SELECT ... FOR UPDATE` lock ticket row.
  2. Verify `status == 'ACTIVE'`.
  3. Update to `PENDING_ENTRY`.
  4. On turnstile trigger → commit as `USED`.
- [ ] **5-second Retry Grace Window:** same ticket_id within 5s → return cached result (Redis TTL=5s).
- [ ] **HMAC QR signing:** `internal/gate/qr.go` — payload `[UUID | Timestamp | HMAC_SHA256]`, 30s rotation, key from env (`HMAC_SECRET`).
- [ ] `GET /api/wristband/qr-token` — returns HMAC-signed QR payload (backend only, no UI).
- [ ] Gate = **Kafka producer**: after successful verification, publish `ScanEvent` to `ride-scans`.
- [ ] `cmd/consumer`: consumer-group `ride-scan-consumers`, manual commit (at-least-once), worker pool.
- [ ] `internal/kafka/consumer.go`: context-aware loop, backpressure on Redis/PG errors.
- [ ] `internal/redis/client.go`: idempotency via `SET dedup:{trace_id} 1 NX EX <ttl>`; `EXISTS` check before processing.
- [ ] Dead-letter topic `ride-scans-dlq` for poison messages.
- [ ] Retry strategy: exponential backoff for transient Redis failures; max retries before DLQ.

### Testing Milestones
- [ ] **M3.1** *Duplicate test:* publish same `trace_id` 5× → exactly 1 processed, 4 dropped, Redis key present with TTL.
- [ ] **M3.2** *TTL test:* after TTL expiry, same `trace_id` is processed again (idempotency window respected).
- [ ] **M3.3** *Poison message:* malformed JSON → routed to DLQ, consumer continues.
- [ ] **M3.4** *Gate double-scan within 5s* → same result (grace window).
- [ ] **M3.5** *Concurrent gate scans same ticket* → exactly one `USED`, other rejected (FOR UPDATE lock).
- [ ] **M3.6** *Concurrency test:* 10k events, 5 partitions, consumer pool=10 → no cross-worker duplicate processing.

### CI/CD Milestones
- [ ] **CI gate:** integration test job spins compose + loadgen(1k events with 20% duplicates) → assert dedup count.

### Deliverables
- [ ] Gate API binary, Consumer binary, Redis idempotency layer, DLQ handling, dedup unit + integration tests.

---

## Week 4 — Postgres Persistence, Redis Aggregation & Ticket Voucher System

**Goal:** Persist valid scans to Postgres; aggregate per-user ride sets in Redis; implement ticket voucher lifecycle.

### Technical Requirements
- [ ] `internal/postgres`: repository pattern, `ScanRepository.Insert` (idempotent on `trace_id` UNIQUE constraint), `pgx` pool.
- [ ] Migrations v2: add `UNIQUE(trace_id)` constraint, `CHECK` constraints on `ride_id` enum, ticket status enum (`UNCLAIMED`, `CLAIMED`, `ACTIVE`, `USED`).
- [ ] Redis Set aggregation: `SADD user:{user_id}:rides {ride_id}` per valid scan; daily key `user:{user_id}:rides:{date}` with TTL 48h.
- [ ] Transactional boundary: insert Postgres → SADD Redis; on Redis failure, compensate or mark for retry (outbox pattern).
- [ ] Batch insert optimization for high throughput (batch size configurable).
- [ ] **Ticket Voucher System (`internal/voucher`, `cmd/voucher`):**
  - `POST /api/vouchers/purchase` → create N voucher rows in PG (`status='unclaimed'`, `purchaser_id`).
  - `POST /api/vouchers/share` → generate magic link (signed JWT with `voucher_id`).
  - `GET /api/vouchers/claim?token=...` → JIT registration: if user exists, transfer ownership; if new, trigger zkLogin (stub in W4, full in W6).
  - State machine: `UNCLAIMED → CLAIMED → ACTIVE` (on gate entry) → `USED`.

### Testing Milestones
- [ ] **M4.1** *Persistence test:* 5k valid events → Postgres row count == 5k, no duplicates, indexes performant (EXPLAIN on lookup).
- [ ] **M4.2** *Aggregation test:* user scans 3 distinct rides → Redis Set cardinality == 3; duplicate ride scan → set unchanged.
- [ ] **M4.3** *Failure test:* Redis down mid-processing → Postgres insert still succeeds, aggregation retried via outbox/backoff, no data loss.
- [ ] **M4.4** *End-to-end pipeline test:* loadgen(10k, 15% dups) → consumer → assert Postgres == 8.5k unique, Redis sets correct per user.
- [ ] **M4.5** *Voucher purchase:* purchase 8 → 8 rows `unclaimed` → claim 1 → ownership transferred, status `claimed`.
- [ ] **M4.6** *Magic link expired/invalid* → rejected.

### CI/CD Milestones
- [ ] **CI gate:** full pipeline integration test in CI; coverage gate ≥ 70% on `internal/`.

### Deliverables
- [ ] Postgres repository, Redis aggregation, outbox/retry, voucher system, full ingestion pipeline tested.

---

## Week 5 — Sui Move Smart Contract (Attendance NFT)

**Goal:** Deploy Move module that mints attendance NFTs.

### Technical Requirements
- [ ] Install Sui CLI + Move toolchain; `move/attendance_nft/` package.
- [ ] Move module `attendance_nft::attendance`:
  - `MintCap` admin capability (transfer to deployer).
  - `mint_attendance_nft(recipient: address, ride_id: vector<u8>, date: u64, ctx)` → creates `AttendanceNFT` object, transfers to recipient.
  - **NFT (transferable)** — no `TransferPolicy` restriction (per Q7).
  - Events: `AttendanceMinted { recipient, ride_id, date }` for indexing.
  - Zero-address checks, access control via `MintCap`.
  - **Batch mint support:** `mint_batch(recipient, ride_ids: vector<vector<u8>>, date, ctx)` using `tx_context` for single-transaction multi-mint.
- [ ] Deploy to Sui **testnet** (devnet for iteration); record package ID in `.env`.
- [ ] Unit tests in Move (`#[test]` functions) for mint, capability guard, event emission, batch mint.

### Testing Milestones
- [ ] **M5.1** *Move unit tests:* `sui move test` passes — mint creates object, event emitted, unauthorized mint reverts, batch mint works.
- [ ] **M5.2** *Testnet deploy:* `sui client publish` succeeds; package ID verified on Sui explorer.
- [ ] **M5.3** *Manual mint:* call `mint_attendance_nft` + `mint_batch` via CLI → NFTs appear in recipient address.

### CI/CD Milestones
- [ ] **CI gate:** `ci-move.yml` runs `sui move test` on PRs touching `move/`.

### Deliverables
- [ ] Audited Move module, testnet deployment, package ID documented.

---

## Week 6 — Sui Go SDK, zkLogin Custodial & Batch Mint Pipeline

**Goal:** Go service reads Redis aggregation and mints NFTs via Sui SDK + zkLogin (custodial, server-side signed, gas sponsored).

### Technical Requirements
- [ ] `internal/sui`: wrapper around `github.com/block-vision/sui-go-sdk v1.2.1`; client init from `SUI_RPC_URL` env.
- [ ] **Full zkLogin custodial support:**
  - Backend receives Google JWT (`POST /api/auth/google`).
  - Exchange JWT for Sui address via zkLogin: derive ephemeral key pair server-side, generate zkLogin proof, compute Sui address.
  - Store ephemeral key + user mapping in Postgres `users` table (encrypted at rest with `ENCRYPTION_KEY` env).
  - **User never sees seed phrase, wallet extension, or pays gas.**
- [ ] **Gas sponsorship:** custodial gas pool wallet (funded via testnet faucet) whose gas coins are referenced in each zkLogin transaction (per Q4).
- [ ] `cmd/minter`: HTTP API `POST /mint/daily?user_id=...&date=...`:
  1. Read `SMEMBERS user:{user_id}:rides:{date}` from Redis.
  2. **Batch mint:** call `mint_batch` Move function (single tx, multiple ride_ids) instead of 5k individual txs.
  3. Record tx digest in Postgres `mint_logs` (user_id, ride_id, tx_digest, status, gas).
  4. Idempotency: skip already-minted (ride_id, date) pairs.
- [ ] **Sui RPC Throttling mitigation:**
  - Client-side request batching.
  - Exponential backoff with jitter for HTTP 429 Too Many Requests.
  - Configurable max concurrency semaphore.
- [ ] **JIT registration integration:** voucher claim flow (W4) now triggers real zkLogin → custodial wallet creation → background mint task ("anonymous to known" State 2).

### Testing Milestones
- [ ] **M6.1** *SDK smoke:* Go client reads package object on testnet.
- [ ] **M6.2** *Mint E2E:* trigger `/mint/daily` for a test user with 3 rides → 1 batch tx → 3 NFTs minted on testnet, tx digests recorded, Redis set cleared/marked.
- [ ] **M6.3** *Idempotency:* re-trigger same day → no duplicate mints, returns existing digests.
- [ ] **M6.4** *RPC 429:* simulate HTTP 429 → backoff retries succeed, no partial state corruption.
- [ ] **M6.5** *zkLogin:* Google JWT → Sui address derived, tx signed server-side, NFT in user's address.

### CI/CD Milestones
- [ ] **CI gate:** `ci-build.yml` builds `minter` image; mock Sui RPC for unit tests.

### Deliverables
- [ ] Minter service, zkLogin custodial integration, gas sponsorship, end-to-end batch mint pipeline validated on testnet.

---

## Week 7 — Observability: OpenTelemetry & Datadog

**Goal:** Full distributed tracing from Gate ingest → Sui mint, with metrics + structured logs.

### Technical Requirements
- [ ] `internal/telemetry`: OpenTelemetry SDK v1.44.0 init, OTLP gRPC exporters → Datadog agent / OTel collector.
- [ ] Trace propagation: Gate API injects `traceparent` → Kafka headers → consumer extracts → spans for dedup, PG insert, Redis SADD, Sui tx.
- [ ] Span attributes: `user_id`, `ride_id`, `trace_id`, `ticket_id`, `kafka.partition`, `kafka.offset`, `sui.tx_digest`, `dedup.hit`, `gate.decision`.
- [ ] Metrics (OTel metrics → Datadog): `events_consumed_total`, `events_dropped_duplicate_total`, `gate_verifications_total`, `mint_duration_seconds`, `kafka_lag`, `redis_ops_duration`, `pg_insert_duration`, `sui_rpc_429_total`.
- [ ] Structured logs (zerolog v1.35.1) with trace_id correlation for Datadog log correlation.
- [ ] Dashboards: gate health, consumer health, pipeline throughput, mint success rate, error budget.
- [ ] Alerts: Kafka lag > threshold, mint failure rate > 5%, consumer restarts, gate verification latency.

### Testing Milestones
- [ ] **M7.1** *Trace continuity:* single event's trace_id visible across gate → consumer → PG → Sui spans in Datadog.
- [ ] **M7.2** *Metrics validation:* run loadgen 5k events → dashboards reflect counts/latencies within 30s.
- [ ] **M7.3** *Error tracing:* inject Redis failure → error span + log with trace_id visible in Datadog.

### CI/CD Milestones
- [ ] **CI gate:** telemetry unit tests; lint ensures no `fmt.Println` (structured logging only).

### Deliverables
- [ ] OTel instrumentation across all services, Datadog dashboards + alerts, trace continuity verified.

---

## Week 8 — CI/CD, AWS Deployment & Final Load Test

**Goal:** Production-grade pipeline; deploy to AWS; final load test milestone.

### Technical Requirements
- [ ] GitHub Actions workflows:
  - `ci.yml`: lint → unit tests → integration tests (compose) → build images → push to ECR.
  - `cd.yml`: on merge to `main` → deploy to EC2 via `docker compose` (or ECS task def).
- [ ] Docker images: `Dockerfile.gate`, `Dockerfile.consumer`, `Dockerfile.loadgen`, `Dockerfile.minter`, `Dockerfile.voucher` — multi-stage, distroless/non-root.
- [ ] AWS: ECR repos per service; EC2 instance (t3.medium+) running Docker; IAM roles, no static creds (use instance profile); SG rules for Kafka/PG/Redis internal only.
- [ ] Secrets via AWS SSM Parameter Store / `.env` injected at runtime; **no secrets in images.**
- [ ] `README.md` finalized: architecture, setup, env vars, deploy, load-test instructions.

### Testing & CI/CD Milestones
- [ ] **M8.1** *CI green:* PR → full pipeline passes (lint, unit, integration, image build).
- [ ] **M8.2** *Deploy:* `cd.yml` deploys gate + consumer + minter + voucher to EC2; healthchecks pass.
- [ ] **M8.3** *FINAL LOAD TEST (capstone):* loadgen 50,000 events @ 2,000 RPS with 10% duplicates against AWS-deployed stack →
  - Kafka lag returns to 0 within 30s.
  - Postgres unique rows == 45,000 (±0.1%).
  - Dedup rate == 10%.
  - Gate p99 verify latency < 50ms.
  - Consumer p99 process latency < 50ms.
  - No message loss, no duplicate processing.
  - Datadog traces + dashboards reflect full run.
- [ ] **M8.4** *Mint validation:* trigger `/mint/daily` for sampled users → batch NFTs minted on testnet, digests logged.
- [ ] **M8.5** *Voucher E2E:* purchase 8 → magic link → JIT claim → gate entry → mint.

### Deliverables
- [ ] Full CI/CD, AWS deployment, capstone load test report, complete README.

---

## Cross-Cutting Concerns (Every Week)

- **Security:** Zero hardcoded secrets; zero-address checks in Move; zkLogin keys encrypted at rest; least-privilege IAM.
- **Code Quality:** `golangci-lint`, `go vet`, `gofumpt`; Move `sui move lint`; coverage ≥ 70%.
- **Memory Bank:** Update `memory-bank/activeContext.md`, `systemPatterns.md`, `progress.md` at each week's end.
- **Implementation Plan:** This file checked off (`[x]`) as each milestone completes.

---

## Risk Register

| Risk | Mitigation |
|------|-----------|
| Sui Go SDK immaturity | Spike in Week 5; fallback to Sui JSON-RPC via HTTP client |
| zkLogin complexity | Start with test OAuth (Google) in testnet; isolate in `internal/sui` |
| Kafka throughput on single EC2 | Partition tuning; horizontal consumer scaling; monitor lag |
| Redis memory growth | TTL on all keys; daily key rotation; monitor `used_memory` |
| Sui RPC throttling (429) | Client-side batching + exponential backoff with jitter (Week 6) |
| Gate double-scan race | `SELECT FOR UPDATE` + 5s retry grace window (Week 3) |

---

## Summary Timeline

| Week | Phase | Key Milestone |
|------|-------|---------------|
| 1 | Infra & Scaffolding | Local stack healthy |
| 2 | Load Generator | 10k events @ 1k RPS delivered |
| 3 | Gate Engine & Consumer Dedup | Duplicate detection + atomic gate verified |
| 4 | Persistence, Aggregation & Vouchers | Full ingestion pipeline E2E + voucher lifecycle |
| 5 | Sui Move Contract | Testnet deployment + batch mint |
| 6 | Sui SDK & zkLogin Custodial Mint | E2E batch mint on testnet |
| 7 | Observability | Trace continuity in Datadog |
| 8 | CI/CD & AWS | 50k load test on AWS |