# ThemeParkNFT

> **Amusement park attendance NFT engine** — a Go-based backend that ingests ride-scan events from turnstiles, deduplicates them, persists to PostgreSQL, aggregates user ride sets in Redis, and batch-mints attendance NFTs on Sui blockchain via zkLogin custodial sponsorship.

## Architecture

```
[Wristband QR Scanner] → Gate API (POST /api/gate/verify)
    ├── HMAC-signed QR tokens (30s rotation)
    ├── Atomic ticket verification (SELECT FOR UPDATE)
    ├── 5-second grace window (Redis cache)
    └── Publishes verified scans → Kafka (ride-scans)

[Kafka Consumer] → Dedup (Redis SETNX) → Postgres persistence → Redis aggregation (SADD)

[Minter Service] → Reads Redis sets → Batch mints NFTs on Sui (zkLogin custodial, gas sponsored)

[Voucher Service] → Purchase vouchers → Magic link (JWT) → JIT zkLogin claim
```

### Services

| Service | Port | Description |
|---|---|---|
| **Gate API** | `8080` | QR token generation, ticket verification, turnstile confirmation |
| **Consumer** | `8081` (health only) | Kafka consumer: deduplication, persistence, aggregation |
| **Load Generator** | — | Test harness: produces synthetic scan events into Kafka |
| **Minter** | `8083` | End-of-day batch NFT minting + zkLogin authentication |
| **Voucher** | `8084` | Voucher purchase, sharing, and JIT claim system |

### Infrastructure

| Component | Port | Image |
|---|---|---|
| ZooKeeper | `2181` | confluentinc/cp-zookeeper:7.6.0 |
| Kafka | `29092` (host) | confluentinc/cp-kafka:7.6.0 |
| Redis | `6379` | redis:7-alpine |
| PostgreSQL | `5432` | postgres:16-alpine |
| OTel Collector | `4317` | otel/opentelemetry-collector-contrib:0.120.0 |

## Prerequisites

- **Go 1.26.5+**
- **Docker** and **Docker Compose** (for infrastructure)
- **Sui CLI** v1.76+ (for Move contract deployment)
- **Pinata** account (free tier, for IPFS metadata pinning)

## Sui CLI Installation

```bash
# Download prebuilt binary
curl -sL "https://github.com/MystenLabs/sui/releases/download/testnet-v1.76.0/sui-testnet-v1.76.0-ubuntu-x86_64.tgz" -o /tmp/sui.tgz
mkdir -p ~/.local/bin
tar -xzf /tmp/sui.tgz -C ~/.local/bin/

# Add to PATH
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
sui --version

# Configure testnet
sui client new-env --alias testnet --rpc https://sui-testnet.g.alchemy.com/v2/YOUR_KEY
sui client switch --env testnet
```

## Move Contract

The project includes a Sui Move smart contract for attendance NFTs with IPFS metadata.

### Deployed Contract

| Field | Value |
|---|---|
| **Network** | Sui Testnet |
| **Package ID** | `0x78c9dbba118923c4976599877450cd281f880f94d217d806714782a658b01d1e` |
| **Deployer** | `0x3cd9956f278d5cfa00ff5444db60957a796b8e97213cb843526a7318382d7dbe` |
| **MintCap Object** | `0xfe4dcb534bf96074a1e2f2ac62a6261d8d52a2e0dfbea8bd0f4effb7745f30cb` |
| **View on Explorer** | [Suivision](https://testnet.suivision.xyz/package/0x78c9dbba118923c4976599877450cd281f880f94d217d806714782a658b01d1e) |

### Available Functions

| Function | Visibility | Description |
|---|---|---|
| `mint_attendance_nft` | public | Mint a single NFT with IPFS metadata URL, name, ride ID, and date |
| `mint_batch` | public | Batch mint multiple NFTs in one transaction |
| `burn` | public | Admin-only: destroy an NFT (requires MintCap) |
| `update_metadata` | public | Admin-only: correct IPFS metadata URL |
| `get_recipient` | public | View: returns the NFT owner address |
| `get_ride_id` | public | View: returns the ride identifier |
| `get_date` | public | View: returns the mint date (YYYYMMDD) |
| `get_name` | public | View: returns the NFT display name |
| `get_metadata_url` | public | View: returns the IPFS metadata URL |

### Re-deploying

```bash
cd move/attendance_nft
sui move build          # Compile the Move package
sui move test           # Run unit tests (11 tests)
sui client publish --gas-budget 100000000   # Deploy to testnet
# Copy the Package ID from output → put in .env as SUI_PACKAGE_ID
```

## IPFS / Pinata

NFT artwork and metadata are pinned to IPFS via Pinata. The minter implements **Option A** — artwork and metadata are pinned once per ride type (~20 pins total for 10 ride types) and reused across all NFTs for that ride. This keeps pin count under the Pinata free tier limit (500 pins) regardless of how many NFTs are minted.

### Configuration

```bash
# .env
PINATA_API_KEY=your_api_key       # From https://app.pinata.cloud/developers/api-keys
PINATA_API_SECRET=your_api_secret
PINATA_GATEWAY=https://gateway.pinata.cloud   # Or your dedicated gateway
```

### What Gets Pinned

| Content | Example Filename | Pinata Name |
|---|---|---|
| SVG Artwork | `ride-001-2026-07-27.svg` | Pinata dashboard shows `ride-001-2026-07-27.svg` |
| Metadata JSON | `ride-001-metadata-2026-07-27` | Pinata dashboard shows `ride-001-metadata-2026-07-27` |

Each NFT metadata JSON includes:
```json
{
  "name": "Space Mountain — 2026-07-27",
  "description": "Attendance NFT for completing Space Mountain on 2026-07-27",
  "image": "ipfs://QmX7Y.../space-mountain.svg",
  "attributes": [
    {"trait_type": "Ride", "value": "Space Mountain"},
    {"trait_type": "Ride ID", "value": "ride-001"},
    {"trait_type": "Date", "value": "2026-07-27"},
    {"trait_type": "Rarity", "value": "Common"}
  ]
}
```

### CID Cache

The `CIDCache` (in `internal/storage/cache.go`) holds pinned CIDs in memory. Once a ride's artwork + metadata are pinned, all subsequent NFTs for that ride use the same CIDs — no additional Pinata API calls.

### Permissions

Pinata API keys need **Admin** permissions. The code uses:
- `POST /pinning/pinFileToIPFS` — pin SVG artwork
- `POST /pinning/pinJSONToIPFS` — pin metadata JSON

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/Logiqode/ThemeParkNFT.git
cd ThemeParkNFT

# 2. Configure environment
cp .env.example .env
# Edit .env with your credentials if needed (defaults work for local dev)

# 3. Build all services
make build

# 4. Start infrastructure (Postgres, Redis, Kafka, ZooKeeper, OTel)
make up

# 5. Wait until the whole compose stack is healthy (< 60s)
make healthy

# 6. Apply database migrations (idempotent — safe to run again)
make migrate-up

# 7. Source environment variables and start services (in separate terminals)
set -a && source .env && set +a

# Terminal 1 — Gate API
./bin/gate

# Terminal 2 — Kafka Consumer
./bin/consumer

# Terminal 3 — Minter (optional)
./bin/minter

# Terminal 4 — Voucher (optional)
./bin/voucher
```

## Environment Configuration

Copy `.env.example` to `.env` and configure:

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_HOST` | `localhost` | PostgreSQL host |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_USER` | `themepark` | PostgreSQL user |
| `POSTGRES_PASSWORD` | `themepark_dev` | PostgreSQL password |
| `POSTGRES_DB` | `themepark` | PostgreSQL database |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `KAFKA_BROKERS` | `localhost:29092` | Kafka bootstrap servers |
| `HMAC_SECRET` | *(required)* | 32-byte secret for QR signing |
| `SUI_RPC_URL` | `https://fullnode.testnet.sui.io` | Sui RPC endpoint |
| `SUI_GAS_POOL_MNEMONIC` | *(required)* | Gas pool wallet mnemonic |
| `GOOGLE_OAUTH_CLIENT_ID` | *(required)* | Google OAuth client (zkLogin) |
| `ENCRYPTION_KEY` | *(required)* | 32-byte key for ephemeral key encryption |

## Testing the System

### Health Checks

```bash
# Gate API
curl http://localhost:8080/healthz

# Consumer (headless Kafka worker — health only)
curl http://localhost:8081/healthz

# Minter
curl http://localhost:8083/healthz

# Voucher
curl http://localhost:8084/healthz
```

### Gate API (port 8080)

```bash
# Generate a QR token (gate staff: one-time HMAC QR for the visitor to present)
curl http://localhost:8080/api/wristband/scan-visitor-qr-token

# Verify a ticket
curl -X POST http://localhost:8080/api/gate/verify \
  -H "Content-Type: application/json" \
  -d '{"ticket_id":"test-001"}'

# Confirm turnstile entry
curl -X POST http://localhost:8080/api/gate/confirm \
  -H "Content-Type: application/json" \
  -d '{"ticket_id":"test-001"}'
```

### Voucher System (port 8084)

```bash
# Purchase vouchers
curl -X POST http://localhost:8084/api/vouchers/purchase \
  -H "Content-Type: application/json" \
  -d '{"purchaser_email":"alice@test.com","quantity":3}'

# Generate magic link (share voucher)
curl -X POST http://localhost:8084/api/vouchers/share \
  -H "Content-Type: application/json" \
  -d '{"voucher_id":"<id-from-purchase>"}'

# Claim with magic link
curl "http://localhost:8084/api/vouchers/claim?token=<jwt-from-share>&email=bob@test.com"
```

### Minter — Batch NFT Mint (port 8083)

```bash
# zkLogin registration
curl -X POST http://localhost:8083/api/auth/google \
  -H "Content-Type: application/json" \
  -d '{"token":"<google-jwt>"}'

# Batch mint daily attendance NFTs
curl -X POST http://localhost:8083/mint/daily \
  -H "Content-Type: application/json" \
  -d '{"email":"alice@test.com","date":"2026-07-26"}'
```

### Load Generator — Stress Testing

```bash
# Basic: 1,000 events at 100 RPS for 10 seconds
./bin/loadgen -rate 100 -duration 10s -users 50 -rides 5

# Throughput: 10,000 events at 1,000 RPS with 15% duplicates
./bin/loadgen -rate 1000 -duration 10s -users 500 -rides 10 -dup-pct 15

# Spike test: 5,000 events burst in 1 second
./bin/loadgen -rate 5000 -duration 1s -users 1000 -rides 5

# All flags
./bin/loadgen --help
```

### Run Tests & Lint

```bash
make test    # go test ./... -v -count=1
make lint    # golangci-lint run ./...
make tidy    # go mod tidy
```

## Database Inspection

No need to install database tools locally — use the tools inside the running Docker containers.

### PostgreSQL

```bash
# Run a single query
docker exec deployments-postgres-1 psql -U themepark -d themepark -c "SELECT COUNT(*) FROM scan_events;"

# Interactive session
docker exec -it deployments-postgres-1 psql -U themepark -d themepark
```

Common queries:
```sql
SELECT * FROM users;
SELECT COUNT(*) FROM scan_events;
SELECT user_id, ride_id, COUNT(*) FROM scan_events GROUP BY user_id, ride_id;
SELECT * FROM ticket_vouchers WHERE status = 'unclaimed';
SELECT * FROM mint_logs;
```

### Redis

```bash
# List all keys matching a pattern
docker exec deployments-redis-1 redis-cli KEYS "user:*"

# Count deduplication entries
docker exec deployments-redis-1 redis-cli KEYS "dedup:*" | wc -l

# Get a user's ride set for a specific date
docker exec deployments-redis-1 redis-cli SMEMBERS "user:<email>:rides:2026-07-26"

# Check a specific dedup key
docker exec deployments-redis-1 redis-cli GET "dedup:<trace_id>"
```

### Kafka

```bash
# List topics
docker exec deployments-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list

# Check consumer group lag
docker exec deployments-kafka-1 kafka-consumer-groups --bootstrap-server localhost:9092 --group ride-scan-consumers --describe

# Peek at messages in ride-scans topic
docker exec deployments-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic ride-scans --max-messages 5
```

## Makefile Targets

| Command | Description |
|---|---|
| `make up` | Start all infrastructure (Docker Compose) |
| `make down` | Stop all infrastructure |
| `make healthy` | Wait until all compose services are healthy (< 60s; `TIMEOUT=NN` to override) |
| `make migrate` / `migrate-up` / `migrate-down` / `migrate-version` | Run versioned migrations via `cmd/migrate` |
| `make build` | Compile all 6 binaries to `bin/` (incl. `migrate`) |
| `make test-integration` | Run integration smoke tests (needs `make up`; set `INTEGRATION=1`) |
| `make test` | Run all Go tests |
| `make lint` | Run golangci-lint |
| `make tidy` | Run `go mod tidy` |
| `make clean` | Remove `bin/` directory |
| `make reset` | Tear down infrastructure, remove all data, restart fresh |

## Resetting for Fresh Tests

To wipe all data and start with a clean slate:

```bash
# 1. Stop all Go services (Ctrl+C in each terminal)

# 2. Rebuild binaries (picks up any code changes)
make build

# 3. Tear down infrastructure AND remove all data (volumes)
docker compose -f deployments/docker-compose.yml down -v

# 4. Start fresh (schema auto-applied on first Postgres boot)
make up

# 5. Verify all healthy
docker compose -f deployments/docker-compose.yml ps

# 6. Clear stale env vars and reload
unset POSTGRES_USER POSTGRES_PASSWORD POSTGRES_DB KAFKA_BROKERS
set -a && source .env && set +a

# 7. Start consumer first (drains any stale Kafka messages), then other services
./bin/consumer    # Terminal 1
./bin/gate        # Terminal 2
```

What gets reset:

| Data | Reset by |
|---|---|
| PostgreSQL (all tables) | `docker compose down -v` removes the `pgdata` volume |
| Redis (all keys) | `docker compose down -v` destroys the Redis container |
| Kafka (topics, messages) | `docker compose down -v` destroys Kafka data |
| Go binaries | `make build` recompiles everything fresh |

## Infrastructure Management

```bash
# Start all backing services
make up

# Check health
docker compose -f deployments/docker-compose.yml ps

# View logs for a specific service
docker compose -f deployments/docker-compose.yml logs -f kafka
docker compose -f deployments/docker-compose.yml logs -f postgres

# Stop all services
make down

# Tear down and remove volumes
docker compose -f deployments/docker-compose.yml down -v
```

## Troubleshooting

### Services crash on startup with nil pointer dereference
Ensure `os.Stderr` is set as the zerolog output writer (fixed in current codebase):
```go
log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
```

### Postgres connection refused or auth failed
```bash
# Check ports are mapped
docker compose -f deployments/docker-compose.yml ps | grep postgres
# Should show: 0.0.0.0:5432->5432/tcp

# Clear stale env vars and reload
unset POSTGRES_USER POSTGRES_PASSWORD POSTGRES_DB KAFKA_BROKERS
set -a && source .env && set +a
```

### Kafka connection refused
Kafka is exposed on host port `29092` (not `9092`). Ensure `.env` has:
```
KAFKA_BROKERS=localhost:29092
```

## Project Structure

```
.
├── cmd/
│   ├── gate/         # Gate Access API
│   ├── consumer/     # Kafka consumer (dedup + persist + aggregate)
│   ├── loadgen/      # Test-only Kafka producer
│   ├── minter/       # Batch NFT mint + zkLogin
│   └── voucher/      # Ticket voucher system
├── internal/
│   ├── config/       # Environment configuration (envconfig + viper)
│   ├── gate/         # QR HMAC signing, ticket verification
│   ├── health/       # Health check endpoints
│   ├── kafka/        # Producer/consumer wrappers
│   ├── models/       # Shared data models
│   ├── postgres/     # Database repository (sqlx + pgx)
│   ├── redis/        # Redis client (dedup + aggregation)
│   ├── sui/          # Sui blockchain client (zkLogin, batch mint)
│   └── telemetry/    # OpenTelemetry initialization
├── deployments/      # Docker Compose, Dockerfiles, OTel config
├── migrations/       # SQL migration files
├── move/             # Sui Move smart contract (attendance NFT)
├── scripts/          # Utility scripts
└── memory-bank/      # Project documentation
```

## License

Proprietary. All rights reserved.