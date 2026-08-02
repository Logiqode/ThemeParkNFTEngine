.PHONY: up down migrate migrate-up migrate-down migrate-version healthy build test test-integration bench-milestones bench-congestion bench-verify lint tidy clean reset

# Start all services via docker-compose
up:
	docker compose -f deployments/docker-compose.yml up -d

# Stop all services
down:
	docker compose -f deployments/docker-compose.yml down

# Run database migrations (default: up). Usage: make migrate   or   make migrate CMD=down
migrate:
	go run ./cmd/migrate $(CMD)

# Apply all pending migrations
migrate-up:
	go run ./cmd/migrate up

# Roll back all migrations (destructive)
migrate-down:
	go run ./cmd/migrate down

# Print current schema_migrations version
migrate-version:
	go run ./cmd/migrate version

# Wait until the whole compose stack is healthy (M1.1). Usage: make healthy [TIMEOUT=60]
# Each container must be either a healthy running service (Health == "healthy")
# or a successfully completed one-shot job (exited with code 0, e.g. kafka-init).
healthy:
	@echo "Waiting for compose services to become healthy (timeout: $${TIMEOUT:-60}s)..."
	@docker compose -f deployments/docker-compose.yml up -d
	@timeout $${TIMEOUT:-60} bash -c 'until docker compose -f deployments/docker-compose.yml ps --all --format json | jq -s -e '\''length > 0 and ([.[] | ((.State == "running" and .Health == "healthy") or (.State == "exited" and .ExitCode == 0))] | all)'\'' >/dev/null 2>&1; do sleep 2; done'
	@echo "All compose services healthy."

# Build all binaries
build:
	go build -o bin/gate ./cmd/gate
	go build -o bin/consumer ./cmd/consumer
	go build -o bin/loadgen ./cmd/loadgen
	go build -o bin/minter ./cmd/minter
	go build -o bin/voucher ./cmd/voucher
	go build -o bin/migrate ./cmd/migrate

# Run tests
test:
	go test ./... -v -count=1

# Run integration smoke tests (requires `make up` + INTEGRATION=1)
test-integration:
	go test -tags=integration ./internal/pipeline -v -count=1
	go test -tags=integration ./internal/gate -v -count=1
	go test -tags=integration ./internal/kafka -v -count=1

# Week 2 benchmarks (R15): Kafka delivery reliability under congestion.
# Requires: healthy compose stack (`make healthy`), go, jq, bc.
# Usage: make bench-milestones [BROKERS=localhost:29092] [TOPIC=ride-scans]
bench-milestones:
	./scripts/bench/run_milestones.sh $${BROKERS:-localhost:29092} $${TOPIC:-ride-scans}

bench-congestion:
	./scripts/bench/run_congestion.sh $${BROKERS:-localhost:29092} $${TOPIC:-ride-scans}

# Verify a loadgen manifest against the topic (see scripts/bench/verify_delivery.go)
bench-verify:
	go run ./scripts/bench/verify_delivery.go --manifest $${MANIFEST:-manifest.jsonl} --brokers $${BROKERS:-localhost:29092}

# Run linter (requires golangci-lint)
lint:
	golangci-lint run ./...

# Tidy dependencies
tidy:
	go mod tidy

# Clean binaries
clean:
	rm -rf bin/

# Full reset: rebuild binaries, wipe all data, restart infrastructure
reset:
	docker compose -f deployments/docker-compose.yml down -v
	docker compose -f deployments/docker-compose.yml up -d
	go build -o bin/gate ./cmd/gate
	go build -o bin/consumer ./cmd/consumer
	go build -o bin/loadgen ./cmd/loadgen
	go build -o bin/minter ./cmd/minter
	go build -o bin/voucher ./cmd/voucher
	go build -o bin/migrate ./cmd/migrate
	@echo "=== Reset complete ==="
	@echo "Next: source .env, then run 'make migrate-up', then start ./bin/consumer and ./bin/gate"
