.PHONY: up down migrate build test lint tidy clean reset

# Start all services via docker-compose
up:
	docker compose -f deployments/docker-compose.yml up -d

# Stop all services
down:
	docker compose -f deployments/docker-compose.yml down

# Build all binaries
build:
	go build -o bin/gate ./cmd/gate
	go build -o bin/consumer ./cmd/consumer
	go build -o bin/loadgen ./cmd/loadgen
	go build -o bin/minter ./cmd/minter
	go build -o bin/voucher ./cmd/voucher

# Run tests
test:
	go test ./... -v -count=1

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
	@echo "=== Reset complete ==="
	@echo "Next: source .env, then start ./bin/consumer and ./bin/gate"
