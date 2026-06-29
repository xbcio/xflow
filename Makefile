.PHONY: all build test lint fmt tidy clean run-server run-runner

# ── Build ──────────────────────────────────────────────────────────────────────

all: build

build:
	go build -o bin/server ./cmd/server
	go build -o bin/runner ./cmd/runner

build-server:
	go build -o bin/server ./cmd/server

build-runner:
	go build -o bin/runner ./cmd/runner

# ── Test ───────────────────────────────────────────────────────────────────────

test:
	go test ./sdk/... ./engine/... ./node/... ./types/... ./store/... -race -count=1 -timeout 120s

test-verbose:
	go test ./sdk/... ./engine/... ./node/... ./types/... ./store/... -race -count=1 -timeout 120s -v

test-examples:
	go test ./sdk/examples/ -run TestExpenseClaim -race -count=1 -v -timeout 30s

test-cluster:
	go test ./sdk/examples/ -run TestOrderFulfillment -race -count=1 -v -timeout 60s

# ── Code quality ───────────────────────────────────────────────────────────────

lint:
	golangci-lint run ./...

fmt:
	go fmt ./...

vet:
	go vet ./...

tidy:
	go mod tidy

# ── Run ────────────────────────────────────────────────────────────────────────

run-server:
	XFLOW_REDIS_ADDR=$(or $(REDIS_ADDR),localhost:6379) \
	XFLOW_HTTP_ADDR=$(or $(HTTP_ADDR),:8080) \
	go run ./cmd/server

run-runner:
	XFLOW_REDIS_ADDR=$(or $(REDIS_ADDR),localhost:6379) \
	go run ./cmd/runner

# ── Database ───────────────────────────────────────────────────────────────────

# Apply the xflow schema to a running MySQL instance.
# Usage: make db-migrate DSN="user:pass@tcp(localhost:3306)/xflow?parseTime=true"
db-migrate:
	@if [ -z "$(DSN)" ]; then echo "Usage: make db-migrate DSN=<mysql-dsn>"; exit 1; fi
	mysql "$(DSN)" < db/xflow_schema.sql

# ── Clean ──────────────────────────────────────────────────────────────────────

clean:
	rm -rf bin/
