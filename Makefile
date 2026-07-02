.PHONY: all build test test-concurrency lint fmt tidy clean run-server run-runner install-hooks proto proto-tools

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
	go test ./... -race -count=1 -timeout 120s

test-verbose:
	go test ./... -race -count=1 -timeout 120s -v

test-examples:
	go test ./sdk/examples/ -race -count=1 -v -timeout 30s

# Concurrency stress suite. Gated behind the `concurrency` build tag so the
# default `make test` stays fast. Spec: .claude/specs/lua-concurrency-tests.md
test-concurrency:
	go test -tags=concurrency ./backend/memory/ ./backend/asynq/ -race -count=3 -timeout 5m

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

# ── Protobuf / gRPC ──────────────────────────────────────────────────────────

# Install the protoc Go plugins (run once). Requires protoc on PATH.
proto-tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Regenerate gRPC stubs from .proto sources. Requires protoc + plugins on PATH.
proto:
	protoc \
		--go_out=. --go_opt=module=github.com/xbcio/xflow \
		--go-grpc_out=. --go-grpc_opt=module=github.com/xbcio/xflow \
		service/protocol/runnerpb/runner.proto

# ── Git hooks ──────────────────────────────────────────────────────────────────

install-hooks:
	cp scripts/hooks/commit-msg .git/hooks/commit-msg
	chmod +x .git/hooks/commit-msg
	@echo "git hooks installed"

# ── Clean ──────────────────────────────────────────────────────────────────────

clean:
	rm -rf bin/
