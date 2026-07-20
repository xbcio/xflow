.PHONY: all build test test-concurrency lint fmt tidy clean run-server run-runner install-hooks proto proto-tools \
        env-up env-down env-reset env-logs env-migrate test-integration test-integration-required test-perf perf-sample test-soak

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
	go test -tags=concurrency ./backend/local/ ./backend/distributed/... -race -count=3 -timeout 5m

# ── Code quality ───────────────────────────────────────────────────────────────

lint:
	# --build-tags soak makes the standalone soak harness package visible to
	# the linter (it is excluded from the default build config). Pre-existing
	# issues surfaced by broader tags (integration/perf) are out of scope here.
	golangci-lint run --build-tags soak ./...

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

# ── Test environment (podman) ────────────────────────────────────────────────
ENV_FILE := $(wildcard test/env/.env)
ENV_FLAG := $(if $(ENV_FILE),--env-file $(ENV_FILE),)
COMPOSE  := podman compose -f test/env/docker-compose.yml $(ENV_FLAG)

env-up:
	@$(COMPOSE) up -d
env-down:
	@$(COMPOSE) down
env-reset:
	@$(COMPOSE) down -v
env-logs:
	@$(COMPOSE) logs -f
env-migrate:
	@bash test/env/migrate.sh

# ── Integration / perf tests (gated by build tags) ───────────────────────────
# test-integration skips when Redis/MySQL/Kafka are unavailable (local dev).
# test-integration-required fails the job when deps are missing — use in CI so a
# skipped dependency cannot be mistaken for a passing gate (A0, 2026-07-18 §6.3).
test-integration:
	@set -a; [ -f test/env/.env ] && . ./test/env/.env; set +a; \
	go test -tags=integration -race -count=1 -timeout 600s ./test/integration/...

test-integration-required:
	@set -a; [ -f test/env/.env ] && . ./test/env/.env; set +a; \
	XFLOW_REQUIRE_REDIS_INTEGRATION=1 \
	go test -tags=integration -race -count=1 -timeout 600s ./test/integration/...

test-perf:
	@set -a; [ -f test/env/.env ] && . ./test/env/.env; set +a; \
	: "$${XFLOW_TEST_REDIS_ADDR:=localhost:$${REDIS_PORT:-6379}}"; \
	: "$${XFLOW_TEST_KAFKA_BROKERS:=localhost:$${KAFKA_PORT:-9092}}"; \
	export XFLOW_TEST_REDIS_ADDR XFLOW_TEST_KAFKA_BROKERS; \
	go test -tags=perf -bench=. -benchtime=2s -timeout 30m ./test/perf/...

# test-soak runs the HA soak harness smoke over in-process miniredis (no real
# Redis / multi-host topology required). The smoke only verifies multi-replica
# start/stop + single-leader convergence; real fault injection (Task 5.2) and
# SLO quantification (Task 5.3) are ENVIRONMENT-GATED — see
# docs/references/ha-soak-plan.md §4/§6. Standalone `soak` build tag keeps the
# harness out of the default integration suite.
test-soak:
	go test -tags=soak -race -count=1 -timeout 120s ./test/soak/...

# perf-sample runs the perf bench suite and records results to a file for
# regression monitoring. Sampling only — NOT a capacity commitment. Requires
# `make env-up` (Redis + Kafka). See docs/design/HIGH-THROUGHPUT-INGESTION.md §6.
perf-sample:
	@set -a; [ -f test/env/.env ] && . ./test/env/.env; set +a; \
	: "$${XFLOW_TEST_REDIS_ADDR:=localhost:$${REDIS_PORT:-6379}}"; \
	: "$${XFLOW_TEST_KAFKA_BROKERS:=localhost:$${KAFKA_PORT:-9092}}"; \
	export XFLOW_TEST_REDIS_ADDR XFLOW_TEST_KAFKA_BROKERS; \
	./scripts/perf-sample.sh
