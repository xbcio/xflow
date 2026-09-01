.PHONY: all build test test-verbose test-examples test-concurrency test-script-wasm test-coverage lint fmt vet tidy clean run-server run-runner install-hooks \
        check-go check-proto-tools proto proto-check proto-tools \
        env-up env-down env-reset env-logs env-migrate env-ready test-integration test-integration-required test-g0-evidence-required test-g1-evidence-required test-perf perf-sample test-soak \
        web-install web-lint web-typecheck web-test web-test-coverage web-check-boundaries web-check-production-fixtures web-build web-e2e web-e2e-preview web-ci web-all validate-openapi

GO ?= go
GIT ?= git
GOTOOLCHAIN := go1.25.0
export GOTOOLCHAIN
GO_VERSION := 1.25.0
SCRIPT_PACKAGE_ROOT := ./node/internal/code/script
SCRIPT_JS_PACKAGES := $(SCRIPT_PACKAGE_ROOT) $(SCRIPT_PACKAGE_ROOT)/js
WASM_PACKAGE := $(SCRIPT_PACKAGE_ROOT)/wasm
SCRIPT_WASM_PACKAGES := $(SCRIPT_JS_PACKAGES) $(WASM_PACKAGE)
# Eight shards keep heavyweight multi-MB compiles from consuming most of the
# finite five-minute package watchdog under race-enabled host contention.
WASM_TEST_SHARDS := 8
WASM_COVERAGE_SHARDS := $(WASM_TEST_SHARDS)
INTEGRATION_PACKAGE := ./test/integration
INTEGRATION_TEST_SHARDS := 4
COVERAGE_PROFILE ?= coverage.out
PROTOC_VERSION := 35.1
PROTOC_GEN_GO_VERSION := v1.36.11
PROTOC_GEN_GO_GRPC_VERSION := v1.6.2
PROTOC_GEN_GO_VERSION_NO_V := $(patsubst v%,%,$(PROTOC_GEN_GO_VERSION))
PROTOC_GEN_GO_GRPC_VERSION_NO_V := $(patsubst v%,%,$(PROTOC_GEN_GO_GRPC_VERSION))

# ── Toolchain baseline ─────────────────────────────────────────────────────────

check-go:
	@mod_version="$$($(GO) list -m -f '{{.GoVersion}}')"; \
	if [ "$$mod_version" != "$(GO_VERSION)" ]; then \
		echo "ERROR: go.mod declares go $$mod_version, expected $(GO_VERSION)"; \
		exit 1; \
	fi; \
	go_version="$$($(GO) env GOVERSION)"; \
	if [ "$$go_version" != "go$(GO_VERSION)" ]; then \
		echo "ERROR: Go toolchain $$go_version, expected go$(GO_VERSION)"; \
		exit 1; \
	fi; \
	gtoolchain="$$($(GO) env GOTOOLCHAIN)"; \
	if [ "$$gtoolchain" != "go$(GO_VERSION)" ]; then \
		echo "ERROR: GOTOOLCHAIN=$$gtoolchain, expected go$(GO_VERSION)"; \
		exit 1; \
	fi; \
	echo "Go toolchain OK: module go $$mod_version, toolchain $$go_version, GOTOOLCHAIN=$$gtoolchain"

# ── Build ──────────────────────────────────────────────────────────────────────

all: build

build: check-go
	$(GO) build -o bin/server ./cmd/server
	$(GO) build -o bin/runner ./cmd/runner

build-server: check-go
	$(GO) build -o bin/server ./cmd/server

build-runner: check-go
	$(GO) build -o bin/runner ./cmd/runner

# ── Test ───────────────────────────────────────────────────────────────────────

# Discover top-level WASM tests at runtime so new tests are included without a
# maintained allowlist. Round-robin shards keep every race-enabled invocation
# within the focused 5-minute package budget on supported developer/CI hosts.
define RUN_WASM_TEST_SHARDS
set -eu; \
case "$(WASM_TEST_SHARDS)" in ''|*[!0-9]*|0) echo "ERROR: WASM_TEST_SHARDS must be a positive integer" >&2; exit 1;; esac; \
wasm_list="$$($(GO) test $(WASM_PACKAGE) -count=1 -list '^(Test|Fuzz|Example)')" || exit 1; \
wasm_tests="$$(printf '%s\n' "$$wasm_list" | awk '/^(Test|Fuzz|Example)/')"; \
if [ -z "$$wasm_tests" ]; then echo "ERROR: no runnable WASM tests discovered" >&2; exit 1; fi; \
wasm_test_count="$$(printf '%s\n' "$$wasm_tests" | awk 'NF { count++ } END { print count + 0 }')"; \
assigned=0; \
shard=0; \
while [ "$$shard" -lt "$(WASM_TEST_SHARDS)" ]; do \
	shard_tests="$$(printf '%s\n' "$$wasm_tests" | awk -v shard="$$shard" -v shards="$(WASM_TEST_SHARDS)" \
		'((NR - 1) % shards) == shard { printf "%s%s", separator, $$0; separator = "|" } END { print "" }')"; \
	if [ -z "$$shard_tests" ]; then echo "ERROR: WASM test shard $$shard is empty" >&2; exit 1; fi; \
	shard_count="$$(printf '%s\n' "$$shard_tests" | awk -F '|' '{ print NF }')"; \
	assigned=$$((assigned + shard_count)); \
	echo "==> WASM tests: shard $$((shard + 1))/$(WASM_TEST_SHARDS) ($$shard_count tests, serialized, 5m timeout)"; \
	$(GO) test -p=1 $(WASM_PACKAGE) -run="^($$shard_tests)$$" -race -count=1 -timeout 5m $(1); \
	shard=$$((shard + 1)); \
done; \
if [ "$$assigned" -ne "$$wasm_test_count" ]; then \
	echo "ERROR: assigned $$assigned of $$wasm_test_count discovered WASM tests" >&2; \
	exit 1; \
fi
endef

# Discover every top-level integration test and run the root package in serial
# shards so each race-enabled process keeps its own 10-minute timeout budget.
# Descendant utility packages are outside the root test binary and run once
# after the shards, preserving the coverage of ./test/integration/....
define RUN_INTEGRATION_TEST_SHARDS
set -eu; \
set -a; [ -f test/env/.env ] && . ./test/env/.env; set +a; \
: "$${XFLOW_TEST_REDIS_ADDR:=localhost:$${REDIS_PORT:-6379}}"; \
: "$${XFLOW_TEST_KAFKA_BROKERS:=localhost:$${KAFKA_PORT:-9092}}"; \
export XFLOW_TEST_REDIS_ADDR XFLOW_TEST_KAFKA_BROKERS; \
if [ "$(1)" = "required" ]; then \
	export XFLOW_REQUIRE_REDIS_INTEGRATION=1; \
	export XFLOW_REQUIRE_MYSQL_INTEGRATION=1; \
	export XFLOW_REQUIRE_KAFKA_INTEGRATION=1; \
fi; \
case "$(INTEGRATION_TEST_SHARDS)" in ''|*[!0-9]*|0) echo "ERROR: INTEGRATION_TEST_SHARDS must be a positive integer" >&2; exit 1;; esac; \
integration_list="$$($(GO) test -tags=integration $(INTEGRATION_PACKAGE) -count=1 -list '^(Test|Fuzz|Example)')" || exit 1; \
integration_tests="$$(printf '%s\n' "$$integration_list" | awk '/^(Test|Fuzz|Example)/')"; \
if [ -z "$$integration_tests" ]; then echo "ERROR: no runnable integration tests discovered" >&2; exit 1; fi; \
integration_test_count="$$(printf '%s\n' "$$integration_tests" | awk 'NF { count++ } END { print count + 0 }')"; \
tmpdir="$$(mktemp -d "$${TMPDIR:-/tmp}/xflow-integration-shards.XXXXXX")"; \
trap 'rm -rf "$$tmpdir"' EXIT HUP INT TERM; \
printf '%s\n' "$$integration_tests" | sort > "$$tmpdir/discovered"; \
: > "$$tmpdir/assigned"; \
assigned=0; \
shard=0; \
while [ "$$shard" -lt "$(INTEGRATION_TEST_SHARDS)" ]; do \
	shard_tests="$$(printf '%s\n' "$$integration_tests" | awk -v shard="$$shard" -v shards="$(INTEGRATION_TEST_SHARDS)" \
		'((NR - 1) % shards) == shard { printf "%s%s", separator, $$0; separator = "|" } END { print "" }')"; \
	if [ -z "$$shard_tests" ]; then echo "ERROR: integration test shard $$shard is empty" >&2; exit 1; fi; \
	shard_count="$$(printf '%s\n' "$$shard_tests" | awk -F '|' '{ print NF }')"; \
	assigned=$$((assigned + shard_count)); \
	printf '%s\n' "$$shard_tests" | tr '|' '\n' >> "$$tmpdir/assigned"; \
	echo "==> integration tests: shard $$((shard + 1))/$(INTEGRATION_TEST_SHARDS) ($$shard_count tests, serialized, 10m timeout)"; \
	$(GO) test -tags=integration -race -count=1 -timeout 600s $(2) $(INTEGRATION_PACKAGE) -run="^($$shard_tests)$$"; \
	shard=$$((shard + 1)); \
done; \
sort "$$tmpdir/assigned" > "$$tmpdir/assigned.sorted"; \
if [ "$$assigned" -ne "$$integration_test_count" ] || ! cmp -s "$$tmpdir/discovered" "$$tmpdir/assigned.sorted"; then \
	echo "ERROR: integration shards did not assign every discovered test exactly once ($$assigned/$$integration_test_count)" >&2; \
	exit 1; \
fi; \
root_package="$$($(GO) list -tags=integration $(INTEGRATION_PACKAGE))"; \
child_packages="$$($(GO) list -tags=integration $(INTEGRATION_PACKAGE)/... | awk -v root="$$root_package" '$$0 != root')"; \
if [ -n "$$child_packages" ]; then \
	echo "==> integration descendant packages (once, serialized, 10m timeout)"; \
	$(GO) test -p=1 -tags=integration -race -count=1 -timeout 600s $(2) $$child_packages; \
fi; \
echo "==> integration assignment verified: $$integration_test_count tests exactly once across $(INTEGRATION_TEST_SHARDS) shards"
endef

# The script seam and WASM packages compile several real wasip1 guests. Running
# them in the same package fan-out as the rest of ./... can starve their bounded
# execution contexts under -race. Run the ordinary packages first, then serialize
# the focused script/WASM gate; every package remains covered by make test.
test: check-go
	@all_packages="$$($(GO) list ./...)" || exit 1; \
	script_package="$$($(GO) list $(SCRIPT_PACKAGE_ROOT))" || exit 1; \
	packages="$$(printf '%s\n' "$$all_packages" | awk -v script_package="$$script_package" \
		'$$0 != script_package && $$0 != script_package "/js" && $$0 != script_package "/wasm"')" || exit 1; \
	if [ -z "$$packages" ]; then echo "ERROR: ordinary test package list is empty" >&2; exit 1; fi; \
	$(GO) test $$packages -race -count=1 -timeout 5m
	$(GO) test -p=1 $(SCRIPT_JS_PACKAGES) -race -count=1 -timeout 5m
	@$(call RUN_WASM_TEST_SHARDS,)

test-verbose: check-go
	@all_packages="$$($(GO) list ./...)" || exit 1; \
	script_package="$$($(GO) list $(SCRIPT_PACKAGE_ROOT))" || exit 1; \
	packages="$$(printf '%s\n' "$$all_packages" | awk -v script_package="$$script_package" \
		'$$0 != script_package && $$0 != script_package "/js" && $$0 != script_package "/wasm"')" || exit 1; \
	if [ -z "$$packages" ]; then echo "ERROR: ordinary test package list is empty" >&2; exit 1; fi; \
	$(GO) test $$packages -race -count=1 -timeout 5m -v
	$(GO) test -p=1 $(SCRIPT_JS_PACKAGES) -race -count=1 -timeout 5m -v
	@$(call RUN_WASM_TEST_SHARDS,-v)

test-examples: check-go
	$(GO) test ./sdk/examples/ -race -count=1 -v -timeout 30s

# Concurrency stress suite. Gated behind the `concurrency` build tag so the
# default `make test` stays fast. Spec: .claude/specs/lua-concurrency-tests.md
test-concurrency: check-go
	$(GO) test -tags=concurrency ./backend/providers/local/ ./backend/providers/distributed/... -race -count=3 -timeout 5m

# Group entry-admission stress suite. Gated behind the `stress` build tag, so
# like test-concurrency it stays out of the default `make test`. It needs no
# external infrastructure (in-process local backend) and finishes in under a
# second; the tag exists to keep the default package list stable, not because
# the suite is expensive.
test-stress: check-go
	$(GO) test -tags=stress ./test/stress/... -race -count=1 -timeout 5m

# Focused script-engine regression suite. Package execution is serialized so
# the node-layer seam and wazero compilation do not compete for CPU under -race.
# WASM tests are auto-discovered and sharded to retain the 5-minute package
# timeout without omitting newly added top-level tests.
test-script-wasm: check-go
	$(GO) test -p=1 $(SCRIPT_JS_PACKAGES) -race -count=1 -timeout 5m
	@$(call RUN_WASM_TEST_SHARDS,)

# Generate one atomic coverage profile without reintroducing full-repository
# package fan-out for the heavyweight script/WASM packages. Atomic coverage can
# make the complete WASM test binary exceed its focused 5m package budget, so
# enumerate every runnable top-level test and split it deterministically across
# serial shards. Merge duplicate blocks by summing counters, validate the result,
# then atomically replace the requested output profile.
test-coverage: check-go
	@set -eu; \
	profile="$(COVERAGE_PROFILE)"; \
	if [ -z "$$profile" ]; then echo "ERROR: COVERAGE_PROFILE must not be empty" >&2; exit 1; fi; \
	profile_dir="$$(dirname "$$profile")"; \
	if [ ! -d "$$profile_dir" ]; then echo "ERROR: coverage output directory does not exist: $$profile_dir" >&2; exit 1; fi; \
	rm -f "$$profile"; \
	case "$(WASM_COVERAGE_SHARDS)" in ''|*[!0-9]*|0) echo "ERROR: WASM_COVERAGE_SHARDS must be a positive integer" >&2; exit 1;; esac; \
	tmpdir="$$(mktemp -d "$${TMPDIR:-/tmp}/xflow-coverage.XXXXXX")"; \
	merged=""; \
	cleanup() { rm -rf "$$tmpdir"; if [ -n "$$merged" ]; then rm -f "$$merged"; fi; }; \
	trap cleanup EXIT HUP INT TERM; \
	merged="$$(mktemp "$$profile_dir/.xflow-coverage.XXXXXX")"; \
	all_packages="$$($(GO) list ./...)"; \
	script_package="$$($(GO) list $(SCRIPT_PACKAGE_ROOT))"; \
	packages="$$(printf '%s\n' "$$all_packages" | awk -v script_package="$$script_package" \
		'$$0 != script_package && $$0 != script_package "/js" && $$0 != script_package "/wasm"')"; \
	if [ -z "$$packages" ]; then echo "ERROR: ordinary coverage package list is empty" >&2; exit 1; fi; \
	ordinary_profile="$$tmpdir/ordinary.out"; \
	script_profile="$$tmpdir/script-js.out"; \
	echo "==> coverage: ordinary packages (5m timeout)"; \
	$(GO) test $$packages -race -count=1 -timeout 5m -covermode=atomic -coverprofile="$$ordinary_profile"; \
	echo "==> coverage: script/JS packages (serialized, 5m timeout)"; \
	$(GO) test -p=1 $(SCRIPT_JS_PACKAGES) -race -count=1 -timeout 5m -covermode=atomic -coverprofile="$$script_profile"; \
	wasm_list="$$($(GO) test $(WASM_PACKAGE) -count=1 -list '^(Test|Fuzz|Example)')"; \
	wasm_tests="$$(printf '%s\n' "$$wasm_list" | awk '/^(Test|Fuzz|Example)/')"; \
	if [ -z "$$wasm_tests" ]; then echo "ERROR: no runnable WASM tests discovered" >&2; exit 1; fi; \
	wasm_test_count="$$(printf '%s\n' "$$wasm_tests" | awk 'NF { count++ } END { print count + 0 }')"; \
	assigned=0; \
	shard=0; \
	while [ "$$shard" -lt "$(WASM_COVERAGE_SHARDS)" ]; do \
		shard_tests="$$(printf '%s\n' "$$wasm_tests" | awk -v shard="$$shard" -v shards="$(WASM_COVERAGE_SHARDS)" \
			'((NR - 1) % shards) == shard { printf "%s%s", separator, $$0; separator = "|" } END { print "" }')"; \
		if [ -z "$$shard_tests" ]; then echo "ERROR: WASM coverage shard $$shard is empty" >&2; exit 1; fi; \
		shard_count="$$(printf '%s\n' "$$shard_tests" | awk -F '|' '{ print NF }')"; \
		assigned=$$((assigned + shard_count)); \
		echo "==> coverage: WASM shard $$((shard + 1))/$(WASM_COVERAGE_SHARDS) ($$shard_count tests, serialized, 5m timeout)"; \
		$(GO) test -p=1 $(WASM_PACKAGE) -run="^($$shard_tests)$$" -race -count=1 -timeout 5m \
			-covermode=atomic -coverprofile="$$tmpdir/wasm-$$shard.out"; \
		shard=$$((shard + 1)); \
	done; \
	if [ "$$assigned" -ne "$$wasm_test_count" ]; then \
		echo "ERROR: assigned $$assigned of $$wasm_test_count discovered WASM coverage tests" >&2; \
		exit 1; \
	fi; \
	if ! awk ' \
		function invalid() { failed = 1; exit 1 } \
		FNR == 1 { if ($$0 != "mode: atomic") invalid(); next } \
		NF != 3 { invalid() } \
		{ \
			location = $$1; statements = $$2; count = $$3; \
			if ((location in statement_count) && statement_count[location] != statements) invalid(); \
			if (!(location in statement_count)) { order[++blocks] = location; statement_count[location] = statements } \
			coverage_count[location] += count; \
		} \
		END { \
			if (failed) exit 1; \
			print "mode: atomic"; \
			for (i = 1; i <= blocks; i++) { \
				location = order[i]; \
				printf "%s %s %.0f\n", location, statement_count[location], coverage_count[location]; \
			} \
		}' "$$ordinary_profile" "$$script_profile" "$$tmpdir"/wasm-*.out > "$$merged"; then \
		echo "ERROR: coverage profiles are malformed or have incompatible blocks" >&2; \
		exit 1; \
	fi; \
	$(GO) tool cover -func="$$merged" > "$$tmpdir/summary.txt"; \
	mv "$$merged" "$$profile"; \
	merged=""; \
	echo "==> coverage summary"; \
	tail -n 1 "$$tmpdir/summary.txt"

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
	# Build-tagged files are excluded from the default build config, so
	# `go vet ./...` above cannot see them and `go build ./...` cannot either.
	# Without one pass per tag a tagged suite rots silently: test/stress
	# stopped compiling at 3144e02 (an unused import left behind when the
	# durable group suspend subsystem was removed) and nothing reported it,
	# because no Makefile target and no workflow ever built that package.
	# Add a pass here whenever you introduce a build tag.
	go vet -tags=integration ./test/integration/...
	go vet -tags=perf ./test/perf/...
	go vet -tags=soak ./test/soak/...
	go vet -tags=stress ./test/stress/...
	go vet -tags=concurrency ./backend/providers/local/ ./backend/providers/distributed/...

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

# Install the pinned protoc Go plugins (run once). Requires Go 1.25.x.
proto-tools: check-go
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

# Validate protoc and Go plugin versions. protoc 35.1 reports as "libprotoc 35.1"
# while generated Go headers render it as "protoc v7.35.1".
check-proto-tools:
	@protoc_version="$$(protoc --version 2>/dev/null || true)"; \
	if [ "$$protoc_version" != "libprotoc $(PROTOC_VERSION)" ]; then \
		echo "ERROR: protoc $$protoc_version, expected libprotoc $(PROTOC_VERSION)"; \
		exit 1; \
	fi; \
	gen_go_version="$$(protoc-gen-go --version 2>/dev/null | awk '{print $$2}')"; \
	case "$$gen_go_version" in \
		$(PROTOC_GEN_GO_VERSION)|$(PROTOC_GEN_GO_VERSION_NO_V)) ;; \
		*) echo "ERROR: protoc-gen-go $$gen_go_version, expected $(PROTOC_GEN_GO_VERSION)"; exit 1 ;; \
	esac; \
	gen_grpc_version="$$(protoc-gen-go-grpc --version 2>/dev/null | awk '{print $$2}')"; \
	case "$$gen_grpc_version" in \
		$(PROTOC_GEN_GO_GRPC_VERSION)|$(PROTOC_GEN_GO_GRPC_VERSION_NO_V)) ;; \
		*) echo "ERROR: protoc-gen-go-grpc $$gen_grpc_version, expected $(PROTOC_GEN_GO_GRPC_VERSION)"; exit 1 ;; \
	esac; \
	echo "Protobuf tools OK: $$protoc_version, protoc-gen-go $$gen_go_version, protoc-gen-go-grpc $$gen_grpc_version"

# Regenerate gRPC stubs from .proto sources. Requires pinned protoc + plugins on PATH.
proto: check-proto-tools
	protoc \
		--go_out=. --go_opt=module=github.com/xbcio/xflow \
		--go-grpc_out=. --go-grpc_opt=module=github.com/xbcio/xflow \
		service/protocol/runnerpb/runner.proto

# Non-destructive generated-code check: render into a temp dir and compare with
# checked-in stubs without touching service/protocol/runnerpb/*.pb.go.
proto-check: check-proto-tools
	@set -e; \
	tmp="$$(mktemp -d)"; \
	trap 'rm -rf "$$tmp"' EXIT; \
	protoc \
		--go_out="$$tmp" --go_opt=module=github.com/xbcio/xflow \
		--go-grpc_out="$$tmp" --go-grpc_opt=module=github.com/xbcio/xflow \
		service/protocol/runnerpb/runner.proto; \
	diff -u service/protocol/runnerpb/runner.pb.go "$$tmp/service/protocol/runnerpb/runner.pb.go"; \
	diff -u service/protocol/runnerpb/runner_grpc.pb.go "$$tmp/service/protocol/runnerpb/runner_grpc.pb.go"

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
env-ready:
	@echo "Waiting for Redis, MySQL, and Kafka readiness..."
	@bash test/env/wait-ready.sh
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
# R7: Redis, MySQL, and Kafka are all required so a missing dependency on any
# integration surface (sqlstore / distributed / queue) fails rather than skips.
test-integration: check-go
	@$(call RUN_INTEGRATION_TEST_SHARDS,optional,)

test-integration-required: check-go
	@$(call RUN_INTEGRATION_TEST_SHARDS,required,-json)

# test-g0-evidence-required is the single P0-G0 A0/A3 real-evidence entry
# (spec §9). It builds ONE fixed test binary, runs the A0/A3 required manifest
# directly from that binary (so os.Args[0] is stable and its digest can be
# recomputed), records raw ledger fragments stamped with real test-time source
# provenance, then runs the independent verifier. The verifier recomputes
# source provenance from the SAME binary path and the SAME git tree and compares
# to the recorder's stamped values; only on PASS does it atomically publish the
# final artifact + digest. A0/A3 do not depend on Kafka, so
# XFLOW_REQUIRE_KAFKA_INTEGRATION is intentionally NOT set (Kafka unavailability
# must not cause a skip). Redis (6380) + MySQL (3306) are required.
#
# JSON capture: the prebuilt test binary does not accept `go test`'s `-json`
# driver flag (only `-test.*` flags). `go tool test2json` runs the binary
# directly and converts its `-test.v` stream into the GoTestEvent JSON the
# verifier parses; test2json's exit code mirrors the binary's, so a non-zero
# suite exit fails the target before the verifier runs.
G0_TEST_BIN := test/integration/testdata/xflow-g0.test
G0_RAW_DIR  := test/integration/testdata/evidence
# G0_JSON is the `go test -json` stream. It MUST live OUTSIDE G0_RAW_DIR so
# MergeRawEnvelopes (which scans G0_RAW_DIR for *.json fragments) does not try
# to parse the go-test stream as an evidence fragment.
G0_JSON     := test/integration/testdata/g0-evidence.json
EVIDENCE_CANDIDATE_SHA ?=
export G0_TEST_BIN G0_RAW_DIR G0_JSON EVIDENCE_CANDIDATE_SHA

define G0_EVIDENCE_VALIDATE_PY
import hashlib
import json
import pathlib
import sys
import uuid


def fail(message):
    print("ERROR: G0 artifact validation failed: " + message, file=sys.stderr)
    raise SystemExit(1)


artifact_path = pathlib.Path(sys.argv[1])
digest_path = pathlib.Path(sys.argv[2])
candidate_sha = sys.argv[3]
try:
    artifact_bytes = artifact_path.read_bytes()
    document = json.loads(artifact_bytes)
except Exception as exc:
    fail("cannot parse final JSON: " + str(exc))
if not isinstance(document, dict):
    fail("top-level JSON must be an object")
if type(document.get("schema_version")) is not int or document["schema_version"] != 2:
    fail("schema_version must be integer 2")
run_id = document.get("run_id")
try:
    parsed_run_id = uuid.UUID(run_id)
except (AttributeError, TypeError, ValueError):
    fail("run_id must be a canonical RFC 4122 UUIDv4")
if parsed_run_id.version != 4 or parsed_run_id.variant != uuid.RFC_4122 or str(parsed_run_id) != run_id:
    fail("run_id must be a canonical RFC 4122 UUIDv4")
verification = document.get("verification")
if not isinstance(verification, dict):
    fail("verification must be an object")
for field in ("passed", "source_recomputed", "suite_recomputed"):
    if verification.get(field) is not True:
        fail("verification." + field + " must be true")
if verification.get("errors", []) != []:
    fail("verification.errors must be empty")
source = document.get("source")
if not isinstance(source, dict):
    fail("source must be an object")
if source.get("commit_sha") != candidate_sha:
    fail("source.commit_sha does not match candidate SHA")
if source.get("relevant_tree_clean") is not True:
    fail("source.relevant_tree_clean must be true")
suite = document.get("suite")
if not isinstance(suite, dict):
    fail("suite must be an object")
for field in ("exit_code", "skip_count", "dropped_runtime_events"):
    value = suite.get(field)
    if type(value) is not int or value != 0:
        fail("suite." + field + " must be integer zero")
for field in ("required_rows", "observed_rows"):
    value = suite.get(field)
    if type(value) is not int or value != 20:
        fail("suite." + field + " must be integer 20")
derived = document.get("derived_observations")
if not isinstance(derived, list) or len(derived) != 20:
    fail("derived_observations must contain exactly 20 rows")
expected_a0 = {
    "CommitThenFlushBeforeDelivery",
    "ReportAckLoss",
    "ReportRequestLoss",
    "QueueHandoff",
    "OSKillSIGKILL",
}
expected_a3_fixtures = {
    "transient_then_success",
    "transient_retry_exhausted",
    "permanent_no_retry",
    "business_error_no_retry",
    "error_port_retry_exhausted",
}
expected_a3_topologies = {"local", "server-runner", "cluster-durable"}
expected_a3 = {(fixture, topology) for fixture in expected_a3_fixtures for topology in expected_a3_topologies}
observed_a0 = set()
observed_a3 = set()
for index, row in enumerate(derived):
    if not isinstance(row, dict):
        fail("derived_observations[" + str(index) + "] must be an object")
    kind = row.get("kind")
    if kind == "a0_scenario":
        scenario = row.get("scenario")
        if not isinstance(scenario, str) or not scenario:
            fail("derived_observations[" + str(index) + "].scenario must be non-empty")
        if row.get("fixture") not in (None, "") or row.get("topology") not in (None, ""):
            fail("a0_scenario rows must not declare fixture or topology")
        if scenario in observed_a0:
            fail("duplicate a0_scenario row " + scenario)
        observed_a0.add(scenario)
    elif kind == "a3_matrix_row":
        fixture = row.get("fixture")
        topology = row.get("topology")
        if not isinstance(fixture, str) or not fixture or not isinstance(topology, str) or not topology:
            fail("derived_observations[" + str(index) + "] must declare fixture and topology")
        if row.get("scenario") not in (None, ""):
            fail("a3_matrix_row rows must not declare scenario")
        key = (fixture, topology)
        if key in observed_a3:
            fail("duplicate a3_matrix_row (" + fixture + ", " + topology + ")")
        observed_a3.add(key)
    else:
        fail("derived_observations[" + str(index) + "].kind is not a required evidence kind")
if observed_a0 != expected_a0:
    fail("a0_scenario set mismatch: missing=" + repr(sorted(expected_a0 - observed_a0)) + " extra=" + repr(sorted(observed_a0 - expected_a0)))
if observed_a3 != expected_a3:
    fail("a3_matrix_row set mismatch: missing=" + repr(sorted(expected_a3 - observed_a3)) + " extra=" + repr(sorted(observed_a3 - expected_a3)))
environment = document.get("environment")
if not isinstance(environment, dict):
    fail("environment must be an object")
for field in ("redis_version", "mysql_version"):
    value = environment.get(field)
    if not isinstance(value, str) or not value.strip():
        fail("environment." + field + " must be non-empty")
expected_digest = hashlib.sha256(artifact_bytes).hexdigest() + "\n"
try:
    actual_digest = digest_path.read_text(encoding="ascii")
except Exception as exc:
    fail("cannot read SHA-256 sidecar: " + str(exc))
if actual_digest != expected_digest:
    fail("SHA-256 sidecar does not exactly match independently recomputed JSON digest")
endef
export G0_EVIDENCE_VALIDATE_PY

test-g0-evidence-required: check-go
	@set -eu; \
	test_bin="$${G0_TEST_BIN:-}"; \
	raw_dir="$${G0_RAW_DIR:-}"; \
	json_path="$${G0_JSON:-}"; \
	expected_candidate_sha="$${EVIDENCE_CANDIDATE_SHA:-}"; \
	if [ -z "$$test_bin" ]; then echo "ERROR: G0_TEST_BIN must not be empty" >&2; exit 1; fi; \
	if [ -z "$$json_path" ]; then echo "ERROR: G0_JSON must not be empty" >&2; exit 1; fi; \
	if [ -z "$$raw_dir" ]; then echo "ERROR: G0_RAW_DIR must not be empty" >&2; exit 1; fi; \
	raw_dir_guard="$$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$$raw_dir")"; \
	if [ "$$raw_dir_guard" = "/" ]; then echo "ERROR: G0_RAW_DIR must not resolve to /" >&2; exit 1; fi; \
	candidate_sha="$$($(GIT) rev-parse --verify HEAD)" || { echo "ERROR: cannot resolve G0 candidate HEAD" >&2; exit 1; }; \
	if [ -z "$$candidate_sha" ]; then echo "ERROR: empty G0 candidate SHA" >&2; exit 1; fi; \
	if [ -n "$$expected_candidate_sha" ] && [ "$$candidate_sha" != "$$expected_candidate_sha" ]; then \
		echo "ERROR: G0 HEAD $$candidate_sha does not match EVIDENCE_CANDIDATE_SHA $$expected_candidate_sha" >&2; \
		exit 1; \
	fi; \
	pre_status="$$($(GIT) status --porcelain=v1 --untracked-files=all)" || { echo "ERROR: cannot inspect worktree before G0 evidence" >&2; exit 1; }; \
	if [ -n "$$pre_status" ]; then \
		echo "ERROR: worktree is dirty before G0 evidence (candidate $$candidate_sha)" >&2; \
		printf '%s\n' "$$pre_status" >&2; \
		exit 1; \
	fi; \
	echo "==> G0 candidate SHA: $$candidate_sha (full worktree clean)"; \
	set -a; if [ -f test/env/.env ]; then . ./test/env/.env; fi; set +a; \
	tmpdir="$$(mktemp -d "$${TMPDIR:-/tmp}/xflow-g0-evidence.XXXXXX")"; \
	run_test_bin="$$tmpdir/xflow-g0.test"; \
	run_raw_dir="$$tmpdir/raw"; \
	run_json="$$tmpdir/g0-events.json"; \
	verify_out="$$tmpdir/verified"; \
	binary_stage=""; json_stage=""; artifact_stage=""; digest_stage=""; \
	cleanup() { \
		rm -rf -- "$$tmpdir"; \
		for stage in "$$binary_stage" "$$json_stage" "$$artifact_stage" "$$digest_stage"; do \
			if [ -n "$$stage" ]; then rm -f -- "$$stage"; fi; \
		done; \
	}; \
	trap cleanup EXIT; \
	trap 'exit 129' HUP INT TERM; \
	mkdir -p -- "$$run_raw_dir" "$$verify_out"; \
	echo "==> building fixed test binary"; \
	$(GO) test -c -tags=integration -race -o "$$run_test_bin" ./test/integration/; \
	echo "==> running A0/A3 required manifest"; \
	rc=0; \
	XFLOW_REQUIRE_REDIS_INTEGRATION=1 \
	XFLOW_REQUIRE_MYSQL_INTEGRATION=1 \
	XFLOW_G0_EVIDENCE_RUN_ID=$$(python3 -c "import uuid; print(uuid.uuid4())" 2>/dev/null || uuidgen | tr 'A-F' 'a-f') \
	XFLOW_G0_EVIDENCE_RAW_DIR="$$run_raw_dir" \
	XFLOW_G0_TEST_BIN="$$run_test_bin" \
	$(GO) tool test2json -p github.com/xbcio/xflow/test/integration "$$run_test_bin" \
		-test.run '^TestA0FaultMatrix$$|^TestA0OSKillSIGKILLRecovery$$|^TestActionErrorParityMatrix$$|^TestHTTPActionErrorParity$$|^TestGRPCActionErrorParity$$|^TestScriptFunctionActionParity$$|^TestOnErrorActionParity$$|^TestDatabaseActionErrorParity$$' \
		-test.timeout=900s -test.v > "$$run_json" || rc=$$?; \
	if [ $$rc -ne 0 ]; then echo "test suite failed (exit $$rc)"; cat "$$run_json"; exit 1; fi; \
	echo "==> running independent verifier"; \
	$(GO) run ./test/integration/cmd/evidence-verify \
		-in "$$run_json" -raw "$$run_raw_dir" -binary "$$run_test_bin" -out "$$verify_out"; \
	artifact_list="$$(find "$$verify_out" -maxdepth 1 -type f -name 'evidence-*.json' ! -name '*.diagnostic.json' -print)"; \
	artifact_count="$$(printf '%s\n' "$$artifact_list" | awk 'NF { count++ } END { print count + 0 }')"; \
	if [ "$$artifact_count" -ne 1 ]; then \
		echo "ERROR: verifier produced $$artifact_count final G0 artifacts, expected exactly 1" >&2; \
		exit 1; \
	fi; \
	artifact="$$artifact_list"; \
	digest="$${artifact%.json}.sha256"; \
	if [ ! -s "$$digest" ]; then echo "ERROR: G0 verifier digest missing or empty: $$digest" >&2; exit 1; fi; \
	python3 -c "$$G0_EVIDENCE_VALIDATE_PY" "$$artifact" "$$digest" "$$candidate_sha"; \
	post_sha="$$($(GIT) rev-parse --verify HEAD)" || { echo "ERROR: cannot resolve HEAD after G0 evidence" >&2; exit 1; }; \
	if [ "$$post_sha" != "$$candidate_sha" ]; then \
		echo "ERROR: HEAD changed during G0 evidence (candidate=$$candidate_sha current=$$post_sha)" >&2; \
		exit 1; \
	fi; \
	post_status="$$($(GIT) status --porcelain=v1 --untracked-files=all)" || { echo "ERROR: cannot inspect worktree after G0 evidence" >&2; exit 1; }; \
	if [ -n "$$post_status" ]; then \
		echo "ERROR: worktree became dirty during G0 evidence (candidate $$candidate_sha)" >&2; \
		printf '%s\n' "$$post_status" >&2; \
		exit 1; \
	fi; \
	mkdir -p -- "$$raw_dir"; \
	artifact_name="$$(python3 -c 'import os,sys; print(os.path.basename(sys.argv[1]))' "$$artifact")"; \
	digest_name="$$(python3 -c 'import os,sys; print(os.path.basename(sys.argv[1]))' "$$digest")"; \
	artifact_dest="$$raw_dir/$$artifact_name"; \
	digest_dest="$$raw_dir/$$digest_name"; \
	binary_dir="$$(dirname "$$test_bin")"; json_dir="$$(dirname "$$json_path")"; \
	mkdir -p -- "$$binary_dir" "$$json_dir"; \
	binary_stage="$$(mktemp "$$binary_dir/.xflow-g0-bin.XXXXXX")"; \
	json_stage="$$(mktemp "$$json_dir/.xflow-g0-events.XXXXXX")"; \
	artifact_stage="$$(mktemp "$$raw_dir/.xflow-g0-artifact.XXXXXX")"; \
	digest_stage="$$(mktemp "$$raw_dir/.xflow-g0-digest.XXXXXX")"; \
	cp "$$run_test_bin" "$$binary_stage"; chmod 0755 "$$binary_stage"; \
	cp "$$run_json" "$$json_stage"; chmod 0644 "$$json_stage"; \
	cp "$$artifact" "$$artifact_stage"; chmod 0644 "$$artifact_stage"; \
	cp "$$digest" "$$digest_stage"; chmod 0644 "$$digest_stage"; \
	: "Publish the validated JSON last as the commit marker for this G0 set"; \
	rm -f -- "$$artifact_dest"; \
	mv -f -- "$$binary_stage" "$$test_bin"; binary_stage=""; \
	mv -f -- "$$json_stage" "$$json_path"; json_stage=""; \
	mv -f -- "$$digest_stage" "$$digest_dest"; digest_stage=""; \
	mv -f -- "$$artifact_stage" "$$artifact_dest"; artifact_stage=""; \
	echo "artifact: $$artifact_dest"; \
	echo "digest:   $$digest_dest"; \
	echo "==> G0 evidence artifact published"

# test-g1-evidence-required is the single focused P0-G1 evidence entry. It
# requires real Redis + MySQL and a stable, completely clean candidate worktree.
# The producer report and go-test event stream are first validated independently.
# Each successful run is then published as immutable, UUID-named generation
# files using create-only same-filesystem links. G1_MANIFEST is atomically
# replaced last and is the sole authoritative commit marker; G1_REPORT and
# G1_EVENTS are non-authoritative compatibility aliases. An interrupted
# publication therefore leaves the previous manifest and every generation it
# references intact.
# The target intentionally does not manage containers, migrations, or Kafka.
G1_REPORT ?= test/integration/testdata/g1_e2e_report.json
G1_EVENTS ?= test/integration/testdata/g1_e2e_events.json
G1_MANIFEST ?= test/integration/testdata/g1_e2e_manifest.json
export G1_REPORT G1_EVENTS G1_MANIFEST

define G1_EVIDENCE_VALIDATE_PY
import datetime
import json
import pathlib
import re
import sys
import uuid


SCHEMA_VERSION = 1
REQUIRED_METRICS = {
    "xflow_lease_acquire_duration_seconds",
    "xflow_audit_reconcile_scan_total",
    "xflow_audit_reconcile_settled_total",
}
REQUIRED_SPANS = {
    "xflow.workflow.execute",
    "xflow.task.dispatch",
    "xflow.task.execute",
    "xflow.task.report",
    "xflow.task.commit",
}
EXPECTED_AUTHZ_ROWS = [
    {"scenario": "workflow_submit_allow", "operation": "workflow.create", "route": "POST /v1/workflows/execute", "token": "full-A", "scope": "workflow", "expected": 200, "got": 200, "decision": "allow"},
    {"scenario": "workflow_submit_no_execution_scope_allow", "operation": "workflow.create", "route": "POST /v1/workflows/execute", "token": "noexec-A", "scope": "workflow", "expected": 200, "got": 200, "decision": "allow"},
    {"scenario": "workflow_entry_invoke_allow", "operation": "workflow.create", "route": "POST /v1/workflows/execute", "token": "full-A", "scope": "workflow", "expected": 200, "got": 200, "decision": "allow"},
    {"scenario": "execution_inspect_allow", "operation": "execution.read", "route": "GET /v1/executions/{id}", "token": "full-A", "scope": "execution", "expected": 200, "got": 200, "decision": "allow"},
    {"scenario": "execution_inspect_scope_deny", "operation": "execution.read", "route": "GET /v1/executions/{id}", "token": "noexec-A", "scope": "", "expected": 403, "got": 403, "decision": "deny"},
    {"scenario": "execution_inspect_cross_namespace_idor", "operation": "execution.read", "route": "GET /v1/executions/{id}", "token": "full-B (cross-namespace)", "scope": "execution", "expected": 404, "got": 404, "decision": "deny"},
    {"scenario": "execution_signal_allow", "operation": "execution.signal", "route": "POST /v1/executions/{id}/signals", "token": "full-A", "scope": "execution", "expected": 200, "got": 200, "decision": "allow"},
    {"scenario": "execution_signal_scope_deny", "operation": "execution.signal", "route": "POST /v1/executions/{id}/signals", "token": "noexec-A", "scope": "", "expected": 403, "got": 403, "decision": "deny"},
    {"scenario": "execution_revoke_allow", "operation": "execution.revoke", "route": "DELETE /v1/executions/{id}/signals/{name}", "token": "full-A", "scope": "execution", "expected": 200, "got": 200, "decision": "allow"},
    {"scenario": "execution_revoke_scope_deny", "operation": "execution.revoke", "route": "DELETE /v1/executions/{id}/signals/{name}", "token": "noexec-A", "scope": "", "expected": 403, "got": 403, "decision": "deny"},
    {"scenario": "execution_cancel_allow", "operation": "execution.cancel", "route": "POST /v1/executions/{id}/cancel", "token": "full-A", "scope": "execution", "expected": 200, "got": 200, "decision": "allow"},
    {"scenario": "execution_cancel_scope_deny", "operation": "execution.cancel", "route": "POST /v1/executions/{id}/cancel", "token": "noexec-A", "scope": "", "expected": 403, "got": 403, "decision": "deny"},
]


def fail(message):
    print("ERROR: G1 report validation failed: " + message, file=sys.stderr)
    raise SystemExit(1)


def exact_keys(value, expected, field):
    if not isinstance(value, dict):
        fail(field + " must be an object")
    actual = set(value)
    expected = set(expected)
    if actual != expected:
        fail(field + " fields mismatch: missing=" + repr(sorted(expected - actual)) + " extra=" + repr(sorted(actual - expected)))
    return value


def nonempty(value, field):
    if not isinstance(value, str) or not value.strip():
        fail(field + " must be a non-empty string")
    return value


def integer(document, field, minimum=None, maximum=None, prefix=""):
    value = document.get(field)
    name = prefix + field
    if type(value) is not int:
        fail(name + " must be an integer")
    if minimum is not None and value < minimum:
        fail(name + " must be >= " + str(minimum))
    if maximum is not None and value > maximum:
        fail(name + " must be <= " + str(maximum))
    return value


def valid_sha(value, field):
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{40}", value) is None:
        fail(field + " must be a full lowercase 40-hex commit SHA")
    return value


def valid_run_id(value, field):
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, TypeError, ValueError):
        fail(field + " must be a canonical UUIDv4")
    if parsed.version != 4 or str(parsed) != value:
        fail(field + " must be a canonical UUIDv4")
    return value


def exact_string_set(value, expected, field):
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        fail(field + " must be an array of non-empty strings")
    if len(value) != len(expected) or set(value) != set(expected):
        fail(field + " must contain exactly " + repr(sorted(expected)) + " without duplicates")


report_path = pathlib.Path(sys.argv[1])
candidate_sha = valid_sha(sys.argv[2], "candidate SHA")
run_id = valid_run_id(sys.argv[3], "expected run ID")
try:
    document = json.loads(report_path.read_bytes())
except Exception as exc:
    fail("cannot parse structured report: " + str(exc))
exact_keys(document, {
    "schema_version", "run_id", "generated_at", "go_version", "os",
    "commit_sha", "full_worktree_clean", "redis_addr", "mysql_dsn_host",
    "runtime", "authz_matrix", "trace_graph", "approval_dag",
    "audit_reconcile", "dead_letter", "metrics_scrape", "idempotency_report",
}, "top-level report")
if document.get("schema_version") != SCHEMA_VERSION:
    fail("schema_version must equal " + str(SCHEMA_VERSION))
if document.get("run_id") != run_id:
    fail("run_id does not match the Make-issued run ID")
valid_run_id(document.get("run_id"), "run_id")
if document.get("commit_sha") != candidate_sha:
    fail("commit_sha does not match candidate SHA")
valid_sha(document.get("commit_sha"), "commit_sha")
if document.get("full_worktree_clean") is not True:
    fail("full_worktree_clean must be true")
generated_at = document.get("generated_at")
try:
    datetime.datetime.strptime(generated_at, "%Y-%m-%dT%H:%M:%SZ")
except (TypeError, ValueError):
    fail("generated_at must be UTC RFC3339 with a trailing Z")
if document.get("go_version") != "go1.25.0":
    fail("go_version must equal go1.25.0")
os_name = nonempty(document.get("os"), "os")
if "/" not in os_name or os_name.startswith("/") or os_name.endswith("/"):
    fail("os must identify GOOS/GOARCH")

runtime = exact_keys(document.get("runtime"), {
    "redis_endpoint", "redis_version", "mysql_network", "mysql_endpoint",
    "mysql_database", "mysql_server_version",
}, "runtime")
for field in runtime:
    nonempty(runtime.get(field), "runtime." + field)
if document.get("redis_addr") != runtime.get("redis_endpoint"):
    fail("redis_addr must exactly match runtime.redis_endpoint")
if document.get("mysql_dsn_host") != runtime.get("mysql_endpoint"):
    fail("mysql_dsn_host must exactly match runtime.mysql_endpoint")

rows = document.get("authz_matrix")
if not isinstance(rows, list) or len(rows) != len(EXPECTED_AUTHZ_ROWS):
    fail("authz_matrix must contain exactly 12 required rows")
expected_by_scenario = {row["scenario"]: row for row in EXPECTED_AUTHZ_ROWS}
observed_by_scenario = {}
for index, row in enumerate(rows):
    exact_keys(row, EXPECTED_AUTHZ_ROWS[0].keys(), "authz_matrix[" + str(index) + "]")
    if type(row.get("expected")) is not int or type(row.get("got")) is not int:
        fail("authz_matrix[" + str(index) + "] expected/got must be integers")
    scenario = nonempty(row.get("scenario"), "authz_matrix[" + str(index) + "].scenario")
    if scenario in observed_by_scenario:
        fail("authz_matrix has duplicate scenario " + scenario)
    observed_by_scenario[scenario] = row
if set(observed_by_scenario) != set(expected_by_scenario):
    fail("authz_matrix scenario set does not exactly match the 12-row contract")
for scenario, expected in expected_by_scenario.items():
    if observed_by_scenario[scenario] != expected:
        fail("authz_matrix scenario " + scenario + " metadata does not match the required contract")

trace = exact_keys(document.get("trace_graph"), {
    "spans_present", "one_trace_id", "dispatch_parented_to_submit",
    "commit_parented_to_report", "namespace_a_one_trace_id",
    "namespace_a_dispatch_parented_to_submit",
    "namespace_a_commit_parented_to_report", "cross_namespace_carrier_isolated",
}, "trace_graph")
exact_string_set(trace.get("spans_present"), REQUIRED_SPANS, "trace_graph.spans_present")
for field in set(trace) - {"spans_present"}:
    if trace.get(field) is not True:
        fail("trace_graph." + field + " must be true")

approval = exact_keys(document.get("approval_dag"), {
    "multi_signal_quorum", "timer_fired", "cancel", "cyclic_reset", "repeat_signal_409",
}, "approval_dag")
for field, value in approval.items():
    if value != "pass":
        fail("approval_dag." + field + " must equal pass")

audit = exact_keys(document.get("audit_reconcile"), {
    "admission_rows", "outcome_rows", "reconciled_by_worker",
    "idempotent_outcome_appends", "sweeps_to_settle", "fault_matrix_pass",
}, "audit_reconcile")
admission_rows = integer(audit, "admission_rows", 1, prefix="audit_reconcile.")
outcome_rows = integer(audit, "outcome_rows", 1, prefix="audit_reconcile.")
reconciled_rows = integer(audit, "reconciled_by_worker", 1, prefix="audit_reconcile.")
integer(audit, "sweeps_to_settle", 1, 256, prefix="audit_reconcile.")
if outcome_rows != admission_rows or reconciled_rows != admission_rows:
    fail("audit_reconcile row counts must be equal and non-zero")
if audit.get("idempotent_outcome_appends") is not True or audit.get("fault_matrix_pass") is not True:
    fail("audit_reconcile idempotency and fault matrix must pass")

dead_letter = exact_keys(document.get("dead_letter"), {
    "seeded", "replay_outcome", "receipt_audit_id_set", "durable_projection_rows",
}, "dead_letter")
if dead_letter.get("seeded") is not True or dead_letter.get("replay_outcome") != "replayed" or dead_letter.get("receipt_audit_id_set") is not True:
    fail("dead_letter seed, replay, and receipt checks must pass")
if integer(dead_letter, "durable_projection_rows", prefix="dead_letter.") != 1:
    fail("dead_letter.durable_projection_rows must equal 1")

metrics = exact_keys(document.get("metrics_scrape"), {
    "scraped", "counters_observed", "required_families", "observed_families", "missing_families",
}, "metrics_scrape")
if metrics.get("scraped") is not True:
    fail("metrics_scrape.scraped must be true")
for field in ("counters_observed", "required_families", "observed_families"):
    exact_string_set(metrics.get(field), REQUIRED_METRICS, "metrics_scrape." + field)
if metrics.get("missing_families") != []:
    fail("metrics_scrape.missing_families must be an empty array")

idempotency = exact_keys(document.get("idempotency_report"), {
    "repeat_signal_outcome", "duplicate_report_outcome",
    "handler_side_effects_assertion", "independent_executions_for_same_def",
    "invocation_level_idempotency_key", "handler_invocations", "business_rows",
    "idempotency_key", "host_fence_outcome",
}, "idempotency_report")
if idempotency.get("repeat_signal_outcome") != "409":
    fail("idempotency_report.repeat_signal_outcome must equal 409")
fence_outcomes = {"duplicate_terminal", "stale_token", "execution_inactive"}
host_fence = idempotency.get("host_fence_outcome")
if host_fence not in fence_outcomes or idempotency.get("duplicate_report_outcome") != host_fence:
    fail("idempotency_report host fence outcomes must match an allowed terminal fence")
if idempotency.get("independent_executions_for_same_def") is not True:
    fail("idempotency_report.independent_executions_for_same_def must be true")
if idempotency.get("invocation_level_idempotency_key") != "not implemented (out of G1 scope)":
    fail("idempotency_report.invocation_level_idempotency_key has an unexpected claim")
if idempotency.get("idempotency_key") != "execution_id+node_name (UNIQUE constraint)":
    fail("idempotency_report.idempotency_key has an unexpected contract")
handler_invocations = integer(idempotency, "handler_invocations", 2, prefix="idempotency_report.")
business_rows = integer(idempotency, "business_rows", prefix="idempotency_report.")
if business_rows != 1:
    fail("idempotency_report.business_rows must equal 1")
expected_assertion = (
    "handler_invocations=" + str(handler_invocations) + ", business_rows=" + str(business_rows) +
    " (idempotent receiver keyed by execution_id+node_name; host fence=" + host_fence + ")"
)
if idempotency.get("handler_side_effects_assertion") != expected_assertion:
    fail("idempotency_report.handler_side_effects_assertion must exactly match measured values")
endef
export G1_EVIDENCE_VALIDATE_PY

define G1_EVIDENCE_EVENTS_VALIDATE_PY
import json
import pathlib
import sys


def fail(message):
    print("ERROR: G1 event validation failed: " + message, file=sys.stderr)
    raise SystemExit(1)


events_path = pathlib.Path(sys.argv[1])
expected_package = sys.argv[2]
run_id = sys.argv[3]
test_name = "TestG1ProductionE2E"
marker = "xflow-g1-evidence-run-id=" + run_id
try:
    lines = events_path.read_text(encoding="utf-8").splitlines()
except Exception as exc:
    fail("cannot read event stream: " + str(exc))
if not lines:
    fail("event stream is empty")
test_runs = []
test_passes = []
package_passes = []
marker_positions = []
for line_number, line in enumerate(lines, 1):
    if not line.strip():
        continue
    try:
        event = json.loads(line)
    except Exception as exc:
        fail("line " + str(line_number) + " is not JSON: " + str(exc))
    if not isinstance(event, dict):
        fail("line " + str(line_number) + " must be a JSON object")
    if event.get("Package") != expected_package:
        fail("line " + str(line_number) + " Package does not match " + expected_package)
    action = event.get("Action")
    if not isinstance(action, str) or not action:
        fail("line " + str(line_number) + " Action must be non-empty")
    if action in {"skip", "fail"}:
        fail("line " + str(line_number) + " contains forbidden Action=" + action)
    test = event.get("Test")
    if test == test_name and action == "run":
        test_runs.append(line_number)
    if test == test_name and action == "pass":
        test_passes.append(line_number)
    if test in (None, "") and action == "pass":
        package_passes.append(line_number)
    output = event.get("Output", "")
    if output is not None and not isinstance(output, str):
        fail("line " + str(line_number) + " Output must be a string when present")
    marker_count = output.count(marker) if isinstance(output, str) else 0
    if marker_count:
        if test != test_name or action != "output":
            fail("run ID marker must be an output event for " + test_name)
        marker_positions.extend([line_number] * marker_count)
if len(test_runs) != 1 or len(test_passes) != 1:
    fail(test_name + " must have exactly one run and one pass event")
if len(package_passes) != 1:
    fail("package must have exactly one package-level pass event")
if len(marker_positions) != 1:
    fail("run ID marker must occur exactly once")
if not (test_runs[0] < marker_positions[0] < test_passes[0] < package_passes[0]):
    fail("run, marker, test pass, and package pass events are out of order")
endef
export G1_EVIDENCE_EVENTS_VALIDATE_PY

define G1_EVIDENCE_MANIFEST_PY
import hashlib
import json
import pathlib
import re
import sys
import uuid


KIND = "xflow.g1-evidence"
SCHEMA_VERSION = 1
BINDING_VERSION = "xflow-g1-evidence-v2"


def fail(message):
    print("ERROR: G1 manifest generation failed: " + message, file=sys.stderr)
    raise SystemExit(1)


def safe_basename(value, field):
    if not isinstance(value, str) or not value or value in {".", ".."} or "/" in value or "\\" in value or pathlib.Path(value).name != value:
        fail(field + " must be a safe relative basename")
    return value


def valid_sha(value):
    if re.fullmatch(r"[0-9a-f]{40}", value) is None:
        fail("candidate SHA must be full lowercase 40-hex")


def valid_run_id(value):
    try:
        parsed = uuid.UUID(value)
    except (TypeError, ValueError):
        fail("run ID must be a canonical UUIDv4")
    if parsed.version != 4 or str(parsed) != value:
        fail("run ID must be a canonical UUIDv4")


def artifact(path, name):
    raw = pathlib.Path(path).read_bytes()
    return {"path": safe_basename(name, "artifact path"), "size": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


output, candidate_sha, run_id, report_path, events_path, report_name, events_name, package = sys.argv[1:]
valid_sha(candidate_sha)
valid_run_id(run_id)
report = artifact(report_path, report_name)
events = artifact(events_path, events_name)
if report["path"] == events["path"]:
    fail("report and events basenames must differ")
bound = {
    "kind": KIND,
    "schema_version": SCHEMA_VERSION,
    "run_id": run_id,
    "candidate_sha": candidate_sha,
    "test": {"package": package, "name": "TestG1ProductionE2E", "exit_code": 0},
    "report": report,
    "events": events,
}
binding_material = dict(bound)
binding_material["binding_version"] = BINDING_VERSION
canonical = json.dumps(binding_material, sort_keys=True, separators=(",", ":")).encode("utf-8")
document = dict(bound)
document["binding"] = {"version": BINDING_VERSION, "sha256": hashlib.sha256(canonical).hexdigest()}
pathlib.Path(output).write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
endef
export G1_EVIDENCE_MANIFEST_PY

define G1_EVIDENCE_MANIFEST_VALIDATE_PY
import hashlib
import json
import pathlib
import re
import sys
import uuid


KIND = "xflow.g1-evidence"
SCHEMA_VERSION = 1
BINDING_VERSION = "xflow-g1-evidence-v2"


def fail(message):
    print("ERROR: G1 manifest validation failed: " + message, file=sys.stderr)
    raise SystemExit(1)


def exact_keys(value, expected, field):
    if not isinstance(value, dict) or set(value) != set(expected):
        fail(field + " fields do not match the manifest schema")
    return value


def safe_basename(value, field):
    if not isinstance(value, str) or not value or value in {".", ".."} or "/" in value or "\\" in value or pathlib.Path(value).name != value:
        fail(field + " must be a safe relative basename")
    return value


def valid_sha(value, field, length):
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{" + str(length) + r"}", value) is None:
        fail(field + " has an invalid lowercase hexadecimal digest")


def valid_run_id(value):
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, TypeError, ValueError):
        fail("run_id must be a canonical UUIDv4")
    if parsed.version != 4 or str(parsed) != value:
        fail("run_id must be a canonical UUIDv4")


def validate_artifact(root, descriptor, field):
    exact_keys(descriptor, {"path", "size", "sha256"}, field)
    name = safe_basename(descriptor.get("path"), field + ".path")
    size = descriptor.get("size")
    if type(size) is not int or size < 0:
        fail(field + ".size must be a non-negative integer")
    valid_sha(descriptor.get("sha256"), field + ".sha256", 64)
    candidate = root / name
    if candidate.is_symlink() or not candidate.is_file():
        fail(field + " generation file is missing or not a regular file")
    resolved = candidate.resolve(strict=True)
    if resolved.parent != root:
        fail(field + " generation file escapes the manifest directory")
    raw = resolved.read_bytes()
    if len(raw) != size:
        fail(field + " size mismatch")
    if hashlib.sha256(raw).hexdigest() != descriptor.get("sha256"):
        fail(field + " digest mismatch")
    return resolved


manifest_path = pathlib.Path(sys.argv[1])
root = pathlib.Path(sys.argv[2]).resolve()
expected_candidate = sys.argv[3]
expected_run_id = sys.argv[4]
expected_package = sys.argv[5]
try:
    document = json.loads(manifest_path.read_bytes())
except Exception as exc:
    fail("cannot parse manifest: " + str(exc))
exact_keys(document, {"kind", "schema_version", "run_id", "candidate_sha", "test", "report", "events", "binding"}, "manifest")
if document.get("kind") != KIND or document.get("schema_version") != SCHEMA_VERSION:
    fail("kind or schema_version mismatch")
valid_run_id(document.get("run_id"))
if document.get("run_id") != expected_run_id:
    fail("run_id does not match expected run")
valid_sha(document.get("candidate_sha"), "candidate_sha", 40)
if document.get("candidate_sha") != expected_candidate:
    fail("candidate_sha does not match expected candidate")
test = exact_keys(document.get("test"), {"package", "name", "exit_code"}, "test")
if test != {"package": expected_package, "name": "TestG1ProductionE2E", "exit_code": 0}:
    fail("test identity or exit code mismatch")
report_path = validate_artifact(root, document.get("report"), "report")
events_path = validate_artifact(root, document.get("events"), "events")
if report_path == events_path:
    fail("report and events must reference different generation files")
binding = exact_keys(document.get("binding"), {"version", "sha256"}, "binding")
if binding.get("version") != BINDING_VERSION:
    fail("binding.version mismatch")
valid_sha(binding.get("sha256"), "binding.sha256", 64)
bound = {key: document[key] for key in ("kind", "schema_version", "run_id", "candidate_sha", "test", "report", "events")}
bound["binding_version"] = BINDING_VERSION
canonical = json.dumps(bound, sort_keys=True, separators=(",", ":")).encode("utf-8")
if hashlib.sha256(canonical).hexdigest() != binding.get("sha256"):
    fail("binding digest mismatch")
print(str(report_path))
print(str(events_path))
endef
export G1_EVIDENCE_MANIFEST_VALIDATE_PY

test-g1-evidence-required: check-go
	@set -eu; \
	set -a; [ -f test/env/.env ] && . ./test/env/.env; set +a; \
	: "$${XFLOW_TEST_REDIS_ADDR:=localhost:$${REDIS_PORT:-6379}}"; \
	export XFLOW_TEST_REDIS_ADDR; \
	export XFLOW_REQUIRE_REDIS_INTEGRATION=1; \
	export XFLOW_REQUIRE_MYSQL_INTEGRATION=1; \
	unset XFLOW_REQUIRE_KAFKA_INTEGRATION; \
	final_report="$${G1_REPORT:-}"; \
	final_events="$${G1_EVENTS:-}"; \
	final_manifest="$${G1_MANIFEST:-}"; \
	expected_candidate_sha="$${EVIDENCE_CANDIDATE_SHA:-}"; \
	if [ -z "$$final_report" ]; then echo "ERROR: G1_REPORT must not be empty" >&2; exit 1; fi; \
	if [ -z "$$final_events" ]; then echo "ERROR: G1_EVENTS must not be empty" >&2; exit 1; fi; \
	if [ -z "$$final_manifest" ]; then echo "ERROR: G1_MANIFEST must not be empty" >&2; exit 1; fi; \
	report_key="$$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$$final_report")"; \
	events_key="$$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$$final_events")"; \
	manifest_key="$$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$$final_manifest")"; \
	if [ "$$report_key" = "$$events_key" ] || [ "$$report_key" = "$$manifest_key" ] || [ "$$events_key" = "$$manifest_key" ]; then \
		echo "ERROR: G1_REPORT, G1_EVENTS, and G1_MANIFEST must be different paths" >&2; \
		exit 1; \
	fi; \
	report_dir_key="$$(python3 -c 'import os,sys; print(os.path.realpath(os.path.dirname(sys.argv[1]) or "."))' "$$final_report")"; \
	events_dir_key="$$(python3 -c 'import os,sys; print(os.path.realpath(os.path.dirname(sys.argv[1]) or "."))' "$$final_events")"; \
	manifest_dir_key="$$(python3 -c 'import os,sys; print(os.path.realpath(os.path.dirname(sys.argv[1]) or "."))' "$$final_manifest")"; \
	if [ "$$report_dir_key" != "$$events_dir_key" ] || [ "$$report_dir_key" != "$$manifest_dir_key" ]; then \
		echo "ERROR: G1_REPORT, G1_EVENTS, and G1_MANIFEST must share one canonical directory" >&2; \
		exit 1; \
	fi; \
	candidate_sha="$$($(GIT) rev-parse --verify HEAD)" || { echo "ERROR: cannot resolve G1 candidate HEAD" >&2; exit 1; }; \
	python3 -c 'import re,sys; raise SystemExit(0 if re.fullmatch(r"[0-9a-f]{40}", sys.argv[1]) else 1)' "$$candidate_sha" || { echo "ERROR: G1 candidate SHA must be full lowercase 40-hex" >&2; exit 1; }; \
	if [ -n "$$expected_candidate_sha" ] && [ "$$candidate_sha" != "$$expected_candidate_sha" ]; then \
		echo "ERROR: G1 HEAD $$candidate_sha does not match EVIDENCE_CANDIDATE_SHA $$expected_candidate_sha" >&2; \
		exit 1; \
	fi; \
	pre_status="$$($(GIT) status --porcelain=v1 --untracked-files=all)" || { echo "ERROR: cannot inspect worktree before G1 evidence" >&2; exit 1; }; \
	if [ -n "$$pre_status" ]; then \
		echo "ERROR: worktree is dirty before G1 evidence (candidate $$candidate_sha)" >&2; \
		printf '%s\n' "$$pre_status" >&2; \
		exit 1; \
	fi; \
	run_id="$$(python3 -c 'import uuid; print(uuid.uuid4())')"; \
	echo "==> G1 candidate SHA: $$candidate_sha (full worktree clean)"; \
	echo "==> G1 evidence run ID: $$run_id"; \
	tmpdir="$$(mktemp -d "$${TMPDIR:-/tmp}/xflow-g1-evidence.XXXXXX")"; \
	report_stage=""; events_stage=""; manifest_stage=""; alias_report_stage=""; alias_events_stage=""; \
	cleanup() { \
		rm -rf -- "$$tmpdir"; \
		for stage in "$$report_stage" "$$events_stage" "$$manifest_stage" "$$alias_report_stage" "$$alias_events_stage"; do \
			if [ -n "$$stage" ]; then rm -f -- "$$stage"; fi; \
		done; \
	}; \
	trap cleanup EXIT; \
	trap 'exit 129' HUP INT TERM; \
	events="$$tmpdir/g1-test-events.json"; \
	report="$$tmpdir/g1-e2e-report.json"; \
	integration_package="$$($(GO) list -tags=integration $(INTEGRATION_PACKAGE))"; \
	if [ -z "$$integration_package" ]; then echo "ERROR: empty G1 integration package identity" >&2; exit 1; fi; \
	echo "==> G1 required evidence: TestG1ProductionE2E (real Redis + MySQL)"; \
	rc=0; \
	XFLOW_G1_REPORT_PATH="$$report" XFLOW_G1_CANDIDATE_SHA="$$candidate_sha" XFLOW_G1_RUN_ID="$$run_id" \
	$(GO) test -json -tags=integration -race -count=1 -timeout 600s \
		-run '^TestG1ProductionE2E$$' $(INTEGRATION_PACKAGE) > "$$events" || rc=$$?; \
	cat "$$events"; \
	if [ "$$rc" -ne 0 ]; then \
		echo "ERROR: G1 required evidence test failed (exit $$rc)" >&2; \
		exit "$$rc"; \
	fi; \
	if [ ! -s "$$report" ]; then \
		echo "ERROR: G1 temporary structured report missing or empty" >&2; \
		exit 1; \
	fi; \
	python3 -c "$$G1_EVIDENCE_EVENTS_VALIDATE_PY" "$$events" "$$integration_package" "$$run_id"; \
	python3 -c "$$G1_EVIDENCE_VALIDATE_PY" "$$report" "$$candidate_sha" "$$run_id"; \
	post_sha="$$($(GIT) rev-parse --verify HEAD)" || { echo "ERROR: cannot resolve HEAD after G1 evidence" >&2; exit 1; }; \
	if [ "$$post_sha" != "$$candidate_sha" ]; then \
		echo "ERROR: HEAD changed during G1 evidence (candidate=$$candidate_sha current=$$post_sha)" >&2; \
		exit 1; \
	fi; \
	post_status="$$($(GIT) status --porcelain=v1 --untracked-files=all)" || { echo "ERROR: cannot inspect worktree after G1 evidence" >&2; exit 1; }; \
	if [ -n "$$post_status" ]; then \
		echo "ERROR: worktree became dirty during G1 evidence (candidate $$candidate_sha)" >&2; \
		printf '%s\n' "$$post_status" >&2; \
		exit 1; \
	fi; \
	artifact_dir="$$(dirname "$$final_manifest")"; \
	mkdir -p -- "$$artifact_dir"; \
	generation_prefix="g1-evidence-$$run_id"; \
	generation_report_name="$$generation_prefix.report.json"; \
	generation_events_name="$$generation_prefix.events.json"; \
	generation_report="$$artifact_dir/$$generation_report_name"; \
	generation_events="$$artifact_dir/$$generation_events_name"; \
	if [ -e "$$generation_report" ] || [ -e "$$generation_events" ]; then \
		echo "ERROR: immutable G1 generation already exists for run $$run_id" >&2; \
		exit 1; \
	fi; \
	report_stage="$$(mktemp "$$artifact_dir/.xflow-g1-generation-report.XXXXXX")"; \
	events_stage="$$(mktemp "$$artifact_dir/.xflow-g1-generation-events.XXXXXX")"; \
	manifest_stage="$$(mktemp "$$artifact_dir/.xflow-g1-manifest.XXXXXX")"; \
	alias_report_stage="$$(mktemp "$$artifact_dir/.xflow-g1-report-alias.XXXXXX")"; \
	alias_events_stage="$$(mktemp "$$artifact_dir/.xflow-g1-events-alias.XXXXXX")"; \
	cp "$$report" "$$report_stage"; chmod 0644 "$$report_stage"; \
	cp "$$events" "$$events_stage"; chmod 0644 "$$events_stage"; \
	cp "$$report" "$$alias_report_stage"; chmod 0644 "$$alias_report_stage"; \
	cp "$$events" "$$alias_events_stage"; chmod 0644 "$$alias_events_stage"; \
	python3 -c 'import os,sys; os.link(sys.argv[1], sys.argv[2]); os.unlink(sys.argv[1])' "$$report_stage" "$$generation_report"; report_stage=""; \
	python3 -c 'import os,sys; os.link(sys.argv[1], sys.argv[2]); os.unlink(sys.argv[1])' "$$events_stage" "$$generation_events"; events_stage=""; \
	python3 -c "$$G1_EVIDENCE_MANIFEST_PY" "$$manifest_stage" "$$candidate_sha" "$$run_id" \
		"$$generation_report" "$$generation_events" "$$generation_report_name" "$$generation_events_name" "$$integration_package"; \
	chmod 0644 "$$manifest_stage"; \
	python3 -c "$$G1_EVIDENCE_MANIFEST_VALIDATE_PY" "$$manifest_stage" "$$artifact_dir" "$$candidate_sha" "$$run_id" "$$integration_package" >/dev/null; \
	mv -f -- "$$alias_report_stage" "$$final_report"; alias_report_stage=""; \
	mv -f -- "$$alias_events_stage" "$$final_events"; alias_events_stage=""; \
	: "Publish the manifest last; never remove the previous commit marker first"; \
	mv -f -- "$$manifest_stage" "$$final_manifest"; manifest_stage=""; \
	generation_paths="$$(python3 -c "$$G1_EVIDENCE_MANIFEST_VALIDATE_PY" "$$final_manifest" "$$artifact_dir" "$$candidate_sha" "$$run_id" "$$integration_package")"; \
	published_report="$$(printf '%s\n' "$$generation_paths" | sed -n '1p')"; \
	published_events="$$(printf '%s\n' "$$generation_paths" | sed -n '2p')"; \
	python3 -c "$$G1_EVIDENCE_VALIDATE_PY" "$$published_report" "$$candidate_sha" "$$run_id"; \
	python3 -c "$$G1_EVIDENCE_EVENTS_VALIDATE_PY" "$$published_events" "$$integration_package" "$$run_id"; \
	echo "==> G1 immutable report published: $$published_report"; \
	echo "==> G1 immutable events published: $$published_events"; \
	echo "==> G1 report alias published: $$final_report"; \
	echo "==> G1 event alias published: $$final_events"; \
	echo "==> G1 manifest published: $$final_manifest"; \
	echo "==> G1 evidence bundle committed for run $$run_id"

test-perf: check-go
	@set -a; [ -f test/env/.env ] && . ./test/env/.env; set +a; \
	: "$${XFLOW_TEST_REDIS_ADDR:=localhost:$${REDIS_PORT:-6379}}"; \
	: "$${XFLOW_TEST_KAFKA_BROKERS:=localhost:$${KAFKA_PORT:-9092}}"; \
	export XFLOW_TEST_REDIS_ADDR XFLOW_TEST_KAFKA_BROKERS; \
	$(GO) test -tags=perf -bench=. -benchtime=2s -timeout 30m ./test/perf/...

# test-soak runs the HA soak harness smoke over in-process miniredis (no real
# Redis / multi-host topology required). The smoke only verifies multi-replica
# start/stop + single-leader convergence; real fault injection (Task 5.2) and
# SLO quantification (Task 5.3) are ENVIRONMENT-GATED — see
# docs/references/ha-soak-plan.md §4/§6. Standalone `soak` build tag keeps the
# harness out of the default integration suite.
test-soak: check-go
	$(GO) test -tags=soak -race -count=1 -timeout 120s ./test/soak/...

# perf-sample runs the perf bench suite and records results to a file for
# regression monitoring. Sampling only — NOT a capacity commitment. Requires
# `make env-up` (Redis + Kafka). See docs/design/HIGH-THROUGHPUT-INGESTION.md §6.
perf-sample:
	@set -a; [ -f test/env/.env ] && . ./test/env/.env; set +a; \
	: "$${XFLOW_TEST_REDIS_ADDR:=localhost:$${REDIS_PORT:-6379}}"; \
	: "$${XFLOW_TEST_KAFKA_BROKERS:=localhost:$${KAFKA_PORT:-9092}}"; \
	export XFLOW_TEST_REDIS_ADDR XFLOW_TEST_KAFKA_BROKERS; \
	./scripts/perf-sample.sh

# ── Web (frontend) ─────────────────────────────────────────────────────────────

WEB_DIR := web
WEB_LOCKFILE := $(WEB_DIR)/pnpm-lock.yaml

web-install:
	@cd $(WEB_DIR) && if [ -f pnpm-lock.yaml ]; then pnpm install --frozen-lockfile; else pnpm install; fi

web-lint:
	@cd $(WEB_DIR) && pnpm lint

web-typecheck:
	@cd $(WEB_DIR) && pnpm typecheck

web-test:
	@cd $(WEB_DIR) && pnpm test

web-check-boundaries:
	@cd $(WEB_DIR) && pnpm check:boundaries

web-check-production-fixtures:
	@cd $(WEB_DIR) && pnpm check:production-fixtures

web-build:
	@cd $(WEB_DIR) && pnpm build

web-e2e:
	@cd $(WEB_DIR) && pnpm e2e

web-e2e-preview:
	@cd $(WEB_DIR) && pnpm e2e:preview

web-test-coverage:
	@cd $(WEB_DIR) && pnpm test:coverage

web-ci: web-install web-lint web-typecheck web-test web-check-boundaries web-check-production-fixtures web-build web-e2e web-e2e-preview web-test-coverage

web-all: web-ci

# ── OpenAPI contract validation (C0) ───────────────────────────────────────────
validate-openapi: check-go
	@echo "Linting OpenAPI spec with Spectral..."
	npx @stoplight/spectral-cli lint api/openapi/xflow-v1.yaml --ruleset api/openapi/.spectral.yaml
	@echo "Validating OpenAPI spec structure with redocly..."
	npx --yes @redocly/cli@1.34.2 lint --config .redocly.yaml api/openapi/xflow-v1.yaml
	@echo "Running Go OpenAPI fixture + round-trip tests..."
	$(GO) test ./api/openapi/ -count=1
