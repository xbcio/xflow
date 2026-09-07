# Testing Strategy

## Toolchain baseline

The repository Go contract is **module and toolchain `go1.25.0`**. Make exports
`GOTOOLCHAIN=go1.25.0` by default, so local and CI verification select that exact
toolchain even when the host `go` launcher is newer. Override it only for an
explicit toolchain-upgrade investigation; such a run is not release evidence.

```bash
make check-go
```

`make check-go` verifies the `go.mod` directive, active `go env GOVERSION`, and
`GOTOOLCHAIN` are all exactly `1.25.0` / `go1.25.0`.

## Commands

Use Makefile targets as the canonical entry points so local runs match CI
semantics.

```bash
# All default tests: race-enabled, uncached, 5m package timeout
make test

# Verbose variant of the same default gate
make test-verbose

# Engine core only (pure unit tests, no IO)
go test ./engine/... -race -count=1 -timeout 120s

# SDK + backend focused checks
go test ./backend/... ./sdk/... -race -count=1 -timeout 120s
```

### Script / WASM focused gate

The script engines have version-sensitive behavior around qjs, wazero, wasip1,
`-buildmode=c-shared`, cancellation, and sandbox globals. Use this focused target
when touching script execution, changing Go, or changing qjs/wazero:

```bash
make test-script-wasm
```

This target runs the node-layer script seam and JavaScript engine serially,
then discovers every runnable top-level WASM `Test`, `Fuzz`, and `Example` and
round-robin partitions that complete list across eight serial race-enabled
shards. Every shard retains the focused 5-minute package timeout; there is no
maintained test allowlist, so newly added top-level tests are included
automatically.

`make test` first runs all other packages with the normal 120-second package
timeout, then runs this same serialized and sharded focused gate. This keeps
every package and every WASM test in the default gate without letting
full-repository package fan-out starve real WASM compilation. Do not globally
raise the ordinary package timeout to absorb that resource contention.

### Go coverage gate

Use the same package split when generating the race-enabled Go coverage
artifact:

```bash
make test-coverage
```

The target runs every ordinary package with a 120-second package timeout, then
runs the script seam and JavaScript package serially with `-p=1` and a focused
5-minute timeout. Atomic race coverage makes the full WASM test binary exceed
that focused budget on some machines, so the target discovers every runnable
top-level WASM test and partitions the complete list deterministically across
eight serial shards. Each shard still uses `-p=1` and a 5-minute timeout; newly
added tests are included automatically rather than relying on a maintained skip
list.

Every coverage invocation uses `-race -count=1 -covermode=atomic`. The profiles
are mode-checked and merged by source block, with duplicate counters summed and
statement-count mismatches rejected. The merged profile must parse with
`go tool cover` before `coverage.out` is replaced. Any list, test, merge, or
profile-validation failure fails the target and leaves no stale output from that
invocation. The final total is printed to the log.

To write the validated profile elsewhere, point `COVERAGE_PROFILE` at a path in
an existing directory:

```bash
make test-coverage COVERAGE_PROFILE=/tmp/xflow-coverage.out
```

## Protobuf / gRPC generation

Generated runner protocol stubs are reproducible only with the pinned generator
versions below:

| Tool | Required version |
|---|---|
| `protoc` | `libprotoc 35.1` |
| `protoc-gen-go` | `v1.36.11` |
| `protoc-gen-go-grpc` | `v1.6.2` |

Install the Go plugins with:

```bash
make proto-tools
```

`make proto-tools` does not install `protoc`; install protoc 35.1 separately and
confirm with:

```bash
protoc --version
protoc-gen-go --version
protoc-gen-go-grpc --version
make check-proto-tools
```

Use the non-destructive check in review/CI contexts. It renders generated files
to a temporary directory and compares them with the checked-in stubs without
rewriting `service/protocol/runnerpb/*.pb.go`:

```bash
make proto-check
```

Use `make proto` only when intentionally regenerating checked-in stubs.

## Integration Tests

Integration tests live in `test/integration/` and are gated behind the `integration` build tag. They require a running Redis, MySQL, and Kafka instance.

### Test environment (podman)

```bash
make env-up       # Start Redis, MySQL, Kafka containers
make env-ready    # Wait for Redis, MySQL, and Kafka protocol readiness
make env-migrate  # Apply the SQL schema to the test MySQL instance
make env-down     # Stop and remove containers
```

### Redis port — critical

The podman environment maps Redis to **host port 6380** (not 6379). This is set in `test/env/.env`:

```
REDIS_PORT=6380
```

Integration tests read `XFLOW_TEST_REDIS_ADDR` to locate Redis. The Make targets
derive it from `REDIS_PORT` after loading `test/env/.env`; direct `go test`
invocations still need the variable explicitly or may skip Redis-only probes.

**Always set this before running integration tests:**

```bash
export XFLOW_TEST_REDIS_ADDR=127.0.0.1:6380
```

### Running integration tests

```bash
# Silently skips if Redis / MySQL / Kafka are unavailable (safe for local dev
# without the containers running)
make test-integration

# Fails loudly if any required dependency is missing — use in CI so a skipped
# gate cannot masquerade as a pass
make test-integration-required
```

Both targets auto-discover every top-level test in the root integration package
and run four deterministic, serial shards. Each shard retains the race detector
and its own 10-minute package timeout; descendant packages under
`test/integration/...` run once afterward. The target verifies that every
discovered test was assigned exactly once.

`test-integration-required` also sets
`XFLOW_REQUIRE_REDIS_INTEGRATION=1`,
`XFLOW_REQUIRE_MYSQL_INTEGRATION=1`, and
`XFLOW_REQUIRE_KAFKA_INTEGRATION=1`. With those flags set, any unavailable
dependency causes the test binary to exit non-zero rather than skipping.

### G0 evidence gate

```bash
make test-g0-evidence-required
```

Builds a fixed test binary, runs the A0/A3 required manifest (real Redis + MySQL; Kafka not required), records raw evidence fragments, runs the independent verifier, and publishes the artifact. Use this for the P0-G0 gate — not for everyday development.

### G1 required evidence gate

```bash
make test-g1-evidence-required
```

This is the single supported P0-G1 required-evidence entry. It loads
`test/env/.env`, derives `XFLOW_TEST_REDIS_ADDR` from `REDIS_PORT` when needed,
and requires real Redis and MySQL by setting
`XFLOW_REQUIRE_REDIS_INTEGRATION=1` and
`XFLOW_REQUIRE_MYSQL_INTEGRATION=1`. Kafka is not part of this focused gate.
Prepare the services separately with `make env-up env-ready env-migrate`; the
evidence target itself does not start, stop, reset, or migrate containers and
does not clear the shared evidence directory.

The target runs exactly `TestG1ProductionE2E` with the integration tag, race
detector, `-count=1`, and a 600-second timeout. It captures the `go test -json`
event stream, rejects any `skip` event even when the test process exits zero,
and requires exactly one top-level run/pass plus a non-empty structured G1
report. The structured Go test event stream is emitted to stdout and retained
only in a temporary file for validation; it is deliberately not placed in the
G0 raw evidence directory. A successful run publishes the generated, gitignored G1
behavior report at `test/integration/testdata/g1_e2e_report.json`.
`make test-integration-required` remains the full Redis/MySQL/Kafka integration
gate and is not the focused G1 evidence entry.

### Other test suites

| Target | Directory | Notes |
|---|---|---|
| `make test-script-wasm` | `node/internal/code/script`, `node/internal/code/script/js`, `node/internal/code/script/wasm` | Serialized script/qjs gate plus eight auto-discovered WASM shards, each with a 5m timeout |
| `make test-coverage` | all Go packages | Race-enabled atomic coverage; ordinary packages use 5m, script/WASM packages use serialized 5m gate |
| `make test-perf` | `test/perf/` | Benchmarks, needs `make env-up` (Redis + Kafka) |
| `make test-soak` | `test/soak/` | HA soak smoke, runs on in-process miniredis; no real Redis required |
| `make test-concurrency` | `backend/providers/...` | Concurrency stress, gated by `concurrency` build tag |
| `make validate-openapi` | `api/openapi/` | Spectral + Redocly + Go OpenAPI fixture/round-trip tests |
| `make proto-check` | `service/protocol/runnerpb/` | Non-destructive generated-code drift check |

`test/security/` carries no build tag, so `go list ./...` picks it up and `make test` already runs it — it needs no dedicated target. `test/stress/` is gated by the `stress` build tag and runs via `make test-stress`.

Build-tagged files are invisible to both `go build ./...` and `go vet ./...`, so a suite behind a tag can stop compiling without anything reporting it — `test/stress/` did exactly that (an unused import left behind by `cf4bc62`, unnoticed because no target ever built it). `make vet` therefore runs one extra pass per tag; add a pass there whenever you add a tag.

## Engine Core Unit Tests (zero IO deps)

Uses fake StateStore + fake TaskQueue:
- `scheduler_test.go`: linear chain / fan-out / fan-in / port routing / skip cascade / multiple nodes ready simultaneously
- `errorpolicy_test.go`: four strategies
- `suspend_test.go`: signal early/late arrival, timer, timeout, multi-signal quorum
- `engine/graph/compile_test.go`: Compile validation, cycle detection (a separate `engine/graph` package, not a sibling of the three files above)

Fake StateStore uses mutex (~100 lines) to simulate concurrent contention.

## Backend / IO Binding Integration Tests

- `backend/providers/local/` — real memoryState + memoryQueue, end-to-end
- `backend/providers/distributed/` — Redis state + Asynq queue, full scenario coverage
- Shared contract cases in `backend/internal/statestoretest` (`RunStateStoreContract`), consumed by `backend/providers/local/state_store_contract_test.go` and `backend/providers/distributed/internal/rstate/state_store_contract_test.go` — the same scenarios run against both backends

## Testing Conventions

- Use `t.Run` for sub-cases; do not create separate `Test*_Case` functions
- Use table-driven tests for parametric cases
- Include inputs in failure messages: `t.Errorf("Wait(%q) status = %q, want %q", id, got, want)`
- Test helpers must call `t.Helper()`
- No `time.Sleep` — use buffered channels / sync.WaitGroup / context.WithTimeout
- Test-only handler types use `test.` prefix, registered in `init()` inside `_test.go` files
