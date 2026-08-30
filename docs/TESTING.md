# Testing Strategy

## Commands

```bash
# All tests
go test ./... -race -count=1

# Engine core only (pure unit tests, no IO)
go test ./engine/... -race -count=1

# SDK + backend integration
go test ./backend/... ./sdk/... -race -count=1
```

## Integration Tests

Integration tests live in `test/integration/` and are gated behind the `integration` build tag. They require a running Redis, MySQL, and Kafka instance.

### Test environment (podman)

```bash
make env-up       # Start Redis, MySQL, Kafka containers
make env-ready    # Wait until Redis and MySQL are healthy
make env-migrate  # Apply the SQL schema to the test MySQL instance
make env-down     # Stop and remove containers
```

### Redis port — critical

The podman environment maps Redis to **host port 6380** (not 6379). This is set in `test/env/.env`:

```
REDIS_PORT=6380
```

Integration tests read `XFLOW_TEST_REDIS_ADDR` to locate Redis. If this variable is not set, the tests will silently skip rather than fail — a real footgun if you are debugging a CI failure or verifying a fix locally.

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

`test-integration-required` sets `XFLOW_REQUIRE_REDIS_INTEGRATION=1`, `XFLOW_REQUIRE_MYSQL_INTEGRATION=1`, and `XFLOW_REQUIRE_KAFKA_INTEGRATION=1`. With those flags set, any unavailable dependency causes the test binary to exit non-zero immediately rather than skipping.

### G0 evidence gate

```bash
make test-g0-evidence-required
```

Builds a fixed test binary, runs the A0/A3 required manifest (real Redis + MySQL; Kafka not required), records raw evidence fragments, runs the independent verifier, and publishes the artifact. Use this for the P0-G0 gate — not for everyday development.

### Other test suites

| Target | Directory | Notes |
|---|---|---|
| `make test-perf` | `test/perf/` | Benchmarks, needs `make env-up` (Redis + Kafka) |
| `make test-soak` | `test/soak/` | HA soak smoke, runs on in-process miniredis; no real Redis required |
| `make test-concurrency` | `backend/providers/...` | Concurrency stress, gated by `concurrency` build tag |

`test/security/` carries no build tag, so `go list ./...` picks it up and `make test` already runs it — it needs no dedicated target. `test/stress/` is gated by the `stress` build tag and runs via `make test-stress`.

Build-tagged files are invisible to both `go build ./...` and `go vet ./...`, so a suite behind a tag can stop compiling without anything reporting it — `test/stress/` did exactly that (an unused import left behind by `3144e02`, unnoticed because no target ever built it). `make vet` therefore runs one extra pass per tag; add a pass there whenever you add a tag.



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
