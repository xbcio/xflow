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
# Default tests: race-enabled, uncached, 5m package timeout; excludes WASM
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
then runs the complete WASM package once in an isolated race-enabled process
with a 45-minute watchdog (raise-only; see the measured cost above). This lets
`TestMain` compile its WASI guests once
rather than once per shard; all package tests remain included automatically.

`make test` runs ordinary packages plus the script and JavaScript packages with
the normal 5-minute package timeout, but deliberately excludes the heavyweight
WASM package. Run `make test-script-wasm` when changing script execution, Go,
qjs, wazero, or wasip1 behavior. CI runs the full WASM suite once through its
coverage gate; do not add it back to the default local feedback gate. Do not
globally raise the ordinary package timeout to absorb that resource contention.

#### Measured cost of the WASM package

The WASM package is the most expensive gate in the repository, and its cost is
spread across the whole suite rather than concentrated in one test. Clean
measurement of 2026-09-17, 8-core host, cold WASM cache, workspace-local
`GOCACHE`, started at 1-min load ≈ 9 (decaying after an unrelated full-repo test
run finished — so treat the wall time as mildly load-inflated):

| Invocation | Result | Wall time |
|---|---|---|
| `go test -p=1 ./node/internal/code/script/wasm -count=1 -v -timeout 15m` | pass (176 + 2 env-gated skips) | **114.5 s** |
| `go test -p=1 ./node/internal/code/script/wasm -race -count=1 -v -timeout 60m` | 175 pass, **1 fail** (the isolation-sensitive sweep test — see below), 2 env-gated skips | **1424.7 s (~24 m)** |

An earlier `-race` run the same day measured 2040.9 s with 13 failures; that
number is **not comparable** — 12 of the failures were the externally deleted
cache directory described below (they aborted early yet the run was inflated by
CPU contention from concurrent test suites), and the fail count says nothing
about those tests.

The one `-race` failure is `TestCompileMissTriggersSweepReportsEngineCount`, and
its cause is **cross-test mis-attribution**, not a lagging reclaim. An earlier
revision of this section diagnosed it as "the TTL reclaim lags, so the idle
engine is still alive" and prescribed waiting longer. That diagnosis was wrong
and the numbers refute it: a lagging reclaim can only ever yield 2 (both modules
resident), whereas the degraded run reported **6**, which a two-engine host
cannot produce at all.

The actual mechanism: the test reads `engineCountCalls()`, which comes from the
**process-wide** observer whose `OnEngineCount` carries no host identity, and it
treats "the first report past my baseline" as its own host's sweep. Other
`reactorHost` instances in the process sweep continuously and publish into that
same channel. An instrumented full-package `-race` run (per-host id added at the
report site) shows the target host is 28 and the read was **host 16's report**:

```text
TEST   phase=base        host=28  base=4  all=[1 0 2 2]
report                   host=16  count=2      <-- foreign, inside the window
TEST   phase=second-done host=28  all=[1 0 2 2 2]
report                   host=28  count=1      <-- its own, correct, 0.95 s later
TEST   phase=read        host=28  read=2  -> FAIL "= 2, want exactly 1"
```

The reclaim contract held throughout. The window is wide because the test takes
its baseline before compiling a trigger module, which under `-race` costs ~21 s,
during which other hosts publish. Many of those hosts inherit the production 15 m
TTL (37 hosts in one run, from the test files that call `newReactorHost()`
directly rather than `newReactorHost(t)`), so each arms a self-re-arming sweep
timer at ttl/4 ≈ 3 m 45 s and republishes its resident count for the rest of the
~24 min run — `sharedReactorHost` alone reported 6 twelve times, which is exactly
the "= 6" observed.

So: a test-isolation defect, not a product defect. The same host-less
process-wide-channel pattern appears in 12 test files; two of them assert on the
host-less `OnInstanceRecycled` stream (`TestReclaimReportsCountAndCause` and
`TestSupplyChangedOrphansNothingWhenReclaimWinsMidSwap`) and would inflate the
same way if a foreign reclaim landed in their window. They have not been observed
to flake, but they are the same defect shape and need an owner.

Measured 2026-09-17 (8-core host, workspace-local `GOCACHE`): the same
`-race` invocation on a compile-dominated subset costs **2m28s cold vs 28s
warm — 5.2x** — because wazero's guest-module cache is a separate directory
(`XFLOW_WASM_CACHE_DIR`, defaulting under `os.UserCacheDir()`) that
`actions/setup-go`'s `cache: true` does **not** cover. So the ~24 m figure above
is substantially a COLD-cache tax, and CI re-paid it on every run.

CI now caches that directory — the `Restore WASM compilation cache` step in the
`test` and `integration` jobs of `.github/workflows/ci.yml`, which share one
archive. A warm **full-package** number has not been measured: the 5.2x comes
from a subset deliberately chosen to be compile-dominated, so read it as the
direction and rough size of the win, not as a predicted full-package time. A
partial hit is safe by construction — wazero namespaces the directory by its own
version and GOOS/GOARCH and keys each entry by module content, so an entry that
is stale or from another platform is inert rather than wrong.
Two consequences worth knowing before adjusting anything here:

- **The pre-2026-09-17 15-minute watchdog sat ~1.6x below the measured `-race`
  cost.** `WASM_TEST_TIMEOUT ?= 15m` fired on a gate whose real cost is ~24 m;
  when it fires, the goroutine dump it prints names whichever test was running,
  that goroutine is not a culprit, and reading it as one is what produced the
  earlier misattributed "902 s" failure. `WASM_TEST_TIMEOUT` is now 45 m —
  headroom for slower and noisier CI hosts, not a budget to fill.
- **Under `-race`, a test that makes wazero compile a fresh guest module costs a
  flat ~23 s**, so per-test runtime is a poor measure of how "heavy" a test is.
  `TestABI_MinimalGuestWorks` is 0.64 s untagged and 23.6 s under `-race`, while
  tests that reuse an already-compiled module stay under 1 s. A test resolving
  several distinct modules costs a multiple of ~23 s regardless of what it
  asserts. This is also why a "partition off the slowest tests" gate cannot
  work: per-test time measures how many distinct modules a test resolves, which
  correlates with contract breadth, not fixture-ness.

Raise `WASM_TEST_TIMEOUT` only to a measured value recorded alongside the change.

A bare `go test ./... -race` cannot carry this package: Go's per-package default
timeout is 10 m, well under the ~24 m the run needs, so the package fails there
with a timeout panic and the same misleading goroutine dump. Use the Makefile
targets, which supply the watchdog. (`make test` deliberately excludes the WASM
package from its 5 m fan-out for the same reason.)

#### Known hazard: an externally deleted WASM cache directory

wazero's `fileCache.Add` calls `os.CreateTemp(dirPath, ...)`, which returns
ENOENT when the *parent directory* is gone, so a compile fails with a
distinctive signature:

```text
compile module: open <cache dir>/wazero-<ver>-<arch>-<os>/<sha>.NNNN.tmp: no such file or directory
```

The cause observed on 2026-09-17 was **external**, not a defect in this
package: the cache directory configured for the run (`XFLOW_WASM_CACHE_DIR`) was
deleted by a concurrent disk-cleanup step while the package was still running.
12 of the 13 `-race` failures in the run above carry that signature, they
cluster at the tail of the run, and tests that did not need a fresh guest
compile kept passing afterwards.

So the actionable reading is: this signature means the cache directory vanished
mid-run. Check for an external cleanup before investigating this package. The
package's own sweep is not a candidate — `sweepCache` preserves empty version
directories by design and skips `.tmp` files younger than `staleTempAge`.

Because a run's cache directory can be a gigabyte or more, do not delete
`.tmp/*` (or any `XFLOW_WASM_CACHE_DIR`) while a package run is in flight.

### Go coverage gate

Use the same package split when generating the race-enabled Go coverage
artifact:

```bash
make test-coverage
```

The target runs every ordinary package with a 5-minute package timeout, then
runs the script seam and JavaScript package serially with `-p=1` and a focused
5-minute timeout. It runs the complete WASM package once with `-p=1` and a
45-minute watchdog, then merges that profile with the others. This is the single
CI execution of the WASM suite; newly added package tests are included
automatically rather than relying on a maintained skip list.

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

`make proto-tools` does not install `protoc`. Fetch and checksum-verify the
pinned binary into `bin/` (macOS arm64/x86_64 and Linux x86_64/arm64 are
supported) with:

```bash
make fetch-protoc
```

`check-proto-tools`, `proto`, and `proto-check` automatically prefer the
pinned binary in `bin/` over whatever `protoc` is on `PATH`. If your platform
isn't covered, install protoc 35.1 separately and confirm with:

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
| `make test-script-wasm` | `node/internal/code/script`, `node/internal/code/script/js`, `node/internal/code/script/wasm` | Serialized script/qjs gate plus one isolated WASM package run (45m watchdog) |
| `make test-coverage` | all Go packages | Race-enabled atomic coverage; ordinary/script packages use 5m and WASM runs once in isolation (45m) |
| `make test-perf` | `test/perf/` | Benchmarks, needs `make env-up` (Redis + Kafka) |
| `make test-soak` | `test/soak/` | HA soak smoke, runs on in-process miniredis; no real Redis required |
| `make test-concurrency` | `backend/providers/...` | Concurrency stress, gated by `concurrency` build tag |
| `make validate-openapi` | `api/openapi/` | Spectral + Redocly + Go OpenAPI fixture/round-trip tests |
| `make proto-check` | `service/protocol/runnerpb/` | Non-destructive generated-code drift check |

`test/security/` carries no build tag, so `go list ./...` picks it up and `make test` already runs it — it needs no dedicated target. `test/stress/` is gated by the `stress` build tag and runs via `make test-stress`.

Build-tagged files are invisible to both `go build ./...` and `go vet ./...`, so a suite behind a tag can stop compiling without anything reporting it — `test/stress/` did exactly that (an unused import left behind by `92bd8fb`, unnoticed because no target ever built it). `make vet` therefore runs one extra pass per tag; add a pass there whenever you add a tag.

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
