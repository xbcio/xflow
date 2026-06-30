# Spec: Lua / StateStore Concurrency Tests

**Status**: draft
**Tracks**: review concern #8 — Lua scripts and memory locks lack concurrent test coverage
**Severity**: high (verifier-confirmed for asynq)

## Problem

`backend/asynq/redis_state.go` defines 9+ Lua scripts implementing critical
cross-process coordination (DecrementInDegree, suspendOrConsume, signalOrStore,
claimTaskLease, resumeNode, revokeSignal, resuspendAtomic, checkCompletion,
propagate). None are exercised under concurrency:

- `backend/asynq/redis_state_test.go` has 3 functions, all single-threaded.
- `backend/memory/state_store_test.go` has 2 functions, single-threaded.
- No `-race` violations because no goroutines.

Effect: future regressions in Lua scripts or memstore locking would only
surface in production.

## Goals

- Add a shared concurrency test suite that runs against any `engine.StateStore`
  implementation.
- Run it against both memory and asynq (miniredis) backends.
- Catch atomicity regressions before they reach production.

## Non-goals

- Performance/throughput benchmarks (separate spec).
- Cross-host Redis cluster testing — miniredis is enough for atomicity.

## Design

### Directory layout

```
backend/contract/
  state_store.go            (existing)
  state_store_test.go       (existing — sequential)
  concurrency_suite.go      (NEW: reusable goroutine-heavy cases)
  concurrency_helpers.go    (NEW: barrier, worker pool, assert helpers)
backend/memory/
  state_store_concurrency_test.go   (NEW: instantiates suite with memstore)
backend/asynq/
  state_store_concurrency_test.go   (NEW: instantiates suite with miniredis)
```

### Suite shape

`concurrency_suite.go`:

```go
type StoreFactory func(t *testing.T) engine.StateStore

func RunConcurrencySuite(t *testing.T, newStore StoreFactory) {
    t.Run("DecrementInDegree_atomic", func(t *testing.T) { ... })
    t.Run("ClaimTaskLease_singleWinner", func(t *testing.T) { ... })
    t.Run("SuspendThenDeliverSignal_noLoss", func(t *testing.T) { ... })
    t.Run("ResuspendAtomic_vs_DeliverSignal", func(t *testing.T) { ... })
    t.Run("CheckCompletion_eventualTrue", func(t *testing.T) { ... })
    t.Run("RevokeSignal_idempotent", func(t *testing.T) { ... })
}
```

### Cases (minimum bar)

1. **DecrementInDegree atomicity**
   - Pre-set inDegree=N for one node, fan-in N concurrent goroutines call
     DecrementInDegree.
   - Sum of returned `remainingInDeg` decreases monotonically, exactly one
     caller sees `remaining==0`, all see consistent `arrivedActiveIn`.

2. **ClaimTaskLease single winner**
   - 10 goroutines all call `BuildTaskLease` for the same node (impossible in
     practice but tests the lock); each gets a unique token. Then 10 different
     tokens compete in `ClaimTaskLease` — exactly one returns `(ok=true)`.

3. **Suspend / DeliverSignal interleave**
   - 50 goroutines each suspend a fresh node (different names) on the same
     signal name; 50 goroutines each call `DeliverSignal` for the same signal.
   - All 50 suspends must observe exactly one signal payload. No signal loss,
     no double delivery.

4. **ResuspendAtomic vs DeliverSignal**
   - Start with node suspended on signal A.
   - Two goroutines race: one calls `ResuspendAtomic(A→B)`, the other calls
     `DeliverSignal(A)`.
   - Final state must be one of:
     - (a) node consumed A's signal, B is irrelevant; OR
     - (b) node is now suspended on B, A's signal is harmlessly stored or
           rejected.
   - Never: node suspended on A while A's signal arrived (lost), never both
     consumed.

5. **CheckCompletion convergence**
   - Build a 50-node DAG with random fan-out. Spawn workers that complete each
     node concurrently. After all done, `CheckCompletion` must eventually
     return `allDone=true` exactly once observable by any caller.

6. **RevokeSignal idempotent**
   - 20 goroutines all revoke the same signal. Exactly one observes `revoked=true`,
     others get `false`. No panic, no leak.

### Helpers

`concurrency_helpers.go`:

```go
func barrier(n int) (start func(), wait func()) { ... } // sync.WaitGroup pair
func runN(n int, fn func(i int)) { ... }                // bounded parallel
```

Every case uses an explicit start barrier to maximize collisions.

### Integration gating

- New `Makefile` target `test-concurrency`:
  ```
  test-concurrency:
  \tgo test ./backend/... -tags=concurrency -race -count=3 -timeout 5m
  ```
- Cases live behind `//go:build concurrency` build tag so default `go test`
  remains fast. CI runs both targets.

### Miniredis fit check

- Confirms support for EVAL/SCRIPT LOAD — current asynq tests already use
  miniredis successfully (`redis_state_test.go`).
- For `ZADD GT` (used in timeout monitor) verify miniredis behavior in a smoke
  test before relying on it; if absent, gate that specific case to
  testcontainers.

## API surface changes

None — purely test additions.

## Acceptance

- `make test-concurrency` runs with `-race -count=3` and passes.
- Deliberately breaking a Lua script (e.g., removing one HSET in
  `claimTaskLeaseLua`) causes at least one case to fail.
- Same for the memory backend — removing the lock around `inDegrees` causes
  case 1 to fail.
- Suite reports per-case durations for future tuning.

## Open items

- `signal_*` cases need TTL handling — pick a TTL >> test duration to avoid
  flakiness from expiry.
- Decide whether to seed PRNGs via `args` (workflows ban `Math.random`,
  but plain test code can use `t.Name()` hash).
