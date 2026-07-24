# Task 8.1 Fix Report — Close Cross-Tenant IDOR in Cancel and Signal Mutations

## Bug Root Cause

`Engine.Cancel` in `engine/cancel.go:12` checked the in-process graph cache `e.graphs[id]` before verifying tenant ownership. The same `*engine.Engine` instance is shared across tenants, so tenant A's submitted graph remained cached under the execution ID. When tenant B called `Cancel(ctx, tenantA_execID)`:

1. The cache hit returned tenant A's graph without a tenant-scoped check.
2. `UpdateExecutionStatus(ctx, id, ...)` used `tenant.FromContext(ctx)` = tenant B, so it wrote to a tenant-B key that did not own the execution and did not affect tenant A's state.
3. Cancel still returned success (`200`), called `notifyExecutionComplete`, and evicted the execution from the shared cache.

This leaked the existence of tenant A's execution ID and triggered side effects on behalf of tenant B, violating `tenant-boundary-design.md` §5 (cross-tenant access must return 404/NotFound).

## Cancel Fix

Replaced the direct cache read in `engine/cancel.go` with `loadActiveGraph(ctx, id)`, the same tenant-aware helper used by runner commit and signal paths. `loadActiveGraph` verifies the execution exists in the caller's tenant namespace via `e.state.GetExecution(ctx, id)` before returning the graph. When the execution is inactive or belongs to another tenant, it returns `active=false`; `Cancel` then returns a wrapped `ErrExecutionInactive` whose message contains "not found" so the HTTP layer maps it to 404.

File: `engine/cancel.go:12`

```go
// before: direct e.graphs[id] lookup
// after:
g, active, err := e.loadActiveGraph(ctx, id)
if err != nil {
    return fmt.Errorf("load graph for canceled execution %q: %w", id, err)
}
if !active {
    return fmt.Errorf("execution %q not found: %w", id, ErrExecutionInactive)
}
```

The rest of the cancel logic (`UpdateExecutionStatus`, `ListSuspendedNodes`, `UpsertNode`, final `UpdateExecutionStatus`, `notifyExecutionComplete`, `EvictExecution`) is unchanged.

## Other Mutation Path Audit

| File:Line | Function | Cache-first? | Tenant-aware? | Action |
|-----------|----------|--------------|---------------|--------|
| `engine/signal.go:15` | `DeliverSignal` | No | Yes — calls `loadActiveGraph` in both `deliverSignalDurable` (`:34`) and `deliverSignalLegacy` (`:98`) | Compliant, no change |
| `engine/signal.go:144` | `TimeoutNode` | No | Yes — calls `loadActiveGraph` at `:145` | Compliant, no change |
| `engine/signal.go:183` | `RevokeSignal` | No | Yes — delegates directly to `e.state.RevokeSignal(ctx, id, signalName)`, which uses tenant-scoped keys | Compliant, no change |
| `engine/signal.go:199` | `currentActivationID` | No | Yes — helper uses `e.state.GetNode(ctx, id, nodeName)` after caller has already passed `loadActiveGraph` | Compliant, no change |
| `engine/expand.go:162` | `ExecuteBatch` | **Yes** | **No** — direct `e.graphs[lease.Task.ExecutionID]` read could bypass tenant-scoped validation | Fixed — replaced with `loadActiveGraph` |
| `engine/engine.go:191` | `loadActiveGraph` | Internal helper | Yes — uses `GetExecution` to confirm tenant-scoped existence | Already the correct pattern |

`ExecuteBatch` was the only other production mutation path that read `e.graphs[id]` directly. It is an internal system-task handler, so returning bare `ErrExecutionInactive` on inactive/cross-tenant is sufficient; both dispatchers (`execution/dispatcher.go:146`, `service/control/dispatcher.go:109`) drop `ErrExecutionInactive` tasks.

## Verification

### Build and vet

```bash
$ go build ./...
$ go vet ./engine/...
```

Both passed with no output.

### Engine tests

```bash
$ go test -race -count=1 ./engine/...
ok  	github.com/xbcio/xflow/engine	1.994s
ok  	github.com/xbcio/xflow/engine/graph	2.576s
```

### Task 8.1 security suite

```bash
$ go test -race -count=1 ./test/security/...
ok  	github.com/xbcio/xflow/test/security	1.958s
```

`TestTenantIsolationCrossTenantExecuteRejected` now returns 404 as expected; the full tenant-isolation matrix is green.

### APIserever regression tests

```bash
$ go test -race -count=1 ./service/apiserver/...
ok  	github.com/xbcio/xflow/service/apiserver	4.957s
```

## Concerns

- `ErrExecutionInactive` itself does not map to 404 in `service/apiserver/module_control.go:writeEngineError`; the 404 mapping depends on the error message containing the substring "not found". For `Cancel` we therefore return `fmt.Errorf("execution %q not found: %w", id, ErrExecutionInactive)` so the HTTP layer returns 404 while callers can still use `errors.Is(err, engine.ErrExecutionInactive)`. This keeps the API handler unchanged as required.
- `RevokeSignal` remains tenant-scoped at the state layer but its existing HTTP handler maps `ErrSignalConsumed` to 409 Conflict. That is a handler-level mapping outside the scope of this engine-layer fix.
- No store/rstate key changes, no API handler changes, no audit/metrics changes, no runner changes, and no dead-letter manager changes were made.

## Fix: 404 mapping via errors.Is

Reviewer feedback: the 404 mapping in `writeEngineError` relied on the substring "not found" appearing in `Cancel`'s error text. That is brittle and could leak internal details if an unrelated error happens to contain those words. The fix makes the mapping explicit with `errors.Is` while keeping the substring branch for backward compatibility with other paths.

### Changes

1. `engine/cancel.go:23`

   Removed the hard-coded "not found" text from the inactive/cross-tenant error. The error is still wrapped with `ErrExecutionInactive`, so `errors.Is` can detect it.

   ```go
   if !active {
       return fmt.Errorf("execution %q: %w", id, ErrExecutionInactive)
   }
   ```

2. `service/apiserver/module_control.go:386-396`

   Added an explicit `errors.Is(err, engine.ErrExecutionInactive)` branch that maps to HTTP 404 before the legacy substring check.

   ```go
   func writeEngineError(w http.ResponseWriter, err error) {
       if errors.Is(err, engine.ErrExecutionInactive) {
           writeError(w, http.StatusNotFound, err.Error())
           return
       }
       if strings.Contains(strings.ToLower(err.Error()), "not found") {
           writeError(w, http.StatusNotFound, err.Error())
           return
       }
       writeError(w, http.StatusInternalServerError, "internal server error")
   }
   ```

### Verification

```bash
$ go build ./...
$ go vet ./engine/... ./service/apiserver/...
$ XFLOW_TEST_REDIS_ADDR=127.0.0.1:6380 go test -race -count=1 ./engine/... ./service/apiserver/... ./test/security/...
ok  	github.com/xbcio/xflow/engine	2.199s
ok  	github.com/xbcio/xflow/engine/graph	1.654s
ok  	github.com/xbcio/xflow/service/apiserver	5.776s
ok  	github.com/xbcio/xflow/test/security	1.954s
```

`TestTenantIsolationCrossTenantExecuteRejected` still returns 404; no regressions in the targeted engine, apiserver, or security suites.

### Concerns

- The legacy substring branch remains in `writeEngineError` for other code paths that still surface "not found" text. Once those paths are migrated to sentinel errors, the substring branch can be removed.
- Cross-tenant access continues to return 404 without leaking existence, and single-tenant behavior is unchanged.

