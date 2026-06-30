# Spec: Retry Policy

**Status**: draft
**Tracks**: review concern #7 — `types.RetrySettings` defined but never consumed
**Severity**: high (verifier-confirmed)

## Problem

- `types/workflow.go:110-117` defines `RetrySettings{MaxAttempts, Strategy, InitialInterval}`.
- `NodeSnapshot.Attempt` is incremented per lease (`engine/engine.go:288`) but
  never compared against `MaxAttempts`.
- `grep "retry" engine/*.go` → zero matches. Any handler error transitions
  immediately to `stop|error_output|main_output|continue` via `ApplyOnError`.
- Net effect: transient failures (network blip, temporary 5xx, lease lost mid-poll)
  fail the workflow instead of retrying.

## Goals

- Make `RetrySettings` load-bearing.
- Insert retry **before** OnError. OnError handles retry-exhausted errors only.
- Distinguish transient vs permanent errors so configuration mistakes don't
  spin in retry loops.
- Backoff is exponential with jitter; capped.

## Non-goals

- Idempotency guarantees (handler-side concern; we document the contract).
- Circuit breakers — separate spec if needed later.

## Design

### Error classification

`types/node.go`:

```go
// Permanent marks an error as not retryable. Handlers may also return any
// error wrapped via errors.Join(types.ErrPermanent, ...).
type PermanentError interface {
    error
    Permanent() bool // true → never retry
}

var ErrPermanent = errors.New("xflow: permanent error")

func IsPermanent(err error) bool {
    var p PermanentError
    if errors.As(err, &p) {
        return p.Permanent()
    }
    return errors.Is(err, ErrPermanent)
}
```

Default classification: any error returned by a handler is **transient** unless
explicitly marked. Conservative: would rather waste a few retries than drop a
workflow on a misclassified blip.

Node-side examples (drive-by in implementation PR):

- HTTP: 4xx (except 408, 425, 429) → Permanent. 5xx, 408, 425, 429, network
  errors → Transient.
- gRPC: `codes.InvalidArgument`, `codes.NotFound`, `codes.PermissionDenied`,
  `codes.Unauthenticated`, `codes.FailedPrecondition` → Permanent. Others →
  Transient.
- script: parse error → Permanent. runtime error → Transient (could be
  resource pressure).
- database: SQL syntax / constraint violation → Permanent. Connection errors
  → Transient.

### Backoff function

```go
// engine/retry.go
func backoff(attempt int, base time.Duration) time.Duration {
    const max = 5 * time.Minute
    if base <= 0 { base = time.Second }
    d := base * (1 << minInt(attempt, 10))
    if d > max { d = max }
    // ±20% jitter
    jitter := time.Duration(rand.Int63n(int64(d / 5)))
    if rand.Intn(2) == 0 {
        return d - jitter
    }
    return d + jitter
}
```

### Engine path

`engine/engine.go` — split `handleNodeError`:

```go
func (e *Engine) handleNodeError(ctx context.Context, snap *NodeSnapshot, g *graph.Graph, err error) {
    if retried, rerr := e.tryRetry(ctx, snap, g, err); rerr != nil {
        e.logger.Error("retry enqueue failed", "err", rerr, "exec", snap.ExecutionID, "node", snap.Name)
        // fall through to OnError on enqueue failure
    } else if retried {
        return
    }
    e.applyOnError(ctx, snap, g, err)
}

func (e *Engine) tryRetry(ctx context.Context, snap *NodeSnapshot, g *graph.Graph, err error) (bool, error) {
    retry := g.Nodes[snap.NodeIdx].Retry
    if retry.MaxAttempts <= 0 {
        return false, nil // retries disabled
    }
    if types.IsPermanent(err) {
        return false, nil
    }
    // Attempt was incremented before handler ran; current attempt count is snap.Attempt.
    if snap.Attempt >= retry.MaxAttempts {
        return false, nil // retries exhausted → OnError
    }
    delay := backoff(snap.Attempt, retry.InitialInterval)
    // Reset node to pending and re-enqueue task with delay.
    if err := e.state.ResetNodeForRetry(ctx, snap.ExecutionID, snap.Name); err != nil {
        return false, err
    }
    task := buildTaskFromSnap(snap)
    if err := e.queue.EnqueueDelayed(ctx, task, delay); err != nil {
        return false, err
    }
    e.hooks.OnNodeRetry(ctx, snap.ExecutionID, snap.Name, snap.Attempt, delay)
    return true, nil
}
```

### Graph compile-in

`engine/graph/compile.go` — copy `NodeDef.Retry` (if any) into compiled
`graph.Node.Retry`:

```go
type Node struct {
    // ... existing
    Retry types.RetrySettings
}
```

### StateStore additions

`engine/interfaces.go` (Nodes group):

```go
type Nodes interface {
    // ... existing
    // ResetNodeForRetry rolls a node back from Running → Pending so it can be
    // re-leased after a backoff delay. Clears LeaseToken/LeaseID but keeps
    // Attempt counter, ActivationID, and accumulated history.
    ResetNodeForRetry(ctx context.Context, exec types.ExecutionID, name string) error
}
```

Both backends implement it:
- **memory**: under mutex, mutate `nodeSnapshots[exec][name]` to clear lease
  fields, set `Status=NodeStatusPending`.
- **asynq**: new Lua `resetNodeForRetryLua`: validates current status is
  `running` and node belongs to the right activation, then clears lease fields
  and resets status. Idempotent.

### Hooks

`engine/interfaces.go`:

```go
type Hooks interface {
    // ... existing
    OnNodeRetry(ctx context.Context, id types.ExecutionID, name string, attempt int, delay time.Duration)
}
```

Default no-op hook gets a no-op implementation; tests can use it directly.

### SDK ergonomics

`sdk/xflow/builder.go` add:

```go
func (b *Builder) WithRetry(max int, initial time.Duration) *Builder {
    b.workflow.Options.DefaultRetry = &types.RetrySettings{
        MaxAttempts:     max,
        InitialInterval: initial,
        Strategy:        types.RetryExponential,
    }
    return b
}

// Per-node override:
b.Node(node.HTTP("call_billing")...).Retry(types.RetrySettings{MaxAttempts: 5})
```

Default propagation: per-node `Retry` overrides; otherwise `Options.DefaultRetry`;
otherwise zero (disabled). Compiled into `graph.Node.Retry`.

### DSL

`types/workflow.go.NodeDef` already has `Retry *RetrySettings`. DSL spec
gets a `nodes[].retry: {max_attempts, initial_interval, strategy}` block,
documented in `docs/design/DSL-SPECIFICATION.md`.

## Concurrency / correctness

- `Attempt` is incremented in `acquireNodeLease` (before handler runs); the
  retry check uses it after handler fails, so the comparison is against the
  *next attempt count*. Mental model: `Attempt=N` means "this is the Nth try";
  retry only if `N < MaxAttempts`.
- `ResetNodeForRetry` must be atomic to avoid losing a concurrent
  `DeliverSignal` race. asynq script returns OK only if the current state is
  `running` with the expected `(ExecutionID, name, ActivationID)`.

## Testing

- Unit: fake handler returns transient err 2 times then success; `MaxAttempts=3` →
  workflow succeeds; `Attempt` reaches 3.
- Unit: fake handler returns transient err 5 times; `MaxAttempts=3` → after
  attempt 3, OnError path triggered with original error.
- Unit: handler returns `types.ErrPermanent` → no retry regardless of
  `MaxAttempts`.
- Unit: `ResetNodeForRetry` race against `DeliverSignal` for same node →
  one wins, no orphan state.
- Integration: HTTP node hitting flaky upstream (returns 503 then 200) with
  `MaxAttempts=3` → succeeds, hook log shows one OnNodeRetry call.
- `-race`: clean.

## Migration

- Engines created with default settings get **no retries** (`MaxAttempts=0`),
  preserving current behavior.
- Users opt in via `WithRetry` or DSL `retry:` blocks per node.
- A future release can flip the default after observability is in place.

## Acceptance

- `engine/scheduler_test.go` exists alongside a new `engine/retry_test.go`
  covering the cases above.
- Documented retry semantics: at-least-once delivery, **handlers must be
  idempotent** if they have side effects.
- Compile errors if any code path consumes `Retry` without going through
  `tryRetry`.
