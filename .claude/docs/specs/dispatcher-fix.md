# Spec: Dispatcher — Lease TTL, Capacity Backpressure, Recoverable Routing

**Status**: draft
**Tracks**: review concern #2 — server/runner dispatch path is non-durable, unbounded, and silently lossy
**Severity**: critical (verifier-elevated)

## Problem

Three independent gaps in the cluster control plane combine into a fatal
production blocker:

1. **No lease TTL** — `engine.TaskLease` has no `IssuedAt/TTL`. If a runner
   crashes mid-execute, the lease is never reclaimed; node stays at
   `NodeStatusRunning` forever (verifier-confirmed).
2. **No capacity gating** — `service/control/runner_pool.go:69` appends to
   `state.queue` unconditionally. `Poll()` (75-95) ignores `Capacity - InFlight`.
   `Dispatcher.HandleTask` returns `ErrNoRunnerAvailable` and the memory queue's
   `_ = handler(...)` line at `memory_queue.go:53` silently drops the task.
3. **Pending leases live only in RAM** — `RunnerPool.runners` is rebuilt fresh
   on `cmd/server` start; in-flight leases are lost across restarts.

The first task lost in production is enough to make the cluster MVP unusable.

## Goals

1. Every lease has an issued-at timestamp and a TTL.
2. A background sweeper reclaims expired leases and re-enqueues their tasks.
3. The dispatcher returns an explicit `ErrNoCapacity` distinguishable from
   permanent errors; the queue layer requeues with backoff.
4. Pending leases are persisted (Redis) so server restart doesn't drop them.
5. Throughout: idempotent — retrying a lease never advances state twice
   (already protected by token fencing; we lean on that).

## Non-goals

- Multi-server HA for the control plane itself (separate spec).
- Tags / region / placement metadata (separate spec; mentioned in observability follow-up).
- Bidirectional gRPC streaming for the runner protocol.

## Design

### 1. Lease lifetime

`engine/types.go`:

```go
type TaskLease struct {
    // ... existing fields
    IssuedAt time.Time     `json:"issued_at"`
    TTL      time.Duration `json:"ttl"`
}
```

`engine.BuildTaskLease` populates `IssuedAt = now`, `TTL = e.defaultLeaseTTL`
(default 60s). Configurable via `engine.WithDefaultLeaseTTL(d)` SDK option and
per-node `NodeDef.LeaseTTL` override.

Wire-format:

- HTTP/gRPC `PollTaskResponse.Lease` carries `issued_at` and `ttl` so the
  runner can context-deadline its handler.
- `service/protocol/grpc_conv.go` extended to convert these fields.

Handler wrap:

```go
// service/runner/runner.go execute()
deadline := lease.IssuedAt.Add(lease.TTL)
ctx, cancel := context.WithDeadline(parentCtx, deadline)
defer cancel()
result, err := r.executor.Execute(ctx, lease)
```

### 2. Capacity gating

`service/control/runner_pool.go`:

```go
func (p *RunnerPool) Assign(lease *engine.TaskLease) error {
    p.mu.Lock()
    defer p.mu.Unlock()
    for _, state := range p.runners {
        if !canRun(state.snapshot.Capabilities, lease) {
            continue
        }
        if state.headroom() <= 0 {
            continue // backpressure
        }
        state.queue = append(state.queue, lease)
        state.snapshot.InFlight++ // reserved seat
        return nil
    }
    return ErrNoCapacity
}

func (s *runnerState) headroom() int {
    return s.snapshot.Capacity - s.snapshot.InFlight - len(s.queue)
}
```

`Poll()` corrects `InFlight` when handing out: keeps the reservation when the
runner pulls the lease; releases reservation on successful `ReportResult`.

`ErrNoCapacity` is distinct from `ErrNoMatchingRunner`. Both are *transient* —
the task will be retried by the queue (#3 below).

### 3. Don't drop transient failures

`backend/memory/memory_queue.go:52-53`:

```go
if err := q.handler(ctx, t); err != nil {
    var transient interface{ Transient() bool }
    if errors.As(err, &transient) && transient.Transient() {
        // Requeue with delay; cap retries via Asynq-style attempt header.
        _ = q.enqueueDelayed(t.WithAttempt(t.Attempt+1), backoff(t.Attempt))
        return
    }
    q.deadLetter(t, err)
}
```

`dispatcher.ErrNoCapacity` and `ErrNoMatchingRunner` both implement `Transient()`.
Cap retries: after N attempts (default 10) the task lands in a `deadLetter`
sink — initially just a counter + log; later (#6 observability) emits a metric.

For the asynq backend, `dispatcher.HandleTask` returns the error to Asynq; we
must explicitly mark permanent errors with `asynq.SkipRetry` so we don't hide
real problems behind retries.

### 4. Persistent pending

`backend/asynq/redis_state.go` — add lease tracking:

```
HSET xflow:lease:{exec}:{node} {
    token, runner_id, issued_at, ttl, activation_id
}
EXPIREAT xflow:lease:{exec}:{node} now+ttl+grace
```

Written by `Nodes.ClaimTaskLease` (atomic with the existing claim Lua) so a
crashed server can read inflight leases on boot.

`RunnerPool.Recover(ctx)` on server startup:

1. Scan `xflow:lease:*` keys.
2. For each, mark the runner's queue with a "recovered" lease so re-dispatch
   to the same runner is preferred (if it reconnects within `TTL`).
3. For leases whose runner doesn't reconnect within a grace window, treat
   them as expired (see sweeper).

For memory backend, pending is in-process anyway; recovery is a no-op.

### 5. Lease sweeper

`service/control/lease_sweeper.go` (new):

```go
type LeaseSweeper struct {
    engine *engine.Engine
    state  engine.StateStore
    pool   *RunnerPool
    period time.Duration // default 10s
    log    Logger
}

func (s *LeaseSweeper) Run(ctx context.Context) {
    tick := time.NewTicker(s.period)
    defer tick.Stop()
    for {
        select {
        case <-ctx.Done(): return
        case now := <-tick.C: s.sweepOnce(ctx, now)
        }
    }
}

func (s *LeaseSweeper) sweepOnce(ctx context.Context, now time.Time) {
    expired, err := s.state.ListExpiredLeases(ctx, now)
    // ... for each: RevokeLease (atomic via Lua) → EnqueueDelayed task → log
}
```

New StateStore methods:

```go
// engine/interfaces.go
type Nodes interface {
    // ... existing
    ListExpiredLeases(ctx context.Context, before time.Time) ([]ExpiredLease, error)
    RevokeLease(ctx context.Context, exec types.ExecutionID, name, token string) error
}
```

Atomic `revokeLeaseLua`:
- Check current lease token matches.
- Check `IssuedAt + TTL <= now`.
- Clear lease fields, set node back to `pending`, leave Attempt and
  ActivationID untouched.
- Return success → caller may re-enqueue the task.

Idempotency: if the lease was already claimed by a re-enqueued task, the token
won't match — sweeper logs `lease_already_reclaimed` and moves on.

### 6. Runner self-protect

`service/runner/runner.go` execute path:

- Wrap `executor.Execute(ctx, lease)` with `context.WithDeadline`.
- On deadline exceeded: send `ReportResult` with `Status=Failed`,
  `Error="lease deadline exceeded"`, so the server marks the node failed (or
  the sweeper would have anyway).
- Best-effort: even if `ReportResult` fails, server will sweep.

### 7. Backpressure observability hooks

Stub now, fill in #6:

```go
type DispatcherObserver interface {
    OnAssignSuccess(typ string)
    OnAssignNoCapacity(typ string)
    OnAssignNoMatch(typ string)
    OnSweepReclaim(typ string, ageMs int64)
    OnDeadLetter(typ string, reason string)
}
```

## Wire compat

- New fields on existing types — JSON keys are additive; old clients can ignore.
- gRPC proto definitions need new fields; regenerate `runnerpb`. Pin a version
  to detect downgrades.

## Testing

### Unit
- `BuildTaskLease` populates IssuedAt/TTL with engine defaults; per-node TTL
  override wins.
- `Assign` rejects when `headroom <= 0`.
- `Poll` decrements queue length; `ReportResult` decrements `InFlight`.
- `revokeLeaseLua` rejects when token mismatches; succeeds when token matches
  and TTL expired.

### Integration (memory backend)
- Start engine, dispatcher, fake runner with concurrency 2.
- Enqueue 10 tasks, runner pauses 1s each → all 10 complete, queue depth ≤ 3
  at any time (capacity + headroom).
- Kill runner mid-task: sweeper revokes lease within 2× sweep period, task
  re-dispatched and succeeds.

### Integration (asynq + miniredis)
- Start server + runner.
- Submit 5 tasks, restart server during execution: pending leases recovered;
  runner reconnects and completes.

### Soak
- 1000 tasks, kill runner randomly, no task loss; commit count = enqueue count.

### Race
- `go test -race -count=10` on dispatcher + lease_sweeper tests.

## Acceptance

- Capacity exhaustion no longer drops tasks (the `_ = handler(...)` line goes
  away or properly routes).
- A killed runner's task reappears on a healthy runner within `2 * TTL`.
- Server restart preserves in-flight task records (asynq backend only;
  documented as a memory-backend non-goal).
- All concurrency tests from spec #8 still pass.
- The fix is small enough that we can land it in stages: TTL + sweeper first,
  capacity gating second, persistent pending third (each stage is independently
  testable and shippable).
