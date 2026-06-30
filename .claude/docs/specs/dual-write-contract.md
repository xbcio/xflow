# Spec: Dual-Write Contract — Redis is SoR

**Status**: draft
**Tracks**: review concern #5 — asymmetric dual-write between Redis and store/sqlstore
**Severity**: high (verifier-confirmed)

## Problem

Today `backend/asynq/redis_state.go` writes to Redis and optionally to a
`store.Store` (sqlstore) implementation. The handling is inconsistent:

| Operation | Redis | Store on failure | Source-of-truth |
| --- | --- | --- | --- |
| `CreateExecution` (288-356) | written first | rollback Redis (`cleanupCreatedExecution`) | both must agree |
| `UpdateExecutionStatus` (420-422) | written | `_` discarded | Redis silently |
| `UpsertNode` (548) | written | `_` discarded | Redis silently |
| `DeliverSignal` (893-900) | written | `_` discarded | Redis silently |
| `RevokeSignal` (931) | written | `_` discarded | Redis silently |

Failure modes:

- Store write fails → Redis and MySQL drift, no observable signal.
- Operator has no way to know which side is canonical.
- No reconciliation tool exists.

## Goals

- **Designate Redis as the system of record** for runtime scheduling state.
- **Audit-only role for store/sqlstore**: durable trail, queryable for
  reporting, but not load-bearing for scheduling correctness.
- Replace silent error drops with a uniform `auditWrite` wrapper that records
  failure for observability and reconciliation.
- Provide an offline `audit reconcile` command for diff'ing.

## Non-goals

- True 2PC / distributed transactions across Redis + MySQL.
- Switching to MySQL-as-SoR.
- Real-time replication; reconciliation is offline/manual.

## Design

### Canonical contract

Add `.claude/docs/storage-contract.md` with:

1. **Authoritative**: Redis. All scheduling decisions read from Redis. Engine
   state is consistent if and only if Redis is consistent.
2. **Audit**: `store.Store` (sqlstore). Optional. When configured, every write
   is mirrored on a best-effort basis. Failures are counted and logged but do
   not abort the operation.
3. **Recovery model**: on restart, replay from Redis. The audit store is **not**
   used to bootstrap Redis. Lost audit writes are a reporting gap, not a
   correctness loss.

### Operation classification

Every dual-write site is tagged with one of three semantic levels in code:

```go
type WriteLevel int

const (
    LevelCritical    WriteLevel = iota // both must succeed; on store failure, rollback Redis
    LevelBestEffort                    // Redis authoritative; store failure logged + counted
    LevelAsync                          // queue store write to background channel
)
```

Initial mapping:

| Operation | Level | Rationale |
| --- | --- | --- |
| CreateExecution | Critical | wf submit must establish provenance |
| UpdateExecutionStatus (terminal) | BestEffort | scheduling unaffected by audit failure |
| UpsertNode | BestEffort | state lives in Redis; audit catches up |
| DeliverSignal | BestEffort | ditto |
| RevokeSignal | BestEffort | ditto |
| SaveOutput (`PutOutput`) | BestEffort | could promote to Async later |

### `auditWrite` wrapper

`backend/asynq/redis_state.go`:

```go
func (s *redisState) auditWrite(ctx context.Context, op string, fn func(context.Context) error) {
    if s.db == nil { return } // sqlstore not configured
    if err := fn(ctx); err != nil {
        s.audit.failures.Inc(op) // metric stub now, wired up in #6
        s.log.Warn("audit_write_failed",
            "op", op,
            "err", err,
        )
        // Optionally enqueue to async retry channel for transient errors:
        if isTransientDBErr(err) {
            s.auditRetryQueue.PushOrDrop(retryEntry{op, time.Now(), fn})
        }
        return
    }
    s.audit.ok.Inc(op)
}
```

For `LevelCritical`, callers still keep their explicit rollback path
(`cleanupCreatedExecution`). The wrapper isn't used there; we keep the existing
pattern.

For `LevelAsync`, push onto a bounded channel and a single goroutine drains it
with backoff. Channel full → drop + counter increment.

### Replacing `_, _ = ...` patterns

For each existing site:

```go
// Before:
_, _ = s.db.UpsertNodeStatus(ctx, exec, name, "running")

// After:
s.auditWrite(ctx, "upsert_node", func(ctx context.Context) error {
    return s.db.UpsertNodeStatus(ctx, exec, name, "running")
})
```

### Audit metrics surface

Expose counters per `op`:
- `xflow_audit_ok_total{op}`
- `xflow_audit_failed_total{op}`
- `xflow_audit_retry_total{op}`
- `xflow_audit_dropped_total{op}`

Implementation behind an `AuditObserver` interface, default no-op, hooked up in #6.

### Reconcile command

`cmd/server` gets a subcommand `audit reconcile`:

```
xflow-server audit reconcile \
    --redis-addr=... \
    --mysql-dsn=... \
    --since=2026-06-01 \
    --out=report.csv
```

Output CSV columns:
- `execution_id`, `field`, `redis_value`, `mysql_value`, `last_redis_update`, `last_mysql_update`.

Initial pass covers `executions` and `nodes` tables; signals and outputs added
later (lower priority).

Reconcile is **read-only** in v1. A future `--fix` flag could backfill MySQL
from Redis, but writing to Redis from MySQL is explicitly out of scope —
Redis is SoR.

### Sqlstore status documentation

`store/sqlstore/README.md` (new) declaring:

- This implementation is an **audit trail**, not a primary store.
- It is safe to deploy without it; the SDK cluster mode will tolerate `db=nil`.
- Schema is in `db/xflow_schema.sql`; migrations are out-of-band.
- Read APIs (`ListNodes`, `ListExpiredSuspensions`) are intentionally not used
  at runtime; they exist for reporting/reconciliation only.

## Testing

- Unit: with sqlstore mock returning error, `DeliverSignal` still succeeds;
  failure counter increments.
- Unit: `CreateExecution` with sqlstore mock returning error after Redis
  writes → Redis state is rolled back; returns wrapped error.
- Unit: async retry channel processes 10 enqueued failures, none lost when
  capacity > 10; metrics show enqueued/ok counts.
- Integration: run a workflow with sqlstore deliberately broken (closed conn) →
  workflow completes, audit failure counter > 0, Redis state is consistent.
- Reconcile: prepopulate Redis with state, leave sqlstore empty → reconcile
  reports 100% mismatches; populate sqlstore from a normal run → reconcile
  reports zero diffs.

## Migration

- No DSL or public API changes.
- Operators currently relying on sqlstore for primary recovery should be
  notified via release notes; the recommended path is to add an external
  CDC pipeline if they need a real warm replica.
- Existing deployments with `db == nil` are unaffected.

## Acceptance

- Every `_ = s.db.X(...)` pattern is replaced with `auditWrite` or an explicit
  Critical-level rollback.
- `.claude/docs/storage-contract.md` is published and linked from
  `.claude/docs/architecture.md`.
- `xflow-server audit reconcile` runs and produces a CSV against a real
  workflow execution.
- Metrics surface compiles with a no-op observer (wired live in #6).
