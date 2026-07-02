# Storage Contract — Redis is the System of Record

> Status: **implemented**.

This is the rule that resolves the dual-write asymmetry called out in
[`.claude/specs/dual-write-contract.md`](../../.claude/specs/dual-write-contract.md).

## Roles

| Store | Role | What happens on write failure |
| --- | --- | --- |
| Redis (`backend/asynq/redis_state.go`) | **Authoritative**. All scheduling reads from Redis. The engine state is consistent iff Redis is consistent. | The operation fails. Caller propagates the error. |
| `store.Store` (sqlstore) | **Audit trail**. Best-effort dual-write, queryable for reporting and reconciliation. | The error is reported via `AuditObserver` + atomic counters. The scheduling operation succeeds. |

## Recovery

On restart, replay scheduling state from Redis. The audit store is **never**
used to bootstrap Redis. A missing audit row is a reporting gap, not a
correctness loss.

## Operation classification

Each dual-write site in `redis_state.go` is one of:

- **Critical** (`CreateExecution`): both writes must succeed. The handler keeps
  its explicit `cleanupCreatedExecution` rollback path — `auditWrite` is **not**
  used here.
- **BestEffort** (`UpdateExecutionStatus`, `UpsertNode`, `DeliverSignal` →
  `SaveSignal`, `RevokeSignal`): routed through `s.auditWrite(ctx, op, fn)`.
  Failures increment counters and fire `AuditObserver.OnAuditFailed`; Redis
  state is unchanged.

## Observability

`asynq.Backend` exposes:

- `AuditObserver` (`asynq.WithAuditObserver`) — hot-path hook for metrics
  adapters; default is a no-op.
- `AuditStats()` — atomic counters per op (`ok`, `failed`). Always live; reads
  are lock-free.
- `engine.Logger` (`asynq.WithStateLogger`) — used by `auditWrite` to log
  failed audit writes for ops.

## Reconciliation

A `xflow-server audit reconcile` command — outputting a CSV diff of Redis vs
sqlstore — is planned but not yet implemented. Until then, audit failure
counters are the canonical signal for "this row drifted; investigate."
