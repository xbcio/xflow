# Storage Contract — Redis is the System of Record

> Status: **implemented**.

This contract defines the durable scheduling boundary for Redis-backed
executions. It also resolves the dual-write asymmetry described in
[`.claude/specs/dual-write-contract.md`](../../.claude/specs/dual-write-contract.md).

## Authority and projections

| Store | Role | What happens on write failure |
| --- | --- | --- |
| Redis (`backend/asynq/redis_state.go`) | **Authoritative** for execution state, leases, scheduling counters, durable outbox, and runner handoff state. Scheduling is consistent iff Redis is consistent. | The operation fails; callers must not treat it as a successful state transition. |
| `store.Store` (sqlstore) | **Audit trail** and query projection. It is not a scheduling source of truth. | Best-effort writes report failure through `AuditObserver` and atomic counters; accepted Redis scheduling state remains valid. |

On restart, recovery reads Redis; SQL is never used to reconstruct scheduling
state. A missing SQL row is a reporting/reconciliation gap, not a reason to
roll back or synthesize Redis state.

## Atomic scheduling contract

`engine.AtomicStateStore` is an optional `StateStore` capability owned by each
backend. The engine only depends on `StateStore` and `TaskQueue`; it discovers
this capability by interface assertion and never imports a concrete storage
implementation.

For an acyclic execution, `CommitNode` is the durable linearization point. The
Redis Lua transition validates the active lease identity (lease ID, token,
attempt, and activation), then atomically:

1. writes the node output and terminal node state;
2. clears lease metadata and the lease-expiry index member;
3. updates `remaining_nodes` and, when applicable, `failed_nodes`;
4. writes the terminal execution status exactly once when the remaining count
   reaches zero or a fatal result is accepted; and
5. persists a deterministic follow-up outbox intent for downstream advance.

Duplicate terminal results return a stable `duplicate_terminal` outcome;
stale tokens and inactive executions do not mutate counters or create new
outbox work. `remaining_nodes` is initialized when an acyclic execution is
created, so normal completion is O(1) rather than a scan of every node key.
Cyclic and experimental expansion paths retain their separate activation-based
completion protocols and do not reuse the static counter.

## Lease discovery and repair

The authoritative lease record is the running-node status plus its metadata:
lease ID/token, issued time, TTL, absolute deadline, attempt, activation, and
queued task metadata. `AcquireTaskLease` writes that record and the
execution-scoped expiry ZSET in one Redis Cluster-safe Lua transition. Retry,
revoke, suspend, and terminal transitions remove the same index member in
their corresponding atomic transition.

The expiry ZSET is a discovery index, not an independent source of truth.
`RepairLeaseIndex` periodically reconciles a bounded page of node state with
the index, restoring a missing deadline member or removing a malformed/stale
one. The control-plane lease sweeper invokes this repair on a leader-gated
cadence and token-fences every reclaim, so a racing result commit wins rather
than being overwritten.

A short-lived `committing` state remains only for suspend and experimental
expansion protocols. It retains the original lease metadata and expiry-index
membership, making it visible to normal lease recovery rather than an
unbounded orphan state.

## Durable scheduling outbox

Every root task and every follow-up scheduling action is first recorded as a
durable outbox entry in the same state transition that makes the task ready.
The `OutboxDispatcher` later hands ready entries to `TaskQueue` and
acknowledges them only after enqueue succeeds.

| Event | Required behavior |
| --- | --- |
| Queue handoff succeeds, outbox ack succeeds | Remove the entry and its retry-attempt record. |
| Queue handoff succeeds, outbox ack response fails | Keep the entry. A later dispatcher may enqueue a duplicate; lease fencing makes that safe. |
| Queue or local system-task handoff fails | Keep the entry, durably increment its delivery attempts, and emit a retry observation. |
| Attempt threshold reached | Remove the pending entry and move its immutable body to execution-scoped dead-letter storage; emit a dead-letter observation. |

The default dead-letter threshold is `engine.DefaultOutboxMaxDeliveryAttempts`
(10) and can be overridden when the engine is constructed. Dead letters are
not silently discarded: they remain in an independent Redis index/body store
for the execution retention window and contribute to backlog metrics. Pending
backlog metrics report count, oldest creation age, and dead-letter count.

This is an at-least-once delivery protocol. It deliberately favors a duplicate
queue delivery over losing a ready task; `BuildTaskLease`, atomic result
commit, and deterministic outbox IDs provide the idempotence/fencing boundary.

## Operation classification

Each Redis-to-SQL projection is one of:

- **Critical creation** (`CreateExecution`): the explicit
  `cleanupCreatedExecution` rollback remains in use when the SQL create fails;
  `auditWrite` is not used for this path.
- **Best effort audit projection** (`UpdateExecutionStatus`, node upserts,
  signal persistence/revocation): routed through `s.auditWrite(ctx, op, fn)`.
  A failure increments counters and invokes `AuditObserver.OnAuditFailed`
  without changing already-accepted Redis scheduling state.

## Observability and reconciliation

`asynq.Backend` exposes `AuditObserver` (`asynq.WithAuditObserver`),
lock-free `AuditStats()`, and optional state logging (`asynq.WithStateLogger`)
for audit projection failures. Lease lifecycle, commit outcome, durable outbox,
runner-claim recovery, and sweeper timing are exposed through optional observer
interfaces and can be adapted to Prometheus without adding an observability
dependency to `engine/`.

A `xflow-server audit reconcile` CLI that produces a Redis-versus-sqlstore
diff remains **planned**. Until it exists, audit failure counters and observers
are the operational signal that a projection needs investigation; they do not
alter Redis authority.
