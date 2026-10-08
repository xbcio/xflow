# Storage Contract — Redis is the System of Record

> Status: **implemented**.

This contract defines the durable scheduling boundary for Redis-backed
executions. It resolves the dual-write asymmetry by designating Redis as
the sole scheduling source of record: every dual-write site either uses the
`auditWrite` best-effort wrapper (routing `UpdateExecutionStatus`, `UpsertNode`,
`DeliverSignal`, `RevokeSignal` through `distributed.Backend.auditWrite`) or
retains the explicit `cleanupCreatedExecution` rollback for the `CreateExecution`
critical path. See `backend/providers/distributed/internal/rstate/state.go` for
the full dual-write site inventory.

## Authority and projections

| Store | Role | What happens on write failure |
| --- | --- | --- |
| Redis (`backend/providers/distributed/internal/rstate`) | **Authoritative** for execution state, leases, scheduling counters, durable outbox, and runner handoff state. Scheduling is consistent iff Redis is consistent. | The operation fails; callers must not treat it as a successful state transition. |
| `store.Store` (sqlstore) | **Audit trail** and query projection. It is not a scheduling source of truth. | Best-effort writes report failure through `AuditObserver` and atomic counters; accepted Redis scheduling state remains valid. |

On restart, recovery reads Redis; SQL is never used to reconstruct scheduling
state. A missing SQL row is a reporting/reconciliation gap, not a reason to
roll back or synthesize Redis state.

## Atomic scheduling contract

`engine.AtomicStateStore` is an optional `StateStore` capability owned by each
backend. The engine only depends on `StateStore` and `TaskQueue`; it discovers
this capability by interface assertion and never imports a concrete storage
implementation.

`StateStore` is a broad facade over the engine's state domains rather than a
single small repository. Redis-backed implementations should keep the
`rstate` code organized by contract area: execution/node lifecycle, lease
fencing and repair, durable outbox/dead-letter, suspend/signal, audit
projection, and namespace isolation. Changes to one area should update the
matching state-store contract tests.

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

### Terminal transitions inside a Lua script

Three paths finalize an execution **inside** their Redis script rather than
through `UpdateExecutionStatus`: node commit (`commitNodeLua`), group commit
(`commitGroupLua`) and entry admission (`seedExecutionFromEntryLua`). They do
not pass through the `UpdateExecutionStatus` wrapper, so each one projects the
terminal state itself:

- node and group commit call `projectExecutionStatus` when the commit result
  reports `ExecutionDone`, projecting the status the Lua landed on plus the
  failure reason (`CyclicFinalError` takes precedence over the node error: on a
  depth-limit overrun the node that tripped the limit succeeded and carries no
  message);
- entry admission calls `projectSeededExecution`, which **creates** the row —
  that path never calls `CreateExecution`, so without it a trigger-group-seeded
  execution has no SQL row at all. It is best effort like the others: an
  admission Redis already accepted is never failed by a projection error.

The projected error text is an engine-supplied reason only. Node output and
boundary-exit data never enter it: upstream output routinely contains
credentials from HTTP responses.

The same reason is also readable online, independently of this projection:
`xflow:ns:<namespace>:exec:{<id>}:error` is loaded back by `GetExecution` into
`engine.ExecutionSnapshot.Error`, surfaced by `Inspect` as
`ExecutionDetail.Error`, and reaches callers as `types.Result.Error`. The SQL
row is the audit trail, not the only readback. Both backends derive the value
through `engine.TerminalExecutionError`, and each mirrors its own counterpart
Lua's write rule — `updateExecutionStatusLua` writes on any non-empty reason
regardless of status, while `commitGroupLua` requires a failed status as well.
See [COMMIT-PATH-TODO.md](./COMMIT-PATH-TODO.md) for why those two rules must
not be unified.

## Execution retention, delivery latency, and the lost-task signal

An execution's Redis keys carry a retention TTL (`execTTL`, `WithExecTTL`;
transient mode uses `activeTTL` and shortens to `completionTTL` at completion).
That TTL is a **delivery SLA**, not just a cleanup policy: the durable intent
for a task is `AckOutbox`-deleted the moment its enqueue succeeds, so once a
task is in the broker the only record that it must still run is the execution's
own state. The contract the deployment must satisfy is:

    execTTL >= discoveryLag + queueResidency + nodeRuntime + commitSlack

Renewal today is activity-based and only covers the suspend/park paths
(`extendExecTTL` callers) plus per-node commit re-EXPIREs; nothing renews the
execution while its work sits in the broker queue, and queue residency is
unbounded (a producer can outrun the consumer indefinitely). There is
deliberately no fixed-TTL "alignment" for that: no constant covers an unbounded
queue, so the residual is made **detectable** instead:

- `xflow_queue_depth{queue,state}` and
  `xflow_queue_oldest_pending_age_seconds{queue}` (consumer-sampled, 30s
  default) measure the backlog and the residency clock directly, including a
  fully stalled consumer.
- `xflow_task_delivery_lag_seconds` measures the same wait per consumed task,
  from the outbox entry's deliverable instant (`AvailableAt` when set, else
  `CreatedAt`) to consumption. A sample above the TTL is **not** proof of
  loss: commits and advances re-`EXPIRE` the status key while the task waits,
  so an execution can stay live past the TTL its task was enqueued under and
  the task still route normally. What such a sample proves is that the
  execution's keys were exposed to expiry for part of the wait.
- `xflow_dispatch_dropped_total{reason}` counts tasks dropped without an
  assignment, including system-task (advance/skip) deliveries for an inactive
  execution. `reason="execution_gone"` is a **loss claim bounded to the
  evidence window**: no live state, no terminal marker, **and** the delivery's
  measured age — from the durable intent's creation, never a delayed intent's
  availability (see the classifier note below) — is inside the window (the
  execution's active retention), so a terminal transition during that span
  would still be readable. It is a claim, not proof: the terminal marker write
  is best-effort, so a benign drop in a lost-marker failure shape reads the
  same way. `reason="execution_unattributed"`
  is the honest middle: no live state and no terminal evidence, but the age
  exceeded the window, the task carried no stamp to measure it, or the backend
  could not confirm the window its writes used, and there a benign late
  duplicate of a long-finished execution is indistinguishable from
  work lost to expiry — during a backlog longer than the retention both land in
  this bucket, so it must be investigated, not paged as confirmed loss.
  `reason="execution_terminal"` is a benign late/duplicate delivery,
  `reason="node_stale"` a stale node route on a live execution, and
  `reason="classify_error"` an unreadable classification (returned retryable,
  neither loss nor health). The two no-evidence reasons do not bracket true
  loss with `gone` as a floor: `gone` can undercount (losses whose age could
  not be bounded land in `unattributed`) and overcount (the lost-marker
  failure shape above), so it is a loss signal, not a lower bound; `gone` plus
  `unattributed` is the **upper bound**: every adjudicated no-evidence drop
  lands in one of the two, and actual lost work among them lies between zero
  and that sum. Both count
  dropped deliveries, not lost executions, and both are rate-limited in logs
  (`dispatch dropped queued work: the execution no longer exists` at error
  level, the unattributed message at warning level).

Alert rules the series are designed for:

- `rate(xflow_dispatch_dropped_total{reason="execution_gone"}[5m]) > 0` — paged
  immediately, deliberately without a "sustained rate" qualifier: the verdict
  is age-bounded and requires no terminal evidence, so waiting for a second
  occurrence would delay a real loss by up to a scrape interval. A single
  qualifying drop keeps the series positive for the whole 5m window, so treat
  a first page as investigate-now rather than confirmed loss — the window
  bounds the claim but does not make it proof — and a sustained rate as the
  loss signature.
- `rate(xflow_dispatch_dropped_total{reason="execution_unattributed"}[5m]) > 0`
  — investigate, do not page as loss: it fires when consumers are draining
  deliveries whose measured age exceeds the retention, which is exactly the
  backlog condition, and the bucket holds benign late duplicates beside any
  true loss.
- `xflow_queue_oldest_pending_age_seconds` approaching `execTTL` (or the
  transient `activeTTL`) — the pre-loss warning; the drop counter is the
  aftermath.
- `xflow_task_delivery_lag_seconds` p99 approaching the same TTL, and
  `xflow_queue_depth{state="pending"}` rising while `state="active"` stays flat.

Two operational notes. First, a deployment rolling this out over an existing
backlog will see a one-time spike in these counters as the queued backlog
drains, and that spike **cannot** be read as pure historic loss: with the
marker window equal to the status retention (durable executions get no marker
at all), a delivery consumed more than one retention after a terminal
transition has no evidence left, and since cancel/fail paths do not purge
broker tasks (`cleanupOnCancel` deletes outbox/lease keys only; the repo
contains no asynq `DeleteTask`/`PurgeQueue` call), benign late duplicates of
executions that terminalized long before consumption are guaranteed to be in
the mix. Those land predominantly in `execution_unattributed` — including
delayed intents whose creation is already older than the window — which is why
it must not be counted as confirmed loss; the `execution_gone` share is bounded
to drops whose measured age is still inside the window. Second, the transport's archive is
a bounded forensic sample (asynq keeps a per-queue maximum of 10,000 archived
tasks), not a dead-letter ledger — the counter is the complete loss record, and
recovery is re-submitting the upstream trigger, not replaying the archive.

### Terminal marker

Because a transient execution's status key is shortened to `completionTTL` at
completion, "no status" alone cannot distinguish a finished execution from one
that expired under its queued work. Every terminal transition
(`UpdateExecutionStatus`, `commitNodeLua`, `commitGroupLua`,
`checkCompletionLua`, `seedExecutionFromEntryLua`) therefore records a terminal
marker (`exec:{<id>}:terminal`) with the execution's **active** retention when
the execution is transient; it is excluded from completion-time shortening, so
the terminal verdict stays readable for the whole window a backlogged task can
be consumed in. Durable executions need no extra marker: nothing shortens
their status key, so the status itself carries the verdict for its whole
retention — and once even that has expired, the same retention window is what
bounds the classifier's verdict (below).

Residual, stated rather than hidden: once a terminal record has itself fully
aged out — a durable execution's status key at `execTTL`, a transient
execution's marker at its active TTL — a task for a finished execution is
indistinguishable from one for an execution that never ran. No finite
retention removes this. What the classifier does instead of guessing is expose
the backend's retention (`GetExecutionRetention`) and split the verdict by
provability: a no-evidence delivery whose measured age is **inside** the
window is `execution_gone` (a terminal transition during that span would still
be readable, so its absence means the execution did not finish), and a
no-evidence delivery whose age **exceeded** the window — or that carries no
stamp to measure it, or whose window the backend cannot prove (the per-execution
retention record has itself expired, so the resolved fallback could name a
longer TTL than the writes used; `GetExecutionRetention` then returns
`engine.ExecutionRetentionUnknown`) — is `execution_unattributed`, which during
a backlog is loss plus late duplicates of long-finished executions and is
documented as neither. A backend that cannot prove its window never falls back
to a longer value: under-reporting `gone` is the deliberate cost, because
reporting a window the evidence cannot support is the false-loss shape the
split exists to remove. Age is measured from the durable intent's creation, not from a
delayed intent's availability (`Task.IntentCreatedAt`): a suspend wakeup or
retry replay armed hours before it becomes deliverable would otherwise be
measured as brand new, and a benign cancelled execution's late wakeup would
read as `gone`. The window bounds the verdict; it does not turn the verdict
into proof — the terminal marker write is best-effort, so a benign drop whose
marker was never written can still read as `gone`, which is why `gone` is a
loss signal, not a lower bound on loss. The pre-loss signals
(`xflow_queue_oldest_pending_age_seconds`,
`xflow_task_delivery_lag_seconds`) are what make the condition visible before
the counters move.

## Observability and reconciliation

`distributed.Backend` exposes `AuditObserver`
(`distributed.WithAuditObserver`),
lock-free `AuditStats()`, and optional state logging (`distributed.WithStateLogger`)
for audit projection failures. Lease lifecycle, commit outcome, durable outbox,
runner-claim recovery, and sweeper timing are exposed through optional observer
interfaces and can be adapted to Prometheus outside `engine/`. Engine-owned
spans may use the narrow tracing facade, but concrete metrics, logging, and
exporter setup must stay outside `engine/`.

Two reconciliation paths already exist: the in-process
`control.AuditReconcileWorker` (leader-gated, reconciles admission/outcome
phases) and the one-shot `xflow dead-letter reconcile` command. What remains
**planned** is a standalone Redis-versus-sqlstore *execution state* diff tool.
Until it exists, audit failure counters and observers are the operational
signal that a projection needs investigation; they do not alter Redis
authority.
