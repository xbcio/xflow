# Node Group Co-location

> Status: Implemented (Milestones A–J complete)
> Branch: `feat/node-group-milestone-b`
> Full spec: `.claude/specs/2026-07-27-node-group-colocation-design.md`

## 1. Overview

Node groups pin a connected subgraph of workflow nodes to a single runner,
executing them locally as one scheduling unit. This eliminates per-node
cross-WAN round-trips for latency-sensitive pipelines (e.g., Kafka consume →
transform → analyze on a remote-cloud runner, emitting only sparse results back
to the control plane).

**Key invariant:** A group is the atomic unit of scheduling, execution, and
durability. Members never leave the assigned runner; the control plane sees the
group as a single vertex in the durable scheduling topology.

## 2. Architecture Layers

```
types/group.go           GroupDef contract (Name, Members, RunnerSelector, OnError, Retry, Timeout, Mode)
engine/graph/            Compile-time IR: GroupMeta, UnitMeta (two-layer scheduling), boundary edges
engine/                  Runtime types: GroupLease, GroupResult, GroupCommitRequest, scheduling intents
backend/.../rstate/      Redis atomic state: group_state.go (commit Lua), group_suspend.go, trigger_admission.go
service/control/         Control loop: group dispatch, activation controller, runner selector
service/runner/          Runner-side: group runtime (embedded engine), package cache, backpressure
service/protocol/        Wire DTOs: GroupLeaseDTO, activation directives, admission RPC
observability/           metrics/group.go, tracing/group_spans.go, engine/group_audit.go
```

## 3. Key Types and Interfaces

### 3.1 Contract (`types/`)

```go
type GroupDef struct {
    Name           string
    Members        []string           // single source of truth for membership
    RunnerSelector *RunnerSelector    // placement; members must NOT set their own
    OnError        string             // "stop" (default) | group-level error policy
    Retry          *RetrySettings     // group-level retry = replay from entry
    Timeout        time.Duration      // business deadline (not lease TTL)
    Mode           string             // "" = durable | "transient"
}
```

### 3.2 Compiled IR (`engine/graph/`)

- `GroupMeta` — compiled group with resolved `Members []int`, `EntryIdx`, `Trigger bool`, `BoundaryInputs/Outputs []BoundaryEdge`, `PackageHash`.
- `UnitMeta` — vertex in the durable scheduling graph (`UnitNode` or `UnitGroup`).
- `UnitEdge` — cross-unit scheduling edge preserving node-level port endpoints.
- Two-layer IR: ungrouped nodes → `UnitNode`; each group collapses to one `UnitGroup`. DAG cycle check runs at unit level.

### 3.3 State Stores (`engine/`)

| Interface | Responsibility |
|-----------|---------------|
| `GroupStateStore` | Acquire/renew/commit group leases atomically (fenced by token+attempt) |
| `GroupSuspender` | Transition running → suspended; persist spec + signal journal + entry input |
| `GroupResumer` | Deliver signal → quorum check → produce resume outbox entry |
| `TriggerAdmissionStore` | Atomic first-writer-wins admission: create execution + commit trigger-group + downstream outbox |
| `TriggerActivationStore` | Desired/active state for trigger-group runner assignment (SetDesired, AssignRunner, Revoke, Renew) |
| `GroupLeaseExpirer` | Reclaim expired leases back to retry-ready |
| `GroupSuspendReader` / `GroupCanceler` / `GroupSignalRevoker` / `GroupTimeoutHandler` | Suspend lifecycle helpers |

### 3.4 Audit (`engine/group_audit.go`)

```go
type GroupAuditObserver interface {
    OnGroupAuditEvent(ctx context.Context, event GroupAuditEvent)
}
```

Operations: `lease_acquired`, `lease_expired`, `committed`, `admission_accepted`, `admission_conflict`, `activation_changed`, `suspended`, `resumed`, `canceled`, `timeout`.

## 4. Lifecycle: Normal Group (Lease-Based)

```
1. Engine advances to group entry node → enqueues TaskTypeGroupExec
2. Control plane dispatches to runner matching group RunnerSelector + capabilities
3. Runner acquires GroupLease (AcquireGroupLease — fenced by token+attempt)
4. Runner executes subgraph locally via embedded engine (in-process memory queues)
5. Runner renews lease periodically during execution
6. Runner reports GroupResult (outcome + fired exit ports + data)
7. Control plane calls CommitGroup: atomic { validate fence, write exits, mark done,
   decrement remaining, advance downstream outbox }
8. Lease expires if runner crashes → ExpireGroupLease → retry from entry
```

## 5. Lifecycle: Trigger-Group (Admission-Based)

```
1. Workflow registered with trigger-group → ActivationController.SetDesired
2. Reconcile loop assigns a live runner → AssignRunner (generation-fenced lease)
3. Runner receives ActivateDirective via heartbeat piggyback → starts Kafka consumer
4. Each batch triggers local group execution (embedded engine, same as normal group)
5. Runner emits result via SeedTriggeredGroupResult (admission key = ns/wf/ver/group/topic/partition/offset-range)
   → Atomic: first-writer-wins occupancy + create execution + commit group unit + downstream outbox
6. Control plane responds: accepted | duplicate-accepted (idempotent) | conflict
7. On accepted: runner commits Kafka offsets. On failure/crash: offsets uncommitted → Kafka replays batch
8. ActivationController renews activation lease; revokes if runner dies
```

**Backpressure:** Runner limits in-flight unconfirmed emits (`EmitBackpressure` semaphore). Window full → consumer pauses. Kafka offset is the single truth for flow control.

## 6. Suspend/Resume (Signal Journal)

Groups support durable suspend when a member node issues a wait:

1. Runner sends `GroupSuspendRequest` with `SuspendSpec` (wait signals, quorum, timeout) + accumulated `SignalJournal` + entry input checkpoint.
2. Backend atomically clears lease, persists suspend state.
3. External signal delivered via `GroupResumer.ResumeGroup` — if quorum satisfied, produces `TaskTypeGroupResume` outbox entry.
4. On resume, runner replays from entry input with full signal journal for deterministic re-execution.
5. Timeout/cancel transitions handled by `GroupTimeoutHandler` / `GroupCanceler`.

Multi-signal quorum: `Quorum` field specifies how many distinct signals are needed before resume.

## 7. Compile-Time Validation (`engine/graph/group_compile.go`)

- Members exist and are unique; each node belongs to at most one group
- Members must NOT set `RunnerSelector` (placement belongs to group)
- Group is a connected subgraph; entry node dominates all members
- Single entry: trigger (priority) > external-incoming > sole root
- Trigger groups: trigger must be the unique entry
- Cross-group edges must not form cycles at unit level
- Portability: rejects non-portable members (validates handler availability)
- Secret literals rejected in group members

## 8. Capability and Routing

- `CapabilityRequirement` = union of all member node requirements + `FeatureGroupExecV1`
- `RunnerSelector.MatchLabels` matched against runner advertised labels
- `RunnerSelector.Mode`: `required` (hard constraint) | `default` (prefer, fallback allowed)
- Runner must register `group.exec.v1` feature to claim group tasks

## 9. Observability

**Metrics** (`observability/metrics/group.go`):
- Lease: `xflow_group_lease_acquired_total`, `_expired_total`, `_renew_total`
- Commit: `xflow_group_commit_total{outcome}`
- Admission: `xflow_group_admission_total{outcome}`, `_duration_seconds`
- Activation: `xflow_group_activation_total{action}`, `_generation_fenced_total`, `_active` gauge
- Emit: `xflow_group_emit_total{result}`, `_duration_seconds`, `_batch_size`, `_inflight` gauge
- Suspend: `xflow_group_suspend_total{action}`
- Backpressure: `xflow_group_backpressure_paused_total`
- Execution: `xflow_group_exec_duration_seconds`
- Package cache: `xflow_group_package_cache_total{result}`, selector fallback

**Tracing** (`observability/tracing/group_spans.go`):
- Spans: `xflow.group.{dispatch,execute,member,emit,admission,commit,activate,renew,suspend,resume}`
- Attributes: group ID, workflow ID, execution ID, runner ID, generation, outcome, batch size, admission key, package hash, signal name

**Audit** (`engine/group_audit.go`):
- `GroupAuditObserver` interface receives structured `GroupAuditEvent` for all lifecycle transitions.

## 10. Feature Gate

Group execution requires the `group.exec.v1` feature capability. Runners that do not advertise this feature will not receive group tasks. The feature gate is enforced at:
- Compile time: `RequirementsFromGraphPackage` always includes the feature requirement
- Dispatch time: capability matching in runner directory
- Runner side: package validation rejects unknown features

## 11. Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Members stored only in `GroupDef.Members` | Single source of truth; nodes do not record group membership |
| Two-layer IR (node graph + unit graph) | Groups collapse to one vertex for durable scheduling; intra-group edges are runner-local only |
| Lease-fenced commit (token+attempt) | Prevents stale runner from committing after lease expired and was reassigned |
| Trigger admission via first-writer-wins | No lease lifecycle for trigger-groups; Kafka offset is the durability checkpoint |
| Deterministic execution ID from admission key | All Redis keys share hash slot for single-script atomicity |
| Backpressure via offset non-commit | Natural flow control; no distributed protocol needed |
| Signal journal replay on resume | Deterministic re-execution from entry input; no partial member state persisted |
| Activation directives piggybacked on heartbeat | No extra RPC; runner learns assignments on next heartbeat response |
