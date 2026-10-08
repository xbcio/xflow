# Node Group Co-location

> Status: Implemented, experimental/limited (Milestones A–J + Phase 5 remote-runner trigger hosting; see §12.2)

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
types/group.go           GroupDef contract (Name, Members, RunnerSelector, OnError, ErrorOutputs, Retry, Timeout, Mode)
engine/graph/            Compile-time IR: GroupMeta, UnitMeta (two-layer scheduling), boundary edges
engine/                  Runtime types: GroupLease, GroupResult, GroupCommitRequest, scheduling intents
backend/.../rstate/      Redis atomic state: group_state.go (commit Lua), entry_admission.go
service/control/         Control loop: group dispatch, entry-activation manager + reconciler, runner selector
service/runner/          Runner-side: group runtime (embedded engine), package cache
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
    OnError        string             // "stop" (default) | "continue" | "error_output"
    ErrorOutputs   []Connection       // required iff OnError == "error_output"; routing targets for the group's synthesized "error" port
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
| `EntryAdmissionStore` | Atomic first-writer-wins admission: create execution + commit entry unit + downstream outbox |
| `EntryActivationStore` | Desired/active state for node-generic entry-activation runner assignment (Upsert desired state, Assign/Renew/Fence for generation-fenced ownership). Replaces the retired group-centric `TriggerActivationStore`. |
| `GroupLeaseExpirer` | Reclaim expired leases back to retry-ready |

### 3.4 Audit (`engine/group_audit.go`)

```go
type GroupAuditObserver interface {
    OnGroupAuditEvent(ctx context.Context, event GroupAuditEvent)
}
```

Operations: `lease_acquired`, `lease_expired`, `committed`, `admission_accepted`, `admission_conflict`, `activation_changed`.

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
1. Workflow registered (POST /v1/workflows/register) → control plane persists the
   compiled graph in the WorkflowRegistry → EntryActivationManager derives the
   desired per-entry-unit activation (desired-state only; does not fence)
2. EntryActivationReconciler (single fence+assign authority) matches the activation
   to a live runner by selector + capability (fail-closed) and assigns it with a
   generation-fenced lease
3. Runner receives a node-generic ActivateDirective (carrying the generation) via
   heartbeat piggyback → TriggerActivationHandler starts the Kafka consumer
4. Each batch triggers local group execution (embedded engine, same as normal group)
5. Runner seeds the entry via SeedExecutionRequest (carrying the generation) →
   admission key = ns/wf/ver/group/topic/partition/offset-range → Atomic:
   first-writer-wins occupancy + create execution + commit unit + downstream outbox.
   The server fence admits a new admission key ONLY when the request generation
   equals the activation's generation (stale/forged generation fails closed).
6. Control plane responds: accepted | duplicate-accepted (idempotent) | conflict.
   A stale-generation 409 does NOT commit the Kafka offset → Kafka replays the batch.
7. On accepted: runner commits Kafka offsets. On failure/crash: offsets uncommitted → Kafka replays batch
8. Reconciler renews the lease from reported inventory (generation-stable) and
   revokes/deactivates assignments for runners that stop reporting; on reconnect it
   reconciles reported inventory (renew live owners, revoke stale ones)
```

> The old group-centric `ActivationController` was retired in favor of the
> node-generic `EntryActivationManager` (desired state) + `EntryActivationReconciler`
> (fence/assign/renew/revoke) split described above.

> **`EmitBackpressure` 信号量已从代码库中删除。** 完整实现（`service/runner/backpressure.go`
> + `backpressure_test.go`）可用 `git show` 从本次提交之前取回，不是重写。
>
> 删除的理由不是「没写完」，而是**它挡不住的那件事从来没被建出来**：group 侧
> runtime（`group_exec_trigger_runtime.go`）继承的是 `HTTPEntrySeedRuntime` 的
> fail-closed `Emit` 桩，`engine/group_exec.go` 的 `commitGroup` 一次性提交全部
> exits，没有流式 emit 循环去调用这个信号量——它全树零调用方，只有自己的构造函数
> 测试。90 行通用信号量本身重写成本几乎为零；真正的成本在耐久 emit 流那一侧，
> 那一侧从未存在过。
>
> Kafka offset is the single truth for flow control（见下文决策表同一条）——这个
> 设计取向不依赖被删的信号量，仍然成立。

## 6. Suspend/Resume (Signal Journal) — 已移除

> **组级持久化挂起已从代码库中删除。** 完整实现保留在提交
> `981805c feat(engine): implement durable group suspend/resume (Milestone I)`
> 里（含 `29e3868` 那个只有真 Redis 才暴露的 `cjson.null` 修复），要恢复是
> `git show`，不是重写。
>
> 删除的理由不是「没写完」，而是**写完的那部分不是难的那部分**。两个后端的
> 状态层（Lua / 内存 map）和共享契约测试都完整，但它们只回答「挂起状态怎么
> 存」。真正挡住这个特性的是两件一行代码都没有的事：
>
> 1. **挂起的成员会停住一个外层租约无法恢复的子执行**——与 map body 内禁止挂起
>    同源。生产因此刻意关闭组内挂起（`sdk/xflow/runner.go` 的
>    `runnersvc.WithSuspendDisabled()`、`group_exec_trigger_runtime.go` 硬设
>    `SuspendDisabled: true`），成员发出的 wait 在 `engine/commit.go` 就被判失败。
>    这条**保留不变**，删除不影响它。
> 2. **没有「列出挂起中的组」原语**。`Engine.Cancel` 只遍历 `ListSuspendedNodes`，
>    两个后端都没有可枚举挂起组的入口。
>
> 真要做，这两件都得重新设计，状态层大概率跟着改——留着并不能缩短将来的路。
>
> 而它不是零成本的死代码：`resumeGroupLua` 在配额满足时会往**现役 outbox** 写一条
> `TaskTypeGroupResume`，而 `handleSystemTask` 的 `default` 分支语义是「交给
> Dispatcher 投给远端 runner」，runner 不认这个类型。任何一次调用都会往队列里投
> 一条没有消费者的毒条目，契约测试对这条边零覆盖。
>
> **顺带修掉的一个真洞**：原先 `CommitGroupResult` 的致命性 switch 没有 default
> 分支，只挡 `"suspended"` 一个值。`Outcome` 是从远端 runner 过来的裸 JSON
> 字符串，任何其他未知值都会落进 `fatal=false`，被当成非致命失败提交并放行下游。
> 删除时把挡板收紧成通用的未知 outcome 拒绝，比删除前更严
> （`TestCommitGroupResult_UnknownOutcomeRejected`）。

节点级挂起（`xflow.wait`）不受影响：它走的是另一套 `types.SuspendSpec` +
`SuspendTaskLeaseWithOutbox` + `ListSuspendedNodes`，与被删的组级类型零共享。

## 7. Compile-Time Validation (`engine/graph/group_compile.go`)

- Members exist and are unique; each node belongs to at most one group
- Members must NOT set `RunnerSelector` (placement belongs to group)
- Group is a connected subgraph; entry node dominates all members
- Single entry: trigger (priority) > external-incoming > sole root
- Trigger groups: trigger must be the unique entry
- Cross-group edges must not form cycles at unit level
- Portability: rejects non-portable members (validates handler availability)
- Secret literals rejected in group members
- `OnError` restricted to `""` | `stop` | `continue` | `error_output` (`validateGroupOnError`); `main_output` and unknown values rejected
- `ErrorOutputs` required iff `OnError == error_output`, and rejected otherwise (`validateGroupErrorOutputs`); each target must be a real node outside the group, not a supply node, and must respect the target's own declared input ports (`resolveGroupErrorOutputs`)

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
- Execution: `xflow_group_exec_duration_seconds`
- Package cache: `xflow_group_package_cache_total{result}`, selector fallback

**Tracing** (`observability/tracing/group_spans.go`):
- Spans: `xflow.group.{dispatch,execute,member,emit,admission,commit,activate,renew}`
- Attributes: group ID, workflow ID, execution ID, runner ID, generation, outcome, member count, batch size, admission key, package hash

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
| Signal journal replay on resume — **已移除**（见 §6：组级持久化挂起已从代码库删除；`sdk/xflow/runner.go` 用 `runnersvc.WithSuspendDisabled()`，`group_exec_trigger_runtime.go` 硬设 `SuspendDisabled: true`） | Deterministic re-execution from entry input; no partial member state persisted |
| Activation directives piggybacked on heartbeat | No extra RPC; runner learns assignments on next heartbeat response |
| `error_output` is a synthetic unit-level edge, not a `BoundaryOutputs` entry | A group has no member edge to derive a failure port from; `GroupMeta.ErrorOutputs` + a `UnitEdge.ErrorEdge` flag keep the synthetic route in the same scheduling machinery as a real boundary edge without polluting `BoundaryOutputs`'s "derived purely from real member edges" invariant |
| Routed `error_output` commit reports `GroupOutcomeSuccess`, not `GroupOutcomeFailed` | Every backend's failed-unit counter fires on `Outcome==Failed` regardless of `Fatal`, and finalizes the WHOLE EXECUTION as Failed once remaining-units hits zero; mirrors `ApplyOnError`'s node-level `error_output`, which sets `NodeStatus: types.NodeStatusSuccess` for the same reason |
| `fatal`/`routeToErrorOutput` are caller-decided in `commitGroup`, never re-derived from `execErr != nil` | `CommitGroupResult`'s canceled/timeout outcome must stay unconditionally fatal even under `on_error: continue`/`error_output`; re-deriving from `execErr != nil` cannot distinguish that case from an ordinary tolerated/routed failure |

## 12. Known Limitations & Future Work

Phase 5 (remote-runner trigger hosting) shipped the subsystem end-to-end: server
workflow registry, `EntryActivationManager` + `EntryActivationReconciler`,
generation-fenced seeds, runner `ActivationHandler` + `ActivationTracker` with
reconnect inventory reconciliation. A follow-up pass then closed the deferred
items listed below under §12.1. What remains open is in §12.2.

### 12.1 Closed follow-ups

- **P1-3 map-as-group-member is closed by `3cfa1db`.** MAP members now retain their projected body through compile/snapshot and execute that body inside the group runtime. Current MAP work is runner-level resource governance (limiter/queue/wiring plus cancel/deadline verdict closure); it does not reopen P1-3.
- **gRPC register carries the activation inventory.** `RegisterRequest`
  (`service/protocol/runnerpb/runner.proto`) has a repeated
  `ActivationInventoryItem`, mapped both ways by `RegisterRequestToProto` /
  `RegisterRequestFromProto`. gRPC-transport runners now get the same
  zero-orphan reconnect reconciliation as HTTP ones.
- **Inventory reconciliation keys by `(workflowID, workflowVersion,
  entryUnitID)`.** `ReconcileRunnerInventory` no longer collapses multiple
  versions of the same entry unit. An item reporting an empty version (a runner
  predating the field) degrades to the versionless two-tuple match so it is
  renewed rather than falsely revoked; the `gen == act.Generation` gate still
  applies on both paths.
- **Redis `ListLiveRunners` is O(1) in round-trips.** `HKeys` followed by one
  pipelined batch of `HMGet` per runner-attribute hash — two round-trips
  regardless of runner count, down from `1 + 7N`. `Runner()` and
  `ListLiveRunners()` share one decoder (`decodeRunnerSnapshot`) so the
  field-defaulting rules cannot drift apart.
- **The seed HTTP client is injectable.** `NewTriggerActivationHandler` takes
  `WithSeedHTTPClient`; the runner entrypoint passes a client whose timeout sits
  above the per-request context deadline so the context deadline stays the
  effective bound.
- **The seed admission deadline is configurable.** The per-request context
  timeout is `protocol.DefaultEntrySeedRequestTimeout` (15s) unless the runner
  sets `RunnerConfig.SeedRequestTimeout` (or the standalone
  `seed.request_timeout`). The integrator's batch size sets how long an
  admission takes, so the integrator is who has to be able to move this bound:
  at 15s a large batch fails with `context.DeadlineExceeded`, the offset is left
  uncommitted, and the whole batch is redelivered. The seed HTTP client timeout
  is derived from the deadline (twice it), so raising the deadline does not leave
  the client cutting the request off first and reporting a transport error where
  the deadline would have reported a timeout.
- **The generation-upgrade stale-close branch is tested.** Two cases cover it:
  a second `Activate` at a higher generation closes the superseded subscription
  exactly once, and a failing `Activate` leaves the old subscription open and
  installed. The branch's reliance on `ActivationTracker` serializing directives
  per activation identity is now stated in a comment beside it.
- **Aggregate Kafka offset commits are emit-then-commit.**
  `kafkaPartitionAggregator` no longer marks a message deduplicated before its
  side effect is durable, which removes the window where a crash between the
  dedup write and the emit lost the event permanently. A failed flush retains
  the buffer and retries; offsets commit only after the whole batch emits.
- **Aggregate Kafka mode is hosted via entry-seed admission.** `Activate` no
  longer rejects the combination. The batch admission key encodes the batch's
  **actual** offset range (`…/topic/partition/start-end`), so it is not
  reproducible across a redelivery — batch boundaries are decided by broker
  fetch timing, not by the aggregation logic, and no alignment scheme can
  change that. The delivery semantics are therefore **at-least-once**, matching
  the rest of xflow: a batch that seeds successfully but whose offsets never
  commit is reprocessed after the reader is rebuilt. What differs on the
  per-message entry-seed path is not the guarantee but the key: `seedEntryBatch`
  admits on the record's stable source identity
  (`…/topic/partition/offset-offset`), so a redelivered record is answered
  `duplicate` and never seeds a second execution. That is idempotent admission
  (dedup on a stable key), not exactly-once delivery — no path in xflow promises
  that. A batch key carries the batch's actual offset range and
  therefore recurs only when the same boundary recurs, which is why a redelivered
  batch can re-execute while a redelivered record cannot. Duplication is bounded
  at one batch and is what `xflow_trigger_batch_admissions_total` exists to
  measure. The seed-then-commit rule and the ride-along of schema-discarded
  offsets are shared verbatim with the legacy `Emit` branch. Consumers must be
  idempotent on `(topic, partition, offset)` — not on `execution_id`, since the
  duplicate is by construction a different execution.
- **`default`-selector fallback grace period** (spec §11.7). A `default`-mode
  activation with no label-matching runner waits out `FallbackGrace`, then falls
  back to any live runner with headroom whose capabilities satisfy the entry
  unit. `required` mode still fail-closes. Capability matching is never relaxed
  by the fallback. An empty `Mode` counts as `default`, matching the compiler's
  normalization in `engine/graph/compile.go`.

### 12.2 Open items

- **Non-Kafka trigger types are fail-closed for entry-seed hosting.**
  `TriggerActivationHandler` dispatches generically to any registered trigger
  type, but only the Kafka path consumes the entry-seed runtime
  (`SeedExecutionFromEntry`). A trigger mis-wired onto the legacy `Emit` path
  with `HTTPEntrySeedRuntime` hits fail-closed stubs: `Emit`/`Dedup`/`TryLock`
  return an error so the offset is never committed and the message is
  redelivered rather than silently dropped. `State` returns nil — its interface
  signature has no error return, so a mis-wired caller panics instead, which is
  still fail-closed but not an error return. This is a phased-rollout
  limitation: only Kafka has entry-seed hosting today.
- **Supply nodes (see [SUPPLY-NODE.md](./SUPPLY-NODE.md)) reuse the activation
  record/reconciler layer but not the entry-seed/admission-key layer, and that
  split is deliberate, not a gap.** `EntryActivationManager.SuppliesForEntryUnit`
  attaches `Supplies []engine.SupplyRequirement` to the same `EntryActivation`
  record used for trigger hosting, and `entry_activation_reconciler.go`'s
  `activationKindFor` now classifies a record as `ActivationKindSupply` when
  its `NodeType` has the `xflow.supply.` prefix — so the fence/assign/renew/
  lease machinery this section describes is no longer Kafka-trigger-only. What
  it does **not** do is create an execution or an admission key for a supply
  node: `buildUnits` excludes every `NodeKindSupply` node from the unit layer
  (`nodeUnit` stays `-1`), and `entryUnitIndex` explicitly rejects a supply
  node used as an entry unit rather than surfacing that `-1` as a real index.
  A cron (or any other) trigger's entry-seed hosting was never a prerequisite
  for a supply's content to become available — the supply collection face is
  gated by `SupplyGate.Admit` at activation time, independent of whether the
  consuming workflow's own trigger uses entry-seed hosting at all.
- **Group-level `on_error: error_output` is implemented (2026-10-03);
  `main_output` stays rejected at compile time.** The two prior investigations
  (2026-08-11, 2026-08-14, preserved below for provenance) found correctly
  that no group-level error port existed to route to and that the obstacle was
  real, not a wiring gap. The mechanism below is what was built to close it —
  smaller than the "new compile-time IR" scope the investigations anticipated,
  because it deliberately does NOT make a group a node-name-indexed connection
  source (that would have required reordering `compileGroups` ahead of
  `buildEdges` and teaching every `g.index` consumer about group names). It
  stays entirely inside the two-layer unit IR instead.

  - `types.GroupDef` gained `ErrorOutputs []Connection`: the downstream
    targets for the group's synthesized "error" port. Required and validated
    only when `OnError == error_output` (`validateGroupErrorOutputs`,
    `engine/graph/group_compile.go`) — declaring one without the other is
    rejected the same way an unimplemented policy used to be, because it is
    the same silent-no-op failure mode.
  - `compileOneGroup` resolves each `ErrorOutputs` target to a
    `graph.BoundaryEndpoint` (`resolveGroupErrorOutputs`): the target must be a
    real node outside the group (not a member — there is no running engine
    instance left inside an abandoned group to deliver to), must not be a
    supply node, and must respect the target's own declared input ports via
    the same `declaredInputPorts.validateEdgeTarget` an ordinary connection
    uses. Resolved endpoints are stored on `GroupMeta.ErrorOutputs`.
  - `buildUnitEdges` (`engine/graph/unit.go`) synthesizes one `UnitEdge` per
    resolved `GroupMeta.ErrorOutputs` entry, from the group's own unit to the
    target's unit, marked `ErrorEdge: true`. Its `Src.NodeIdx` is `-1` (the
    group itself is the source, not a member) and `Src.Port` is the reserved
    `errorOutputPort` ("error"). This is the "no legal exit to compute
    arrivals from" problem the investigations named: an `ErrorEdge` is a
    first-class member of the SAME unit edge lists `BoundaryOutputs`-derived
    edges live in, so it participates in the ordinary static in-degree count
    and `detectUnitCycle` without a second scheduling mechanism. It is kept
    out of `GroupMeta.BoundaryOutputs` itself — that field stays "derived
    purely from real member-level edges" exactly as the investigations found;
    the synthetic edge lives in the new `GroupMeta.ErrorOutputs` field instead,
    which is what makes the "non-fatal branch reaches `downstreamUnitArrivals`
    with no legal exit" problem no longer apply — there IS now a legal exit,
    it is just not a `BoundaryOutputs` one.
  - `commitGroup` (`engine/group_exec.go`) takes the fatal/routing decision as
    a caller-supplied `routeToErrorOutput bool` rather than re-deriving it
    (see below for why), and when true: skips the real `exits` the executor
    reported entirely (a terminal member failure means the member subgraph
    never reached its normal boundary outputs), activates every `ErrorEdge`
    unconditionally in `downstreamUnitArrivals`, and appends one synthetic
    `GroupExitResult{NodeIdx: -1, NodeName: meta.Name, Port: "error", Data:
    groupErrorOutputData(...)}` to the commit's `Exits`. The payload is
    `{"group": <name>, "error": {"message": <errMsg>}}` — the same shape
    `engine/errorpolicy.go`'s `buildErrData` uses for a node's own
    `error_output`, stored under the GROUP's own name (not any member's) so a
    downstream node reads it via `$('g').json.error` exactly like a node's
    output: `GetOutput`/`outputKey` are keyed by a bare name string in every
    backend, never by node index, so there is nothing group-specific a lookup
    needs to know. **The payload also carries a `failed_members: []any` key
    when the caller has that identity to offer** (added 2026-10-08, closing the
    gap this section used to describe): `execution/subgraph.Result` gained a
    `FailedMember` field set from the same `engine.ObservedNodeFailure` its
    `Error`/`Permanent` fields were already derived from
    (`failureCapture.fatal()` in `execution/subgraph/subgraph.go`'s `Execute`);
    `service/runner/group_runtime.go`'s `ExecuteRequest` copies it onto
    `engine.GroupResult.FailedMember`; `engine/group_lease.go`'s
    `CommitGroupResult` passes it through to `commitGroup`, which hands it to
    `groupErrorOutputData` as a `failedMember string` parameter. The key is
    added to the payload only when `failedMember != ""` — a submit-time
    failure (the package never reached a member), a timeout, a cancellation,
    or the local `GroupExecutor` test fake (which has no such identity to
    offer) all omit it exactly as before this field existed; the omission
    reads as "unknown", never as "no member failed". The value is `[]any`
    (holding one string), not `[]string`: this map is stored as output Data,
    and the Redis backend (`rstate`) round-trips it through JSON, which
    decodes a JSON array back into `[]any` — the memory backend keeps
    whatever Go value was stored, with no such round trip. A `[]string`
    literal would read back as `[]string` on memory and `[]any` on Redis,
    which an `expr`/function node doing a type assertion or a
    `DeepEqual`-style comparison against this field would see differently
    depending on which backend committed the group; `[]any` is already that
    shape, consistently, on both. The field holds one name today because the
    single-attempt commit path only ever has one failing member to report,
    but the shape leaves room for a future caller that collects more than
    one without a breaking change. `engine.GroupResult.FailedMember` (the
    singular string one layer up) is the structured source this payload's
    `failed_members` is built from, not the same field under a different
    name. It is carried end to end over `service/protocol`'s
    `GroupResultWire`/`ReportResultRequest` wire conversions on both runner
    transports: HTTP always has, and gRPC since the `group_result_json`
    field was added to `runnerpb.ReportResultRequest` (previously gRPC's
    `ReportResultRequestToProto`/`FromProto` dropped `GroupResult` entirely,
    so a gRPC-reported group result committed through the ordinary node path
    and was rejected as a misleading stale lease token rather than reaching
    `CommitGroupResult` at all).
  - **The commit's reported `Outcome` is `GroupOutcomeSuccess`, not
    `GroupOutcomeFailed`, when the engine decided the failure is non-fatal.**
    This was the one piece the investigations did not have visibility into
    because it lives in the backends, not the engine:
    `backend/providers/local/group_state.go`,
    `backend/providers/distributed/internal/rstate/group_state.go`, and
    `backend/providers/local/entry_admission.go` all increment their
    failed-unit counter whenever `Outcome == GroupOutcomeFailed`, regardless
    of `Fatal`, and a non-zero failed count finalizes the WHOLE EXECUTION as
    Failed once the remaining-unit counter reaches zero — even for a unit this
    engine itself decided was non-fatal. This was first fixed for a routed
    `error_output` commit, and then — closing a gap that commit left open —
    extended to EVERY non-fatal failure, including a tolerated `on_error:
    continue` member failure that never routes anywhere:
    `commitGroup` (`engine/group_exec.go`) reports `GroupOutcomeSuccess`
    whenever `fatal` is false, not only when `routeToErrorOutput` is true.
    `continue` and `error_output` share the same contract this mirrors:
    `ApplyOnError` sets `NodeStatus: types.NodeStatusSuccess` (`error_output`)
    or `types.NodeStatusContinued` (`continue`), never `NodeStatusFailed`, for
    exactly this reason — the node/group "handled" the error, so the
    execution must not be failed by it. The real failure is not lost: it
    still travels as `GroupCommitRequest.Error` (and, for `error_output`, as
    the synthetic exit's payload); it is simply not counted as a terminal
    execution failure. A tolerated `continue` failure stays observable
    through the `xflow_group_commit_total{outcome="failed_tolerated"}` metric
    (`commitGroup`'s `notifyGroupCommit` call) and through whatever real
    exits the executor managed to produce before the failure — unlike
    `error_output`, `continue` has no declared target to route to, so it
    reports exactly those real exits (possibly none, if the group failed
    before producing any) rather than a synthesized error edge; the
    downstream fan-out follows from whichever of those exits actually fired,
    the same unit-arrival mechanism `error_output` uses for its synthetic
    edge.
  - **`fatal` and `routeToErrorOutput` are caller-decided, never re-derived
    inside `commitGroup`.** The two production callers disagree in one case:
    `executeGroup`'s `GroupExecutor.ExecuteGroup` only ever reports an
    ordinary handler failure, so group-level `OnError` governs its fatality
    unconditionally. `CommitGroupResult`'s remote wire path carries a distinct
    canceled/timeout `GroupOutcome` classification
    (`GroupOutcomeTimeout`/`GroupOutcomeCanceled`) that must stay fatal
    regardless of `on_error: continue`/`error_output` — a cancellation is not
    the failure `OnError` exists to tolerate or route. An earlier draft of
    this feature had `commitGroup` unconditionally overwrite the caller's
    `fatal` with `groupOnErrorFatal(meta.OnError)` whenever `execErr != nil` —
    true for every non-success outcome, including canceled/timeout — which
    would have silently downgraded a cancellation to a routed/tolerated
    non-fatal commit on any group configured with `on_error: continue` or
    `error_output`. `TestCommitGroupResult_CanceledOutcomeIsFatalDespiteErrorOutput`
    (`engine/group_error_output_test.go`) pins that this stays fatal and
    unrouted.
  - `GroupMeta.ErrorOutputs` (a plain `[]BoundaryEndpoint`) travels inside
    `GroupMeta`, which already rides the existing `Groups []GroupMeta`
    serialization path (`graphHashPayload`, `graphSerializedForm`) with no new
    wire plumbing: it enters the graph hash automatically, round-trips through
    snapshot `MarshalJSON`/`UnmarshalJSON` as part of `g.groups`, and is
    rebuilt deterministically by `buildUnits`/`buildUnitEdges` on every
    decode, the same way `BoundaryOutputs` always has been.
  - Runtime coverage: `engine/group_error_output_test.go` (member failure
    routes to the error target with the success-path downstream skipped and
    the execution finishing successfully; the ordinary success path is
    unchanged by the feature's presence; a terminal failure after exhausted
    retries still routes identically — `commitGroup` does not distinguish "the
    only attempt" from "the last of several," since `GroupMeta.Retry` has no
    runtime enforcement to begin with, see `validateGroupRetry`; and
    cancellation/timeout stay fatal and unrouted on both the local-executor and
    remote `CommitGroupResult` entrypoints). `test/integration/group_binary_e2e_test.go`'s
    `TestGroupBinaryE2E_OnErrorOutputRoutesOnMemberFailure` exercises the same
    contract against a real server, runner, and Redis backend.
  - `on_error: continue` coverage: `engine/group_continue_test.go` (a
    `continue`d member failure completes the execution successfully instead
    of finalizing Failed through the failed-unit counter; the commit still
    carries `Fatal: false` and the real error message; cancellation stays
    fatal despite `continue`, mirroring the `error_output` cancellation
    pin). `backend/internal/statestoretest/group_state_contract.go`'s
    `NonFatalSuccessOutcomeWithErrorDoesNotFailExecution` pins the shared
    backend mechanism (`Outcome: GroupOutcomeSuccess` with `Fatal: false`
    and a non-empty `Error` must not fail the execution) against both the
    local and Redis/Lua backends, including the real-Redis contract run.
    `test/integration/group_binary_e2e_test.go`'s
    `TestGroupBinaryE2E_OnErrorContinueCompletesExecution` exercises the same
    contract against a real server, runner, and Redis backend.

  <details>
  <summary>Provenance: the two prior investigations that scoped this before it was built</summary>

  Two independent investigations (2026-08-11) found there is **no group-level error
  port to wire to** — this was a new mechanism, not a blank to fill:

  - `GroupMeta.BoundaryOutputs` (`engine/graph/unit.go`, built by
    `buildUnitEdges`) is derived purely from real member-level edges that cross
    the group boundary. Nothing synthesized an entry into it.
  - `compileOneGroup` (`engine/graph/group_compile.go`) stored `GroupDef.OnError`
    as a plain string and never read it to manufacture an edge or port.
  - `CommitGroupResult` (`engine/group_lease.go`) validated every exit against
    `(nodeIdx, port)` pairs in `BoundaryOutputs`. A fabricated "group failed"
    exit was rejected as an invalid boundary output.

  So narrowing `groupOnErrorFatal` to `OnErrorStop` alone would not have enabled
  routing — it would have **stranded the failure**: the non-fatal branch reached
  `downstreamUnitArrivals` with no legal exit to compute arrivals from, leaving
  a group that was neither fatal nor advancing.

  **2026-08-14: `validateGroupOnError` was changed to reject the two output
  policies — and any unknown value — in `compileGroups`.** The value was
  previously accepted and run as `stop`, so an author who asked for the failure
  to be routed to a downstream branch got the whole execution failed instead,
  with no diagnostic, on the path least likely to be exercised before
  production. `OnError` had no validation at all, so a typo (`fail`, which
  `types/group.go`'s own doc comment warns does not exist, or `error-output`)
  degraded the same way. A group accepted only `""`, `"stop"`, `"continue"`.

  The gate lived in `graph.Compile`, not in the builder: `GroupRef.OnError`
  takes a `types.OnError`, so `types.OnErrorOutput` is a type-legal argument
  and nothing in the SDK's assembly half could refuse it. `AddWorkflow` is the
  only production path to a compiled graph, so that is where it was caught.

  Snapshot decode (`Graph.UnmarshalJSON`) deliberately did **not** apply the
  validation — a graph persisted by a writer predating the gate would otherwise
  become undecodable mid-rolling-upgrade; `groupOnErrorFatal`'s catch-all is
  fatal, the safe reading of a value it cannot honor. This remains true: the
  implemented mechanism above still only validates `OnError`/`ErrorOutputs` at
  `graph.Compile`, not at decode.

  </details>
- **`main_output` on a group remains rejected, by design, not by remaining
  scope.** Unlike a node — which has its own successful output to merge the
  error into — a group that failed before committing has no single member
  output to stand in for "the group's main result." There is no
  `error_outputs`-shaped escape hatch for it the way there is for
  `error_output`; building one would mean inventing a payload with no
  principled source, which is the same objection the investigations raised
  for the mechanism as a whole, just not resolvable by giving it a target
  list.

- **Activation replica count > 1 per entry unit** (spec §11.6 explicit-replica
  scaling): implemented (2026-08-28, `0d30009`, `8e1aa3e`; graph IR and rstate
  replica plumbing 2026-09-01, `5520a01`). `ActivationReplicas` flows through
  the graph IR and snapshots; `EntryActivationManager` emits one activation per
  declared replica (zero and one both mean one); the reconciler only assigns
  `ReplicaIndex > 0` to runners advertising `FeatureEntryActivationReplicaV1`.
  This entry previously said one active hosting runner per entry unit.
- **Full runner→control activation ACK RPC.** The retired path's ACK was dead
  code; renewal is now via reconnect inventory + proactive reconcile. A dedicated
  ACK RPC is future work if tighter delivery confirmation is needed.
