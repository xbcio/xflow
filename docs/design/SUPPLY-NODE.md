# Supply Node — Server-Driven Shared Config (D-supply)

> Status: Implemented (Task 1–19 of the 2026-07-30 supply-node branch). Pull-mode
> collection (a `.http` supply node that a runner actively fetches on a schedule)
> is **not implemented** — see §9.
> Related: [WASM-ENGINE-POOLING.md](./WASM-ENGINE-POOLING.md) (the primary
> consumer today), [NODE-GROUP-COLOCATION.md](./NODE-GROUP-COLOCATION.md)
> (the activation/reconciler machinery supply reuses), [DEPLOYMENT-TOPOLOGIES.md
> §4.6](./DEPLOYMENT-TOPOLOGIES.md) (the gRPC heartbeat transport gap).
> Code: `types/workflow.go` (`NodeKindSupply`, `DependencyEdge`),
> `engine/graph/dependency.go`, `engine/graph/unit.go`, `store/supply.go`,
> `node/supply/{supply.go,registry.go,params.go}`,
> `node/internal/supply/{external.go,static.go}`, `service/runner/supply_gate.go`,
> `service/control/{entry_activation_manager.go,entry_activation_reconciler.go,
> supply_hints.go}`, `service/apiserver/module_supply.go`,
> `exprx/exprx.go`.

## 1. What `NodeKindSupply` is, and is not

```go
// types/workflow.go — NodeKindSupply constant block
const (
	NodeKindAction  NodeKind = "action"
	NodeKindTrigger NodeKind = "trigger"
	// NodeKindSupply marks a node that maintains long-lived shared data for
	// other nodes to read. A supply node never advances an execution: it is
	// registered in the graph and referable by dependency edges, but it is
	// deliberately excluded from the unit layer, so it never counts toward the
	// remaining-unit denominator.
	NodeKindSupply NodeKind = "supply"
)
```

A supply node is a **named, graph-visible declaration** that a workflow
consumes a piece of long-lived shared content — currently rules/config for a
wasm reactor, but the mechanism is content-agnostic. There are exactly two
supply node types today, and both are **declaration-only**: neither has an
`Execute` method nor a registered handler.

- `xflow.supply.external` (`node/internal/supply/external.go`, `External` 函数) — content
  lives in a `SupplyResource` outside the workflow definition, written by an
  embedder through the in-process SDK call (`sdk/xflow.Server.UpdateSupply` /
  `UpdateSupplyIfMatch`). HTTP `PUT /v1/supplies/{name}` used to be the write
  path; it is sealed (spec appendix Z.5) — the endpoint only serves GET now.
- `xflow.supply.static` (`node/internal/supply/static.go`, `Static` 函数) — content is
  literal bytes carried in the node's parameters, part of the workflow
  definition itself.

Public constructors: `node.SupplyExternal(resource string)` and
`node.SupplyStatic(content []byte)` (`node/node.go`, `SupplyExternal`/`SupplyStatic`), paired with
`WorkflowBuilder.DependsOn(consumer, supply *NodeRef)` (`sdk/xflow/builder.go`, `DependsOn`)
to declare which node reads which supply.

**What it is not:**

- **Not a trigger.** A trigger's multiplicity is "one external event creates
  one execution." A supply node creates zero executions, ever — it is a
  standing declaration, not an event source.
- **Not an action.** An action executes once per traversal and produces
  outputs on ports. A supply node has no `Outputs` in its `Descriptor()` (a
  supply node is never connected by a dataflow edge — see §4) and is skipped
  entirely by the handler pre-check that every action/trigger node type goes
  through:

  ```go
  // sdk/xflow/workflow_registry.go — NodeKindSupply case
  case types.NodeKindSupply:
      // A supply node's handler lives in the supply registry, not the
      // action registry. Falling through to default would look it up as an
      // action, miss, and fail closed with ErrMissingHandlerVersions.
      continue
  ```

There is no runtime dispatch loop that checks `if kind == supply { skip }` —
exclusion is structural, enforced at compile time (§2), not by a runtime
branch that could be bypassed.

## 2. The two-layer graph — the single highest-risk invariant in this design

The compiled `Graph` (`engine/graph/graph.go`, `Graph` struct) has two layers:

- **Node layer**: `nodes []NodeMeta`, `index map[string]int`. Every node,
  including supply nodes, lives here. Supply nodes additionally get an entry
  in `supplyIndexes map[string]int` and (if referenced) `supplyRefs map[int][]string`
  (`graph.go`, fields `supplyIndexes`/`supplyRefs`, both explicitly commented "Supply nodes live in the node
  layer only — never in g.units").
- **Unit layer**: `units []UnitMeta`, `nodeUnit []int`. This is the durable
  scheduling topology — the thing whose size drives execution completion.

```go
// engine/graph/graph.go — UnitCount
func (g *Graph) UnitCount() int { return len(g.units) }
```

`UnitCount()` seeds the completion counter on every execution — in-process
(`backend/providers/local/memory_state.go`, `s.remaining[e.ID] = e.Graph.UnitCount()`) and Redis-backed
(`backend/providers/distributed/internal/rstate/state_execution.go`,
same seed). Every unit that finishes decrements it (`engine/atomic_state.go` and
the equivalent Lua `DECR` scripts); the execution completes when it reaches
zero.

**Why a supply node must never become a unit:** nothing schedules a supply
node, dispatches it as a task, or calls the commit/decrement path for it — it
never runs. If it were counted into `UnitCount()`, `remaining` would include a
slot that nothing will ever decrement, and every execution of that workflow
would hang forever, silently, with no error. This is not a hypothetical: it is
exactly the failure mode the unit-layer exclusion exists to prevent.

The exclusion itself:

```go
// engine/graph/unit.go — buildUnits, Pass 1
// A supply node maintains long-lived shared data and never advances an
// execution. Keeping it out of the unit layer is what makes the
// remaining-unit denominator (UnitCount) unchanged: if it became a unit,
// nothing would ever complete it and every execution would hang forever.
// nodeUnit[i] stays -1; buildDependencyEdges has already rejected any
// dataflow edge touching it, so no unit edge can dereference that -1.
if g.nodes[i].Kind == types.NodeKindSupply {
	continue
}
```

A supply node's `nodeUnit[i]` therefore stays `-1` permanently. Several other
places had to be independently hardened so that `-1` never leaks into code
that assumes a valid unit index:

- `engine/graph/dependency.go` (`ErrSupplyInDataflow`) — a supply
  node with any `Connections` edge is rejected at compile time, before the
  unit pass runs, so `buildUnitEdges` never has to index `unitOutEdges[-1]`.
- `engine/graph/group_compile.go` — a supply node is rejected as a group
  member.
- `engine/signal.go` (`resolveResumeIntent`) — a supply node cannot be the
  target of a resume/signal.
- `service/control/entry_seed_topology.go` (`entryUnitIndex`) —
  returns `(-1, false)` rather than surfacing a raw unit index of `-1` when a
  supply node is looked up as an entry unit.

**Tests that guard this invariant** (all in the repo, all named for exactly
this property):

| File | Test |
|---|---|
| `engine/graph/unit_supply_test.go` | `TestSupplyNodeExcludedFromUnitLayer`, `TestSupplyDoesNotShiftUnitIndexes`, `TestBuildUnitsIsIdempotentForBoundaryEdges`, `TestSupplyNodeRejectedAsGroupMember` |
| `engine/graph/supply_refs_test.go` | `TestDeriveSupplyRefs`, `TestCompileRejectsSupplyUseWithoutEdge`, `TestCompileAcceptsSupplyUseWithEdge`, `TestCompileRejectsDynamicSupplyName`, `TestCompileRejectsSupplyNodeUsingSupplies` |
| `engine/graph/snapshot_supply_test.go` | `TestGraphSnapshotRoundTripsSupplyRefs`, `TestNoSupplyMeansNoWireOrHashChange` |
| `engine/signal_supply_test.go` | `TestResumeIntentRejectsSupplyNode` |
| `service/control/entry_seed_topology_supply_test.go` | `TestEntryUnitIndexRejectsSupplyNode` |

## 3. Two faces: collection vs distribution

The design has two structurally different halves, joined by one storage type.

- **Collection** — getting content *into* the platform. Today this is push
  mode only: an embedder writes through the in-process SDK call
  (`sdk/xflow.Server.UpdateSupply` / `UpdateSupplyIfMatch`, which reaches
  `Supplies.PutSupply`); the HTTP write verb (`PUT /v1/supplies/{name}`) is
  sealed (spec appendix Z.5) — `service/apiserver/module_supply.go` serves GET
  only now. Or the content is literal bytes in a
  `xflow.supply.static` node. There is no runtime pull loop yet (see §9).
  Activation-time collection at the runner — fetching content this runner
  does not yet have — happens through `SupplyGate.Admit`
  (`service/runner/supply_gate.go`, `Admit` method), which is exclusive per entry unit:
  exactly one runner is ever assigned to host a given entry unit's supply
  requirements (the same lease-fenced single-owner model
  [NODE-GROUP-COLOCATION.md](./NODE-GROUP-COLOCATION.md) describes for trigger
  activations — see §6 below for how the two now share machinery).
- **Distribution** — fanning content out to every in-process consumer that
  needs it, on the runner that already has it. This is `node/supply/registry.go`
  (`Registry`, in-process, `Apply`/`Decoded`/`Observed`) plus per-consumer
  registration (`RegisterConsumer`/`UnregisterConsumer`) and content-change
  push (`Consumer.OnSupplyChanged`, `node/supply/supply.go`). It is a
  **broadcast**: every registered consumer for a name is notified on every
  content change, unconditionally.

**`SupplyResource` is the structurally required handoff point between the
two.** Collection writes it (`Supplies.PutSupply`); the runner's fetch client
reads it (`SupplyFetcher.Fetch`) and feeds the result into the distribution
registry (`Registry.Apply`). Without a durable, versioned, namespace-scoped
resource in between, collection and distribution would have to negotiate a
transport-specific protocol per supply source — the store is what lets
"how content arrives" and "how content spreads inside one process" vary
independently.

```go
// store/supply.go — SupplyResource
type SupplyResource struct {
	Namespace string
	Name      string
	Content     []byte
	ContentType string
	Revision    uint64
	ContentHash string
	UpdatedAt   time.Time
	UpdatedBy   string
	LastFetchAt time.Time
	LastError   string
}
```

Stored via `store.Supplies` (`GetSupply`/`PutSupply`), backed today by
`store/memstore/supply.go` (in-memory) and `store/sqlstore/supply.go`
(GORM-persisted).

## 4. Dependency edges — a separate wire field, not an overload of `Connections`

```go
// types/workflow.go — DependencyEdge
// DependencyEdge declares that Node reads the shared data maintained by the
// supply node named Supply. It is deliberately separate from Connections:
// a dependency edge carries no data and takes no part in unit-edge
// construction, so it must not pollute the dataflow topology.
type DependencyEdge struct {
	Node   string `json:"node"`
	Supply string `json:"supply"`
}
```

```go
// types/workflow.go — WorkflowDef.DependencyEdges field
DependencyEdges []DependencyEdge `json:"dependency_edges,omitempty"`
```

It is a top-level `WorkflowDef` field, appended last, separate from
`Connections Connections`. Two reasons it is not folded into `Connections`:

1. **Pollution of unit topology.** `Connections` edges become `Edge`s that
   `buildUnitEdges` walks to build cross-unit scheduling edges. A dependency
   edge carries no data and must never generate a unit edge — a supply node
   has `nodeUnit == -1`, so treating a dependency edge like a data edge would
   dereference that `-1` (see §2).
2. **Hash stability of existing workflows.** `DependencyEdges` is appended as
   the last field of `WorkflowDef` and tagged `omitempty` specifically so that
   a workflow with zero supply usage serializes and hashes byte-identically
   to how it did before this feature existed —
   `TestNoSupplyMeansNoWireOrHashChange` (`engine/graph/snapshot_supply_test.go`)
   pins exactly this. `omitempty` here is not cosmetic; it is the mechanism
   that keeps every pre-existing compiled-graph hash unchanged.

Compilation: `buildDependencyEdges(def *types.WorkflowDef, g *Graph) error`
(`engine/graph/dependency.go`, `buildDependencyEdges`) runs after `buildEdges` (so it can reject a
supply node that also appears in `Connections`) and before `buildUnits` (so no
invalid graph reaches the unit pass). It populates `g.supplyIndexes` and
`g.supplyRefs` (consumer node index → sorted supply names) and rejects:

- a supply node that has any dataflow edge (`ErrSupplyInDataflow`);
- a dependency edge naming an unknown node on either side;
- a dependency edge whose `Supply` target is not actually `NodeKindSupply`;
- a supply node depending on another supply node.

`WorkflowIdentity` canonicalization sorts `DependencyEdge`s by `(Node, Supply)`
(`sdk/xflow/workflow_identity.go`, canonicalization logic) so edge declaration order never
affects the hash.

## 5. Two version numbers: `Revision` vs `ContentHash`

Both live on `SupplyResource`, deliberately mirroring Kubernetes'
`resourceVersion` vs `observedGeneration` split (`store/supply.go`, `SupplyResource` struct):

- **`Revision`** — monotonic, bumped on *every* write, even a byte-identical
  one. It is the **write-side CAS token**: `PutSupply`'s `ifMatch *uint64`
  parameter (`store/supply.go`, `PutSupply`) implements optimistic concurrency —
  `nil` unconditional, `*ifMatch == 0` create-only, `*ifMatch == N` write only
  if the current `Revision` is exactly `N`; a mismatch returns
  `ErrRevisionConflict` and leaves the row untouched.
- **`ContentHash`** — `sha256` of `Content`. It is the **read-side comparison**
  a consumer uses to decide whether anything needs rebuilding: identical
  content means no rebuild, even across a `Revision` bump.

The write side exposes both through the SDK call, not HTTP — `PUT
/v1/supplies/{name}` is sealed (spec appendix Z.5):

- `sdk/xflow.Server.UpdateSupply` / `UpdateSupplyIfMatch`: `UpdateSupplyIfMatch`
  takes the CAS token as `ifMatch`; `store.ErrRevisionConflict` on conflict;
  the returned `*store.SupplyResource` carries `Revision`/`ContentHash`. An
  embedder wraps this call with its own session, authorization, and audit.
- `GET` (`handleGet`, `service/apiserver/module_supply.go`): sets `ETag` to
  `ContentHash` and a custom `X-Supply-Revision` header to `Revision`; honors
  `If-None-Match` for `304 Not Modified`. This is the only verb the HTTP
  surface still serves.

## 6. Activation-time gate

Three cold-start races collapse into one code path:

- **C1** — content was never written for this supply.
- **C2** — this runner just started and has never fetched anything.
- **C3** — a consumer registers before its supply's content has arrived (or
  vice versa; activation order between a supply and its consumer is not
  guaranteed).

All three reduce to the same operation: "fetch synchronously, right now, as
part of taking the activation" — `SupplyGate.Admit(ctx, workflowID, reqs)`
(`service/runner/supply_gate.go`, `Admit` method). It fetches every required supply this
process does not already have, applies it to the distribution registry, and
either lets the activation proceed or returns `*NotReadyError` listing every
missing supply (not just the first one, so an operator does not need one
reconcile round per missing item).

**How supply requirements travel to the runner:** a `SupplyRequirement` is
attached per entry unit onto the same `EntryActivation` record used for
trigger hosting (`engine/entry_activation.go`, `SupplyRequirement` type and `EntryActivation.Supplies`):

```go
type SupplyRequirement struct {
	Node     string `json:"node"`
	Resource string `json:"resource"`
	RequireReady bool `json:"require_ready"`
}
```

**Two tiers, `require_ready`** — this is a binary switch, not a three-way
choice:

- `true` (the DSL default on both `xflow.supply.external` and
  `xflow.supply.static`) — the runner **declines** the activation if the
  supply has no usable content. Traffic stays in Kafka; the offset never
  advances; consumer-group lag is the operator-visible signal
  (`supply_gate.go`, `Admit` gate-declined branch).
- `false` — the runner **takes** the activation anyway and the consumer runs
  with empty semantics (`supply_gate.go`, `require_ready=false` branch), while
  `xflow_supply_unavailable_serving` is set to `1` for that name so the gap is
  observable.

**The removed third tier.** An earlier design considered a `default: <bytes>`
fallback — embed a stale/default rule set to serve when the external source
is unreachable. It was deliberately removed and never shipped. The reason is
recorded directly in the shipped code, not just in a design note:

```go
// node/internal/supply/static.go — package comment
// This is NOT a fallback for an external supply. The spec deliberately removed
// the `default: <bytes>` tier: serving live traffic with stale embedded rules
// produces wrong data that looks right, which is worse than not serving.
```

**This is not to be confused with** the wasm reactor's own three-tier
`Availability` ladder (`AvailUnavailable`/`AvailStale`/`AvailFresh`,
`node/internal/code/script/wasm/pool.go`, `Availability` type) — that is a *consumer-side*
staleness signal for content already admitted through the gate, orthogonal to
the DSL-level `require_ready` decision. See
[WASM-ENGINE-POOLING.md §6.5](./WASM-ENGINE-POOLING.md).

## 7. `$supplies` vs `$config`

Both are expression roots built by `BuildExprEnv` (`exprx/exprx.go`, `BuildExprEnv`):

```go
env["$config"]   = input.Config     // line 105
...
env["$supplies"] = supply.Default.Decoded()  // line 117
```

Three independent reasons they are separate roots, not one merged namespace
(`exprx.go`, `BuildExprEnv` comment block):

1. **Different mutability.** `$config` is immutable and travels with the
   definition version. A supply is mutable, versioned, and can be stale — a
   state a caller must be able to distinguish, which a merged namespace would
   erase.
2. **No sound collision policy.** If both fed the same top-level names,
   deciding which wins on a name clash has no answer that is not surprising
   in the other direction — there is no way to merge them that a reader could
   predict without also knowing which one shadows the other.
3. **Failure semantics.** `$supplies` failure (not ready, rejected) is a
   first-class gated state (§6); `$config` has none. Flattening them into one
   root would also flatten "this can be missing/stale" into "this is always
   present," which is false for supply content.

**Implementation shape: an eagerly predecoded shared map, not a lazy view.**
`Registry.decoded atomic.Pointer[map[string]any]` (`node/supply/registry.go`, `Registry.decoded`)
is rebuilt wholesale on every content change and swapped in with a single
atomic store; readers hold the reference lock-free. This is deliberate, and
the comment explains why a lazy view was not chosen:

```go
// node/supply/registry.go — decoded field comment
// Decoding happens here — once per content change — rather than per message.
// expr's runtime.Fetch offers no lazy hook for a custom type (it only tries
// MethodByName and struct fields), so a plain map is the only shape that
// resolves a dynamic $supplies.<name>. Publishing a shared reference keeps
// per-message cost at one map assignment regardless of content size.
```

In other words: `expr-lang`'s `vm/runtime.Fetch` can resolve a dynamic member
access (`$supplies.<name>` where `<name>` varies per node) against a
`map[string]any` or against a struct's fields/methods, but there is no hook
for a custom type to intercept an arbitrary key lazily. A plain map is
therefore the only shape that makes `$supplies.<name>` resolvable at all —
this is a **documented deviation from an original "lazy dereference at eval
time" design intent**, made because the expr runtime does not support it, not
a stylistic choice.

Cost of this shape: decode happens once per content change (not per message),
and the per-message cost is one pointer/map assignment regardless of content
size — `republishDecodedLocked` rebuilds the whole map rather than mutating
the live one in place, specifically because mutating in place would race with
lock-free readers (`registry.go`, `republishDecodedLocked`).

## 8. Two invariants written into the design, not just followed by convention

**Full-snapshot replace, never incremental.** Every content update — whether
from an embedder's SDK write, static declaration, or a re-fetch triggered by a
heartbeat hint — replaces the cached snapshot wholesale
(`node/supply/registry.go`, `Apply`/`republishDecodedLocked`). There is no diff/patch type
anywhere in `node/supply`. This was a deliberate design choice, not an
oversight: an incremental-update model reintroduces the cross-instance
convergence problem that systems like Flink's Broadcast State have to solve
with careful barrier/checkpoint coordination — full-snapshot replace makes
convergence a property of "did this replace land" rather than "did every
delta land in the right order."

**Supply is a lookup, not a trigger.** A content change creates no execution
and dispatches no task — `Registry.Apply`'s doc is explicit: *"It never
creates an execution or dispatches a task"* (`registry.go`, `Apply` doc comment), and the
package doc reinforces it: *"the whole package is deliberately invisible to
the execution lifecycle... touches neither the remaining counter nor unit
in-degree"* (`node/supply/supply.go`, package doc). A content change never replays or
backfills messages already processed on the old content — messages in flight
finish against the old pool; only messages *after* the swap see new rules
(`node/internal/code/script/wasm/supply_consumer.go`, `OnSupplyChanged`).

## 9. Known gaps and costs

This section is mandatory and this document will go stale the moment any of
these get fixed without an edit here.

**(a) A gate-declined activation now self-heals via ActivationAck + backoff +
reconcile.** Runner 的 `ActivationTracker.ProcessDirectives` 调用
`Activate` 失败后，通过 `SetOnActivateFailed` 回调
（`service/runner/activation_tracker.go`，`SetOnActivateFailed`，在 `t.mu` 释放后同步调用）
将失败逐条交给 `activationAcker.ackFailed`
（`service/runner/activation_acker.go`，`ackFailed`）。`ackFailed` 按
`(WorkflowID, WorkflowVersion, EntryUnitID)` 记最高 generation 去重
（`shouldAck`，`activation_acker.go`，`shouldAck`），然后 fire-and-forget POST
`protocol.ActivationAckPath`（`/v1/runners/activation/ack`），10s 超时是该
goroutine 唯一的生命周期上界。

Server 端 HTTP handler 与 `register` 同形，使用 `AuthenticateOngoing` 鉴权
（`service/control/core.go`，`AuthenticateOngoing`）；namespace 取自**服务端权威的 runner 注册记录**
（`runnerNamespaces`，`core.go` 中 `runnerNamespaces` 定义），绝不取自
客户端 body。处理流程
（`MarkActivationFailed`，`entry_activation_reconciler.go`，`MarkActivationFailed`）：

1. `Store.Get` 精确定位（不是 List/扫描）。
2. 校验 `act.RunnerID == runnerID && act.Generation == ack.Generation`——stale
   ack 静默忽略，不会破坏更新的健康分配。
3. `Store.Fence` 清空 `runner_id`。
4. `noteActivationFailure` 登记退避时间戳。
5. **不做 `Assign`、不加 leader 门控**——由 leader 的周期 reconcile 看到
   unassigned activation 后走正常分配路径重派。

退避策略（`noteActivationFailure`，`entry_activation_reconciler.go`，`noteActivationFailure`）：
per-key、**内存、不持久化**（与 `noMatchSince` 同一把 `r.mu`，每轮
`pruneRetryBackoff(seen)` 剪枝）；初值 10s
（`DefaultActivationRetryBackoffMin`）、每次翻倍、上限 5min
（`DefaultActivationRetryBackoffMax`）、**无重试上限**（封顶后维持 5min 一次，
永不放弃）、±20% 抖动（`jitter`，`entry_activation_reconciler.go`，`jitter` 函数；
防止共享 supply 挂掉时上千个 activation 齐发的同步脉冲）。清除点在
**renew 分支**（`clearRetryBackoff`，`entry_activation_reconciler.go`，`clearRetryBackoff`）：
`Assign` 成功不等于被 runner 真正接纳，只有下一轮确认 owner 存活且匹配才算成功；
再次失败时退避从已有档位继续翻倍。

已知代价（此机制 knowingly 接受的降级）：

- **leader 切换丢失退避状态**，导致一次立即重试——远优于为此引入持久化。
- ack 路径不经 leader 门控，**非 leader 副本写入的退避时间戳对 leader 不可见**——
  最坏是 leader 少看到一次失败记录，下一轮 reconcile 仍会重派并重新收到 ack。
- `protocol.ActivationAck` 的 `WorkflowVersion`（`activation.go`，`ActivationAck.WorkflowVersion` 字段）与
  `AuthToken`（`activation.go`，`ActivationAck.AuthToken` 字段）此前已定义但零接线，本次**首次获得生产调用
  点**。空 `WorkflowVersion` 被当作格式非法请求（`ErrMissingWorkflowVersion` →
  400），**不是**向后兼容路径：ack 能力与该字段是同一特性的两半、同批引入，不存在
  只实现前者的 runner。
- **gRPC 传输没有 ActivationAck 的 RPC/proto 定义**，因此 gRPC-only 部署下
  ack 无处可发、静默丢弃、fence 永不发生，退化为「只能重启 runner」——这不是
  延迟问题，是**自愈能力的完全缺失**。这是 gRPC 目前**唯一**的控制面缺口
  （心跳载荷已补齐，见 §9(b)），
  在 [DEPLOYMENT-TOPOLOGIES.md §4.6](./DEPLOYMENT-TOPOLOGIES.md#46-传输差异gRPC-缺-ActivationAck)
  已追加记录。

测试支撑：`test/integration/supply_gating_test.go` 的
`TestSupplyGateRetriesWithoutRestart` 证明完整闭环（gate decline → ack → fence →
backoff → supply 恢复 → reconcile 重派 → activation 成功接纳）。

**(b) gRPC transport now carries both supply hints and activation directives.**
`runnerpb.HeartbeatResponse` has four fields:

```protobuf
// service/protocol/runnerpb/runner.proto — HeartbeatResponse
message HeartbeatResponse {
  int64 server_time = 1;
  map<string, string> supply_hints = 2;
  string supply_key_rotation = 3;
  bytes activations_json = 4;
}
```

`service/protocol/grpc_conv.go` (`HeartbeatResponse` round-trip) round-trips all four (activations, like
leases, travel as JSON bytes to avoid modeling `map[string]any` params in
proto), `service/control/grpc_server.go` fills them on the response, and
`grpc_client.go` decodes them back on the runner. An earlier revision of
this document described the proto as having "exactly one field"; that is no
longer true.

The remaining gRPC gap is `ActivationAck` — see (a) above and
[DEPLOYMENT-TOPOLOGIES.md §4.6](./DEPLOYMENT-TOPOLOGIES.md#46-传输差异gRPC-缺-ActivationAck).
It is a self-healing gap, not a supply-latency one.

Supply-specific note that stands regardless of transport: losing a hint is
never a correctness problem, only a latency one. Hints are computed by
`SupplyHinter.HintsForRunner` (`service/control/supply_hints.go`) purely as an
optimization; the real convergence guarantee is `SupplyGate.Admit` at
activation time plus the TTL-based re-check. If hints are lost — partition,
runner restart, server leadership change — an already-hosted consumer's
content-refresh latency degrades from "one heartbeat interval" to "one TTL poll
interval," never to "never."

**(c) Cross-runner pool swaps are not synchronized; the skew is observable,
not eliminated.** When a supply's content changes, each runner hosting a
consumer swaps to the new content independently, as its own hint/fetch lands.
Worst case the skew across runners is 10–20 seconds; at 12000 msg/s that is
roughly 120,000–240,000 records tagged with a mix of two rule versions during
the window. The design does not try to close this window — doing so would
need a global barrier that pauses the whole stream, which no comparable
system attempts for this kind of config swap. Instead every result is stamped
with `config_generation` (the `SupplyResource.Revision` that actually produced
it — see `node/internal/code/script/wasm/reactor.go` (`ConfigGenerationKey`)), so a downstream
warehouse can group by `(message_key, config_generation)`, identify rows
produced under an older revision, and recompute exactly those. This is a
deliberate "make it traceable, not invisible" trade, matching
[WASM-ENGINE-POOLING.md §6.3](./WASM-ENGINE-POOLING.md#63-配置变更协议核心b-案配置版本--实例代次).

**(d) The gate converts immediate silent loss into delayed loss with an
alerting window — it does not eliminate loss.** Before this design, a
consumer with no config would either crash or silently pass everything
through unfiltered the moment it started; there was no gate at all. Now, with
`require_ready:true`, a runner that cannot get content simply never takes the
activation — Kafka retains the backlog and consumer-group lag climbs
immediately and visibly. But retention is finite. **If a supply stays
not-ready long enough for the topic's retention window to expire, those
messages are gone**, exactly as they would have been under any other
sustained-outage scenario. The gate buys an alerting window sized by
retention, not immunity from loss — an operator must size Kafka retention and
the alert threshold on consumer-group lag together, with the explicit
understanding that "gate is declining" must page a human well before
retention runs out. This is a genuine trade-off this design makes, not an
oversight: declining immediately is strictly safer than serving wrong data
(see §6's discussion of the removed `default:` tier), but it is not free.

(a) 已关闭（`fix/activation-ack-retry`）。(b)–(d) 仍是 knowingly accepted 的代价，
不是 blocking defect。未来关闭其中任何一项（`.http` pull-mode collector、gRPC
proto 更新、cross-runner swap barrier）必须更新本节，不是在旁边加新一节。

## 10. Supply 内容加密

三层密钥，各自匹配自己的可靠性等级：

| 层 | 来源 | 保护 | 丢失后果 |
|---|---|---|---|
| KEK | `XFLOW_MASTER_KEY` 或 `--master-key-file`（0600） | 派生 DEK | 需重发 + 重包 DEK |
| DEK | KEK 经 HKDF 派生（info `xflow-supply-content-v1`） | MySQL 中的 content 列 | 随库备份走 |
| 传输 key | server 生成，经 Redis `SET NX` 共享 | server→runner 响应体 | 重发，自愈 |

**ContentHash 始终基于明文。** 密文只写入 `content` 列。hash 若算在密文上，
AES-GCM 的随机 nonce 会让同样的内容每次产生不同的 hash，幂等判重与
consumer 侧「hash 未变不重建」同时失效。守护测试见
`store/storetest/supply.go` (`TestSupplyContentHashBasedOnPlaintext` 等)。

**KEK 不存 MySQL、不写死在源码里。** 前者让密钥与它保护的数据在同一份 dump
里；后者进 git 后永久不可撤销、随二进制分发到每个 runner、轮换需要发版加
全量重新加密。

**加密边界是分层而非端到端**：server 写入时用 DEK 加密落库，发给 runner 时
解密后用传输 key 重新加密，中间在内存里过一道明文。这是为保留 ContentHash
语义而接受的取舍。

**production 模式缺 KEK 拒绝启动**；dev 模式允许，落库明文并打 stderr 警告。

**运维注意**：KEK 只影响 `cmd/server`，`cmd/runner` 完全不涉及 KEK。`--mode=production`
下 server 在启动阶段（`apiserver.New` 的生产姿态校验，见
`service/apiserver/production.go` 的 `supply-encryption-at-rest` 一项）检测到
`XFLOW_MASTER_KEY` 未设置时会
**立即返回错误退出**——server 进程不会拉起，依赖它的后续流程会中断，表现为「管道
中途挂掉」而非「启动即报 KEK 缺失」（进程已不存在，日志往往被淹没）。dev 模式
（默认）只打 stderr 警告，不拒绝启动。生产环境与使用 `--mode=production` 的集成
测试环境，必须在启动 server 前设置好 `XFLOW_MASTER_KEY`（或 `--master-key-file`）。

同一条检查对 SDK 嵌入者同样生效：嵌入者通过
`xflow.WithServerProduction(apiserver.ProductionDeclaration{SupplyEncryptionAtRest: ...})`
声明 store 构造时是否带了静态加密。`store.Store` 接口不暴露这件事，所以只能由
构造它的那一方来说。

### 10.1 传输 key 轮换

**调度**：`ControlPlane.Start` 起一条轮换协程（仅在启用 supply 加密时），每
30s 一跳（`supplyKeyRefreshPeriod`）。每跳先 `Refresh` 采纳别的副本已轮换到
的 key，再尝试 `SET NX` 抢一个 TTL 恰等于轮换周期的槽位
（`xflow:supply:transport-key:rotation-slot`），抢到才 `Rotate`。用租约而不是
leader 门控，是因为这里要的语义是「整个集群每周期至多一次」，而 leader 回答的
是「谁来做」——TTL 即周期的租约直接给出前者。周期默认 24h
（`DefaultSupplyKeyRotationPeriod`），经 `--supply-key-rotation` 配置，负值关闭
轮换，正值下限 1 分钟。启动时先播一次种，使第一次轮换落在启动后一个完整周期。

**副本间可见**：`Rotate` 先把新 key 写回 Redis，再换本地 `current`。顺序不能
反——反了就是「用一个同伴拿不到的 key 加密」；这个顺序下最坏是「同伴已经有了
但本副本还没换」。Redis 写失败则整个轮换是空操作，本地 key 不动。`Refresh` 在
Redis 上没有该 key 时返回错误而不是重新生成：两个副本各自补一把就会分叉，正是
共享 key 要消除的故障。

**投递是收敛式的，不是推送式的**：runner 每次心跳带上它当前持有的 key ID
（`HeartbeatRequest.SupplyKeyID`，即 SHA-256 前 4 字节的十六进制指纹，不是密钥
材料），server 的 `RotationForHolder` 只在这个 ID 与自己的 current 不同时才回
一把 key。因此服务端不需要 per-runner 状态；当时宕机的、重启的、后加入的
runner 都在下一次心跳自愈。

这个方向不能反过来。`Keyring` 只有两格 `[new, old]`，重复投递同一把 key 会把
刚提升的那把降级、把上一把挤掉，于是轮换前加密的内容全部解不开。收敛式投递让
重复投递在结构上不可能发生：已收敛的 runner 报的 ID 与 current 相同。

`SupplyKeyID` 为空是**刻意的空操作**——不带该字段的旧 runner 与已收敛的 runner
无法区分，对空值推送轮换就正是上面那种毁 keyring 的重复投递。

日志只带 `key_id`，绝不带 key 本身；`supplyenc.Key` 的 `String()`/`GoString()`
都返回 `supplyenc.Key(redacted)`。

**未做**：无 KMS 集成，KEK 由部署方注入。
