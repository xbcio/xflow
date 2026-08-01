# Supply Node — Server-Driven Shared Config (D-supply)

> Status: Implemented (Task 1–19 of the 2026-07-30 supply-node branch). Pull-mode
> collection (a `.http` supply node that a runner actively fetches on a schedule)
> is **not implemented** — see §9.
> Related: [WASM-ENGINE-POOLING.md](./WASM-ENGINE-POOLING.md) (the primary
> consumer today), [NODE-GROUP-COLOCATION.md](./NODE-GROUP-COLOCATION.md)
> (the activation/reconciler machinery supply reuses), [DEPLOYMENT-TOPOLOGIES.md
> §4.5](./DEPLOYMENT-TOPOLOGIES.md) (the gRPC heartbeat transport gap).
> Code: `types/workflow.go` (`NodeKindSupply`, `DependencyEdge`),
> `engine/graph/dependency.go`, `engine/graph/unit.go`, `store/supply.go`,
> `node/supply/{supply.go,registry.go,params.go}`,
> `node/internal/supply/{external.go,static.go}`, `service/runner/supply_gate.go`,
> `service/control/{entry_activation_manager.go,entry_activation_reconciler.go,
> supply_hints.go}`, `service/apiserver/module_supply.go`,
> `node/internal/utils/exprx/exprx.go`.

## 1. What `NodeKindSupply` is, and is not

```go
// types/workflow.go:104-113
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

- `xflow.supply.external` (`node/internal/supply/external.go:48`) — content
  lives in a `SupplyResource` outside the workflow definition, written by an
  HTTP `PUT` to `/v1/supplies/{name}`.
- `xflow.supply.static` (`node/internal/supply/static.go:29`) — content is
  literal bytes carried in the node's parameters, part of the workflow
  definition itself.

Public constructors: `node.SupplyExternal(resource string)` and
`node.SupplyStatic(content []byte)` (`node/node.go:118-132`), paired with
`WorkflowBuilder.DependsOn(consumer, supply *NodeRef)` (`sdk/xflow/builder.go:220`)
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
  // sdk/xflow/workflow_registry.go:198-203
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

The compiled `Graph` (`engine/graph/graph.go:12-53`) has two layers:

- **Node layer**: `nodes []NodeMeta`, `index map[string]int`. Every node,
  including supply nodes, lives here. Supply nodes additionally get an entry
  in `supplyIndexes map[string]int` and (if referenced) `supplyRefs map[int][]string`
  (`graph.go:47-51`, both explicitly commented "Supply nodes live in the node
  layer only — never in g.units").
- **Unit layer**: `units []UnitMeta`, `nodeUnit []int`. This is the durable
  scheduling topology — the thing whose size drives execution completion.

```go
// engine/graph/graph.go:148
func (g *Graph) UnitCount() int { return len(g.units) }
```

`UnitCount()` seeds the completion counter on every execution — in-process
(`backend/providers/local/memory_state.go:113-123`,
`s.remaining[e.ID] = e.Graph.UnitCount()`) and Redis-backed
(`backend/providers/distributed/internal/rstate/state_execution.go:122-130`,
same seed). Every unit that finishes decrements it (`atomic_state.go:104` and
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
// engine/graph/unit.go:88-96 (buildUnits, Pass 1)
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

- `engine/graph/dependency.go:12-16,32-34` — `ErrSupplyInDataflow`: a supply
  node with any `Connections` edge is rejected at compile time, before the
  unit pass runs, so `buildUnitEdges` never has to index `unitOutEdges[-1]`.
- `engine/graph/group_compile.go:67-68` — a supply node is rejected as a group
  member.
- `engine/signal.go` (`resolveResumeIntent`) — a supply node cannot be the
  target of a resume/signal.
- `service/control/entry_seed_topology.go:110-119` (`entryUnitIndex`) —
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
  mode only: an operator or external system `PUT`s to `/v1/supplies/{name}`
  (`service/apiserver/module_supply.go`), or the content is literal bytes in a
  `xflow.supply.static` node. There is no runtime pull loop yet (see §9).
  Activation-time collection at the runner — fetching content this runner
  does not yet have — happens through `SupplyGate.Admit`
  (`service/runner/supply_gate.go:128`), which is exclusive per entry unit:
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
// store/supply.go:28-42
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
// types/workflow.go:136-143
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
// types/workflow.go:28
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
   `TestNoSupplyMeansNoWireOrHashChange` (`engine/graph/snapshot_supply_test.go:46`)
   pins exactly this. `omitempty` here is not cosmetic; it is the mechanism
   that keeps every pre-existing compiled-graph hash unchanged.

Compilation: `buildDependencyEdges(def *types.WorkflowDef, g *Graph) error`
(`engine/graph/dependency.go:24`) runs after `buildEdges` (so it can reject a
supply node that also appears in `Connections`) and before `buildUnits` (so no
invalid graph reaches the unit pass). It populates `g.supplyIndexes` and
`g.supplyRefs` (consumer node index → sorted supply names) and rejects:

- a supply node that has any dataflow edge (`ErrSupplyInDataflow`);
- a dependency edge naming an unknown node on either side;
- a dependency edge whose `Supply` target is not actually `NodeKindSupply`;
- a supply node depending on another supply node.

`WorkflowIdentity` canonicalization sorts `DependencyEdge`s by `(Node, Supply)`
(`sdk/xflow/workflow_identity.go:188-191`) so edge declaration order never
affects the hash.

## 5. Two version numbers: `Revision` vs `ContentHash`

Both live on `SupplyResource`, deliberately mirroring Kubernetes'
`resourceVersion` vs `observedGeneration` split (`store/supply.go:21-27`):

- **`Revision`** — monotonic, bumped on *every* write, even a byte-identical
  one. It is the **write-side CAS token**: `PutSupply`'s `ifMatch *uint64`
  parameter (`store/supply.go:50-57`) implements optimistic concurrency —
  `nil` unconditional, `*ifMatch == 0` create-only, `*ifMatch == N` write only
  if the current `Revision` is exactly `N`; a mismatch returns
  `ErrRevisionConflict` and leaves the row untouched.
- **`ContentHash`** — `sha256` of `Content`. It is the **read-side comparison**
  a consumer uses to decide whether anything needs rebuilding: identical
  content means no rebuild, even across a `Revision` bump.

The HTTP surface exposes both, using the header names their K8s analogues
would suggest (`service/apiserver/module_supply.go`):

- `PUT` (`handlePut`, lines 87-135): reads `If-Match` as the CAS token;
  `409` on conflict; response sets `ETag` to `ContentHash` and returns
  `revision`/`content_hash` in the body.
- `GET` (`handleGet`, lines 137-162): sets `ETag` to `ContentHash` and a
  custom `X-Supply-Revision` header to `Revision`; honors `If-None-Match` for
  `304 Not Modified`.

## 6. Activation-time gate

Three cold-start races collapse into one code path:

- **C1** — content was never written for this supply.
- **C2** — this runner just started and has never fetched anything.
- **C3** — a consumer registers before its supply's content has arrived (or
  vice versa; activation order between a supply and its consumer is not
  guaranteed).

All three reduce to the same operation: "fetch synchronously, right now, as
part of taking the activation" — `SupplyGate.Admit(ctx, workflowID, reqs)`
(`service/runner/supply_gate.go:128`). It fetches every required supply this
process does not already have, applies it to the distribution registry, and
either lets the activation proceed or returns `*NotReadyError` listing every
missing supply (not just the first one, so an operator does not need one
reconcile round per missing item).

**How supply requirements travel to the runner:** a `SupplyRequirement` is
attached per entry unit onto the same `EntryActivation` record used for
trigger hosting (`engine/entry_activation.go:67,134-142`):

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
  (`supply_gate.go:154-156,176-178,190-192`).
- `false` — the runner **takes** the activation anyway and the consumer runs
  with empty semantics (`supply_gate.go:157-159`), while
  `xflow_supply_unavailable_serving` is set to `1` for that name so the gap is
  observable.

**The removed third tier.** An earlier design considered a `default: <bytes>`
fallback — embed a stale/default rule set to serve when the external source
is unreachable. It was deliberately removed and never shipped. The reason is
recorded directly in the shipped code, not just in a design note:

```go
// node/internal/supply/static.go:14-16
// This is NOT a fallback for an external supply. The spec deliberately removed
// the `default: <bytes>` tier: serving live traffic with stale embedded rules
// produces wrong data that looks right, which is worse than not serving.
```

**This is not to be confused with** the wasm reactor's own three-tier
`Availability` ladder (`AvailUnavailable`/`AvailStale`/`AvailFresh`,
`node/internal/code/script/wasm/pool.go:457-470`) — that is a *consumer-side*
staleness signal for content already admitted through the gate, orthogonal to
the DSL-level `require_ready` decision. See
[WASM-ENGINE-POOLING.md §6.5](./WASM-ENGINE-POOLING.md).

## 7. `$supplies` vs `$config`

Both are expression roots built by `BuildExprEnv` (`node/internal/utils/exprx/exprx.go:93-124`):

```go
env["$config"]   = input.Config     // line 105
...
env["$supplies"] = supply.Default.Decoded()  // line 117
```

Three independent reasons they are separate roots, not one merged namespace
(`exprx.go:108-116`):

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
`Registry.decoded atomic.Pointer[map[string]any]` (`node/supply/registry.go:93-103`)
is rebuilt wholesale on every content change and swapped in with a single
atomic store; readers hold the reference lock-free. This is deliberate, and
the comment explains why a lazy view was not chosen:

```go
// node/supply/registry.go:98-102
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
lock-free readers (`registry.go:409-421`).

## 8. Two invariants written into the design, not just followed by convention

**Full-snapshot replace, never incremental.** Every content update — whether
from collection PUT, static declaration, or a re-fetch triggered by a
heartbeat hint — replaces the cached snapshot wholesale
(`node/supply/registry.go:93-102,415-421`). There is no diff/patch type
anywhere in `node/supply`. This was a deliberate design choice, not an
oversight: an incremental-update model reintroduces the cross-instance
convergence problem that systems like Flink's Broadcast State have to solve
with careful barrier/checkpoint coordination — full-snapshot replace makes
convergence a property of "did this replace land" rather than "did every
delta land in the right order."

**Supply is a lookup, not a trigger.** A content change creates no execution
and dispatches no task — `Registry.Apply`'s doc is explicit: *"It never
creates an execution or dispatches a task"* (`registry.go:142-143`), and the
package doc reinforces it: *"the whole package is deliberately invisible to
the execution lifecycle... touches neither the remaining counter nor unit
in-degree"* (`node/supply/supply.go:1-9`). A content change never replays or
backfills messages already processed on the old content — messages in flight
finish against the old pool; only messages *after* the swap see new rules
(`node/internal/code/script/wasm/supply_consumer.go:27-30`).

## 9. Known gaps and costs

This section is mandatory and this document will go stale the moment any of
these get fixed without an edit here.

**(a) A gate-declined activation does not self-heal. Only a runner restart
recovers it today.** `EntryActivationReconciler.reconcileOne`'s
already-assigned branch renews a runner's lease based **only** on
`!expired && ownerMatches` (lease not expired, runner alive, selector still
matches) — `service/control/entry_activation_reconciler.go:202-226`. It never
checks whether that runner's `Activate` call (and therefore
`SupplyGate.Admit`) actually succeeded:

```go
// service/control/entry_activation_reconciler.go:202-206
if !expired && ownerMatches {
    if !act.LeaseDeadline.IsZero() && act.LeaseDeadline.Sub(now) < r.cfg.RenewThreshold {
        // renew lease
    }
    return nil
}
```

If `Activate` returns an error because `SupplyGate.Admit` declined (missing
required content), the reconciler has no way to learn that and no reason to
retry `Activate` — it just keeps renewing the same lease forever. The wire
type that was meant to close this loop, `protocol.ActivationAck` /
`ActivationAckPath` (`service/protocol/activation.go:9,71-79`), exists **only
as a type definition** — grepping the repo turns up zero production callers
in `service/control`, `service/runner`, or any route registration. **If this
document ever implies that periodic reconcile alone recovers a declined
activation, that implication is false.** The only recovery path today is a
runner restart, which forces `ReconcileRunnerInventory` to revoke the stale
assignment and the next reconcile pass to reassign at a fresh generation.
This is proven, not assumed: `test/integration/supply_gating_test.go`'s
`TestSupplyGateLosesNoMessages` explicitly drives several `Reconcile()` passes
with the gate still declining and asserts the activation `Generation` does
**not** advance, then restarts the runner and asserts it does;
`TestSupplyGateRecoversOnRestart` isolates the same assertion on its own so a
future regression confined to the retry path fails independently of the
message-accounting assertions.

**(b) gRPC transport carries no supply hint — and, separately, no activation
directive at all.** `runnerpb.HeartbeatResponse` has exactly one field:

```protobuf
// service/protocol/runnerpb/runner.proto:73-75
message HeartbeatResponse {
  int64 server_time = 1;
}
```

`service/control/grpc_server.go`'s `Heartbeat` handler only fills
`ServerTime` on the proto response, even though the HTTP-transport
`protocol.HeartbeatResponse` carries both `Activations` and `SupplyHints`.
This is documented in detail, including the gRPC-activation gap that predates
supply entirely, in
[DEPLOYMENT-TOPOLOGIES.md §4.5](./DEPLOYMENT-TOPOLOGIES.md#45-传输差异gRPC-心跳不携带控制载荷) —
this document defers to that section rather than repeating it. The
correctness-relevant point for supply specifically: losing a hint is never a
correctness problem, only a latency one. Hints are computed by
`SupplyHinter.HintsForRunner` (`service/control/supply_hints.go`) purely as an
optimization; the real convergence guarantee is `SupplyGate.Admit` at
activation time plus the TTL-based re-check. Under gRPC, an already-hosted
consumer's content-refresh latency degrades from "one heartbeat interval" to
"one TTL poll interval" — never to "never."

**(c) Cross-runner pool swaps are not synchronized; the skew is observable,
not eliminated.** When a supply's content changes, each runner hosting a
consumer swaps to the new content independently, as its own hint/fetch lands.
Worst case the skew across runners is 10–20 seconds; at 12000 msg/s that is
roughly 120,000–240,000 records tagged with a mix of two rule versions during
the window. The design does not try to close this window — doing so would
need a global barrier that pauses the whole stream, which no comparable
system attempts for this kind of config swap. Instead every result is stamped
with `config_generation` (the `SupplyResource.Revision` that actually produced
it — see `node/internal/code/script/wasm/reactor.go:120-131`), so a downstream
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

None of (a)–(d) are blocking defects in what shipped; they are the costs this
design knowingly accepted. A future change that closes any of them (a
`.http` pull-mode collector, a wired `ActivationAck`, a gRPC proto update, or
a cross-runner swap barrier) must update this section, not just add a new one
next to it.
