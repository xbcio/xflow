// Package graph produces the immutable compiled IR that the engine schedules.
// It is the first station in the execution pipeline:
//
//	[engine/graph]  Compile(WorkflowDef) → *Graph (immutable, lock-free)
//	      │
//	      ▼
//	[engine]  scheduling algorithm (in-degree, lease, result verdict, outbox)
//
// The compiled *Graph is handed to engine.Submit or engine.Invoke and then
// cached process-wide. Every concurrent execution that references the same
// workflow version reads from that single shared pointer with no locking.
//
// # Position in the main execution line
//
// graph has no upstream dependency within xflow (it imports only types and
// exprx). Downstream packages that READ the graph are engine (scheduling),
// execution (parameter evaluation, body dispatch), and service/control
// (activation package projection and hash verification).
//
// # Core invariants and boundaries
//
// Immutability: once Compile returns, the *Graph must never be mutated.
// NodeAt returns a defensive deep copy. Vars and Config clone their maps.
// EvaluateActivationParams writes to a NEW map and never touches the Graph.
//
// No IO: graph performs no network calls, file access, or synchronisation
// beyond the expression-program LRU cache inside exprx.CompileExpr.
//
// # Key flow: what Compile does
//
//	Compile(WorkflowDef)
//	      │
//	      ├─ reject reserved xflow.group_* types and bare xflow.subgraph
//	      │
//	      ├─ registerNodes — build g.index / g.nodes; record xflow.start and
//	      │    trigger nodes as entry indexes; validate body shape per node
//	      │
//	      ├─ validateGraphValueDomain / validateTemplateForm
//	      │    └─ rejectMalformedTemplate: "${{ }}" must wrap whole value
//	      │
//	      ├─ buildEdges — materialise Connections into outEdges/inEdges/inDegree;
//	      │    route dependency edges separately (supply nodes stay off dataflow)
//	      │
//	      ├─ buildDependencyEdges — populate g.supplyRefs per consumer node
//	      │
//	      ├─ projectNodeBodies — for every node whose parameters carry a
//	      │    "body" sub-graph (shape-detected by declaresSubgraphBody, not
//	      │    by node type), compile it into a NodeBodyPackage with a
//	      │    canonical SHA-256 hash; supply visibility widens from parent
//	      │
//	      ├─ compileGroups — assign GroupMeta, UnitMeta two-layer IR
//	      │    └─ assignPackageHashes — ProjectGroupPackage per group,
//	      │         compute SubgraphPackage hash (enters graphHash)
//	      │
//	      ├─ detectCycle (Kahn's algorithm) — or require one xflow.start node
//	      │
//	      ├─ buildNodesRefs — extract $nodes['name'] references from params
//	      │
//	      ├─ buildUnits — build unit-level scheduling topology
//	      │
//	      └─ assignGraphHash — SHA-256 of the full compiled state
//
// Entry nodes (g.entryIndexes): xflow.start and nodes whose Kind ==
// NodeKindTrigger are recorded as named entry points for engine.Invoke.
// They are topology markers only — Compile does NOT start any listener,
// Kafka consumer, or HTTP server; that is the control-plane's job.
//
// Supply visibility (buildVisibleSupplies / g.supplyRefs): only the supply
// node NAMES are recorded in the compiled artifact. Supply content is fetched
// at activation time by the runner's gate; it must never enter a compile-time
// artifact, so the package hash does not drift when a supply's value changes.
//
// Sub-graph bodies (NodeBodyPackage): xflow.map and any future body-bearing
// node type store their compiled inner graph here. N map batches share one
// package and one hash, which lets execution/subgraph compile the body once
// regardless of batch count. The engine decides whether to expand by reading
// g.BodyAt(nodeIdx), not by sniffing payload keys.
//
// Projected packages (SubgraphPackage / CompileProjectedPackage): a
// co-location group or a map body is serialised into a self-contained
// SubgraphPackage and shipped to the runner as part of the directive. The
// runner re-compiles it via CompileProjectedPackage, which accepts the
// reserved xflow.group_exit type that user-authored WorkflowDefs cannot use.
// The reconciler re-derives the hash at directive-build time and refuses to
// ship a package whose hash drifted from the one assignPackageHashes computed
// at Compile time.
//
// # Pitfalls when modifying this package
//
// 1. Activation-time template evaluation ignores env on a cache hit.
// exprx.CompileExpr caches programs by (code, asBool) only. A "${{ $input.a }}"
// expression compiled under the full env and then re-evaluated under the
// activation env (which lacks $input) may succeed on a warm cache and silently
// produce a wrong value. checkActivationRoots in activation_params.go guards
// against this by scanning expression roots before handing to expr;
// any new activation-time evaluation must run the same root check.
//
// 2. compileTrusted does not call registerNodes, so validateNodeBody is
// absent for the trusted path. assertFanOutNodesResolved runs as a
// post-condition of projectNodeBodies to catch this: a fan-out node compiled
// with neither a body nor an expression would silently commit its handler's
// descriptor as an ordinary value. Do not add passes to Compile without
// auditing compileTrusted for the same omission.
//
// 3. Supply node names must stay out of the package hash. Building
// buildVisibleSupplies from content instead of names would move the package
// hash on every supply rotation and invalidate every cached directive in
// service/control, forcing a full re-projection per change. Only names belong
// in the package; content travels on the lease at activation time.
//
// 4. Timeout must cross the projection boundary; Retry and RunnerSelector
// must not. In ProjectGroupPackage the mini-WorkflowDef deliberately omits
// Retry and RunnerSelector from member NodeDefs (the group takes those over),
// but Timeout is carried through: group and member timeouts nest, and the
// executor takes the minimum. Dropping Timeout here silently downgrades every
// member to the global default with no diagnostic.
package graph
