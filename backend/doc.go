// Package backend defines the assembly contract between the engine and its
// infrastructure providers. It is the Provider-interface layer in the main
// pipeline, sitting between [engine] and the concrete provider implementations:
//
//	[engine]
//	   │  depends on: StateStore, TaskQueue, HandlerRegistry
//	   │
//	[backend.Provider]  ← assembly contract (this package)
//	   │
//	   ├─ [providers/local]   in-memory, single-process
//	   └─ [providers/distributed]  Redis + SQL, multi-replica
//	           └─ [rstate]  Redis authoritative state machine
//
// Upstream: sdk/xflow (NewLocal, NewCluster) and service/control (ControlPlane)
// assemble an engine.Engine from a backend.Provider. Downstream: each provider
// implements the interfaces declared here against its own storage layer.
//
// # Core invariants
//
// Provider groups the six engine surfaces the engine and execution boundary
// need: State, Queue, Registry, WorkflowRegistry, TriggerPrimitives, and
// lifecycle binding (Bind). Providers are named by capability, not by
// deployment mode — "local" and "distributed" are topology names, not
// abstraction levels.
//
// The three optional capability interfaces extend Provider by type-assertion:
//   - Waiter (WaitDone): event-driven completion wait for sub-graph and SDK use.
//   - LeaderElector (Campaign/IsLeader/Resign/Notify): leader-only background
//     work coordination across replicas. Backends with at most one replica return
//     AlwaysLeader; its Campaign returns immediately, IsLeader is always true.
//   - TaskHandlerBinder (BindTaskHandler): control-plane path that routes tasks
//     to remote runners via an injected handler instead of running them in-process.
//     ControlPlane.Start fails closed if the backend does not implement this.
//
// StartBinder is the fail-closed production startup path used by sdk/xflow's
// NewLocal and NewCluster factories. Provider.Bind is retained as a
// compatibility adapter for legacy callers only.
//
// LeaderElector is NOT Raft. The distributed provider implements it as a Redis
// lease (TTL 15 s), implemented in backend/providers/distributed/leader.go.
// AlwaysLeader is the correct implementation for the local (in-memory) backend.
//
// WorkflowRegistry is a backend-scoped registry of compiled workflow records.
// Its AddWorkflow/GetWorkflowByKey operations carry hash-based conflict
// semantics (ErrWorkflowConflict) so concurrent registrars reconcile legacy
// hash formats without a full re-registration.
//
// TriggerPrimitives supplies the three coordination primitives triggers need:
//   - Dedup: first-writer-wins TTL key (Kafka flush dedup, periodic trigger).
//   - TryLock: exclusive TTL lock with TriggerLock release handle.
//   - State: mutable KV scope for a trigger's durable state (supply consumer
//     offsets, last-fired timestamps).
//
// # Capability diagram
//
//	backend.Provider
//	  ├─ State()              → engine.StateStore
//	  ├─ Queue()              → engine.TaskQueue
//	  ├─ Registry()           → engine.HandlerRegistry
//	  ├─ WorkflowRegistry()   → WorkflowRegistry (AddWorkflow / GetWorkflow / ...)
//	  ├─ TriggerPrimitives()  → TriggerPrimitives (Dedup / TryLock / State)
//	  └─ Bind(eng)            → stop func   (embedded local/cluster use)
//
//	Optional (type-assert on Provider):
//	  Waiter          WaitDone(ctx, id) → Result
//	  LeaderElector   Campaign / IsLeader / Resign / Notify
//	  TaskHandlerBinder  BindTaskHandler(eng, handler) → stop, error
//	  StartBinder     StartBinding(eng) → stop, error
//
// # Traps for maintainers
//
// ControlPlane.Start requires TaskHandlerBinder, not Provider.Bind. A backend
// that only implements Provider.Bind would silently run handlers in-process
// inside the server, bypassing remote dispatch. The fail-closed check is in
// service/control; do not add a fallback to Provider.Bind there.
//
// LeaderElector.IsLeader must return false when leadership state is unknown
// (e.g. lost connection to Redis), not assume leadership. A replica that
// incorrectly claims leadership while another holds the lease runs
// leader-gated reconcilers (LeaseSweeper, AuditReconcileWorker,
// EntryActivationReconciler) concurrently, producing duplicate work and
// potentially conflicting state mutations.
//
// AlwaysLeader.Notify returns a buffered channel pre-loaded with true. A bare
// for-range over it blocks forever after the first read because no further
// values are ever sent. Callers must use select with ctx.Done (as
// runLeaderCampaign in service/control does).
//
// WorkflowRegistry.AddWorkflow returns ErrWorkflowConflict when the same key
// already exists with a different definition hash. The engine's AddWorkflow
// method uses GetWorkflowByKey + UpdateDefinitionHash to reconcile legacy-format
// hashes; do not treat ErrWorkflowConflict as a hard error without checking
// whether the semantic content matches.
package backend
