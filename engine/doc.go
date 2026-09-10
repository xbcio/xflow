// Package engine is the pure-algorithm workflow execution core. It sits at
// the centre of the main execution pipeline:
//
//	[engine/graph]  compiles WorkflowDef → immutable *Graph IR
//	      │
//	      ▼
//	[engine]        scheduling algorithm — in-degree decrement, lease issuance,
//	      │         result verdict, OnError routing, durable outbox
//	      │         depends only on StateStore + TaskQueue (interfaces)
//	      ├──────────────────────────────┐
//	      ▼                              ▼
//	[backend]                      [execution]
//	Provider assembly                Task boundary: Dispatcher → Executor → commit
//
// The engine has zero IO dependencies of its own. Every persistence and
// queuing operation is injected via StateStore and TaskQueue. Redis, Asynq,
// MySQL, and HTTP are invisible from here; they live in the providers and
// rstate packages below.
//
// # Position in the main execution line
//
// After engine/graph produces a *Graph, callers drive the engine through
// three entry points:
//
//   - Submit / Invoke — start a new execution, persist the snapshot, and
//     enqueue the initial tasks through the durable outbox.
//   - BuildTaskLease / RecoverTaskLease — issue a lease token to a runner.
//   - CommitTaskResult — validate the lease token, persist the result, and
//     advance the scheduling topology.
//
// Downstream packages consume the engine: execution.Runner calls
// CommitTaskResult; service/control houses the OutboxDispatcher that calls
// FlushOutbox for crash recovery; backend packages supply the concrete
// StateStore and TaskQueue implementations.
//
// # Core invariants and boundaries
//
// Dependency rule: engine must not import redis, asynq, sql, database/sql,
// or any network transport. Permitted imports are types, namespace,
// engine/graph, and observability/tracing (narrow facade for span creation).
//
// Time: there is no Clock interface. Callers that need the current time pass
// now time.Time explicitly (e.g. SuspendOutboxEntries, claimOutbox).
//
// Optional capabilities are discovered via interface assertions at runtime,
// not at construction:
//
//   - AtomicStateStore — required for durable outbox and atomic node commits;
//     its absence degrades to a legacy create-then-enqueue path.
//   - OutboxLeaser — exclusive delivery lease on outbox entries; absent
//     stores fall back to ListOutbox with at-least-once duplicates.
//   - DurableLeaseExpander / DurableLeaseSuspender — fan-out and suspend
//     transitions.
//   - types.SuspendingHandler — handler capability asserted in execution,
//     not here; the engine only observes the TaskResult.Suspend field.
//
// Hooks (WithHooks / Hooks interface) are a pure observer: they fire after a
// scheduling decision is already committed and cannot change control flow.
// Panics inside hooks are recovered by safeHook with a 5-second timeout.
//
// # Key flow: result verdict and in-degree advance
//
//	CommitTaskResult
//	      │
//	      ├─ subgraph payload? ──► CommitSubgraphResult (fan-out barrier)
//	      │
//	      ├─ cyclic graph? ──────► commitLegacyTaskResult
//	      │                                  │
//	      └─ acyclic graph ───────► commitAcyclicTaskResult
//	                                         │
//	Both entry points are thin: each builds a taskResultCommitStrategy naming the
//	three steps where its path differs, then delegates to the one shared verdict
//	sequence.
//	                                         │
//	      commitTaskResultWithStrategy
//	            ├─ error / exhausted error-port ─► strategy.commitError
//	            │        └─ commitNodeErrorOutcome: retry → ApplyOnError
//	            │           (errorpolicy.go; four strategies: stop /
//	            │           error_output / main_output / continue) → classify
//	            │           → terminal committer
//	            ├─ node has a projected body ────► strategy.commitExpand
//	            │        ├─ legacy:  ClaimTaskLease → expandLoopSplit
//	            │        └─ acyclic: refuseAcyclicExpansion (backstop, not a
//	            │                    second decision about what expands)
//	            └─ otherwise ───────────────────► strategy.commitNode (success)
//	                                         │
//	      terminal committers
//	            ├─ cyclic:  planCyclicDownstream (reads graph, no IO)
//	            │           → CommitLeasedNode (fenced, +CyclicOutbox)
//	            └─ acyclic: AtomicStateStore.CommitNode (fenced)
//	                          ┌─ LeaseToken check (stale → reject)
//	                          ├─ persist terminal node status + output
//	                          ├─ decrement downstream in-degree counter
//	                          └─ append OutboxEntry for each ready unit
//	                                         │
//	                                    FlushOutbox
//	                                      ┌─ LeaseOutbox → lease each entry
//	                                      ├─ outboxLeaseKeeper (renews OutboxDeliveryLeaseTTL/3)
//	                                      ├─ Enqueue / EnqueueDelayed
//	                                      └─ AckOutbox
//
// # Durable outbox and at-least-once delivery
//
// Root tasks are written into the outbox atomically with the execution
// snapshot (CreateExecutionWithOutbox). Every subsequent scheduling intent —
// retry, advance, cyclic downstream — is written atomically with the terminal
// node transition. This closes the window where a crash between a state write
// and a queue.Enqueue permanently loses a task.
//
// Delivery is at-least-once. LeaseToken fencing in CommitNode and
// CommitLeasedNode makes duplicate delivery idempotent: a stale runner that
// arrives after a timeout reclaim receives ErrInvalidLeaseToken and its commit
// is discarded.
//
// Dead-letter threshold: an outbox entry that fails delivery more than
// DefaultOutboxMaxDeliveryAttempts (10) times is moved to the dead-letter set.
// The OutboxDispatcher (NewOutboxDispatcher) drains pending outboxes on a
// background ticker; its interval is independent of DefaultLeaseTTL.
//
// # Pitfalls when modifying this package
//
// 1. Error port erases error classification. outputPortRetryError
// (commit.go) rebuilds the error as errors.New, stripping the unwrap chain.
// A handler that returned types.NewPermanentError so the retry path would
// decline it now looks transient to the retry logic. Any new retry-or-fail
// decision on the error-port path must derive permanence from
// output.Data["error"] before the errors.New reconstruction discards it.
//
// 2. CommitNodeRequest.AllowCycles must be stated explicitly. The field
// defaults to false (acyclic), and the two completion protocols share no
// state: CyclicOutbox entries are silently dropped on an acyclic commit,
// and an unseeded remaining-unit counter is decremented on a cyclic commit.
// CommitNodeRequest.Validate cross-checks these payload fields and will
// return an error for a mismatched request.
//
// 3. NodeDef field projection must be whole-struct, not field-by-field.
// Anywhere that copies a NodeDef into another shape (e.g. projected package
// mini-WorkflowDef in engine/graph's ProjectGroupPackage) must transfer the
// struct in one step. A field-by-field copy silently drops any field added
// later; the engine's inner execution then sees a zero value with no error
// or diagnostic. The Timeout field was dropped this way before the whole
// struct assignment was introduced.
//
// 4. Cache hits under (cached) are not verification. Tests that rely on
// the in-process graph cache in e.graphs (loadActiveGraph) may see a
// non-terminal execution status from an earlier run. A test that must
// observe the engine as inactive should call EvictExecution or use a fresh
// Engine instance.
package engine
