// Package control is the server-side control plane: it bundles the workflow
// engine, the runner-protocol HTTP and gRPC servers, the task dispatcher,
// the lease sweeper, and the leader-gated background workers into a single
// ControlPlane value that a host program mounts and manages.
//
// # Position in the main line
//
// In the cross-process execution path this package sits between the
// engine/rstate layer (Redis authoritative state) and the remote runner:
//
//	[engine + backend/providers/distributed/rstate]
//	       │  Redis – authoritative schedule, leases, outbox
//	       ▼
//	[service/control]  ← THIS PACKAGE
//	  ControlPlane: Dispatcher → RunnerDirectory → HTTP/gRPC core
//	  leader-gated: EntryActivationReconciler, LeaseSweeper,
//	                AuditReconcileWorker (external), timeout monitors
//	       │  HTTP (primary) / gRPC (experimental)
//	       ▼
//	[service/runner]   runner process
//	       │  dispatches via service/protocol path constants
//	       ▼
//	[execution]  executes ActionHandlers, reports result back up
//
// User traffic arrives from service/apiserver, which delegates workflow
// registration, execution submission, signal/revoke/cancel, dead-letter
// replay, and supply management to this package's ControlPlane facade.
//
// # Core invariants and boundaries
//
// ControlPlane owns the full lifecycle: NewControlPlane wires all components
// without starting goroutines; Start binds the dispatcher and launches
// background loops; Shutdown cancels them and unwinds the queue binding.
//
// Leader election is a Redis SET-NX lease (RedisLeaderElector in
// backend/providers/distributed), not Raft. Backends without real election
// (local/memory) satisfy backend.LeaderElector via backend.AlwaysLeader{}
// and always report IsLeader() == true. The leader gate is per-component:
//
//   - LeaseSweeper.SweepOnce: leader only (LeaseSweeperConfig.Elector)
//   - AuditReconcileWorker.ReconcileOnce: leader only (AuditReconcileConfig.Elector)
//   - runEntryReconciler: leader only (ControlPlane.elector.IsLeader())
//   - All runner-protocol handlers (register/poll/report/heartbeat): every
//     replica — there is NO leader restriction on the data path.
//
// Responsibilities the package does NOT own:
//   - Redis key schema and Lua atomics (backend/providers/distributed/rstate)
//   - ActionHandler execution (execution package)
//   - Wire types and path constants (service/protocol)
//   - User-facing HTTP paths (service/apiserver)
//
// # Key flow: a task from queue to runner
//
//	Queue publishes task
//	      │
//	      ▼
//	Dispatcher.HandleTask
//	      │ RegisterClaim in RunnerDirectory
//	      ▼
//	Runner.Poll → Core.poll
//	      │ claim → engine.BuildTaskLease (group/subgraph variants: dispatchGroupLease/
//	      │         dispatchSubgraphLease) → RunnerDirectory.FinalizeClaim
//	      ▼
//	Runner executes, calls ReportResult → Core.report
//	      │ engine.CommitTaskResult[WithOutcome] / CommitGroupResult
//	      ▼
//	Engine flushes outbox → next tasks enqueued
//
// Background loops (all started by ControlPlane.Start):
//
//	LeaseSweeper.Run       — reclaims expired leases every ~10s (leader-gated)
//	runEntryReconciler     — assigns/fences trigger activations every ~10s (leader-gated)
//	runClaimRecovery       — recovers stale in-flight claims every ~1s (all replicas)
//	runSupplyKeyRotation   — rotates supply transport AES key (leader-gated via Redis lease)
//	runLeaderCampaign      — acquires/renews the Redis leadership lease
//
// # Pitfalls when modifying this package
//
//   - Confusing leader-only with all-replica: adding a sweep-style mutation to
//     a handler that runs on every replica causes split-brain writes. Always
//     check elector.IsLeader() before performing global mutations in a
//     background loop.
//
//   - DeadLetterManager is a list/replay API, not a background scanner.
//     Replay moves one dead-letter entry back to ready in Redis; it does not
//     scan the queue. Do not add a Run() method or a background scan loop
//     to it — that would duplicate the outbox dispatcher's existing re-delivery
//     path.
//
//   - RedisMetricsStore uses a {control} hash tag (RedisMetricsKeyPrefix) so
//     the index SET and payload keys land in the same Cluster slot. Removing
//     the hash tag silently breaks cross-replica SMEMBERS+MGET in Cluster mode.
//     The MetricsInbox has a shared 90-second retention (DefaultMetricsRetention).
//     Assertion code must match family + runner_id per row, not totals, to
//     avoid cross-test pollution when multiple runners share the same store.
//
//   - Transport failures in reconnect loops must be tested against ctx.Err(),
//     not against specific error values. An error value check will silently
//     swallow transport faults that don't match the expected string, making
//     the loop appear to exit cleanly when it is actually stuck.
//
//   - Supply TLS: both the runner-protocol client AND the supply/artifact HTTP
//     client must use the same custom transport when a private CA is configured.
//     A TLS config that only reaches the protocol client leaves supply/artifact
//     fetches on http.DefaultTransport, which fails private-CA certificate
//     validation and causes the supply gate to reject every fetch — trigger
//     activations will never start. See service/runner.installSupplyKey.
package control
