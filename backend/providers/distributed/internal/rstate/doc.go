// Package rstate is the Redis-backed authoritative state machine for the
// distributed provider. It is the deepest layer in the execution pipeline —
// the point where every scheduling decision is linearized into durable,
// crash-safe Redis state before any SQL audit trail is written.
//
// In the main pipeline it sits below backend/providers/distributed:
//
//	[engine] → [execution.Dispatcher]
//	                    │
//	             [providers/distributed]
//	                    │
//	               [rstate.Store]  ← this package
//	                    │
//	              Redis (system of record)
//	                    │
//	              SQL/sqlstore (audit projection, best-effort)
//
// Upstream: providers/distributed constructs a Store (rstate.New) and exposes
// it as engine.StateStore / engine.AtomicStateStore. Downstream: Store issues
// Redis commands and Lua scripts; on success it projects to sqlstore for the
// audit trail. Redis failure is fatal; sqlstore failure is surfaced via
// AuditObserver and counters but does not affect scheduling correctness.
//
// # Redis as system of record
//
// Redis is the sole scheduling truth. A restart recovers entirely from Redis;
// a missing SQL row is a reporting gap, not a reason to roll back. The converse
// does NOT hold: a SQL row without a Redis key means the execution's data has
// expired (DefaultExecTTL = 24 h) and the record is read-only history.
//
// SQL audit writes are best-effort. Every dual-write site calls s.auditWrite,
// which routes errors to AuditObserver.OnAuditFailed and never blocks the
// Redis path. Do not add SQL-read fallbacks for live scheduling decisions.
//
// # Key schema
//
// Every execution-scoped key follows the pattern:
//
//	xflow:ns:<namespace>:exec:{<id>}:<suffix>
//
// The namespace prefix is intentionally brace-less. The first '{' opens the
// execution ID, so the Redis Cluster hash tag is {<id>}. All keys for one
// execution co-locate on a single slot (Lua CROSSSLOT never triggers), while
// different executions spread across slots. The namespace prefix is a pure
// isolation boundary; it does NOT become the hash tag.
//
// Execution-level keys (suffix → role):
//
//	status        current execution status (running/success/failed/canceled/timeout)
//	graph         serialized graph.Graph (cached in-process after first load)
//	remaining_nodes  decrement counter; 0 triggers terminal transition
//	failed_nodes  increment on node failure; >0 → execution failed
//	outbox:ready  ZSET scored by available_at_ms; delivery window
//	outbox:body   HSET entryID→JSON task body
//	outbox:attempts  delivery failure counter; threshold → dead-letter
//	outbox:dead   ZSET dead-lettered entry IDs
//	leases        ZSET node/group-name → lease expiry ms (discovery index)
//	transient     per-execution transient marker (HSET with ttl_ms fields)
//
// Node-level keys (suffix → role):
//
//	node:<name>:status  pending/running/committing/waiting/success/failed/...
//	node:<name>:meta    lease fields (lease_id, lease_token, attempt, activation_id, ...)
//	output:<name>       node output JSON
//
// # Lua atomic transitions
//
// All state mutations use Lua scripts to achieve atomicity without
// multi-key transactions. The four central scripts:
//
//   - commitNodeLua (atomic_state.go): the durable linearization point for
//     acyclic node results. In one Redis command it: (1) validates the lease
//     identity (lease_id, lease_token, attempt, activation_id), (2) writes the
//     terminal node status, (3) stores the output, (4) removes the lease expiry
//     ZSET entry, (5) decrements remaining_nodes and, if zero, sets the
//     execution terminal status. Duplicate terminal → CommitOutcomeDuplicateTerminal
//     (stable, no counter change). Stale token → CommitOutcomeStaleToken (silent no-op).
//
//   - advanceNodeLua (atomic_state.go): converts in-degree arrivals from a
//     completed source into execute-or-skip outbox intents for downstream nodes.
//     The advance marker (advanceMarkerKey, SET NX) makes repeated delivery
//     idempotent: a second delivery of the same outbox entry is a no-op.
//
//   - seedExecutionFromEntryLua (entry_admission.go): first-writer-wins admission
//     of an entry-unit result. Creates the execution keys and directly terminates
//     the execution if it is a single-node workflow, all in one script.
//
//   - replayDeadLetterLua (atomic_state.go): activation-safe dead→ready move with
//     an immutable receipt keyed by RequestID so a lost response is recovered by
//     retrying the same RequestID. Intent-branched node guards distinguish the
//     "safe to replay" precondition per intent source (root/retry/requeue/resume/
//     advance/execute/skip). Fail-closed: any missing guard state → outcome 7
//     (rejected_metadata_missing) without moving the entry.
//
// # Durable outbox (at-least-once delivery)
//
// Tasks are delivered via a ZSET-scored visibility window, not removed on claim.
// AckOutbox is the only removal path (it atomically ZREMs ready and HDELs body).
// A delivery failure increments outbox:attempts; at engine.DefaultOutboxMaxDeliveryAttempts
// (10) the entry moves to dead-letter storage. ReplayDeadLetter restores it.
//
// The lease expiry ZSET (outbox:leases) is a discovery index for the sweeper,
// not an independent truth source. RepairLeaseIndex (lease_repair.go) runs
// periodically to reconcile it against authoritative node state via
// reconcileLeaseIndexLua.
//
// # Projection error text
//
// This section used to claim that the error text written to the execution-level
// error key (execKey "error") "carries only the engine-generated reason string,
// never a node's output". Read that as a description of today's built-in nodes,
// not as an invariant: nothing in this package, or anywhere below it, enforces
// it.
//
// The text originates at engine/errorpolicy.go, which does errMsg =
// sysErr.Error() on whatever error the node returned, verbatim. A node that
// formats a response body, a request header or an upstream node's output into
// its error — fmt.Errorf("POST %s: %s", url, body) is the obvious shape — puts
// that text in execKey "error", in the ExecutionEvent published to subscribers,
// and, for a durable execution, in xflow_executions.error. Node output routinely
// contains HTTP response bodies with bearer tokens, so the difference between
// "reason string" and "payload" is a convention the node author has to keep,
// with no compile-time or runtime check behind it. The types.Error split is the
// convention: a body belongs in Details, which is not what Error() renders.
//
// What IS enforced is narrower and worth stating separately, because it is the
// part the credential-disclosure constraint actually rests on: for a transient
// execution the error text does not reach SQL at all. Both routes to
// db.UpdateExecutionStatus — UpdateExecutionStatus itself and
// projectExecutionStatus — are gated on !isTransient, and isTransient fails
// closed (a Redis error resolves to transient). Those are two independent
// guards, not one: dropping either leaves the other's test green, so both are
// pinned separately (TestPerWorkflowTransient_SkipsExecutionStatusProjection and
// TestPerWorkflowTransient_SkipsGroupCommitStatusProjection). The guard is
// maintained by repetition across four call sites and one of those sites has
// already been found missing it once.
//
// # Traps for maintainers
//
// group member outputs are not projected by UpsertNode. CommitGroup does not
// call UpsertNode for group members. "Scanning xflow_nodes to prove data never
// landed in SQL" is therefore always true for group members — the correct
// assertion is to check whether the Redis key's TTL is transient-short or
// durable-long.
//
// The lease expiry ZSET is a discovery index, not truth. An entry in
// outbox:leases may be stale after a lease revoke or terminal commit that
// raced with the ZADD. RepairLeaseIndex exists for exactly this reason; treat
// ZSET presence as "might be expired" not "is expired".
//
// Sentinel error overloading hides corruption. The compatibility guard for
// legacy-format hashes must run before any parse attempt. Any error returned
// after parsing is a real error, not a format mismatch; collapsing them into
// the same sentinel would mask data corruption (see the haiku in
// sentinel-error-overload-hides-corruption.md).
//
// Write-path namespace normalisation, read-path gap. A write that normalises
// an empty namespace to "default" produces keys under "default"; a read that
// passes "" as the namespace produces keys under "" and will never find them.
// Supply namespace handling (UpdateSupply) had this exact bug. Always normalise
// at the same layer in both directions.
//
// Test databases from AutoMigrate have all-nullable columns. NOT NULL
// constraints that exist in production MySQL are absent in test schemas
// generated by GORM AutoMigrate. Tests that rely on constraint violations to
// detect missing values pass unconditionally in the test environment. Assert
// the actual stored value, not the absence of a constraint error.
package rstate
