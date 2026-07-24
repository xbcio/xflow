# Dead-Letter Runbook

Durable scheduling outbox entries that exceed the delivery attempt limit
(`xflow_outbox_dead_letters_replayed_total`'s sibling gauge
`xflow_outbox_dead_letters`) are moved to per-execution dead-letter storage for
operator review. This runbook covers detection, inspection, and safe replay.

## Semantics

- **At-least-once**: replay redelivers an entry; lease/commit fencing ensures a
  duplicate delivery never double-advances the DAG. Business side effects must
  be idempotent (host idempotency key).
- **Atomicity**: replay moves an entry dead→ready in one Redis Lua transition,
  writing an immutable receipt keyed by `--request-id` so a lost response can be
  recovered by retrying with the same request-id.
- **Activation-safe**: replay rejects an entry whose node is already terminal
  (`rejected_node_terminal`) or whose activation no longer matches the node's
  current activation (`rejected_activation_mismatch`) — a stale cyclic re-entry
  cannot be resurrected to advance a node that has moved on.
- **Idempotent under request-id**: a retry with the same `--request-id` after a
  lost response returns `already_replayed` with the original `audit_id`, not an
  unprovable `not_found`. Concurrent replays of the same entry collapse to
  exactly one `replayed`; the rest return `already_replayed`.
- **Rejection**: replay is rejected (no mutation) when the execution is
  terminal (`rejected_terminal`) or expired/missing (`rejected_inactive`).
- **Attempt reset**: replay clears the delivery attempt counter so the entry is
  not immediately re-dead-lettered. The original immutable task body is
  preserved byte-for-byte across the move.
- **Privileged write**: replay is a privileged write, not a read-only
  inspection. Operator identity is injected from the authenticated principal
  (`cli:<user>` for the G0 maintenance CLI; the B3 authorizer for the G1 HTTP
  management API) — `--operator` is not accepted. `--reason` is required and
  length-bounded.

## Outcomes

| Outcome | Meaning |
|---|---|
| `replayed` | Entry moved dead→ready; will be redelivered on the next dispatcher scan. |
| `already_replayed` | A prior replay (same `--request-id` or same entry) already produced a receipt; the original `audit_id` is returned. Recover from a lost response by retrying with the same `--request-id`. |
| `not_found` | No dead entry and no prior receipt for this `--request-id`. Stable no-op. |
| `rejected_terminal` | Execution is already terminal (success/failed/canceled/timeout). |
| `rejected_inactive` | Execution status key is gone (expired/missing). |
| `rejected_node_terminal` | The entry's node is already terminal; replaying would advance a node that has moved on. |
| `rejected_activation_mismatch` | The entry's activation no longer matches the node's current activation (stale cyclic re-entry). |
| `unauthorized` | The principal lacks `deadletter.replay` scope (G1 authorizer). Never reaches Redis. |
| `invalid_request` | Missing required fields or over-length reason (manager layer). Never reaches Redis. |

## Metrics

| Metric | Type | Meaning |
|---|---|---|
| `xflow_outbox_dead_letters` | gauge | Current entries in dead-letter storage. |
| `xflow_outbox_dead_letters_total` | counter | Entries ever dead-lettered. |
| `xflow_outbox_dead_letters_replayed_total{outcome}` | counter | Replay attempts by outcome: `replayed`, `already_replayed`, `not_found`, `rejected_terminal`, `rejected_inactive`, `rejected_node_terminal`, `rejected_activation_mismatch`, `unauthorized`, `invalid_request`. |
| `xflow_outbox_pending` | gauge | Entries awaiting delivery (ready set). |

### Alerts

- **Dead-letter backlog** — `xflow_outbox_dead_letters > 0` for >5m. Entries are
  stuck; inspect and replay or purge after root-cause analysis.
- **Replay rejection rate** —
  `rate(xflow_outbox_dead_letters_replayed_total{outcome=~"rejected_terminal|rejected_inactive|rejected_node_terminal|rejected_activation_mismatch"}[5m]) > 0`.
  Operators are replaying entries for finished/expired executions or stale
  activations; likely a runaway automation or stale runbook being followed
  against the wrong execution. Investigate the operator/audit trail.
- **Unauthorized replay attempts** —
  `rate(xflow_outbox_dead_letters_replayed_total{outcome="unauthorized"}[5m]) > 0`.
  A principal without the `deadletter.replay` scope is attempting replays; check
  authz configuration and credentials.

## Inspection & replay (CLI)

The `xflow` admin CLI goes through the `DeadLetterManager` (request validation,
the metric outlet, and the audit projection), which wraps the backend
`DeadLetterStore` capability (the Redis atomic contract and the authoritative
receipt). The CLI never constructs Redis keys directly.

```bash
# List dead-lettered entries for an execution (read-only, JSON lines, paginated)
xflow dead-letter list --redis-addr $REDIS_ADDR --execution <execID> --limit 50
# A non-empty next_cursor means more pages exist:
# xflow dead-letter list ... --cursor <next_cursor>

# Replay one entry back to the ready set (privileged write).
# --request-id makes the replay recoverable: retry with the same value after a
# lost response returns already_replayed + the original audit_id.
xflow dead-letter replay \
  --redis-addr $REDIS_ADDR \
  --execution <execID> \
  --entry <entryID> \
  --reason "queue outage resolved, redeliver" \
  --request-id "$(uuidgen)"
```

Operator identity is `cli:<user>` (from the authenticated principal), not
self-reported; `--operator` is not accepted. The authoritative receipt is
written to Redis; the stdout/stderr audit line is a secondary projection only
(`event`, `outcome`, `audit_id`, `execution`, `entry`, `node`, `activation`,
`operator`, `ts`). Capture it in the deployment log pipeline.

After replay, the control plane's running `OutboxDispatcher` redelivers the
entry within its scan interval (default 1s). No restart is required.

## Operational checklist

1. Confirm the underlying delivery failure is resolved (queue/runner healthy)
   before replaying, or the entry will dead-letter again.
2. Prefer replay over re-submit; replay preserves the original immutable task
   body and activation fencing.
3. Always pass `--request-id` so a lost response is recoverable; document the
   request-id in the incident ticket.
4. For terminal/expired executions or stale activations, replay is a no-op — do
   not treat the `rejected_*` outcome as an error; it means the entry no longer
   applies. `rejected_activation_mismatch` in particular means the node has
   re-entered via a cyclic transition; the stale intent must not be replayed.
5. `unauthorized` or `invalid_request` outcomes never reach Redis — fix the
   authz scope or the request before retrying.
6. Replayed entries that dead-letter again indicate a persistent delivery
   problem, not a one-off. Escalate to queue/runner diagnostics.

## Audit & reconciliation (G1)

The Redis receipt (`xflow:t<tenant>:exec:{id}:replay:receipt:<request_id>` hash) is the
authoritative record of each replay. The SQL audit sink is a secondary
projection; a reconciliation worker compares the two and appends `reconciled`
outcomes for receipts missing a SQL projection without modifying the
authoritative Redis receipt. Stdout is a log projection only, not authoritative.
This reconciliation path is part of G1 (B3 audit); for G0 the Redis receipt +
stdout projection are the record.
