# Dead-Letter Runbook

Durable scheduling outbox entries that exceed the delivery attempt limit
(`xflow_outbox_dead_letters_replayed_total`'s sibling gauge
`xflow_outbox_dead_letters`) are moved to per-execution dead-letter storage for
operator review. This runbook covers detection, inspection, and safe replay.

## Semantics

- **At-least-once**: replay redelivers an entry; lease/commit fencing ensures a
  duplicate delivery never double-advances the DAG. Business side effects must
  be idempotent (host idempotency key).
- **Atomicity**: replay moves an entry dead→ready in one Redis Lua transition.
  Concurrent replays of the same entry collapse to exactly one `replayed` and
  the rest return `not_found`.
- **Rejection**: replay is rejected (no mutation) when the execution is
  terminal (`rejected_terminal`) or expired/missing (`rejected_inactive`).
- **Attempt reset**: replay clears the delivery attempt counter so the entry is
  not immediately re-dead-lettered.

## Metrics

| Metric | Type | Meaning |
|---|---|---|
| `xflow_outbox_dead_letters` | gauge | Current entries in dead-letter storage. |
| `xflow_outbox_dead_letters_total` | counter | Entries ever dead-lettered. |
| `xflow_outbox_dead_letters_replayed_total{outcome}` | counter | Replay attempts by outcome: `replayed`, `not_found`, `rejected_terminal`, `rejected_inactive`. |
| `xflow_outbox_pending` | gauge | Entries awaiting delivery (ready set). |

### Alerts

- **Dead-letter backlog** — `xflow_outbox_dead_letters > 0` for >5m. Entries are
  stuck; inspect and replay or purge after root-cause analysis.
- **Replay rejection rate** —
  `rate(xflow_outbox_dead_letters_replayed_total{outcome=~"rejected_terminal|rejected_inactive"}[5m]) > 0`.
  Operators are replaying entries for finished/expired executions; likely a
  runaway automation or stale runbook being followed against the wrong
  execution. Investigate the operator/audit trail.

## Inspection & replay (CLI)

The `xflow` admin CLI goes through the backend `DeadLetterStore` capability and
never constructs Redis keys directly.

```bash
# List dead-lettered entries for an execution (read-only, JSON lines)
xflow dead-letter list --redis-addr $REDIS_ADDR --execution <execID> --limit 50

# Replay one entry back to the ready set (privileged write; emits an audit line)
xflow dead-letter replay \
  --redis-addr $REDIS_ADDR \
  --execution <execID> \
  --entry <entryID> \
  --reason "queue outage resolved, redeliver" \
  --operator "$(whoami)"
```

The replay command emits one JSON audit line to stdout capturing `operator`,
`reason`, `execution`, `entry`, `outcome`, and `occurred_at`. Capture it in the
deployment log pipeline.

After replay, the control plane's running `OutboxDispatcher` redelivers the
entry within its scan interval (default 1s). No restart is required.

## Operational checklist

1. Confirm the underlying delivery failure is resolved (queue/runner healthy)
   before replaying, or the entry will dead-letter again.
2. Prefer replay over re-submit; replay preserves the original immutable task
   body and activation fencing.
3. For terminal/expired executions, replay is a no-op — do not treat the
  `rejected_*` outcome as an error; it means the entry no longer applies.
4. Replayed entries that dead-letter again indicate a persistent delivery
  problem, not a one-off. Escalate to queue/runner diagnostics.
