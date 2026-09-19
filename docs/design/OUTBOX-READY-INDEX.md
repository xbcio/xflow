# Outbox Ready Index — Discovery That Tracks the Backlog, Not the Keyspace

> Status: **implemented, unverified against a real Redis Cluster**.

The authoritative contract lives in the doc comment at the head of
`backend/providers/distributed/internal/rstate/state_outbox_index.go`. This
document records the decision, the analysis behind it, and what is and is not
verified.

## Problem

`OutboxDispatcher` discovered work through `state.ListOutboxExecutions`, which
walked the keyspace with `SCAN` and filtered for `...:outbox:ready`. `SCAN`'s
`COUNT` counts keys **examined**, not keys matched, so at a keyspace of N keys
one page reached roughly `page/N` of the ready backlog and a cursor needed
`N/page` drains to come around once.

A host deployed on a shared Redis Cluster reported:

| Measure | Value |
| --- | --- |
| Keyspace | 13k–67k keys |
| Ready backlog | ~850 executions |
| Executions created | 364/min |
| Executions dispatched | 68/min |

Delivery itself was healthy; the backlog grew anyway. Discovery throughput
tracked the keyspace, not the backlog. Raising the discovery page
(`engine.DefaultOutboxDiscoveryPage`) only moves the constant — the cost stays
`O(keyspace)` and no fixed page can track an unbounded keyspace.

## Decision: the index is an accelerator, not the source of truth

A namespace-global ready index **cannot** be written in the same atomic
transition as the outbox entry it indexes. Every outbox mutation is a Lua script
over keys sharing the execution's `{id}` hash tag (`keys.go` documents that the
namespace prefix is deliberately brace-less so the first `{` still opens on the
execution ID), and Redis Cluster refuses a script that touches a key in another
slot. A namespace-global `ZSET` is in another slot, so the write would fail with
`Lua script attempted to access a non local key in a cluster node`.

Rather than change the key layout to make the index atomic, the index is
reframed as a **best-effort, eventually-consistent accelerator layered on top of
the state machine**. The outbox body and the per-execution ready `ZSET` remain
the single source of truth, written atomically exactly as before. Everything
else follows from that one reframing:

- A missed registration is not data loss — the sweep still finds it.
- A stale entry is not a false execution — draining it costs one no-op claim
  that repairs it.
- An absent, empty, disabled or failing index changes nothing — discovery falls
  back to sweeping, which is what it did before.

`namespace_registry.go` (`xflow:namespaces`) is the precedent for a best-effort
namespace-global registry in this store. It is safe because it is append-only;
this index removes entries, which is where the prune protocol below goes.

## Layout

```
xflow:ns:<namespace>:outbox:ready-index    ZSET
    member = execution ID
    score  = LOWER BOUND on the instant that execution's next entry is deliverable
```

The score is a lower bound, not the exact minimum, and that is what keeps
registration cheap: appending an entry registers at `now` with a single `ZADD`
and no read of the ready set. A drain that then finds nothing due re-arms the
member to the exact minimum, or removes it. Because the score only ever
understates readiness, an imprecise score can surface work **early**, never
late — it costs one no-op drain instead of hiding work behind the read cutoff.

One member per execution, so the index is bounded by executions with work, not
by entries. The key is deliberately not execution-scoped and does not match
`execScanPattern(t, "outbox:ready")`, so the sweep and `OutboxMetrics` cannot
mistake it for an execution's ready set. It carries no hash tag, which is fine
because it is only ever accessed with ordinary commands (`ZADD`, `ZREM`,
`ZRANGEBYSCORE`), never inside a multi-key script.

### The two transitions

| Transition | Call | Cost |
| --- | --- | --- |
| An atomic transition that **appends** an outbox entry | `markOutboxReadyIndex` | one `ZADD`, no read |
| A lease claim that **finds nothing** | `refreshOutboxReadyIndex` | one read, then `ZADD`/`ZREM` |

The second is the one moment an execution's readiness is fully determined after
a drain, because `flushOutbox` loops until a claim returns zero entries: every
ack, release and dead-letter has happened, and any advance or skip intent the
flush appended is already in the ready set.

Producer hooks: execution create, admission by entry seed, node/subgraph commit,
node advance, retry reset, lease revocation, group commit, group-lease
revocation, task expansion, task suspend, signal delivery, dead-letter replay,
create rollback, cancellation cleanup.

## (a) No entry can be lost

**Missed registration.** Registration is a second, non-atomic step, so it can be
skipped (process death between the transition and the `ZADD`, or a Redis error).
The keyspace sweep bounds it:

```
staleness bound = outboxIndexSweepEveryCalls drains + one cursor round
```

On the reported deployment (~0.9s drains, 13k keys, page 2048 ⇒ a round is ~7
drains) that is ~17s. It is a bound on **delay**, never on delivery. The bound
only applies while the sweep is throttled at all, and the sweep is throttled
only once the index has proven it carries work.

**The prune races a re-registration.** A prune can interleave with a producer
that just appended and registered:

```
prune reads ready -> empty        (t1)
producer appends entry E          (t2)
producer registers: ZADD index    (t3)
prune removes:      ZREM index    (t4)   <- the fresh registration is gone
```

This is closed rather than merely bounded, by making every prune read the ready
set **again** after its own `ZREM` and re-register if it is no longer empty:

```
prune removes:      ZREM index    (t4)
prune verifies: read ready -> E   (t5)   <- arrives after t4, sees t2
prune repairs:      ZADD index    (t6)
```

`t5` is ordered after `t4` on the same connection, so it observes every append
that preceded `t4`, including `t2`. Remaining interleavings are harmless: a
producer that appends after `t5` also registers after `t5`, so its `ZADD`
outlives the `ZREM`. The argument is inductive over concurrent pruners as well,
because each pruner verifies after its own `ZREM` and each producer registers
after its own append. A `ZADD` racing a `ZADD` is benign: the worst case is a
stale-early score, which costs one no-op drain.

**Without the verify step** the outcome would be a lost registration recovered
only by the sweep — extra staleness, not lost work. The verify step is what
turns that into a closed race. It is paid only on the prune branch, which keeps
the common path (registration) at one round trip and the drain path at two.

## (b) No entry can be duplicated

The index is a discovery hint, never a delivery permission. Delivery is still
gated by the per-entry lease (`LeaseOutbox`, `engine.OutboxDeliveryLeaseTTL`),
unchanged. Finding an execution twice — from the index and from the sweep, or
from two drains — can at most cause a second flush that finds every entry
already leased. `TestOutboxRediscoveryCannotRedeemALeasedEntry` pins that for
the sweep; the index adds no new way to bypass it.

## (c) Cluster correctness

No key layout changes. Every atomic transition still touches only keys under the
execution's `{id}` hash tag, and the index is written by ordinary single-key
commands issued from Go, outside any script. No new bare `SCAN` is introduced —
`TestProductionScansUseClusterAwareHelpers` pins the audited call sites. The
index key does not match the sweep or metrics scan patterns.

## (d) Operation without the index

Three independent ways, all ending at the sweep:

1. `ConfigureOutboxReadyIndex(false)` / `distributed.WithOutboxReadyIndex(false)`
   disables reading and writing it.
2. A backend with no index at all (the local provider's `memoryState`) is
   unaffected — the engine only ever calls `ListOutboxExecutions`.
3. A Redis error on the index read abandons the index for that call and sweeps
   instead.

The sweep is throttled **only** once the index has proven it carries work in
this process (`outboxIndexProven`), which is set by a discovery read that
returned at least one execution — the only direct evidence the loop the
dispatcher depends on is closed. A `ZADD` succeeding proves only that `ZADD`
works. Until then — a cold store, an index that is never written, an index whose
reads fail — every discovery call sweeps, which is exactly the pre-index
behaviour.

**Residual.** An index that carried work and then stopped registering keeps the
sweep at one call in ten. That is the state `xflow_outbox_ready` rising against
a flat `xflow_outbox_drain_discovered` reports, and
`WithOutboxReadyIndex(false)` is the operator's way back.

## Measured effect

Fixture: 24 ready executions, 4000 unrelated keys, page 512 (4146-key keyspace).

| Discovery path | Calls to cover the backlog | Keyspace scans |
| --- | --- | --- |
| Readiness index | 1 | 0 |
| Keyspace sweep (index disabled) | 9 | 9 |

Steady state, one ready execution, over 10 discovery calls: 10 sweeps with the
index disabled, 1 with it proven.

On the reported deployment the arithmetic is: a page-2048 sweep reaches about
`2048/13000 ≈ 16%` of the ready backlog per drain, while an index read reaches
all of it (its cost is proportional to the due backlog). With the sweep
amortized to one drain in ten, effective discovery rises by roughly 6×, and
`xflow_outbox_drain_discovered` should rise with it. That is an argument from the
measurement above plus the two constants, not a measurement on the host.

## Verification status

**Verified by construction:** cluster safety (single-key commands only, no
layout change, no new bare `SCAN`), the no-duplication property (the lease is
untouched), and the fallback paths.

**Verified by test:** registration on an append; removal once drained;
recovery of a missed registration by the sweep, with the bound asserted; a stale
entry costing exactly one no-op claim and repairing itself; the prune race
(closed by fault injection, and again under real concurrency); agreement between
the index and the ready sets after concurrent traffic; the index disabled; the
throttle applying only to a proven index; and the cost claim above.

**Not verified:** behaviour against a real Redis Cluster. `miniredis` is
single-slot and single-node, so it cannot exercise `CROSSSLOT`, slot migration,
or a multi-master `SCAN`. The design is what keeps that from being a correctness
gap — the index is written with plain commands, so cluster rules cannot make it
fail in a way the fallback does not already cover — but that is an argument, not
a measurement.

## Deliberately not done

- No new engine interface and no capability-gated `OutboxDiscoverer`. The
  accelerator lives entirely inside the state store, behind
  `ListOutboxExecutions`; an optional engine-side interface would trade a
  smaller change for a wider contract.
- No pure-index mode. The sweep stays mandatory as the staleness bound.
- No change to `DefaultOutboxDiscoveryPage` (still 2048) and no removal of the
  scan fallback.
- No metrics changes. `xflow_outbox_ready` against
  `xflow_outbox_drain_discovered` is the existing signal that the index has
  stopped carrying work.
- No in-index expiry. A member whose execution's ready set expired with the
  execution's own TTL is still returned by one discovery read, and the drain
  that follows removes it — the path cleans up after itself without a reaper.

## Files

| File | Role |
| --- | --- |
| `rstate/state_outbox_index.go` | Index contract, design note, layout, all index I/O |
| `rstate/state_outbox.go` | `ListOutboxExecutions` reads the index, sweeps as fallback |
| `rstate/state.go`, `rstate/exports.go` | `outboxIndexOn`/`outboxIndexProven`, `ConfigureOutboxReadyIndex` |
| `rstate/state_commit.go`, `group_state.go`, `expansion_state.go`, `suspend_outbox.go`, `state_suspend.go`, `state_deadletter.go`, `state_execution.go`, `entry_admission.go` | Producer hooks |
| `distributed/backend.go` | `WithOutboxReadyIndex` |
