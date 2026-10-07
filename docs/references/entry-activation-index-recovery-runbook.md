# Entry Activation Index Recovery Runbook

This runbook covers **restoring the entry-activation key index when it no longer
matches the records it is supposed to expose**, using the one recovery lever the
design provides: deleting the namespace's ready gate, after which the next List
call serves from the pre-index scan path and a background rebuild re-derives
every index set from the records themselves.

The index is the read path behind `engine.EntryActivationStore.List` for the
Redis-backed store. It shipped with the v0.0.38 line
(`9a2734c perf(rstate): serve entry activation lists from a maintained key
index`) to replace the pre-index full-keyspace `SCAN` sweep that ran per List
call on the reconciler, heartbeat, and projection paths
(`backend/providers/distributed/internal/rstate/entry_activation_index.go:3`).

> **The lever has been exercised only in the unit regression suite**
> (`TestEntryActivationIndexManualRecoveryLeversRebuildsLostState`,
> `backend/providers/distributed/internal/rstate/entry_activation_index_regression_test.go:389`),
> against miniredis. It has never been rehearsed against a live deployment. The
> procedure below is derived from source; §4's sweep-cost estimate is the one
> number to establish for your deployment before you need this runbook under
> pressure.

## 1. What the index is, and the two ways it can break

All keys for one namespace share the prefix `xflow:ns:<esc>:`, where `<esc>` is
`url.PathEscape(ns)` (`entry_activation.go:104`). Namespaces that contain no
characters that need escaping (no `/`, spaces, `%`, `{}`, …) appear verbatim;
`a/b` becomes `a%2Fb`. The frozen legacy record layout uses the *raw* namespace
and has no writers left.

| Key | Role | Maintained by |
|---|---|---|
| `…:entryactidx:ready` | Ready gate. Value = the last rebuild's lock token; **no TTL**. Set only by a rebuild that completed a full scan. | Rebuild only — and it is the recovery lever (§3). |
| `…:entryactidx:rebuild` | Cross-process rebuild lock, TTL 30 min, released by token-checked Lua. | Rebuild attempts. |
| `…:entryactidx:wfs` | Enumeration set: every workflow digest tag (`wf-<64 hex>`) that has a per-workflow index. Carries no hash tag. | Record writes — fail-closed `SADD` (`entry_activation.go:560`); also the rebuild. |
| `…:entryactidx:{wf-<64 hex>}` | Per-workflow index: the full key names of that workflow's modern records. Hash tag = the workflow tag (same slot as the records). | The record-write Lua (same slot, one call); also the rebuild. |
| `…:entryactidx:legacy` | Frozen legacy-layout record key names. | Rebuild only. |
| `…:entryact:{wf-<64 hex>}:act:<64 hex>:replica:<n>` | Modern activation records. | Write path (Upsert / transitions). |
| `…:entryactrev:{wf-<64 hex>}` | Workflow revision watermark. | Write path. **Never touched by the index or its rebuild.** |

The read path (`List`, `entry_activation.go:636`) consults `:ready` once: set →
indexed read (`wfs` → per-workflow sets → pipelined `HGETALL`); unset → full
scan and ask for a rebuild. A probe *error* falls back to the scan path **without
requesting a rebuild** (deliberate: a Redis erroring on `EXISTS` should not be
asked to sweep for it too).

Two failure modes exist:

- **F1 — the index no longer names a live record.** Either (a) the workflow's tag
  is missing from `wfs`, or (b) a record key is missing from its per-workflow
  index. List then silently omits those records. Case (a) is the dangerous one:
  it is the single index link with no automatic repair once `:ready` is set —
  every other link is re-derived from the records' own writes, but a missing tag
  makes the whole workflow invisible, with no reconciliation and no unassignment,
  until the ready gate is reset (`entry_activation.go:123`). Nothing in the
  process that lost the tag can observe the loss.
- **F2 — the rebuild cannot run.** `:ready` stays unset, List keeps serving the
  scan path — correct results, but every call is a full sweep. Rebuild attempts
  fail and are logged when a logger is installed (the distributed backend
  installs one at construction, `backend/providers/distributed/backend.go:350`).

## 2. Symptoms and confirmation

F1 presents as a workflow whose activation records demonstrably exist in Redis
while the control plane behaves as if it had none: assignments are neither
created nor removed for that workflow, and a `List` from a debugger or a
one-off tool omits records a bounded `SCAN` finds. Confirm with keys, once you
have any record key of the workflow (the tag is embedded in it):

```bash
# 1. Find a modern record (bounded pattern; glob metachars in the namespace
#    must be escaped as they appear in the key, i.e. use <esc>):
redis-cli --scan --pattern 'xflow:ns:<esc>:entryact:{wf-*' --count 1000 | head
# → xflow:ns:<esc>:entryact:{wf-<sha>}:act:<identity>:replica:<n>

# 2. The tag between the braces must be a member of the enumeration set:
redis-cli SISMEMBER 'xflow:ns:<esc>:entryactidx:wfs' 'wf-<sha>'          # expect 1; 0 ⇒ F1a
# 3. The record key must be a member of its per-workflow index:
redis-cli SISMEMBER 'xflow:ns:<esc>:entryactidx:{wf-<sha>}' '<record key>'  # expect 1; 0 ⇒ F1b
```

F2 presents as the Warn line `entry activation index rebuild failed; List keeps
serving the scan path` recurring, and/or List latency stuck in the
minutes-per-call regime with `:ready` absent. Check `:ready` first — absent is
also the normal state before a namespace's first rebuild, so pair it with the
log line before drawing a conclusion.

Authenticate the client per your deployment's credential policy (e.g. a
`REDISCLI_AUTH`-style environment variable); never put credentials on an
argv-visible command line.

## 3. The recovery lever

```bash
redis-cli DEL 'xflow:ns:<esc>:entryactidx:ready'
```

That is the whole procedure. The key is safe to delete at any time: it carries
no data (its value is just the last rebuild's lock token) and no TTL, and
losing it moves List to the scan path — the pre-index behaviour — which is
correct, just slower.

What happens next, in any process that serves a List for that namespace:

1. That call re-probes `:ready`, finds it unset, serves from the scan path, and
   asks for a rebuild.
2. The rebuild is single-flight and debounced per process (at most one attempt
   per 30 s, `entry_activation_index.go:50`) and serialized across processes by
   the `:rebuild` lock (TTL 30 min, `:54`); each attempt is bounded by a 25-min
   budget (`:58`).
3. The rebuild re-derives all three sets and re-sets `:ready`
   (`entry_activation_index.go:138`).

Cost while `:ready` is unset: **every List call is a full sweep of both key
layouts**, and the rebuild itself is one more sweep. A field observation on a
shared Redis with ~120k keys and ~270 ms round trip measured ~6 minutes per
sweep. Delete the ready key in a quiet window; List latency for that namespace
jumps until the rebuild lands. If no slow window is available, this lever is
still the only recovery path — but treat it as a maintenance action.

## 4. What the rebuild does — and does not do

One attempt (`rebuildEntryActivationIndex`, `entry_activation_index.go:138`):

1. Takes the `:rebuild` lock (`SETNX`, token = timestamp). **Losing the lock is
   not an error**: the attempt returns silently (`:145`).
2. Re-checks `:ready`; if a concurrent rebuild already finished, releases the
   lock and returns (`:149`).
3. Scans both layouts — legacy raw-namespace pattern and modern escaped-namespace
   pattern — deduplicating the keys (`scanEntryActivationKeys`, `:184`).
4. Classifies modern keys by key-name shape alone and confirms legacy candidates
   against their stored `namespace` field (the glob over-match guard,
   `:238`).
5. Pipeline-writes: tags into `wfs`, member key names into each per-workflow
   index, legacy keys into `:legacy`. Each index's expiry is derived from its
   members' remaining PTTLs — longest member, permanent if any member is, store
   TTL as the floor (`:283`, `:327`).
6. Sets `:ready` = the attempt's token, then releases the lock
   (`:165`-`:168`).

The rebuild is **additive only**:

- It never deletes index members, record keys, or watermarks. Stale members
  (records that have expired) are harmless: the read path reads members via
  `HGETALL`, and an empty record contributes nothing.
- A record written *during* the sweep is covered by its own write path, which
  maintains its tag and index member in the same slot as the record; a sweep
  that misses it changes nothing.
- Repeating it, or (after deleting the lock) running two concurrently, is
  wasteful but cannot corrupt state.

A **failed** attempt deliberately leaves its lock to expire: the lock doubles as
retry backoff against a persistent fault, `:ready` stays unset, and List keeps
serving the scan path (`entry_activation_index.go:23`).

## 5. Verifying recovery

1. `redis-cli GET 'xflow:ns:<esc>:entryactidx:ready'` returns a value (the new
   token). Until it does, the rebuild has not completed.
2. The keys from §2 now answer `SISMEMBER … → 1` for the tag and the record.
3. Application level: List returns the previously missing records again, and the
   control plane resumes assigning / unassigning for that workflow.
4. TTL invariant: for every member of a per-workflow index, the index's `PTTL`
   must be ≥ the member's (`-1` on the index covers everything; a permanent
   member requires a permanent index). This mirrors
   `assertEntryActivationIndexTTLCoversMembers` in the regression suite.

## 6. Triage: rebuild attempts keep failing

Read the `err` field of `entry activation index rebuild failed; List keeps
serving the scan path` — it names the failing phase: `scan entry activations:`
(SCAN), `write entry activation index:` (the set pipeline), or `mark entry
activation index ready` (the final `SET`).

- **`:rebuild` lock present**: an attempt is in flight, or one failed and its
  lock has not expired (≤30 min). Wait out the TTL; the lock's expiry *is* the
  cross-process retry pacing.
- **A different Warn**: `entry activation index readiness probe failed; falling
  back to the scan path` — this path never requests a rebuild by design; it
  clears when the `EXISTS` probe succeeds again. If it persists, fix Redis
  connectivity; the index state itself is untouched.
- **`context deadline exceeded` from the rebuild**: the 25-min attempt budget
  overran — the sweep did not finish. Check keyspace size and round-trip
  latency; the next attempt (after the lock expires) retries the same sweep.
- Do **not** delete record keys or watermarks to "help". They are the rebuild's
  only source of truth. Deleting the `:rebuild` lock is at worst wasteful
  (concurrent additive rebuilds); prefer waiting out the TTL.

## 7. Alarms — none implemented today

- **A. Log-based (no code).** Alert on the rate of the two Warn messages above.
  Fully covers F2. Does **not** cover F1: a lost tag produces no log line
  anywhere, by construction.
- **B. Consistency check (needs a job or code).** Periodically (off the hot
  path; this is a sweep) verify that every modern record key that exists is a
  member of its per-workflow index and its tag is in `wfs`, and alert on
  divergence. This is the only mechanical detector for F1 and the same sweep
  the rebuild already performs.
- **C. Functional canary (needs test infra).** Periodically write and List a
  sentinel activation in a canary namespace. Catches total blindness of the read
  path and latency regressions; does not catch per-workflow tag loss.

## 8. What is not verified

The unit suite covers fail-closed tag registration, tag-loss restore on the next
write, the extend-only TTL rule, probe fallback, the manual lever itself, and
escaped-namespace parity
(`entry_activation_index_regression_test.go`). Never observed, in any
environment:

- a live deployment recovering through this procedure;
- the 25-min attempt budget against a production-sized keyspace;
- a rebuild racing a concurrent write workload (benign by design, per §4, but
  unmeasured).

Until the first of those is rehearsed, treat this as a code-derived procedure
with unit-level evidence, not a drill-tested one.
