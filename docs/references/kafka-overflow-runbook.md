# Kafka Aggregate Overflow Runbook

Under `aggregate.enabled: true`, each partition of a Kafka trigger keeps a
bounded retained buffer. When that bound is reached and another record arrives,
`aggregate.on_overflow` decides what happens to the arriving record. This
runbook covers detection, diagnosis and remediation of the three resulting
states: **permanent data loss**, **a stalled assignment**, and **a failing
dead-letter publish**.

The policy is a parameter of the Kafka trigger node, so it is set per workflow,
while every metric on this axis labels only the **source topic** (plus namespace,
and partition on the gauges) — no group. Two workflows reading the same topic with
different groups therefore have independent overflow behaviour that no single
series distinguishes, and every expression below is per topic unless stated
otherwise.

> **Status.** No policy has been selected or approved. The choice is tracked as
> decision **D7** in
> [RELEASE-GATES.md](../design/RELEASE-GATES.md) §6 and is `OPEN — 未批准`
> there. `discard` being the current default is the preservation of pre-existing
> behaviour (`node/trigger/kafka/aggregate.go:1360-1369`), **not** an accepted
> product decision, and nothing in this runbook may be read as one.
>
> **Status.** No topic inventory has been filled in for any real environment.
> See §"Topic inventory" below for the procedure and
> [kafka-overflow-topic-inventory.md](kafka-overflow-topic-inventory.md) for the
> unfilled template.

## Not the outbox dead-letter axis

Two unrelated features in this repository are both called "dead letter". Applying
the wrong procedure to the wrong axis wastes the incident window, and the two
share no metric, no storage and no tooling:

| | **This runbook** — Kafka aggregate overflow | [dead-letter-runbook.md](dead-letter-runbook.md) — outbox / per-execution |
|---|---|---|
| What is parked | A Kafka record the aggregator read but had no buffer room for | A durable scheduling outbox entry that exceeded its delivery attempt limit |
| Where it is parked | A Kafka topic (`aggregate.dead_letter_topic`) | Redis per-execution dead-letter storage |
| Metrics | `xflow_trigger_messages_discarded_total`, `xflow_trigger_consumption_blocked`, `xflow_trigger_messages_dead_lettered_total` | `xflow_outbox_dead_letters*` |
| Operator tooling | **None.** Nothing in this repository reads the overflow topic | `xflow dead-letter list` / `replay` (`cmd/xflow/dead_letter.go`) |
| Replay | **No implemented replay path** — see §"Semantics verification" | Atomic dead→ready replay with a receipt and `--request-id` |

Do not reach for `xflow dead-letter replay` for a record that overflowed into a
Kafka topic: that command operates on Redis outbox entries
(`cmd/xflow/dead_letter.go:56-71`) and cannot see the overflow topic at all.

## Semantics

`on_overflow` has exactly three values, validated at activation against a closed
set (`node/trigger/kafka/aggregate.go:1325-1335`). There is no fourth setting
that avoids every cost, and that is a property of the implementation rather than
an oversight: one goroutine reads `consumer.Messages()` for every partition
(`aggregate.go:267-283`), and kafka-go's group `Reader` exposes no way to pause a
single partition (`aggregate.go:87-91`). Bounded memory therefore forces a
choice.

| Policy | What happens to the arriving record | Cost |
|---|---|---|
| `discard` (default) | **Permanent loss.** The record is counted and logged, then dropped; a later commit of a higher offset sweeps past it (`aggregate.go:808-827`, drop at `aggregate.go:971-974`) | Records are lost. Recovery is not possible at runtime |
| `block` | Nothing is lost. The coordinator stops receiving, its channel fills, `submit` blocks, and the **whole assignment** stops consuming until that partition's backlog drains (`aggregate.go:944-951`, `aggregate.go:913-917`) | Consumption halts for every partition this runner owns — its whole reader assignment, not the group's, so a sibling runner keeps its own partitions. **Invisible to the lag gauge** |
| `dead_letter` | Republishes the record to `dead_letter_topic` with its key, payload and provenance, and **only then** lets the offset advance (`aggregate.go:733-752`, `aggregate.go:847-864`) | One write round trip per overflowed record; a slow or failing dead-letter topic stalls the shared reader exactly as `block` does (`aggregate.go:96-109`) |

Two of these are counter-intuitive enough to state on their own.

**`discard` is permanent, not deferred.** The dropped record was already read;
the commit frontier then advances over its offset as ordinary subsequent batches
commit (`aggregate.go:634-671` builds the commit from buffered records only). The
loss becomes unrecoverable the moment any higher offset in that partition
commits. `TestKafkaAggregateShedMessagesAreSilentlySkipped` pins this: it
asserts that delivered offsets end up *below* the committed position without ever
having been emitted
(`node/trigger/kafka/aggregate_shed_test.go:236-247`). Until such a commit
happens — i.e. if the process dies first — the record would be redelivered by
Kafka, because nothing was committed past it. Treat "permanent" as "permanent
once the partition advances", which under any live traffic is immediate.

**`block` is invisible to lag.** Lag is sampled only when a message is *fetched*
(`node/trigger/kafka/consumer.go:380-387`; `ReadLagInterval: -1` at
`consumer.go:253` disables kafka-go's own reporting). A partition that has stopped
fetching therefore holds its last healthy value and reads as fine — under this
policy that is the steady state, not a corner case (`aggregate.go:865-873`,
`observability/metrics/metrics.go:451`). `xflow_trigger_consumption_blocked` is
the signal that does work. It is per **partition**, and only the blocked
partition reports it: its healthy-looking siblings on the same shared reader read
`0` while not being fetched at all.

`dead_letter` is the only policy that neither loses the record nor deliberately
stalls, and its failure direction is deliberate: a publish that *fails* is treated
as backpressure (the record is held, the partition stops receiving, the publish is
retried with backoff — `aggregate.go:753-772`), so a dead-letter outage degrades
to `block`'s trade and never to `discard`'s. `dead_letter` without a
`dead_letter_topic` **fails activation** rather than silently downgrading to
`discard` (`aggregate.go:1327-1331`), and activation also fails if the
dead-letter writer cannot be constructed, so a broker-unreachable dead-letter
topic surfaces as an activation failure rather than as a first-message surprise
(`node/trigger/kafka/kafka.go:350-368`).

This axis exists only when aggregation is enabled: with `aggregate.enabled:
false` there is no retained buffer and no overflow policy
(`aggregate.go:1296-1298`, `kafka.go:369-372`).

## Metrics

All three series carry a `namespace` label in addition to the ones listed
(`observability/metrics/metrics.go:333-339`); names are registered in
`observability/metrics/trigger.go:12-24`. Citations below are repository-relative
paths; `metrics.go` is `observability/metrics/metrics.go` and the `kafka` files
are under `node/trigger/kafka/`.

| Metric | Type | Meaning |
|---|---|---|
| `xflow_trigger_messages_discarded_total{topic,reason}` | counter | Records consumed but never emitted. `reason` is a closed enum: `schema`, `schema_fail`, `buffer_overflow` (`node/trigger/kafka/observer.go:23-38`). **`buffer_overflow` is the overflow-axis loss**, and is produced only under `on_overflow=discard` |
| `xflow_trigger_consumption_blocked{topic,partition}` | gauge | `1` while this partition has stopped consuming to avoid dropping records: `block` sitting at its retained bound, **or** `dead_letter` holding a record whose publish has not succeeded (`observer.go:68-88`, `aggregate.go:944-951`). Set on transitions, reported only by the affected partition |
| `xflow_trigger_messages_dead_lettered_total{topic,result}` | counter | Dead-letter publish attempts, `result` = `ok` \| `error`. **Both dead-letter axes report here**; the axis is only in the published record's `xflow-dlq-reason` header (`metrics.go:446`, `node/trigger/kafka/dlq.go:124-134`) |
| `xflow_trigger_consumer_lag{topic,partition}` | gauge | Fetch position vs. high-water mark. Frozen by a stalled consumer — never read it without the next series |
| `xflow_trigger_last_fetch_timestamp_seconds{topic,partition}` | gauge | Unix time of the most recent fetch on this partition. `time()` minus this is consumer staleness, and is the only way to tell a frozen lag reading from a live one (`observability/metrics/trigger.go:130-157`) |

### Alerts

- **Permanent data loss (overflow, `discard`)** —
  `sum by (namespace, topic) (rate(xflow_trigger_messages_discarded_total{reason="buffer_overflow"}[5m])) > 0`.
  Records were read and thrown away and nothing will redeliver them. Severity
  should reflect the topic: for a stream that cannot lose records this is a page,
  and it is also the evidence that the topic is on the wrong policy.
  *Does not cover:* topics configured `block` or `dead_letter` (they never
  produce this series — `TestKafkaAggregateOverflowPoliciesDivergeThreeWays`
  asserts that only the discard arm reports `buffer_overflow`, and that only the
  block arm reports blocked:
  `node/trigger/kafka/aggregate_overflow_dead_letter_test.go:259-297`); the
  `schema`/`schema_fail` reasons, which are a different event (see §"What this
  runbook does not cover"); records already swept past, which are unrecoverable;
  and **which partition** overflowed — this counter has no `partition` label, so
  take that from the WARN log line, which does
  (`aggregate.go:816-825`).

- **Stalled assignment (`block`, or a held `dead_letter`)** —
  `max by (namespace, topic) (xflow_trigger_consumption_blocked) > 0`.
  One blocked partition is enough: the reader is shared, so every partition this
  runner owns has stopped fetching (a sibling runner in the same group keeps
  consuming its own partitions). The gap in the expression is deliberate — the
  gauge only carries the state, and the transition log line names the cause
  (`aggregate.go:881-899`).
  *Does not cover:* which of the two causes it is (log only — `block` at the cap
  and a failed dead-letter publish set the same series, `observer.go:71-77`); the
  healthy-looking sibling partitions, which report `0` while not being fetched
  either; the **slow-but-succeeding** dead-letter write, which stalls the shared
  reader without ever flipping this gauge (the transition is evaluated between
  loop iterations and a successful publish clears `pendingOverflow` before the
  next one — `aggregate.go:855-861`, `aggregate.go:944-963`); and stalls from
  other causes, such as a poison batch or a broker rejecting commits, which have
  their own signals (`xflow_trigger_batch_flush_outcomes_total{result="error"}`
  and the stuck-batch WARN at `aggregate.go:1153-1172`).

- **Failing dead-letter publish** —
  `sum by (namespace, topic) (rate(xflow_trigger_messages_dead_lettered_total{result="error"}[5m])) > 0`.
  The record is being redelivered rather than parked and the source partition has
  stopped consuming on it. Fix the dead-letter broker, not the workflow.
  *Does not cover:* the schema axis, which reports into the same series
  (`metrics.go:446`) — that axis parks records that failed validation, not
  records the buffer had no room for; the topic records are being parked *into*
  (no label); whether anyone ever reads that topic (nothing does — §"Semantics
  verification", replay); or a dead letter topic that is slow but succeeding
  (no error series, and see the stall alert above).

- **Guard for a frozen lag gauge** —
  `(time() - xflow_trigger_last_fetch_timestamp_seconds) > 300 and on (namespace, topic, partition) xflow_trigger_consumer_lag > 0`.
  A partition that is behind *and* has not fetched for five minutes is stalled,
  not idle. This is the second opinion for the slow-dead-letter case the blocked
  gauge can miss, and the reason a lag figure alone must never be trusted.
  *Does not cover:* a partition that stalled at exactly `lag = 0` — with no
  fetches there is no new sample, so a producer writing into it fast cannot move
  the gauge, and only the blocked gauge or the log reveals it; and an idle topic
  looks identical unless the `lag > 0` term is kept.

Alert definitions live in this runbook by repository convention; there are no
Prometheus rule files in this tree.

## Diagnosis

Start from the two questions that separate the states: *is a partition blocked*,
and *is the loss counter moving*.

1. **Is any partition blocked?** `xflow_trigger_consumption_blocked == 1`. Note
   the topic and partition, then read the WARN log for that topic+partition:

   - `... at cap with on_overflow=block; HALTING consumption ...` — the `block`
     policy at its retained bound (`aggregate.go:893-898`).
   - `... at cap with on_overflow=dead_letter and the dead-letter publish
     FAILING ...` — a held record (`aggregate.go:882-890`). Go to step 3.
   - Neither line, but the gauge is `1`: the series is held from an earlier
     transition; the matching `backlog drained; resuming consumption` info line
     means it has ended (`aggregate.go:901-903`).

2. **Is the loss counter moving?** `rate(xflow_trigger_messages_discarded_total{reason="buffer_overflow"}[5m]) > 0`
   means a record was read and thrown away in the last five minutes, so the group
   responsible is on `discard`. Since the series carries no group label, a topic
   read by more than one consumer group needs that step confirmed per group;
   within one activation the two states are mutually exclusive.

3. **Lag looks healthy but consumption has stopped.** That is the expected
   reading, not a contradiction: the lag gauge is frozen at the last fetch
   (`consumer.go:380-387`). Confirm staleness with
   `time() - xflow_trigger_last_fetch_timestamp_seconds` and compare it against
   the assignment's other partitions; a partition whose timestamp is minutes
   older than its siblings is stalled, whatever its lag says. Remember that under
   `block` and a failing `dead_letter` publish **all** partitions of the
   assignment stop fetching, so the staleness appears on the siblings too while
   only one of them reports `1` on the blocked gauge. Then check whether the
   downstream or the dead-letter broker is actually the problem:
   `xflow_trigger_batch_flush_outcomes_total{result="error"}` rising means the
   emit path, not the dead-letter topic, and the WARN line above names the policy.

4. **Distinguishing "sustained overflow" from "traffic grew".** The retained
   bound is derived, not configured directly: `max_size × (4 + min(4,
   max_inflight))` records per partition (`aggregate.go:36`, `aggregate.go:43`,
   `aggregate.go:457-468`). A partition that has been at its cap for a long time
   has a downstream or a dead-letter topic that cannot keep up; a partition that
   touches the cap at a peak and drains has neither. `xflow_trigger_batch_size`
   and the flush trigger mix (`xflow_trigger_batches_flushed_total{trigger}`)
   tell the two apart: a mix dominated by `timeout` means the batch is configured
   larger than the traffic (`observability/metrics/metrics.go:449`).

## Remediation

Per state:

- **`discard` losing records now.** Nothing recoverable exists at runtime: the
  records are gone once the partition advances past them. Two actions remain —
  move the topic to `block` or `dead_letter` (a parameter change; see §"Choosing
  a policy"), and reduce the pressure that produced the overflow (more runners,
  faster downstream, or a dead-letter topic that can absorb the volume). Reading
  the topic again from an earlier offset with a *different* consumer group is the
  only way to see those records again — subject to the topic's retention, which
  may already have expired them — and it re-executes everything else in the range.

- **`block` at its cap.** The stall ends when the backlog drains, so the fix is
  whatever the downstream needs; nothing must be restarted for it to recover
  (`aggregate.go:901-903`). If a stalled assignment is unacceptable for this
  topic, that is the argument for `dead_letter`, not for staying on `block`.
  Restarting the trigger does *not* lose records — nothing past the stalled point
  was committed — but it does re-fetch from the group position.

- **`dead_letter` with a failing publish.** The dead-letter broker or topic is the
  incident. Fix the broker, the topic's existence, or the credentials; the held
  record is retried with backoff on its own (`aggregate.go:753-772`) and the
  partition resumes without a restart. Do **not** "unblock" a partition by
  switching the policy while it holds an unpublished record: the held record's
  offset was never committed, so switching to `discard` and restarting means it
  will be re-read and, if the buffer is still at its cap, dropped.

- **`dead_letter` publishing successfully but slowly.** Sustained overflow with a
  slow dead-letter topic makes that topic's write throughput this topic's ceiling
  (`aggregate.go:96-102`); the evidence is a stalled reader with a zero blocked
  gauge (see the alert), and the remediation is dead-letter throughput, not
  workflow tuning.

Not fixable at runtime, listed so it is not attempted during an incident:

- Records already discarded, and records already swept past in the dead-letter
  case but never consumed downstream.
- No dead-letter topic exists for a topic that is on `discard`: there is nothing
  to replay, and no historical overflow was ever captured.
- There is no claim-check, offset registry, or backfill of records discarded
  before the policy was changed.
- The blocked gauge cannot be used to prove an assignment is healthy: absence of
  the series means "no transition was ever reported for that partition", which is
  also what a partition that has never been blocked looks like.

## Choosing a policy

This runbook does not choose. The decision criteria are these, and the sign-off
is **D7** in [RELEASE-GATES.md](../design/RELEASE-GATES.md) §6, currently
`OPEN — 未批准`:

| If the topic... | then |
|---|---|
| can lose a record, and staying current matters more than completeness | `discard` is the pre-existing behaviour — but say so explicitly in the trigger definition rather than relying on the default |
| cannot lose a record, and a whole-assignment stall is tolerable for the duration of a downstream outage | `block`; it is the cheapest zero-loss option and needs no extra topic |
| cannot lose a record **and** cannot accept a stalled assignment during ordinary overflow | `dead_letter`, with a dead-letter topic that is provisioned, monitored and consumed — and only after the UNPROVEN items in §"Semantics verification" are closed for this deployment |

Overflow is a symptom of a capacity mismatch, so the durable answer to any of
these is usually throughput, not policy. Also note that `discard` is the only
policy an operator can end up on without having chosen it, because it is what an
absent `on_overflow` means (`aggregate.go:1360-1369`) — every topic that cannot
afford the loss must set `on_overflow` explicitly, and until D7 is signed off the
default's loss semantics must be disclosed in release notes rather than assumed
acceptable.

## Capacity: the dead-letter topic needs an operator

`dead_letter` moves records out of the aggregator and into a Kafka topic, and
nothing in xflow consumes that topic: "the records are durable but nothing
replays them on its own" (`kafka.go:196-199`). Consequences to plan for:

- The overflow topic needs **its own consumer, or its own retention policy with
  an explicit decision to let records expire**, and its own monitoring. Without
  either, an overflow event converts data loss into unbounded accumulation, and
  the first symptom is a broker disk alert rather than anything in this runbook.
- xflow neither creates the topic nor configures its replication factor,
  `min.insync.replicas` or retention. The publish is `Async: false` with
  `RequiredAcks: RequireAll` (`dlq.go:93-98`), so a successful publish is
  acknowledged by every in-sync replica — but with a single-replica topic that is
  one broker, and xflow cannot tell which.
- If the topic does not exist and broker auto-creation is disabled, every publish
  fails; that is the fail-closed path (the partition holds and stalls), not silent
  loss.
- The DLQ write budget must exceed the overflow rate it is absorbing, or the
  topic becomes this trigger's bottleneck (`aggregate.go:100-102`).
- **A dead-letter topic equal to the source topic is not rejected by any
  validation** (`aggregateConfigFromParamForMode`, `aggregate.go:1275-1337`, checks
  only that the field is non-empty). Republishing a record into the topic it was
  read from re-reads it later as new traffic, which is a self-feeding loop. Check
  this by hand in any configuration review.

Sizing the aggregator itself: retained memory is bounded per partition at roughly
`max_size × (4 + min(4, max_inflight))` records inside the reorder window, plus
`max_size` in the coordinator's own channel (`aggregate.go:340`) plus
`max_inflight` in the reader's channel (`consumer.go:222-224`, `consumer.go:196`).
Under `dead_letter`, an unpublished record adds exactly one more per partition
(`aggregate.go:505-511`).

## Topic inventory

The inventory for this axis does not exist yet — not for any environment. It is a
required input to D7 (see `docs/plans/2026-09-17-xflow-pre-ui-remediation-plan.md`
§0.2/C and §6/C.3), because "which topics must not lose records" is not derivable
from the code.

Procedure:

1. Enumerate every workflow definition containing a `xflow.trigger.kafka` node
   with `aggregate.enabled: true`. The node's `RawParams()` carries the effective
   `aggregate` object (`node/trigger/kafka/kafka.go:256-312`).
2. For each, record `topic`, `group`, `max_size`, `flush_interval`,
   `on_overflow`, `dead_letter_topic`, `max_inflight`, and whether `on_overflow`
   was written explicitly or is being defaulted. A definition whose `on_overflow`
   key is absent is on `discard`, and `on_overflow` is emitted only when it
   differs from the default (`kafka.go:289-311`), so **absence is the normal case
   and must be recorded as `discard (implicit)`, not left blank**.
3. Get the topic's real partition count from the broker, not from the
   definition: partitions are a broker property, and the exposure per topic is
   per partition.
4. Derive the exposure columns with the formulas above, then a human decides, per
   topic, whether `discard` is acceptable, whether `block`'s whole-assignment
   stall is acceptable, or whether the topic needs `dead_letter`.

The fillable table, its status banner and a worked derivation are in
[kafka-overflow-topic-inventory.md](kafka-overflow-topic-inventory.md).

## Semantics verification (`dead_letter`)

The plan requires that the `dead_letter` path, if adopted, have proven offset,
persistence, capacity, duplicate delivery, replay, failure and rollback
semantics. Audited against the implementation on this tree; the unproven items
lead.

| Semantics | Status | Evidence |
|---|---|---|
| **Replay** | **NOT ESTABLISHED** | No replay path for the overflow axis exists. The dead-letter topic is written and never read by anything in this repository; `xflow dead-letter replay` is the unrelated outbox axis (`cmd/xflow/dead_letter.go:56-71`). `kafka.go:196-199` states it: the records are durable but nothing replays them on its own. A re-drive must be built by the operator, and because the parked record may also have been executed (see duplicate delivery) a re-drive is not automatically safe |
| **Duplicate delivery** | **NOT ESTABLISHED (no dedup; at-least-once both ways)** | Nothing tracks parked offsets, and no dedup exists on either side. A parked record whose offset has not yet been swept past is redelivered after a restart *and* remains in the topic, so the same source offset can appear in the dead-letter topic twice across generations and can additionally be emitted downstream by the later generation. Retrying a publish after a client-side timeout duplicates it in the topic as well. The contract is the package-wide one: at-least-once, idempotency pushed to the consumer (`kafka.go:247`) |
| **Rollback** | **PARTIALLY ESTABLISHED** | Switching back to `discard` is a config change only: `aggregate.dead_letter_topic` is ignored under another policy (`aggregate.go:1322-1324`) and no dead-letter writer is opened (`kafka.go:385-391`). Records already parked stay in the topic; nothing un-parks or deletes them, and the switch does not lose them. **Not established:** a record held unpublished at the moment of the switch is never parked, and after a restart on `discard` it is re-read and — if the buffer is at its cap — dropped. There is also no guard against a mixed rollout, where runners on the same group hold different policies and therefore apply different policies to different partitions; and a binary predating `dead_letter` is not present in this tree and cannot be audited here (the current code rejects unknown values rather than falling back, `aggregate.go:1332-1335`) |
| **Offset** | ESTABLISHED | The publish is synchronous on the partition's own coordinator and nothing may commit while it is unpublished: `handleOverflow` sets `pendingOverflow` and returns only after success (`aggregate.go:847-864`); `publishOverflowDeadLetter` is the single call site (`aggregate.go:733-752`); the receive arm is disabled while `pendingOverflow != nil`, so no higher offset is even read (`aggregate.go:944-951`); the parked record never enters buffer or batches, so the retained bound survives (`aggregate.go:505-511`, `aggregate.go:831-840`). After a successful publish the offset is swept past by an ordinary higher commit, which is the audited hand-off the policy intends |
| **Persistence** | ESTABLISHED (within the topic's own durability) | `Async: false` with `RequiredAcks: RequireAll`, so `Publish` returns only after the broker acknowledges (`dlq.go:93-98`, `dlq.go:103-113`), and key, value and original headers travel with the record plus `xflow-dlq-reason` / `xflow-dlq-source-{topic,partition,offset}` (`dlq.go:124-134`). A publish returns an error rather than dropping. What is *not* established by xflow is topic-level durability configuration — replication factor and `min.insync.replicas` are broker-side and unset here |
| **Capacity** | ESTABLISHED for the aggregator, NOT ESTABLISHED for the topic | Aggregator memory stays bounded: an unpublished record is one per partition and is never added to the buffer (`aggregate.go:505-511`, `aggregate.go:110-114`), and the retained bound is unchanged. **Not established:** the dead-letter topic's own capacity — no retention, no size bound, no consumer, so accumulation is unbounded until a broker-level policy is applied by an operator |
| **Failure** | ESTABLISHED | A failed publish holds the record, stops the receive and retries with the same flat-then-exponential cadence as a failed batch (`aggregate.go:753-772`), so a dead-letter outage degrades to `block` and never to `discard`. A missing publisher fails closed rather than dropping (`aggregate.go:733-743`). A publish is skipped while closing, and the record's offset was never committed, so the next generation redelivers it (`aggregate.go:753-756`, `aggregate.go:1062-1073`). `dead_letter` without a topic fails activation (`aggregate.go:1327-1331`), as does a publisher that cannot be built (`kafka.go:350-368`) |

Tests pinning this behaviour: `TestKafkaAggregateOverflowPoliciesDivergeThreeWays`,
`TestKafkaAggregateOverflowDeadLetterPublishFailureHoldsAndStalls`,
`TestKafkaAggregateOverflowDeadLetterWithoutPublisherFailsClosed`,
`TestKafkaAggregateOnOverflowDeadLetterValidation` and
`TestKafkaActivateBuildsPublisherForOverflowDeadLetterPolicy` in
`node/trigger/kafka/aggregate_overflow_dead_letter_test.go`; the discard
permanence probe is `TestKafkaAggregateShedMessagesAreSilentlySkipped` in
`node/trigger/kafka/aggregate_shed_test.go`.

## What this runbook does not cover

- **The outbox / per-execution dead-letter axis.** Different storage, metrics and
  tooling; see [dead-letter-runbook.md](dead-letter-runbook.md).
- **Invalid-message handling** (`message_schema.on_invalid`). It has its own
  policy enum, its own dead-letter topic, and it reuses
  `xflow_trigger_messages_dead_lettered_total` — so a non-zero `result="error"`
  rate there is the same series as an overflow dead-letter failure. Tell them
  apart by the record's `xflow-dlq-reason` header (`schema` vs `buffer_overflow`,
  `dlq.go:15-26`) and by the log line. A batch held by a failing schema
  dead-letter publish also occupies the partition and looks like a stuck batch,
  not like overflow.
- **Poison batches and commit failures.** A partition whose commit frontier is
  frozen by a batch that keeps failing looks similar on a lag dashboard; its
  signals are the flush-outcome error ratio and the stuck-batch WARN with offset
  bounds (`aggregate.go:1153-1172`), not the overflow metrics.
- **Whether the topic is configured at all.** No inventory has been filled in for
  any environment; nothing here describes a real topic, group, partition count or
  load figure.
