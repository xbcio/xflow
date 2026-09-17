# Kafka Aggregate Topic Inventory (template)

> **Status: EMPTY — no inventory has been filled in for any environment.** This
> file is a procedure and a blank template. Until it is filled in from a real
> deployment it is not evidence that any topic's overflow behaviour is known, and
> it must not be cited as such.
>
> Required input to the Kafka overflow decision (D7 in
> [RELEASE-GATES.md](../design/RELEASE-GATES.md) §6, `OPEN — 未批准`) and to
> `docs/plans/2026-09-17-xflow-pre-ui-remediation-plan.md` §0.2/C. Operating
> procedure and metric semantics: [kafka-overflow-runbook.md](kafka-overflow-runbook.md).

## Why this exists

`aggregate.on_overflow` defaults to `discard`, which loses records permanently
(`node/trigger/kafka/aggregate.go:1360-1369`). Which topics can afford that is a
product decision per topic, and it cannot be derived from the code: the code
knows the policy, not whether the records matter. The decision needs the current
state of every aggregating Kafka trigger — including the ones that are silently
on the default.

Record the **effective** configuration, not the intended one. A trigger
definition that never wrote `on_overflow` is on `discard`, and that is the case
worth surfacing: `RawParams()` emits `on_overflow` only when it differs from the
default (`node/trigger/kafka/kafka.go:289-311`), so the key's **absence** means
`discard`, and a blank cell in the table below is a missing measurement rather
than an "unset" policy.

## Procedure

1. **Enumerate the triggers.** List every workflow definition containing a
   `xflow.trigger.kafka` node with `aggregate.enabled: true`
   (`node/trigger/kafka/kafka.go:256-312` is the serializer; the descriptor at
   `kafka.go:242-249` lists the parameter names). A Kafka trigger without
   aggregation has no overflow axis at all (`aggregate.go:1296-1298`,
   `kafka.go:369-372`) and does not belong in this table.

2. **Record the effective settings per trigger.** `topic`, `group`, `max_size`,
   `flush_interval`, `on_overflow` (with `(implicit)` when the key is absent),
   `dead_letter_topic`, and `max_inflight`. Also record the hosting mode —
   entry-seed or legacy Emit — because the *default* `flush_interval` differs
   between them (1s vs 100ms) and the Go DSL path bakes its interval at
   construction time (`kafka.go:274-281`).

3. **Get the partition count and group membership from the broker**, not from the
   definition: partitions are a broker property, and every exposure figure below
   is per partition. Note whether more than one consumer group reads the topic —
   each group has its own assignment, its own policies and therefore its own
   exposure. Within one group the assignment is decided by the group balancers
   (`node/trigger/kafka/consumer.go:266`), not by xflow.

4. **Derive the exposure columns.** Do not estimate; compute from the recorded
   settings:

   - `retained_bound_per_partition = max_size × (4 + min(4, max_inflight))`
     — records the reorder window may hold
     (`aggregate.go:36`, `aggregate.go:43`, `aggregate.go:457-468`).
   - `in_flight_ceiling_per_partition = retained_bound + max_size + max_inflight`
     — adds the coordinator's own channel (`aggregate.go:340`) and the reader's
     channel (`consumer.go:222-224`, `consumer.go:196`). This is the number of
     records one partition can be holding when it stops or drops, so multiply by
     the mean record size before comparing it against a memory budget.
   - `loss_exposure` — `permanent, unrecorded-until-alert` under `discard`;
     `none, but the whole assignment stops` under `block`; `none, plus one
     dead-letter record per overflow` under `dead_letter`.
   - `stall_exposure` — the partitions that stop consuming when this partition
     hits its bound: under `block` and a failing `dead_letter` publish it is
     **every partition this runner owns** — its whole reader assignment, since
     one goroutine reads the shared `consumer.Messages()` channel
     (`aggregate.go:913-917`, `aggregate.go:267-283`) — not just the overflowing
     one, and not necessarily the group's whole set of partitions, which is split
     across runners by the group balancers.
   - `dead_letter_topic_owner` — which team consumes the overflow topic, and its
     retention. Nothing in xflow reads it (`kafka.go:196-199`); if the answer is
     "nobody", the topic accumulates.

5. **Decide per topic** and record the decision, the decider's role and the date.
   Until D7 is signed off, the correct status for every row is the current
   effective policy plus an explicitly open decision — not a policy this template
   implies was approved.

## Inventory

Copy this block per environment. Replace every placeholder; a `<...>` left in
place means the row is unmeasured.

```markdown
# Kafka aggregate topic inventory — <environment name>

- Date: <YYYY-MM-DD>
- Collector (role): <role>
- Cluster / brokers (no credentials): <broker list or cluster name>
- Scope: <workflows | namespaces> covered by this table

## Triggers

| # | Workflow | Namespace | Topic | Group | Hosting mode | max_size | flush_interval | max_inflight | on_overflow | dead_letter_topic |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | <workflow> | <ns> | <topic> | <group> | entry-seed \| legacy | <n> | <dur> | <n> | discard (implicit) \| discard \| block \| dead_letter | n/a \| <topic> |

## Broker facts

| Topic | Partitions | Replication factor | min.insync.replicas | Retention | Groups reading it |
|---|---|---|---|---|---|
| <topic> | <n> | <n> | <n> | <dur> | <group(s)> |

## Derived exposure

| Topic | Group | retained_bound_per_partition | in_flight_ceiling_per_partition | mean record bytes | peak in-flight bytes per partition | loss_exposure | stall_exposure (partitions) | dead_letter_topic_owner / retention |
|---|---|---|---|---|---|---|---|---|
| <topic> | <group> | <computed> | <computed> | <bytes> | <computed> | <see procedure> | <n of assignment> | <team> / <dur> |

## Decisions

| Topic | Decision | Rationale | Decider (role) | Date | D7 status |
|---|---|---|---|---|---|
| <topic> | keep discard \| move to block \| move to dead_letter \| open | <why> | <role> | <YYYY-MM-DD> | OPEN — 未批准 |
```

## Worked derivation

For a trigger with the documented defaults — `max_size: 100`, `max_inflight: 64`
— and a 12-partition topic in one group:

- `retained_bound_per_partition = 100 × (4 + min(4, 64)) = 100 × 8 = 800` records.
- `in_flight_ceiling_per_partition = 800 + 100 + 64 = 964` records.
- With 1 KiB records that is ≈ 964 KiB per partition, ≈ 11.3 MiB across the
  assignment, held in memory before either dropping or stopping.
- `loss_exposure`: permanent under the default `discard`. The partition that
  overflows is identified only from the WARN log — the loss counter has no
  partition label (`observability/metrics/trigger.go:55-59`).
- `stall_exposure`: with `block` (or a failing `dead_letter` publish) on any one
  of the 12 partitions, all 12 stop fetching, because the reader is shared
  (`aggregate.go:944-951`). One partition is enough to stall the assignment. If
  the group's 12 partitions were split across three runners, each stall would be
  confined to the ~4 partitions of the runner that owns the overflowing one;
  size this column from the observed assignment, not from the topic's partition
  count.

These figures are arithmetic from the constants, not measurements. They bound
memory; they say nothing about throughput, which is a load question for a
capacity report — see [capacity-report-template.md](capacity-report-template.md).

## Anti-claims

- ❌ This table is not filled in for any environment; it is not evidence of any
  topic's configuration.
- ❌ A blank `on_overflow` cell is not "unset" — absence means `discard`
  (`kafka.go:289-311`). Record it as `discard (implicit)`.
- ❌ An empty `dead_letter_topic_owner` is not "no action needed" — it means
  overflow records accumulate in a topic nobody reads (`kafka.go:196-199`).
- ❌ Filling this table does not select a policy. D7 owns that sign-off and is
  `OPEN — 未批准`.
