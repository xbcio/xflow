# Runner Replacement / Drain Runbook

This runbook covers **taking one runner out of new-work admission, confirming it
has actually quiesced, replacing the process, and putting the replacement back
into service** using the platform-operator runner-control API.

It is not a server-maintenance procedure. Server graceful shutdown is a
different mechanism with a different trigger and a different safety argument —
see [maintenance-window-runbook.md](maintenance-window-runbook.md) §2.1. Queue
and execution-plane context for the runner topology is in
[DEPLOYMENT-TOPOLOGIES.md](../design/DEPLOYMENT-TOPOLOGIES.md).

> **This procedure has never been exercised end-to-end in this repository.**
> Every statement below is derived from source, not from an observed drill. The
> state machine, the HTTP surface, and the runner-side gate each have unit and
> integration coverage (see [§10](#10-what-is-not-verified)), but **no run has
> been observed driving a real runner process through drain → complete →
> replace → re-register → resume against a live control plane.** Treat this as a
> code-derived procedure that still needs its first rehearsal. Owner assignment
> and a review deadline for this runbook are still open under RELEASE-GATES §6.1
> / decision **D6**, and nothing in this document decides them.

## 1. What the drain API does and does not mean

`POST /v1/management/runners/{id}/drain` is a real operator control, not a gap in
capability:

| Item | Where |
|---|---|
| `PathManagementRunnerDrain` = `/v1/management/runners/{id}/drain` | `service/apiserver/paths.go:52` |
| `PathManagementRunnerResume` = `/v1/management/runners/{id}/resume` | `service/apiserver/paths.go:53` |
| Route registration (POST, principal-auth gated) | `service/apiserver/module_management.go:215` (drain), `:218` (resume) |
| `OpManagementRunnerDrain` = `"management.runner.drain"` | `service/apiserver/authz.go:125` |
| `OpManagementRunnerResume` = `"management.runner.resume"` | `service/apiserver/authz.go:126` |
| OpenAPI operation `drainRunner` | `api/openapi/xflow-v1.yaml:933`–`938` |

**Read this paragraph before you run the command.** The only thing a `200` from
drain tells you is that the server-side new-claim gate is now closed
(`service/apiserver/doc.go:70`–`77`):

> A successful POST /v1/management/runners/{id}/drain confirms only that the
> server-side new-claim gate is closed. It neither exits a runner nor proves
> that all work has settled. The returned complete phase is a conditional
> convergence observation, not process termination. timed_out leaves the runner
> in draining state and keeps the gate closed; it does not implicitly resume,
> evict, cancel, or reclaim work.

A successful drain therefore does **not** mean the runner stopped, does **not**
mean in-flight work finished, and does **not** cancel a running handler, revoke
a lease, or terminate a process. The same three negatives are stated in the
OpenAPI description of the operation: it "does not remotely terminate the
process, cancel a handler, or force-revoke a lease"
(`api/openapi/xflow-v1.yaml:941`–`942`), and the `RunnerControlSnapshot` schema
warns that `phase=complete` is "not a remote shutdown, a command to terminate a
worker, or authorization to discard unknown or unsettled work"
(`api/openapi/xflow-v1.yaml:1819`–`1821`).

"Recovery-only" does not mean the runner stops its poll loop either. The runner
keeps polling at the same cadence with `RecoveryOnly: true` in the poll request
(`service/runner/runner.go:361`, inside `pollLoop`, `service/runner/runner.go:340`);
what changes is that those polls can no longer pick up new queue claims.

## 2. Preconditions and access

### 2.1 Two scopes, both required

Drain and resume are **platform-operator** controls. They are deliberately *not*
namespace-scoped, because one runner may serve several tenants, and a
namespace-scoped principal must not be able to take another tenant's shared
worker out of service.

- They are mounted **only** behind `PrincipalAuth`, with no unauthenticated dev
  fallback (`service/apiserver/module_management.go:211`–`214`).
- Authorization is two-layered: `authzWrap` admits the per-action base scope,
  and the shared authorization decision additionally requires
  `ScopeManagementRunnerControlGlobal` = `"management.runner.control_global"`
  for exactly these two operations (`service/apiserver/authz.go:193`,
  `service/apiserver/authz.go:338`–`346`). The handler repeats the global-scope
  check before any directory capability assertion or runner lookup, so an
  otherwise-authorized tenant principal cannot use this route as a runner-ID
  existence oracle (`service/apiserver/module_management.go:523`–`526`).

Practically: your credential needs **both** `management.runner.drain` (or
`.resume`) **and** `management.runner.control_global`. The `--auth-tokens-file`
mapping format is `[{token, subject, namespace, scopes}]`
(`cmd/server/main.go:208`), so both scopes go in that principal's `scopes`
array. This surface requires `--management` on `cmd/server`
(`cmd/server/main.go:210`).

### 2.2 Request shape

```bash
# Drain: close the new-claim gate for one runner.
curl -sS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "X-Request-Id: $(uuidgen)" \
  -H "Content-Type: application/json" \
  -d '{"reason":"replace runner build 2026-09-18-a"}' \
  "$SERVER/v1/management/runners/$RUNNER_ID/drain"
```

- `X-Request-Id` is a **required idempotency key**, not a correlation header.
  It must match `[A-Za-z0-9._-]{1,128}`; absent or malformed values return
  `400 request_id_required` (`api/openapi/xflow-v1.yaml:1456`–`1466`,
  `service/apiserver/module_management.go:531`–`535`).
- `reason` is the entire request body and is mandatory (`minLength: 1`,
  ≤512 UTF-8 bytes) (`api/openapi/xflow-v1.yaml:1803`–`1810`,
  `service/apiserver/module_management.go:541`–`549`).
- The same principal + action + runner + body with the same `X-Request-Id`
  replays the original receipt; a **changed** body reusing the id returns
  `409 idempotency_key_reused`
  (`service/apiserver/module_management.go:580`–`581`).
- Error mapping: `404 runner_not_found` for an unknown runner, `501
  runner_control_unsupported` when the configured directory cannot atomically
  persist the transition and gate its own claim path
  (`service/apiserver/module_management.go:555`–`559`, `:582`–`583`).

## 3. The safety model has two halves

Do not conflate them. Only the first one is a guarantee.

**Server side is authoritative.** `protocol.RunnerControlDirective` is described
as "a cooperative convergence signal only: the control plane always applies the
authoritative DRAINING gate before creating a queue claim, including for old
runners that do not understand this DTO"
(`service/protocol/types.go:65`–`76`). The gate is the first check in the claim
Lua transition — `if (redis.call('HGET', KEYS[15], ARGV[1]) or 'active') ==
'draining' then return 'draining' end`
(`service/control/redis_runner_directory.go:1840`). Active desired state admits
compatible new claims; draining state does not, and permits only recovery of
already-finalized leases owned by the runner's current session
(`service/control/doc.go:54`–`58`). **A runner that ignores the directive is
still fenced.** This is why drain is safe to issue to an old binary.

**Runner side is convergence only.** `runnerControlGate` "never provides
server-side safety: a control-plane directory independently fences new claims.
Its job is to stop ordinary polling promptly, keep recovery polling alive for
pre-drain leases, and handle duplicate/out-of-order protocol responses without
reopening admission accidentally" (`service/runner/control_gate.go:14`–`18`).
The gate fails closed on a malformed directive and refuses to let an
equal-generation `ACTIVE` response reopen scheduling
(`service/runner/control_gate.go:29`–`61`). `RunnerControlDirective.RecoveryOnly`
tells a capable runner to stop ordinary polling and request only
already-finalized handoff replay (`service/protocol/types.go:72`–`75`).

**Consequence for the operator:** the gate closes as soon as the drain call
returns, even if the runner process is wedged, unresponsive, or an old build.
Conversely, an unresponsive runner can never *delay* the gate closing — but it
can and will delay the `complete` projection, because `complete` also requires
runner-side evidence.

## 4. Drain projection states and the exact completion predicate

A draining runner projects one of `quiescing`, `complete`, or `timed_out`
(`service/control/doc.go:60`; `service/control/runner_control.go:33`–`40`).

The projection is computed in `runnerControlProjection`
(`service/control/runner_control.go:201`–`251`):

```
server_quiescent = active_claims == 0 && leased_tasks == 0
                && unsettled_drain_debt == 0 && pending_activation_cleanup == 0
runner_quiescent = freshness-fenced quiet observation (see below)
phase = complete    if server_quiescent && runner_quiescent
      = timed_out   else if the drain deadline has elapsed
      = quiescing   otherwise
```

**The predicate you must wait for is `drain.phase == "complete"` — nothing
weaker.** That requires *both* halves:

1. **Zero durable server-side drain debt** — no active claims, no leased tasks,
   no unsettled handoff debt, and no durable drain-triggered trigger
   deactivation obligations (`service/control/runner_control.go:221`–`224`).
2. **A fresh quiet observation from the runner**, fenced to the current live
   session and control generation, reporting `recovery_only=true`,
   `in_flight=0`, and `active_activations=0`
   (`service/control/runner_control.go:274`–`293`). Freshness is measured
   against the **server** clock, not a runner timestamp
   (`service/control/doc.go:63`–`64`).

`RunnerDrainObservation` is a runner-*local* observation made only after the
runner applied a DRAINING directive (`service/protocol/types.go:78`–`84`), and
the runner only emits it once its own gate has converged —
`heartbeat()` attaches it only when `drainingGeneration()` reports draining
(`service/runner/runner.go:621`–`627`), and `drainingGeneration` exists so a
heartbeat can use it as drain evidence "without trusting a response that merely
arrived but has not converged the local gate yet"
(`service/runner/control_gate.go:72`–`74`).

Diagnostic fields in the drain snapshot, all server-derived
(`service/control/runner_control.go:58`–`70`, JSON names in
`api/openapi/xflow-v1.yaml:1842`–`1893`):

| Field | Means |
|---|---|
| `phase` | `quiescing` / `complete` / `timed_out` |
| `deadline_at` | Fixed deadline created by the ACTIVE→DRAINING transition. Same-state requests, receipt replays, and re-registration do **not** extend it; resume clears it (`service/control/doc.go:64`–`67`) |
| `active_claims`, `leased_tasks` | Server-side admission/execution counters |
| `unsettled_drain_debt` | Non-terminal pre-drain handoffs owned by this runner |
| `handoff_debt`, `lease_may_exist_debt` | Unfinalized handoffs needing an engine-aware resolver; `lease_may_exist` ones are crash-fenced with an engine lease possibly still live |
| `replayable_debt` | Finalized lease handoffs awaiting result commit or token-fenced reclaim |
| `pending_activation_cleanup` | Drain-triggered trigger deactivation obligations awaiting a fenced receipt |
| `server_quiescent`, `runner_quiescent` | The two halves of the predicate, exposed separately so you can see which one is blocking |

**Config defaults** (these are defaults, not tunables you should assume for your
deployment): drain deadline 30 minutes, observation freshness 30 seconds,
drain-idempotency receipt retention 24 hours
(`service/control/runner_control.go:14`–`16`). They are overridable through the
directory options `WithMemoryRunnerDirectoryDrainDeadline` /
`WithRedisRunnerDirectoryDrainDeadline` and the matching
`...DrainObservationFreshness` options
(`service/control/memory_runner_directory.go:41`, `:52`;
`service/control/redis_runner_directory.go:81`, `:92`). **Confirm the values your
deployment actually configured before you promise anyone a maintenance window
length.**

## 5. Pre-flight

Do these before you issue the drain. All are read-only.

1. **Control plane is serving and knows its leadership state.**
   `/healthz` returns `{"status":"ok"}` (`service/apiserver/module_management.go:670`–`675`);
   `/readyz` returns readiness plus `leader`, and answers `503` with
   `ready:false` when the readiness check fails
   (`service/apiserver/module_management.go:682`–`697`). Both stay open for
   probes; `/v1/management/*` is gated (`cmd/server/main.go:210`).
2. **Enumerate the fleet.**
   `GET /v1/management/runners` lists every runner id with an `enrolled` flag
   (`api/openapi/xflow-v1.yaml:828`–`861`). **A `501 runner_listing_unsupported`
   here means "this backend cannot answer the question", not "no runners are
   online".** The handler returns 501 rather than an empty array precisely so
   those two pictures stay distinguishable
   (`service/apiserver/module_management.go:408`–`417`;
   `service/apiserver/module_management_runners_test.go:46`–`52`). If you get a
   501, **fall back to `GET /v1/management/runners/{id}` for the specific runner
   you intend to replace** — single-runner lookup still works, which is the
   whole point of the 501. Do not read a 501 as an empty fleet, and do not
   conclude the runner is gone. A generic `500 internal_error` here is a third,
   different condition: the directory *can* enumerate but the call failed (for
   example a transient backend outage), and it must not be read as "nothing is
   online" either (`service/apiserver/module_management.go:431`–`435`).
3. **Read the target's current control state before you mutate it.**
   `GET /v1/management/runners/{id}` returns the `RunnerSnapshot`, whose
   `control` field carries `desired_state`, `generation`, `requested_at`, and
   the `drain` projection (`service/control/runner_directory.go:70`–`87`,
   `service/control/runner_control.go:75-81`). `control` is omitted entirely
   when the directory does not implement the optional control capability
   (`service/control/runner_directory.go:83`–`86`) — if you do not see it, the
   drain route will answer 501 and this runbook does not apply.
   Record `generation`: after the drain it must advance by exactly one.
4. **Confirm the replacement is ready to start** — same runner id, working
   config, and a successful pre-flight. `bin/runner verify` answers "how would
   *this* runner behave once it starts" and shares the connection path with
   `run` (`sdk/runner/verify.go:13`–`31`, `sdk/runner/verify.go:35`–`49`;
   build target `bin/runner`, `Makefile:62`).
5. **Declare the window.** Callers whose work is routed to this runner will stop
   receiving new tasks from this runner. Coordinate with
   [maintenance-window-runbook.md](maintenance-window-runbook.md) if a control
   plane change is landing in the same window.

## 6. Sequence

The order below is the procedure. Steps 3 and 4 are the two that operators skip
and then regret.

1. **Announce.** Note the runner id, the reason, and the expected window.
2. **Drain.**
   `POST /v1/management/runners/{id}/drain` with a required `X-Request-Id` and
   `reason` (§2.2). `200` means **the gate is closed** — not that the runner
   stopped (§1). Record the returned `generation` and `deadline_at`.
3. **Wait for `phase == "complete"`, re-reading the projection.** Poll
   `GET /v1/management/runners/{id}` on the order of the heartbeat interval, and
   require `control.drain.phase == "complete"`. `quiescing` is not done. Track
   `server_quiescent` and `runner_quiescent` separately to see which half is
   blocking you. **Do not proceed on the drain response alone, and do not
   proceed on `quiescing`.**
4. **Decide what is safe while `quiescing`.**
   - **Safe:** letting admitted work finish; watching the projection; preparing
     the replacement binary; issuing the same drain again with the same
     `X-Request-Id` if you lost the response (it replays the receipt).
   - **NOT safe:** stopping the runner process (that strands in-flight leases as
     handoff debt; see §7); assuming no new work will arrive on *this* runner's
     already-admitted claims; treating the drain as a cancellation; discarding
     or reassigning work you believe is stuck without resolving it through the
     control plane.
5. **Replace the process.** Only after `complete`: stop the old process with
   `SIGTERM` and start the replacement. Runner `SIGTERM`/`SIGINT` handling is
   the runner-process lifecycle path (`sdk/runner/run.go:522`) and is a
   **different** mechanism from drain; drain itself never sends a signal and
   never exits the process.
6. **Confirm re-registration.** The replacement registers and appears again in
   the directory with a **new session**. `Register` installs a fresh fenced
   session, returns the prior session's ordinary unfinalized claims to the
   durable queue, and rebinds uncertain handoffs and finalized leases to the
   replacement session
   (`service/control/redis_runner_directory.go:217`–`221`).
   **Re-registration does not clear a drain**: the register transition only
   initializes desired state on first sight (`HSETNX ... 'active'` at
   `service/control/redis_runner_directory.go:1701`–`1702`), and it does not
   extend the deadline.
7. **Resume.**
   `POST /v1/management/runners/{id}/resume` with a fresh `X-Request-Id`.
   Resume sets the desired state back to `active`, which clears the drain
   deadline in the same atomic transition
   (`service/control/redis_runner_control.go:315`–`318`) and lets the runner
   claim new work again. Old drain receipts stay immutable: retrying an earlier
   drain id cannot reapply it after a newer resume
   (`api/openapi/xflow-v1.yaml:986`–`1000`).
8. **Confirm admission is back.** Check `desired_state == "active"` with a
   generation one higher than step 2's, confirm `xflow_runner_up` (or the
   equivalent liveness signal for your deployment, §8) is back to 1, and only
   then close the window.

> Resume is not a synchronous scheduling action: it does not start a process and
> does not place trigger activations. Normal reconciliation remains responsible
> for subsequent activation placement
> (`api/openapi/xflow-v1.yaml:996`–`1000`). Expect a reconciliation interval
> before every activation is hosted again; do not "fix" that by re-triggering
> activation work by hand.

## 7. Rollback and the two failure cases

**If you `resume` a runner whose drain already projected `timed_out`:** resume is
legal and it does exactly what resume always does — desired state returns to
`active`, the admission gate reopens, and the deadline is cleared
(`service/control/runner_control.go:30` — the drain phase "is deliberately not a
desired state: resume always transitions the desired state back to ACTIVE").
What resume does **not** do is settle the work that caused the timeout. If you
resume, the same unsettled debt is still there, and it will be scheduled against
this runner again, which may be exactly what you did not want. **Resuming a
timed-out drain is a decision to put the runner back into service with its
unresolved debt intact — not a way to clean up the timeout.** Before resuming,
read `unsettled_drain_debt`, `handoff_debt`, `lease_may_exist_debt`,
`replayable_debt`, and `pending_activation_cleanup` and decide whether returning
this runner to admission is the right answer for the work they represent.

**If you kill the drained runner process instead of letting it drain:** you have
skipped the only step that settles its in-flight work. The consequences are
structural, not cosmetic:

- A claim that had crossed into lease construction but was never finalized
  acquires `lease_may_exist` handoff state, which is **deliberately never
  requeued** — the replacement session must resolve it against the engine first
  (`service/control/redis_runner_directory.go:217`–`221`, `:1631`–`1637`).
- Durable assignment/lease/outbox state persists and is recovered per the
  at-least-once argument in
  [maintenance-window-runbook.md](maintenance-window-runbook.md) §2.2; a handler
  that was mid-flight may be executed a second time, so side effects must be
  idempotent.
- With `--id` / `XFLOW_RUNNER_ID` unchanged
  (`--id`, `sdk/runner/run.go:144`; env `XFLOW_RUNNER_ID`, `sdk/runner/config.go:312`–`314`; default id is
  `runner-<pid>`, so an unchanged explicit id is what makes a replacement the
  *same* runner), the new process registers as the same runner id and therefore
  **inherits the draining desired state** (§6 step 6). Do not assume a restart
  resets drain.

**To abandon a drain without replacing the process:** `resume` is the only
supported way back. There is no "cancel drain" verb; `resume` is it.

## 8. Observability

Fleet-level drain state **is** observable in Prometheus. Registry help text
(`observability/metrics/metrics.go:423`–`426`) and label sets
(`observability/metrics/runner_control.go:10`–`45`):

| Metric | Type / labels | Meaning |
|---|---|---|
| `xflow_runner_control_transitions_total{action,result}` | counter; `action` ∈ `drain`/`resume`/`other`, `result` ∈ `transitioned`/`unchanged`/`replayed`/`conflict`/`not_found`/`invalid`/`unsupported`/`error`/`other` | Completed drain/resume operations. Unknown values collapse to `other` so caller mistakes cannot create a high-cardinality label (`observability/metrics/runner_control.go:150`–`173`) |
| `xflow_runner_draining_count` | gauge, no labels | Runners whose desired state is draining, across **every** drain phase — it does not tell you which phase |
| `xflow_runner_drain_duration_seconds` | histogram, no labels | Elapsed time for drains that reached `complete` only |
| `xflow_runner_drain_blockers{kind}` | gauge; `kind` ∈ `handoff_debt`/`replay_debt`/`activation_receipt`/`runner_ack`/`deadline` | Fleet-summed blockers across one observation. `kind="deadline"` is the count of drains whose phase is `timed_out` (`observability/metrics/runner_control.go:57`–`64`, `:131`–`146`) |

Two caveats an operator must know before alerting on these:

- **These are fleet aggregates with no runner identity and no namespace.** That
  is deliberate: "management APIs are the surface for per-runner diagnosis"
  (`observability/metrics/runner_control.go:47`–`53`). To answer "is *my* runner
  draining, and what is blocking it", read
  `GET /v1/management/runners/{id}` — not Prometheus.
- **They are emitted only when the control plane was constructed with metrics**
  (`if cfg.Metrics != nil` — `service/control/controlplane.go:368`–`374`). The
  collector refreshes fleet gauges every 15 s
  (`service/control/runner_control_metrics.go:13`, `:131`–`145`) because
  heartbeat, handoff, and activation-cleanup changes can complete a drain
  without a new management mutation
  (`service/control/runner_control_metrics.go:128`–`130`). If metrics are not
  wired, none of the above exists and you must drive the runbook off the
  management API.

Runner liveness is observable through `xflow_runner_up{runner_id="…"}`
(1 live, 0 retained-but-judged-dead) and
`xflow_runner_metrics_last_report_age_seconds{runner_id="…"}`, both help-texted
at `observability/metrics/metrics.go:428`–`429` and emitted from
`service/control/metrics_inbox.go:251`–`253`. **They are only emitted when the
runner-metrics proxy is enabled** (`--enable-runner-metrics-proxy`,
`cmd/server/main.go:218`) and only for runners that have shipped a metrics
report; series for a runner judged dead are dropped rather than zeroed
(`service/control/metrics_inbox.go:255`–`259`).

> Do not treat any of these as an SLO. No availability target, drain-time
> budget, or throughput number is asserted anywhere in this document, because
> none has been measured or agreed.

### Alerts

The expressions below follow the convention in
[dead-letter-runbook.md](dead-letter-runbook.md) — a plain PromQL condition plus
what it means operationally. They are **proposed**: derived from the metric
semantics above, not from an incident that actually fired.

- **Drain did not converge before its deadline** —
  `xflow_runner_drain_blockers{kind="deadline"} > 0` for >5m.
  At least one runner's drain deadline elapsed with blockers outstanding. The
  gate is still closed. Go to §9 (`timed_out` decision) and read that runner's
  projection to find which debt kind is holding it.
- **Runner acknowledgements are missing while drains are outstanding** —
  `xflow_runner_drain_blockers{kind="runner_ack"} > 0` for >5m while
  `xflow_runner_draining_count > 0`. The control plane has not received a
  quiet, generation-fenced observation from a draining runner. Likely causes are
  an unreachable or wedged runner process, or one that does not understand the
  directive. `drain_blockers{kind="runner_ack"}` also counts draining runners
  with no projection at all (`service/control/runner_control_metrics.go:69`–`72`).
- **Drains are being attempted but not transitioning** —
  `rate(xflow_runner_control_transitions_total{action="drain",result="unchanged"}[5m]) > 0`.
  The request was accepted but the state did not change — most often a repeated
  drain of an already-draining runner. Generally benign; a spike usually means
  an operator is retrying instead of reading the projection.
- **Drain requests are failing** —
  `rate(xflow_runner_control_transitions_total{action="drain",result=~"conflict|error|unsupported"}[5m]) > 0`.
  `conflict` means an `X-Request-Id` was reused with a different body;
  `unsupported` means the configured directory cannot perform runner control at
  all (the route answers 501 and this runbook does not apply); `error` is a
  durable-write failure. None of these mean the gate closed — verify with
  `GET /v1/management/runners/{id}` before assuming any drain took effect.

## 9. When the drain projects `timed_out`

`timed_out` is a real operational decision point, not a transient state that
resolves itself.

The deadline is finite and created by the ACTIVE→DRAINING transition. When it
elapses with work outstanding, the projection changes to `timed_out` but the
**desired state stays `draining` and the new-claim gate stays closed**; a
timed-out drain "never implicitly resumes, evicts, cancels, or reclaims work"
(`service/control/doc.go:69`–`73`,
`service/control/runner_control.go:36`–`39`). So a timeout leaves you with a
runner that admits no new work and still owns unsettled debt. That is a
deliberate fail-closed outcome, but it is not a resting state — you must choose:

1. **Read the blockers.** `GET /v1/management/runners/{id}` and inspect
   `unsettled_drain_debt`, `handoff_debt`, `lease_may_exist_debt`,
   `replayable_debt`, `pending_activation_cleanup`, and the two quiescence
   booleans. The split counters exist precisely to say whether the debt is
   reserved, uncertain, or already finalized
   (`service/control/runner_control.go:53`–`57`).
2. **If `server_quiescent` is false:** work is still live server-side. Let it
   settle — a later valid quiet observation can still change the projection to
   `complete` (`service/control/doc.go:71`–`73`). Do not stop the runner.
3. **If `server_quiescent` is true but `runner_quiescent` is false:** the
   server side is clean and the runner is not confirming. Check that the runner
   is alive and that its observations are arriving for the **current** session
   and control generation — a stale session or generation is fenced out and can
   never satisfy the predicate (`service/control/runner_control.go:274`–`286`).
4. **If debt is genuinely stuck** (the same non-zero blockers persist across
   repeated reads, and the owning work is unrecoverable), that is an incident
   with two acceptable endings: resolve or reclaim the debt through the normal
   control-plane path, or make an explicit decision to `resume` this runner
   knowing its unresolved debt returns with it (§7). **Escalate rather than
   improvise.** This runbook deliberately does not prescribe a reclaim recipe:
   no operator procedure for resolving individual handoff debt exists in this
   repository, and inventing one here would be fiction.

`timed_out` also does not mean the runner is dead, wedged, or safe to kill. It
means one thing: the deadline passed with blockers.

## 10. What is not verified

State this plainly to anyone who asks whether the procedure is proven.

- **No end-to-end drain-and-replace drill has ever been observed in this
  repository.** No incident record, drill log, or exercise artifact for this
  procedure exists (negative search: `grep -rni
  "drain[-_ ]drill|drain drill|排空演练|drain and replace" docs/ test/` matches
  nothing about runner drain; the only hits are two unrelated `drain and replace`
  comments about Go channels in a gitignored plan document).
- **This runbook is derived from source.** What *is* covered by tests is the
  mechanism, not the procedure: the drain state machine and projection
  (`service/control/runner_control_freshness_test.go`,
  `service/control/runner_control_protocol_test.go`,
  `service/control/runner_handoff_test.go`), the Redis control transitions
  (`service/control/redis_runner_control_test.go`), and the listing 501 contract
  (`service/apiserver/module_management_runners_test.go`). None of those drives
  a real runner process through replace-and-resume.
- **The alert expressions in [§8](#alerts) have never fired in production.**
  They are derived from the metrics' documented semantics, not from an incident.
- **The step 3 polling cadence is unspecified on purpose.** No measurement of
  drain convergence time exists in this repository, so no polling interval or
  expected duration is asserted. Poll on the order of your heartbeat interval
  (`--heartbeat-interval`, `sdk/runner/run.go:150`; default `5s`,
  `sdk/runner/config.go:104`) and watch the projection rather than a clock.
- **No owner and no review deadline.** RELEASE-GATES §6.1 now records this
  runbook as existing, but still defers owner/deadline assignment to decision
  **D6**, which remains `OPEN — 未批准` (`docs/design/RELEASE-GATES.md:252`,
  `:268`). Writing this file does not decide D6: it supplies the content so
  that only the owner/deadline decision remains.

## 11. Related material

- [maintenance-window-runbook.md](maintenance-window-runbook.md) — server
  graceful shutdown (§2.1), its at-least-once safety argument (§2.2), and
  post-upgrade verification (§3). **Server shutdown and runner drain are
  different mechanisms; do not substitute one for the other.**
- [DEPLOYMENT-TOPOLOGIES.md](../design/DEPLOYMENT-TOPOLOGIES.md) — runner
  topology, runner authentication posture, and the runner pre-flight path.
- [lease-metadata-upgrade.md](lease-metadata-upgrade.md) — relevant only when a
  release changes assignment-lease metadata. That upgrade requires a coordinated
  drain of runners and control-plane instances **before** the version switch and
  explicitly refuses to run as an ordinary rolling release with in-flight
  leases; use the drain procedure here as its admission-stop step, but note that
  it additionally requires confirming no active or recoverable old-version
  claim/lease remains (see that document's §3).
