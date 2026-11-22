package rstate

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

var _ engine.ConcurrencyOrphanReclaimer = (*Store)(nil)

// reclaimOrphanExecutionLua is the atomic re-check and convergence of one
// graph-lost execution. Every key it names shares the execution's {id} hash
// tag, so the judgement and the terminal write happen in one Redis cluster-safe
// step and cannot race a graph write, a lease acquisition, or a suspension.
//
// It refuses unless the execution is exactly the torn shape: a live status
// (running/queued), no graph, no unexpired node or group lease, and no
// suspended node. The age guard applies in sweep mode; force mode ignores the
// status clock and nothing else. A dry run takes the same decision and reports
// "would apply" without writing, which is what makes a preview and a reset agree.
//
// The age anchor is the newer of updated_at_ms (status transitions) and
// activity_at_ms (a node lease acquisition, renewal, or accepted commit).
// updated_at_ms alone would call a long execution idle whenever it sat between
// two leases, because ordinary node progress never moved that clock. An absent
// activity key falls back to updated_at_ms, which is every execution created
// before the key existed. Force mode still refuses an execution whose activity
// is inside the grace: recent activity is direct evidence a node is working,
// and an explicit reset opts out with include_live.
//
// include_live (ARGV[7]) is the operator-reset override: it lifts the graph,
// lease, suspension and activity guards so a live execution the operator
// explicitly named can be converged. It is honored in force mode only; the Go
// callers never set it for a sweep. The status guard and the terminal write are
// unchanged.
//
// KEYS[1] = execution status key
// KEYS[2] = execution graph key
// KEYS[3] = execution updated_at_ms key
// KEYS[4] = execution lease-expiry ZSET
// KEYS[5] = execution suspended_nodes SET
// KEYS[6] = execution error key
// KEYS[7] = execution activity_at_ms key
// ARGV[1] = now (unix milliseconds)
// ARGV[2] = grace (milliseconds)
// ARGV[3] = terminal reason (written to the error key)
// ARGV[4] = status TTL seconds
// ARGV[5] = 1 force mode / 0 sweep mode
// ARGV[6] = 1 dry run / 0 apply
// ARGV[7] = 1 include live executions / 0 orphans only
// Returns {outcome, previous status, age milliseconds}, where outcome is
// 0 (not an orphan, or a guard refused), 1 (converged), or 2 (dry run: would
// converge).
var reclaimOrphanExecutionLua = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if not current then
    return {0, '', 0}
end
if current ~= 'running' and current ~= 'queued' then
    return {0, current, 0}
end
local includeLive = tonumber(ARGV[7]) or 0
if includeLive ~= 1 and redis.call('EXISTS', KEYS[2]) == 1 then
    return {0, current, 0}
end
local now = tonumber(ARGV[1])
local updated = tonumber(redis.call('GET', KEYS[3]) or '0')
local activity = tonumber(redis.call('GET', KEYS[7]) or '0')
-- The age anchor is the newer clock. updated_at_ms only moves on status
-- transitions; activity_at_ms moves on node lease acquisition/renewal and every
-- accepted node commit. A missing activity key is an old execution: it falls
-- back to updated_at_ms. Both missing means the age is unknown, which no guard
-- may treat as old enough.
local anchor = updated
if activity > anchor then
    anchor = activity
end
local age = 0
if anchor > 0 then
    age = now - anchor
end
if tonumber(ARGV[5]) ~= 1 then
    if anchor <= 0 or age < tonumber(ARGV[2]) then
        return {0, current, age}
    end
elseif includeLive ~= 1 and activity > 0 and (now - activity) < tonumber(ARGV[2]) then
    -- Force skips the status clock, never direct evidence of work: Cancel's
    -- self-heal must not converge an execution whose node was leased or
    -- committed inside the grace. The explicit operator reset uses include_live
    -- to lift exactly this guard.
    return {0, current, age}
end
if includeLive ~= 1 then
    -- An unexpired node or group lease is work that is visibly running. It is
    -- not an orphan whatever the routing state says.
    if redis.call('ZCOUNT', KEYS[4], ARGV[1], '+inf') > 0 then
        return {0, current, age}
    end
    -- A suspended node is a signal or timer wait: the execution is parked on
    -- purpose, and its graph absence is a routing fault to investigate, not work
    -- to terminate behind the owner's back.
    if redis.call('SCARD', KEYS[5]) > 0 then
        return {0, current, age}
    end
end
if tonumber(ARGV[6]) == 1 then
    return {2, current, age}
end
local ttl = tonumber(ARGV[4])
redis.call('SET', KEYS[1], 'canceled', 'EX', ttl)
if ARGV[3] ~= '' then
    redis.call('SET', KEYS[6], ARGV[3], 'EX', ttl)
end
redis.call('SET', KEYS[3], now, 'EX', ttl)
return {1, current, age}
`)

// concurrencyMember is one execution named by a workflow's concurrency state.
type concurrencyMember struct {
	id        types.ExecutionID
	inSlot    bool
	inQueue   bool
	memberAge time.Duration
	// ageKnown is false when neither the slot score nor the arrival score
	// could date the member. A sweep refuses to reclaim on an unknown age.
	ageKnown bool
}

// executionProbe is the authoritative read the orphan judgement starts from.
type executionProbe struct {
	status      types.ExecutionStatus
	hasStatus   bool
	updatedMs   int64
	activityMs  int64
	graphExists bool
}

// ageAnchorMs is the newest clock the age guard may use: the status transition
// clock or the node activity clock, whichever is later. Zero means the age is
// unknown, which never qualifies as "old enough".
func (p executionProbe) ageAnchorMs() int64 {
	if p.activityMs > p.updatedMs {
		return p.activityMs
	}
	return p.updatedMs
}

// orphanTagResult is one workflow tag's contribution to a sweep.
type orphanTagResult struct {
	inspected  int
	reclaimed  int
	candidates []engine.OrphanReclaimCandidate
}

// ReclaimConcurrencyOrphans implements engine.ConcurrencyOrphanReclaimer.
//
// A zero ExecutionID sweeps every workflow's slot and queue members in the
// store's scopes, which is what the control plane's concurrency maintenance
// calls. A non-zero ExecutionID converges just that execution, which is what
// Cancel and the reset API call. See the interface documentation for the
// judgement and convergence contract.
func (s *Store) ReclaimConcurrencyOrphans(ctx context.Context, req engine.OrphanReclaimRequest) (engine.OrphanReclaimResult, error) {
	mode := req.Mode
	if mode == "" {
		mode = engine.OrphanReclaimSweep
	}
	grace := req.Grace
	if grace <= 0 {
		grace = s.concurrencyOrphanGrace
	}
	if grace <= 0 {
		grace = engine.DefaultOrphanReclaimGrace
	}
	limit := req.Limit
	if limit <= 0 {
		limit = defaultConcurrencySweepBatch
	}

	if req.ExecutionID != "" {
		return s.reclaimTargetedOrphan(ctx, req, mode, grace)
	}

	s.concurrencySweepMu.Lock()
	defer s.concurrencySweepMu.Unlock()

	namespaces, err := s.orphanScanNamespaces(ctx, req.Namespace)
	if err != nil {
		return engine.OrphanReclaimResult{}, err
	}
	reason := req.Trigger()
	var out engine.OrphanReclaimResult
	for _, t := range namespaces {
		if out.Inspected >= limit {
			break
		}
		var tags []string
		if req.Workflow != "" {
			// A workflow-scoped pass addresses one tag directly: the reset
			// surface names a workflow, and going through the index would make
			// a workflow that never charged a slot invisible even when it has
			// members (the index is registered on first charge, which can lag).
			tags = []string{workflowDigestTag(t, req.Workflow)}
		} else {
			tags, err = s.rdb.SMembers(ctx, concurrencyWorkflowIndexKey(t)).Result()
			if err != nil && err != redis.Nil {
				return out, fmt.Errorf("read concurrency workflow index for namespace %q: %w", t, err)
			}
		}
		for _, tag := range tags {
			if out.Inspected >= limit {
				break
			}
			res := s.reclaimOrphansForTag(ctx, t, tag, mode, grace, req.DryRun, req.IncludeLive, reason, limit-out.Inspected)
			out.Inspected += res.inspected
			out.Reclaimed += res.reclaimed
			out.Candidates = append(out.Candidates, res.candidates...)
		}
	}
	return out, nil
}

// orphanScanNamespaces resolves which namespaces a sweep covers. An explicit
// request namespace narrows the pass to it; otherwise the store's owned set (or
// the namespace registry) is used, exactly like every other maintenance sweep.
func (s *Store) orphanScanNamespaces(ctx context.Context, only namespace.Namespace) ([]namespace.Namespace, error) {
	if only != "" {
		return []namespace.Namespace{only}, nil
	}
	return s.scopedNamespaces(ctx)
}

// reclaimTargetedOrphan evaluates one named execution, whether or not it still
// holds a concurrency member. That is deliberate: Cancel may be the only
// operation ever issued against a graph-lost execution of an unlimited
// workflow, which holds no slot at all.
func (s *Store) reclaimTargetedOrphan(ctx context.Context, req engine.OrphanReclaimRequest, mode engine.OrphanReclaimMode, grace time.Duration) (engine.OrphanReclaimResult, error) {
	t := req.Namespace
	if t == "" {
		t = namespace.FromContext(ctx)
	}
	id := req.ExecutionID
	probe, err := s.probeExecution(ctx, t, id)
	if err != nil {
		return engine.OrphanReclaimResult{}, err
	}
	tag, err := s.workflowTagFor(ctx, id, t)
	if err != nil {
		return engine.OrphanReclaimResult{}, err
	}
	member := s.concurrencyMembership(ctx, t, tag, id)
	res := engine.OrphanReclaimResult{Inspected: 1}
	candidate, reclaimed := s.evaluateOrphan(ctx, t, tag, id, probe, member, mode, grace, req.DryRun, req.IncludeLive, req.Trigger())
	if candidate != nil {
		res.Candidates = append(res.Candidates, *candidate)
	}
	if reclaimed {
		res.Reclaimed = 1
	}
	return res, nil
}

// reclaimOrphansForTag sweeps one workflow's slot and queue members. It returns
// what it inspected, reclaimed and judged.
func (s *Store) reclaimOrphansForTag(ctx context.Context, t namespace.Namespace, tag string, mode engine.OrphanReclaimMode, grace time.Duration, dryRun, includeLive bool, reason string, limit int) orphanTagResult {
	if limit <= 0 {
		return orphanTagResult{}
	}
	var out orphanTagResult

	slotMembers, err := s.rdb.ZRangeWithScores(ctx, concurrencySlotKeyForTag(t, tag), 0, int64(limit-1)).Result()
	if err != nil && err != redis.Nil {
		s.logConcurrencySweepError(t, tag, "read slot members", err)
		return out
	}
	queueMembers, err := s.rdb.ZRange(ctx, concurrencyQueueKeyForTag(t, tag), 0, int64(limit-1)).Result()
	if err != nil && err != redis.Nil {
		s.logConcurrencySweepError(t, tag, "read queue members", err)
		return out
	}
	members := make(map[string]*concurrencyMember, len(slotMembers)+len(queueMembers))
	// The line is judged before the slots that feed it. Converging a queued
	// orphan removes only its place in line; converging a holder frees a slot and
	// refills it, and a refill promoting a member that has not been judged yet
	// would reclaim it through the promotion path instead — correct, but then the
	// pass could not report it. Queue-first keeps one pass's accounting complete.
	order := make([]string, 0, len(slotMembers)+len(queueMembers))
	now := time.Now().UTC()
	for _, raw := range queueMembers {
		member := &concurrencyMember{id: types.ExecutionID(raw), inQueue: true}
		members[raw] = member
		order = append(order, raw)
	}
	for _, z := range slotMembers {
		raw := fmt.Sprint(z.Member)
		member := members[raw]
		if member == nil {
			member = &concurrencyMember{id: types.ExecutionID(raw)}
			members[raw] = member
			order = append(order, raw)
		}
		member.inSlot = true
		if z.Score > 0 {
			member.memberAge = now.Sub(time.Unix(int64(z.Score), 0))
			member.ageKnown = true
		}
	}
	for _, raw := range queueMembers {
		member := members[raw]
		if member.ageKnown {
			continue
		}
		if score, err := s.rdb.ZScore(ctx, concurrencyQueueArrivalKeyForTag(t, tag), raw).Result(); err == nil {
			member.memberAge = now.Sub(time.Unix(int64(score), 0))
			member.ageKnown = true
		}
	}

	for _, raw := range order {
		member := members[raw]
		if out.inspected >= limit {
			break
		}
		out.inspected++
		probe, err := s.probeExecution(ctx, t, member.id)
		if err != nil {
			s.logConcurrencySweepError(t, tag, "probe execution "+string(member.id), err)
			continue
		}
		candidate, reclaimed := s.evaluateOrphan(ctx, t, tag, member.id, probe, member, mode, grace, dryRun, includeLive, reason)
		if candidate != nil {
			out.candidates = append(out.candidates, *candidate)
		}
		if reclaimed {
			out.reclaimed++
		}
	}
	return out
}

// probeExecution reads the authoritative facts an orphan judgement needs in one
// round trip: status, graph presence, and both age clocks.
func (s *Store) probeExecution(ctx context.Context, t namespace.Namespace, id types.ExecutionID) (executionProbe, error) {
	pipe := s.rdb.Pipeline()
	statusCmd := pipe.Get(ctx, execKey(t, id, "status"))
	updatedCmd := pipe.Get(ctx, execKey(t, id, "updated_at_ms"))
	activityCmd := pipe.Get(ctx, activityAtKey(t, id))
	graphCmd := pipe.Exists(ctx, execKey(t, id, "graph"))
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return executionProbe{}, fmt.Errorf("probe orphan execution %q: %w", id, err)
	}
	p := executionProbe{}
	if raw, err := statusCmd.Result(); err == nil {
		p.status = types.ExecutionStatus(raw)
		p.hasStatus = true
	} else if err != redis.Nil {
		return executionProbe{}, fmt.Errorf("probe status for %q: %w", id, err)
	}
	if raw, err := updatedCmd.Int64(); err == nil {
		p.updatedMs = raw
	} else if err != redis.Nil {
		return executionProbe{}, fmt.Errorf("probe updated_at for %q: %w", id, err)
	}
	if raw, err := activityCmd.Int64(); err == nil {
		p.activityMs = raw
	} else if err != redis.Nil {
		return executionProbe{}, fmt.Errorf("probe activity_at for %q: %w", id, err)
	}
	if n, err := graphCmd.Result(); err == nil {
		p.graphExists = n > 0
	} else {
		return executionProbe{}, fmt.Errorf("probe graph for %q: %w", id, err)
	}
	return p, nil
}

// concurrencyMembership reports where (if anywhere) one execution stands in its
// workflow's concurrency state, and how long it has been there.
func (s *Store) concurrencyMembership(ctx context.Context, t namespace.Namespace, tag string, id types.ExecutionID) *concurrencyMember {
	member := &concurrencyMember{id: id}
	if tag == "" {
		return member
	}
	raw := concurrencySlotMember(id)
	now := time.Now().UTC()
	if score, err := s.rdb.ZScore(ctx, concurrencySlotKeyForTag(t, tag), raw).Result(); err == nil {
		member.inSlot = true
		member.memberAge = now.Sub(time.Unix(int64(score), 0))
		member.ageKnown = true
	}
	if _, err := s.rdb.ZScore(ctx, concurrencyQueueKeyForTag(t, tag), raw).Result(); err == nil {
		member.inQueue = true
		if !member.ageKnown {
			if arrived, err := s.rdb.ZScore(ctx, concurrencyQueueArrivalKeyForTag(t, tag), raw).Result(); err == nil {
				member.memberAge = now.Sub(time.Unix(int64(arrived), 0))
				member.ageKnown = true
			}
		}
	}
	return member
}

// logConcurrencySweepError reports a per-workflow read failure without failing
// the whole sweep: one unreadable workflow must not stop the others.
func (s *Store) logConcurrencySweepError(t namespace.Namespace, tag, operation string, err error) {
	if s.logger == nil {
		return
	}
	s.logger.Error("concurrency_orphan_sweep_failed", "namespace", string(t), "workflow_tag", tag, "operation", operation, "err", err)
}

// luaReplyString coerces a Lua reply element into a string, tolerating the
// encodings the client may surface for an empty bulk string.
func luaReplyString(reply []any, i int) string {
	if i < 0 || i >= len(reply) {
		return ""
	}
	switch typed := reply[i].(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return ""
	}
}

// hasMember reports whether the execution occupies a slot or a place in line.
func (m *concurrencyMember) hasMember() bool {
	return m != nil && (m.inSlot || m.inQueue)
}

// evaluateOrphan is the single judgement both the sweep and the targeted
// (Cancel / reset) callers use. It is split from the scan so the two paths
// cannot drift: whatever the sweep refuses, Cancel refuses too, with force mode
// as the only difference.
func (s *Store) evaluateOrphan(ctx context.Context, t namespace.Namespace, tag string, id types.ExecutionID, probe executionProbe, member *concurrencyMember, mode engine.OrphanReclaimMode, grace time.Duration, dryRun, includeLive bool, reason string) (*engine.OrphanReclaimCandidate, bool) {
	if probe.hasStatus {
		return s.evaluateLiveOrphan(ctx, t, tag, id, probe, member, mode, grace, dryRun, includeLive, reason)
	}
	return s.evaluateMissingOrphan(ctx, t, tag, id, member, mode, grace, dryRun, reason)
}

// evaluateLiveOrphan handles the torn shape proper: the status key says live,
// the graph key is absent. Everything else — a live graph unless includeLive,
// a terminal status, a parked status — is not this shape and is left alone.
func (s *Store) evaluateLiveOrphan(ctx context.Context, t namespace.Namespace, tag string, id types.ExecutionID, probe executionProbe, member *concurrencyMember, mode engine.OrphanReclaimMode, grace time.Duration, dryRun, includeLive bool, reason string) (*engine.OrphanReclaimCandidate, bool) {
	if types.IsTerminalExecutionStatus(probe.status) {
		// A terminal execution is never an orphan, and its concurrency release
		// belongs to the terminal writer. Removing its membership here would race
		// that release: whichever ran second would see no slot and would not
		// promote, leaving the freed capacity idle. A genuinely leaked membership
		// is the periodic sweep's job, which removes it and refills in one pass.
		return nil, false
	}
	if probe.status != types.ExecutionStatusRunning && probe.status != types.ExecutionStatusQueued {
		return nil, false
	}
	if probe.graphExists && !includeLive {
		return nil, false
	}
	if mode != engine.OrphanReclaimForce {
		// A cheap pre-filter; the Lua script repeats the check atomically. The
		// guard's clock is the newer of the status transition and node activity,
		// so an execution that committed a node (or renewed a lease) inside the
		// grace is not old just because its status has not moved. An unknown age
		// is not proof of age, so it never qualifies.
		anchorMs := probe.ageAnchorMs()
		if anchorMs <= 0 || time.Since(time.UnixMilli(anchorMs)) < grace {
			return nil, false
		}
	}
	force := int64(0)
	if mode == engine.OrphanReclaimForce {
		force = 1
	}
	dry := int64(0)
	if dryRun {
		dry = 1
	}
	live := int64(0)
	if includeLive {
		live = 1
	}
	// An include-live convergence terminates work the operator explicitly
	// named, so its durable reason is the audit label operator_reset rather
	// than the automatic-repair label.
	terminalReason := engine.OrphanReclaimReason
	if includeLive {
		terminalReason = engine.OrphanReclaimReasonOperatorReset
	}
	reply, err := reclaimOrphanExecutionLua.Run(ctx, s.rdb,
		[]string{
			execKey(t, id, "status"),
			execKey(t, id, "graph"),
			execKey(t, id, "updated_at_ms"),
			leaseExpiryZSetKey(t, id),
			suspendedNodesKey(t, id),
			execKey(t, id, "error"),
			activityAtKey(t, id),
		},
		time.Now().UTC().UnixMilli(), grace.Milliseconds(), terminalReason,
		int(s.getExecTTL(ctx, id).Seconds()), force, dry, live,
	).Slice()
	if err != nil && err != redis.Nil {
		return nil, false
	}
	outcome := luaReplyInt(reply, 0)
	if outcome != 1 && outcome != 2 {
		return nil, false
	}
	previous := types.ExecutionStatus(luaReplyString(reply, 1))
	if previous == "" {
		previous = probe.status
	}
	age := time.Duration(luaReplyInt(reply, 2)) * time.Millisecond
	candidate := &engine.OrphanReclaimCandidate{
		ExecutionID: id,
		Namespace:   t,
		Workflow:    s.executionWorkflowID(ctx, t, id),
		Status:      previous,
		Age:         age,
		Evidence:    engine.OrphanEvidenceGraphMissing,
	}
	if outcome == 2 || dryRun {
		return candidate, false
	}
	s.observeOrphanReclaimed(ctx, candidate, reason)
	s.convergeLiveOrphan(ctx, t, tag, id, candidate, member, reason, terminalReason)
	return candidate, true
}

// evaluateMissingOrphan handles the shape the status key cannot describe: a
// slot or line member whose execution is entirely gone. Only the member's own
// age can date it, and an undatable member is left for the next sweep rather
// than guessed at.
func (s *Store) evaluateMissingOrphan(ctx context.Context, t namespace.Namespace, tag string, id types.ExecutionID, member *concurrencyMember, mode engine.OrphanReclaimMode, grace time.Duration, dryRun bool, reason string) (*engine.OrphanReclaimCandidate, bool) {
	if !member.hasMember() || tag == "" {
		return nil, false
	}
	if mode != engine.OrphanReclaimForce {
		if !member.ageKnown || member.memberAge < grace {
			return nil, false
		}
	}
	candidate := &engine.OrphanReclaimCandidate{
		ExecutionID: id,
		Namespace:   t,
		Workflow:    s.executionWorkflowID(ctx, t, id),
		Age:         member.memberAge,
		Evidence:    engine.OrphanEvidenceExecutionMissing,
	}
	if dryRun {
		return candidate, false
	}
	s.dropOrphanMember(ctx, t, tag, id, member, false, true)
	s.projectMissingExecutionOrphan(ctx, t, id)
	s.observeOrphanReclaimed(ctx, candidate, reason)
	s.logOrphanReclaimed(candidate, reason)
	return candidate, true
}

// convergeLiveOrphan performs the Go-side half of a reclaim whose Redis half
// is already durable: release the concurrency membership, retire every index
// that could still drive work, mirror the terminal status, and announce it.
//
// Each step is best-effort for the same reason every other terminal writer's
// side effects are: the status write is the authoritative part and it has
// landed. What must not be best-effort is the reporting — every failure is
// logged, because a silently lost release or projection is the bug this whole
// path exists to repair.
func (s *Store) convergeLiveOrphan(ctx context.Context, t namespace.Namespace, tag string, id types.ExecutionID, candidate *engine.OrphanReclaimCandidate, member *concurrencyMember, reason, terminalReason string) {
	if tag != "" && member.hasMember() {
		if err := s.releaseWorkflowSlot(ctx, id, t, tag); err != nil && s.logger != nil {
			s.logger.Error("orphan_slot_release_failed", "namespace", string(t), "execution_id", string(id), "err", err)
		}
	}
	s.convergeLiveOrphanEffects(ctx, t, id, candidate, reason, terminalReason)
}

// convergeLiveOrphanEffects is the side-effect half of a live-status reclaim,
// shared by the sweep (which has already released the membership) and the
// promotion path (which removes the slot itself and lets its caller refill).
func (s *Store) convergeLiveOrphanEffects(ctx context.Context, t namespace.Namespace, id types.ExecutionID, candidate *engine.OrphanReclaimCandidate, reason, terminalReason string) {
	nctx := namespace.WithNamespace(ctx, t)
	s.cleanupOnCancel(nctx, id)
	// Marker before cache eviction, exactly as UpdateExecutionStatus orders
	// them: the marker's TTL resolution reads the transient decision.
	s.shortenTransientCompletionTTLBestEffort(nctx, id,
		execKey(t, id, "status"), execKey(t, id, "error"))
	s.markExecutionTerminalBestEffort(nctx, id, types.ExecutionStatusCanceled)
	s.evictExecutionCaches(id)
	s.withdrawExecutionDeadline(nctx, id, types.ExecutionStatusCanceled)
	s.clearExecutionCanceling(nctx, t, id)
	s.projectExecutionStatus(nctx, id, types.ExecutionStatusCanceled, terminalReason)
	_ = s.PublishExecutionEvent(nctx, engine.ExecutionEvent{
		ExecutionID: id,
		Status:      types.ExecutionStatusCanceled,
		Error:       terminalReason,
	})
	s.logOrphanReclaimed(candidate, reason)
}

// dropOrphanMember removes an execution from its workflow's concurrency state
// and hands the freed capacity to the head of the line. It is the missing-status
// path's counterpart to releaseWorkflowSlot, which needs an execution status to
// drive. whenFreeSlot says whether the removal frees capacity (a slot membership
// does; a place in line does not).
func (s *Store) dropOrphanMember(ctx context.Context, t namespace.Namespace, tag string, id types.ExecutionID, member *concurrencyMember, dryRun, whenFreeSlot bool) {
	if tag == "" || !member.hasMember() || dryRun {
		return
	}
	raw := concurrencySlotMember(id)
	pipe := s.rdb.Pipeline()
	pipe.ZRem(ctx, concurrencySlotKeyForTag(t, tag), raw)
	pipe.ZRem(ctx, concurrencyQueueKeyForTag(t, tag), raw)
	pipe.ZRem(ctx, concurrencyDeadlineKeyForTag(t, tag), raw)
	pipe.ZRem(ctx, concurrencyQueueArrivalKeyForTag(t, tag), raw)
	_, _ = pipe.Exec(ctx)
	if !member.inSlot || !whenFreeSlot {
		return
	}
	if err := s.fillConcurrencySlots(ctx, t, tag); err != nil && s.logger != nil {
		s.logger.Error("orphan_slot_fill_failed", "namespace", string(t), "workflow_tag", tag, "execution_id", string(id), "err", err)
	}
}

// executionWorkflowID reads the execution's immutable definition identity for
// the reclaim report. Best effort: an execution whose identity keys are already
// gone is reported with an empty workflow rather than a guessed one.
func (s *Store) executionWorkflowID(ctx context.Context, t namespace.Namespace, id types.ExecutionID) string {
	raw, err := s.rdb.Get(ctx, execKey(t, id, "workflow_id")).Result()
	if err != nil {
		return ""
	}
	return raw
}

// logOrphanReclaimed emits the one line an operator needs to tell a deliberate
// repair from a vanished execution: which execution, which workflow, what it
// looked like before, how old that evidence was, which torn shape it was, and
// which path converged it.
func (s *Store) logOrphanReclaimed(candidate *engine.OrphanReclaimCandidate, reason string) {
	if s.logger == nil || candidate == nil {
		return
	}
	s.logger.Info("execution_orphan_reclaimed",
		"namespace", string(candidate.Namespace),
		"execution_id", string(candidate.ExecutionID),
		"workflow", candidate.Workflow,
		"original_status", string(candidate.Status),
		"age_ms", candidate.Age.Milliseconds(),
		"evidence", candidate.Evidence,
		"reason", reason,
	)
}

// observeOrphanReclaimed reports one convergence to the optional observer. It
// is published only for applied transitions; a dry run reports candidates in
// the result and counts nothing, so a preview can never move the metric.
func (s *Store) observeOrphanReclaimed(ctx context.Context, candidate *engine.OrphanReclaimCandidate, reason string) {
	if s.orphanObserver == nil || candidate == nil {
		return
	}
	defer func() { _ = recover() }()
	s.orphanObserver.OnConcurrencyOrphanReclaimed(
		namespace.WithNamespace(ctx, candidate.Namespace),
		candidate.Kind(), reason,
	)
}

// projectMissingExecutionOrphan converges the SQL mirror for an execution
// whose Redis status key is already gone. Redis has nothing left to terminate,
// so this is a direct projection: the drift repair skips this shape as
// not_found, which is exactly the blind spot this write closes.
func (s *Store) projectMissingExecutionOrphan(ctx context.Context, t namespace.Namespace, id types.ExecutionID) {
	if s.db == nil {
		return
	}
	nctx := namespace.WithNamespace(ctx, t)
	if s.isTransient(nctx, id) {
		return
	}
	err := s.db.UpdateExecutionStatus(nctx, id, types.ExecutionStatusCanceled, engine.OrphanReclaimReason)
	if err != nil && !errors.Is(err, store.ErrNotFound) && s.logger != nil {
		s.logger.Error("orphan_execution_sql_projection_failed", "namespace", string(t), "execution_id", string(id), "err", err)
	}
}
