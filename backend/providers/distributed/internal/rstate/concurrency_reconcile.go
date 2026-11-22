package rstate

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

const defaultConcurrencySweepBatch = 256

// concurrencyStaleGrace is how long a slot may name an execution that does not
// exist before the sweep treats it as leaked.
//
// It has to exceed the window between charging a slot and persisting the
// execution, because that window is the NORMAL state of a slot mid-submission:
// a sweep that removed members the moment their execution key was missing would
// release live slots under load. Anything on the order of the create round trip
// is enough; the sweep runs on its own (slower) cadence, so this is a
// comfortably generous margin rather than a tuned value.
const concurrencyStaleGrace = 2 * time.Minute

// ReconcileConcurrencySlots sweeps workflow concurrency slot sets for members
// that no longer belong to a live execution.
//
// It exists because admission is not atomic with execution creation, and
// neither is a terminal transition with the slot release: Redis Cluster puts
// the slot set and the execution keys in different slots, so no single Lua
// script can span them. Two leaks follow from that split, and both are
// invisible from the engine:
//
//   - a process that dies between charging a slot and writing the execution
//     leaves a member naming an execution that does not exist;
//   - a terminal transition whose release was lost leaves a member naming an
//     execution that has already finished.
//
// Either way the workflow silently runs one short of its limit until something
// removes the member. This is that something.
//
// The sweep is bounded by limit and safe to run concurrently from several
// processes: removing a member is idempotent, and nothing here can remove a
// slot that a live execution holds.
func (s *Store) ReconcileConcurrencySlots(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = defaultConcurrencySweepBatch
	}

	s.concurrencySweepMu.Lock()
	defer s.concurrencySweepMu.Unlock()

	namespaces, err := s.scopedNamespaces(ctx)
	if err != nil {
		return 0, fmt.Errorf("list namespaces for concurrency sweep: %w", err)
	}
	swept := 0
	for _, t := range namespaces {
		if swept >= limit {
			break
		}
		n, err := s.reconcileConcurrencySlotsForNamespace(ctx, t, limit-swept)
		if err != nil {
			return swept, err
		}
		swept += n
	}
	return swept, nil
}

func (s *Store) reconcileConcurrencySlotsForNamespace(ctx context.Context, t namespace.Namespace, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	tags, err := s.rdb.SMembers(ctx, concurrencyWorkflowIndexKey(t)).Result()
	if err != nil && err != redis.Nil {
		return 0, fmt.Errorf("read concurrency workflow index for namespace %q: %w", t, err)
	}
	swept := 0
	for _, tag := range tags {
		if swept >= limit {
			break
		}
		n, err := s.sweepConcurrencySlotSet(ctx, t, tag, limit-swept)
		if err != nil {
			return swept, err
		}
		swept += n
	}
	return swept, nil
}

// sweepConcurrencySlotSet inspects one workflow's slot set AND its line, and
// repairs everything that can no longer start: members whose execution is gone,
// members whose execution already finished, promotions a crash left half-done,
// and — the case this pass exists for — executions whose status is still live
// but whose graph is gone. The last shape is invisible to every status-based
// repair, because the status itself is the thing that lies.
func (s *Store) sweepConcurrencySlotSet(ctx context.Context, t namespace.Namespace, tag string, limit int) (int, error) {
	slotKey := concurrencySlotKeyForTag(t, tag)
	queueKey := concurrencyQueueKeyForTag(t, tag)
	arrivalKey := concurrencyQueueArrivalKeyForTag(t, tag)
	slotMembers, err := s.rdb.ZRangeWithScores(ctx, slotKey, 0, int64(limit-1)).Result()
	if err != nil && err != redis.Nil {
		return 0, fmt.Errorf("read concurrency slots %q: %w", slotKey, err)
	}
	queueMembers, err := s.rdb.ZRange(ctx, queueKey, 0, int64(limit-1)).Result()
	if err != nil && err != redis.Nil {
		return 0, fmt.Errorf("read concurrency queue %q: %w", queueKey, err)
	}
	if len(slotMembers) == 0 && len(queueMembers) == 0 {
		// A tag whose set never materialized — a refused admission, or a crash
		// between registering the tag and charging the slot — or whose set was
		// emptied without its index entry being dropped. Either way the tag now
		// points at nothing, and dropping it is what keeps the index
		// proportional to the workflows that hold slots.
		//
		// The check is not atomic with the index removal: admission registers
		// its tag before charging, so a submission landing between these two
		// reads can have its tag removed. The failure mode is bounded and
		// self-healing — that submission's slot is swept once the workflow is
		// written to again, and every later admission or release registers the
		// tag anew.
		if exists, err := s.rdb.Exists(ctx, slotKey, queueKey).Result(); err == nil && exists == 0 {
			s.dropConcurrencyIndexIfIdle(ctx, t, tag)
		}
		return 0, s.fillConcurrencySlots(ctx, t, tag)
	}

	// A member can be in both sets (a queued execution is promoted by moving it
	// from the line into the slot), so the union is keyed by execution ID. The
	// line is ordered first: a holder that converges frees a slot and refills it,
	// and a refill would judge a not-yet-visited line member through the promotion
	// path — correct, but invisible to this pass's repair count.
	now := time.Now().UTC()
	members := make(map[string]*concurrencyMember, len(slotMembers)+len(queueMembers))
	order := make([]string, 0, len(slotMembers)+len(queueMembers))
	for _, raw := range queueMembers {
		members[raw] = &concurrencyMember{id: types.ExecutionID(raw), inQueue: true}
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

	// One pipelined read for every member's status, graph presence, both age
	// clocks, and — when the slot score cannot date it — its arrival clock: the
	// sets are bounded by the workflow's limit and the line, and the sweep runs
	// on every namespace on a maintenance cadence.
	pipe := s.rdb.Pipeline()
	statuses := make(map[string]*redis.StringCmd, len(order))
	updated := make(map[string]*redis.StringCmd, len(order))
	activities := make(map[string]*redis.StringCmd, len(order))
	graphs := make(map[string]*redis.IntCmd, len(order))
	arrivals := make(map[string]*redis.FloatCmd, len(order))
	for _, raw := range order {
		id := types.ExecutionID(raw)
		statuses[raw] = pipe.Get(ctx, execKey(t, id, "status"))
		updated[raw] = pipe.Get(ctx, execKey(t, id, "updated_at_ms"))
		activities[raw] = pipe.Get(ctx, activityAtKey(t, id))
		graphs[raw] = pipe.Exists(ctx, execKey(t, id, "graph"))
		if member := members[raw]; member.inQueue && !member.ageKnown {
			arrivals[raw] = pipe.ZScore(ctx, arrivalKey, raw)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return 0, fmt.Errorf("read concurrency members for %q: %w", slotKey, err)
	}

	type staleRemoval struct {
		member *concurrencyMember
		cmds   []*redis.IntCmd
	}
	stale := make([]staleRemoval, 0, len(order))
	stranded := make([]string, 0, len(order))
	reclaimed := 0
	for _, raw := range order {
		member := members[raw]
		id := member.id
		if !member.ageKnown {
			if cmd, ok := arrivals[raw]; ok {
				if score, err := cmd.Result(); err == nil {
					member.memberAge = now.Sub(time.Unix(int64(score), 0))
					member.ageKnown = true
				}
			}
		}
		rawStatus, statusErr := statuses[raw].Result()
		hasStatus := statusErr == nil
		if statusErr != nil && statusErr != redis.Nil {
			return reclaimed, fmt.Errorf("read execution status for %q: %w", id, statusErr)
		}
		graphExists := false
		if n, err := graphs[raw].Result(); err == nil {
			graphExists = n > 0
		} else {
			return reclaimed, fmt.Errorf("read execution graph presence for %q: %w", id, err)
		}
		var updatedMs int64
		if rawUpdated, err := updated[raw].Int64(); err == nil {
			updatedMs = rawUpdated
		} else if err != redis.Nil {
			return reclaimed, fmt.Errorf("read execution updated_at for %q: %w", id, err)
		}
		var activityMs int64
		if rawActivity, err := activities[raw].Int64(); err == nil {
			activityMs = rawActivity
		} else if err != redis.Nil {
			return reclaimed, fmt.Errorf("read execution activity_at for %q: %w", id, err)
		}

		if !hasStatus {
			// The execution key is gone. Either it was never written (a crash
			// between admission and create) or its retention lapsed, which means
			// the member cannot belong to anything live either way — but only
			// once the create window has certainly passed. A line member is dated
			// by its arrival clock, a slot member by its slot score.
			if !member.ageKnown && member.inQueue {
				// A member admitted before the arrival index existed has only the
				// line's sequence score, which is not a time, and with its
				// execution keys gone there is no status clock either — so it
				// could never be dated and used to be skipped forever. Date it at
				// first sighting instead and let the ordinary grace below decide
				// on a later pass: stamping "now" can only delay a reclaim, never
				// shorten one, so an in-flight create stays protected.
				s.stampMissingQueueArrival(ctx, t, tag, raw)
				continue
			}
			if !member.ageKnown || member.memberAge < concurrencyStaleGrace {
				continue
			}
			s.projectMissingExecutionOrphan(ctx, t, id)
			candidate := &engine.OrphanReclaimCandidate{
				ExecutionID: id, Namespace: t, Workflow: s.executionWorkflowID(ctx, t, id),
				Age: member.memberAge, Evidence: engine.OrphanEvidenceExecutionMissing,
			}
			s.observeOrphanReclaimed(ctx, candidate, engine.OrphanTriggerSweep)
			s.logOrphanReclaimed(candidate, engine.OrphanTriggerSweep)
			stale = append(stale, staleRemoval{member: member})
			// Not counted as reclaimed: this member is in stale, and the removal
			// count below is what reports it. Counting both would double every
			// retention-lapsed member in the pass's released total.
			continue
		}
		status := types.ExecutionStatus(rawStatus)
		if types.IsTerminalExecutionStatus(status) {
			stale = append(stale, staleRemoval{member: member})
			continue
		}
		if !graphExists {
			if status != types.ExecutionStatusRunning && status != types.ExecutionStatusQueued {
				continue
			}
			// The torn shape the status-based repairs cannot see: live status,
			// absent graph. A forced partial read would repeat the very blindness
			// being repaired, so the judgement is the atomic Lua script, which
			// also refuses a live lease, a suspension, and an execution whose
			// newer age clock — status transition or node activity — is inside
			// the grace.
			probe := executionProbe{status: status, hasStatus: true, updatedMs: updatedMs, activityMs: activityMs, graphExists: false}
			_, ok := s.evaluateLiveOrphan(ctx, t, tag, id, probe, member, engine.OrphanReclaimSweep, s.concurrencyOrphanGrace, false, false, engine.OrphanTriggerSweep)
			if ok {
				reclaimed++
			}
			continue
		}
		if member.inSlot && status == types.ExecutionStatusQueued {
			// A queued execution holding a slot is a promotion that was decided
			// — the slot was handed to it — and never finished: its status and
			// its intents are written in its own hash slot, which the release
			// script cannot reach. Removing the member would strand it; not
			// acting would leave a slot held by work that is not running. The
			// repair is to finish the promotion, which also validates the graph.
			stranded = append(stranded, raw)
		}
	}
	s.completePromotions(ctx, t, tag, stranded)
	if len(stale) == 0 {
		return reclaimed, s.fillConcurrencySlots(ctx, t, tag)
	}

	// Removal is per-key because a member can occupy four workflow-scoped
	// indexes: the slot, the line, the queue deadline and the arrival clock.
	removePipe := s.rdb.Pipeline()
	for i := range stale {
		member := stale[i].member
		raw := concurrencySlotMember(member.id)
		if member.inSlot {
			stale[i].cmds = append(stale[i].cmds, removePipe.ZRem(ctx, slotKey, raw))
		}
		if member.inQueue {
			stale[i].cmds = append(stale[i].cmds, removePipe.ZRem(ctx, queueKey, raw))
			stale[i].cmds = append(stale[i].cmds, removePipe.ZRem(ctx, concurrencyDeadlineKeyForTag(t, tag), raw))
			stale[i].cmds = append(stale[i].cmds, removePipe.ZRem(ctx, arrivalKey, raw))
		}
	}
	if _, err := removePipe.Exec(ctx); err != nil && err != redis.Nil {
		return reclaimed, fmt.Errorf("sweep concurrency members for %q: %w", slotKey, err)
	}
	removed := 0
	for i := range stale {
		for _, cmd := range stale[i].cmds {
			if n, err := cmd.Result(); err == nil && n > 0 {
				removed++
				break
			}
		}
	}
	if removed == 0 {
		return reclaimed, s.fillConcurrencySlots(ctx, t, tag)
	}
	// Dropping the last holder takes the set with it, exactly as a normal
	// release does — otherwise the namespace index keeps naming a workflow
	// whose set is gone, and every later sweep reads a key that does not exist.
	card, err := s.rdb.ZCard(ctx, slotKey).Result()
	if err != nil && err != redis.Nil {
		return reclaimed + removed, fmt.Errorf("read swept concurrency set size %q: %w", slotKey, err)
	}
	if card == 0 {
		_ = s.rdb.Del(ctx, slotKey).Err()
		s.dropConcurrencyIndexIfIdle(ctx, t, tag)
	}
	return reclaimed + removed, s.fillConcurrencySlots(ctx, t, tag)
}

// stampMissingQueueArrival dates the sweep's first sighting of a line member
// that has no arrival record.
//
// It is the only way back for a member admitted before the arrival index
// existed: the line's own score is a sequence, not a time, so once the
// execution's status clock is gone nothing can prove the member is older than
// the create grace. Writing the instant it was first seen gives it an age, and
// the next pass past the grace reclaims it through the ordinary path.
//
// The write is best effort for the same reason every other sweep repair is: a
// lost stamp costs one more pass, while failing the sweep would hide every
// other workflow behind this one. NX is what keeps it from overwriting an
// arrival time a concurrent admission wrote.
func (s *Store) stampMissingQueueArrival(ctx context.Context, t namespace.Namespace, tag, raw string) {
	err := concurrencyArrivalStampLua.Run(ctx, s.rdb,
		[]string{concurrencyQueueArrivalKeyForTag(t, tag)},
		time.Now().UTC().Unix(), raw, s.concurrencySlotTTL().Milliseconds(),
	).Err()
	if err != nil && s.logger != nil {
		s.logger.Error("workflow_concurrency_arrival_stamp_failed",
			"namespace", string(t), "workflow_tag", tag, "execution_id", raw, "err", err)
	}
}

// dropConcurrencyIndexIfIdle removes a workflow's tag from its namespace index
// once there is nothing left for a sweep to find, and keeps it while the
// workflow still has a line.
//
// The line is why this is not a plain SRem. A non-empty queue is live state of
// its own — waiters whose deadlines have not been reached — and it can outlive
// the slot set: the set is only written while something holds a slot, while a
// queue_timeout can be longer than the slot TTL. Dropping the tag in that window
// would hide the line from the expiry sweep, which reaches every workflow
// through this index and nowhere else, and the waiters would then sit queued
// with nothing left to time them out — the failure the index's own wait-extended
// expiry exists to prevent. An empty ZSET does not exist in Redis, so the key's
// presence is exactly "the line is non-empty".
//
// A read failure keeps the tag for the same reason: a stale tag costs the sweep
// one read, a lost one hides work.
func (s *Store) dropConcurrencyIndexIfIdle(ctx context.Context, t namespace.Namespace, tag string) {
	queued, err := s.rdb.Exists(ctx, concurrencyQueueKeyForTag(t, tag)).Result()
	if err != nil || queued != 0 {
		return
	}
	_ = s.rdb.SRem(ctx, concurrencyWorkflowIndexKey(t), tag).Err()
}

// fillConcurrencySlots hands free capacity to the head of the line, the repair
// half of a sweep that released slots.
//
// Releasing a leaked slot frees capacity the line is owed, and nothing else
// would hand it over: promotions happen on release, and the sweep is not a
// release. Without this, a workflow whose slot leaked and whose line was
// waiting would recover its limit and still not start the waiting work until
// some later execution finished.
//
// It is also how a line entry naming an execution that was never created
// resolves itself. Such a member is promoted once, spends a slot it cannot
// start, and is dropped from the line by that promotion (ZPOPMIN removes it);
// the next sweep finds the slot it took pointing at nothing and clears it, and
// this fill then advances the real head. The cost is one sweep cycle for a
// submission that crashed between taking its place in line and being written.
func (s *Store) fillConcurrencySlots(ctx context.Context, t namespace.Namespace, tag string) error {
	reply, err := s.fillConcurrencySlotsOnce(ctx, t, tag)
	if err != nil {
		return err
	}
	s.completePromotions(ctx, t, tag, reply)
	return nil
}

// fillConcurrencySlotsOnce performs one fill and reports the members it
// promoted, without finishing their promotions. The promotion loop needs that
// separation: it completes what one fill promoted, reaps what cannot start, and
// then fills again, while the ordinary callers only need the whole sequence.
func (s *Store) fillConcurrencySlotsOnce(ctx context.Context, t namespace.Namespace, tag string) ([]string, error) {
	reply, err := workflowFillLua.Run(ctx, s.rdb,
		concurrencyReleaseKeys(t, tag),
		time.Now().UTC().Unix(), int(s.concurrencySlotTTL().Seconds()),
	).StringSlice()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("fill concurrency slots for %q: %w", tag, err)
	}
	return reply, nil
}
