package rstate

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

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

// sweepConcurrencySlotSet inspects one workflow's slot set and drops the
// members that no longer name a live execution, finishing the promotions a
// crash left half-done.
func (s *Store) sweepConcurrencySlotSet(ctx context.Context, t namespace.Namespace, tag string, limit int) (int, error) {
	slotKey := concurrencySlotKeyForTag(t, tag)
	members, err := s.rdb.ZRangeWithScores(ctx, slotKey, 0, int64(limit-1)).Result()
	if err != nil && err != redis.Nil {
		return 0, fmt.Errorf("read concurrency slots %q: %w", slotKey, err)
	}
	if len(members) == 0 {
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
		if exists, err := s.rdb.Exists(ctx, slotKey).Result(); err == nil && exists == 0 {
			s.dropConcurrencyIndexIfIdle(ctx, t, tag)
		}
		return 0, s.fillConcurrencySlots(ctx, t, tag)
	}

	// One pipelined read for every member's execution status, rather than a
	// round trip per slot: the set is bounded by the workflow's limit, and the
	// sweep runs on every namespace on a maintenance cadence.
	pipe := s.rdb.Pipeline()
	statuses := make([]*redis.StringCmd, 0, len(members))
	now := time.Now().UTC()
	for _, member := range members {
		id := types.ExecutionID(fmt.Sprint(member.Member))
		statuses = append(statuses, pipe.Get(ctx, execKey(t, id, "status")))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return 0, fmt.Errorf("read execution statuses for %q: %w", slotKey, err)
	}

	stale := make([]any, 0, len(members))
	stranded := make([]string, 0, len(members))
	for i, cmd := range statuses {
		id := types.ExecutionID(fmt.Sprint(members[i].Member))
		raw, err := cmd.Result()
		if err == redis.Nil {
			// The execution key is gone. Either it was never written (a crash
			// between admission and create) or its retention lapsed, which
			// means the member cannot belong to anything live either way — but
			// only once the create window has certainly passed.
			if now.Sub(time.Unix(int64(members[i].Score), 0)) < concurrencyStaleGrace {
				continue
			}
			stale = append(stale, concurrencySlotMember(id))
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("read execution status for %q: %w", id, err)
		}
		switch status := types.ExecutionStatus(raw); {
		case types.IsTerminalExecutionStatus(status):
			stale = append(stale, concurrencySlotMember(id))
		case status == types.ExecutionStatusQueued:
			// A queued execution holding a slot is a promotion that was decided
			// — the slot was handed to it — and never finished: its status and
			// its intents are written in its own hash slot, which the release
			// script cannot reach. Removing the member would strand it; not
			// acting would leave a slot held by work that is not running. The
			// repair is to finish the promotion.
			stranded = append(stranded, string(id))
		}
	}
	s.completePromotions(ctx, t, tag, stranded)
	if len(stale) == 0 {
		return 0, s.fillConcurrencySlots(ctx, t, tag)
	}

	removed, err := s.rdb.ZRem(ctx, slotKey, stale...).Result()
	if err != nil && err != redis.Nil {
		return 0, fmt.Errorf("sweep concurrency slots %q: %w", slotKey, err)
	}
	if removed == 0 {
		return 0, s.fillConcurrencySlots(ctx, t, tag)
	}
	// Dropping the last holder takes the set with it, exactly as a normal
	// release does — otherwise the namespace index keeps naming a workflow
	// whose set is gone, and every later sweep reads a key that does not exist.
	card, err := s.rdb.ZCard(ctx, slotKey).Result()
	if err != nil && err != redis.Nil {
		return int(removed), fmt.Errorf("read swept concurrency set size %q: %w", slotKey, err)
	}
	if card == 0 {
		_ = s.rdb.Del(ctx, slotKey).Err()
		s.dropConcurrencyIndexIfIdle(ctx, t, tag)
	}
	return int(removed), s.fillConcurrencySlots(ctx, t, tag)
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
	reply, err := workflowFillLua.Run(ctx, s.rdb,
		concurrencyReleaseKeys(t, tag),
		time.Now().UTC().Unix(), int(s.concurrencySlotTTL().Seconds()),
	).StringSlice()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("fill concurrency slots for %q: %w", tag, err)
	}
	s.completePromotions(ctx, t, tag, reply)
	return nil
}
