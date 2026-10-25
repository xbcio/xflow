package rstate

// Execution-deadline index.
//
// An execution created from a graph with settings.timeout carries an absolute
// Deadline (deadline_ms, see GetExecution). A running node enforces it through
// its lease, but an execution can sit past its deadline with no live lease at
// all: it is parked (suspended, paused, queued) or every runner died. The
// engine.ExecutionDeadlineExpirer capability is how a leader-driven sweep finds
// those; this file implements it for Redis.
//
// ---------------------------------------------------------------------------
// Layout
// ---------------------------------------------------------------------------
//
//	executionDeadlineIndexKey(t) = xflow:ns:<namespace>:exec_deadlines
//	    ZSET, member = execution ID, score = the deadline in unix milliseconds
//	    (the value deadline_ms stores).
//
// Namespace-scoped like the status keys it points at, with no {id} hash tag: it
// aggregates every deadline-bearing execution of the namespace, so it cannot
// live in any one execution's slot and must never be a KEYS entry of a
// per-execution Lua script (Redis Cluster refuses a script spanning slots).
// That is why registration and withdrawal are ordinary single-key commands
// around the writes, not part of them.
//
// ---------------------------------------------------------------------------
// Register-before-write
// ---------------------------------------------------------------------------
//
// Every creation path registers BEFORE it writes the execution, and a failed
// registration fails the creation. An execution with a Deadline and no entry is
// exactly the undiscoverable state the index exists to prevent, so the other
// order (best-effort registration after the write) is not acceptable:
//
//   - registration fails            -> nothing was written; the caller retries;
//   - registration succeeds, write
//     fails or is never sent        -> a stale entry for a missing execution,
//     which List repairs;
//   - both succeed                  -> the entry is exactly the candidate.
//
// ---------------------------------------------------------------------------
// Withdrawal and repair
// ---------------------------------------------------------------------------
//
// success, failed and canceled withdraw the entry after the terminal status is
// durable (afterExecutionTerminal, next to the slot release every terminal
// writer already performs). timeout does NOT: ExpireExecution writes timeout
// first and cascades afterwards, so a sweep must keep seeing the execution
// until AckDeadlineExpired says the cascade finished.
//
// A failed or skipped ZREM is not a correctness problem. List reads each
// candidate's live status and drops entries whose execution is gone or already
// concluded, so a stale member costs one inspection and then disappears.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

var _ engine.ExecutionDeadlineExpirer = (*Store)(nil)

// registerExecutionDeadline records an execution's deadline in its namespace's
// index. It is a no-op for the zero deadline (the graph declares no timeout) and
// returns an error otherwise, so the caller can refuse to write the execution
// (see the register-before-write argument above).
//
// ifAbsent keeps an existing entry's score. The entry-admission path needs it:
// its deadline_ms is written with SetNX, so a redelivered admission keeps the
// original deadline, and the index must keep the same one rather than move to a
// later instant. The other creation paths write a deadline that does not change
// for the execution's lifetime, where overwriting is the same value.
func (s *Store) registerExecutionDeadline(ctx context.Context, t namespace.Namespace, id types.ExecutionID, deadline time.Time, ifAbsent bool) error {
	if deadline.IsZero() {
		return nil
	}
	z := redis.Z{Score: float64(deadline.UnixMilli()), Member: string(id)}
	var err error
	if ifAbsent {
		err = s.rdb.ZAddNX(ctx, executionDeadlineIndexKey(t), z).Err()
	} else {
		err = s.rdb.ZAdd(ctx, executionDeadlineIndexKey(t), z).Err()
	}
	if err != nil {
		return fmt.Errorf("register execution %q in the deadline index: %w", id, err)
	}
	return nil
}

// withdrawExecutionDeadline removes an execution's entry once it concluded
// without timing out. timeout keeps its entry until AckDeadlineExpired, and a
// non-terminal status has nothing to withdraw. Best-effort by construction: the
// status is already authoritative, and a leftover member is repaired by the next
// List that reads it.
func (s *Store) withdrawExecutionDeadline(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus) {
	if status == types.ExecutionStatusTimeout || !types.IsTerminalExecutionStatus(status) {
		return
	}
	t := namespace.FromContext(ctx)
	if err := s.rdb.ZRem(ctx, executionDeadlineIndexKey(t), string(id)).Err(); err != nil && s.logger != nil {
		s.logger.Error("remove execution from deadline index failed",
			"namespace", string(t), "execution_id", string(id), "err", err)
	}
}

// afterExecutionTerminal is the post-terminal hook every terminal writer calls:
// it gives the workflow slot back and retires the deadline entry. Both take the
// status the writer just persisted rather than re-reading it, so neither can
// race the next transition into believing a different execution concluded.
func (s *Store) afterExecutionTerminal(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus) {
	s.releaseWorkflowSlotAfterTerminal(ctx, id, status)
	s.withdrawExecutionDeadline(ctx, id, status)
}

// deadlineIndexMember is a scanned entry kept with its scan-time score, so the
// repair write-back can restore the original deadline.
type deadlineIndexMember struct {
	id    types.ExecutionID
	score float64
}

// ListDeadlineExceededExecutions implements engine.ExecutionDeadlineExpirer. It
// returns at most limit executions whose deadline is at or before before and
// whose execution is not concluded (timeout included, until acknowledged),
// ordered by deadline and then by ID.
//
// The scan spans the store's namespaces (ConfigureOwnedNamespaces scope when
// set, the registry otherwise), so ctx needs no namespace. Candidates are
// gathered per namespace and ordered globally before the page is cut, so a
// backlog in one namespace cannot starve another's older entries out of the
// batch. Only the passed before is consulted; the wall clock is not read.
func (s *Store) ListDeadlineExceededExecutions(ctx context.Context, before time.Time, limit int) ([]engine.DeadlineExpiredExecution, error) {
	if limit <= 0 {
		return nil, nil
	}
	namespaces, err := s.scopedNamespaces(ctx)
	if err != nil {
		return nil, err
	}
	cutoff := strconv.FormatInt(before.UTC().UnixMilli(), 10)
	out := make([]engine.DeadlineExpiredExecution, 0, limit)
	for _, ns := range namespaces {
		key := executionDeadlineIndexKey(ns)
		members, err := s.rdb.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{
			Min:   "-inf",
			Max:   cutoff,
			Count: int64(limit),
		}).Result()
		if err != nil {
			return nil, fmt.Errorf("list deadline-exceeded executions in namespace %q: %w", ns, err)
		}
		if len(members) == 0 {
			continue
		}
		ids := make([]types.ExecutionID, len(members))
		pipe := s.rdb.Pipeline()
		cmds := make([]*redis.StringCmd, len(members))
		for i, member := range members {
			id, ok := member.Member.(string)
			if !ok {
				// Malformed member (never written by this package). Leave it
				// rather than deleting on a guess; it never names a candidate.
				continue
			}
			ids[i] = types.ExecutionID(id)
			cmds[i] = pipe.Get(ctx, execKey(ns, ids[i], "status"))
		}
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("read deadline-exceeded execution statuses in namespace %q: %w", ns, err)
		}
		var concluded []types.ExecutionID
		var missing []deadlineIndexMember
		for i, cmd := range cmds {
			if cmd == nil {
				continue
			}
			status, err := cmd.Result()
			if errors.Is(err, redis.Nil) {
				// The execution is gone (its keys expired, or registration won a
				// race with a creation that never landed): only the entry is left.
				missing = append(missing, deadlineIndexMember{id: ids[i], score: members[i].Score})
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("read deadline-exceeded execution status %q in namespace %q: %w", ids[i], ns, err)
			}
			switch types.ExecutionStatus(status) {
			case types.ExecutionStatusSuccess, types.ExecutionStatusFailed, types.ExecutionStatusCanceled:
				concluded = append(concluded, ids[i])
				continue
			}
			out = append(out, engine.DeadlineExpiredExecution{
				ExecutionID: ids[i],
				Namespace:   ns,
				Deadline:    time.UnixMilli(int64(members[i].Score)).UTC(),
			})
		}
		s.repairDeadlineIndex(ctx, ns, concluded, missing)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].Deadline.Equal(out[j].Deadline) {
			return out[i].Deadline.Before(out[j].Deadline)
		}
		if out[i].ExecutionID != out[j].ExecutionID {
			return out[i].ExecutionID < out[j].ExecutionID
		}
		return out[i].Namespace < out[j].Namespace
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// repairDeadlineIndex removes entries List found stale: concluded executions are
// final; missing ones are re-checked after the removal, because registration
// precedes the status write and a creation in flight can land between List's read
// and the ZREM. Anything that did appear goes back with its original score.
//
// A creation that lands after that re-check, for an entry whose deadline had
// already passed, is not recoverable here; it needs a timeout shorter than one
// creation round trip, which is not a budget a workflow can usefully declare.
func (s *Store) repairDeadlineIndex(ctx context.Context, ns namespace.Namespace, concluded []types.ExecutionID, missing []deadlineIndexMember) {
	stale := make([]any, 0, len(concluded)+len(missing))
	for _, id := range concluded {
		stale = append(stale, string(id))
	}
	for _, entry := range missing {
		stale = append(stale, string(entry.id))
	}
	if len(stale) == 0 {
		return
	}
	key := executionDeadlineIndexKey(ns)
	if err := s.rdb.ZRem(ctx, key, stale...).Err(); err != nil {
		if s.logger != nil {
			s.logger.Error("repair stale deadline index entries failed",
				"namespace", string(ns), "entries", len(stale), "err", err)
		}
		return
	}
	restore := make([]redis.Z, 0, len(missing))
	for _, entry := range missing {
		_, gerr := s.rdb.Get(ctx, execKey(ns, entry.id, "status")).Result()
		if errors.Is(gerr, redis.Nil) {
			continue
		}
		// Either the execution appeared, or the re-check itself failed and cannot
		// tell "gone" from "created". Restoring costs one more inspection;
		// losing a live registration strands the execution.
		restore = append(restore, redis.Z{Score: entry.score, Member: string(entry.id)})
	}
	if len(restore) == 0 {
		return
	}
	if err := s.rdb.ZAddNX(ctx, key, restore...).Err(); err != nil && s.logger != nil {
		s.logger.Error("restore deadline index entries failed",
			"namespace", string(ns), "entries", len(restore), "err", err)
	}
}

// AckDeadlineExpired implements engine.ExecutionDeadlineExpirer: it removes the
// entry of an execution ExpireExecution fully handled, from the namespace in
// ctx. Removing a member that is not there succeeds, so it is idempotent.
func (s *Store) AckDeadlineExpired(ctx context.Context, id types.ExecutionID) error {
	if err := s.rdb.ZRem(ctx, executionDeadlineIndexKey(namespace.FromContext(ctx)), string(id)).Err(); err != nil {
		return fmt.Errorf("acknowledge deadline expiry of %q: %w", id, err)
	}
	return nil
}
