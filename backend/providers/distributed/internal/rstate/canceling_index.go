package rstate

// Canceling-execution index.
//
// Cancel is driven in five steps (engine/cancel.go), and a failure or crash
// after the first one leaves the execution in canceling with no request left to
// finish it. engine.CancelingExecutionLister is the capability a leader-driven
// reaper uses to find those executions; this file implements it for Redis.
//
// ---------------------------------------------------------------------------
// Layout
// ---------------------------------------------------------------------------
//
//	cancelingIndexKey(t) = xflow:ns:<namespace>:cancel:index
//	    ZSET, member = execution ID, score = the millisecond instant the
//	    canceling transition was registered.
//
// The key is namespace-scoped because the status keys it points at are, and it
// deliberately carries no {id} hash tag: it aggregates every execution in the
// namespace, so it cannot live in any one execution's slot.
//
// ---------------------------------------------------------------------------
// Register-before-write
// ---------------------------------------------------------------------------
//
// ZADD NX cannot be folded into updateExecutionStatusLua. That script touches
// only the execution's own `{id}`-tagged keys, and Redis Cluster refuses a
// script spanning slots — the same reason state_outbox_index.go gives for its
// readiness index. The usual answer for a non-atomic index is "best effort,
// with a scan fallback", but that answer is wrong here: a missed registration
// would leave an invisible canceling execution that nothing ever reaps, which
// is exactly the defect this index exists to remove.
//
// The ordering instead inverts the dependency: UpdateExecutionStatus performs
// the ZADD NX BEFORE the status write and fails the transition if it cannot.
// Every reachable canceling status therefore has an entry:
//
//   - registration fails            → the status write is never attempted, so
//     the execution is not canceling and the caller can retry Cancel;
//   - registration succeeds, status
//     write fails or is never sent  → a stale entry with a non-canceling (or
//     missing) execution, which List verifies and repairs;
//   - both succeed                  → the entry is exactly the stuck candidate.
//
// ZADD NX is also what keeps a retried or concurrently resumed Cancel from
// resetting the age: the score is stamped once, when the execution first
// entered canceling, and older is the conservative direction for a stuck
// threshold.
//
// ---------------------------------------------------------------------------
// Removal and repair
// ---------------------------------------------------------------------------
//
// A terminal status write removes the entry best-effort after the status is
// durable (the same place cleanupOnCancel runs). A failed ZREM is not
// correctness: List reads each candidate's live status and removes entries that
// are no longer canceling or whose execution has expired, so a stale member
// costs one no-op inspection and then disappears.
//
// The removal re-verifies what it deletes. The status read that classified a
// member stale and the ZREM that removes it are not atomic, so the member can
// re-enter canceling in between — a retried Cancel whose first registration
// succeeded but whose status write did not land is exactly that shape: its ZADD
// NX is a no-op on the surviving member, and its status write lands after the
// stale verdict. List therefore reads the status again after the ZREM and puts
// back anything still canceling, with the original score so the age is
// preserved. Without that write-back the member would be gone for good and the
// execution would sit in canceling where no reaper can see it.

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

// markExecutionCanceling registers an execution in its namespace's canceling
// index. It is called by UpdateExecutionStatus before the canceling status
// write; a failure is returned so the transition is not attempted (see the
// register-before-write argument above).
func (s *Store) markExecutionCanceling(ctx context.Context, t namespace.Namespace, id types.ExecutionID) error {
	if err := s.rdb.ZAddNX(ctx, cancelingIndexKey(t), redis.Z{
		Score:  float64(time.Now().UTC().UnixMilli()),
		Member: string(id),
	}).Err(); err != nil {
		return fmt.Errorf("register execution %q in the canceling index: %w", id, err)
	}
	return nil
}

// clearExecutionCanceling removes an execution from its namespace's canceling
// index after a terminal status write. Best-effort by construction: the status
// is already authoritative, and a leftover member is repaired by the next List
// that reads it.
func (s *Store) clearExecutionCanceling(ctx context.Context, t namespace.Namespace, id types.ExecutionID) {
	if err := s.rdb.ZRem(ctx, cancelingIndexKey(t), string(id)).Err(); err != nil && s.logger != nil {
		s.logger.Error("remove execution from canceling index failed",
			"namespace", string(t), "execution_id", string(id), "err", err)
	}
}

var _ engine.CancelingExecutionLister = (*Store)(nil)

// cancelingIndexMember is a scanned entry kept together with its scan-time
// score. The repair write-back must restore the original age, so the score read
// by the scan — not the instant of the repair — is what goes back into the
// index.
type cancelingIndexMember struct {
	id    types.ExecutionID
	score float64
}

// ListCancelingExecutions returns up to limit executions whose canceling
// registration is at or before olderThan, oldest first.
//
// The scan spans the store's namespaces (ConfigureOwnedNamespaces scope when
// set, the registry otherwise). For every member the live status key is read
// and checked: only executions still in canceling are returned, and anything
// else is removed from the index as stale. Reading the index first means the
// cost tracks the number of stuck executions, not the size of the keyspace.
//
// Candidates are gathered per namespace and then ordered globally by
// registration time before the page is cut, so a backlog in one namespace
// cannot starve another's older entries out of the bounded batch.
func (s *Store) ListCancelingExecutions(ctx context.Context, olderThan time.Time, limit int) ([]engine.CancelingExecution, error) {
	if limit <= 0 {
		return nil, nil
	}
	namespaces, err := s.scopedNamespaces(ctx)
	if err != nil {
		return nil, err
	}
	cutoff := strconv.FormatInt(olderThan.UTC().UnixMilli(), 10)
	out := make([]engine.CancelingExecution, 0, limit)
	for _, ns := range namespaces {
		key := cancelingIndexKey(ns)
		members, err := s.rdb.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{
			Min:   "-inf",
			Max:   cutoff,
			Count: int64(limit),
		}).Result()
		if err != nil {
			return nil, fmt.Errorf("list canceling executions in namespace %q: %w", ns, err)
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
				// Malformed member (never written by this package). Leave it in
				// the index rather than deleting on a guess; it simply never
				// names a candidate.
				continue
			}
			ids[i] = types.ExecutionID(id)
			cmds[i] = pipe.Get(ctx, execKey(ns, ids[i], "status"))
		}
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("read canceling execution statuses in namespace %q: %w", ns, err)
		}
		var stale []cancelingIndexMember
		for i, cmd := range cmds {
			if cmd == nil {
				continue
			}
			status, err := cmd.Result()
			if errors.Is(err, redis.Nil) {
				// The execution's status key expired (or the execution never
				// existed): the registration is the only thing left.
				stale = append(stale, cancelingIndexMember{id: ids[i], score: members[i].Score})
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("read canceling execution status %q in namespace %q: %w", ids[i], ns, err)
			}
			if types.ExecutionStatus(status) != types.ExecutionStatusCanceling {
				stale = append(stale, cancelingIndexMember{id: ids[i], score: members[i].Score})
				continue
			}
			out = append(out, engine.CancelingExecution{
				ExecutionID: ids[i],
				Namespace:   ns,
				Since:       time.UnixMilli(int64(members[i].Score)).UTC(),
			})
		}
		if len(stale) > 0 {
			staleMembers := make([]any, len(stale))
			for i, entry := range stale {
				staleMembers[i] = string(entry.id)
			}
			if err := s.rdb.ZRem(ctx, key, staleMembers...).Err(); err != nil {
				if s.logger != nil {
					s.logger.Error("repair stale canceling index entries failed",
						"namespace", string(ns), "entries", len(stale), "err", err)
				}
			} else {
				// Re-verify what the ZREM just deleted: the status can have
				// moved to canceling after the read above (see the file
				// comment). Anything still canceling goes back with its
				// original score; anything gone or otherwise moved stays out.
				restore := make([]redis.Z, 0, len(stale))
				unreadable := false
				for _, entry := range stale {
					status, gerr := s.rdb.Get(ctx, execKey(ns, entry.id, "status")).Result()
					if errors.Is(gerr, redis.Nil) {
						continue
					}
					if gerr != nil {
						unreadable = true
						continue
					}
					if types.ExecutionStatus(status) == types.ExecutionStatusCanceling {
						restore = append(restore, redis.Z{Score: entry.score, Member: string(entry.id)})
					}
				}
				if unreadable {
					// A failed re-check cannot tell "expired" from "still
					// canceling". Restore the whole batch and let the next
					// List decide: losing a live registration would strand a
					// canceling execution forever, while one extra sweep of a
					// dead entry costs a single inspection.
					restore = restore[:0]
					for _, entry := range stale {
						restore = append(restore, redis.Z{Score: entry.score, Member: string(entry.id)})
					}
					if s.logger != nil {
						s.logger.Error("re-check stale canceling index entries failed; restored the batch",
							"namespace", string(ns), "entries", len(stale))
					}
				}
				if len(restore) > 0 {
					if err := s.rdb.ZAddNX(ctx, key, restore...).Err(); err != nil && s.logger != nil {
						s.logger.Error("restore canceling index entries failed",
							"namespace", string(ns), "entries", len(restore), "err", err)
					}
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].Since.Equal(out[j].Since) {
			return out[i].Since.Before(out[j].Since)
		}
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].ExecutionID < out[j].ExecutionID
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
