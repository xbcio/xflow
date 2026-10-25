package rstate

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// deadlineGrace is how long an execution's keys outlive its deadline. The
// deadline sweep finds an overdue execution on its next pass and ExpireExecution
// then writes the timeout status and cascades over the node keys; all of that
// needs the keys to still exist, and the concluded execution stays inspectable
// for a while afterwards. The margin matches the one suspendTTL already adds to
// a node's own timeout.
const deadlineGrace = time.Hour

// retentionPastDeadline lengthens ttl so the execution's keys outlive deadline
// by deadlineGrace. It never shortens: an execution whose retention already
// covers its deadline keeps the retention it has. A zero deadline (no time
// budget) and a non-positive ttl (unbounded in this store) are both left alone.
//
// Without it an execution whose time budget exceeds the retention (24h by
// default) loses its status key before the deadline, and the expiry the
// deadline exists to trigger finds nothing to act on.
func retentionPastDeadline(ttl time.Duration, deadline time.Time) time.Duration {
	if deadline.IsZero() || ttl <= 0 {
		return ttl
	}
	if want := time.Until(deadline) + deadlineGrace; want > ttl {
		return want
	}
	return ttl
}

// executionDeadline reads the execution's persisted deadline for retention
// purposes. deadline is the zero time for an execution with no time budget, for
// one that does not exist yet, and when the read fails: callers use it only to
// lengthen a retention, so each of those degrades to the retention they would
// have used anyway.
//
// settled tells the caller whether the answer may be memoized. It is true only
// when the answer is final:
//   - deadline_ms exists: the deadline is immutable once written, so it is final.
//   - deadline_ms is absent and the status key exists: the execution is there and
//     has no time budget.
//
// It is false when deadline_ms is absent and the status key is absent too (the
// execution has not been created yet, or is gone), and when the read fails (which
// is logged, because it is otherwise indistinguishable from "no budget"). Without
// the existence check an "absent" reply cannot tell "no budget" from "not created
// yet", and a replica that happens to ask first would pin that execution to the
// default retention for as long as the process runs: its status would be shrunk
// back to the default by every later write while deadline_ms and the index member
// lived on, and the execution would never time out.
//
// The two keys are read in ONE transaction, so the pair is a single snapshot. Two
// separate reads could straddle the creation: "deadline_ms absent" read just
// before it, "status present" read just after, which reads as "no budget". Both
// keys carry the execution's {id} hash tag, so the transaction is one slot and
// valid on Redis Cluster.
//
// The rule rests on this ordering, which every creation path keeps: when the
// status key exists, an execution that has a time budget already has deadline_ms.
// createExecution writes both in one MULTI/EXEC; SeedExecution writes deadline_ms
// (linkPipe) before the seed Lua creates the status; SeedExecutionFromEntry
// SETNXes deadline_ms before its Lua does.
func (s *Store) executionDeadline(ctx context.Context, id types.ExecutionID) (deadline time.Time, settled bool) {
	t := namespace.FromContext(ctx)
	var statusExists *redis.IntCmd
	var deadlineMs *redis.StringCmd
	_, err := s.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		statusExists = pipe.Exists(ctx, execKey(t, id, "status"))
		deadlineMs = pipe.Get(ctx, execKey(t, id, "deadline_ms"))
		return nil
	})
	// The transaction reports the first command error, and a missing deadline_ms
	// is redis.Nil: that is an answer, handled below, not a failure.
	if err != nil && !errors.Is(err, redis.Nil) {
		s.logDeadlineReadFailure(id, err)
		return time.Time{}, false
	}
	ms, err := deadlineMs.Int64()
	switch {
	case err == nil:
		return time.UnixMilli(ms), true
	case errors.Is(err, redis.Nil):
		return time.Time{}, statusExists.Val() > 0
	default:
		s.logDeadlineReadFailure(id, err)
		return time.Time{}, false
	}
}

func (s *Store) logDeadlineReadFailure(id types.ExecutionID, err error) {
	if s.logger != nil {
		s.logger.Error("read execution deadline for retention failed",
			"execution_id", string(id), "err", err)
	}
}
