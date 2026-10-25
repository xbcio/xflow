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

// executionDeadline reads the execution's persisted deadline. It returns the
// zero time both for an execution with no time budget and when the read fails:
// callers use it only to lengthen a retention, so a miss degrades to the
// retention they would have used anyway. ok tells the two apart: true when the
// store answered (an absent key is the answer "no budget"), false when the read
// failed, which is logged because it is otherwise indistinguishable from "no
// budget" and which a caller must not cache.
func (s *Store) executionDeadline(ctx context.Context, id types.ExecutionID) (deadline time.Time, ok bool) {
	ms, err := s.rdb.Get(ctx, execKey(namespace.FromContext(ctx), id, "deadline_ms")).Int64()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return time.Time{}, true
		}
		if s.logger != nil {
			s.logger.Error("read execution deadline for retention failed",
				"execution_id", string(id), "err", err)
		}
		return time.Time{}, false
	}
	return time.UnixMilli(ms), true
}
