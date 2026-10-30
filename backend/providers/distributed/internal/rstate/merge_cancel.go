package rstate

import (
	"context"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// retireCanceledWaiterLua removes what could still resume a node a merge just
// canceled: its entry in the suspended set, its timeout member, its per-node
// spec / batch / resume lock, and the signal-name keyed waiter keys that still
// name it.
//
// A waiter key is deleted only when its value is this node. Waiter keys are
// keyed by signal name alone, so a name shared with another node's live wait
// would otherwise lose that node's waiter. Buffered signal values are left
// alone for the same reason: an unconsumed signal belongs to whichever node
// parks on that name next, not to the node being canceled.
//
// KEYS: suspended set, timeout ZSET, waiter spec, signal batch, resume lock,
// then the waiter keys. ARGV: node name, timeout member. Every key carries the
// execution's hash tag, so the script stays on one slot in Redis Cluster.
var retireCanceledWaiterLua = redis.NewScript(`
redis.call('SREM', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[2])
redis.call('DEL', KEYS[3], KEYS[4], KEYS[5])
for i = 6, #KEYS do
    if redis.call('GET', KEYS[i]) == ARGV[1] then
        redis.call('DEL', KEYS[i])
    end
end
return 1
`)

// retireCanceledWaiter runs after commitNodeLua accepted a canceled System
// commit. The commit script cannot do this itself: the waiter keys are named
// by signal, and the signal names live only in the node's stored waiter spec,
// which a script cannot read before it has its KEYS (the layout is fixed and
// Cluster-safe).
//
// The two steps are not one transition. Between the commit and this script a
// signal can still find the waiter and write a resume intent; that intent dies
// at lease acquisition, because the node is terminal by then, so it wakes
// nothing. A failure here is logged and swallowed: the cancel is already
// durable and reported as accepted, and a later signal reaches the same
// terminal-node fence. The waiter then survives until its TTL or the
// execution's own teardown.
func (s *Store) retireCanceledWaiter(ctx context.Context, id types.ExecutionID, nodeName string) {
	t := namespace.FromContext(ctx)
	keys := []string{
		suspendedNodesKey(t, id),
		timeoutZSetKey(t, id),
		waiterSpecKey(t, id, nodeName),
		signalBatchKey(t, id, nodeName),
		resumeLockKey(t, id, nodeName),
	}
	spec, err := s.loadWaiterSpec(ctx, id, nodeName)
	if err != nil {
		s.logRetireWaiterFailure(id, nodeName, err)
	}
	if spec != nil {
		for _, signalName := range spec.Signals {
			keys = append(keys, waiterKey(t, id, signalName))
		}
	}
	if err := retireCanceledWaiterLua.Run(ctx, s.rdb, keys, nodeName, timeoutMember(id, nodeName)).Err(); err != nil && err != redis.Nil {
		s.logRetireWaiterFailure(id, nodeName, err)
	}
}

func (s *Store) logRetireWaiterFailure(id types.ExecutionID, nodeName string, err error) {
	if s.logger == nil {
		return
	}
	s.logger.Error("retire_canceled_waiter_failed",
		"execution_id", string(id), "node", nodeName, "err", err)
}
