package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TestUpdateExecutionStatusCancelingRejectedByTerminal verifies Defect 2:
// writing 'canceling' when the execution is already in a terminal state
// (success/failed/canceled/timeout) must be rejected by the Lua fence.
func TestUpdateExecutionStatusCancelingRejectedByTerminal(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()

	terminals := []types.ExecutionStatus{
		types.ExecutionStatusSuccess,
		types.ExecutionStatusFailed,
		types.ExecutionStatusCanceled,
		types.ExecutionStatusTimeout,
	}
	for _, terminal := range terminals {
		t.Run(string(terminal), func(t *testing.T) {
			id := types.ExecutionID("cancel-fence-" + string(terminal))
			if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
				ID:     id,
				Status: types.ExecutionStatusRunning,
			}); err != nil {
				t.Fatalf("CreateExecution: %v", err)
			}
			// Move to terminal state.
			if err := rdb.Set(ctx, execKey(namespace.Default, id, "status"), string(terminal), time.Minute).Err(); err != nil {
				t.Fatalf("set terminal: %v", err)
			}

			// Attempt to write 'canceling' — must be fenced.
			if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceling, ""); err != nil {
				t.Fatalf("UpdateExecutionStatus(canceling): %v", err)
			}

			// Verify status was NOT overwritten.
			got, err := rdb.Get(ctx, execKey(namespace.Default, id, "status")).Result()
			if err != nil {
				t.Fatalf("Get status: %v", err)
			}
			if got != string(terminal) {
				t.Fatalf("status = %q after canceling attempt, want %q (fenced)", got, terminal)
			}
		})
	}
}

// TestTimeoutZSetExpireOnSuspend verifies Defect 4: the timeout ZSET key gets
// an EXPIRE set when a node is suspended with a timeout.
func TestTimeoutZSetExpireOnSuspend(t *testing.T) {
	state, mr, rdb := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("timeout-ttl-1")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     id,
		Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	// Manually set node status to create a suspendable scenario.
	if err := rdb.Set(ctx, nodeStatusKey(namespace.Default, id, "w"), "running", time.Minute).Err(); err != nil {
		t.Fatalf("set node status: %v", err)
	}

	spec := &types.SuspendSpec{
		Signals: []string{"sig1"},
		Timeout: 30 * time.Second,
	}
	// SuspendOrConsume parks the node and registers the timeout ZSET entry.
	_, err := state.SuspendOrConsume(ctx, id, "w", spec)
	if err != nil {
		t.Fatalf("SuspendOrConsume: %v", err)
	}

	// Verify the timeout ZSET exists and has a TTL.
	key := timeoutZSetKey(namespace.Default, id)
	card, err := rdb.ZCard(ctx, key).Result()
	if err != nil {
		t.Fatalf("ZCard timeout: %v", err)
	}
	if card != 1 {
		t.Fatalf("timeout ZSET has %d members, want exactly 1", card)
	}
	// In miniredis, TTL is tracked. Use mr to check.
	_ = mr
	ttl := rdb.TTL(ctx, key).Val()
	if ttl <= 0 {
		t.Fatalf("timeout ZSET TTL = %v, want > 0 (EXPIRE must be set)", ttl)
	}
	// The ZSET's own EXPIRE comes from the store-wide TTL (state_lua.go's
	// ARGV[2] = s.ttlSec()), which is computed independently of the member's
	// score -- so ZCard and TTL above are both satisfied no matter what score
	// was written. The score is the part that decides when the node's wait
	// actually ends: timeout.Monitor's peekExpiredLua fires on
	// ZRANGEBYSCORE -inf now, and there is no test anywhere for the Monitor
	// itself. Dropping the `.Add(spec.Timeout)` at state_suspend.go:55 makes
	// every timed suspend fire on the Monitor's next poll (~5s) instead of
	// after its configured duration, waking waiting nodes early with no error
	// raised on any path.
	score, err := rdb.ZScore(ctx, key, timeoutMember(id, "w")).Result()
	if err != nil {
		t.Fatalf("ZScore timeout member: %v", err)
	}
	// spec.Timeout is 30s; allow generous slack for test scheduling but stay
	// well clear of "now", which is what the broken form writes.
	wantMin := float64(time.Now().Add(20 * time.Second).Unix())
	wantMax := float64(time.Now().Add(40 * time.Second).Unix())
	if score < wantMin || score > wantMax {
		t.Fatalf("timeout score = %v, want within [%v, %v] (now + spec.Timeout, "+
			"not now): the score is the only thing that defers the timeout",
			score, wantMin, wantMax)
	}
}

// TestTimeoutZSetDeletedOnCancel verifies Defect 4: the timeout ZSET is
// DELeted as part of cleanupOnCancel.
func TestTimeoutZSetDeletedOnCancel(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("timeout-cancel-1")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     id,
		Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	// Seed a timeout ZSET entry.
	key := timeoutZSetKey(namespace.Default, id)
	if err := rdb.ZAdd(ctx, key, redis.Z{Score: float64(time.Now().Unix()), Member: timeoutMember(id, "w")}).Err(); err != nil {
		t.Fatalf("ZAdd timeout: %v", err)
	}
	// Add the node to suspended_nodes so cleanupOnCancel iterates it.
	if err := rdb.SAdd(ctx, suspendedNodesKey(namespace.Default, id), "w").Err(); err != nil {
		t.Fatalf("SAdd suspended: %v", err)
	}

	// Cancel the execution — this calls cleanupOnCancel internally.
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceled, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceled): %v", err)
	}

	// Verify the timeout ZSET is deleted.
	exists, err := rdb.Exists(ctx, key).Result()
	if err != nil {
		t.Fatalf("Exists timeout: %v", err)
	}
	if exists != 0 {
		t.Fatalf("timeout ZSET still exists after cancel, want deleted")
	}
}

// TestTimeoutZSetInTransientKeys verifies Defect 4: transientExecutionKeys
// includes the timeout ZSET key.
func TestTimeoutZSetInTransientKeys(t *testing.T) {
	id := types.ExecutionID("timeout-transient-1")
	keys := transientExecutionKeys(namespace.Default, id, nil)
	want := timeoutZSetKey(namespace.Default, id)
	found := false
	for _, k := range keys {
		if k == want {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("transientExecutionKeys does not include timeoutZSetKey %q", want)
	}
}
