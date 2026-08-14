package rstate

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
)

// TestRedisReleaseOutboxDoesNotResurrectAnAckedEntry inspects the ready ZSET
// directly, because the store's own read path hides the defect it targets.
//
// Without the body check in releaseOutboxLua, a release that races an ack
// re-adds a ready member whose body is gone. ListOutbox self-heals that member
// on the next pass, so the black-box contract cannot see it — but
// recordOutboxFailureLua's dead-letter guard is documented to rely on
// body-presence ⇔ ready-membership, and the OutboxMetrics scan counts ready
// members without consulting bodies, so a ghost inflates the pending backlog
// until something happens to list that execution again.
//
// Real Redis only: the assertion lands entirely on Lua behaviour, and miniredis
// runs a different Lua engine.
func TestRedisReleaseOutboxDoesNotResurrectAnAckedEntry(t *testing.T) {
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("XFLOW_TEST_REDIS_ADDR unset; set 127.0.0.1:6380 for the podman env")
	}
	rdb := freshRealRedis(t, addr)
	state := New(rdb, nil, time.Minute)
	ctx := context.Background()

	id := types.ExecutionID("outbox-release-ghost-1")
	entry := engine.OutboxEntry{
		ID: "root/outbox-release-ghost-1/start/1",
		Task: engine.Task{
			ExecutionID: id, NodeName: "start", NodeIdx: 0,
			Type: engine.TaskTypeNodeExec, ActivationID: 1,
		},
	}
	if err := state.CreateExecutionWithOutbox(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: statestoretest.ContractGraph(), Status: types.ExecutionStatusRunning,
	}, []engine.OutboxEntry{entry}); err != nil {
		t.Fatalf("CreateExecutionWithOutbox() error = %v", err)
	}

	leased, err := state.ListOutbox(ctx, id, time.Now().UTC(), 8)
	if err != nil || len(leased) != 1 {
		t.Fatalf("ListOutbox() = (%d entries, %v), want 1 entry", len(leased), err)
	}
	if err := state.AckOutbox(ctx, id, entry.ID); err != nil {
		t.Fatalf("AckOutbox() error = %v", err)
	}
	// The deliverer holding the lease now discovers a full queue and releases.
	if err := state.ReleaseOutbox(ctx, id, leased[0]); err != nil {
		t.Fatalf("ReleaseOutbox() error = %v", err)
	}

	ready := outboxReadyKey(namespace.FromContext(ctx), id)
	members, err := rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key: ready, Start: "-inf", Stop: "+inf", ByScore: true,
	}).Result()
	if err != nil {
		t.Fatalf("ZRANGEBYSCORE(%s) error = %v", ready, err)
	}
	if len(members) != 0 {
		t.Fatalf("ready index holds %v after releasing an acked entry; a ready "+
			"member with no body inflates the pending backlog and breaks the "+
			"body-presence invariant the dead-letter guard reasons from", members)
	}
}
