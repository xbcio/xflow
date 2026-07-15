package asynq

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

func TestRedisCreateExecutionWithOutboxPersistsInitialIntent(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("outbox-create")
	entry := engine.OutboxEntry{
		ID: "root/outbox-create/start/0",
		Task: engine.Task{
			ExecutionID: id,
			NodeName:    "start",
			NodeIdx:     0,
			Type:        engine.TaskTypeNodeExec,
		},
	}
	if err := state.CreateExecutionWithOutbox(ctx, &engine.ExecutionSnapshot{
		ID:     id,
		Graph:  testGraphOneNode(),
		Status: types.ExecutionStatusRunning,
	}, []engine.OutboxEntry{entry}); err != nil {
		t.Fatalf("CreateExecutionWithOutbox() error = %v", err)
	}

	snapshot, err := state.GetExecution(ctx, id)
	if err != nil || snapshot == nil || snapshot.Status != types.ExecutionStatusRunning {
		t.Fatalf("execution snapshot=%+v err=%v, want running execution", snapshot, err)
	}
	entries, err := state.ListOutbox(ctx, id, time.Now().Add(time.Second), 4)
	if err != nil {
		t.Fatalf("ListOutbox() error = %v", err)
	}
	if len(entries) != 1 || entries[0].ID != entry.ID || entries[0].Task.NodeName != "start" {
		t.Fatalf("initial outbox entries=%+v, want %+v", entries, entry)
	}
}

func TestRedisRetryAndReleaseOutboxTransitionsAreFenced(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	issued := time.Now().Add(-time.Minute).UTC().Truncate(time.Millisecond)

	t.Run("retry reset records delayed intent", func(t *testing.T) {
		id := types.ExecutionID("outbox-retry")
		mustUpsertRunning(t, state, id, "start", "retry-token", issued, time.Minute)
		entry := engine.OutboxEntry{
			ID:          "retry/outbox-retry/start/0/1",
			Task:        engine.Task{ExecutionID: id, NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeExec},
			AvailableAt: time.Now().Add(time.Minute).UTC(),
		}
		scheduled, err := state.ResetNodeForRetryWithOutbox(ctx, id, "start", "retry-token", entry)
		if err != nil || !scheduled {
			t.Fatalf("ResetNodeForRetryWithOutbox() scheduled=%v err=%v, want true/nil", scheduled, err)
		}
		node, err := state.GetNode(ctx, id, "start")
		if err != nil || node == nil || node.Status != types.NodeStatusPending || node.LeaseToken != "" {
			t.Fatalf("node after reset=%+v err=%v, want pending without token", node, err)
		}
		entries, err := state.ListOutbox(ctx, id, time.Now().Add(2*time.Minute), 4)
		if err != nil || len(entries) != 1 || entries[0].ID != entry.ID {
			t.Fatalf("retry outbox=%+v err=%v, want %+v", entries, err, entry)
		}
	})

	t.Run("release stale token cannot create intent", func(t *testing.T) {
		id := types.ExecutionID("outbox-release")
		mustUpsertRunning(t, state, id, "start", "current-token", issued, time.Minute)
		entry := engine.OutboxEntry{
			ID:   "requeue/outbox-release/start/0/lease-1",
			Task: engine.Task{ExecutionID: id, NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeExec},
		}
		revoked, err := state.RevokeLeaseWithOutbox(ctx, id, "start", "stale-token", entry)
		if err != nil || revoked {
			t.Fatalf("stale RevokeLeaseWithOutbox() revoked=%v err=%v, want false/nil", revoked, err)
		}
		entries, err := state.ListOutbox(ctx, id, time.Now().Add(time.Second), 4)
		if err != nil || len(entries) != 0 {
			t.Fatalf("stale release created outbox entries=%+v err=%v", entries, err)
		}

		revoked, err = state.RevokeLeaseWithOutbox(ctx, id, "start", "current-token", entry)
		if err != nil || !revoked {
			t.Fatalf("current RevokeLeaseWithOutbox() revoked=%v err=%v, want true/nil", revoked, err)
		}
		entries, err = state.ListOutbox(ctx, id, time.Now().Add(time.Second), 4)
		if err != nil || len(entries) != 1 || entries[0].ID != entry.ID {
			t.Fatalf("release outbox=%+v err=%v, want %+v", entries, err, entry)
		}
	})
}
