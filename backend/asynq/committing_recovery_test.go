package asynq

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

func TestRedisClaimedLeaseRemainsDiscoverableAndRecoverable(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("committing-recovery")
	lease := &engine.TaskLease{
		LeaseID:    "lease-1",
		LeaseToken: "token-1",
		IssuedAt:   time.Now().Add(-time.Minute).UTC().Truncate(time.Millisecond),
		TTL:        time.Second,
		Task: engine.Task{
			ExecutionID:  id,
			NodeName:     "wait",
			NodeIdx:      4,
			Type:         engine.TaskTypeNodeResume,
			ActivationID: 3,
			AutoDepth:    2,
			Payload: &types.SignalPayload{
				Triggered: types.SignalReceived,
				Name:      "approval",
				Data:      map[string]any{"approved": true},
			},
		},
	}
	_, acquired, err := state.AcquireTaskLease(ctx, lease)
	if err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	lease.Attempt = 1
	claimed, valid, err := state.ClaimTaskLease(ctx, lease)
	if err != nil || !valid || claimed.Status != types.NodeStatusCommitting {
		t.Fatalf("ClaimTaskLease() = (%+v, %v, %v), want committing claim", claimed, valid, err)
	}

	node, err := state.GetNode(ctx, id, "wait")
	if err != nil {
		t.Fatal(err)
	}
	if node == nil || node.Status != types.NodeStatusCommitting || node.LeaseID != lease.LeaseID || node.LeaseToken != lease.LeaseToken || node.LeaseTaskType != engine.TaskTypeNodeResume || node.LeasePayload == nil || node.LeasePayload.Name != "approval" {
		t.Fatalf("claimed node = %+v, want preserved lease metadata", node)
	}
	if _, err := rdb.ZScore(ctx, leaseExpiryZSetKey(id), leaseExpiryMember(id, "wait")).Result(); err != nil {
		t.Fatalf("committing lease is missing expiry index: %v", err)
	}

	expired, err := state.ListExpiredLeases(ctx, time.Now())
	if err != nil || len(expired) != 1 {
		t.Fatalf("ListExpiredLeases() = %+v, %v; want claimed lease", expired, err)
	}
	got := expired[0]
	if got.LeaseToken != lease.LeaseToken || got.NodeIdx != 4 || got.TaskType != engine.TaskTypeNodeResume || got.Payload == nil || got.Payload.Name != "approval" || got.Payload.Data["approved"] != true || got.ActivationID != 3 || got.AutoDepth != 2 {
		t.Fatalf("expired lease = %+v, want exact replay metadata", got)
	}

	revoked, err := state.RevokeLease(ctx, id, "wait", lease.LeaseToken)
	if err != nil || !revoked {
		t.Fatalf("RevokeLease(committing) revoked=%v err=%v, want true/nil", revoked, err)
	}
	node, err = state.GetNode(ctx, id, "wait")
	if err != nil || node == nil || node.Status != types.NodeStatusPending || node.LeaseToken != "" {
		t.Fatalf("node after recovery = %+v err=%v, want pending tokenless node", node, err)
	}
	if _, err := rdb.ZScore(ctx, leaseExpiryZSetKey(id), leaseExpiryMember(id, "wait")).Result(); err != redis.Nil {
		t.Fatalf("recovered lease index entry = %v, want removed", err)
	}
}

func TestRedisSuspendTaskLeaseIsFencedAndClearsRecoveryLease(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("fenced-suspend")
	lease := &engine.TaskLease{
		LeaseID:    "lease-suspend",
		LeaseToken: "token-suspend",
		IssuedAt:   time.Now().UTC().Truncate(time.Millisecond),
		TTL:        time.Minute,
		Task: engine.Task{
			ExecutionID:  id,
			NodeName:     "wait",
			NodeIdx:      0,
			Type:         engine.TaskTypeNodeExec,
			ActivationID: 1,
		},
	}
	_, acquired, err := state.AcquireTaskLease(ctx, lease)
	if err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	lease.Attempt = 1
	if _, claimed, err := state.ClaimTaskLease(ctx, lease); err != nil || !claimed {
		t.Fatalf("ClaimTaskLease() claimed=%v err=%v, want true/nil", claimed, err)
	}
	if _, _, err := state.DeliverSignal(ctx, id, "approval", map[string]any{"by": "lead"}); err != nil {
		t.Fatalf("DeliverSignal(pre) error = %v", err)
	}

	payload, committed, err := state.SuspendTaskLease(ctx, lease, map[string]any{"request": "42"}, true, &types.SuspendSpec{
		Mode:    types.ModeSignal,
		Signals: []string{"approval"},
	}, "")
	if err != nil || !committed {
		t.Fatalf("SuspendTaskLease() = (%+v, %v, %v), want committed payload", payload, committed, err)
	}
	if payload == nil || payload.Name != "approval" || payload.Data["by"] != "lead" {
		t.Fatalf("suspend payload = %+v, want pre-delivered approval", payload)
	}
	node, err := state.GetNode(ctx, id, "wait")
	if err != nil {
		t.Fatal(err)
	}
	if node == nil || node.Status != types.NodeStatusSuspended || node.LeaseToken != "" {
		t.Fatalf("node after suspend = %+v, want suspended without lease", node)
	}
	output, err := state.GetOutput(ctx, id, "wait")
	if err != nil || output["request"] != "42" {
		t.Fatalf("suspend output = %+v err=%v, want persisted base output", output, err)
	}
	if expired, err := state.ListExpiredLeases(ctx, time.Now().Add(time.Hour)); err != nil || len(expired) != 0 {
		t.Fatalf("expired after suspend = %+v err=%v, want none", expired, err)
	}
	if _, committed, err := state.SuspendTaskLease(ctx, lease, nil, false, &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"approval"}}, ""); err != nil || committed {
		t.Fatalf("stale SuspendTaskLease() committed=%v err=%v, want false/nil", committed, err)
	}
}

func TestRedisSuspendTaskLeaseMultiSignalPreservesPayloadSet(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("fenced-multi-suspend")
	lease := &engine.TaskLease{
		LeaseID:    "lease-multi",
		LeaseToken: "token-multi",
		IssuedAt:   time.Now().UTC().Truncate(time.Millisecond),
		TTL:        time.Minute,
		Task: engine.Task{
			ExecutionID:  id,
			NodeName:     "wait",
			NodeIdx:      0,
			Type:         engine.TaskTypeNodeExec,
			ActivationID: 1,
		},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	lease.Attempt = 1
	if _, claimed, err := state.ClaimTaskLease(ctx, lease); err != nil || !claimed {
		t.Fatalf("ClaimTaskLease() claimed=%v err=%v, want true/nil", claimed, err)
	}
	for signalName, data := range map[string]map[string]any{
		"security": {"by": "sec"},
		"approval": {"by": "lead"},
	} {
		if _, _, err := state.DeliverSignal(ctx, id, signalName, data); err != nil {
			t.Fatalf("DeliverSignal(%q) error = %v", signalName, err)
		}
	}
	payload, committed, err := state.SuspendTaskLease(ctx, lease, nil, false, &types.SuspendSpec{
		Mode:    types.ModeMultiSignal,
		Signals: []string{"security", "approval"},
		Quorum:  2,
	}, "")
	if err != nil || !committed {
		t.Fatalf("SuspendTaskLease() = (%+v, %v, %v), want committed multi payload", payload, committed, err)
	}
	if payload == nil || len(payload.All) != 2 || payload.All["security"]["by"] != "sec" || payload.All["approval"]["by"] != "lead" {
		t.Fatalf("multi payload = %+v, want both pre-delivered signals", payload)
	}
}

func TestRedisWaitingExpansionLeaseIsRepairableAndFencesOldBatches(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("waiting-expansion-recovery")
	lease := &engine.TaskLease{
		LeaseID:    "lease-one",
		LeaseToken: "token-one",
		IssuedAt:   time.Now().Add(-time.Minute).UTC().Truncate(time.Millisecond),
		TTL:        time.Second,
		Task: engine.Task{
			ExecutionID: id,
			NodeName:    "loop",
			NodeIdx:     2,
			Type:        engine.TaskTypeNodeExec,
		},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease(first) acquired=%v err=%v, want true/nil", acquired, err)
	}
	lease.Attempt = 1
	if _, claimed, err := state.ClaimTaskLease(ctx, lease); err != nil || !claimed {
		t.Fatalf("ClaimTaskLease(first) claimed=%v err=%v, want true/nil", claimed, err)
	}
	if started, err := state.BeginTaskExpansion(ctx, lease); err != nil || !started {
		t.Fatalf("BeginTaskExpansion(first) started=%v err=%v, want true/nil", started, err)
	}
	if accepted, err := state.CreateExpandedSubExecution(ctx, lease, &engine.SubExecution{
		ParentExecID: id,
		ParentNode:   "loop",
		ChildExecID:  "child-one",
		BatchIndex:   0,
		Status:       types.ExecutionStatusRunning,
	}); err != nil || !accepted {
		t.Fatalf("CreateExpandedSubExecution(first) accepted=%v err=%v, want true/nil", accepted, err)
	}

	index := leaseExpiryZSetKey(id)
	member := leaseExpiryMember(id, "loop")
	if err := rdb.Del(ctx, index).Err(); err != nil {
		t.Fatalf("delete waiting lease index: %v", err)
	}
	if reconciled, err := state.RepairLeaseIndex(ctx, 16); err != nil || reconciled != 1 {
		t.Fatalf("RepairLeaseIndex() reconciled=%d err=%v, want 1/nil", reconciled, err)
	}
	if _, err := rdb.ZScore(ctx, index, member).Result(); err != nil {
		t.Fatalf("waiting lease is not indexed after repair: %v", err)
	}
	if expired, err := state.ListExpiredLeases(ctx, time.Now()); err != nil || len(expired) != 1 || expired[0].LeaseToken != lease.LeaseToken {
		t.Fatalf("ListExpiredLeases(waiting) = %+v, %v; want first waiting lease", expired, err)
	}
	if revoked, err := state.RevokeLease(ctx, id, "loop", lease.LeaseToken); err != nil || !revoked {
		t.Fatalf("RevokeLease(waiting) revoked=%v err=%v, want true/nil", revoked, err)
	}

	second := &engine.TaskLease{
		LeaseID:    "lease-two",
		LeaseToken: "token-two",
		IssuedAt:   time.Now().UTC().Truncate(time.Millisecond),
		TTL:        time.Minute,
		Task: engine.Task{
			ExecutionID: id,
			NodeName:    "loop",
			NodeIdx:     2,
			Type:        engine.TaskTypeNodeExec,
		},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, second); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease(second) acquired=%v err=%v, want true/nil", acquired, err)
	}
	second.Attempt = 2
	if _, claimed, err := state.ClaimTaskLease(ctx, second); err != nil || !claimed {
		t.Fatalf("ClaimTaskLease(second) claimed=%v err=%v, want true/nil", claimed, err)
	}
	if started, err := state.BeginTaskExpansion(ctx, second); err != nil || !started {
		t.Fatalf("BeginTaskExpansion(second) started=%v err=%v, want true/nil", started, err)
	}
	if allDone, accepted, _, err := state.CompleteExpandedSubExecution(ctx, lease, "child-one", types.ExecutionStatusSuccess, map[string]any{"old": true}); err != nil || accepted || allDone {
		t.Fatalf("CompleteExpandedSubExecution(stale) = allDone=%v accepted=%v err=%v, want false/false/nil", allDone, accepted, err)
	}
	node, err := state.GetNode(ctx, id, "loop")
	if err != nil {
		t.Fatal(err)
	}
	if node == nil || node.Status != types.NodeStatusWaiting || node.LeaseToken != second.LeaseToken || node.Attempt != second.Attempt {
		t.Fatalf("node after stale batch = %+v, want second waiting generation", node)
	}
}

func TestRedisSuspendWithOutboxPersistsPreDeliveredResume(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("durable-suspend-resume")
	lease := &engine.TaskLease{
		LeaseID:    "lease-durable-suspend",
		LeaseToken: "token-durable-suspend",
		IssuedAt:   time.Now().UTC().Truncate(time.Millisecond),
		TTL:        time.Minute,
		Task: engine.Task{
			ExecutionID:  id,
			NodeName:     "wait",
			NodeIdx:      3,
			Type:         engine.TaskTypeNodeExec,
			ActivationID: 1,
		},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	lease.Attempt = 1
	if _, claimed, err := state.ClaimTaskLease(ctx, lease); err != nil || !claimed {
		t.Fatalf("ClaimTaskLease() claimed=%v err=%v, want true/nil", claimed, err)
	}
	if _, _, err := state.DeliverSignal(ctx, id, "approval", map[string]any{"by": "lead"}); err != nil {
		t.Fatalf("DeliverSignal() error = %v", err)
	}
	if committed, err := state.SuspendTaskLeaseWithOutbox(ctx, lease, map[string]any{"request": "42"}, true, &types.SuspendSpec{
		Mode:    types.ModeSignal,
		Signals: []string{"approval"},
	}, ""); err != nil || !committed {
		t.Fatalf("SuspendTaskLeaseWithOutbox() committed=%v err=%v, want true/nil", committed, err)
	}
	entries, err := state.ListOutbox(ctx, id, time.Now().Add(time.Second), 4)
	if err != nil {
		t.Fatalf("ListOutbox() error = %v", err)
	}
	if len(entries) != 1 || entries[0].Task.Type != engine.TaskTypeNodeResume || entries[0].Task.Payload == nil || entries[0].Task.Payload.Name != "approval" || entries[0].Task.Payload.Data["by"] != "lead" {
		t.Fatalf("durable suspend outbox = %+v, want approval resume", entries)
	}
	node, err := state.GetNode(ctx, id, "wait")
	if err != nil || node == nil || node.Status != types.NodeStatusSuspended || node.LeaseToken != "" {
		t.Fatalf("node after durable suspend = %+v err=%v, want suspended without lease", node, err)
	}
}
