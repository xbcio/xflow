package control

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// TestCoreReplayRebuildsTheInputTheDirectoryDropped pins the other half of the
// trade: a replayed lease whose persisted record carries no input still reaches
// the runner complete, because Core rebuilds it through the engine.
func TestCoreReplayRebuildsTheInputTheDirectoryDropped(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-replay-input", 1)
	assignment := redisDirectoryTestAssignment("exec-1/node-replay-input/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	lease := &engine.TaskLease{
		LeaseID:    "lease-replay-input",
		LeaseToken: "token-replay-input",
		Task:       assignment.Task,
		Input:      &types.Input{Data: map[string]any{"request_id": "req-1"}},
		NodeType:   assignment.Routing.NodeType,
		IssuedAt:   time.Now().UTC(),
		TTL:        time.Minute,
	}
	if err := directory.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	core := &Core{engine: &fakeControlEngine{recoverLease: lease}, runners: directory, pollWait: time.Second}
	resp, err := core.pollTask(ctx, protocol.PollTaskRequest{
		RunnerID: session.RunnerID, SessionID: session.SessionID, Capacity: 1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("pollTask() error = %v", err)
	}
	if resp.Lease == nil || resp.Lease.LeaseToken != lease.LeaseToken {
		t.Fatalf("replayed lease = %+v, want token %q", resp.Lease, lease.LeaseToken)
	}
	if resp.Lease.Input == nil || resp.Lease.Input.Data["request_id"] != "req-1" {
		t.Fatalf("replayed lease input = %+v, want the rebuilt input", resp.Lease.Input)
	}
}

// TestRedisLeaseMetaKeepsInputOnlyForSuspendingNodes pins what the persisted
// lease carries: a suspending node keeps its input, because its first
// suspension stores it as the node's output; every other node drops it, because
// nothing reads it back and it is the bulk of the record.
func TestRedisLeaseMetaKeepsInputOnlyForSuspendingNodes(t *testing.T) {
	const marker = "payload-marker-value"
	for _, tc := range []struct {
		nodeType string
		keep     bool
	}{
		{"xflow.wait", true},
		{"xflow.approval", true},
		{"xflow.sas.sink", false},
		{"xflow.http", false},
	} {
		t.Run(tc.nodeType, func(t *testing.T) {
			lease := &engine.TaskLease{
				LeaseID:    "lease-1",
				LeaseToken: "token-1",
				Task:       engine.Task{ExecutionID: "exec-1", NodeName: "n"},
				NodeType:   tc.nodeType,
				Input:      &types.Input{Data: map[string]any{"results": marker}},
			}
			raw, err := marshalRedisLeaseMeta(lease)
			if err != nil {
				t.Fatalf("marshalRedisLeaseMeta() error = %v", err)
			}
			if got := strings.Contains(raw, marker); got != tc.keep {
				t.Fatalf("persisted record carries input = %v, want %v", got, tc.keep)
			}
			if lease.Input == nil {
				t.Fatal("marshalRedisLeaseMeta() cleared the caller's lease input")
			}
			decoded, err := unmarshalRedisLeaseMeta(raw, lease.Task)
			if err != nil {
				t.Fatalf("unmarshalRedisLeaseMeta() error = %v", err)
			}
			if decoded.LeaseID != lease.LeaseID || decoded.LeaseToken != lease.LeaseToken || decoded.NodeType != lease.NodeType {
				t.Fatalf("decoded identity = %+v, want the lease identity preserved", decoded)
			}
			if (decoded.Input != nil) != tc.keep {
				t.Fatalf("decoded input present = %v, want %v", decoded.Input != nil, tc.keep)
			}
		})
	}
}
