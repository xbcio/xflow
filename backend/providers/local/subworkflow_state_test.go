package local

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// newSubWorkflowWaitState builds a running execution whose "sub" node holds an
// acquired lease: the state a sub-workflow dispatch parks from.
func newSubWorkflowWaitState(t *testing.T, id types.ExecutionID) (*memoryState, *engine.TaskLease) {
	t.Helper()
	ctx := context.Background()
	compiled, err := graph.Compile(&types.WorkflowDef{
		Name: "subworkflow-wait",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.start"},
			{Name: "sub", Type: types.SubWorkflowNodeType, Parameters: map[string]any{"workflow": "child"}},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "sub"}}}},
		},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	state := newMemoryState()
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: compiled, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	subIdx, _ := compiled.NodeIndex("sub")
	lease := transientOutputReclaimLease(id, "sub", subIdx, "sub-lease", "sub-token")
	lease.Task.Type = engine.TaskTypeSubWorkflow
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease(sub) acquired=%v err=%v, want true/nil", acquired, err)
	}
	lease.Attempt = 1
	return state, lease
}

func subWorkflowWaitLink(lease *engine.TaskLease, childID types.ExecutionID) engine.SubWorkflowLink {
	return engine.SubWorkflowLink{
		ChildExecutionID: childID,
		ParentNode:       lease.Task.NodeName,
		Generation:       1,
		Depth:            1,
		WaitDeadline:     time.Now().UTC().Add(5 * time.Minute),
		LeaseID:          lease.LeaseID,
		LeaseToken:       lease.LeaseToken,
		Attempt:          lease.Attempt,
		ActivationID:     lease.Task.ActivationID,
	}
}

func TestMemoryBeginSubWorkflowWaitParksNodeAndStoresLink(t *testing.T) {
	ctx := context.Background()
	id := types.ExecutionID("sub-wait-happy")
	state, lease := newSubWorkflowWaitState(t, id)
	link := subWorkflowWaitLink(lease, "child-exec-1")

	accepted, err := state.BeginSubWorkflowWait(ctx, lease, link, []engine.OutboxEntry{{
		ID: "subworkflow/child-exec-1",
		Task: engine.Task{
			ExecutionID: id, NodeName: "sub", NodeIdx: 1,
			Type: engine.TaskTypeSubWorkflow,
		},
	}})
	if err != nil || !accepted {
		t.Fatalf("BeginSubWorkflowWait() accepted=%v err=%v, want true/nil", accepted, err)
	}

	node, err := state.GetNode(ctx, id, "sub")
	if err != nil {
		t.Fatalf("GetNode(sub) error = %v", err)
	}
	if node.Status != types.NodeStatusWaiting {
		t.Fatalf("node status = %q, want waiting", node.Status)
	}
	// The lease identity is what the completing side commits under, so parking
	// must preserve it rather than clear it.
	if node.LeaseID != lease.LeaseID || node.LeaseToken != lease.LeaseToken || node.Attempt != lease.Attempt {
		t.Fatalf("parked node lost its lease identity: %+v", node)
	}
	got, ok, err := state.GetSubWorkflowLink(ctx, id, "sub")
	if err != nil || !ok {
		t.Fatalf("GetSubWorkflowLink() ok=%v err=%v, want true/nil", ok, err)
	}
	if got != link {
		t.Fatalf("link = %+v, want %+v", got, link)
	}
	if entry, ok := state.outbox[id]["subworkflow/child-exec-1"]; !ok {
		t.Fatal("delivery intent was not written with the park")
	} else if entry.entry.Task.Type != engine.TaskTypeSubWorkflow {
		t.Fatalf("intent task type = %d, want TaskTypeSubWorkflow", entry.entry.Task.Type)
	}
}

func TestMemoryBeginSubWorkflowWaitFences(t *testing.T) {
	ctx := context.Background()
	id := types.ExecutionID("sub-wait-fence")

	t.Run("WrongToken", func(t *testing.T) {
		state, lease := newSubWorkflowWaitState(t, id)
		bad := lease
		badCopy := *lease
		badCopy.LeaseToken = "other-token"
		bad = &badCopy

		accepted, err := state.BeginSubWorkflowWait(ctx, bad, subWorkflowWaitLink(bad, "child-exec-fence"), nil)
		if err != nil || accepted {
			t.Fatalf("BeginSubWorkflowWait(wrong token) accepted=%v err=%v, want false/nil", accepted, err)
		}
		node, _ := state.GetNode(ctx, id, "sub")
		if node.Status != types.NodeStatusRunning {
			t.Fatalf("node status = %q, want it left running", node.Status)
		}
		if _, ok, _ := state.GetSubWorkflowLink(ctx, id, "sub"); ok {
			t.Fatal("a refused park wrote a link")
		}
	})

	t.Run("NotClaimed", func(t *testing.T) {
		state := newMemoryState()
		if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID: id, Status: types.ExecutionStatusRunning,
		}); err != nil {
			t.Fatalf("CreateExecution() error = %v", err)
		}
		lease := transientOutputReclaimLease(id, "sub", 0, "sub-lease", "sub-token")
		accepted, err := state.BeginSubWorkflowWait(ctx, lease, subWorkflowWaitLink(lease, "child-exec-fence"), nil)
		if err != nil || accepted {
			t.Fatalf("BeginSubWorkflowWait(unclaimed node) accepted=%v err=%v, want false/nil", accepted, err)
		}
	})

	t.Run("AlreadyParked", func(t *testing.T) {
		state, lease := newSubWorkflowWaitState(t, id)
		first := subWorkflowWaitLink(lease, "child-exec-first")
		if accepted, err := state.BeginSubWorkflowWait(ctx, lease, first, nil); err != nil || !accepted {
			t.Fatalf("first park accepted=%v err=%v", accepted, err)
		}
		second := subWorkflowWaitLink(lease, "child-exec-second")
		accepted, err := state.BeginSubWorkflowWait(ctx, lease, second, nil)
		if err != nil || accepted {
			t.Fatalf("replay accepted=%v err=%v, want false/nil", accepted, err)
		}
		if got, _, _ := state.GetSubWorkflowLink(ctx, id, "sub"); got != first {
			t.Fatalf("link after a refused replay = %+v, want the first %+v", got, first)
		}
	})

	t.Run("Structural", func(t *testing.T) {
		state, lease := newSubWorkflowWaitState(t, id)
		wrongParent := subWorkflowWaitLink(lease, "child-exec-structural")
		wrongParent.ParentNode = "other"
		cases := []struct {
			name string
			run  func() (bool, error)
		}{
			{"nil lease", func() (bool, error) {
				return state.BeginSubWorkflowWait(ctx, nil, subWorkflowWaitLink(lease, "c"), nil)
			}},
			{"empty child id", func() (bool, error) {
				return state.BeginSubWorkflowWait(ctx, lease, subWorkflowWaitLink(lease, ""), nil)
			}},
			{"parent mismatch", func() (bool, error) { return state.BeginSubWorkflowWait(ctx, lease, wrongParent, nil) }},
			{"empty entry id", func() (bool, error) {
				return state.BeginSubWorkflowWait(ctx, lease, subWorkflowWaitLink(lease, "c"), []engine.OutboxEntry{{}})
			}},
		}
		for _, tc := range cases {
			accepted, err := tc.run()
			if !errors.Is(err, engine.ErrInvalidLeaseToken) || accepted {
				t.Fatalf("%s: accepted=%v err=%v, want false/ErrInvalidLeaseToken", tc.name, accepted, err)
			}
		}
		node, _ := state.GetNode(ctx, id, "sub")
		if node.Status != types.NodeStatusRunning {
			t.Fatalf("a structurally invalid park changed the node to %q", node.Status)
		}
	})
}

// TestMemorySubWorkflowLinkSurvivesReclaimAndRetry pins the property the link
// exists for: the wait can be torn down (reclaim) or restarted (retry) without
// losing which child the generation owns.
func TestMemorySubWorkflowLinkSurvivesReclaimAndRetry(t *testing.T) {
	ctx := context.Background()

	t.Run("Reclaim", func(t *testing.T) {
		id := types.ExecutionID("sub-wait-reclaim")
		state, lease := newSubWorkflowWaitState(t, id)
		link := subWorkflowWaitLink(lease, "child-exec-reclaim")
		if accepted, err := state.BeginSubWorkflowWait(ctx, lease, link, nil); err != nil || !accepted {
			t.Fatalf("park accepted=%v err=%v", accepted, err)
		}

		// Reclaim revokes the park and re-queues the task. The link must remain
		// so the redelivery resumes the same child instead of minting another.
		revoked, err := state.RevokeLease(ctx, id, "sub", lease.LeaseToken)
		if err != nil || !revoked {
			t.Fatalf("RevokeLease() revoked=%v err=%v, want true/nil", revoked, err)
		}
		if node, _ := state.GetNode(ctx, id, "sub"); node.Status != types.NodeStatusPending {
			t.Fatalf("node status after revoke = %q, want pending", node.Status)
		}
		if got, ok, _ := state.GetSubWorkflowLink(ctx, id, "sub"); !ok || got != link {
			t.Fatalf("link after a reclaim = %+v ok=%v, want the parked link", got, ok)
		}
	})

	t.Run("RetryReset", func(t *testing.T) {
		id := types.ExecutionID("sub-wait-retry")
		state, lease := newSubWorkflowWaitState(t, id)
		link := subWorkflowWaitLink(lease, "child-exec-retry")
		if accepted, err := state.BeginSubWorkflowWait(ctx, lease, link, nil); err != nil || !accepted {
			t.Fatalf("park accepted=%v err=%v", accepted, err)
		}

		// A retry resets the same parked node for the next attempt. The stale
		// link is expected to stay until the next generation parks over it — the
		// dispatch path compares generations to tell "resume this child" from
		// "mint the next".
		reset, err := state.ResetNodeForRetryWithOutbox(ctx, id, "sub", lease.LeaseToken, engine.OutboxEntry{
			ID:   "retry/sub/1",
			Task: engine.Task{ExecutionID: id, NodeName: "sub", Type: engine.TaskTypeSubWorkflow},
		})
		if err != nil || !reset {
			t.Fatalf("ResetNodeForRetryWithOutbox() reset=%v err=%v, want true/nil", reset, err)
		}
		if node, _ := state.GetNode(ctx, id, "sub"); node.Status != types.NodeStatusPending {
			t.Fatalf("node status after retry reset = %q, want pending", node.Status)
		}
		if got, ok, _ := state.GetSubWorkflowLink(ctx, id, "sub"); !ok || got != link {
			t.Fatalf("link after a retry reset = %+v ok=%v, want the stale link kept for the generation check", got, ok)
		}
	})
}

func TestMemoryGetSubWorkflowLinkAbsent(t *testing.T) {
	state := newMemoryState()
	if _, ok, err := state.GetSubWorkflowLink(context.Background(), "no-such-exec", "sub"); ok || err != nil {
		t.Fatalf("GetSubWorkflowLink(absent) ok=%v err=%v, want false/nil", ok, err)
	}
}
