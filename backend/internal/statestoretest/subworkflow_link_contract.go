package statestoretest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// SubWorkflowLinkStore is a backend that can run a sub-workflow wait: full
// state, the atomic transitions that tear a wait down the two ways production
// does (reclaim, retry reset), and the link capability itself.
type SubWorkflowLinkStore interface {
	engine.StateStore
	engine.AtomicStateStore
	engine.SubWorkflowLinkStore
}

// leasedSubWorkflowNode writes a node claimed by one lease and returns that
// lease, so each case starts from the exact status and identity it wants to
// fence.
func leasedSubWorkflowNode(t *testing.T, ctx context.Context, store SubWorkflowLinkStore, id types.ExecutionID, status types.NodeStatus) *engine.TaskLease {
	t.Helper()
	if err := store.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID:   id,
		Name:          "sub",
		NodeIdx:       0,
		Status:        status,
		LeaseID:       "sw-lease",
		LeaseToken:    "sw-token",
		Attempt:       1,
		ActivationID:  1,
		LeaseIssuedAt: time.Now().UTC().Truncate(time.Millisecond),
		LeaseTTL:      time.Minute,
	}); err != nil {
		t.Fatalf("UpsertNode() error = %v", err)
	}
	return &engine.TaskLease{
		LeaseID: "sw-lease", LeaseToken: "sw-token", Attempt: 1,
		Task: engine.Task{
			ExecutionID: id, NodeName: "sub", NodeIdx: 0,
			Type: engine.TaskTypeSubWorkflow, ActivationID: 1,
		},
	}
}

func contractSubWorkflowLink(childID types.ExecutionID) engine.SubWorkflowLink {
	return engine.SubWorkflowLink{
		ChildExecutionID: childID,
		ParentNode:       "sub",
		Generation:       2,
		Depth:            3,
		WaitDeadline:     time.Now().UTC().Add(5 * time.Minute).Truncate(time.Millisecond),
		LeaseID:          "sw-lease",
		LeaseToken:       "sw-token",
		Attempt:          1,
		ActivationID:     1,
	}
}

func subWorkflowIntent(id types.ExecutionID, childID types.ExecutionID) engine.OutboxEntry {
	return engine.OutboxEntry{
		ID: "subworkflow/" + string(childID),
		Task: engine.Task{
			ExecutionID: id, NodeName: "sub", NodeIdx: 0,
			Type: engine.TaskTypeSubWorkflow, ActivationID: 1,
		},
	}
}

// RunSubWorkflowLinkContract exercises the SubWorkflowLinkStore contract every
// backend implementing it must honour: the park transition and its fence, the
// link's round trip, replay refusal, and the link outliving both teardown paths.
func RunSubWorkflowLinkContract(t *testing.T, newStore func(*testing.T) SubWorkflowLinkStore) {
	ctx := context.Background()

	t.Run("ParksNodeAndStoresLink", func(t *testing.T) {
		store := newStore(t)
		id := engine.NewExecutionID()
		lease := leasedSubWorkflowNode(t, ctx, store, id, types.NodeStatusRunning)
		link := contractSubWorkflowLink("child-exec-happy")
		intent := subWorkflowIntent(id, "child-exec-happy")

		accepted, err := store.BeginSubWorkflowWait(ctx, lease, link, []engine.OutboxEntry{intent})
		if err != nil || !accepted {
			t.Fatalf("BeginSubWorkflowWait() accepted=%v err=%v, want true/nil", accepted, err)
		}
		node, err := store.GetNode(ctx, id, "sub")
		if err != nil || node == nil {
			t.Fatalf("GetNode(sub) node=%+v err=%v", node, err)
		}
		if node.Status != types.NodeStatusWaiting {
			t.Fatalf("node status = %q, want waiting", node.Status)
		}
		// The completing side commits under this identity, so a park that
		// cleared it would strand the node at Waiting forever.
		if node.LeaseID != lease.LeaseID || node.LeaseToken != lease.LeaseToken ||
			node.Attempt != lease.Attempt || node.ActivationID != lease.Task.ActivationID {
			t.Fatalf("parked node lost its lease identity: %+v", node)
		}
		got, ok, err := store.GetSubWorkflowLink(ctx, id, "sub")
		if err != nil || !ok {
			t.Fatalf("GetSubWorkflowLink() ok=%v err=%v, want true/nil", ok, err)
		}
		if got != link {
			t.Fatalf("link = %+v, want %+v", got, link)
		}
		entries, err := store.ListOutbox(ctx, id, time.Now().UTC().Add(time.Minute), 10)
		if err != nil {
			t.Fatalf("ListOutbox() error = %v", err)
		}
		found := false
		for _, entry := range entries {
			if entry.ID == intent.ID {
				found = true
				if entry.Task.Type != engine.TaskTypeSubWorkflow || entry.Task.NodeName != "sub" {
					t.Fatalf("delivered intent = %+v, want the sub-workflow task", entry.Task)
				}
			}
		}
		if !found {
			t.Fatalf("delivery intent %q was not written with the park: %+v", intent.ID, entries)
		}
	})

	t.Run("RefusesUnfencedParks", func(t *testing.T) {
		cases := []struct {
			name   string
			status types.NodeStatus
			mutate func(*engine.TaskLease)
		}{
			{"wrong token", types.NodeStatusRunning, func(l *engine.TaskLease) { l.LeaseToken = "other" }},
			{"wrong attempt", types.NodeStatusRunning, func(l *engine.TaskLease) { l.Attempt = 7 }},
			{"wrong activation", types.NodeStatusRunning, func(l *engine.TaskLease) { l.Task.ActivationID = 9 }},
			{"unclaimed node", types.NodeStatusPending, func(*engine.TaskLease) {}},
			{"already waiting", types.NodeStatusWaiting, func(*engine.TaskLease) {}},
			{"terminal node", types.NodeStatusSuccess, func(*engine.TaskLease) {}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				store := newStore(t)
				id := engine.NewExecutionID()
				lease := leasedSubWorkflowNode(t, ctx, store, id, tc.status)
				tc.mutate(lease)

				accepted, err := store.BeginSubWorkflowWait(ctx, lease, contractSubWorkflowLink("child-exec-fenced"), nil)
				if err != nil || accepted {
					t.Fatalf("BeginSubWorkflowWait() accepted=%v err=%v, want false/nil", accepted, err)
				}
				if _, ok, _ := store.GetSubWorkflowLink(ctx, id, "sub"); ok {
					t.Fatal("a refused park wrote a link")
				}
				node, err := store.GetNode(ctx, id, "sub")
				if err != nil || node == nil {
					t.Fatalf("GetNode(sub) node=%+v err=%v", node, err)
				}
				if node.Status != tc.status {
					t.Fatalf("node status = %q, want it left at %q", node.Status, tc.status)
				}
			})
		}
	})

	t.Run("RefusesReplayWithoutReplacingTheLink", func(t *testing.T) {
		store := newStore(t)
		id := engine.NewExecutionID()
		lease := leasedSubWorkflowNode(t, ctx, store, id, types.NodeStatusRunning)
		first := contractSubWorkflowLink("child-exec-first")
		if accepted, err := store.BeginSubWorkflowWait(ctx, lease, first, nil); err != nil || !accepted {
			t.Fatalf("first park accepted=%v err=%v", accepted, err)
		}
		accepted, err := store.BeginSubWorkflowWait(ctx, lease, contractSubWorkflowLink("child-exec-second"), nil)
		if err != nil || accepted {
			t.Fatalf("replay accepted=%v err=%v, want false/nil", accepted, err)
		}
		if got, _, _ := store.GetSubWorkflowLink(ctx, id, "sub"); got != first {
			t.Fatalf("link after a refused replay = %+v, want the first %+v", got, first)
		}
	})

	t.Run("RefusesStructuralMisuse", func(t *testing.T) {
		store := newStore(t)
		id := engine.NewExecutionID()
		lease := leasedSubWorkflowNode(t, ctx, store, id, types.NodeStatusRunning)
		emptyChild := contractSubWorkflowLink("")
		wrongParent := contractSubWorkflowLink("child-exec-structural")
		wrongParent.ParentNode = "other"
		runs := map[string]func() (bool, error){
			"nil lease": func() (bool, error) {
				return store.BeginSubWorkflowWait(ctx, nil, contractSubWorkflowLink("c"), nil)
			},
			"empty child id": func() (bool, error) { return store.BeginSubWorkflowWait(ctx, lease, emptyChild, nil) },
			"parent mismatch": func() (bool, error) {
				return store.BeginSubWorkflowWait(ctx, lease, wrongParent, nil)
			},
			"empty intent id": func() (bool, error) {
				return store.BeginSubWorkflowWait(ctx, lease, contractSubWorkflowLink("c"), []engine.OutboxEntry{{}})
			},
		}
		for name, run := range runs {
			accepted, err := run()
			if !errors.Is(err, engine.ErrInvalidLeaseToken) || accepted {
				t.Fatalf("%s: accepted=%v err=%v, want false/ErrInvalidLeaseToken", name, accepted, err)
			}
		}
		if node, _ := store.GetNode(ctx, id, "sub"); node == nil || node.Status != types.NodeStatusRunning {
			t.Fatalf("a structurally invalid park changed the node: %+v", node)
		}
		if _, ok, _ := store.GetSubWorkflowLink(ctx, id, "sub"); ok {
			t.Fatal("a structurally invalid park wrote a link")
		}
	})

	t.Run("LinkSurvivesReclaim", func(t *testing.T) {
		store := newStore(t)
		id := engine.NewExecutionID()
		lease := leasedSubWorkflowNode(t, ctx, store, id, types.NodeStatusRunning)
		link := contractSubWorkflowLink("child-exec-reclaim")
		if accepted, err := store.BeginSubWorkflowWait(ctx, lease, link, nil); err != nil || !accepted {
			t.Fatalf("park accepted=%v err=%v", accepted, err)
		}
		revoked, err := store.RevokeLease(ctx, id, "sub", lease.LeaseToken)
		if err != nil || !revoked {
			t.Fatalf("RevokeLease() revoked=%v err=%v, want true/nil", revoked, err)
		}
		if node, _ := store.GetNode(ctx, id, "sub"); node == nil || node.Status != types.NodeStatusPending {
			t.Fatalf("node after revoke = %+v, want pending", node)
		}
		// The redelivery reads this link to resume the same child; losing it
		// would mint a second child for the same generation.
		if got, ok, _ := store.GetSubWorkflowLink(ctx, id, "sub"); !ok || got != link {
			t.Fatalf("link after a reclaim = %+v ok=%v, want the parked link", got, ok)
		}
	})

	t.Run("LinkSurvivesRetryReset", func(t *testing.T) {
		store := newStore(t)
		id := engine.NewExecutionID()
		lease := leasedSubWorkflowNode(t, ctx, store, id, types.NodeStatusRunning)
		link := contractSubWorkflowLink("child-exec-retry")
		if accepted, err := store.BeginSubWorkflowWait(ctx, lease, link, nil); err != nil || !accepted {
			t.Fatalf("park accepted=%v err=%v", accepted, err)
		}
		reset, err := store.ResetNodeForRetryWithOutbox(ctx, id, "sub", lease.LeaseToken, engine.OutboxEntry{
			ID: "retry/sub/1",
			Task: engine.Task{
				ExecutionID: id, NodeName: "sub", NodeIdx: 0,
				Type: engine.TaskTypeSubWorkflow, ActivationID: 1,
			},
		})
		if err != nil || !reset {
			t.Fatalf("ResetNodeForRetryWithOutbox() reset=%v err=%v, want true/nil", reset, err)
		}
		// The stale link is kept on purpose: the dispatch path compares the
		// task's generation against it to tell "resume this child" from "mint
		// the next one", and a cleared link would read as "no child yet".
		if got, ok, _ := store.GetSubWorkflowLink(ctx, id, "sub"); !ok || got != link {
			t.Fatalf("link after a retry reset = %+v ok=%v, want the stale link kept", got, ok)
		}
	})

	t.Run("AbsentLinkIsNotAnError", func(t *testing.T) {
		store := newStore(t)
		if _, ok, err := store.GetSubWorkflowLink(ctx, engine.NewExecutionID(), "sub"); ok || err != nil {
			t.Fatalf("GetSubWorkflowLink(absent) ok=%v err=%v, want false/nil", ok, err)
		}
	})
}
