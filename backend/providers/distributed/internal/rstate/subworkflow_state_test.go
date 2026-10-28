package rstate

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// leasedSubWorkflowNode writes a node claimed by one lease and returns that
// lease, so each case starts from the exact status/identity it wants to fence.
func leasedSubWorkflowNode(t *testing.T, ctx context.Context, state *Store, id types.ExecutionID, status types.NodeStatus) *engine.TaskLease {
	t.Helper()
	if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID:   id,
		Name:          "sub",
		NodeIdx:       0,
		Status:        status,
		LeaseID:       "sub-lease",
		LeaseToken:    "sub-token",
		Attempt:       1,
		ActivationID:  1,
		LeaseIssuedAt: time.Now().UTC().Truncate(time.Millisecond),
		LeaseTTL:      time.Minute,
	}); err != nil {
		t.Fatalf("UpsertNode() error = %v", err)
	}
	return &engine.TaskLease{
		LeaseID: "sub-lease", LeaseToken: "sub-token", Attempt: 1,
		Task: engine.Task{
			ExecutionID: id, NodeName: "sub", NodeIdx: 0,
			Type: engine.TaskTypeSubWorkflow, ActivationID: 1,
		},
	}
}

func subWorkflowCaseLink(id types.ExecutionID, childID types.ExecutionID) engine.SubWorkflowLink {
	return engine.SubWorkflowLink{
		ChildExecutionID: childID,
		ParentNode:       "sub",
		Generation:       1,
		Depth:            1,
		WaitDeadline:     time.Now().UTC().Add(5 * time.Minute).Truncate(time.Millisecond),
		LeaseID:          "sub-lease",
		LeaseToken:       "sub-token",
		Attempt:          1,
		ActivationID:     1,
	}
}

func TestRedisBeginSubWorkflowWaitParksNodeAndStoresLink(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("rstate-sub-wait-happy")
	lease := leasedSubWorkflowNode(t, ctx, state, id, types.NodeStatusRunning)
	link := subWorkflowCaseLink(id, "child-exec-1")

	accepted, err := state.BeginSubWorkflowWait(ctx, lease, link, []engine.OutboxEntry{{
		ID: "subworkflow/child-exec-1",
		Task: engine.Task{
			ExecutionID: id, NodeName: "sub", NodeIdx: 0,
			Type: engine.TaskTypeSubWorkflow,
		},
	}})
	if err != nil || !accepted {
		t.Fatalf("BeginSubWorkflowWait() accepted=%v err=%v, want true/nil", accepted, err)
	}

	node, err := state.GetNode(ctx, id, "sub")
	if err != nil || node == nil {
		t.Fatalf("GetNode(sub) node=%+v err=%v", node, err)
	}
	if node.Status != types.NodeStatusWaiting {
		t.Fatalf("node status = %q, want waiting", node.Status)
	}
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
	// The link key shares the execution's retention TTL, so a wait cannot
	// outlive the records it is read from.
	if ttl, err := rdb.TTL(ctx, subWorkflowLinkKey(namespace.Default, id, "sub")).Result(); err != nil || ttl <= 0 {
		t.Fatalf("link TTL = %v err=%v, want a positive expiry", ttl, err)
	}
	// The delivery intent must be discoverable: body written and scored due.
	if _, err := rdb.HGet(ctx, outboxBodyKey(namespace.Default, id), "subworkflow/child-exec-1").Result(); err != nil {
		t.Fatalf("outbox body missing: %v", err)
	}
	if _, err := rdb.ZScore(ctx, outboxReadyKey(namespace.Default, id), "subworkflow/child-exec-1").Result(); err != nil {
		t.Fatalf("outbox ready score missing: %v", err)
	}
}

func TestRedisBeginSubWorkflowWaitFences(t *testing.T) {
	ctx := context.Background()
	id := types.ExecutionID("rstate-sub-wait-fence")

	cases := []struct {
		name   string
		status types.NodeStatus
		mutate func(*engine.TaskLease)
	}{
		{"wrong token", types.NodeStatusRunning, func(l *engine.TaskLease) { l.LeaseToken = "other" }},
		{"wrong attempt", types.NodeStatusRunning, func(l *engine.TaskLease) { l.Attempt = 7 }},
		{"wrong activation", types.NodeStatusRunning, func(l *engine.TaskLease) { l.Task.ActivationID = 9 }},
		{"pending node", types.NodeStatusPending, func(*engine.TaskLease) {}},
		{"already waiting", types.NodeStatusWaiting, func(*engine.TaskLease) {}},
		{"terminal node", types.NodeStatusSuccess, func(*engine.TaskLease) {}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state, _, _ := newTestRedisState(t)
			lease := leasedSubWorkflowNode(t, ctx, state, id, tc.status)
			tc.mutate(lease)

			accepted, err := state.BeginSubWorkflowWait(ctx, lease, subWorkflowCaseLink(id, "child-exec-fence"), nil)
			if err != nil || accepted {
				t.Fatalf("BeginSubWorkflowWait() accepted=%v err=%v, want false/nil", accepted, err)
			}
			if _, ok, _ := state.GetSubWorkflowLink(ctx, id, "sub"); ok {
				t.Fatal("a refused park wrote a link")
			}
			node, err := state.GetNode(ctx, id, "sub")
			if err != nil || node == nil {
				t.Fatalf("GetNode(sub) node=%+v err=%v", node, err)
			}
			if node.Status != tc.status {
				t.Fatalf("node status = %q, want it left at %q", node.Status, tc.status)
			}
		})
	}

	t.Run("structural", func(t *testing.T) {
		state, _, _ := newTestRedisState(t)
		lease := leasedSubWorkflowNode(t, ctx, state, id, types.NodeStatusRunning)
		emptyChild := subWorkflowCaseLink(id, "")
		wrongParent := subWorkflowCaseLink(id, "child-exec-structural")
		wrongParent.ParentNode = "other"
		runs := map[string]func() (bool, error){
			"nil lease":      func() (bool, error) { return state.BeginSubWorkflowWait(ctx, nil, subWorkflowCaseLink(id, "c"), nil) },
			"empty child id": func() (bool, error) { return state.BeginSubWorkflowWait(ctx, lease, emptyChild, nil) },
			"parent mismatch": func() (bool, error) {
				return state.BeginSubWorkflowWait(ctx, lease, wrongParent, nil)
			},
			"empty entry id": func() (bool, error) {
				return state.BeginSubWorkflowWait(ctx, lease, subWorkflowCaseLink(id, "c"), []engine.OutboxEntry{{}})
			},
		}
		for name, run := range runs {
			accepted, err := run()
			if !errors.Is(err, engine.ErrInvalidLeaseToken) || accepted {
				t.Fatalf("%s: accepted=%v err=%v, want false/ErrInvalidLeaseToken", name, accepted, err)
			}
		}
		if node, _ := state.GetNode(ctx, id, "sub"); node == nil || node.Status != types.NodeStatusRunning {
			t.Fatalf("a structurally invalid park changed the node: %+v", node)
		}
	})
}

// TestRedisSubWorkflowLinkSurvivesReclaim pins the redelivery contract: the
// link outlives the revoked wait so a reclaimed dispatch resumes the same child.
func TestRedisSubWorkflowLinkSurvivesReclaim(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("rstate-sub-wait-reclaim")
	lease := leasedSubWorkflowNode(t, ctx, state, id, types.NodeStatusRunning)
	link := subWorkflowCaseLink(id, "child-exec-reclaim")
	if accepted, err := state.BeginSubWorkflowWait(ctx, lease, link, nil); err != nil || !accepted {
		t.Fatalf("park accepted=%v err=%v", accepted, err)
	}

	revoked, err := state.RevokeLease(ctx, id, "sub", lease.LeaseToken)
	if err != nil || !revoked {
		t.Fatalf("RevokeLease() revoked=%v err=%v, want true/nil", revoked, err)
	}
	if node, _ := state.GetNode(ctx, id, "sub"); node == nil || node.Status != types.NodeStatusPending {
		t.Fatalf("node after revoke = %+v, want pending", node)
	}
	if got, ok, _ := state.GetSubWorkflowLink(ctx, id, "sub"); !ok || got != link {
		t.Fatalf("link after a reclaim = %+v ok=%v, want the parked link", got, ok)
	}
}

func TestRedisGetSubWorkflowLinkAbsent(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	if _, ok, err := state.GetSubWorkflowLink(context.Background(), "no-such-exec", "sub"); ok || err != nil {
		t.Fatalf("GetSubWorkflowLink(absent) ok=%v err=%v, want false/nil", ok, err)
	}
}

// TestRedisSubWorkflowConclusionLivesInTheParentsSlot pins the key layout the
// recovery depends on: the conclusion shares the PARENT's hash tag (so it
// expires with the parent's records, not the child's), is keyed by child, and
// carries the parent's execution TTL from the moment it is written.
func TestRedisSubWorkflowConclusionLivesInTheParentsSlot(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("rstate-sub-conclusion-slot")
	conclusion := engine.SubWorkflowConclusion{
		ChildExecutionID: "child-exec-slot",
		Status:           types.ExecutionStatusSuccess,
		Output:           map[string]any{"work": map[string]any{"total": float64(7)}},
	}
	if err := state.RecordSubWorkflowConclusion(ctx, id, "sub", conclusion); err != nil {
		t.Fatalf("RecordSubWorkflowConclusion() error = %v", err)
	}

	key := subWorkflowConclusionKey(namespace.FromContext(ctx), id, "sub", conclusion.ChildExecutionID)
	if want := "{" + string(id) + "}"; !strings.Contains(key, want) {
		t.Fatalf("conclusion key %q does not carry the parent's hash tag %q", key, want)
	}
	if !strings.HasSuffix(key, ":conclusion:"+string(conclusion.ChildExecutionID)) {
		t.Fatalf("conclusion key %q is not keyed by child", key)
	}
	ttl, err := rdb.TTL(ctx, key).Result()
	if err != nil || ttl <= 0 {
		t.Fatalf("conclusion TTL = %v err=%v, want the parent's positive execution TTL", ttl, err)
	}
	if linkTTL := rdb.TTL(ctx, subWorkflowLinkKey(namespace.FromContext(ctx), id, "sub")).Val(); linkTTL > 0 {
		t.Fatalf("recording a conclusion created a link key (TTL %v): it must not touch the link", linkTTL)
	}
}

// TestRedisSubWorkflowConclusionRoundTripsACompressedOutput pins the codec seam:
// a child result large enough to be stored compressed is read back identically,
// and a store that has compression switched OFF still reads it, because the
// value — not the process configuration — says whether it is compressed.
func TestRedisSubWorkflowConclusionRoundTripsACompressedOutput(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	state.ConfigureOutputCompression(true)
	ctx := context.Background()
	id := types.ExecutionID("rstate-sub-conclusion-zstd")
	big := strings.Repeat("compressible child output ", 400)
	conclusion := engine.SubWorkflowConclusion{
		ChildExecutionID: "child-exec-zstd",
		Status:           types.ExecutionStatusSuccess,
		Output:           map[string]any{"work": map[string]any{"blob": big}},
	}
	if err := state.RecordSubWorkflowConclusion(ctx, id, "sub", conclusion); err != nil {
		t.Fatalf("RecordSubWorkflowConclusion() error = %v", err)
	}
	raw, err := rdb.Get(ctx, subWorkflowConclusionKey(namespace.FromContext(ctx), id, "sub", conclusion.ChildExecutionID)).Bytes()
	if err != nil {
		t.Fatalf("read the stored conclusion: %v", err)
	}
	if len(raw) >= len(big) {
		t.Fatalf("stored conclusion is %d bytes for a %d byte output: it was not compressed", len(raw), len(big))
	}

	state.ConfigureOutputCompression(false)
	got, ok, err := state.GetSubWorkflowConclusion(ctx, id, "sub", conclusion.ChildExecutionID)
	if err != nil || !ok {
		t.Fatalf("GetSubWorkflowConclusion() ok=%v err=%v, want true/nil", ok, err)
	}
	work, isMap := got.Output["work"].(map[string]any)
	if !isMap || work["blob"] != big {
		t.Fatalf("conclusion output did not survive the compressed round trip")
	}
}

// TestRedisSubWorkflowConclusionReadFailsLoudlyOnCorruption pins that an
// unreadable conclusion is an error and never "no conclusion": reporting
// absence would send the reclaim down the rebuild path and run the child again.
func TestRedisSubWorkflowConclusionReadFailsLoudlyOnCorruption(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("rstate-sub-conclusion-corrupt")
	key := subWorkflowConclusionKey(namespace.FromContext(ctx), id, "sub", "child-exec-corrupt")
	if err := rdb.Set(ctx, key, "{not json", time.Minute).Err(); err != nil {
		t.Fatalf("seed a corrupt conclusion: %v", err)
	}
	if _, ok, err := state.GetSubWorkflowConclusion(ctx, id, "sub", "child-exec-corrupt"); err == nil || ok {
		t.Fatalf("GetSubWorkflowConclusion(corrupt) ok=%v err=%v, want an error", ok, err)
	}
}
