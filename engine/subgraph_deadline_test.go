package engine

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

func TestBatchDeadline(t *testing.T) {
	issued := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name    string
		issued  time.Time
		timeout time.Duration
		outer   time.Time
		want    time.Time
	}{
		{name: "node timeout only", issued: issued, timeout: time.Minute, want: issued.Add(time.Minute)},
		{name: "outer only", issued: issued, outer: issued.Add(time.Hour), want: issued.Add(time.Hour)},
		{name: "outer tighter", issued: issued, timeout: time.Hour, outer: issued.Add(time.Minute), want: issued.Add(time.Minute)},
		{name: "node tighter", issued: issued, timeout: time.Minute, outer: issued.Add(time.Hour), want: issued.Add(time.Minute)},
		{name: "neither", issued: issued},
		// No anchor means no node-timeout half; inventing one from now would
		// hand every rebuild a fresh budget.
		{name: "missing issue time", timeout: time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := batchDeadline(tc.issued, tc.timeout, tc.outer); !got.Equal(tc.want) {
				t.Errorf("batchDeadline = %v, want %v", got, tc.want)
			}
		})
	}
}

// timedBatchWorkflow is batchLeaseWorkflow with a declared map node timeout and
// extra engine options.
func timedBatchWorkflow(t *testing.T, timeout time.Duration, opts ...Option) (*Engine, *fakeState, *fakeQueue, types.ExecutionID) {
	t.Helper()
	def := &types.WorkflowDef{
		Name: "batch-deadline",
		Nodes: []types.NodeDef{
			{Name: "loop", Type: "xflow.map", Parameters: mapBodyParamsForTest(), Timeout: timeout},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue, opts...)
	testRegistries.Store(eng, &fakeRegistry{handlers: map[string]types.ActionHandler{
		"xflow.map": &loopHandler{},
	}})
	t.Cleanup(func() { testRegistries.Delete(eng) })
	id, err := eng.Submit(context.Background(), g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	return eng, state, queue, id
}

func parentIssuedAt(t *testing.T, state *fakeState, id types.ExecutionID) time.Time {
	t.Helper()
	node, err := state.GetNode(context.Background(), id, "loop")
	if err != nil || node == nil || node.LeaseIssuedAt.IsZero() {
		t.Fatalf("parent node = %+v err=%v, want a leased map node", node, err)
	}
	return node.LeaseIssuedAt
}

// A map node's timeout must bound its expansion, not only its own handler run:
// the batch lease carries the parent's budget measured from the parent lease's
// issue time.
func TestBuildSubgraphLeaseDeadlineFromParentTimeout(t *testing.T) {
	eng, state, queue, id := timedBatchWorkflow(t, 5*time.Minute)
	batches := drainBatchTasks(t, eng, queue)
	want := parentIssuedAt(t, state, id).Add(5 * time.Minute)

	_, payload, err := eng.BuildSubgraphLease(context.Background(), batches[0])
	if err != nil {
		t.Fatalf("BuildSubgraphLease: %v", err)
	}
	if !payload.Deadline.Equal(want) {
		t.Errorf("payload.Deadline = %v, want parent issue time + timeout = %v", payload.Deadline, want)
	}
}

func TestBuildSubgraphLeaseDeadlineTightenedByOuterDeadline(t *testing.T) {
	outer := time.Now().Add(time.Minute).UTC()
	eng, _, queue, _ := timedBatchWorkflow(t, time.Hour, WithOuterDeadline(outer))
	batches := drainBatchTasks(t, eng, queue)

	_, payload, err := eng.BuildSubgraphLease(context.Background(), batches[0])
	if err != nil {
		t.Fatalf("BuildSubgraphLease: %v", err)
	}
	if !payload.Deadline.Equal(outer) {
		t.Errorf("payload.Deadline = %v, want the tighter outer deadline %v", payload.Deadline, outer)
	}
}

func TestBuildSubgraphLeaseDeadlineOuterOnly(t *testing.T) {
	outer := time.Now().Add(time.Minute).UTC()
	// A negative timeout opts the node out of any bound, including the default.
	eng, _, queue, _ := timedBatchWorkflow(t, -1, WithOuterDeadline(outer))
	batches := drainBatchTasks(t, eng, queue)

	_, payload, err := eng.BuildSubgraphLease(context.Background(), batches[0])
	if err != nil {
		t.Fatalf("BuildSubgraphLease: %v", err)
	}
	if !payload.Deadline.Equal(outer) {
		t.Errorf("payload.Deadline = %v, want the outer deadline %v", payload.Deadline, outer)
	}
}

func TestBuildSubgraphLeaseNoDeadlineWhenUnbounded(t *testing.T) {
	eng, _, queue, _ := timedBatchWorkflow(t, -1)
	batches := drainBatchTasks(t, eng, queue)

	_, payload, err := eng.BuildSubgraphLease(context.Background(), batches[0])
	if err != nil {
		t.Fatalf("BuildSubgraphLease: %v", err)
	}
	if !payload.Deadline.IsZero() {
		t.Errorf("payload.Deadline = %v, want zero for an unbounded map node with no outer deadline", payload.Deadline)
	}
}

// An unconfigured timeout falls back to the engine default, exactly as a
// node's own ExecutionDeadline does.
func TestBuildSubgraphLeaseDeadlineUsesDefaultNodeTimeout(t *testing.T) {
	eng, state, queue, id := timedBatchWorkflow(t, 0, WithDefaultNodeTimeout(7*time.Minute))
	batches := drainBatchTasks(t, eng, queue)
	want := parentIssuedAt(t, state, id).Add(7 * time.Minute)

	_, payload, err := eng.BuildSubgraphLease(context.Background(), batches[0])
	if err != nil {
		t.Fatalf("BuildSubgraphLease: %v", err)
	}
	if !payload.Deadline.Equal(want) {
		t.Errorf("payload.Deadline = %v, want %v", payload.Deadline, want)
	}
}

// Recovery replays the same lease; deriving the deadline from now would give
// every response-loss replay a fresh full budget.
func TestRecoverTaskLeaseBatchDeadlineUsesOriginalIssueTime(t *testing.T) {
	eng, state, queue, id := timedBatchWorkflow(t, 5*time.Minute)
	batches := drainBatchTasks(t, eng, queue)
	want := parentIssuedAt(t, state, id).Add(5 * time.Minute)

	time.Sleep(20 * time.Millisecond)
	recovered, err := eng.RecoverTaskLease(context.Background(), batches[0])
	if err != nil {
		t.Fatalf("RecoverTaskLease: %v", err)
	}
	if got := recovered.SubgraphPayload.Deadline; !got.Equal(want) {
		t.Errorf("recovered Deadline = %v, want original issue time + timeout = %v", got, want)
	}
}

// The in-process path must carry the same bound to the body executor.
func TestExecuteBatchForwardsTheBatchDeadline(t *testing.T) {
	exec := newEchoBodyExecutor()
	eng, state, queue, id := timedBatchWorkflow(t, 5*time.Minute, WithBatchBodyExecutor(exec))
	batches := drainBatchTasks(t, eng, queue)
	want := parentIssuedAt(t, state, id).Add(5 * time.Minute)

	if err := eng.ExecuteBatch(context.Background(), batches[0]); err != nil {
		t.Fatalf("ExecuteBatch: %v", err)
	}
	reqs := exec.requests()
	if len(reqs) != 1 {
		t.Fatalf("body executor saw %d requests, want 1", len(reqs))
	}
	if !reqs[0].Deadline.Equal(want) {
		t.Errorf("BatchBodyRequest.Deadline = %v, want %v", reqs[0].Deadline, want)
	}
}
