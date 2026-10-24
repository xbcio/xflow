package engine

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

func TestBatchDeadlineClampsToExecutionDeadline(t *testing.T) {
	issued := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name    string
		timeout time.Duration
		outer   time.Time
		exec    time.Time
		want    time.Time
	}{
		{name: "earlier execution deadline wins", timeout: time.Hour, exec: issued.Add(time.Minute), want: issued.Add(time.Minute)},
		{name: "later execution deadline loses", timeout: time.Minute, exec: issued.Add(time.Hour), want: issued.Add(time.Minute)},
		{name: "unbounded node takes the execution deadline", exec: issued.Add(time.Minute), want: issued.Add(time.Minute)},
		{name: "execution deadline tighter than outer", outer: issued.Add(time.Hour), exec: issued.Add(time.Minute), want: issued.Add(time.Minute)},
		{name: "outer tighter than execution deadline", outer: issued.Add(time.Minute), exec: issued.Add(time.Hour), want: issued.Add(time.Minute)},
		{name: "no execution deadline keeps the old answer", timeout: time.Minute, want: issued.Add(time.Minute)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := batchDeadline(issued, tc.timeout, tc.outer, tc.exec); !got.Equal(tc.want) {
				t.Errorf("batchDeadline = %v, want %v", got, tc.want)
			}
		})
	}
}

// deadlineBatchWorkflow is timedBatchWorkflow over a graph that declares
// settings.timeout, with the stored execution deadline then pinned to an exact
// instant so a case can place it against the map node's own timeout.
func deadlineBatchWorkflow(t *testing.T, nodeTimeout time.Duration, execDeadline time.Time, opts ...Option) (*Engine, *fakeState, *fakeQueue, types.ExecutionID) {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:     "batch-exec-deadline",
		Settings: &types.WorkflowSettings{Timezone: "UTC", Timeout: time.Hour},
		Nodes: []types.NodeDef{
			{Name: "loop", Type: "xflow.map", Parameters: mapBodyParamsForTest(), Timeout: nodeTimeout},
		},
	})
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
	state.mu.Lock()
	state.executions[id].Deadline = execDeadline
	state.mu.Unlock()
	return eng, state, queue, id
}

func TestBuildSubgraphLeaseClampsToExecutionDeadline(t *testing.T) {
	ctx := context.Background()
	t.Run("an earlier execution deadline takes the smaller", func(t *testing.T) {
		deadline := time.Now().UTC().Add(30 * time.Second)
		eng, _, queue, _ := deadlineBatchWorkflow(t, time.Hour, deadline)
		batches := drainBatchTasks(t, eng, queue)
		_, payload, err := eng.BuildSubgraphLease(ctx, batches[0])
		if err != nil {
			t.Fatalf("BuildSubgraphLease: %v", err)
		}
		if !payload.Deadline.Equal(deadline) {
			t.Errorf("payload.Deadline = %v, want the execution deadline %v", payload.Deadline, deadline)
		}
	})
	t.Run("a later execution deadline keeps the node timeout", func(t *testing.T) {
		eng, state, queue, id := deadlineBatchWorkflow(t, 5*time.Minute, time.Now().UTC().Add(time.Hour))
		batches := drainBatchTasks(t, eng, queue)
		want := parentIssuedAt(t, state, id).Add(5 * time.Minute)
		_, payload, err := eng.BuildSubgraphLease(ctx, batches[0])
		if err != nil {
			t.Fatalf("BuildSubgraphLease: %v", err)
		}
		if !payload.Deadline.Equal(want) {
			t.Errorf("payload.Deadline = %v, want parent issue time + timeout %v", payload.Deadline, want)
		}
	})
	t.Run("an unlimited node takes the execution deadline", func(t *testing.T) {
		deadline := time.Now().UTC().Add(30 * time.Second)
		eng, _, queue, _ := deadlineBatchWorkflow(t, -1, deadline)
		batches := drainBatchTasks(t, eng, queue)
		_, payload, err := eng.BuildSubgraphLease(ctx, batches[0])
		if err != nil {
			t.Fatalf("BuildSubgraphLease: %v", err)
		}
		if !payload.Deadline.Equal(deadline) {
			t.Errorf("payload.Deadline = %v, want the execution deadline %v", payload.Deadline, deadline)
		}
	})
	t.Run("recovery replays the clamped bound", func(t *testing.T) {
		deadline := time.Now().UTC().Add(30 * time.Second)
		eng, _, queue, _ := deadlineBatchWorkflow(t, time.Hour, deadline)
		batches := drainBatchTasks(t, eng, queue)
		recovered, err := eng.RecoverTaskLease(ctx, batches[0])
		if err != nil {
			t.Fatalf("RecoverTaskLease: %v", err)
		}
		if got := recovered.SubgraphPayload.Deadline; !got.Equal(deadline) {
			t.Errorf("recovered Deadline = %v, want the execution deadline %v", got, deadline)
		}
	})
}

func TestExecuteBatchForwardsTheClampedDeadline(t *testing.T) {
	deadline := time.Now().UTC().Add(30 * time.Second)
	exec := newEchoBodyExecutor()
	eng, _, queue, _ := deadlineBatchWorkflow(t, time.Hour, deadline, WithBatchBodyExecutor(exec))
	batches := drainBatchTasks(t, eng, queue)

	if err := eng.ExecuteBatch(context.Background(), batches[0]); err != nil {
		t.Fatalf("ExecuteBatch: %v", err)
	}
	reqs := exec.requests()
	if len(reqs) != 1 {
		t.Fatalf("body executor saw %d requests, want 1", len(reqs))
	}
	if !reqs[0].Deadline.Equal(deadline) {
		t.Errorf("BatchBodyRequest.Deadline = %v, want the execution deadline %v", reqs[0].Deadline, deadline)
	}
}

// subWorkflowDeadlineRig dispatches a sub-workflow node of a parent that
// declares settings.timeout, with the stored execution deadline pinned.
func subWorkflowDeadlineRig(t *testing.T, nodeTimeout time.Duration, execDeadline time.Time) *subWorkflowRig {
	t.Helper()
	def := subWorkflowParentDef()
	def.Settings = &types.WorkflowSettings{Timezone: "UTC", Timeout: time.Hour}
	def.Nodes[0].Timeout = nodeTimeout
	resolver := subWorkflowStubResolver(t)
	g, err := graph.CompileWithResolver(context.Background(), def, resolver)
	if err != nil {
		t.Fatalf("CompileWithResolver: %v", err)
	}
	rig := newSubWorkflowRig(t, g, nil, resolver)
	rig.state.mu.Lock()
	rig.state.executions[rig.parentID].Deadline = execDeadline
	rig.state.mu.Unlock()
	return rig
}

func TestSubWorkflowWaitEndClampsToExecutionDeadline(t *testing.T) {
	t.Run("an earlier execution deadline ends the wait", func(t *testing.T) {
		deadline := time.Now().UTC().Add(20 * time.Second)
		rig := subWorkflowDeadlineRig(t, time.Hour, deadline)
		rig.dispatch(t, rig.task)
		if got := rig.link(t).WaitDeadline; !got.Equal(deadline) {
			t.Fatalf("WaitDeadline = %v, want the execution deadline %v", got, deadline)
		}
	})
	t.Run("a later execution deadline keeps the node timeout", func(t *testing.T) {
		rig := subWorkflowDeadlineRig(t, time.Minute, time.Now().UTC().Add(time.Hour))
		rig.dispatch(t, rig.task)
		want := rig.node(t).LeaseIssuedAt.Add(time.Minute)
		if got := rig.link(t).WaitDeadline; !got.Equal(want) {
			t.Fatalf("WaitDeadline = %v, want lease issue time + node timeout %v", got, want)
		}
	})
	t.Run("an unlimited node still ends with its execution", func(t *testing.T) {
		deadline := time.Now().UTC().Add(20 * time.Second)
		rig := subWorkflowDeadlineRig(t, -1, deadline)
		rig.dispatch(t, rig.task)
		if got := rig.link(t).WaitDeadline; !got.Equal(deadline) {
			t.Fatalf("WaitDeadline = %v, want the execution deadline %v", got, deadline)
		}
	})
	t.Run("no execution deadline matches the old wait end", func(t *testing.T) {
		rig := subWorkflowDeadlineRig(t, time.Minute, time.Time{})
		rig.dispatch(t, rig.task)
		want := rig.node(t).LeaseIssuedAt.Add(time.Minute)
		if got := rig.link(t).WaitDeadline; !got.Equal(want) {
			t.Fatalf("WaitDeadline = %v, want lease issue time + node timeout %v", got, want)
		}
	})
}
