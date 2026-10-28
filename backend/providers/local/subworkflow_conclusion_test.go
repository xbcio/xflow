package local

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"
)

// countedHandler counts how many times a node of its type really ran. The count
// is the observable the rerun defect shows up in: a child that is rebuilt under
// the same derived id executes its handlers a second time.
type countedHandler struct {
	nodeType string
	runs     atomic.Int32
	data     map[string]any
}

func (h *countedHandler) Descriptor() types.Descriptor { return types.Descriptor{Type: h.nodeType} }

func (h *countedHandler) Execute(context.Context, *types.Input) (*types.Output, error) {
	h.runs.Add(1)
	return &types.Output{Data: h.data}, nil
}

type fixedSubWorkflowResolver struct{ target graph.SubWorkflowTarget }

func (r fixedSubWorkflowResolver) ResolveSubWorkflow(context.Context, graph.SubWorkflowRef) (graph.SubWorkflowTarget, error) {
	return r.target, nil
}

// refusingParentCommitState refuses the parent's "sub" commit while armed. It
// stands in for the notifier dying between recording the child's conclusion and
// committing the parent: the conclusion is already durable (it is written
// before the commit), the parent is still parked, and nothing but the reclaim
// redelivery can finish it.
type refusingParentCommitState struct {
	*memoryState
	armed   atomic.Bool
	refused atomic.Int32
}

func (s *refusingParentCommitState) CommitNode(ctx context.Context, req engine.CommitNodeRequest) (engine.CommitNodeResult, error) {
	if s.armed.Load() && req.NodeName == "sub" {
		s.refused.Add(1)
		return engine.CommitNodeResult{}, errors.New("test: parent commit refused")
	}
	return s.memoryState.CommitNode(ctx, req)
}

// expireExecution removes every record of one execution, the state retention
// leaves behind once it has expired a terminal child.
func expireExecution(s *memoryState, id types.ExecutionID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.executions, id)
	prefix := string(id) + "/"
	deleteKeysWithPrefix(s.nodes, prefix)
	deleteKeysWithPrefix(s.outputs, prefix)
	deleteKeysWithPrefix(s.privateOutputs, prefix)
	deleteKeysWithPrefix(s.inDegrees, prefix)
	deleteKeysWithPrefix(s.activeIns, prefix)
	deleteKeysWithPrefix(s.advanced, prefix)
	deleteKeysWithPrefix(s.scheduled, prefix)
	delete(s.remaining, id)
	delete(s.failed, id)
	delete(s.outbox, id)
}

// TestSubWorkflowReclaimConcludesFromTheRecordedConclusionEndToEnd is the
// guarantee the conclusion exists for, driven through the real queue, the
// embedded dispatcher and the real handlers: the child runs once, its
// conclusion is recorded, its records expire while the parent is still parked,
// and the reclaim redelivery finishes the parent WITHOUT running the child's
// handler a second time.
func TestSubWorkflowReclaimConcludesFromTheRecordedConclusionEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	work := &countedHandler{nodeType: "test.sw_work", data: map[string]any{"total": float64(7)}}
	after := &countedHandler{nodeType: "test.sw_after", data: map[string]any{"done": true}}
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.sw_work", work)
	reg.RegisterGlobal("test.sw_after", after)

	childGraph, err := graph.Compile(&types.WorkflowDef{
		Name:    "child",
		Version: "v1",
		Nodes:   []types.NodeDef{{Name: "work", Type: "test.sw_work"}},
	})
	if err != nil {
		t.Fatalf("compile child: %v", err)
	}
	resolver := fixedSubWorkflowResolver{target: graph.SubWorkflowTarget{
		Version:        "v1",
		DefinitionHash: "sha256:child",
		Graph:          childGraph,
	}}
	parentGraph, err := graph.CompileWithResolver(ctx, &types.WorkflowDef{
		Name: "parent",
		Nodes: []types.NodeDef{
			{Name: "sub", Type: types.SubWorkflowNodeType, Parameters: map[string]any{"workflow": "child"}},
			{Name: "after", Type: "test.sw_after"},
		},
		Connections: types.Connections{
			"sub": {"main": {Targets: []types.Connection{{Node: "after"}}}},
		},
	}, resolver)
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	b := New(WithConcurrency(2), WithRegistry(reg))
	state := &refusingParentCommitState{memoryState: b.state}
	state.armed.Store(true)
	eng := engine.New(state, b.Queue(),
		engine.WithSubWorkflowResolver(resolver),
		engine.WithDefaultLeaseTTL(time.Minute))
	stop := b.Bind(eng)
	defer stop()

	parentID, err := eng.Submit(ctx, parentGraph, nil)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// The child concludes, its conclusion is recorded, and the notifier's
	// commit of the parent is refused: the crash window between the two writes.
	waitForMemoryCondition(t, 10*time.Second, func() bool { return state.refused.Load() > 0 })
	link, found, err := b.state.GetSubWorkflowLink(ctx, parentID, "sub")
	if err != nil || !found {
		t.Fatalf("GetSubWorkflowLink: found=%v err=%v, want the parked link", found, err)
	}
	childID := link.ChildExecutionID
	conclusion, found, err := b.state.GetSubWorkflowConclusion(ctx, parentID, "sub", childID)
	if err != nil || !found {
		t.Fatalf("GetSubWorkflowConclusion: found=%v err=%v, want the conclusion recorded before the commit", found, err)
	}
	if conclusion.Status != types.ExecutionStatusSuccess {
		t.Fatalf("recorded conclusion = %+v, want success", conclusion)
	}
	if n := work.runs.Load(); n != 1 {
		t.Fatalf("child handler ran %d times before the reclaim, want 1", n)
	}
	if node, _ := b.state.GetNode(ctx, parentID, "sub"); node == nil || node.Status != types.NodeStatusWaiting {
		t.Fatalf("parent node = %+v, want it still parked", node)
	}

	// Retention expires the child's records while the parent is still parked.
	expireExecution(b.state, childID)
	if snap, _ := b.state.GetExecution(ctx, childID); snap != nil {
		t.Fatal("the child execution survived the simulated expiry")
	}

	// The wait's lease expires and the sweeper reclaims it.
	state.armed.Store(false)
	expired, err := b.state.ListExpiredLeases(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("ListExpiredLeases: %v", err)
	}
	var target *engine.ExpiredLease
	for i := range expired {
		if expired[i].ExecutionID == parentID && expired[i].NodeName == "sub" {
			target = &expired[i]
		}
	}
	if target == nil {
		t.Fatalf("expired leases = %+v, want the parked sub node's", expired)
	}
	if reclaimed, err := eng.ReclaimLease(ctx, *target); err != nil || !reclaimed {
		t.Fatalf("ReclaimLease reclaimed=%v err=%v, want true/nil", reclaimed, err)
	}

	result, err := b.WaitDone(ctx, parentID)
	if err != nil {
		t.Fatalf("WaitDone: %v", err)
	}
	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("parent status = %s (%s), want success", result.Status, result.Error)
	}
	if n := work.runs.Load(); n != 1 {
		t.Fatalf("child handler ran %d times, want 1: the reclaim rebuilt and reran the child", n)
	}
	if n := after.runs.Load(); n != 1 {
		t.Fatalf("downstream handler ran %d times, want 1", n)
	}
	if snap, _ := b.state.GetExecution(ctx, childID); snap != nil {
		t.Fatalf("child execution %q exists again: the reclaim rebuilt it", childID)
	}
	output, err := b.state.GetOutput(ctx, parentID, "sub")
	if err != nil {
		t.Fatalf("GetOutput(sub): %v", err)
	}
	if got, ok := output["work"].(map[string]any); !ok || got["total"] != float64(7) {
		t.Fatalf("sub node output = %#v, want the recorded child output", output)
	}
}
