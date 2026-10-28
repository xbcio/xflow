package local

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"
)

// deadlineChildResolver answers every sub-workflow reference with one compiled
// target, the way the engine tests' stub does. The local backend has no workflow
// registry wired into an Engine built by hand, and the cascade under test does
// not care how the child was found.
type deadlineChildResolver struct{ target graph.SubWorkflowTarget }

func (r deadlineChildResolver) ResolveSubWorkflow(context.Context, graph.SubWorkflowRef) (graph.SubWorkflowTarget, error) {
	return r.target, nil
}

// deadlineCascadeHooks records what the engine announces per execution and per
// node, so a test can say how many times a conclusion was announced rather than
// only what the final state is.
type deadlineCascadeHooks struct {
	engine.BaseHooks
	mu          sync.Mutex
	completions map[types.ExecutionID][]types.ExecutionStatus
	nodes       map[types.ExecutionID]map[string][]types.NodeStatus
}

func (h *deadlineCascadeHooks) OnExecutionComplete(_ context.Context, id types.ExecutionID, status types.ExecutionStatus) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.completions == nil {
		h.completions = make(map[types.ExecutionID][]types.ExecutionStatus)
	}
	h.completions[id] = append(h.completions[id], status)
}

func (h *deadlineCascadeHooks) OnNodeComplete(_ context.Context, id types.ExecutionID, name string, status types.NodeStatus) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.nodes == nil {
		h.nodes = make(map[types.ExecutionID]map[string][]types.NodeStatus)
	}
	if h.nodes[id] == nil {
		h.nodes[id] = make(map[string][]types.NodeStatus)
	}
	h.nodes[id][name] = append(h.nodes[id][name], status)
}

func (h *deadlineCascadeHooks) completionsOf(id types.ExecutionID) []types.ExecutionStatus {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]types.ExecutionStatus(nil), h.completions[id]...)
}

func (h *deadlineCascadeHooks) nodeCompletionsOf(id types.ExecutionID, name string) []types.NodeStatus {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]types.NodeStatus(nil), h.nodes[id][name]...)
}

// compileDeadlineParent compiles a parent holding settings.timeout whose only
// node is a sub-workflow node running child.
func compileDeadlineParent(t *testing.T, timeout time.Duration, child *graph.Graph) *graph.Graph {
	t.Helper()
	g, err := graph.CompileWithResolver(context.Background(), &types.WorkflowDef{
		Name:     "deadline-parent",
		Version:  "v1",
		Settings: &types.WorkflowSettings{Timezone: "UTC", Timeout: timeout},
		Nodes: []types.NodeDef{{
			Name:       "sub",
			Type:       types.SubWorkflowNodeType,
			Parameters: map[string]any{"workflow": "child"},
		}},
	}, deadlineChildResolver{target: graph.SubWorkflowTarget{
		Version:        "v1",
		DefinitionHash: "sha256:child",
		Graph:          child,
	}})
	if err != nil {
		t.Fatalf("CompileWithResolver(parent) error = %v", err)
	}
	return g
}

// compileDeadlineBlockedChild compiles a child with no budget of its own whose
// only node parks on a signal that never arrives: nothing but a cancel ends it.
func compileDeadlineBlockedChild(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    "deadline-child",
		Version: "v1",
		Nodes:   []types.NodeDef{{Name: "park", Type: "test.deadline_park"}},
	})
	if err != nil {
		t.Fatalf("Compile(child) error = %v", err)
	}
	return g
}

// TestBackendExpiryCancelsTheSubWorkflowChild runs the whole cascade on the
// in-memory backend with a real Engine: the parent's settings.timeout elapses
// while its sub-workflow node waits on a child that never finishes. The parent
// ends timeout, the child is canceled by the expiry's own cascade, the parked
// node is closed, and the completion announcement is made once.
func TestBackendExpiryCancelsTheSubWorkflowChild(t *testing.T) {
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.deadline_park", deadlineParkHandler{})
	b := New(WithConcurrency(2), WithRegistry(reg))
	hooks := &deadlineCascadeHooks{}
	parentGraph := compileDeadlineParent(t, 3*time.Second, compileDeadlineBlockedChild(t))
	eng := engine.New(b.State(), b.Queue(), engine.WithHooks(hooks),
		engine.WithSubWorkflowResolver(deadlineChildResolver{target: graph.SubWorkflowTarget{
			Version:        "v1",
			DefinitionHash: "sha256:child",
			Graph:          compileDeadlineBlockedChild(t),
		}}))
	dispatcher := execution.NewEmbeddedDispatcher(eng, b.registry)
	stop := b.bindHandlerWithOutbox(eng, dispatcher.HandleTask, 5*time.Millisecond)
	stopped := false
	defer func() {
		if !stopped {
			stop()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	parentID, err := eng.Submit(ctx, parentGraph, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	// The cascade is only worth testing against a child that is really running:
	// wait for the parent to park on it and for the child to suspend.
	var childID types.ExecutionID
	limit := time.Now().Add(2 * time.Second)
	for {
		link, found, err := b.state.GetSubWorkflowLink(ctx, parentID, "sub")
		if err != nil {
			t.Fatalf("GetSubWorkflowLink() error = %v", err)
		}
		if found {
			if suspended, err := b.state.ListSuspendedNodes(ctx, link.ChildExecutionID); err == nil && len(suspended) > 0 {
				childID = link.ChildExecutionID
				break
			}
		}
		if time.Now().After(limit) {
			t.Fatal("the child never reached its parked node before the parent's deadline")
		}
		time.Sleep(5 * time.Millisecond)
	}

	res, err := b.WaitDone(ctx, parentID)
	if err != nil {
		t.Fatalf("WaitDone(parent) error = %v", err)
	}
	if res.Status != types.ExecutionStatusTimeout || res.Error != engine.ExecutionDeadlineExceededError {
		t.Fatalf("parent = %q %q, want %q %q", res.Status, res.Error,
			types.ExecutionStatusTimeout, engine.ExecutionDeadlineExceededError)
	}

	// WaitDone returns at the status write, before the cascade; the index entry
	// goes after it, so an empty index is a finished expiry.
	waitForDeadlineIndexEmpty(t, b.state, 10*time.Second)

	child, err := b.state.GetExecution(ctx, childID)
	if err != nil || child == nil {
		t.Fatalf("GetExecution(child) = %+v, %v", child, err)
	}
	if child.Status != types.ExecutionStatusCanceled {
		t.Fatalf("child = %q %q, want canceled: the parent's expiry must cascade", child.Status, child.Error)
	}
	node, err := b.state.GetNode(ctx, parentID, "sub")
	if err != nil || node == nil {
		t.Fatalf("GetNode(sub) = %+v, %v", node, err)
	}
	if node.Status != types.NodeStatusCanceled {
		t.Fatalf("parked sub-workflow node = %q, want canceled by the expiry", node.Status)
	}
	if node.ChildExecutionID != childID {
		t.Fatalf("sub-workflow node child = %q, want %q kept", node.ChildExecutionID, childID)
	}

	// The child's own cancellation notifies the parent, and the parent is already
	// in timeout: that notification must conclude nothing a second time.
	time.Sleep(50 * time.Millisecond)
	if got := hooks.completionsOf(parentID); len(got) != 1 || got[0] != types.ExecutionStatusTimeout {
		t.Fatalf("parent completion announcements = %v, want exactly [timeout]", got)
	}
	// The node is closed by the expiry's own finalization, which writes the node
	// without a node-complete announcement. What must never appear is the child's
	// cancellation concluding the node as a result of its own (failed or success):
	// that would be a second conclusion of a node the expiry already closed.
	if got := hooks.nodeCompletionsOf(parentID, "sub"); len(got) > 1 || (len(got) == 1 && got[0] != types.NodeStatusCanceled) {
		t.Fatalf("sub-workflow node completion announcements = %v, want none or one canceled", got)
	}
	if got := hooks.completionsOf(childID); len(got) != 1 || got[0] != types.ExecutionStatusCanceled {
		t.Fatalf("child completion announcements = %v, want exactly [canceled]", got)
	}
	after, err := b.state.GetExecution(ctx, parentID)
	if err != nil || after == nil || after.Status != types.ExecutionStatusTimeout || after.Error != engine.ExecutionDeadlineExceededError {
		t.Fatalf("parent after the child's cancel notification = %+v, %v, want still timeout", after, err)
	}

	stop()
	stopped = true
}
