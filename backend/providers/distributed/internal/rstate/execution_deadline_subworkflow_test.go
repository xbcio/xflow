package rstate

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// deadlineChildResolver answers every sub-workflow reference with one compiled
// target; the cascade under test does not care how the child was found.
type deadlineChildResolver struct{ target graph.SubWorkflowTarget }

func (r deadlineChildResolver) ResolveSubWorkflow(context.Context, graph.SubWorkflowRef) (graph.SubWorkflowTarget, error) {
	return r.target, nil
}

// deadlineCompletionHooks records execution-complete announcements per id.
type deadlineCompletionHooks struct {
	engine.BaseHooks
	mu          sync.Mutex
	completions map[types.ExecutionID][]types.ExecutionStatus
}

func (h *deadlineCompletionHooks) OnExecutionComplete(_ context.Context, id types.ExecutionID, status types.ExecutionStatus) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.completions == nil {
		h.completions = make(map[types.ExecutionID][]types.ExecutionStatus)
	}
	h.completions[id] = append(h.completions[id], status)
}

func (h *deadlineCompletionHooks) completionsOf(id types.ExecutionID) []types.ExecutionStatus {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]types.ExecutionStatus(nil), h.completions[id]...)
}

// TestExpireExecutionCancelsTheSubWorkflowChildOnRedis runs the expiry cascade
// against the Redis store (miniredis) with a real Engine: the parent's
// settings.timeout is reached while its sub-workflow node waits on a child that
// never finishes. The parent ends timeout, the child is canceled by the
// expiry's own cascade, the parked node is closed with the child kept for the
// drill-down, the deadline entry is withdrawn, and the completion is announced
// once.
func TestExpireExecutionCancelsTheSubWorkflowChildOnRedis(t *testing.T) {
	ctx := context.Background()
	_, rdb, state := newMiniredisStore(t)

	child, err := graph.Compile(&types.WorkflowDef{
		Name:    "deadline-child",
		Version: "v1",
		Nodes:   []types.NodeDef{{Name: "work", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("Compile(child) error = %v", err)
	}
	resolver := deadlineChildResolver{target: graph.SubWorkflowTarget{
		Version: "v1", DefinitionHash: "sha256:child", Graph: child,
	}}
	parent, err := graph.CompileWithResolver(ctx, &types.WorkflowDef{
		Name:     "deadline-parent",
		Version:  "v1",
		Settings: &types.WorkflowSettings{Timezone: "UTC", Timeout: 300 * time.Millisecond},
		Nodes: []types.NodeDef{{
			Name:       "sub",
			Type:       types.SubWorkflowNodeType,
			Parameters: map[string]any{"workflow": "child"},
		}},
	}, resolver)
	if err != nil {
		t.Fatalf("CompileWithResolver(parent) error = %v", err)
	}

	hooks := &deadlineCompletionHooks{}
	queue := &StoreTestQueue{}
	eng := engine.New(state, queue, engine.WithHooks(hooks), engine.WithSubWorkflowResolver(resolver))

	parentID, err := eng.Submit(ctx, parent, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if err := eng.FlushOutbox(ctx, parentID); err != nil {
		t.Fatalf("FlushOutbox() error = %v", err)
	}
	// The sub-workflow node is engine-owned: its task reaches the queue like any
	// other and is consumed by the engine's system-task handler, which starts the
	// child and parks the node on it.
	if len(queue.tasks) != 1 || queue.tasks[0].Type != engine.TaskTypeSubWorkflow {
		t.Fatalf("queued tasks = %+v, want exactly the sub-workflow task", queue.tasks)
	}
	if handled, err := eng.HandleSystemTask(ctx, queue.tasks[0]); err != nil || !handled {
		t.Fatalf("HandleSystemTask(sub-workflow) = (%v, %v), want handled", handled, err)
	}

	link, found, err := state.GetSubWorkflowLink(ctx, parentID, "sub")
	if err != nil || !found {
		t.Fatalf("GetSubWorkflowLink() = (%+v, %v, %v), want the node parked on its child", link, found, err)
	}
	childSnap, err := state.GetExecution(ctx, link.ChildExecutionID)
	if err != nil || childSnap == nil || childSnap.Status != types.ExecutionStatusRunning {
		t.Fatalf("child before the expiry = (%+v, %v), want running", childSnap, err)
	}

	snap, err := state.GetExecution(ctx, parentID)
	if err != nil || snap == nil || snap.Deadline.IsZero() {
		t.Fatalf("parent = (%+v, %v), want an execution carrying its deadline", snap, err)
	}
	if !inIndex(t, rdb, namespace.Default, parentID) {
		t.Fatal("the parent is not in the deadline index before its expiry; the test proves nothing")
	}
	// The deadline is wall-clock; the engine checks it against time.Now().
	for time.Now().Before(snap.Deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	outcome, err := eng.ExpireExecution(ctx, parentID)
	if err != nil || outcome != engine.ExpireOutcomeExpired {
		t.Fatalf("ExpireExecution = (%q, %v), want expired", outcome, err)
	}

	after, err := state.GetExecution(ctx, parentID)
	if err != nil || after == nil || after.Status != types.ExecutionStatusTimeout || after.Error != engine.ExecutionDeadlineExceededError {
		t.Fatalf("parent after the expiry = (%+v, %v), want timeout %q", after, err, engine.ExecutionDeadlineExceededError)
	}
	childAfter, err := state.GetExecution(ctx, link.ChildExecutionID)
	if err != nil || childAfter == nil || childAfter.Status != types.ExecutionStatusCanceled {
		t.Fatalf("child after the expiry = (%+v, %v), want canceled by the cascade", childAfter, err)
	}
	node, err := state.GetNode(ctx, parentID, "sub")
	if err != nil || node == nil || node.Status != types.NodeStatusCanceled || node.ChildExecutionID != link.ChildExecutionID {
		t.Fatalf("parked node after the expiry = (%+v, %v), want canceled with child %q kept", node, err, link.ChildExecutionID)
	}
	if inIndex(t, rdb, namespace.Default, parentID) {
		t.Fatal("the deadline entry was not acknowledged after the cascade")
	}

	// A second call finds the execution already timed out for its deadline: it
	// repeats the idempotent cascade, must not announce the timeout again, and
	// must not move the already-canceled child.
	if outcome, err := eng.ExpireExecution(ctx, parentID); err != nil || outcome != engine.ExpireOutcomeResumed {
		t.Fatalf("second ExpireExecution = (%q, %v), want resumed", outcome, err)
	}
	if got := hooks.completionsOf(parentID); len(got) != 1 || got[0] != types.ExecutionStatusTimeout {
		t.Fatalf("parent completion announcements = %v, want exactly [timeout]", got)
	}
	if again, err := state.GetExecution(ctx, link.ChildExecutionID); err != nil || again == nil || again.Status != types.ExecutionStatusCanceled {
		t.Fatalf("child after the repeated cascade = (%+v, %v), want still canceled", again, err)
	}
}
