package xflow

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	enginecore "github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

func TestEngineControlAPIInspectAndCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error: %v", err)
	}
	defer eng.Stop()

	wf := Workflow("control-api")
	start := wf.Node("start", node.Start())
	wait := wf.Node("ApprovalWait", node.Wait("approval"))
	wf.Connect(start, wait)

	workflowID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error: %v", err)
	}
	id, err := eng.Invoke(ctx, workflowID, Start(), map[string]any{"ticket": "VULN-1"})
	if err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}

	detail := waitForNodeStatus(t, ctx, eng, id, "ApprovalWait", types.NodeStatusSuspended)
	if detail.ExecutionID != id {
		t.Fatalf("execution id = %q, want %q", detail.ExecutionID, id)
	}
	if detail.Nodes[0].Status != types.NodeStatusSuspended {
		t.Fatalf("node status = %q, want suspended", detail.Nodes[0].Status)
	}

	if err := eng.Cancel(ctx, id); err != nil {
		t.Fatalf("Cancel() error: %v", err)
	}

	canceled, err := eng.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("Inspect() after cancel error: %v", err)
	}
	if canceled.Status != types.ExecutionStatusCanceled {
		t.Fatalf("execution status = %q, want canceled", canceled.Status)
	}
}

// TestEngineControlAPIPauseResume drives the SDK hold end to end: an execution
// parked on a wait node is paused, reports the paused status an operator acts
// on, and comes back to running on resume.
//
// The node is left suspended on purpose — a paused execution's in-flight work is
// not stopped, so the interesting state is precisely one that still has work
// parked in it.
func TestEngineControlAPIPauseResume(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error: %v", err)
	}
	defer eng.Stop()

	wf := Workflow("control-pause")
	start := wf.Node("start", node.Start())
	wait := wf.Node("ApprovalWait", node.Wait("approval"))
	wf.Connect(start, wait)

	workflowID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error: %v", err)
	}
	id, err := eng.Invoke(ctx, workflowID, Start(), map[string]any{"ticket": "VULN-2"})
	if err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}
	waitForNodeStatus(t, ctx, eng, id, "ApprovalWait", types.NodeStatusSuspended)

	if err := eng.Pause(ctx, id); err != nil {
		t.Fatalf("Pause() error: %v", err)
	}
	paused, err := eng.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("Inspect() after pause error: %v", err)
	}
	if paused.Status != types.ExecutionStatusPaused {
		t.Fatalf("execution status after Pause() = %q, want paused", paused.Status)
	}

	if err := eng.Resume(ctx, id); err != nil {
		t.Fatalf("Resume() error: %v", err)
	}
	resumed, err := eng.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("Inspect() after resume error: %v", err)
	}
	if resumed.Status != types.ExecutionStatusRunning {
		t.Fatalf("execution status after Resume() = %q, want running", resumed.Status)
	}
}

// TestEngineControlAPIRerunFromNode drives a rerun end to end on the local
// backend: a canceled execution is derived from at the node it was parked on,
// the new execution picks the work up there while the upstream result is copied
// rather than re-run, and the execution it derived from is left as it was.
//
// The from-node is the suspended one because that is what makes the copy
// observable: if the rerun re-ran the start node, or failed to record the
// upstream result, this execution could not reach the wait node at all — its
// in-degree would never be satisfied.
func TestEngineControlAPIRerunFromNode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error: %v", err)
	}
	defer eng.Stop()

	wf := Workflow("control-rerun")
	start := wf.Node("start", node.Start())
	wait := wf.Node("ApprovalWait", node.Wait("approval"))
	wf.Connect(start, wait)

	workflowID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error: %v", err)
	}
	id, err := eng.Invoke(ctx, workflowID, Start(), map[string]any{"ticket": "VULN-3"})
	if err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}
	waitForNodeStatus(t, ctx, eng, id, "ApprovalWait", types.NodeStatusSuspended)
	if err := eng.Cancel(ctx, id); err != nil {
		t.Fatalf("Cancel() error: %v", err)
	}

	rerun, err := eng.Rerun(ctx, id, "ApprovalWait")
	if err != nil {
		t.Fatalf("Rerun() error: %v", err)
	}
	if rerun.ExecutionID == "" || rerun.ExecutionID == id {
		t.Fatalf("Rerun() ExecutionID = %q, want a new execution derived from %q", rerun.ExecutionID, id)
	}
	if rerun.Queued {
		t.Fatalf("Rerun() = %+v, want a started execution: the definition declares no limit", rerun)
	}

	// The rerun reaches the wait node again, which is the copy working: the
	// wait node's only predecessor was resolved from the parent's own result.
	detail := waitForNodeStatus(t, ctx, eng, rerun.ExecutionID, "ApprovalWait", types.NodeStatusSuspended)
	if detail.ParentID != id {
		t.Fatalf("rerun parent = %q, want %q", detail.ParentID, id)
	}
	if detail.RerunFrom != "ApprovalWait" {
		t.Fatalf("rerun from = %q, want %q", detail.RerunFrom, "ApprovalWait")
	}

	// The execution it derived from is untouched: a rerun is a new execution,
	// not a rewrite.
	parent, err := eng.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("Inspect(parent) error: %v", err)
	}
	if parent.Status != types.ExecutionStatusCanceled {
		t.Fatalf("parent status = %q, want it left at %q", parent.Status, types.ExecutionStatusCanceled)
	}

	// And the rerun is a live execution: the signal that would have resumed the
	// original now drives this one to a terminal success.
	if err := eng.Signal(ctx, rerun.ExecutionID, "approval", map[string]any{"approver": "alice"}); err != nil {
		t.Fatalf("Signal(rerun) error: %v", err)
	}
	result, err := eng.Wait(ctx, rerun.ExecutionID)
	if err != nil {
		t.Fatalf("Wait(rerun) error: %v", err)
	}
	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("rerun status = %q, want %q", result.Status, types.ExecutionStatusSuccess)
	}
}

// TestEngineControlAPIRerunGuards pins the request-level refusals the SDK owns:
// a from-node is required, and a successful execution is only a rerun source
// when the caller says so.
func TestEngineControlAPIRerunGuards(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error: %v", err)
	}
	defer eng.Stop()

	wf := Workflow("control-rerun-guards")
	start := wf.Node("start", node.Start())
	wait := wf.Node("ApprovalWait", node.Wait("approval"))
	wf.Connect(start, wait)

	workflowID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error: %v", err)
	}
	id, err := eng.Invoke(ctx, workflowID, Start(), map[string]any{"ticket": "VULN-4"})
	if err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}
	if _, err := eng.Rerun(ctx, id, ""); err == nil {
		t.Fatal("Rerun() with no from node returned no error")
	}
	// The execution is not terminal yet, so the status gate is what refuses it.
	if _, err := eng.Rerun(ctx, id, "ApprovalWait"); !errors.Is(err, enginecore.ErrExecutionNotRerunnable) {
		t.Fatalf("Rerun(a running execution) error = %v, want %v", err, enginecore.ErrExecutionNotRerunnable)
	}
	if _, err := eng.Rerun(ctx, id, "no-such-node"); !errors.Is(err, enginecore.ErrExecutionNotRerunnable) {
		t.Fatalf("Rerun(unknown node) error = %v, want the status gate first: %v", err, enginecore.ErrExecutionNotRerunnable)
	}

	// Drive it to success, then check the include-success gate.
	if err := eng.Signal(ctx, id, "approval", nil); err != nil {
		t.Fatalf("Signal() error: %v", err)
	}
	if _, err := eng.Wait(ctx, id); err != nil {
		t.Fatalf("Wait() error: %v", err)
	}
	if _, err := eng.Rerun(ctx, id, "ApprovalWait"); !errors.Is(err, enginecore.ErrExecutionNotRerunnable) {
		t.Fatalf("Rerun(a success) error = %v, want %v", err, enginecore.ErrExecutionNotRerunnable)
	}
	rerun, err := eng.Rerun(ctx, id, "ApprovalWait", RerunIncludeSuccess())
	if err != nil {
		t.Fatalf("Rerun(a success, include_success) error: %v", err)
	}
	if rerun.ExecutionID == "" || rerun.ExecutionID == id {
		t.Fatalf("Rerun() ExecutionID = %q, want a new execution", rerun.ExecutionID)
	}
}

func TestEngineControlAPIRevokePredeliveredSignal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error: %v", err)
	}
	defer eng.Stop()

	wf := Workflow("revoke-api")
	start := wf.Node("start", node.Start())
	blocker := &blockingHandler{release: make(chan struct{})}
	t.Cleanup(blocker.releaseNow)
	gate := wf.LocalNode("Gate", blocker)
	wait := wf.Node("ApprovalWait", node.Wait("approval"))
	wf.Connect(start, gate)
	wf.Connect(gate, wait)

	workflowID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error: %v", err)
	}
	id, err := eng.Invoke(ctx, workflowID, Start(), nil)
	if err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}

	if err := eng.Signal(ctx, id, "approval", map[string]any{"approver": "alice"}); err != nil {
		t.Fatalf("Signal() error: %v", err)
	}
	if err := eng.RevokeSignal(ctx, id, "approval"); err != nil {
		t.Fatalf("RevokeSignal() error: %v", err)
	}

	blocker.releaseNow()
	waitForNodeStatus(t, ctx, eng, id, "ApprovalWait", types.NodeStatusSuspended)
}

func TestEngineControlAPIWaitDurationResumes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error: %v", err)
	}
	defer eng.Stop()

	wf := Workflow("timer-api")
	start := wf.Node("start", node.Start())
	wait := wf.Node("Timer", node.WaitDuration("10ms"))
	done := wf.LocalNode("Done", &echoControlHandler{})
	wf.Connect(start, wait)
	wf.Connect(wait, done)

	workflowID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error: %v", err)
	}
	id, err := eng.Invoke(ctx, workflowID, Start(), map[string]any{"ticket": "VULN-2"})
	if err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}

	result, err := eng.Wait(ctx, id)
	if err != nil {
		t.Fatalf("Wait() error: %v", err)
	}
	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("status = %q, want success", result.Status)
	}
}

func TestEngineControlAPIApprovalTimeoutRoutes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error: %v", err)
	}
	defer eng.Stop()

	wf := Workflow("approval-timeout-api")
	start := wf.Node("start", node.Start())
	approval := wf.Node("SecurityApproval",
		node.Approval([]string{"sec-owner"}, node.ApprovalAny).WithTimeout("10ms", "reject"),
	)
	rejected := wf.LocalNode("Rejected", &echoControlHandler{})
	wf.Connect(start, approval)
	wf.Connect(approval.Output("rejected"), rejected)

	workflowID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error: %v", err)
	}
	id, err := eng.Invoke(ctx, workflowID, Start(), map[string]any{"ticket": "VULN-3"})
	if err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}

	result, err := eng.Wait(ctx, id)
	if err != nil {
		t.Fatalf("Wait() error: %v", err)
	}
	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("status = %q, want success", result.Status)
	}
	out := result.Output["Rejected"].(map[string]any)
	if out["reason"] != "timeout" {
		t.Fatalf("rejected reason = %v, want timeout", out["reason"])
	}
}

func TestAddWorkflowReturnsStableUUIDForSameDefinition(t *testing.T) {
	eng, err := NewLocal()
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Stop()

	wf := Workflow("wf")
	wf.Node("start", node.Start())
	first, err := eng.AddWorkflow(context.Background(), wf)
	if err != nil {
		t.Fatal(err)
	}
	second, err := eng.AddWorkflow(context.Background(), wf)
	if err != nil {
		t.Fatal(err)
	}
	if first == "" || first != second {
		t.Fatalf("ids = %q/%q, want same non-empty UUID", first, second)
	}
}

func TestInvokeStartCreatesExecution(t *testing.T) {
	eng, err := NewLocal()
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Stop()

	wf := Workflow("wf")
	wf.Node("start", node.Start())
	workflowID, err := eng.AddWorkflow(context.Background(), wf)
	if err != nil {
		t.Fatal(err)
	}
	execID, err := eng.Invoke(context.Background(), workflowID, Start(), map[string]any{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	if execID == "" {
		t.Fatal("empty execution id")
	}
}

func TestInvokeOptionsPassTraceMetadataToHandlerInput(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	recorder := &traceInputRecorder{seen: make(chan *types.Input, 1)}
	eng, err := NewLocal(WithNodes(recorder))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Stop()

	wf := Workflow("trace-input")
	start := wf.Node("start", node.Start())
	action := wf.LocalNode("record", recorder)
	wf.Connect(start, action)
	workflowID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatal(err)
	}
	execID, err := eng.Invoke(ctx, workflowID, Start(), nil, WithTraceID("trace-123"), WithSpanID("span-456"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Wait(ctx, execID); err != nil {
		t.Fatal(err)
	}

	select {
	case input := <-recorder.seen:
		if input.TraceID != "trace-123" {
			t.Fatalf("Input.TraceID = %q, want trace-123", input.TraceID)
		}
		if input.SpanID != "span-456" {
			t.Fatalf("Input.SpanID = %q, want span-456", input.SpanID)
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for handler input: %v", ctx.Err())
	}
}

type traceInputRecorder struct {
	seen chan *types.Input
}

func (h *traceInputRecorder) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.trace_input"}
}

func (h *traceInputRecorder) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	cp := *input
	h.seen <- &cp
	return &types.Output{Data: input.Data}, nil
}

type blockingHandler struct {
	release chan struct{}
	once    sync.Once
}

func (h *blockingHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.blocking"}
}

func (h *blockingHandler) Execute(ctx context.Context, input *types.Input) (*types.Output, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-h.release:
		return &types.Output{Data: input.Data}, nil
	}
}

func (h *blockingHandler) releaseNow() {
	h.once.Do(func() { close(h.release) })
}

type echoControlHandler struct{}

func (h *echoControlHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.echo_control"}
}

func (h *echoControlHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	return &types.Output{Data: input.Data}, nil
}

func waitForNodeStatus(t *testing.T, ctx context.Context, eng *Engine, id types.ExecutionID, nodeName string, status types.NodeStatus) enginecore.ExecutionDetail {
	t.Helper()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s to reach %s: %v", nodeName, status, ctx.Err())
		case <-ticker.C:
			detail, err := eng.Inspect(ctx, id, nodeName)
			if err != nil {
				t.Fatalf("Inspect() error: %v", err)
			}
			if len(detail.Nodes) == 1 && detail.Nodes[0].Status == status {
				return detail
			}
		}
	}
}
