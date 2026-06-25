package xflow

import (
	"context"
	"sync"
	"testing"
	"time"

	enginecore "github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/nodes/node"
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
	wait := wf.Node("ApprovalWait", node.Wait("approval"))
	_ = wait

	id, err := eng.Submit(ctx, wf, map[string]any{"ticket": "VULN-1"})
	if err != nil {
		t.Fatalf("Submit() error: %v", err)
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

func TestEngineControlAPIRevokePredeliveredSignal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error: %v", err)
	}
	defer eng.Stop()

	wf := Workflow("revoke-api")
	blocker := &blockingHandler{release: make(chan struct{})}
	t.Cleanup(blocker.releaseNow)
	gate := wf.LocalNode("Gate", blocker)
	wait := wf.Node("ApprovalWait", node.Wait("approval"))
	wf.Connect(gate, wait)

	id, err := eng.Submit(ctx, wf, nil)
	if err != nil {
		t.Fatalf("Submit() error: %v", err)
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
	wait := wf.Node("Timer", node.WaitDuration("10ms"))
	done := wf.LocalNode("Done", &echoControlHandler{})
	wf.Connect(wait, done)

	id, err := eng.Submit(ctx, wf, map[string]any{"ticket": "VULN-2"})
	if err != nil {
		t.Fatalf("Submit() error: %v", err)
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
	approval := wf.Node("SecurityApproval",
		node.Approval([]string{"sec-owner"}, node.ApprovalAny).WithTimeout("10ms", "reject"),
	)
	rejected := wf.LocalNode("Rejected", &echoControlHandler{})
	wf.Connect(approval.Output("rejected"), rejected)

	id, err := eng.Submit(ctx, wf, map[string]any{"ticket": "VULN-3"})
	if err != nil {
		t.Fatalf("Submit() error: %v", err)
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

type blockingHandler struct {
	release chan struct{}
	once    sync.Once
}

func (h *blockingHandler) Descriptor() node.Descriptor {
	return node.Descriptor{Type: "test.blocking"}
}

func (h *blockingHandler) Execute(ctx context.Context, input *node.Input) (*node.Output, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-h.release:
		return &node.Output{Data: input.Data}, nil
	}
}

func (h *blockingHandler) releaseNow() {
	h.once.Do(func() { close(h.release) })
}

type echoControlHandler struct{}

func (h *echoControlHandler) Descriptor() node.Descriptor {
	return node.Descriptor{Type: "test.echo_control"}
}

func (h *echoControlHandler) Execute(_ context.Context, input *node.Input) (*node.Output, error) {
	return &node.Output{Data: input.Data}, nil
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
