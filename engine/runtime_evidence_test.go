package engine

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

func TestRuntimeEvidenceBufferNilNoOp(t *testing.T) {
	var b *RuntimeEvidenceBuffer
	// nil buffer must not panic and must not change behavior
	publishRuntimeEvidence(b, RuntimeEvidenceEvent{EventID: "e1", Type: RuntimeEvidenceCommit})
	if got := b.Dropped(); got != 0 {
		t.Fatalf("nil buffer Dropped() = %d, want 0", got)
	}
	// nil buffer Events() returns nil channel, which blocks forever
	select {
	case ev := <-b.Events():
		t.Fatalf("nil buffer Events() should block, got %+v", ev)
	case <-time.After(10 * time.Millisecond):
		// expected: nil channel blocks
	}
	_ = context.Background()
	_ = types.NodeStatusSuccess
}

func TestRuntimeEvidenceBufferPublishAndDrop(t *testing.T) {
	b := NewRuntimeEvidenceBuffer(2)
	publishRuntimeEvidence(b, RuntimeEvidenceEvent{EventID: "e1", Type: RuntimeEvidenceCommit})
	publishRuntimeEvidence(b, RuntimeEvidenceEvent{EventID: "e2", Type: RuntimeEvidenceCommit})
	publishRuntimeEvidence(b, RuntimeEvidenceEvent{EventID: "e3", Type: RuntimeEvidenceCommit}) // dropped

	if got := b.Dropped(); got != 1 {
		t.Fatalf("Dropped() = %d, want 1", got)
	}
	ev1 := <-b.Events()
	ev2 := <-b.Events()
	if ev1.EventID != "e1" || ev2.EventID != "e2" {
		t.Fatalf("got %q %q, want e1 e2", ev1.EventID, ev2.EventID)
	}
}

func TestRuntimeEvidenceBufferZeroCapacityPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic for capacity<=0")
		}
	}()
	NewRuntimeEvidenceBuffer(0)
}

func TestWithRuntimeEvidenceBufferWired(t *testing.T) {
	buf := NewRuntimeEvidenceBuffer(4)
	eng := &Engine{}
	WithRuntimeEvidenceBuffer(buf)(eng)
	if eng.evidenceBuffer != buf {
		t.Fatalf("engine evidenceBuffer not wired: got %v, want %v", eng.evidenceBuffer, buf)
	}
}

func TestWithRuntimeEvidenceBufferNilNoOp(t *testing.T) {
	// Absent option: default engine has nil buffer.
	eng := &Engine{}
	if eng.evidenceBuffer != nil {
		t.Fatalf("expected nil evidenceBuffer by default, got %v", eng.evidenceBuffer)
	}
	// Explicit nil assignment: WithRuntimeEvidenceBuffer(nil) does not set a buffer.
	WithRuntimeEvidenceBuffer(nil)(eng)
	if eng.evidenceBuffer != nil {
		t.Fatalf("expected nil evidenceBuffer after WithRuntimeEvidenceBuffer(nil), got %v", eng.evidenceBuffer)
	}
	// Verify through New constructor: absent option leaves buffer nil.
	eng2 := New(newFakeState(), &fakeQueue{})
	if eng2.evidenceBuffer != nil {
		t.Fatalf("expected nil evidenceBuffer without option, got %v", eng2.evidenceBuffer)
	}
	// Verify through New constructor: explicit nil option leaves buffer nil.
	eng3 := New(newFakeState(), &fakeQueue{}, WithRuntimeEvidenceBuffer(nil))
	if eng3.evidenceBuffer != nil {
		t.Fatalf("expected nil evidenceBuffer with explicit nil option, got %v", eng3.evidenceBuffer)
	}
}

func newTestEngineWithBuffer(t *testing.T, buf *RuntimeEvidenceBuffer) (*Engine, *fakeQueue) {
	t.Helper()
	state := newFakeState()
	queue := &fakeQueue{}
	return New(state, queue, WithRuntimeEvidenceBuffer(buf)), queue
}

func buildAcceptedLeaseForTest(t *testing.T, eng *Engine, queue *fakeQueue, nodeName string) *TaskLease {
	t.Helper()
	ctx := context.Background()
	def := &types.WorkflowDef{
		Name:  "commit-receipt-helper",
		Nodes: []types.NodeDef{{Name: nodeName, Type: "test.echo"}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile graph: %v", err)
	}
	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatalf("submit: %v", err)
	}
	tasks := queue.Drain()
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d: %v", len(tasks), taskNames(tasks))
	}
	lease, err := eng.BuildTaskLease(ctx, tasks[0])
	if err != nil {
		t.Fatalf("build lease: %v", err)
	}
	return lease
}

func TestCommitReceiptPublishedAfterCommitNode(t *testing.T) {
	buf := NewRuntimeEvidenceBuffer(8)
	eng, queue := newTestEngineWithBuffer(t, buf)
	lease := buildAcceptedLeaseForTest(t, eng, queue, "start")

	_, err := eng.CommitTaskResultWithOutcome(context.Background(), lease, TaskResult{
		Output: &types.Output{Data: map[string]any{"ok": true}},
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	var found bool
	for {
		select {
		case ev := <-buf.Events():
			if ev.Type == RuntimeEvidenceCommit && ev.Applied && ev.CommitOutcome == CommitOutcomeAccepted {
				found = true
				if ev.ExecutionID == "" || ev.NodeName == "" {
					t.Fatalf("commit event missing identity: %+v", ev)
				}
				if ev.ErrorSource != ErrorSourceUnclassified {
					t.Fatalf("success commit event source = %q, want unclassified", ev.ErrorSource)
				}
				if ev.Classified {
					t.Fatalf("success commit event should not be classified: %+v", ev)
				}
			}
		default:
			if !found {
				t.Fatalf("no accepted applied commit receipt published")
			}
			return
		}
	}
}

func TestCommitReceiptPublishedWithClassificationForSystemError(t *testing.T) {
	buf := NewRuntimeEvidenceBuffer(8)
	eng, queue := newTestEngineWithBuffer(t, buf)
	lease := buildAcceptedLeaseForTest(t, eng, queue, "start")

	_, err := eng.CommitTaskResultWithOutcome(context.Background(), lease, TaskResult{
		Error: types.NewPermanentError("test/code", "boom"),
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	var found bool
	for {
		select {
		case ev := <-buf.Events():
			if ev.Type == RuntimeEvidenceCommit {
				found = true
				if ev.ExecutionID == "" || ev.NodeName == "" {
					t.Fatalf("commit event missing identity: %+v", ev)
				}
				if ev.ErrorSource != ErrorSourceSystem {
					t.Fatalf("error commit event source = %q, want system", ev.ErrorSource)
				}
				if !ev.Classified {
					t.Fatalf("error commit event should be classified: %+v", ev)
				}
				if ev.ErrorKind != types.ErrorKindPermanent {
					t.Fatalf("error commit event kind = %q, want permanent", ev.ErrorKind)
				}
				if ev.Retryable == nil || *ev.Retryable {
					t.Fatalf("expected retryable=false: %+v", ev)
				}
				if ev.Permanent == nil || !*ev.Permanent {
					t.Fatalf("expected permanent=true: %+v", ev)
				}
				if ev.ErrorCode != "test/code" {
					t.Fatalf("error code = %q, want test/code", ev.ErrorCode)
				}
			}
		default:
			if !found {
				t.Fatalf("no commit receipt published for system error")
			}
			return
		}
	}
}

func TestCommitReceiptPublishedForErrorPort(t *testing.T) {
	buf := NewRuntimeEvidenceBuffer(8)
	eng, queue := newTestEngineWithBuffer(t, buf)
	ctx := context.Background()

	def := &types.WorkflowDef{
		Name: "error-port-receipt",
		Nodes: []types.NodeDef{
			{
				Name:    "start",
				Type:    "test.echo",
				Retry:   &types.RetrySettings{MaxAttempts: 1},
				OnError: "error_output",
			},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile graph: %v", err)
	}
	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatalf("submit: %v", err)
	}
	tasks := queue.Drain()
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	lease, err := eng.BuildTaskLease(ctx, tasks[0])
	if err != nil {
		t.Fatalf("build lease: %v", err)
	}

	_, err = eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{
		Output: &types.Output{Port: "error", Data: map[string]any{"error": "node returned error port"}},
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	var found bool
	for {
		select {
		case ev := <-buf.Events():
			if ev.Type == RuntimeEvidenceCommit {
				found = true
				if ev.ErrorSource != ErrorSourceErrorPort {
					t.Fatalf("error-port commit event source = %q, want error_port", ev.ErrorSource)
				}
				if ev.Classified {
					t.Fatalf("error-port commit event should not be classified: %+v", ev)
				}
				if ev.RoutePort != "error" {
					t.Fatalf("error-port commit event route port = %q, want error", ev.RoutePort)
				}
			}
		default:
			if !found {
				t.Fatalf("no commit receipt published for error port")
			}
			return
		}
	}
}
