package engine

import (
	"context"
	"testing"
	"time"

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
