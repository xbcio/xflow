package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// TestFAFDurableSubmissionIsRefusedBeforeStateAllocation locks the core escape
// hatch: only the SDK's direct local dispatcher may run a FAF graph. Both core
// entry points must refuse before creating a snapshot or outbox work.
func TestFAFDurableSubmissionIsRefusedBeforeStateAllocation(t *testing.T) {
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    "faf",
		Options: &types.WorkflowOptions{FAF: true},
		Nodes:   []types.NodeDef{{Name: "only", Type: "test.action", Kind: types.NodeKindAction}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	state := newFakeState()
	eng := New(state, &fakeQueue{})

	if id, err := eng.Submit(context.Background(), g, map[string]any{"ignored": true}); !errors.Is(err, ErrFAFRequiresDirectDispatch) || id != "" {
		t.Fatalf("Submit() = (%q, %v), want empty id and ErrFAFRequiresDirectDispatch", id, err)
	}
	if id, err := eng.Invoke(context.Background(), g, "only", nil); !errors.Is(err, ErrFAFRequiresDirectDispatch) || id != "" {
		t.Fatalf("Invoke() = (%q, %v), want empty id and ErrFAFRequiresDirectDispatch", id, err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.executions) != 0 {
		t.Fatalf("FAF durable submission created executions: %#v", state.executions)
	}
}
