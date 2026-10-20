package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// TestSubmissionWithIDLessDefinitionStaysFullyUnattributed pins the half-
// identity rule: a definition whose id is empty contributes NEITHER an id nor a
// version. Writing the version alone would produce a snapshot that names a
// version of no workflow, which no reader can resolve.
func TestSubmissionWithIDLessDefinitionStaysFullyUnattributed(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	eng := newTestEngine(t, state, &fakeQueue{}, &fakeRegistry{handlers: map[string]types.ActionHandler{}})

	g, err := graph.Compile(&types.WorkflowDef{
		Name:    "id-less",
		Version: "v9",
		Nodes:   []types.NodeDef{{Name: "start", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	subCtx := WithWorkflowDef(ctx, &types.WorkflowDef{Name: "id-less", Version: "v9"})
	id, err := eng.Submit(subCtx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	detail, err := eng.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if detail.WorkflowID != "" || detail.WorkflowVersion != "" {
		t.Fatalf("identity = (%q, %q), want both empty for a definition with no id",
			detail.WorkflowID, detail.WorkflowVersion)
	}
}
