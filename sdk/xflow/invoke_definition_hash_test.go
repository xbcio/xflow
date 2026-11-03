package xflow

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend"
)

// TestInvokeRecordsTheRegistryDefinitionHash pins both resolution branches of
// Invoke: the live record and the archived payload a version pin substituted
// each report the hash of the record whose graph actually ran. The value is
// copied from the registry, not recomputed from the graph.
func TestInvokeRecordsTheRegistryDefinitionHash(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	defer eng.Stop()

	id, err := eng.AddWorkflow(ctx, pinVersionWorkflow("sdk-defhash", "v1", "only-v1"))
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}
	publishWorkflowVersion(t, ctx, eng.workflowRegistry, id, pinVersionWorkflow("sdk-defhash", "v2", "only-v2"))
	current, err := eng.workflowRegistry.GetWorkflow(ctx, id)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	versions, ok := eng.workflowRegistry.(backend.WorkflowVersionCapability)
	if !ok {
		t.Fatal("local workflow registry does not serve version history")
	}
	archived, err := versions.GetWorkflowVersion(ctx, id, "v1")
	if err != nil {
		t.Fatalf("GetWorkflowVersion(v1): %v", err)
	}
	if current.DefinitionHash == "" || archived.DefinitionHash == "" {
		t.Fatalf("fixture hashes live=%q archived=%q, want non-empty", current.DefinitionHash, archived.DefinitionHash)
	}

	cases := []struct {
		name string
		opts []InvokeOption
		want string
	}{
		{name: "live record", opts: nil, want: current.DefinitionHash},
		{name: "pinned archived record", opts: []InvokeOption{WithWorkflowVersion("v1")}, want: archived.DefinitionHash},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			execID, err := eng.Invoke(ctx, id, Start(), nil, tc.opts...)
			if err != nil {
				t.Fatalf("Invoke: %v", err)
			}
			detail, err := eng.Inspect(ctx, execID)
			if err != nil {
				t.Fatalf("Inspect: %v", err)
			}
			if detail.DefinitionHash != tc.want {
				t.Fatalf("execution definition_hash = %q, want the registry record's %q", detail.DefinitionHash, tc.want)
			}
		})
	}
}
