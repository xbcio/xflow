package memory

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/backend"
)

func TestWorkflowRegistryAddIsIdempotentByKeyAndHash(t *testing.T) {
	reg := newWorkflowRegistry()
	ctx := context.Background()
	rec := backend.WorkflowRecord{Key: "default/wf@v1", DefinitionHash: "sha256:a"}

	first, err := reg.AddWorkflow(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}
	second, err := reg.AddWorkflow(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == "" || first.ID != second.ID {
		t.Fatalf("IDs = %q/%q, want same non-empty UUID", first.ID, second.ID)
	}
}

func TestWorkflowRegistryConflictsOnSameKeyDifferentHash(t *testing.T) {
	reg := newWorkflowRegistry()
	ctx := context.Background()
	_, err := reg.AddWorkflow(ctx, backend.WorkflowRecord{Key: "default/wf@v1", DefinitionHash: "sha256:a"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = reg.AddWorkflow(ctx, backend.WorkflowRecord{Key: "default/wf@v1", DefinitionHash: "sha256:b"})
	if !errors.Is(err, backend.ErrWorkflowConflict) {
		t.Fatalf("err = %v, want ErrWorkflowConflict", err)
	}
}
