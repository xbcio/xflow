package local

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

// TestWorkflowRegistryGetWorkflowByKey asserts the by-key lookup returns the
// stored record. Used by Engine.AddWorkflow for legacy-hash reconciliation.
func TestWorkflowRegistryGetWorkflowByKey(t *testing.T) {
	reg := newWorkflowRegistry()
	ctx := context.Background()

	if _, err := reg.GetWorkflowByKey(ctx, "default/wf@v1"); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("GetWorkflowByKey missing = %v, want ErrWorkflowNotFound", err)
	}

	created, err := reg.AddWorkflow(ctx, backend.WorkflowRecord{Key: "default/wf@v1", DefinitionHash: "sha256:a"})
	if err != nil {
		t.Fatal(err)
	}

	got, err := reg.GetWorkflowByKey(ctx, "default/wf@v1")
	if err != nil {
		t.Fatalf("GetWorkflowByKey: %v", err)
	}
	if got.ID != created.ID || got.DefinitionHash != "sha256:a" {
		t.Fatalf("GetWorkflowByKey = id=%q hash=%q, want id=%q hash=sha256:a", got.ID, got.DefinitionHash, created.ID)
	}
}

// TestWorkflowRegistryUpdateDefinitionHash exercises the CAS contract.
func TestWorkflowRegistryUpdateDefinitionHash(t *testing.T) {
	reg := newWorkflowRegistry()
	ctx := context.Background()

	if err := reg.UpdateDefinitionHash(ctx, "missing", "sha256:old", "sha256:new"); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("UpdateDefinitionHash missing = %v, want ErrWorkflowNotFound", err)
	}

	created, err := reg.AddWorkflow(ctx, backend.WorkflowRecord{Key: "default/wf@v1", DefinitionHash: "sha256:old"})
	if err != nil {
		t.Fatal(err)
	}

	if err := reg.UpdateDefinitionHash(ctx, created.ID, "sha256:wrong", "sha256:new"); !errors.Is(err, backend.ErrWorkflowConflict) {
		t.Fatalf("UpdateDefinitionHash CAS mismatch = %v, want ErrWorkflowConflict", err)
	}

	if err := reg.UpdateDefinitionHash(ctx, created.ID, "sha256:old", "runtime-sha256:v1:new"); err != nil {
		t.Fatalf("UpdateDefinitionHash: %v", err)
	}

	got, err := reg.GetWorkflow(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if got.DefinitionHash != "runtime-sha256:v1:new" {
		t.Fatalf("DefinitionHash = %q, want runtime-sha256:v1:new", got.DefinitionHash)
	}
}

// TestWorkflowRegistryUpdateDefinitionHashConcurrent asserts two concurrent
// upgrades against the same expectedOldHash cannot both succeed. The loser
// must see ErrWorkflowConflict.
func TestWorkflowRegistryUpdateDefinitionHashConcurrent(t *testing.T) {
	reg := newWorkflowRegistry()
	ctx := context.Background()

	created, err := reg.AddWorkflow(ctx, backend.WorkflowRecord{Key: "default/wf@v1", DefinitionHash: "sha256:old"})
	if err != nil {
		t.Fatal(err)
	}

	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- reg.UpdateDefinitionHash(ctx, created.ID, "sha256:old", "runtime-sha256:v1:winner") }()
	go func() { second <- reg.UpdateDefinitionHash(ctx, created.ID, "sha256:old", "runtime-sha256:v1:loser") }()

	err1 := <-first
	err2 := <-second
	if err1 == nil && err2 == nil {
		t.Fatalf("both concurrent upgrades succeeded; expected exactly one ErrWorkflowConflict")
	}
	if err1 != nil && !errors.Is(err1, backend.ErrWorkflowConflict) {
		t.Fatalf("first upgrade err = %v, want ErrWorkflowConflict or nil", err1)
	}
	if err2 != nil && !errors.Is(err2, backend.ErrWorkflowConflict) {
		t.Fatalf("second upgrade err = %v, want ErrWorkflowConflict or nil", err2)
	}

	got, err := reg.GetWorkflow(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if got.DefinitionHash != "runtime-sha256:v1:winner" && got.DefinitionHash != "runtime-sha256:v1:loser" {
		t.Fatalf("DefinitionHash = %q after concurrent upgrade", got.DefinitionHash)
	}
}
