package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// compileHashGraph compiles the one-node shape every definition-hash case
// submits.
func compileHashGraph(t *testing.T, name string) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  name,
		Nodes: []types.NodeDef{{Name: "start", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

// TestSubmitRecordsDefinitionHash pins the submission half: the hash attached
// to the submission context is recorded verbatim on the snapshot, and the
// inspect projection reports it.
func TestSubmitRecordsDefinitionHash(t *testing.T) {
	state := newFakeState()
	eng := newTestEngine(t, state, &fakeQueue{}, &fakeRegistry{handlers: map[string]types.ActionHandler{}})

	g := compileHashGraph(t, "hash-identity")
	def := &types.WorkflowDef{ID: "wf-hash", Name: "hash-identity", Version: "v1"}
	ctx := WithDefinitionHash(WithWorkflowDef(context.Background(), def), "sha256:abc")

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	state.mu.Lock()
	snap := state.executions[id]
	state.mu.Unlock()
	if snap == nil {
		t.Fatalf("execution %q was not persisted", id)
	}
	if snap.DefinitionHash != "sha256:abc" {
		t.Fatalf("snapshot DefinitionHash = %q, want %q", snap.DefinitionHash, "sha256:abc")
	}

	detail, err := eng.Inspect(context.Background(), id)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if detail.DefinitionHash != "sha256:abc" {
		t.Fatalf("detail DefinitionHash = %q, want %q", detail.DefinitionHash, "sha256:abc")
	}
	statusOnly, err := eng.InspectStatus(context.Background(), id)
	if err != nil {
		t.Fatalf("InspectStatus() error = %v", err)
	}
	if statusOnly.DefinitionHash != detail.DefinitionHash {
		t.Fatalf("InspectStatus DefinitionHash = %q, want %q", statusOnly.DefinitionHash, detail.DefinitionHash)
	}
}

// TestSubmitWithoutWorkflowIDDropsDefinitionHash pins the half-identity rule
// for the hash: without a workflow id the hash cannot be resolved back to any
// registered workflow, so it is not recorded.
func TestSubmitWithoutWorkflowIDDropsDefinitionHash(t *testing.T) {
	state := newFakeState()
	eng := newTestEngine(t, state, &fakeQueue{}, &fakeRegistry{handlers: map[string]types.ActionHandler{}})

	g := compileHashGraph(t, "no-id")
	def := &types.WorkflowDef{Name: "no-id", Version: "v1"}
	ctx := WithDefinitionHash(WithWorkflowDef(context.Background(), def), "sha256:abc")

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	state.mu.Lock()
	snap := state.executions[id]
	state.mu.Unlock()
	if snap == nil {
		t.Fatalf("execution %q was not persisted", id)
	}
	if snap.DefinitionHash != "" {
		t.Fatalf("DefinitionHash = %q, want empty for a definition with no id", snap.DefinitionHash)
	}
}

// TestRerunInheritsParentDefinitionHash pins that a rerun records the hash of
// the graph it actually runs — the parent's — even when its context carries a
// different one, and that a rerun naming another workflow carries no hash.
func TestRerunInheritsParentDefinitionHash(t *testing.T) {
	g := rerunDiamond(t)
	state := newFakeState()
	parent := rerunParent(t, state, g, types.ExecutionStatusFailed, diamondResolved())
	state.mu.Lock()
	state.executions[parent].WorkflowID = "wf-rerun"
	state.executions[parent].WorkflowVersion = "v2"
	state.executions[parent].DefinitionHash = "sha256:parent"
	state.mu.Unlock()

	seeder := &seedRecorder{fakeState: state}
	eng := New(seeder, &fakeQueue{})

	// Same workflow id on the context: the parent is the authority on what is
	// being rerun, so its hash overrides the one the submission carried.
	ctx := WithDefinitionHash(
		WithWorkflowDef(context.Background(), &types.WorkflowDef{ID: "wf-rerun", Version: "v2"}),
		"sha256:ctx-other",
	)
	if _, err := eng.Rerun(ctx, RerunRequest{ExecutionID: parent, FromNode: "mid"}); err != nil {
		t.Fatalf("Rerun() error = %v", err)
	}
	seeded := seeder.lastRequest(t).Execution
	if seeded.WorkflowID != "wf-rerun" {
		t.Fatalf("seeded WorkflowID = %q, want %q", seeded.WorkflowID, "wf-rerun")
	}
	if seeded.DefinitionHash != "sha256:parent" {
		t.Fatalf("seeded DefinitionHash = %q, want the parent's %q", seeded.DefinitionHash, "sha256:parent")
	}

	// A different workflow id cannot reuse the parent's definition, and the
	// execution does not run the named definition either: the hash is cleared.
	otherCtx := WithDefinitionHash(
		WithWorkflowDef(context.Background(), &types.WorkflowDef{ID: "wf-other", Version: "v3"}),
		"sha256:ctx-other",
	)
	if _, err := eng.Rerun(otherCtx, RerunRequest{ExecutionID: parent, FromNode: "mid"}); err != nil {
		t.Fatalf("Rerun(other workflow) error = %v", err)
	}
	if len(seeder.requests) != 2 {
		t.Fatalf("SeedExecution() called %d times, want 2", len(seeder.requests))
	}
	mismatched := seeder.requests[1].Execution
	if mismatched.DefinitionHash != "" {
		t.Fatalf("DefinitionHash = %q, want empty when the rerun names a different workflow", mismatched.DefinitionHash)
	}
}

// TestSubWorkflowChildRecordsPinnedDefinitionHash pins the child half: the
// child records the hash the resolver pinned for the target it runs, not the
// parent's.
func TestSubWorkflowChildRecordsPinnedDefinitionHash(t *testing.T) {
	childGraph, err := graph.Compile(&types.WorkflowDef{
		ID:      "wf-child",
		Name:    "child",
		Version: "v1",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "work", Type: "xflow.noop"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "work"}}}},
		},
	})
	if err != nil {
		t.Fatalf("compile child graph: %v", err)
	}
	resolver := stubSubWorkflowResolver{target: graph.SubWorkflowTarget{
		Version:        "v1",
		DefinitionHash: "sha256:child",
		Graph:          childGraph,
	}}
	parent, err := graph.CompileWithResolver(context.Background(), subWorkflowParentDef(), resolver)
	if err != nil {
		t.Fatalf("CompileWithResolver: %v", err)
	}
	rig := newSubWorkflowRig(t, parent, nil, resolver)
	rig.dispatch(t, rig.task)

	childID := subWorkflowChildID(rig.parentID, "sub", 1, 1)
	child := rig.child(t, childID)
	if child.DefinitionHash != "sha256:child" {
		t.Fatalf("child DefinitionHash = %q, want %q", child.DefinitionHash, "sha256:child")
	}
	detail, err := rig.eng.Inspect(context.Background(), childID)
	if err != nil {
		t.Fatalf("Inspect(child): %v", err)
	}
	if detail.DefinitionHash != "sha256:child" {
		t.Fatalf("child detail DefinitionHash = %q, want %q", detail.DefinitionHash, "sha256:child")
	}
}
