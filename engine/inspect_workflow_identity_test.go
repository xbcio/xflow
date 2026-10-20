package engine

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// TestInspectProjectsWorkflowIdentityFromSubmission pins the submission half of
// B2a: an execution started with a definition attached to its context reports
// that definition's id and version, and the status-only read reports them too
// (GET /v1/executions/{id} projects both through InspectStatus).
func TestInspectProjectsWorkflowIdentityFromSubmission(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	eng := newTestEngine(t, state, &fakeQueue{}, &fakeRegistry{handlers: map[string]types.ActionHandler{}})

	g, err := graph.Compile(&types.WorkflowDef{
		Name:    "audit-identity",
		Version: "v7",
		Nodes:   []types.NodeDef{{Name: "start", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	def := &types.WorkflowDef{ID: "wf-identity", Name: "audit-identity", Version: "v7"}
	subCtx := WithWorkflowDef(ctx, def)

	id, err := eng.Submit(subCtx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	detail, err := eng.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if detail.WorkflowID != "wf-identity" {
		t.Fatalf("WorkflowID = %q, want %q", detail.WorkflowID, "wf-identity")
	}
	if detail.WorkflowVersion != "v7" {
		t.Fatalf("WorkflowVersion = %q, want %q", detail.WorkflowVersion, "v7")
	}
	statusOnly, err := eng.InspectStatus(ctx, id)
	if err != nil {
		t.Fatalf("InspectStatus() error = %v", err)
	}
	if statusOnly.WorkflowID != detail.WorkflowID || statusOnly.WorkflowVersion != detail.WorkflowVersion {
		t.Fatalf("InspectStatus identity = (%q, %q), want (%q, %q)",
			statusOnly.WorkflowID, statusOnly.WorkflowVersion, detail.WorkflowID, detail.WorkflowVersion)
	}
}

// TestInspectLeavesUnattributedExecutionEmpty pins the fail-closed half: a
// submission that attached no definition must not borrow an identity.
func TestInspectLeavesUnattributedExecutionEmpty(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	eng := newTestEngine(t, state, &fakeQueue{}, &fakeRegistry{handlers: map[string]types.ActionHandler{}})

	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "no-identity",
		Nodes: []types.NodeDef{{Name: "start", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	detail, err := eng.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if detail.WorkflowID != "" || detail.WorkflowVersion != "" {
		t.Fatalf("identity = (%q, %q), want empty for a submission without a definition",
			detail.WorkflowID, detail.WorkflowVersion)
	}
}

// TestRerunInheritsWorkflowIdentity pins the rerun half: a rerun re-runs the
// parent's definition, so when its own submission carries no definition it
// inherits the parent snapshot's identity rather than becoming unattributed.
func TestRerunInheritsWorkflowIdentity(t *testing.T) {
	ctx := context.Background()
	g := rerunDiamond(t)
	state := newFakeState()
	parent := rerunParent(t, state, g, types.ExecutionStatusFailed, diamondResolved())
	state.mu.Lock()
	state.executions[parent].WorkflowID = "wf-rerun"
	state.executions[parent].WorkflowVersion = "v2"
	state.mu.Unlock()

	seeder := &seedRecorder{fakeState: state}
	eng := New(seeder, &fakeQueue{})
	res, err := eng.Rerun(ctx, RerunRequest{ExecutionID: parent, FromNode: "mid"})
	if err != nil {
		t.Fatalf("Rerun() error = %v", err)
	}
	seeded := seeder.lastRequest(t).Execution
	if seeded.WorkflowID != "wf-rerun" || seeded.WorkflowVersion != "v2" {
		t.Fatalf("seeded identity = (%q, %q), want the parent's (%q, %q)",
			seeded.WorkflowID, seeded.WorkflowVersion, "wf-rerun", "v2")
	}
	// The seeded snapshot is what both backends persist; inspecting it is the
	// derived execution's audit answer.
	if err := state.CreateExecution(ctx, seeded); err != nil {
		t.Fatalf("CreateExecution(seeded) error = %v", err)
	}
	detail, err := eng.Inspect(ctx, res.ExecutionID)
	if err != nil {
		t.Fatalf("Inspect(rerun) error = %v", err)
	}
	if detail.WorkflowID != seeded.WorkflowID || detail.WorkflowVersion != seeded.WorkflowVersion {
		t.Fatalf("inspected rerun identity = (%q, %q), want (%q, %q)",
			detail.WorkflowID, detail.WorkflowVersion, seeded.WorkflowID, seeded.WorkflowVersion)
	}

	// A parent written before the half-identity rule may carry a version with
	// no id. The rerun must stay unattributed rather than inherit the version
	// alone — a version no workflow id can resolve to.
	state.mu.Lock()
	state.executions[parent].WorkflowID = ""
	state.executions[parent].WorkflowVersion = "v9"
	state.mu.Unlock()
	if _, err := eng.Rerun(ctx, RerunRequest{ExecutionID: parent, FromNode: "mid"}); err != nil {
		t.Fatalf("Rerun(half-identity parent) error = %v", err)
	}
	if len(seeder.requests) != 2 {
		t.Fatalf("SeedExecution() called %d times, want 2", len(seeder.requests))
	}
	half := seeder.requests[1].Execution
	if half.WorkflowID != "" || half.WorkflowVersion != "" {
		t.Fatalf("rerun of a half-identity parent = (%q, %q), want both empty",
			half.WorkflowID, half.WorkflowVersion)
	}
}

// TestSubWorkflowChildInspectsWithItsOwnIdentity pins the sub-execution half: a
// child execution names the resolved target it runs, not the parent's
// workflow, because it is a different definition with its own version.
func TestSubWorkflowChildInspectsWithItsOwnIdentity(t *testing.T) {
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
	detail, err := rig.eng.Inspect(context.Background(), childID)
	if err != nil {
		t.Fatalf("Inspect(child): %v", err)
	}
	if detail.WorkflowID != "wf-child" {
		t.Fatalf("child WorkflowID = %q, want %q", detail.WorkflowID, "wf-child")
	}
	if detail.WorkflowVersion != "v1" {
		t.Fatalf("child WorkflowVersion = %q, want %q", detail.WorkflowVersion, "v1")
	}
	parentDetail, err := rig.eng.Inspect(context.Background(), rig.parentID)
	if err != nil {
		t.Fatalf("Inspect(parent): %v", err)
	}
	if parentDetail.WorkflowID != "" {
		t.Fatalf("parent WorkflowID = %q, want empty: the rig parent was seeded without an identity", parentDetail.WorkflowID)
	}
}

// TestSubWorkflowChildWithoutResolverIdentityStaysUnattributed pins the
// half-identity rule for children: a resolver that supplies only a version
// (the production registry path, whose JSON-decoded graph cannot return an
// ExpandedDefinition) must not stamp that version onto an id-less child. A
// version with no workflow id cannot be resolved back to any definition.
func TestSubWorkflowChildWithoutResolverIdentityStaysUnattributed(t *testing.T) {
	childGraph, err := graph.Compile(&types.WorkflowDef{
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
	raw, err := json.Marshal(childGraph)
	if err != nil {
		t.Fatalf("marshal child graph: %v", err)
	}
	decoded := &graph.Graph{}
	if err := json.Unmarshal(raw, decoded); err != nil {
		t.Fatalf("unmarshal child graph: %v", err)
	}
	if decoded.ExpandedDefinition() != nil {
		t.Fatal("a JSON-decoded graph kept its expanded definition; the test no longer covers the registry path")
	}
	resolver := stubSubWorkflowResolver{target: graph.SubWorkflowTarget{
		Version:        "v1",
		DefinitionHash: "sha256:child",
		Graph:          decoded,
	}}
	parent, err := graph.CompileWithResolver(context.Background(), subWorkflowParentDef(), resolver)
	if err != nil {
		t.Fatalf("CompileWithResolver: %v", err)
	}
	rig := newSubWorkflowRig(t, parent, nil, resolver)
	rig.dispatch(t, rig.task)

	childID := subWorkflowChildID(rig.parentID, "sub", 1, 1)
	detail, err := rig.eng.Inspect(context.Background(), childID)
	if err != nil {
		t.Fatalf("Inspect(child): %v", err)
	}
	if detail.WorkflowID != "" || detail.WorkflowVersion != "" {
		t.Fatalf("child identity = (%q, %q), want both empty: a version without an id is a half identity",
			detail.WorkflowID, detail.WorkflowVersion)
	}
}
