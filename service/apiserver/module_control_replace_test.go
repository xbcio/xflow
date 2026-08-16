package apiserver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

// newReplaceTestServer builds an apiserver over an in-memory backend with an
// entry-activation store wired, so registering a trigger workflow really derives
// activations and replacing one really has something to clear. Without the store
// the manager is nil and the deactivation half of replace would go untested.
func newReplaceTestServer(t *testing.T) (*APIServer, *control.ControlPlane) {
	t.Helper()
	cp, err := control.NewControlPlane(control.Config{
		Backend:              local.New(),
		EntryActivationStore: control.NewMemoryEntryActivationStore(),
	})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	srv, err := New(Config{Concurrency: 1}, WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	return srv, cp
}

// configuredTriggerWorkflow is a workflow in the shape an embedded host builds
// from its own configuration: one remote-hosted trigger whose parameters carry a
// configured value, under a name and version that do not change with it.
func configuredTriggerWorkflow(topic string) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:    "host-configured-wf",
		Version: "v1",
		Nodes: []types.NodeDef{
			{
				Name: "trig", Type: "kafka.source", Version: 1, Kind: types.NodeKindTrigger,
				RunnerSelector: &types.RunnerSelector{MatchLabels: map[string]string{"zone": "a"}},
				Parameters:     map[string]any{"topic": topic},
			},
			{Name: "work", Type: "http.request", Version: 1, Kind: types.NodeKindAction},
		},
		Connections: types.Connections{"trig": {"main": types.PortConnections{Targets: []types.Connection{{Node: "work", Input: "main"}}}}},
	}
}

// TestRegisterWorkflowRejectsChangedDefinitionUnderSameKey pins the behaviour
// ReplaceWorkflow exists to work around, so the two tests below cannot both pass
// for the trivial reason that nothing ever conflicts.
//
// An embedded host that only has RegisterWorkflow is stuck here permanently: its
// name and version are constants in its own source, its definition follows its
// configuration, and it cannot remove the old record from inside its process.
func TestRegisterWorkflowRejectsChangedDefinitionUnderSameKey(t *testing.T) {
	srv, _ := newReplaceTestServer(t)
	ctx := context.Background()

	if _, _, err := srv.RegisterWorkflow(ctx, namespace.Default, configuredTriggerWorkflow("topic-a")); err != nil {
		t.Fatalf("first RegisterWorkflow: %v", err)
	}
	_, _, err := srv.RegisterWorkflow(ctx, namespace.Default, configuredTriggerWorkflow("topic-b"))
	if !errors.Is(err, backend.ErrWorkflowConflict) {
		t.Fatalf("second RegisterWorkflow err = %v, want ErrWorkflowConflict", err)
	}
}

// TestReplaceWorkflowSupersedesChangedDefinition is the whole point: the same
// second registration succeeds, the new definition is what is stored, and the
// old record is gone rather than left behind holding the key.
func TestReplaceWorkflowSupersedesChangedDefinition(t *testing.T) {
	srv, cp := newReplaceTestServer(t)
	ctx := context.Background()

	oldID, _, err := srv.RegisterWorkflow(ctx, namespace.Default, configuredTriggerWorkflow("topic-a"))
	if err != nil {
		t.Fatalf("first RegisterWorkflow: %v", err)
	}

	newID, _, err := srv.ReplaceWorkflow(ctx, namespace.Default, configuredTriggerWorkflow("topic-b"))
	if err != nil {
		t.Fatalf("ReplaceWorkflow: %v — a host whose definition follows its own "+
			"configuration cannot start at all when this fails", err)
	}
	if newID == oldID {
		t.Fatalf("ReplaceWorkflow returned the old id %q; the old record was never removed", oldID)
	}
	if _, err := cp.WorkflowRegistry().GetWorkflow(ctx, oldID); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("GetWorkflow(old) err = %v, want ErrWorkflowNotFound — the superseded "+
			"definition is still registered and its trigger still consumes", err)
	}
	rec, err := cp.WorkflowRegistry().GetWorkflow(ctx, newID)
	if err != nil {
		t.Fatalf("GetWorkflow(new): %v", err)
	}
	if got := rec.Definition.Nodes[0].Parameters["topic"]; got != "topic-b" {
		t.Fatalf("stored topic = %v, want topic-b — the registry kept the old definition", got)
	}
}

// TestReplaceWorkflowLeavesUnchangedDefinitionAlone guards the destructive half.
// A restart that changed nothing must not tear down and re-derive the running
// workflow: same id, and the entry activation still carries the runner it was
// assigned rather than a freshly derived one.
func TestReplaceWorkflowLeavesUnchangedDefinitionAlone(t *testing.T) {
	srv, cp := newReplaceTestServer(t)
	ctx := context.Background()

	firstID, _, err := srv.RegisterWorkflow(ctx, namespace.Default, configuredTriggerWorkflow("topic-a"))
	if err != nil {
		t.Fatalf("first RegisterWorkflow: %v", err)
	}
	key := engine.EntryActivationKey{
		Namespace: namespace.Default, WorkflowID: firstID, WorkflowVersion: "v1", EntryUnitID: "trig",
	}
	// Claim it the way the reconciler does. Upsert deliberately preserves
	// assignment state, so writing RunnerID through it would leave the field
	// empty and the assertion below would hold for the wrong reason.
	ok, err := cp.EntryActivationStore().Assign(ctx, key, "runner-already-assigned", "sess-1", 1, time.Now().Add(time.Minute))
	if err != nil || !ok {
		t.Fatalf("Assign activation: ok=%v err=%v", ok, err)
	}

	secondID, _, err := srv.ReplaceWorkflow(ctx, namespace.Default, configuredTriggerWorkflow("topic-a"))
	if err != nil {
		t.Fatalf("ReplaceWorkflow of an identical definition: %v", err)
	}
	if secondID != firstID {
		t.Fatalf("id changed from %q to %q for an identical definition; the record was "+
			"removed and re-created when it should have matched idempotently", firstID, secondID)
	}
	after, _, err := cp.EntryActivationStore().Get(ctx, key)
	if err != nil {
		t.Fatalf("Get activation after replace: %v", err)
	}
	if after.RunnerID != "runner-already-assigned" {
		t.Fatalf("activation RunnerID = %q, want it untouched — an unchanged definition "+
			"must not deactivate the trigger that is already running", after.RunnerID)
	}
	if !after.Desired {
		t.Fatal("activation is no longer Desired after replacing an identical definition; " +
			"the running trigger was deactivated for no reason")
	}
}
