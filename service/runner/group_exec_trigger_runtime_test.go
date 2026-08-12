package runner

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// NOTE: the task-5-brief.md test literal imports "time" but never calls it —
// a Go compile error (unused import) orthogonal to this task's own logic.
// Removed per the task's instruction to fix such fixtures minimally rather
// than change production code.

// TestGroupExecTriggerRuntime_ExecuteGroupRunsRealMembers proves the adapter's
// ExecuteGroup actually dispatches the package's real member node (not a
// stand-in) and returns its real resulting exits.
func TestGroupExecTriggerRuntime_ExecuteGroupRunsRealMembers(t *testing.T) {
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.echo", echoHandler{})
	cache := NewPackageCache(PackageCacheConfig{MaxEntries: 10})
	groupRT := NewGroupRuntime(reg, cache, WithSuspendDisabled())

	pkg := &graph.SubgraphPackage{
		Version:   1,
		GroupName: "g",
		EntryNode: "a",
		Def: &types.WorkflowDef{
			Name: "g",
			Nodes: []types.NodeDef{
				{Name: "a", Type: "test.echo", Version: 1},
				{Name: "__collector_a_main", Type: graph.NodeTypeGroupExit, Version: 1},
			},
			Connections: types.Connections{
				"a": {"main": types.PortConnections{Targets: []types.Connection{{Node: "__collector_a_main"}}}},
			},
		},
		Exits: []graph.SubgraphPackageExit{{CollectorNode: "__collector_a_main", SrcNode: "a", Port: "main"}},
		Requirements: []graph.Requirement{{NodeType: "test.echo", NodeVersion: 1}},
	}
	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		t.Fatal(err)
	}

	adapter := &groupExecTriggerRuntime{
		HTTPEntrySeedRuntime: &protocol.HTTPEntrySeedRuntime{},
		runtime:              groupRT,
		pkg:                  pkg,
		packageHash:          hash,
	}

	res, err := adapter.ExecuteGroup(context.Background(), map[string]any{"batch": "b1"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != "success" {
		t.Fatalf("outcome = %s, want success; error = %s", res.Outcome, res.Error)
	}
	if len(res.Exits) != 1 || res.Exits[0].NodeName != "a" || res.Exits[0].Data["batch"] != "b1" {
		t.Fatalf("exits = %+v, want one exit from node a echoing batch=b1", res.Exits)
	}

	// The adapter is ALSO a types.EntrySeedRuntime (via the embedded
	// HTTPEntrySeedRuntime) and a types.GroupExecRuntime — both capabilities
	// must be visible through the SAME value, which is what lets
	// TriggerActivationHandler install one Runtime that a trigger can type-
	// assert for either.
	var _ types.EntrySeedRuntime = adapter
	var _ types.GroupExecRuntime = adapter
	var _ types.TriggerRuntime = adapter
}
