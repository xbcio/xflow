package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// setupGroupLeaseEngine is setupGroupLeaseTest's shape with the engine option
// level exposed: the group lease family must follow the same namespace rule as
// BuildTaskLease, so the tests here need to construct the engine with (and
// without) WithNamespace.
func setupGroupLeaseEngine(t *testing.T, ns namespace.Namespace) (*Engine, *graph.Graph, types.ExecutionID) {
	t.Helper()
	def := &types.WorkflowDef{
		Name:    "ns-group-lease",
		Version: "1",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "http.request", Version: 1},
			{Name: "B", Type: "http.request", Version: 1},
		},
		Groups: []types.GroupDef{
			{Name: "grp1", Members: []string{"A", "B"}},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	execID := types.ExecutionID("exec-ns-group-lease")
	state := &fakeGroupLeaseState{fakeState: newFakeState()}
	state.fakeState.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID:     execID,
		Graph:  g,
		Status: types.ExecutionStatusRunning,
	})

	var opts []Option
	if ns != "" {
		opts = append(opts, WithNamespace(ns))
	}
	eng := New(state, &fakeQueue{}, opts...)
	eng.cacheExecutionGraph(execID, g)
	return eng, g, execID
}

// TestBuildGroupLeaseStampsTheEnginesNamespace pins the group lease family to
// the same rule as BuildTaskLease: an engine pinned to one namespace stamps it
// onto every lease it issues, because an embedded engine's lease is the only
// carrier across the queue boundary. The deployed control plane overwrites the
// value with the claim's namespace right after the call (dispatchGroupLease),
// so the stamp is inert there — but for an embedded engine no such caller
// exists, and an unstamped group lease leaves the runner-side namespace
// injection with nothing to apply.
//
// The unstamped sub-test pins the other half: no option, no invented value.
// Stamping namespace.Default here would collapse per-claim naming for any
// caller that overwrites conditionally.
//
// Mutation: drop the Namespace field from the taskLease literal in
// BuildGroupLease — the stamped sub-test fails. Default it to
// namespace.Default — the unstamped sub-test fails.
func TestBuildGroupLeaseStampsTheEnginesNamespace(t *testing.T) {
	for _, tc := range []struct {
		name string
		ns   namespace.Namespace
		want namespace.Namespace
	}{
		{"engine namespace is stamped", "tenant-b", "tenant-b"},
		{"unstamped engine stays empty", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eng, g, execID := setupGroupLeaseEngine(t, tc.ns)
			gm := g.Groups()[0]
			task := &Task{
				ExecutionID: execID,
				NodeName:    gm.Name,
				NodeIdx:     gm.EntryIdx,
				UnitIdx:     gm.UnitIdx,
				Type:        TaskTypeGroupExec,
			}

			lease, _, err := eng.BuildGroupLease(context.Background(), task)
			if err != nil {
				t.Fatalf("BuildGroupLease: %v", err)
			}
			if lease.Namespace != tc.want {
				t.Fatalf("lease.Namespace = %q, want %q", lease.Namespace, tc.want)
			}
		})
	}
}
