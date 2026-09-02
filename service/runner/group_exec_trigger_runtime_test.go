package runner

import (
	"context"
	"errors"
	"strings"
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
		Exits:        []graph.SubgraphPackageExit{{CollectorNode: "__collector_a_main", SrcNode: "a", Port: "main"}},
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

// TestGroupExecTriggerRuntime_DeterministicFromMemberClassification proves the
// deterministic verdict comes from the failing member's own classification and
// not from the shape of its error text.
//
// Both sub-cases use a message that a keyword scan cannot tell apart — the same
// wasm trap wording, which contains none of "compile", "validation", "schema",
// "syntax" or "deterministic". What differs is only whether the member built its
// error with types.NewPermanentError. The consumer
// (node/trigger/kafka/entryseed.go) commits the batch's offset on
// Deterministic and withholds it otherwise, so getting this backwards on a
// permanent failure redelivers the same doomed batch forever.
func TestGroupExecTriggerRuntime_DeterministicFromMemberClassification(t *testing.T) {
	const trapMsg = "wasm trap: unreachable executed at 0x1f4"

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "member classified its failure permanent",
			err:  types.NewPermanentError("guest_trap", trapMsg),
			want: true,
		},
		{
			// Identical text, no classification: the batch may well succeed on
			// redelivery, so the offset must stay withheld.
			name: "member returned an unclassified error",
			err:  errors.New(trapMsg),
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := execution.NewRegistry()
			reg.RegisterGlobal("test.fail", failHandler{err: tc.err})
			cache := NewPackageCache(PackageCacheConfig{MaxEntries: 10})
			groupRT := NewGroupRuntime(reg, cache, WithSuspendDisabled())

			pkg := &graph.SubgraphPackage{
				Version:   1,
				GroupName: "g",
				EntryNode: "a",
				Def: &types.WorkflowDef{
					Name: "g",
					Nodes: []types.NodeDef{
						{Name: "a", Type: "test.fail", Version: 1},
						{Name: "__collector_a_main", Type: graph.NodeTypeGroupExit, Version: 1},
					},
					Connections: types.Connections{
						"a": {"main": types.PortConnections{Targets: []types.Connection{{Node: "__collector_a_main"}}}},
					},
				},
				Exits:        []graph.SubgraphPackageExit{{CollectorNode: "__collector_a_main", SrcNode: "a", Port: "main"}},
				Requirements: []graph.Requirement{{NodeType: "test.fail", NodeVersion: 1}},
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
			if res.Outcome != "failed" {
				t.Fatalf("outcome = %s, want failed", res.Outcome)
			}
			if !strings.Contains(res.Error, trapMsg) {
				t.Fatalf("error = %q, want it to carry the member's own message %q", res.Error, trapMsg)
			}
			if res.Deterministic != tc.want {
				t.Fatalf("Deterministic = %v, want %v (error text is identical in both cases, so only the member's classification can distinguish them)", res.Deterministic, tc.want)
			}
		})
	}
}

// TestGroupExecTriggerRuntime_UnrunnableGroupIsPermanent covers the OTHER half
// of the deterministic/transient split: not "the group ran and failed"
// (Deterministic on the result, above) but "the group could not be run at all",
// which leaves via the error return and therefore carries no result to hold a
// flag.
//
// The consumer that reads this is node/trigger/kafka's batch admission
// (entryseed.go, seedEntryBatchViaGroupExec). Before the classification, it
// reported a package that will never compile with the same "error" state as a
// broker blip — so a permanently broken deploy looked, on the dashboard, like a
// transient one that simply never cleared.
func TestGroupExecTriggerRuntime_UnrunnableGroupIsPermanent(t *testing.T) {
	reg := execution.NewRegistry() // deliberately empty
	cache := NewPackageCache(PackageCacheConfig{MaxEntries: 10})
	groupRT := NewGroupRuntime(reg, cache, WithSuspendDisabled())

	// A package REQUIRING a handler this runner does not have. validatePackage
	// (execution/subgraph/cache.go:162) rejects it before anything runs, which
	// is the shape a runner one deploy behind the control plane produces.
	pkg := &graph.SubgraphPackage{
		Version:   1,
		GroupName: "g",
		EntryNode: "a",
		Def: &types.WorkflowDef{
			Name: "g",
			Nodes: []types.NodeDef{
				{Name: "a", Type: "test.absent", Version: 1},
				{Name: "__collector_a_main", Type: graph.NodeTypeGroupExit, Version: 1},
			},
			Connections: types.Connections{
				"a": {"main": types.PortConnections{Targets: []types.Connection{{Node: "__collector_a_main"}}}},
			},
		},
		Exits:        []graph.SubgraphPackageExit{{CollectorNode: "__collector_a_main", SrcNode: "a", Port: "main"}},
		Requirements: []graph.Requirement{{NodeType: "test.absent", NodeVersion: 1}},
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

	_, execErr := adapter.ExecuteGroup(context.Background(), map[string]any{"batch": "b1"})
	if execErr == nil {
		t.Fatal("a package whose required handler is absent must not execute")
	}
	if !types.IsPermanent(execErr) {
		t.Fatalf("ExecuteGroup error = %v, want it marked types.ErrPermanent; "+
			"unmarked, the Kafka batch path reports a package that can never "+
			"compile as a transient failure", execErr)
	}
	// The original cause must survive the wrapping: it is the only thing that
	// says WHICH handler is missing, and the admission log prints err.Error().
	if !strings.Contains(execErr.Error(), "test.absent") {
		t.Fatalf("ExecuteGroup error = %q, want it to still name the missing handler", execErr)
	}
}

// TestClassifyGroupExecError_UnrecognisedStaysTransient pins the allowlist's
// deliberate default. classifyGroupExecError marks only the shapes it can
// name; anything else keeps the transient reading, whose only cost is a retry.
// A blanket "every error out of ExecuteSubgraph is permanent" would pass every
// test above and would silently be wrong the first time Execute grows a second
// error return.
func TestClassifyGroupExecError_UnrecognisedStaysTransient(t *testing.T) {
	blip := errors.New("dial tcp: connection refused")
	if got := classifyGroupExecError(blip); types.IsPermanent(got) {
		t.Fatalf("classifyGroupExecError(%v) was marked permanent; an unrecognised "+
			"error must keep the transient reading", blip)
	}
	if classifyGroupExecError(nil) != nil {
		t.Fatal("classifyGroupExecError(nil) must stay nil")
	}
}
