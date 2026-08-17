package xflow

import (
	"testing"

	"github.com/xbcio/xflow/types"
)

// TestRuntimeHashFrozenForRunnerSelector is the load-bearing pin for the
// wire-tag decoupling (API-SPECIFICATION.md §9.4). The runtime hash is
// computed by marshalling runtimeHashPayload, which holds *types.RunnerSelector
// directly in three places (workflow-level, per-node, per-group). A nested
// struct marshals with ITS OWN tags, so renaming the wire tags on
// types.RunnerSelector (runnerSelector→runner_selector, matchLabels→
// match_labels) would silently change the marshalled bytes and therefore the
// hash of every definition carrying a selector — turning an upgrade into a
// permanent ErrWorkflowConflict on every previously-registered workflow.
//
// This pin is a LITERAL value, not a self-comparison: a test that hashes two
// definitions and compares them to each other CANNOT fail here, because both
// sides move together under the same binary. The decoupling introduces a
// hash-local mirror (runtimeSelectorHashPayload) whose tags are frozen at the
// pre-rename bytes, so the wire rename leaves the hash bytes untouched.
//
// If this fails after a deliberate change to the hash payload, update the
// constant in the same commit that changes the payload.
func TestRuntimeHashFrozenForRunnerSelector(t *testing.T) {
	def := &types.WorkflowDef{
		Namespace: "default",
		Name:      "sel-wf",
		Version:   "v1",
		RunnerSelector: &types.RunnerSelector{
			Mode:        types.RunnerSelectorModeRequired,
			MatchLabels: map[string]string{"zone": "a", "env": "prod"},
		},
		Nodes: []types.NodeDef{
			{
				Name: "trig", Type: "kafka.source", Kind: types.NodeKindTrigger,
				RunnerSelector: &types.RunnerSelector{
					Mode:        types.RunnerSelectorModeDefault,
					MatchLabels: map[string]string{"region": "cn"},
				},
			},
			{Name: "work", Type: "http.request"},
		},
		Groups: []types.GroupDef{
			{
				Name:    "g1",
				Members: []string{"trig", "work"},
				RunnerSelector: &types.RunnerSelector{
					MatchLabels: map[string]string{"pool": "x"},
				},
			},
		},
		Connections: types.Connections{
			"trig": {"main": types.PortConnections{Targets: []types.Connection{{Node: "work", Input: "main"}}}},
		},
	}
	h, err := runtimeHash(def)
	if err != nil {
		t.Fatalf("runtimeHash: %v", err)
	}
	// Pinned to the literal computed on the pre-rename tree. This is the proof
	// obligation for the wire-tag decoupling: after the §9.4 rename of
	// types.RunnerSelector's tags to snake_case, this value must NOT move,
	// because the hash-local mirror (runtimeSelectorHashPayload) froze the bytes.
	const wantHash = "runtime-sha256:v1:4f770306236df9d5470bd4bc6296ea1267c7cc05683eb4cde8696385ae310ad0"
	if h != wantHash {
		t.Fatalf("runtime hash for selector-bearing definition = %q, want %q; the "+
			"wire-tag decoupling is broken and every previously-registered workflow "+
			"with a selector will hit ErrWorkflowConflict on upgrade", h, wantHash)
	}
}
