package workflows_test

import (
	"slices"
	"testing"

	"github.com/xbcio/xflow/test/workflows"
)

// NodeNamesIn exists so a diagnostic dump names the nodes the definition
// actually has. Pin its three contract points on the real tiers: body members
// are included right after the node carrying them, declaration-only supplies are
// left out, and the first/last entries bracket the run.
func TestNodeNamesInCoversBodyMembersAndSkipsDeclarations(t *testing.T) {
	med, err := workflows.MediumWorkflow().Definition()
	if err != nil {
		t.Fatalf("MediumWorkflow().Definition(): %v", err)
	}
	names := workflows.NodeNamesIn(med)
	if len(names) == 0 || names[0] != "start" || names[len(names)-1] != "done" {
		t.Fatalf("medium names %v: want start first and done last", names)
	}
	fan, dq := slices.Index(names, "fan"), slices.Index(names, "dq")
	if fan < 0 || dq != fan+1 {
		t.Fatalf("medium names %v: dq (map body member) must follow fan", names)
	}

	high, err := workflows.HighWorkflow().Definition()
	if err != nil {
		t.Fatalf("HighWorkflow().Definition(): %v", err)
	}
	names = workflows.NodeNamesIn(high)
	for _, want := range []string{"start", "summarize", "done"} {
		if !slices.Contains(names, want) {
			t.Fatalf("high names %v: missing %q", names, want)
		}
	}
	for _, declaration := range []string{"policy", "rules"} {
		if slices.Contains(names, declaration) {
			t.Fatalf("high names %v: declaration-only node %q must be skipped", names, declaration)
		}
	}
}
