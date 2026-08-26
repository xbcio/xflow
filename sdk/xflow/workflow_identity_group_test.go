package xflow

import (
	"testing"

	"github.com/xbcio/xflow/types"
)

func nodesAB() []types.NodeDef { return []types.NodeDef{{Name: "a"}, {Name: "b"}} }

func TestRuntimeHashIncludesGroups(t *testing.T) {
	base := &types.WorkflowDef{Name: "w", Nodes: nodesAB()}
	grouped := &types.WorkflowDef{Name: "w", Nodes: nodesAB(),
		Groups: []types.GroupDef{{Name: "g", Members: []string{"a", "b"}}}}
	h1, err := runtimeHash(base)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := runtimeHash(grouped)
	if err != nil {
		t.Fatal(err)
	}
	if h1 == h2 {
		t.Fatal("adding a group must change runtime hash")
	}
}

func TestRuntimeHashStableUnderMemberOrder(t *testing.T) {
	d1 := &types.WorkflowDef{Name: "w", Nodes: nodesAB(),
		Groups: []types.GroupDef{{Name: "g", Members: []string{"a", "b"}}}}
	d2 := &types.WorkflowDef{Name: "w", Nodes: nodesAB(),
		Groups: []types.GroupDef{{Name: "g", Members: []string{"b", "a"}}}}
	h1, _ := runtimeHash(d1)
	h2, _ := runtimeHash(d2)
	if h1 != h2 {
		t.Fatal("equivalent member order must produce identical hash")
	}
}

func TestRuntimeHashChangesWithGroupField(t *testing.T) {
	mk := func(mut func(*types.GroupDef)) string {
		g := types.GroupDef{Name: "g", Members: []string{"a", "b"}}
		mut(&g)
		d := &types.WorkflowDef{Name: "w", Nodes: nodesAB(), Groups: []types.GroupDef{g}}
		h, err := runtimeHash(d)
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	base := mk(func(*types.GroupDef) {})
	sel := mk(func(g *types.GroupDef) {
		g.RunnerSelector = &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired}
	})
	oe := mk(func(g *types.GroupDef) { g.OnError = string(types.OnErrorStop) })
	mode := mk(func(g *types.GroupDef) { g.Mode = types.GroupModeTransient })
	if base == sel || base == oe || base == mode {
		t.Fatal("selector/onError/mode change must change runtime hash")
	}
}

// TestRuntimeHashCoversGroups replaces TestRuntimeHashUngroupedStable, which
// asserted only that runtimeHash returned no error — every possible hash
// function passed it.
//
// The property kept here is that Groups reaches the digest at all. Without it,
// two workflows that differ only in their grouping share a runtime hash, and
// grouping is a placement decision: the same nodes co-located on one runner or
// spread across the fleet would be indistinguishable by identity.
//
// The property the old name implied — that an ungrouped definition hashes the
// same whether Groups is nil or an empty slice — is deliberately NOT asserted.
// It cannot fail: canonicalizeGroups takes a slice and normalises both spellings
// before anything is serialised, so the caller's distinction does not survive
// the call. Removing the omitempty tag and the len==0 early return, together,
// still leaves the two identical. An assertion no change can break is the thing
// this sweep exists to remove, not to add.
func TestRuntimeHashCoversGroups(t *testing.T) {
	mk := func(groups []types.GroupDef) string {
		d := &types.WorkflowDef{Name: "w", Nodes: nodesAB(), Groups: groups}
		h, err := runtimeHash(d)
		if err != nil {
			t.Fatal(err)
		}
		return h
	}

	ungrouped := mk(nil)
	if grouped := mk([]types.GroupDef{{Name: "g", Members: []string{"a", "b"}}}); ungrouped == grouped {
		t.Errorf("hash is identical with and without a group (%s); Groups is not "+
			"reaching the digest, so two workflows with different placement share "+
			"one identity", ungrouped)
	}
}
