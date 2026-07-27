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

func TestRuntimeHashUngroupedStable(t *testing.T) {
	// 无 groups 的定义 hash 不受 Groups 字段引入影响（omitempty + nil）。
	d := &types.WorkflowDef{Name: "w", Nodes: nodesAB()}
	if d.Groups != nil {
		t.Fatal("precondition: Groups nil")
	}
	if _, err := runtimeHash(d); err != nil {
		t.Fatal(err)
	}
}
