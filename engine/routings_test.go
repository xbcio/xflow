package engine

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

func dispatchableNames(t *testing.T, g *graph.Graph) []string {
	t.Helper()
	units, err := DispatchableUnits(g)
	if err != nil {
		t.Fatalf("DispatchableUnits: %v", err)
	}
	names := make([]string, 0, len(units))
	for _, u := range units {
		names = append(names, u.Name)
	}
	sort.Strings(names)
	return names
}

// exclusionDef declares one node of every shape DispatchableUnits has to decide
// about. "live" is the only one that is unambiguously dispatched to a runner.
func exclusionDef(pinMode string) *types.WorkflowDef {
	def := &types.WorkflowDef{
		Name: "dispatchable",
		Nodes: []types.NodeDef{
			{Name: "tick", Type: "test.trigger", Version: 1, Kind: types.NodeKindTrigger},
			{Name: "rules", Type: "test.supply", Kind: types.NodeKindSupply},
			{Name: "live", Type: "test.echo", Version: 1},
			{Name: "off", Type: "test.echo", Version: 1, Disabled: true},
			{Name: "child", Type: types.SubWorkflowNodeType, Parameters: map[string]any{"workflow": "other"}},
			{Name: "mock", Type: "test.echo", Version: 1},
		},
		Connections: types.Connections{
			"tick":  {"main": {Targets: []types.Connection{{Node: "live", Input: "main"}}}},
			"live":  {"main": {Targets: []types.Connection{{Node: "off", Input: "main"}}}},
			"off":   {"main": {Targets: []types.Connection{{Node: "child", Input: "main"}}}},
			"child": {"main": {Targets: []types.Connection{{Node: "mock", Input: "main"}}}},
		},
		PinData: map[string]any{"mock": map[string]any{"v": 1}},
	}
	if pinMode != "" {
		def.Settings = &types.WorkflowSettings{PinDataMode: pinMode}
	}
	return def
}

// TestDispatchableUnitsExcludesWhatTheEngineOrControlPlaneOwns pins which
// units produce runner tasks. Each excluded node is a place where the work
// never enters a claim queue, so counting it would report a "gap" no runner
// could ever close.
func TestDispatchableUnitsExcludesWhatTheEngineOrControlPlaneOwns(t *testing.T) {
	tests := []struct {
		name    string
		pinMode string
		want    []string
	}{
		// test_only is the default: whether the pin applies depends on the
		// execution's test flag, so a production run still dispatches "mock".
		{name: "pin test_only (default) still dispatches the pinned node", pinMode: "", want: []string{"live", "mock"}},
		{name: "pin always is served by the engine", pinMode: types.PinDataModeAlways, want: []string{"live"}},
		{name: "pin disabled is ignored", pinMode: types.PinDataModeDisabled, want: []string{"live", "mock"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g, err := graph.Compile(exclusionDef(tt.pinMode))
			if err != nil {
				t.Fatalf("Compile: %v", err)
			}
			if got := dispatchableNames(t, g); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("dispatchable = %v, want %v (trigger, supply, subworkflow and disabled nodes are never dispatched)", got, tt.want)
			}
		})
	}
}

// TestDispatchableUnitsKeepsPinnedNodesOfCyclicGraphs: the engine's pin hook
// never intercepts on a cyclic graph (handlePinnedNode returns early on
// g.AllowCycles()), so a pinned node there is dispatched to a runner even under
// pin_data_mode "always". Excluding it would hide a real claim gap.
//
// graph.Compile cannot produce this shape: it ignores pin_data on a cyclic
// workflow with a warning and leaves the graph unpinned. The shape only arrives
// through a snapshot load, which is the path every cluster dispatch after a
// cache miss takes and the reason the engine guards on AllowCycles at all. So
// the fixture is a compiled cyclic graph whose snapshot is edited to carry a pin
// and then reloaded.
func TestDispatchableUnitsKeepsPinnedNodesOfCyclicGraphs(t *testing.T) {
	compiled, err := graph.Compile(&types.WorkflowDef{
		Name:    "cyclic",
		Options: &types.WorkflowOptions{AllowCycles: true},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "review", Type: "test.echo", Version: 1},
		},
		Connections: types.Connections{
			"start":  {"main": {Targets: []types.Connection{{Node: "review", Input: "main"}}}},
			"review": {"reject": {Targets: []types.Connection{{Node: "start", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	raw, err := json.Marshal(compiled)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var snap map[string]any
	if err := json.Unmarshal(raw, &snap); err != nil {
		t.Fatalf("Unmarshal snapshot: %v", err)
	}
	snap["pin_data_mode"] = types.PinDataModeAlways
	pinned := false
	for _, n := range snap["nodes"].([]any) {
		node := n.(map[string]any)
		if node["Name"] == "review" || node["name"] == "review" {
			node["pin_output"] = map[string]any{"v": 1}
			pinned = true
		}
	}
	if !pinned {
		t.Fatalf("snapshot has no review node to pin: %s", raw)
	}
	edited, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("Marshal edited snapshot: %v", err)
	}
	g := &graph.Graph{}
	if err := json.Unmarshal(edited, g); err != nil {
		t.Fatalf("Unmarshal edited snapshot: %v", err)
	}
	idx, _ := g.NodeIndex("review")
	if _, ok := g.PinnedOutput(idx); !ok || !g.AllowCycles() || g.PinDataMode() != types.PinDataModeAlways {
		t.Fatalf("fixture is not a cyclic pin-always graph: pinned=%v cycles=%v mode=%q", ok, g.AllowCycles(), g.PinDataMode())
	}
	want := []string{"review", "start"}
	if got := dispatchableNames(t, g); !reflect.DeepEqual(got, want) {
		t.Fatalf("dispatchable = %v, want %v (pin never applies on a cyclic graph)", got, want)
	}
}

// TestDispatchableUnitsSkipsTriggerGroups: a co-location group whose entry is a
// trigger is a hosted activation placed by the entry-activation reconciler. It
// never enters a claim queue, so it is not a claim gap even though the group has
// a runner selector and requirements of its own.
func TestDispatchableUnitsSkipsTriggerGroups(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "trigger-group",
		Nodes: []types.NodeDef{
			{Name: "tick", Type: "test.trigger", Version: 1, Kind: types.NodeKindTrigger},
			{Name: "job", Type: "test.echo", Version: 1},
			{Name: "other", Type: "test.echo", Version: 1},
		},
		Groups: []types.GroupDef{{Name: "edge", Members: []string{"tick", "job"}}},
		Connections: types.Connections{
			"tick": {"main": {Targets: []types.Connection{{Node: "job", Input: "main"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	gm := g.Groups()
	if len(gm) != 1 || !gm[0].Trigger {
		t.Fatalf("fixture has no trigger-entry group: %+v", gm)
	}
	want := []string{"other"}
	if got := dispatchableNames(t, g); !reflect.DeepEqual(got, want) {
		t.Fatalf("dispatchable = %v, want %v (the trigger group is hosted, not claimed)", got, want)
	}
}

// TestDispatchableUnitsNilGraph keeps the "no graph" answer empty rather than a
// panic; the caller decides what a missing graph means.
func TestDispatchableUnitsNilGraph(t *testing.T) {
	units, err := DispatchableUnits(nil)
	if err != nil || len(units) != 0 {
		t.Fatalf("DispatchableUnits(nil) = %v, %v, want nothing", units, err)
	}
}

// TestDispatchableUnitsRoutingMatchesTaskRouting is the parity guarantee: for
// every unit it reports, the routing equals what Engine.TaskRouting returns for
// the task the scheduler would enqueue for that unit. A group unit is checked
// as well as plain nodes, because a group's requirements come from projecting
// its package.
func TestDispatchableUnitsRoutingMatchesTaskRouting(t *testing.T) {
	eng, g, execID := setupGroupLeaseTest(t)
	units, err := DispatchableUnits(g)
	if err != nil {
		t.Fatalf("DispatchableUnits: %v", err)
	}
	if len(units) != 2 {
		t.Fatalf("units = %+v, want the group and the ungrouped node", units)
	}
	var sawGroup, sawNode bool
	for _, u := range units {
		task := &Task{ExecutionID: execID, NodeName: u.Name, UnitIdx: u.UnitIdx, Type: u.TaskType}
		if u.TaskType == TaskTypeGroupExec {
			sawGroup = true
			task.NodeIdx = g.GroupMetaAt(u.UnitIdx).EntryIdx
		} else {
			sawNode = true
			task.NodeIdx = g.UnitNodeIndex(u.UnitIdx)
		}
		want, err := eng.TaskRouting(context.Background(), task)
		if err != nil {
			t.Fatalf("TaskRouting(%s): %v", u.Name, err)
		}
		if !reflect.DeepEqual(u.Routing, want) {
			t.Fatalf("unit %q routing = %+v, TaskRouting = %+v", u.Name, u.Routing, want)
		}
	}
	if !sawGroup || !sawNode {
		t.Fatalf("group seen = %v, node seen = %v, want both", sawGroup, sawNode)
	}
}

func TestRoutableTaskType(t *testing.T) {
	for _, tt := range []struct {
		typ  TaskType
		want bool
	}{
		{TaskTypeNodeExec, true},
		{TaskTypeNodeBatch, true},
		{TaskTypeGroupExec, true},
		{TaskTypeNodeAdvance, false},
		{TaskTypeNodeSkip, false},
		{TaskTypeSubWorkflow, false},
	} {
		if got := RoutableTaskType(tt.typ); got != tt.want {
			t.Errorf("RoutableTaskType(%d) = %v, want %v", tt.typ, got, tt.want)
		}
	}
}
