package graph

import (
	"errors"
	"testing"

	"github.com/xbcio/xflow/types"
)

func TestCompile_LinearChain(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "linear",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.a"},
			{Name: "B", Type: "test.b"},
			{Name: "C", Type: "test.c"},
		},
		Connections: types.Connections{
			"A": {"main": []types.Connection{{Node: "B", Input: "main"}}},
			"B": {"main": []types.Connection{{Node: "C", Input: "main"}}},
		},
	}

	g, err := Compile(def)
	if err != nil {
		t.Fatal(err)
	}

	if len(g.Nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(g.Nodes))
	}
	if g.InDegree[g.Index["A"]] != 0 {
		t.Error("A should have in-degree 0")
	}
	if g.InDegree[g.Index["B"]] != 1 {
		t.Error("B should have in-degree 1")
	}
	if g.InDegree[g.Index["C"]] != 1 {
		t.Error("C should have in-degree 1")
	}
}

func TestCompile_CycleDetection(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "cycle",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.a"},
			{Name: "B", Type: "test.b"},
		},
		Connections: types.Connections{
			"A": {"main": []types.Connection{{Node: "B", Input: "main"}}},
			"B": {"main": []types.Connection{{Node: "A", Input: "main"}}},
		},
	}

	_, err := Compile(def)
	if err == nil {
		t.Fatal("expected cycle detection error")
	}
}

func TestCompile_AllowCyclesAllowsCycleWithStart(t *testing.T) {
	def := &types.WorkflowDef{
		Name:    "cycle",
		Options: &types.WorkflowOptions{AllowCycles: true, MaxAutoDepth: 7},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "review", Type: "test.review"},
		},
		Connections: types.Connections{
			"start":  {"main": []types.Connection{{Node: "review", Input: "main"}}},
			"review": {"reject": []types.Connection{{Node: "start", Input: "main"}}},
		},
	}

	g, err := Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	if !g.AllowCycles {
		t.Fatal("expected cyclic graph")
	}
	if g.StartIdx != g.Index["start"] {
		t.Fatalf("StartIdx = %d, want %d", g.StartIdx, g.Index["start"])
	}
	if g.MaxAutoDepth != 7 {
		t.Fatalf("MaxAutoDepth = %d, want 7", g.MaxAutoDepth)
	}
}

func TestCompileCollectsStartAndTriggerEntries(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "entries",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "cron", Type: "xflow.trigger.cron", Kind: types.NodeKindTrigger},
			{Name: "work", Type: "test.work"},
		},
		Connections: types.Connections{
			"start": {"main": []types.Connection{{Node: "work", Input: "main"}}},
			"cron":  {"main": []types.Connection{{Node: "work", Input: "main"}}},
		},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	if len(g.EntryIndexes) != 2 {
		t.Fatalf("EntryIndexes len = %d, want 2", len(g.EntryIndexes))
	}
	if g.EntryIndexes["start"] != 0 || g.EntryIndexes["cron"] != 1 {
		t.Fatalf("EntryIndexes = %+v", g.EntryIndexes)
	}
}

func TestCompileResolvesRunnerSelectors(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "placement",
		RunnerSelector: &types.RunnerSelector{
			Mode:        types.RunnerSelectorModeRequired,
			MatchLabels: map[string]string{"tenant": "tenant-a", "env": "prod"},
		},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{
				Name: "scan",
				Type: "xflow.function",
				RunnerSelector: &types.RunnerSelector{
					MatchLabels: map[string]string{"mode": "remote"},
				},
			},
		},
		Connections: types.Connections{
			"start": {"main": {{Node: "scan", Input: "main"}}},
		},
	}

	g, err := Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	selector := g.Nodes[g.Index["scan"]].RunnerSelector
	if selector == nil {
		t.Fatal("scan RunnerSelector is nil")
	}
	want := map[string]string{"tenant": "tenant-a", "env": "prod", "mode": "remote"}
	for key, value := range want {
		if got := selector.MatchLabels[key]; got != value {
			t.Fatalf("selector[%s] = %q, want %q", key, got, value)
		}
	}
}

func TestCompileRejectsNodeRunnerSelectorMode(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "invalid-node-mode",
		Nodes: []types.NodeDef{
			{
				Name: "start",
				Type: "xflow.start",
				RunnerSelector: &types.RunnerSelector{
					Mode: types.RunnerSelectorModeRequired,
				},
			},
		},
	}

	if _, err := Compile(def); err == nil {
		t.Fatal("Compile() error = nil, want node runner selector mode rejection")
	}
}

func TestCompileRejectsConflictingRequiredRunnerSelectors(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "conflicting-placement",
		RunnerSelector: &types.RunnerSelector{
			Mode:        types.RunnerSelectorModeRequired,
			MatchLabels: map[string]string{"env": "prod"},
		},
		Nodes: []types.NodeDef{
			{
				Name: "start",
				Type: "xflow.start",
				RunnerSelector: &types.RunnerSelector{
					MatchLabels: map[string]string{"env": "dev"},
				},
			},
		},
	}

	if _, err := Compile(def); err == nil {
		t.Fatal("Compile() error = nil, want conflicting selector rejection")
	}
}

func TestCompile_AllowCyclesDefaultsMaxAutoDepth(t *testing.T) {
	def := &types.WorkflowDef{
		Name:    "cycle",
		Options: &types.WorkflowOptions{AllowCycles: true},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
		},
	}

	g, err := Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	if g.MaxAutoDepth != DefaultMaxAutoDepth {
		t.Fatalf("MaxAutoDepth = %d, want %d", g.MaxAutoDepth, DefaultMaxAutoDepth)
	}
}

func TestCompile_AllowCyclesRequiresExactlyOneStart(t *testing.T) {
	tests := []struct {
		name  string
		nodes []types.NodeDef
	}{
		{
			name:  "missing",
			nodes: []types.NodeDef{{Name: "review", Type: "test.review"}},
		},
		{
			name: "multiple",
			nodes: []types.NodeDef{
				{Name: "start1", Type: "xflow.start"},
				{Name: "start2", Type: "xflow.start"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def := &types.WorkflowDef{
				Name:    tt.name,
				Options: &types.WorkflowOptions{AllowCycles: true},
				Nodes:   tt.nodes,
			}

			if _, err := Compile(def); err == nil {
				t.Fatal("expected start validation error")
			}
		})
	}
}

func TestCompile_AllowCyclesDoesNotRejectTriggerEntry(t *testing.T) {
	def := &types.WorkflowDef{
		Name:    "trigger",
		Options: &types.WorkflowOptions{AllowCycles: true},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "trigger", Type: "xflow.trigger", Kind: types.NodeKindTrigger},
		},
	}

	g, err := Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	if g.EntryIndexes["trigger"] != 1 {
		t.Fatalf("trigger entry index = %d, want 1", g.EntryIndexes["trigger"])
	}
}

func TestCompile_AllowCyclesRejectsWaitAllMerge(t *testing.T) {
	def := &types.WorkflowDef{
		Name:    "merge",
		Options: &types.WorkflowOptions{AllowCycles: true},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "join", Type: "xflow.merge", Parameters: map[string]any{"mode": "wait_all"}},
		},
		Connections: types.Connections{
			"start": {"main": []types.Connection{{Node: "join", Input: "main"}}},
			"join":  {"main": []types.Connection{{Node: "start", Input: "main"}}},
		},
	}

	if _, err := Compile(def); err == nil {
		t.Fatal("expected wait_all merge validation error")
	}
}

func TestCompile_FanOutFanIn(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "diamond",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.start"},
			{Name: "left", Type: "test.left"},
			{Name: "right", Type: "test.right"},
			{Name: "join", Type: "test.join"},
		},
		Connections: types.Connections{
			"start": {"main": []types.Connection{
				{Node: "left", Input: "main"},
				{Node: "right", Input: "main"},
			}},
			"left":  {"main": []types.Connection{{Node: "join", Input: "main"}}},
			"right": {"main": []types.Connection{{Node: "join", Input: "main"}}},
		},
	}

	g, err := Compile(def)
	if err != nil {
		t.Fatal(err)
	}

	joinIdx := g.Index["join"]
	if g.InDegree[joinIdx] != 2 {
		t.Errorf("join should have in-degree 2, got %d", g.InDegree[joinIdx])
	}
}

func TestCompile_PortRouting(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "ports",
		Nodes: []types.NodeDef{
			{Name: "check", Type: "test.check"},
			{Name: "ok", Type: "test.ok"},
			{Name: "fail", Type: "test.fail"},
		},
		Connections: types.Connections{
			"check": {
				"main":  []types.Connection{{Node: "ok", Input: "main"}},
				"error": []types.Connection{{Node: "fail", Input: "main"}},
			},
		},
	}

	g, err := Compile(def)
	if err != nil {
		t.Fatal(err)
	}

	checkIdx := g.Index["check"]
	if len(g.OutEdges[checkIdx]) != 2 {
		t.Errorf("check should have 2 out-edges, got %d", len(g.OutEdges[checkIdx]))
	}

	for _, e := range g.OutEdges[checkIdx] {
		if e.SrcPort != "main" && e.SrcPort != "error" {
			t.Errorf("unexpected port: %s", e.SrcPort)
		}
	}
}

func TestCompile_DuplicateNodeName(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "dup",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.a"},
			{Name: "A", Type: "test.b"},
		},
	}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("expected duplicate node name error")
	}
}

func TestCompile_UnknownConnectionNode(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "unknown",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.a"},
		},
		Connections: types.Connections{
			"A": {"main": []types.Connection{{Node: "Z", Input: "main"}}},
		},
	}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("expected unknown destination node error")
	}
}

func TestCompile_BlocksExperimentalExpandByDefault(t *testing.T) {
	cases := []struct {
		name     string
		nodeType string
	}{
		{name: "loop", nodeType: "xflow.loop"},
		{name: "split", nodeType: "xflow.split"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			def := &types.WorkflowDef{
				Name: tc.name,
				Nodes: []types.NodeDef{
					{Name: "iter", Type: tc.nodeType},
					{Name: "next", Type: "test.echo"},
				},
				Connections: types.Connections{
					"iter": {"main": []types.Connection{{Node: "next", Input: "main"}}},
				},
			}
			_, err := Compile(def)
			if err == nil {
				t.Fatalf("expected experimental-expand gate error for %s", tc.nodeType)
			}
			var gateErr *ErrExperimentalExpandRequired
			if !errors.As(err, &gateErr) {
				t.Fatalf("expected *ErrExperimentalExpandRequired, got %T: %v", err, err)
			}
			if len(gateErr.Nodes) != 1 || gateErr.Nodes[0] != "iter ("+tc.nodeType+")" {
				t.Fatalf("unexpected blocked nodes: %v", gateErr.Nodes)
			}
		})
	}
}

func TestCompile_AllowsExperimentalExpandWhenOptedIn(t *testing.T) {
	def := &types.WorkflowDef{
		Name:    "loop-opt-in",
		Options: &types.WorkflowOptions{ExperimentalExpand: true},
		Nodes: []types.NodeDef{
			{Name: "iter", Type: "xflow.loop"},
			{Name: "next", Type: "test.echo"},
		},
		Connections: types.Connections{
			"iter": {"main": []types.Connection{{Node: "next", Input: "main"}}},
		},
	}
	if _, err := Compile(def); err != nil {
		t.Fatalf("unexpected compile error with experimental opt-in: %v", err)
	}
}

func TestCompile_ExperimentalExpandReportsAllOffendingNodes(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "mixed",
		Nodes: []types.NodeDef{
			{Name: "fan", Type: "xflow.split"},
			{Name: "iter", Type: "xflow.loop"},
			{Name: "ok", Type: "test.echo"},
		},
	}
	_, err := Compile(def)
	var gateErr *ErrExperimentalExpandRequired
	if !errors.As(err, &gateErr) {
		t.Fatalf("expected gate error, got %v", err)
	}
	if len(gateErr.Nodes) != 2 {
		t.Fatalf("expected 2 blocked nodes, got %d: %v", len(gateErr.Nodes), gateErr.Nodes)
	}
}

func TestCompileSnapshotsMutableDefinition(t *testing.T) {
	def := &types.WorkflowDef{
		Name:    "snapshot",
		Version: "v7",
		RunnerSelector: &types.RunnerSelector{
			Mode:        types.RunnerSelectorModeRequired,
			MatchLabels: map[string]string{"workflow": "original"},
		},
		Context: &types.WorkflowContext{
			Vars: map[string]any{
				"nested": map[string]any{"value": "vars-original"},
				"list": []any{
					map[string]any{"value": "vars-list-original"},
					[]string{"one", "two"},
				},
			},
			Config: map[string]any{
				"nested": map[string][]string{"labels": []string{"prod", "critical"}},
			},
		},
		Settings: &types.WorkflowSettings{
			Retry: &types.RetrySettings{Enabled: true, MaxAttempts: 2, Strategy: "exponential"},
		},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{
				Name: "worker",
				Type: "test.worker",
				RunnerSelector: &types.RunnerSelector{
					MatchLabels: map[string]string{"node": "original"},
				},
				Parameters: map[string]any{
					"nested": map[string]any{"value": "params-original"},
					"list": []any{
						map[string]any{"value": "params-list-original"},
						[]string{"one", "two"},
					},
				},
				Retry: &types.RetrySettings{Enabled: true, MaxAttempts: 4, Strategy: "linear"},
			},
		},
		Connections: types.Connections{
			"start": {"main": []types.Connection{{Node: "worker", Input: "main"}}},
		},
	}

	g, err := Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	originalHash := g.GraphHash

	def.Context.Vars["new"] = "changed"
	def.Context.Vars["nested"].(map[string]any)["value"] = "changed"
	def.Context.Vars["list"].([]any)[0].(map[string]any)["value"] = "changed"
	def.Context.Vars["list"].([]any)[1].([]string)[0] = "changed"
	def.Context.Config["new"] = "changed"
	def.Context.Config["nested"].(map[string][]string)["labels"][0] = "changed"
	def.RunnerSelector.MatchLabels["workflow"] = "changed"
	def.Settings.Retry.MaxAttempts = 99
	def.Nodes[1].RunnerSelector.MatchLabels["node"] = "changed"
	def.Nodes[1].Parameters["nested"].(map[string]any)["value"] = "changed"
	def.Nodes[1].Parameters["list"].([]any)[0].(map[string]any)["value"] = "changed"
	def.Nodes[1].Parameters["list"].([]any)[1].([]string)[0] = "changed"
	def.Nodes[1].Retry.MaxAttempts = 99

	t.Run("context maps and nested values", func(t *testing.T) {
		if _, ok := g.Vars["new"]; ok {
			t.Fatal("Vars contains mutation made after Compile")
		}
		if got := g.Vars["nested"].(map[string]any)["value"]; got != "vars-original" {
			t.Fatalf("Vars nested value = %q, want %q", got, "vars-original")
		}
		if got := g.Vars["list"].([]any)[0].(map[string]any)["value"]; got != "vars-list-original" {
			t.Fatalf("Vars nested list map value = %q, want %q", got, "vars-list-original")
		}
		if got := g.Vars["list"].([]any)[1].([]string)[0]; got != "one" {
			t.Fatalf("Vars nested list slice value = %q, want %q", got, "one")
		}
		if _, ok := g.Config["new"]; ok {
			t.Fatal("Config contains mutation made after Compile")
		}
		if got := g.Config["nested"].(map[string][]string)["labels"][0]; got != "prod" {
			t.Fatalf("Config nested slice value = %q, want %q", got, "prod")
		}
	})

	t.Run("node metadata", func(t *testing.T) {
		start := g.Nodes[g.Index["start"]]
		worker := g.Nodes[g.Index["worker"]]
		if got := start.RunnerSelector.MatchLabels["workflow"]; got != "original" {
			t.Fatalf("start workflow selector = %q, want %q", got, "original")
		}
		if got := worker.RunnerSelector.MatchLabels["workflow"]; got != "original" {
			t.Fatalf("worker workflow selector = %q, want %q", got, "original")
		}
		if got := worker.RunnerSelector.MatchLabels["node"]; got != "original" {
			t.Fatalf("worker node selector = %q, want %q", got, "original")
		}
		if got := start.Retry.MaxAttempts; got != 2 {
			t.Fatalf("inherited retry MaxAttempts = %d, want %d", got, 2)
		}
		if got := worker.Retry.MaxAttempts; got != 4 {
			t.Fatalf("node retry MaxAttempts = %d, want %d", got, 4)
		}
		if got := worker.Parameters["nested"].(map[string]any)["value"]; got != "params-original" {
			t.Fatalf("Parameters nested value = %q, want %q", got, "params-original")
		}
		if got := worker.Parameters["list"].([]any)[0].(map[string]any)["value"]; got != "params-list-original" {
			t.Fatalf("Parameters nested list map value = %q, want %q", got, "params-list-original")
		}
		if got := worker.Parameters["list"].([]any)[1].([]string)[0]; got != "one" {
			t.Fatalf("Parameters nested list slice value = %q, want %q", got, "one")
		}
	})

	t.Run("hash remains a snapshot", func(t *testing.T) {
		if g.GraphHash == "" {
			t.Fatal("GraphHash is empty")
		}
		if g.GraphHash != originalHash {
			t.Fatalf("GraphHash = %q, want original %q", g.GraphHash, originalHash)
		}
		updated, err := Compile(def)
		if err != nil {
			t.Fatal(err)
		}
		if updated.GraphHash == g.GraphHash {
			t.Fatalf("GraphHash = %q after source mutation, want a new compiled graph hash", updated.GraphHash)
		}
	})
}

func TestCompileGraphMetadataIsStable(t *testing.T) {
	def := &types.WorkflowDef{
		Name:    "metadata",
		Version: "v2",
		Context: &types.WorkflowContext{
			Vars: map[string]any{"second": "value", "first": "value"},
		},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "left", Type: "test.left"},
			{Name: "right", Type: "test.right"},
			{Name: "join", Type: "test.join"},
		},
		Connections: types.Connections{
			"start": {
				"second": []types.Connection{{Node: "right", Input: "main"}},
				"first":  []types.Connection{{Node: "left", Input: "main"}},
			},
			"left":  {"main": []types.Connection{{Node: "join", Input: "main"}}},
			"right": {"main": []types.Connection{{Node: "join", Input: "main"}}},
		},
	}

	var graphHash string
	for i := 0; i < 10; i++ {
		g, err := Compile(def)
		if err != nil {
			t.Fatal(err)
		}
		if g.WorkflowVersion != def.Version {
			t.Fatalf("WorkflowVersion = %q, want %q", g.WorkflowVersion, def.Version)
		}
		if g.CompilerVersion != compilerVersion {
			t.Fatalf("CompilerVersion = %q, want %q", g.CompilerVersion, compilerVersion)
		}
		if g.GraphHash == "" {
			t.Fatal("GraphHash is empty")
		}
		if i == 0 {
			graphHash = g.GraphHash
			continue
		}
		if g.GraphHash != graphHash {
			t.Fatalf("GraphHash = %q, want stable value %q", g.GraphHash, graphHash)
		}
	}
}
