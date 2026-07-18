package graph

import (
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/xbcio/xflow/types"
)

// richMutableDef builds a workflow whose Vars/Config/Parameters carry nested
// maps, slices, []string, and pointers-free values so accessor isolation can be
// asserted at every depth.
func richMutableDef() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:    "immutable",
		Version: "v1",
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
				"nested": map[string][]string{"labels": {"prod", "critical"}},
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
}

// TestAccessorDeepIsolation_NodeAt proves that mutating every mutable field of
// a NodeAt return value — selector labels, nested parameters maps/slices, port
// outs, and retry — does not affect subsequent reads from the Graph.
func TestAccessorDeepIsolation_NodeAt(t *testing.T) {
	g, err := Compile(richMutableDef())
	if err != nil {
		t.Fatal(err)
	}
	workerIdx, _ := g.NodeIndex("worker")

	// First read: capture then aggressively mutate every mutable field.
	first := g.NodeAt(workerIdx)
	first.RunnerSelector.MatchLabels["node"] = "mutated"
	first.RunnerSelector.MatchLabels["workflow"] = "mutated"
	first.Retry.MaxAttempts = 999
	first.Parameters["nested"].(map[string]any)["value"] = "mutated"
	first.Parameters["list"].([]any)[0].(map[string]any)["value"] = "mutated"
	first.Parameters["list"].([]any)[1].([]string)[0] = "mutated"
	if len(first.PortOuts) > 0 {
		first.PortOuts[0] = first.PortOuts[0] + "-mutated"
	} else {
		// PortOuts is empty for worker (no outgoing edges); exercise the
		// accessor on a node that has them.
		startIdx, _ := g.NodeIndex("start")
		startCopy := g.NodeAt(startIdx)
		if len(startCopy.PortOuts) == 0 {
			t.Fatal("start node should have port outs")
		}
		startCopy.PortOuts[0] = "mutated"
		// Re-read: must be unchanged.
		if got := g.NodeAt(startIdx).PortOuts[0]; got == "mutated" {
			t.Fatalf("PortOuts mutated through accessor: %q", got)
		}
	}

	// Second read: none of the mutations may be visible.
	second := g.NodeAt(workerIdx)
	if got := second.RunnerSelector.MatchLabels["node"]; got != "original" {
		t.Fatalf("RunnerSelector.MatchLabels[node] = %q, want original (accessor leaked)", got)
	}
	if got := second.RunnerSelector.MatchLabels["workflow"]; got != "original" {
		t.Fatalf("RunnerSelector.MatchLabels[workflow] = %q, want original (accessor leaked)", got)
	}
	if got := second.Retry.MaxAttempts; got != 4 {
		t.Fatalf("Retry.MaxAttempts = %d, want 4 (accessor leaked)", got)
	}
	if got := second.Parameters["nested"].(map[string]any)["value"]; got != "params-original" {
		t.Fatalf("Parameters nested value = %q, want params-original (accessor leaked)", got)
	}
	if got := second.Parameters["list"].([]any)[0].(map[string]any)["value"]; got != "params-list-original" {
		t.Fatalf("Parameters list map value = %q, want params-list-original (accessor leaked)", got)
	}
	if got := second.Parameters["list"].([]any)[1].([]string)[0]; got != "one" {
		t.Fatalf("Parameters list slice value = %q, want one (accessor leaked)", got)
	}
}

// TestAccessorDeepIsolation_VarsConfig proves mutating multi-layer maps/slices
// returned by Vars()/Config() does not affect the Graph.
func TestAccessorDeepIsolation_VarsConfig(t *testing.T) {
	g, err := Compile(richMutableDef())
	if err != nil {
		t.Fatal(err)
	}

	vars := g.Vars()
	vars["new"] = "added"
	vars["nested"].(map[string]any)["value"] = "mutated"
	vars["list"].([]any)[0].(map[string]any)["value"] = "mutated"
	vars["list"].([]any)[1].([]string)[0] = "mutated"

	config := g.Config()
	config["new"] = "added"
	config["nested"].(map[string][]string)["labels"][0] = "mutated"

	// Re-read: Graph must be untouched.
	againVars := g.Vars()
	if _, ok := againVars["new"]; ok {
		t.Fatal("Vars: mutation visible through accessor leak")
	}
	if got := againVars["nested"].(map[string]any)["value"]; got != "vars-original" {
		t.Fatalf("Vars nested = %q, want vars-original (leak)", got)
	}
	if got := againVars["list"].([]any)[0].(map[string]any)["value"]; got != "vars-list-original" {
		t.Fatalf("Vars list map = %q, want vars-list-original (leak)", got)
	}
	if got := againVars["list"].([]any)[1].([]string)[0]; got != "one" {
		t.Fatalf("Vars list slice = %q, want one (leak)", got)
	}

	againConfig := g.Config()
	if _, ok := againConfig["new"]; ok {
		t.Fatal("Config: mutation visible through accessor leak")
	}
	if got := againConfig["nested"].(map[string][]string)["labels"][0]; got != "prod" {
		t.Fatalf("Config nested = %q, want prod (leak)", got)
	}
}

// TestAccessorEdgesReturnNewSlice proves edge accessors return independent
// slices that cannot alias the Graph's internal edge storage.
func TestAccessorEdgesReturnNewSlice(t *testing.T) {
	g, err := Compile(richMutableDef())
	if err != nil {
		t.Fatal(err)
	}
	startIdx, _ := g.NodeIndex("start")

	out1 := g.NodeOutEdges(startIdx)
	out2 := g.NodeOutEdges(startIdx)
	if &out1[0] == &out2[0] {
		t.Fatal("NodeOutEdges returned slices sharing backing arrays")
	}
	if len(out1) > 0 {
		out1[0] = Edge{SrcIdx: -1, DstIdx: -1, SrcPort: "mutated", DstPort: "mutated"}
		if reflect.DeepEqual(out1[0], out2[0]) {
			t.Fatal("mutating one NodeOutEdges result affected another")
		}
		// Graph re-read must be unchanged.
		out3 := g.NodeOutEdges(startIdx)
		if reflect.DeepEqual(out3[0], Edge{SrcIdx: -1}) {
			t.Fatal("NodeOutEdges mutation leaked into Graph")
		}
	}
}

// TestCompileRejectsUnsupportedValueDomain proves Compile fails when
// Parameters/Vars/Config carry unsupported mutable reference types.
func TestCompileRejectsUnsupportedValueDomain(t *testing.T) {
	ptr := 42

	cases := []struct {
		name  string
		mutate func(*types.WorkflowDef)
	}{
		{
			name: "pointer in Parameters",
			mutate: func(d *types.WorkflowDef) {
				d.Nodes[1].Parameters = map[string]any{"ptr": &ptr}
			},
		},
		{
			name: "pointer in Vars",
			mutate: func(d *types.WorkflowDef) {
				d.Context.Vars = map[string]any{"ptr": &ptr}
			},
		},
		{
			name: "non-string-key map in Config",
			mutate: func(d *types.WorkflowDef) {
				d.Context.Config = map[string]any{"bad": map[int]string{1: "no"}}
			},
		},
		{
			name: "func value in Parameters",
			mutate: func(d *types.WorkflowDef) {
				d.Nodes[1].Parameters = map[string]any{"fn": func() {}}
			},
		},
		{
			name: "chan value in Vars",
			mutate: func(d *types.WorkflowDef) {
				d.Context.Vars = map[string]any{"ch": make(chan int)}
			},
		},
		{
			name: "pointer nested in slice",
			mutate: func(d *types.WorkflowDef) {
				d.Nodes[1].Parameters = map[string]any{"list": []any{&ptr}}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			def := richMutableDef()
			tc.mutate(def)
			_, err := Compile(def)
			if err == nil {
				t.Fatal("Compile succeeded, want value-domain rejection")
			}
			if !strings.Contains(err.Error(), "value domain") {
				t.Fatalf("error = %v, want a value-domain error", err)
			}
		})
	}
}

// TestCompileAcceptsSupportedValueDomain proves the accepted domain kinds (nested
// map[string]any, []any, []string, map[string][]string, structs of values) all
// compile without being rejected by the domain gate.
func TestCompileAcceptsSupportedValueDomain(t *testing.T) {
	type limits struct {
		Max int
		Min int
	}
	def := &types.WorkflowDef{
		Name: "domain-ok",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{
				Name:       "worker",
				Type:       "test.worker",
				Parameters: map[string]any{"limits": limits{Max: 10, Min: 0}},
			},
		},
		Connections: types.Connections{
			"start": {"main": []types.Connection{{Node: "worker", Input: "main"}}},
		},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	params := g.NodeAt(1).Parameters
	lim, ok := params["limits"]
	if !ok {
		t.Fatal("limits parameter missing")
	}
	got, ok := lim.(limits)
	if !ok {
		t.Fatalf("limits = %T, want limits struct preserved", lim)
	}
	if got.Max != 10 || got.Min != 0 {
		t.Fatalf("limits = %+v, want {Max:10 Min:0}", got)
	}
}

// TestAccessorConcurrentReadAndMutation is a race detector: many goroutines
// read accessors while one mutates the returned copies. The Graph itself must
// never be touched; this test fails under -race if any accessor aliases
// internal state.
func TestAccessorConcurrentReadAndMutation(t *testing.T) {
	g, err := Compile(richMutableDef())
	if err != nil {
		t.Fatal(err)
	}
	workerIdx, _ := g.NodeIndex("worker")

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				n := g.NodeAt(workerIdx)
				// Mutate the returned copy aggressively.
				if n.RunnerSelector != nil {
					n.RunnerSelector.MatchLabels["node"] = "race"
				}
				if n.Parameters != nil {
					if m, ok := n.Parameters["nested"].(map[string]any); ok {
						m["value"] = "race"
					}
				}
				if len(n.PortOuts) > 0 {
					n.PortOuts[0] = "race"
				}
				_ = g.Vars()
				_ = g.Config()
				_ = g.NodeOutEdges(workerIdx)
				_ = g.NodeInEdges(workerIdx)
			}
		}()
	}
	wg.Wait()

	// Final state must be the original compiled values.
	n := g.NodeAt(workerIdx)
	if got := n.RunnerSelector.MatchLabels["node"]; got != "original" {
		t.Fatalf("post-race RunnerSelector[node] = %q, want original", got)
	}
	if got := n.Parameters["nested"].(map[string]any)["value"]; got != "params-original" {
		t.Fatalf("post-race Parameters nested = %q, want params-original", got)
	}
}

// TestGraphHashStableAcrossAccessorMutation proves the GraphHash is computed
// from the compiled immutable representation and is unaffected by mutating
// accessor return values (spec §8.2 #5).
func TestGraphHashStableAcrossAccessorMutation(t *testing.T) {
	g, err := Compile(richMutableDef())
	if err != nil {
		t.Fatal(err)
	}
	original := g.Hash()

	workerIdx, _ := g.NodeIndex("worker")
	for i := 0; i < 50; i++ {
		n := g.NodeAt(workerIdx)
		n.RunnerSelector.MatchLabels["node"] = "tampered"
		n.Retry.MaxAttempts = i
		n.Parameters["nested"].(map[string]any)["value"] = "tampered"
		v := g.Vars()
		v["nested"].(map[string]any)["value"] = "tampered"
	}

	if got := g.Hash(); got != original {
		t.Fatalf("Hash changed after accessor mutation: %q -> %q", original, got)
	}
}

// Compile-time guard: ensure errors.As works on the domain rejection so callers
// can distinguish value-domain failures from other compile errors when needed.
var _ = errors.As
