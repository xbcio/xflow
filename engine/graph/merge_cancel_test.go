package graph

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

func mcWorker(name string) types.NodeDef { return types.NodeDef{Name: name, Type: "test.worker"} }

func mcMerge(name string, params map[string]any) types.NodeDef {
	return types.NodeDef{Name: name, Type: "xflow.merge", Parameters: params}
}

// mcDef assembles a workflow from nodes and from->to edges on the main port.
func mcDef(cyclic bool, nodes []types.NodeDef, edges ...[2]string) *types.WorkflowDef {
	def := &types.WorkflowDef{Name: "merge-cancel", Nodes: nodes, Connections: types.Connections{}}
	if cyclic {
		def.Options = &types.WorkflowOptions{AllowCycles: true}
	}
	for _, e := range edges {
		ports := def.Connections[e[0]]
		if ports == nil {
			ports = map[string]types.PortConnections{}
			def.Connections[e[0]] = ports
		}
		pc := ports["main"]
		pc.Targets = append(pc.Targets, types.Connection{Node: e[1], Input: "main"})
		ports["main"] = pc
	}
	return def
}

// mcDiamond is start -> {left,right} -> join -> tail with join's parameters
// supplied by the caller.
func mcDiamond(joinParams map[string]any) *types.WorkflowDef {
	return mcDef(false,
		[]types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			mcWorker("left"), mcWorker("right"),
			mcMerge("join", joinParams),
			mcWorker("tail"),
		},
		[2]string{"start", "left"}, [2]string{"start", "right"},
		[2]string{"left", "join"}, [2]string{"right", "join"},
		[2]string{"join", "tail"},
	)
}

func mcCompile(t *testing.T, def *types.WorkflowDef) *Graph {
	t.Helper()
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return g
}

func mcIdx(t *testing.T, g *Graph, name string) int {
	t.Helper()
	idx, ok := g.NodeIndex(name)
	if !ok {
		t.Fatalf("node %q not in graph", name)
	}
	return idx
}

func mcNames(g *Graph, idxs []int) []string {
	out := make([]string, len(idxs))
	for i, idx := range idxs {
		out[i] = g.NodeName(idx)
	}
	return out
}

// mcAssertTopological fails when some edge between two closure members points
// from a later entry to an earlier one, or when an entry repeats.
func mcAssertTopological(t *testing.T, g *Graph, closure []int) {
	t.Helper()
	pos := make(map[int]int, len(closure))
	for i, idx := range closure {
		if _, dup := pos[idx]; dup {
			t.Fatalf("closure repeats %q: %v", g.NodeName(idx), mcNames(g, closure))
		}
		pos[idx] = i
	}
	for _, idx := range closure {
		for _, e := range g.NodeInEdges(idx) {
			if p, in := pos[e.SrcIdx]; in && p >= pos[idx] {
				t.Fatalf("%q precedes its ancestor %q in %v", g.NodeName(idx), g.NodeName(e.SrcIdx), mcNames(g, closure))
			}
		}
	}
}

func mcSorted(names []string) []string {
	out := slices.Clone(names)
	slices.Sort(out)
	return out
}

func TestMergeCancelsOthersResolution(t *testing.T) {
	cases := []struct {
		name   string
		params map[string]any
		want   bool
	}{
		{"default is cancel", map[string]any{"mode": "wait_any"}, true},
		{"empty string is cancel", map[string]any{"mode": "wait_any", "on_others": ""}, true},
		{"nil value is cancel", map[string]any{"mode": "wait_any", "on_others": nil}, true},
		{"explicit cancel", map[string]any{"mode": "wait_any", "on_others": "cancel"}, true},
		{"detach", map[string]any{"mode": "wait_any", "on_others": "detach"}, false},
		{"wait_all", map[string]any{"mode": "wait_all"}, false},
		{"wait_all with cancel", map[string]any{"mode": "wait_all", "on_others": "cancel"}, false},
		{"wait_all ignores on_others", map[string]any{"mode": "wait_all", "on_others": "bogus"}, false},
		{"no mode", map[string]any{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := mcCompile(t, mcDiamond(tc.params))
			if got := g.MergeCancelsOthers(mcIdx(t, g, "join")); got != tc.want {
				t.Fatalf("MergeCancelsOthers(join) = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMergeCancelsOthersOnlyForMergeNodes(t *testing.T) {
	g := mcCompile(t, mcDiamond(map[string]any{"mode": "wait_any"}))
	for _, name := range []string{"start", "left", "tail"} {
		if g.MergeCancelsOthers(mcIdx(t, g, name)) {
			t.Fatalf("MergeCancelsOthers(%s) = true, want false for a non-merge node", name)
		}
	}
	for _, idx := range []int{-1, g.NodeCount(), g.NodeCount() + 7} {
		if g.MergeCancelsOthers(idx) {
			t.Fatalf("MergeCancelsOthers(%d) = true, want false out of range", idx)
		}
	}
}

func TestCompileRejectsInvalidMergeOnOthers(t *testing.T) {
	for _, bad := range []any{"Cancel", "CANCEL", "abort", "drop", 1, true, []any{}, "${{ $config.policy }}"} {
		def := mcDiamond(map[string]any{"mode": "wait_any", "on_others": bad})
		_, err := Compile(def)
		if err == nil {
			t.Errorf("on_others=%#v compiled clean, want a compile error", bad)
			continue
		}
		msg := err.Error()
		for _, want := range []string{`"join"`, "on_others", "cancel", "detach"} {
			if !strings.Contains(msg, want) {
				t.Errorf("on_others=%#v: error %q does not mention %q", bad, msg, want)
			}
		}
		if s, ok := bad.(string); ok && strings.Contains(msg, s) {
			t.Errorf("on_others=%#v: error %q echoes the value", bad, msg)
		}
	}
}

// on_others is only read by a wait_any merge; the Descriptor hides it for any
// other mode, so a stray value there must keep compiling.
func TestCompileIgnoresOnOthersOutsideWaitAny(t *testing.T) {
	for _, params := range []map[string]any{
		{"mode": "wait_all", "on_others": "bogus"},
		{"mode": "wait_all", "on_others": 7},
	} {
		if _, err := Compile(mcDiamond(params)); err != nil {
			t.Errorf("params %v: Compile: %v", params, err)
		}
	}
	for _, ok := range []string{"", "cancel", "detach"} {
		if _, err := Compile(mcDiamond(map[string]any{"mode": "wait_any", "on_others": ok})); err != nil {
			t.Errorf("on_others=%q: Compile: %v", ok, err)
		}
	}
}

// mcCyclic is start -> {a,b} -> join -> a, a cyclic workflow whose wait_any
// merge sits on a loop.
func mcCyclic(joinParams map[string]any) *types.WorkflowDef {
	return mcDef(true,
		[]types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			mcWorker("a"), mcWorker("b"),
			mcMerge("join", joinParams),
		},
		[2]string{"start", "a"}, [2]string{"start", "b"},
		[2]string{"a", "join"}, [2]string{"b", "join"},
		[2]string{"join", "a"},
	)
}

func mcOnOthersWarnings(g *Graph) []string {
	var out []string
	for _, w := range g.Warnings() {
		if strings.Contains(w, "on_others") {
			out = append(out, w)
		}
	}
	return out
}

func TestCyclicMergeCancelDegradesToDetachWithWarning(t *testing.T) {
	g := mcCompile(t, mcCyclic(map[string]any{"mode": "wait_any", "on_others": "cancel"}))
	join := mcIdx(t, g, "join")
	if g.MergeCancelsOthers(join) {
		t.Fatal("MergeCancelsOthers = true in a cyclic graph, want false")
	}
	if got := g.CancelClosure(join); got != nil {
		t.Fatalf("CancelClosure = %v in a cyclic graph, want nil", mcNames(g, got))
	}
	warnings := mcOnOthersWarnings(g)
	if len(warnings) != 1 || !strings.Contains(warnings[0], `"join"`) {
		t.Fatalf("on_others warnings = %q, want exactly one naming join", warnings)
	}
}

// The warning is about an explicit request that gets ignored. A merge that says
// nothing, or says detach, asked for nothing the graph cannot give.
func TestMergeCancelWarningNeedsCyclicGraphAndExplicitCancel(t *testing.T) {
	cases := []struct {
		name string
		def  *types.WorkflowDef
	}{
		{"cyclic default", mcCyclic(map[string]any{"mode": "wait_any"})},
		{"cyclic empty", mcCyclic(map[string]any{"mode": "wait_any", "on_others": ""})},
		{"cyclic detach", mcCyclic(map[string]any{"mode": "wait_any", "on_others": "detach"})},
		{"acyclic explicit cancel", mcDiamond(map[string]any{"mode": "wait_any", "on_others": "cancel"})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := mcCompile(t, tc.def)
			if w := mcOnOthersWarnings(g); len(w) != 0 {
				t.Fatalf("on_others warnings = %q, want none", w)
			}
		})
	}
}

func TestCancelClosureDiamond(t *testing.T) {
	g := mcCompile(t, mcDiamond(map[string]any{"mode": "wait_any"}))
	closure := g.CancelClosure(mcIdx(t, g, "join"))
	if got, want := mcSorted(mcNames(g, closure)), []string{"left", "right", "start"}; !slices.Equal(got, want) {
		t.Fatalf("closure = %v, want %v", got, want)
	}
	mcAssertTopological(t, g, closure)
	if closure[0] != mcIdx(t, g, "start") {
		t.Fatalf("first = %q, want start", g.NodeName(closure[0]))
	}
	if got := g.CancelClosure(mcIdx(t, g, "start")); got != nil {
		t.Fatalf("closure of a root = %v, want nil", mcNames(g, got))
	}
}

// A node reaching the merge through two ports, or through two paths, appears
// once.
func TestCancelClosureHasNoDuplicates(t *testing.T) {
	def := mcDef(false,
		[]types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			mcWorker("a"), mcWorker("b"), mcMerge("join", map[string]any{"mode": "wait_any"}),
		},
		[2]string{"start", "a"}, [2]string{"start", "b"},
		[2]string{"a", "b"}, [2]string{"a", "join"}, [2]string{"b", "join"},
	)
	def.Connections["a"]["alt"] = types.PortConnections{Targets: []types.Connection{{Node: "join", Input: "alt"}}}
	g := mcCompile(t, def)
	closure := g.CancelClosure(mcIdx(t, g, "join"))
	if got, want := mcSorted(mcNames(g, closure)), []string{"a", "b", "start"}; !slices.Equal(got, want) {
		t.Fatalf("closure = %v, want %v", got, want)
	}
	mcAssertTopological(t, g, closure)
}

// An inner wait_any merge that lost its race is an ancestor of the outer
// merge, so the outer closure covers the inner branches too (design: the outer
// fire ends the larger race).
func TestCancelClosureNestedMergesUnion(t *testing.T) {
	waitAny := map[string]any{"mode": "wait_any"}
	def := mcDef(false,
		[]types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			mcWorker("a1"), mcWorker("a2"), mcMerge("inner", waitAny),
			mcWorker("b"), mcMerge("outer", waitAny),
		},
		[2]string{"start", "a1"}, [2]string{"start", "a2"}, [2]string{"start", "b"},
		[2]string{"a1", "inner"}, [2]string{"a2", "inner"},
		[2]string{"inner", "outer"}, [2]string{"b", "outer"},
	)
	g := mcCompile(t, def)

	outer := g.CancelClosure(mcIdx(t, g, "outer"))
	if got, want := mcSorted(mcNames(g, outer)), []string{"a1", "a2", "b", "inner", "start"}; !slices.Equal(got, want) {
		t.Fatalf("outer closure = %v, want %v", got, want)
	}
	mcAssertTopological(t, g, outer)

	inner := g.CancelClosure(mcIdx(t, g, "inner"))
	if got, want := mcSorted(mcNames(g, inner)), []string{"a1", "a2", "start"}; !slices.Equal(got, want) {
		t.Fatalf("inner closure = %v, want %v", got, want)
	}
	mcAssertTopological(t, g, inner)

	for _, name := range []string{"inner", "outer"} {
		if !g.MergeCancelsOthers(mcIdx(t, g, name)) {
			t.Errorf("MergeCancelsOthers(%s) = false, want true", name)
		}
	}
}

// The closure is the merge's ancestors, whatever else they feed. A fork that
// also reaches a consumer outside the closure stays in it, and the consumer
// does not.
func TestCancelClosureKeepsSharedForkAndExcludesOutsideConsumers(t *testing.T) {
	def := mcDef(false,
		[]types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			mcWorker("fork"), mcWorker("quick"),
			mcMerge("join", map[string]any{"mode": "wait_any"}),
			mcWorker("outside"), mcWorker("after"),
		},
		[2]string{"start", "fork"}, [2]string{"start", "quick"},
		[2]string{"fork", "join"}, [2]string{"quick", "join"},
		[2]string{"fork", "outside"},
		[2]string{"join", "after"},
	)
	g := mcCompile(t, def)
	closure := g.CancelClosure(mcIdx(t, g, "join"))
	if got, want := mcSorted(mcNames(g, closure)), []string{"fork", "quick", "start"}; !slices.Equal(got, want) {
		t.Fatalf("closure = %v, want %v", got, want)
	}
	mcAssertTopological(t, g, closure)
}

// A wide graph where DFS discovery order disagrees with the node order, so a
// closure sorted by index or by discovery would put a descendant first.
func TestCancelClosureIsTopologicalNotIndexOrdered(t *testing.T) {
	waitAny := map[string]any{"mode": "wait_any"}
	// Declared deepest-first, so every edge runs from a higher index to a
	// lower one.
	def := mcDef(false,
		[]types.NodeDef{
			mcMerge("join", waitAny),
			mcWorker("d"), mcWorker("c"), mcWorker("b"), mcWorker("x"),
			{Name: "start", Type: "xflow.start"},
		},
		[2]string{"start", "b"}, [2]string{"start", "x"},
		[2]string{"b", "c"}, [2]string{"c", "d"}, [2]string{"x", "d"},
		[2]string{"d", "join"}, [2]string{"x", "join"},
	)
	g := mcCompile(t, def)
	closure := g.CancelClosure(mcIdx(t, g, "join"))
	if got, want := mcSorted(mcNames(g, closure)), []string{"b", "c", "d", "start", "x"}; !slices.Equal(got, want) {
		t.Fatalf("closure = %v, want %v", got, want)
	}
	mcAssertTopological(t, g, closure)
}

// A self-loop, and a longer loop, in a cyclic graph: the closure is empty by
// contract and the call returns.
func TestCancelClosureSelfLoopInCyclicGraphReturns(t *testing.T) {
	def := mcDef(true,
		[]types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			mcWorker("spin"),
			mcMerge("join", map[string]any{"mode": "wait_any"}),
		},
		[2]string{"start", "spin"}, [2]string{"spin", "spin"}, [2]string{"spin", "join"},
	)
	g := mcCompile(t, def)
	done := make(chan []int, 1)
	go func() { done <- g.CancelClosure(mcIdx(t, g, "join")) }()
	select {
	case got := <-done:
		if got != nil {
			t.Fatalf("closure = %v, want nil", mcNames(g, got))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CancelClosure did not return on a self-loop")
	}
}

// Compile rejects cycles in an acyclic graph, but the walk must not trust that:
// a hand-built graph with a loop under allowCycles=false still terminates and
// lists each ancestor once.
func TestCancelClosureTerminatesOnCyclicInEdges(t *testing.T) {
	g := &Graph{
		nodes: make([]NodeMeta, 4),
		inEdges: [][]Edge{
			0: {{SrcIdx: 1, DstIdx: 0}, {SrcIdx: 0, DstIdx: 0}}, // self-loop on the target
			1: {{SrcIdx: 2, DstIdx: 1}},
			2: {{SrcIdx: 1, DstIdx: 2}, {SrcIdx: 3, DstIdx: 2}}, // 1 <-> 2
			3: nil,
		},
	}
	done := make(chan []int, 1)
	go func() { done <- g.CancelClosure(0) }()
	select {
	case got := <-done:
		if want := []int{3, 2, 1}; !slices.Equal(mcSorted2(got), mcSorted2(want)) {
			t.Fatalf("closure = %v, want the set %v", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CancelClosure did not return on cyclic in-edges")
	}
}

func mcSorted2(in []int) []int {
	out := slices.Clone(in)
	slices.Sort(out)
	return out
}

// The result belongs to the caller and the graph is untouched by the call.
func TestCancelClosureReturnsCallerOwnedSlice(t *testing.T) {
	g := mcCompile(t, mcDiamond(map[string]any{"mode": "wait_any"}))
	join := mcIdx(t, g, "join")
	first := g.CancelClosure(join)
	want := slices.Clone(first)
	for i := range first {
		first[i] = -1
	}
	if got := g.CancelClosure(join); !slices.Equal(got, want) {
		t.Fatalf("second call = %v, want %v after the caller scribbled on the first", got, want)
	}
}

// The policy is read from NodeMeta.Parameters, which already travels in the
// snapshot, so a decoded graph answers like the compiled one and the graph
// hash does not depend on how the policy was spelled.
func TestMergeCancelSurvivesSnapshotRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params map[string]any
		want   bool
	}{
		{"default", map[string]any{"mode": "wait_any"}, true},
		{"detach", map[string]any{"mode": "wait_any", "on_others": "detach"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := mcCompile(t, mcDiamond(tc.params))
			data, err := json.Marshal(g)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var g2 Graph
			if err := json.Unmarshal(data, &g2); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			join := mcIdx(t, &g2, "join")
			if got := g2.MergeCancelsOthers(join); got != tc.want {
				t.Fatalf("decoded MergeCancelsOthers = %v, want %v", got, tc.want)
			}
			if got, want := mcSorted(mcNames(&g2, g2.CancelClosure(join))), mcSorted(mcNames(g, g.CancelClosure(join))); !slices.Equal(got, want) {
				t.Fatalf("decoded closure = %v, want %v", got, want)
			}
			if g2.Hash() != g.Hash() {
				t.Fatalf("decoded hash %q != compiled hash %q", g2.Hash(), g.Hash())
			}
		})
	}
}
