//go:build perf

// Package perf contains performance benchmarks for the xflow scheduler.
// Run with: go test -tags=perf -bench=BenchmarkScheduler -benchtime=2s ./test/perf/ -timeout 5m
package perf

import (
	"context"
	"fmt"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/types"
)

// ── noop handler ──────────────────────────────────────────────────────────────

// noopHandler is a zero-overhead action handler used across all benchmarks.
// It immediately returns an empty output without touching any external resource.
type noopHandler struct{}

func (h *noopHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "bench.noop"}
}

func (h *noopHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return &types.Output{}, nil
}

// ── topology builders: graph.Compile path (pure WorkflowDef) ─────────────────

// linearChainDef builds a WorkflowDef with n nodes connected in a single chain:
//
//	n0 → n1 → n2 → … → n(n-1)
func linearChainDef(n int) *types.WorkflowDef {
	nodes := make([]types.NodeDef, n)
	for i := range nodes {
		nodes[i] = types.NodeDef{Name: nodeName(i), Type: "bench.noop"}
	}
	conns := make(types.Connections, n-1)
	for i := 0; i < n-1; i++ {
		conns[nodeName(i)] = map[string]types.PortConnections{
			"main": {Targets: []types.Connection{{Node: nodeName(i + 1), Input: "main"}}},
		}
	}
	return &types.WorkflowDef{Name: "linear", Nodes: nodes, Connections: conns}
}

// fanOutDef builds a WorkflowDef with one hub connected to n-1 leaf nodes:
//
//	hub → leaf0, leaf1, …, leaf(n-2)
func fanOutDef(n int) *types.WorkflowDef {
	nodes := make([]types.NodeDef, n)
	nodes[0] = types.NodeDef{Name: "hub", Type: "bench.noop"}
	dsts := make([]types.Connection, n-1)
	for i := 1; i < n; i++ {
		name := fmt.Sprintf("leaf%d", i-1)
		nodes[i] = types.NodeDef{Name: name, Type: "bench.noop"}
		dsts[i-1] = types.Connection{Node: name, Input: "main"}
	}
	conns := types.Connections{
		"hub": {"main": types.PortConnections{Targets: dsts}},
	}
	return &types.WorkflowDef{Name: "fanout", Nodes: nodes, Connections: conns}
}

// fanInOutDef builds a WorkflowDef with n-2 source nodes, one merge node, and
// one sink node:
//
//	src0 ─┐
//	src1 ─┤→ merge → sink
//	…    ─┘
//
// This exercises both fan-in (many inputs to merge) and fan-out scheduling
// paths in graph.Compile and the engine scheduler.
func fanInOutDef(n int) *types.WorkflowDef {
	if n < 3 {
		n = 3
	}
	srcCount := n - 2
	nodes := make([]types.NodeDef, 0, n)
	for i := 0; i < srcCount; i++ {
		nodes = append(nodes, types.NodeDef{Name: fmt.Sprintf("src%d", i), Type: "bench.noop"})
	}
	nodes = append(nodes, types.NodeDef{Name: "merge", Type: "bench.noop"})
	nodes = append(nodes, types.NodeDef{Name: "sink", Type: "bench.noop"})

	conns := make(types.Connections, srcCount+1)
	for i := 0; i < srcCount; i++ {
		srcN := fmt.Sprintf("src%d", i)
		conns[srcN] = map[string]types.PortConnections{
			"main": {Targets: []types.Connection{{Node: "merge", Input: "main"}}},
		}
	}
	conns["merge"] = map[string]types.PortConnections{
		"main": {Targets: []types.Connection{{Node: "sink", Input: "main"}}},
	}
	return &types.WorkflowDef{Name: "faninout", Nodes: nodes, Connections: conns}
}

// ── topology builders: xflow SDK path (full roundtrip) ───────────────────────

// buildLinearChainWF builds a WorkflowBuilder for a linear chain: start → n0 → n1 → …
func buildLinearChainWF(n int) *xflow.WorkflowBuilder {
	wf := xflow.Workflow("linear")
	prev := wf.Node("start", node.Start())
	for i := 0; i < n; i++ {
		cur := wf.LocalNode(nodeName(i), &noopHandler{})
		wf.Connect(prev, cur)
		prev = cur
	}
	return wf
}

// buildFanOutWF builds a WorkflowBuilder: start → hub → leaf0, leaf1, …
func buildFanOutWF(n int) *xflow.WorkflowBuilder {
	wf := xflow.Workflow("fanout")
	start := wf.Node("start", node.Start())
	hub := wf.LocalNode("hub", &noopHandler{})
	wf.Connect(start, hub)
	for i := 0; i < n-1; i++ {
		leaf := wf.LocalNode(fmt.Sprintf("leaf%d", i), &noopHandler{})
		wf.Connect(hub, leaf)
	}
	return wf
}

// buildFanInOutWF builds a WorkflowBuilder: start → srcN → merge → sink
func buildFanInOutWF(n int) *xflow.WorkflowBuilder {
	if n < 3 {
		n = 3
	}
	srcCount := n - 2
	wf := xflow.Workflow("faninout")
	start := wf.Node("start", node.Start())
	mergeNode := wf.LocalNode("merge", &noopHandler{})
	for i := 0; i < srcCount; i++ {
		src := wf.LocalNode(fmt.Sprintf("src%d", i), &noopHandler{})
		wf.Connect(start, src)
		wf.Connect(src, mergeNode)
	}
	sink := wf.LocalNode("sink", &noopHandler{})
	wf.Connect(mergeNode, sink)
	return wf
}

// ── Compile benchmarks ────────────────────────────────────────────────────────
//
// These benchmarks measure only graph.Compile(def): the pure-algorithm pass
// that validates the WorkflowDef and builds the immutable Graph IR.
// No I/O, no goroutines — just the CPU hot path.

func BenchmarkSchedulerCompileLinearChain(b *testing.B) {
	def := linearChainDef(20)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := graph.Compile(def)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSchedulerCompileFanOut(b *testing.B) {
	def := fanOutDef(20)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := graph.Compile(def)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSchedulerCompileFanInOut(b *testing.B) {
	def := fanInOutDef(20)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := graph.Compile(def)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// ── Full roundtrip benchmarks ─────────────────────────────────────────────────
//
// These benchmarks measure the full path for a single workflow execution:
//   - engine setup (NewLocal, Bind goroutines)
//   - AddWorkflow (Compile + registry)
//   - Invoke (Submit + enqueue root tasks)
//   - handler execution (noop: immediate return)
//   - Wait (poll until execution reaches terminal state)
//
// Each iteration creates a fresh engine to avoid cross-iteration state leakage.
// b.ResetTimer() is called after the setup phase (workflow builder construction).

func BenchmarkSchedulerLinearChain(b *testing.B) {
	wf := buildLinearChainWF(20)
	benchFullRoundtrip(b, wf)
}

func BenchmarkSchedulerFanOut(b *testing.B) {
	wf := buildFanOutWF(20)
	benchFullRoundtrip(b, wf)
}

func BenchmarkSchedulerFanInOut(b *testing.B) {
	wf := buildFanInOutWF(20)
	benchFullRoundtrip(b, wf)
}

// benchFullRoundtrip runs the full Invoke + Wait cycle b.N times.
// Each iteration spins up a fresh in-memory engine and tears it down.
func benchFullRoundtrip(b *testing.B, wf *xflow.WorkflowBuilder) {
	b.Helper()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eng, err := xflow.NewLocal()
		if err != nil {
			b.Fatal(err)
		}
		wfID, err := eng.AddWorkflow(ctx, wf)
		if err != nil {
			eng.Stop()
			b.Fatal(err)
		}
		execID, err := eng.Invoke(ctx, wfID, xflow.Start(), nil)
		if err != nil {
			eng.Stop()
			b.Fatal(err)
		}
		result, err := eng.Wait(ctx, execID)
		if err != nil {
			eng.Stop()
			b.Fatal(err)
		}
		if result.Status != types.ExecutionStatusSuccess {
			eng.Stop()
			b.Fatalf("unexpected status: %s / %s", result.Status, result.Error)
		}
		eng.Stop()
	}
}

// BenchmarkSchedulerCompileTopologyMatrix records the pure compiler cost for
// the release-gate topology matrix. Each graph contains exactly the labelled
// number of workflow nodes.
func BenchmarkSchedulerCompileTopologyMatrix(b *testing.B) {
	topologies := []struct {
		name  string
		build func(int) *types.WorkflowDef
	}{
		{name: "linear", build: linearChainDef},
		{name: "fan-out", build: fanOutDef},
		{name: "fan-in", build: fanInOutDef},
	}

	for _, topology := range topologies {
		topology := topology
		for _, nodes := range []int{20, 100, 1000} {
			nodes := nodes
			b.Run(fmt.Sprintf("%s/nodes=%d", topology.name, nodes), func(b *testing.B) {
				def := topology.build(nodes)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := graph.Compile(def); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkSchedulerRoundTripTopologyMatrix records the in-memory scheduler
// path for the same release-gate matrix. The builders include one Start node,
// so passing nodes-1 preserves the labelled total workflow-node count.
func BenchmarkSchedulerRoundTripTopologyMatrix(b *testing.B) {
	topologies := []struct {
		name  string
		build func(int) *xflow.WorkflowBuilder
	}{
		{name: "linear", build: func(nodes int) *xflow.WorkflowBuilder { return buildLinearChainWF(nodes - 1) }},
		{name: "fan-out", build: func(nodes int) *xflow.WorkflowBuilder { return buildFanOutWF(nodes - 1) }},
		{name: "fan-in", build: func(nodes int) *xflow.WorkflowBuilder { return buildFanInOutWF(nodes - 1) }},
	}

	for _, topology := range topologies {
		topology := topology
		for _, nodes := range []int{20, 100, 1000} {
			nodes := nodes
			b.Run(fmt.Sprintf("%s/nodes=%d", topology.name, nodes), func(b *testing.B) {
				benchFullRoundtrip(b, topology.build(nodes))
			})
		}
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func nodeName(i int) string {
	return fmt.Sprintf("n%d", i)
}
