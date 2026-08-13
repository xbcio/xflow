package local

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/execution/subgraph"
	"github.com/xbcio/xflow/types"
)

// traceProbeHandler records the trace identity each invocation was handed.
// Recording per node name matters because the point of the assertion is that
// the INNER member carries the same identity as the OUTER node, so both halves
// have to be observed through the same handler.
type traceProbeHandler struct {
	mu   sync.Mutex
	seen map[string][]traceIdentity
}

type traceIdentity struct {
	trace string
	span  string
}

func newTraceProbeHandler() *traceProbeHandler {
	return &traceProbeHandler{seen: map[string][]traceIdentity{}}
}

func (h *traceProbeHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.trace_probe"}
}

func (h *traceProbeHandler) Execute(_ context.Context, in *types.Input) (*types.Output, error) {
	h.mu.Lock()
	h.seen[in.NodeName] = append(h.seen[in.NodeName], traceIdentity{trace: in.TraceID, span: in.SpanID})
	h.mu.Unlock()
	return &types.Output{Data: map[string]any{"items": []any{map[string]any{"id": 1}}}}, nil
}

func (h *traceProbeHandler) rows(node string) []traceIdentity {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]traceIdentity(nil), h.seen[node]...)
}

// TestMapBodyMemberInheritsTheOuterTraceIdentity asserts that a body member runs
// under the SAME trace as the map node that expanded it.
//
// A body runs on its own inner engine against its own execution ID, and that
// inner execution is submitted fresh: without an explicit hand-off, its snapshot
// has no TraceID/SpanID at all, so buildInput hands the member an empty pair.
// The span the member emits then has no parent, and the trace ends at the map
// node -- exactly where a map workflow most needs it to continue, since the body
// is where the per-item work happens.
//
// Asserted on the member's Input.TraceID rather than on collected spans because
// this is the value every tracing integration reads FROM; a span-graph assertion
// would test the exporter as much as the propagation.
func TestMapBodyMemberInheritsTheOuterTraceIdentity(t *testing.T) {
	reg := execution.NewRegistry()
	reg.RegisterGlobal("xflow.map", &mapFanoutHandler{})
	probe := newTraceProbeHandler()
	reg.RegisterGlobal("test.trace_probe", probe)

	b := New(WithConcurrency(2), WithRegistry(reg))
	bodies := subgraph.NewMapBodyExecutor(
		subgraph.NewExecutor(reg, subgraph.NewPackageCache(subgraph.PackageCacheConfig{}),
			func() subgraph.Backend { return New(WithRegistry(reg), WithConcurrency(1)) }),
		true, time.Time{})
	eng := engine.New(b.State(), b.Queue(), engine.WithBatchBodyExecutor(bodies))
	stop := b.Bind(eng)
	defer stop()

	def := &types.WorkflowDef{
		Name: "map-body-trace",
		Nodes: []types.NodeDef{
			{Name: "src", Type: "test.trace_probe"},
			{Name: "m", Type: "xflow.map", Parameters: map[string]any{
				"items": "$input.items",
				"body": map[string]any{
					"type": "xflow.subgraph",
					"parameters": map[string]any{
						"nodes": []any{
							map[string]any{"name": "inner", "type": "test.trace_probe"},
						},
					},
				},
			}},
		},
		Connections: types.Connections{
			"src": {"main": {Targets: []types.Connection{{Node: "m", Input: "main"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	submitCtx := engine.WithSpanID(engine.WithTraceID(ctx, "trace-outer"), "span-outer")
	id, err := eng.Submit(submitCtx, g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	res, err := b.WaitDone(ctx, id)
	if err != nil {
		t.Fatalf("WaitDone: %v", err)
	}
	if res.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution status = %v, want success", res.Status)
	}

	// The outer node establishes what the inner one has to match, read from the
	// same field rather than from the submitted constant -- if the outer half
	// ever stopped carrying the identity, this test should fail as a broken
	// premise instead of silently comparing two empty strings.
	outer := probe.rows("src")
	if len(outer) != 1 {
		t.Fatalf("outer node ran %d times, want 1", len(outer))
	}
	if outer[0].trace != "trace-outer" {
		t.Fatalf("outer node saw TraceID %q, want %q", outer[0].trace, "trace-outer")
	}

	// mapFanoutHandler emits two items in two batches of one, so the single body
	// member runs once per item and every run has to carry the identity.
	inner := probe.rows("inner")
	if len(inner) != 2 {
		t.Fatalf("body member ran %d times, want 2", len(inner))
	}
	for i, got := range inner {
		if got.trace != outer[0].trace {
			t.Errorf("body member run %d saw TraceID %q, want the outer execution's %q -- "+
				"the inner sub-execution was submitted without the trace identity, so "+
				"the trace breaks at the map node", i, got.trace, outer[0].trace)
		}
		// The SPAN is deliberately compared to the outer node's span too: the
		// sub-execution's parent IS the map node's dispatch, so a body member's
		// span must descend from it rather than from the submission's root.
		if got.span != outer[0].span {
			t.Errorf("body member run %d saw SpanID %q, want the outer execution's %q",
				i, got.span, outer[0].span)
		}
	}
}

// TestGroupMemberInheritsTheOuterTraceIdentity is the group half of the same
// break. A group's lease carries a whole *types.Input, which already has the
// outer TraceID/SpanID on it, so nothing new has to travel -- but the executor
// still has to put them on the inner submission context, or the inner snapshot
// is written without them just the same.
func TestGroupMemberInheritsTheOuterTraceIdentity(t *testing.T) {
	reg := execution.NewRegistry()
	probe := newTraceProbeHandler()
	reg.RegisterGlobal("test.trace_probe", probe)

	executor := subgraph.NewExecutor(reg, subgraph.NewPackageCache(subgraph.PackageCacheConfig{}),
		func() subgraph.Backend { return New(WithRegistry(reg), WithConcurrency(1)) })

	// A projected group package is a self-contained sub-graph: one member plus
	// the collector node that carries its output out across the boundary. Built
	// by hand rather than projected, so the test states the shape it depends on.
	pkg := &graph.SubgraphPackage{
		Version:   1,
		GroupName: "group-trace",
		EntryNode: "member",
		Def: &types.WorkflowDef{
			Name: "group-trace",
			Nodes: []types.NodeDef{
				{Name: "member", Type: "test.trace_probe"},
				{Name: "__collector_member_main", Type: graph.NodeTypeGroupExit},
			},
			Connections: types.Connections{
				"member": {"main": types.PortConnections{Targets: []types.Connection{{Node: "__collector_member_main"}}}},
			},
		},
		Exits: []graph.SubgraphPackageExit{
			{CollectorNode: "__collector_member_main", SrcNode: "member", Port: "main"},
		},
		Requirements: []graph.Requirement{{NodeType: "test.trace_probe"}},
	}
	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		t.Fatalf("compute package hash: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := executor.Execute(ctx, subgraph.Request{
		Package:     pkg,
		PackageHash: hash,
		Input:       &types.Input{TraceID: "trace-outer", SpanID: "span-outer"},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.Outcome != subgraph.OutcomeSuccess {
		t.Fatalf("outcome = %v (%s), want success", res.Outcome, res.Error)
	}

	rows := probe.rows("member")
	if len(rows) != 1 {
		t.Fatalf("group member ran %d times, want 1", len(rows))
	}
	if rows[0].trace != "trace-outer" {
		t.Errorf("group member saw TraceID %q, want %q -- the lease's Input already "+
			"carries it, but the inner submission dropped it", rows[0].trace, "trace-outer")
	}
	if rows[0].span != "span-outer" {
		t.Errorf("group member saw SpanID %q, want %q", rows[0].span, "span-outer")
	}
}
