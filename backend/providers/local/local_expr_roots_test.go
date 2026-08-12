package local

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/execution/subgraph"
	"github.com/xbcio/xflow/types"
)

// rootsRecorder records every parameter map its handler is invoked with, so a
// test can assert on what the parameter boundary actually produced.
type rootsRecorder struct {
	mu   sync.Mutex
	seen []map[string]any
}

func (h *rootsRecorder) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.roots_recorder"}
}

func (h *rootsRecorder) Execute(_ context.Context, in *types.Input) (*types.Output, error) {
	h.mu.Lock()
	cp := make(map[string]any, len(in.Params))
	for k, v := range in.Params {
		cp[k] = v
	}
	h.seen = append(h.seen, cp)
	h.mu.Unlock()
	return &types.Output{Data: map[string]any{"ok": true}}, nil
}

func (h *rootsRecorder) records() []map[string]any {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]map[string]any(nil), h.seen...)
}

// TestExecutionAndWorkflowRootsReachAHandler is the wiring test for the
// $execution and $workflow environment roots.
//
// The exprx unit tests prove BuildExprEnv READS types.Input's fields. They
// cannot prove buildInput WRITES them -- a test that constructs its own
// types.Input bypasses the assembly path entirely and would stay green with
// engine/input.go left untouched. This test submits a real workflow, so the
// values it observes came through buildInput or they did not appear at all.
func TestExecutionAndWorkflowRootsReachAHandler(t *testing.T) {
	reg := execution.NewRegistry()
	rec := &rootsRecorder{}
	reg.RegisterGlobal("test.roots_recorder", rec)

	b := New(WithConcurrency(1), WithRegistry(reg))
	eng := engine.New(b.State(), b.Queue())
	stop := b.Bind(eng)
	defer stop()

	def := &types.WorkflowDef{
		Name:    "roots-flow",
		Version: "v3",
		Nodes: []types.NodeDef{
			{Name: "n", Type: "test.roots_recorder", Parameters: map[string]any{
				"exec_id":  "${{ $execution.id }}",
				"wf_name":  "${{ $workflow.name }}",
				"wf_ver":   "${{ $workflow.version }}",
				"combined": "{{ $workflow.name }}@{{ $workflow.version }}",
			}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, err := eng.Submit(ctx, g, nil)
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

	got := rec.records()
	if len(got) != 1 {
		t.Fatalf("handler ran %d times, want 1", len(got))
	}
	p := got[0]

	// $execution.id must be the submitted execution's own ID, not a literal
	// and not the template text.
	if p["exec_id"] != string(id) {
		t.Fatalf("$execution.id rendered to %#v, want the execution ID %q -- "+
			"buildInput must populate Input.ExecutionID's env root", p["exec_id"], string(id))
	}
	if p["wf_name"] != "roots-flow" {
		t.Fatalf("$workflow.name rendered to %#v, want %q -- buildInput must "+
			"populate Input.WorkflowName from Graph.Name()", p["wf_name"], "roots-flow")
	}
	if p["wf_ver"] != "v3" {
		t.Fatalf("$workflow.version rendered to %#v, want %q -- buildInput must "+
			"populate Input.WorkflowVersion from Graph.WorkflowVersion()", p["wf_ver"], "v3")
	}
	if p["combined"] != "roots-flow@v3" {
		t.Fatalf("interpolated form rendered to %#v, want %q", p["combined"], "roots-flow@v3")
	}
}

// TestBodyMemberSeesInnerExecutionRoots pins the sub-graph semantics of both
// roots, which are a real decision rather than a fallout.
//
// A body runs as its OWN execution: execution/subgraph mints an inner
// execution ID, and ProjectNodeBodyPackage builds the inner definition with
// Name set to the map node's name. So a body member reading $execution.id sees
// the inner ID (not the outer one) and reading $workflow.name sees the map
// node's name (not the outer workflow's).
//
// This is the consistent reading -- both roots describe the graph the node is
// actually running in -- but it is surprising enough that it must be pinned:
// a future change that propagated the outer identity into projected packages
// would silently redefine what a body member observes.
func TestBodyMemberSeesInnerExecutionRoots(t *testing.T) {
	reg := execution.NewRegistry()
	reg.RegisterGlobal("xflow.map", &mapFanoutHandler{})
	reg.RegisterGlobal("test.echo", &batchEchoHandler{})
	rec := &rootsRecorder{}
	reg.RegisterGlobal("test.roots_recorder", rec)

	b := New(WithConcurrency(2), WithRegistry(reg))
	bodies := subgraph.NewMapBodyExecutor(
		subgraph.NewExecutor(reg, subgraph.NewPackageCache(subgraph.PackageCacheConfig{}),
			func() subgraph.Backend { return New(WithRegistry(reg), WithConcurrency(1)) }),
		true, time.Time{})
	eng := engine.New(b.State(), b.Queue(), engine.WithBatchBodyExecutor(bodies))
	stop := b.Bind(eng)
	defer stop()

	def := &types.WorkflowDef{
		Name:    "outer-flow",
		Version: "v9",
		Nodes: []types.NodeDef{
			{Name: "m", Type: "xflow.map", Parameters: map[string]any{
				"items": "$input.items",
				"body": map[string]any{
					"type": "xflow.subgraph",
					"parameters": map[string]any{
						"nodes": []any{
							map[string]any{
								"name": "member", "type": "test.roots_recorder",
								"parameters": map[string]any{
									"exec_id": "${{ $execution.id }}",
									"wf_name": "${{ $workflow.name }}",
								},
							},
						},
					},
				},
			}},
			{Name: "done", Type: "test.echo"},
		},
		Connections: types.Connections{
			"m": {"main": {Targets: []types.Connection{{Node: "done", Input: "main"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	outerID, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	res, err := b.WaitDone(ctx, outerID)
	if err != nil {
		t.Fatalf("WaitDone: %v", err)
	}
	if res.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution status = %v, want success", res.Status)
	}

	got := rec.records()
	if len(got) == 0 {
		t.Fatal("the body member never ran")
	}
	for _, p := range got {
		execID, _ := p["exec_id"].(string)
		if execID == "" {
			t.Fatalf("$execution.id rendered to %#v inside a body; the inner "+
				"execution must still populate the root", p["exec_id"])
		}
		if execID == string(outerID) {
			t.Fatalf("$execution.id inside a body rendered to the OUTER execution ID "+
				"%q; a body runs as its own execution and must report its own ID", execID)
		}
		if strings.Contains(execID, "$execution") {
			t.Fatalf("$execution.id was left as template text inside a body: %q", execID)
		}
		// ProjectNodeBodyPackage names the inner definition after the map node.
		if p["wf_name"] != "m" {
			t.Fatalf("$workflow.name inside a body rendered to %#v, want %q (the map "+
				"node's name -- ProjectNodeBodyPackage builds the inner def with "+
				"Name = the map node's name). If this changed deliberately, the DSL "+
				"spec's $workflow entry must say so", p["wf_name"], "m")
		}
	}
}
