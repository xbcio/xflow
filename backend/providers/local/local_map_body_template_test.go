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

// bodyTemplateHandler records the value of its "greeting" parameter on every
// invocation. That parameter carries a ${{ $item.id }} template, so the recorded
// value is the evidence for who evaluated it and against which environment.
type bodyTemplateHandler struct {
	mu   sync.Mutex
	seen []any
}

func (h *bodyTemplateHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.body_template"}
}

func (h *bodyTemplateHandler) Execute(_ context.Context, in *types.Input) (*types.Output, error) {
	h.mu.Lock()
	h.seen = append(h.seen, in.Params["greeting"])
	h.mu.Unlock()
	return &types.Output{Data: map[string]any{"ok": true}}, nil
}

func (h *bodyTemplateHandler) greetings() []any {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]any(nil), h.seen...)
}

// TestMapBodyMemberTemplateEvaluatedByInnerExecution is the end-to-end form of
// the boundary-must-not-walk-into-the-body regression (execution/params.go's
// skipSubgraphBody, unit-tested in execution/params_body_test.go).
//
// A body member's parameter carries ${{ $item.id }} -- one of the three roots
// the DSL promises a body and which execution/subgraph/map_body.go's
// bodyItemInput injects for the INNER execution. Those roots do not exist in
// the OUTER map node's environment.
//
// Before the fix, this workflow HUNG rather than failed. The outer map node's
// boundary evaluation hit "unknown name $item" and returned a system error,
// which the engine classifies as a retriable transient failure, so the outer
// task retried forever: WaitDone timed out with status still "running" and the
// xflow.map handler was never called once. That is why this test asserts on a
// successful terminal status and on the body handler's observed values -- an
// assertion on the error string alone would have passed both before and after.
func TestMapBodyMemberTemplateEvaluatedByInnerExecution(t *testing.T) {
	reg := execution.NewRegistry()
	reg.RegisterGlobal("xflow.map", &mapFanoutHandler{})
	reg.RegisterGlobal("test.echo", &batchEchoHandler{})
	body := &bodyTemplateHandler{}
	reg.RegisterGlobal("test.body_template", body)

	b := New(WithConcurrency(2), WithRegistry(reg))
	bodies := subgraph.NewMapBodyExecutor(
		subgraph.NewExecutor(reg, subgraph.NewPackageCache(subgraph.PackageCacheConfig{}),
			func() subgraph.Backend { return New(WithRegistry(reg), WithConcurrency(1)) }),
		true, time.Time{})
	eng := engine.New(b.State(), b.Queue(), engine.WithBatchBodyExecutor(bodies))
	stop := b.Bind(eng)
	defer stop()

	def := &types.WorkflowDef{
		Name: "map-body-template",
		Nodes: []types.NodeDef{
			{Name: "m", Type: "xflow.map", Parameters: map[string]any{
				"items": "$input.items",
				"body": map[string]any{
					"type": "xflow.subgraph",
					"parameters": map[string]any{
						"nodes": []any{
							map[string]any{
								"name": "member", "type": "test.body_template",
								"parameters": map[string]any{
									"greeting": "${{ $item.id }}",
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	res, err := b.WaitDone(ctx, id)
	if err != nil {
		t.Fatalf("WaitDone: %v (before the skipSubgraphBody fix this timed out -- the "+
			"outer map node failed at its parameter boundary on the body member's "+
			"$item template and retried forever)", err)
	}
	if res.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution status = %v, want success", res.Status)
	}

	// mapFanoutHandler emits two items, {"id":1} and {"id":2}, one per batch.
	// The inner execution evaluates the body member's template against its own
	// per-item env, so each invocation must see its own item's id -- not the
	// template text, and not a value from the outer scope.
	//
	// The expected values are ints, not strings: ${{ }} wrapping the WHOLE value
	// is RenderTemplate's rule 1, which returns the expression's native result
	// rather than stringifying it (that is what {{ }} interpolation is for).
	got := body.greetings()
	if len(got) != 2 {
		t.Fatalf("body member ran %d times with greetings %#v, want 2", len(got), got)
	}
	seen := map[any]bool{}
	for _, g := range got {
		if s, ok := g.(string); ok && s == "${{ $item.id }}" {
			t.Fatal("greeting is still the template text -- the INNER execution's " +
				"boundary must evaluate a body member's parameters")
		}
		seen[g] = true
	}
	if !seen[1] || !seen[2] {
		t.Fatalf("greetings = %#v, want one 1 and one 2 (each body invocation "+
			"renders against its OWN item)", got)
	}
}

// TestMapBodyMemberSeesWorkflowVarsAndConfig guards the two independent ways a
// body member used to lose $vars and $config.
//
// The DSL promises both roots to a body -- DSL-SPECIFICATION.md's own map-body
// example writes `url: "{{ $vars.api_base_url }}/process"`, which under the
// pre-fix code rendered to "/process": a URL with its host silently removed.
//
// Measured before the fix, both roots were empty in a body member:
//
//	Context{Vars,Config}      -> "NOVAR/NOCFG"
//	Submit(..., &Runtime{Vars}) -> "MISSING"
//
// The two halves fail for different reasons and must both be asserted here:
//
//   - STATIC (Context.Vars / Context.Config): ProjectNodeBodyPackage built the
//     body package's Def with only Nodes and Connections. ProjectGroupPackage
//     has always carried Context, so the two projection paths disagreed.
//   - RUNTIME (Runtime.Vars, e.g. per-submission tenant): the inner
//     Engine.Submit call passed no runtime variadic, so the sub-execution's
//     snapshot had a nil Runtime and buildInput merged nothing into Vars.
//
// A test asserting only one of them would stay green with the other half
// broken -- they share no code.
func TestMapBodyMemberSeesWorkflowVarsAndConfig(t *testing.T) {
	reg := execution.NewRegistry()
	reg.RegisterGlobal("xflow.map", &mapFanoutHandler{})
	reg.RegisterGlobal("test.echo", &batchEchoHandler{})
	body := &bodyTemplateHandler{}
	reg.RegisterGlobal("test.body_template", body)

	b := New(WithConcurrency(2), WithRegistry(reg))
	bodies := subgraph.NewMapBodyExecutor(
		subgraph.NewExecutor(reg, subgraph.NewPackageCache(subgraph.PackageCacheConfig{}),
			func() subgraph.Backend { return New(WithRegistry(reg), WithConcurrency(1)) }),
		true, time.Time{})
	eng := engine.New(b.State(), b.Queue(), engine.WithBatchBodyExecutor(bodies))
	stop := b.Bind(eng)
	defer stop()

	def := &types.WorkflowDef{
		Name: "map-body-context",
		Context: &types.WorkflowContext{
			Vars:   map[string]any{"greeting": "hello"},
			Config: map[string]any{"region": "cn-north"},
		},
		Nodes: []types.NodeDef{
			{Name: "m", Type: "xflow.map", Parameters: map[string]any{
				"items": "$input.items",
				"body": map[string]any{
					"type": "xflow.subgraph",
					"parameters": map[string]any{
						"nodes": []any{
							map[string]any{
								"name": "member", "type": "test.body_template",
								"parameters": map[string]any{
									// The ?? defaults turn an absent root into a
									// self-describing value rather than an
									// evaluation error, so a failure reports WHICH
									// root went missing.
									"greeting": "${{ ($vars.greeting ?? 'NOVAR') + '/' + " +
										"($config.region ?? 'NOCFG') + '/' + ($vars.tenant ?? 'NORUNTIME') }}",
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Runtime vars are per-submission, unlike Context.Vars which is per
	// definition. A tenant id is the canonical example and is the half the
	// seeded-input path was silently dropping.
	id, err := eng.Submit(ctx, g, nil, &types.Runtime{Vars: map[string]any{"tenant": "acme"}})
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

	const want = "hello/cn-north/acme"
	got := body.greetings()
	if len(got) != 2 {
		t.Fatalf("body member ran %d times with %#v, want 2", len(got), got)
	}
	for _, g := range got {
		if g != want {
			t.Errorf("body member saw %#v, want %q -- NOVAR means Context.Vars did not "+
				"reach the projected body package, NOCFG the same for Context.Config, "+
				"NORUNTIME means the inner Submit dropped the outer Runtime", g, want)
		}
	}
}
