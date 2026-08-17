package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// mapNodeWithConcurrency builds a map node whose body_concurrency parameter is
// whatever the caller passes -- including the shapes that must NOT enable it. It
// keeps batch_size at 10 so all 10 items land in a single batch: this test is
// about the parameter reaching the executor, and several batches would only
// multiply the assertions.
func mapNodeWithConcurrency(concurrency any) types.NodeDef {
	params := map[string]any{
		"items":      "$input.items",
		"batch_size": 10,
		"body": map[string]any{
			"type": "xflow.subgraph",
			"parameters": map[string]any{
				"nodes": []any{
					map[string]any{"name": "echo", "type": "test.echo"},
				},
			},
		},
	}
	if concurrency != nil {
		params["body_concurrency"] = concurrency
	}
	return types.NodeDef{Name: "m", Type: "xflow.map", Parameters: params}
}

// runMapWithConcurrencyParam drives a single-batch map node and returns the
// BodyConcurrency the engine actually handed the body executor. It goes through
// the real Submit → expand → ExecuteBatch path rather than calling
// mapBodyConcurrency directly: the reader existing is not the same as the
// engine calling it, and a value that stops at the compiler is exactly the
// silent-drop failure this whole wiring is exposed to.
func runMapWithConcurrencyParam(t *testing.T, concurrency any) int {
	t.Helper()
	def := &types.WorkflowDef{
		Name:  "map-body-concurrency",
		Nodes: []types.NodeDef{mapNodeWithConcurrency(concurrency)},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"xflow.map": &sizedMapHandler{batchSize: 10},
		"test.echo": &echoHandler{},
	}}
	exec := newEchoBodyExecutor()
	eng := New(state, queue, WithBatchBodyExecutor(exec))
	testRegistries.Store(eng, reg)
	t.Cleanup(func() { testRegistries.Delete(eng) })
	ctx := context.Background()

	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatalf("submit: %v", err)
	}
	roots := queue.Drain()
	if len(roots) != 1 {
		t.Fatalf("root tasks = %v, want one", taskNames(roots))
	}
	executeTask(t, eng, roots[0])

	batchTasks := queue.Drain()
	if len(batchTasks) != 1 {
		t.Fatalf("batch tasks = %d, want 1 (10 items at batch_size=10)", len(batchTasks))
	}
	if err := eng.ExecuteBatch(ctx, batchTasks[0]); err != nil {
		t.Fatalf("ExecuteBatch: %v", err)
	}

	reqs := exec.requests()
	if len(reqs) != 1 {
		t.Fatalf("body executor saw %d batches, want 1", len(reqs))
	}
	return reqs[0].BodyConcurrency
}

// TestExecuteBatchCarriesBodyConcurrencyToTheBody pins the in-process route:
// engine/expand.go must read the parameter off the compiled node and put it on
// the request. Without this the executor's parallel path is unreachable from a
// workflow definition -- the feature would exist only for a Go caller
// constructing a BatchBodyRequest by hand.
func TestExecuteBatchCarriesBodyConcurrencyToTheBody(t *testing.T) {
	if got := runMapWithConcurrencyParam(t, 8); got != 8 {
		t.Fatalf("BodyConcurrency = %d, want 8: the map node's body_concurrency parameter "+
			"did not reach the body executor", got)
	}
}

// A definition that round-tripped through JSON carries float64 where a Go
// caller wrote int. Rejecting that shape would make the parameter work from the
// SDK and silently do nothing from a YAML or API-registered workflow -- which is
// how every workflow in production arrives.
func TestExecuteBatchAcceptsBodyConcurrencyFromJSON(t *testing.T) {
	if got := runMapWithConcurrencyParam(t, float64(4)); got != 4 {
		t.Fatalf("BodyConcurrency = %d, want 4: a JSON-decoded parameter (float64) must "+
			"enable concurrency the same as an int", got)
	}
}

// The default and the shapes that must not enable it. An absent parameter is
// every map node written before this feature, and both of the others are values
// that cannot be honoured as a promise:
//
//   - a string is an unevaluated expression; the engine reads parameters at
//     scheduling time, before any per-item environment exists, so there is
//     nothing to evaluate it against. Same discipline as mapContinueOnError.
//   - a negative number is not a cap. Silently treating it as unbounded is the
//     shape of failure that turns a typo into 10000 concurrent guest instances.
func TestExecuteBatchLeavesBodyConcurrencySerialByDefault(t *testing.T) {
	for name, param := range map[string]any{
		"absent":     nil,
		"expression": "$vars.concurrency",
		"negative":   -1,
		"zero":       0,
	} {
		t.Run(name, func(t *testing.T) {
			if got := runMapWithConcurrencyParam(t, param); got > 1 {
				t.Fatalf("BodyConcurrency = %d, want <= 1: %s must leave the body serial", got, name)
			}
		})
	}
}

// TestBuildSubgraphLeaseCarriesBodyConcurrency pins the WIRE route, which is
// independent of the in-process one: a remote runner never saw the workflow
// definition, so it cannot read the map node's parameters for itself. If the
// lease drops the field, body_concurrency works in-process and silently does
// nothing on every distributed deployment -- the field-by-field copy failure
// this codebase has already been bitten by twice.
func TestBuildSubgraphLeaseCarriesBodyConcurrency(t *testing.T) {
	def := &types.WorkflowDef{
		Name:  "batch-lease-concurrency",
		Nodes: []types.NodeDef{{Name: "loop", Type: "xflow.map", Parameters: mapBodyConcurrencyParamsForTest(6)}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{"xflow.map": &loopHandler{}}}
	eng := New(state, queue, WithBatchBodyExecutor(newEchoBodyExecutor()))
	testRegistries.Store(eng, reg)
	t.Cleanup(func() { testRegistries.Delete(eng) })
	ctx := context.Background()
	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatalf("submit: %v", err)
	}
	batches := drainBatchTasks(t, eng, queue)

	_, payload, err := eng.BuildSubgraphLease(ctx, batches[0])
	if err != nil {
		t.Fatalf("BuildSubgraphLease() error = %v", err)
	}
	if payload.BodyConcurrency != 6 {
		t.Fatalf("payload BodyConcurrency = %d, want 6: a runner cannot read the map node's "+
			"parameters, so a dropped field makes body_concurrency a no-op on every "+
			"distributed deployment", payload.BodyConcurrency)
	}
}

// mapBodyConcurrencyParamsForTest is mapBodyParamsForTest plus the concurrency
// parameter.
func mapBodyConcurrencyParamsForTest(concurrency int) map[string]any {
	params := mapBodyParamsForTest()
	params["body_concurrency"] = concurrency
	return params
}
