package xflow

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// TestWithTestRunAppliesTestOnlyPinData pins that WithTestRun is what turns
// pin_data on under the default pin_data_mode (test_only): with it the pinned
// node commits the mock as "pinned"; without it the same workflow runs the node
// for real. The builder has no pin_data API, so the record is seeded with a
// definition carrying pin_data the way the HTTP/DSL path would store it.
func TestWithTestRunAppliesTestOnlyPinData(t *testing.T) {
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	defer eng.Stop()

	wf := Workflow("test-run-pin")
	start := wf.Node("start", node.Start())
	calc := wf.Node("calc", node.Expr(`"real"`))
	wf.Connect(start, calc)
	def, err := wf.build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	def.PinData = map[string]any{"calc": map[string]any{"value": "mock"}}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	rec, err := eng.workflowRegistry.AddWorkflow(context.Background(), backend.WorkflowRecord{
		Key:        workflowKey(def),
		Namespace:  def.Namespace,
		Name:       def.Name,
		Version:    def.Version,
		Definition: def,
		Graph:      g,
	})
	if err != nil {
		t.Fatalf("seed workflow: %v", err)
	}

	run := func(opts ...InvokeOption) (types.NodeStatus, any) {
		t.Helper()
		ctx := context.Background()
		id, err := eng.Invoke(ctx, rec.ID, Start(), nil, opts...)
		if err != nil {
			t.Fatalf("Invoke: %v", err)
		}
		waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		res, err := eng.Wait(waitCtx, id)
		if err != nil {
			t.Fatalf("Wait: %v", err)
		}
		if res.Status != types.ExecutionStatusSuccess {
			t.Fatalf("status = %s (%s), want success", res.Status, res.Error)
		}
		d, err := eng.Inspect(ctx, id, "calc")
		if err != nil || len(d.Nodes) != 1 {
			t.Fatalf("Inspect(calc): %v", err)
		}
		return d.Nodes[0].Status, res.Output["calc"]
	}

	status, out := run(WithTestRun())
	if status != types.NodeStatusPinned {
		t.Fatalf("test run: calc = %s, want pinned", status)
	}
	if m, _ := out.(map[string]any); m["value"] != "mock" {
		t.Fatalf("test run: calc output = %#v, want the pin_data mock", out)
	}

	status, out = run()
	if status != types.NodeStatusSuccess {
		t.Fatalf("normal run: calc = %s, want success (test_only must not pin)", status)
	}
	if m, _ := out.(map[string]any); m["value"] == "mock" {
		t.Fatalf("normal run: calc output = %#v, served the mock outside a test run", out)
	}
}
