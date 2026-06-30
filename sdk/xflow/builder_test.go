package xflow

import (
	"context"
	"strings"
	"testing"

	"github.com/xbcio/xflow/backend/memory"
	"github.com/xbcio/xflow/nodes/node"
	"github.com/xbcio/xflow/types"
)

func TestWorkflowBuilderNodeConnectInputOutput(t *testing.T) {
	wf := Workflow("builder-api")

	start := wf.Node("start", node.Function("return input"))
	fetchUser := wf.Node("fetch-user", node.Function("return input"))
	fetchOrder := wf.Node("fetch-order", node.Function("return input"))
	merge := wf.Node("merge", node.Merge(node.MergeWaitAll))

	wf.Connect(start, fetchUser)
	wf.Connect(fetchUser.Output("main"), merge.Input("user"))
	wf.Connect(fetchOrder.Output("main"), merge.Input("order"))

	def, err := wf.build()
	if err != nil {
		t.Fatal(err)
	}

	if len(def.Nodes) != 4 {
		t.Fatalf("node count = %d, want 4", len(def.Nodes))
	}
	if def.Nodes[0].Kind != "action" {
		t.Fatalf("first node kind = %q, want action", def.Nodes[0].Kind)
	}
	if got := def.Connections["start"]["main"][0].Node; got != "fetch-user" {
		t.Fatalf("start main target = %q, want fetch-user", got)
	}
	if got := def.Connections["fetch-user"]["main"][0].Input; got != "user" {
		t.Fatalf("fetch-user input = %q, want user", got)
	}
	if got := def.Connections["fetch-order"]["main"][0].Input; got != "order" {
		t.Fatalf("fetch-order input = %q, want order", got)
	}
}

func TestWorkflowBuilderDefaultsNamespaceAndVersion(t *testing.T) {
	wf := Workflow("security_approval")
	def, err := wf.build()
	if err != nil {
		t.Fatal(err)
	}
	if def.Namespace != "default" {
		t.Fatalf("Namespace = %q, want default", def.Namespace)
	}
	if def.Version != "v1" {
		t.Fatalf("Version = %q, want v1", def.Version)
	}
}

func TestWorkflowBuilderSetsNamespaceAndVersion(t *testing.T) {
	wf := Workflow("security_approval").Namespace("risk").Version("v3")
	def, err := wf.build()
	if err != nil {
		t.Fatal(err)
	}
	if def.Namespace != "risk" {
		t.Fatalf("Namespace = %q, want risk", def.Namespace)
	}
	if def.Version != "v3" {
		t.Fatalf("Version = %q, want v3", def.Version)
	}
}

func TestWorkflowBuilderRejectsCycleByDefault(t *testing.T) {
	wf := Workflow("cycle-default")
	start := wf.Node("start", node.Start())
	review := wf.Node("review", node.Function("input"))
	wf.Connect(start, review)
	wf.Connect(review.Output("reject"), start)

	_, err := wf.build()
	if err == nil {
		t.Fatal("expected cycle error")
	}
}

func TestWorkflowBuilderAllowCyclesEmitsOptionsAndSkipsBuilderCycleDetection(t *testing.T) {
	wf := Workflow("cycle").AllowCycles(9)
	start := wf.Node("start", node.Start())
	review := wf.Node("review", node.Function("input"))
	wf.Connect(start, review)
	wf.Connect(review.Output("reject"), start)

	def, err := wf.build()
	if err != nil {
		t.Fatal(err)
	}
	if def.Options == nil || !def.Options.AllowCycles {
		t.Fatalf("Options = %+v, want allow_cycles", def.Options)
	}
	if def.Options.MaxAutoDepth != 9 {
		t.Fatalf("MaxAutoDepth = %d, want 9", def.Options.MaxAutoDepth)
	}
}

func TestEngineAddWorkflowRejectsDirectHandlersWhenBackendDoesNotAllowThem(t *testing.T) {
	provider := memory.New()
	eng, err := newFromConfig(&engineConfig{allowDirectHandlers: false}, provider)
	if err != nil {
		t.Fatalf("newFromConfig() error = %v", err)
	}
	defer eng.Stop()

	wf := Workflow("cluster-direct-handler")
	wf.LocalNode("local-only", &testBuilderEchoHandler{})

	_, err = eng.AddWorkflow(context.Background(), wf)
	if err == nil {
		t.Fatal("AddWorkflow() error = nil, want direct-handler rejection")
	}
	if !strings.Contains(err.Error(), "node.Define") {
		t.Fatalf("AddWorkflow() error = %v, want node.Define guidance", err)
	}
}

func TestEngineInvokeRegistersWorkflowDeclaredHandlers(t *testing.T) {
	provider := memory.New()
	eng, err := newFromConfig(&engineConfig{allowDirectHandlers: false}, provider)
	if err != nil {
		t.Fatalf("newFromConfig() error = %v", err)
	}
	defer eng.Stop()

	wf := Workflow("portable-handler")
	start := wf.Node("start", node.Start())
	portable := wf.Node("portable", testWorkflowDeclaredNode.New(map[string]any{"source": "workflow"}))
	wf.Connect(start, portable)

	workflowID, err := eng.AddWorkflow(context.Background(), wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}
	id, err := eng.Invoke(context.Background(), workflowID, Start(), map[string]any{"ticket": "VULN-1"})
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}

	result, err := eng.Wait(context.Background(), id)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if got := result.Output["portable"].(map[string]any)["ticket"]; got != "VULN-1" {
		t.Fatalf("portable.ticket = %v, want VULN-1", got)
	}
}

func TestEngineInvokePassesRuntimeVarsToEveryExecution(t *testing.T) {
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	wf := Workflow("runtime-vars")
	start := wf.Node("start", node.Start())
	capture := wf.Node("capture", testRuntimeCaptureNode.New(nil))
	wf.Connect(start, capture)

	workflowID, err := eng.AddWorkflow(context.Background(), wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}
	firstID, err := eng.Invoke(context.Background(), workflowID, Start(),
		map[string]any{"ticket": "VULN-1"},
		WithRuntimeVars(map[string]any{"tenant_id": "tenant-a"}),
	)
	if err != nil {
		t.Fatalf("first Invoke() error = %v", err)
	}
	secondID, err := eng.Invoke(context.Background(), workflowID, Start(),
		map[string]any{"ticket": "VULN-2"},
		WithRuntimeVars(map[string]any{"tenant_id": "tenant-b"}),
	)
	if err != nil {
		t.Fatalf("second Invoke() error = %v", err)
	}

	first, err := eng.Wait(context.Background(), firstID)
	if err != nil {
		t.Fatalf("Wait(first) error = %v", err)
	}
	second, err := eng.Wait(context.Background(), secondID)
	if err != nil {
		t.Fatalf("Wait(second) error = %v", err)
	}

	firstOut := first.Output["capture"].(map[string]any)
	secondOut := second.Output["capture"].(map[string]any)
	if got := firstOut["runtime_tenant_id"]; got != "tenant-a" {
		t.Fatalf("first runtime_tenant_id = %v, want tenant-a", got)
	}
	if got := secondOut["runtime_tenant_id"]; got != "tenant-b" {
		t.Fatalf("second runtime_tenant_id = %v, want tenant-b", got)
	}
	if got := firstOut["vars_tenant_id"]; got != "tenant-a" {
		t.Fatalf("first vars_tenant_id = %v, want tenant-a", got)
	}
	if got := secondOut["vars_tenant_id"]; got != "tenant-b" {
		t.Fatalf("second vars_tenant_id = %v, want tenant-b", got)
	}
}

func TestWithNodesRegistersConsumerCapabilities(t *testing.T) {
	provider := memory.New()
	eng, err := newFromConfig(&engineConfig{
		allowDirectHandlers: false,
		nodes:               []node.Handler{testWorkflowDeclaredNode},
	}, provider)
	if err != nil {
		t.Fatalf("newFromConfig() error = %v", err)
	}
	defer eng.Stop()

	h, err := eng.registry.Get(types.ExecutionID("worker-only"), "portable", "test.workflow_declared", 0)
	if err != nil {
		t.Fatalf("registry.Get() error = %v", err)
	}
	if got := h.Descriptor().Type; got != "test.workflow_declared" {
		t.Fatalf("handler type = %q, want test.workflow_declared", got)
	}

	wf := Workflow("portable-handler")
	start := wf.Node("start", node.Start())
	portable := wf.Node("portable", testWorkflowDeclaredNode.New(nil))
	wf.Connect(start, portable)

	workflowID, err := eng.AddWorkflow(context.Background(), wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}
	id, err := eng.Invoke(context.Background(), workflowID, Start(), map[string]any{"ticket": "VULN-2"})
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}

	result, err := eng.Wait(context.Background(), id)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if got := result.Output["portable"].(map[string]any)["ticket"]; got != "VULN-2" {
		t.Fatalf("portable.ticket = %v, want VULN-2", got)
	}
}

type testBuilderEchoHandler struct{}

func (h *testBuilderEchoHandler) Descriptor() node.Descriptor {
	return node.Descriptor{Type: "test.builder.echo"}
}

func (h *testBuilderEchoHandler) Execute(_ context.Context, input *node.Input) (*node.Output, error) {
	return &node.Output{Data: input.Data}, nil
}

var testWorkflowDeclaredNode = node.Define("test.workflow_declared", func(_ context.Context, input *node.Input) (*node.Output, error) {
	return &node.Output{Data: input.Data}, nil
})

var testRuntimeCaptureNode = node.Define("test.runtime_capture", func(_ context.Context, input *node.Input) (*node.Output, error) {
	out := map[string]any{
		"runtime_tenant_id": nil,
		"vars_tenant_id":    nil,
	}
	if input.Vars != nil {
		out["vars_tenant_id"] = input.Vars["tenant_id"]
	}
	if input.Runtime != nil && input.Runtime.Vars != nil {
		out["runtime_tenant_id"] = input.Runtime.Vars["tenant_id"]
	}
	return &node.Output{Data: out}, nil
})
