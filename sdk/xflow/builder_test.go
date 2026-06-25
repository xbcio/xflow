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

func TestEngineSubmitRejectsDirectHandlersWhenBackendDoesNotAllowThem(t *testing.T) {
	provider := memory.New()
	eng, err := newFromConfig(&engineConfig{allowDirectHandlers: false}, provider)
	if err != nil {
		t.Fatalf("newFromConfig() error = %v", err)
	}
	defer eng.Stop()

	wf := Workflow("cluster-direct-handler")
	wf.LocalNode("local-only", &testBuilderEchoHandler{})

	_, err = eng.Submit(context.Background(), wf, nil)
	if err == nil {
		t.Fatal("Submit() error = nil, want direct-handler rejection")
	}
	if !strings.Contains(err.Error(), "node.Define") {
		t.Fatalf("Submit() error = %v, want node.Define guidance", err)
	}
}

func TestEngineSubmitRegistersWorkflowDeclaredHandlers(t *testing.T) {
	provider := memory.New()
	eng, err := newFromConfig(&engineConfig{allowDirectHandlers: false}, provider)
	if err != nil {
		t.Fatalf("newFromConfig() error = %v", err)
	}
	defer eng.Stop()

	wf := Workflow("portable-handler")
	wf.Node("portable", testWorkflowDeclaredNode.New(map[string]any{"source": "workflow"}))

	id, err := eng.Submit(context.Background(), wf, map[string]any{"ticket": "VULN-1"})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	result, err := eng.Wait(context.Background(), id)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if got := result.Output["portable"].(map[string]any)["ticket"]; got != "VULN-1" {
		t.Fatalf("portable.ticket = %v, want VULN-1", got)
	}
}

func TestWithNodesRegistersConsumerCapabilities(t *testing.T) {
	provider := memory.New()
	eng, err := newFromConfig(&engineConfig{
		allowDirectHandlers: false,
		nodes:               []*node.Definition{testWorkflowDeclaredNode},
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
	wf.Node("portable", testWorkflowDeclaredNode.New(nil))

	id, err := eng.Submit(context.Background(), wf, map[string]any{"ticket": "VULN-2"})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
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
