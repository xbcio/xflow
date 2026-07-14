package xflow

import (
	"context"
	"errors"
	"github.com/xbcio/xflow/node/registry"
	"strings"
	"testing"

	"github.com/xbcio/xflow/backend/memory"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// sdkVersionedHandler is a unique handler type used only by this test file so
// the global node registry doesn't collide with other suites.
type sdkVersionedHandler struct {
	typ     string
	version int
}

func (h sdkVersionedHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: h.typ}
}

func (h sdkVersionedHandler) NodeVersion() int { return h.version }

func (h sdkVersionedHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{"v": h.version, "echo": input.Data}}, nil
}

func TestAddWorkflow_RejectsMissingHandlerVersion(t *testing.T) {
	provider := memory.New()
	eng, err := newFromConfig(&engineConfig{allowDirectHandlers: false}, provider)
	if err != nil {
		t.Fatalf("newFromConfig() error = %v", err)
	}
	defer eng.Stop()

	// Construct a workflow that references a node type with no registered handler.
	wf := Workflow("missing-handler")
	wf.Node("start", node.Start())
	// Bypass the builder's handler-bundling path by registering a direct NodeDef
	// in build() — easiest path is via a small helper struct.

	// Patch in a synthetic NodeDef by replacing build via a custom helper. We
	// can't easily edit the WorkflowDef post-build, so instead just call
	// preCheckHandlerVersions directly with a hand-crafted def.
	def := &types.WorkflowDef{
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start", Kind: types.NodeKindAction},
			{Name: "missing", Type: "test.never-registered", Kind: types.NodeKindAction, Version: 1},
		},
	}
	err = preCheckHandlerVersions(def, wf)
	if err == nil {
		t.Fatal("preCheckHandlerVersions() error = nil, want missing handler error")
	}
	var mismatch *ErrMissingHandlerVersions
	if !errors.As(err, &mismatch) {
		t.Fatalf("error = %v, want *ErrMissingHandlerVersions", err)
	}
	if len(mismatch.Missing) != 1 || mismatch.Missing[0].Type != "test.never-registered" {
		t.Fatalf("Missing = %+v", mismatch.Missing)
	}
	if !strings.Contains(err.Error(), "test.never-registered") {
		t.Fatalf("error message = %q, missing node type", err.Error())
	}
}

func TestAddWorkflow_AcceptsWorkflowBundledHandlers(t *testing.T) {
	provider := memory.New()
	eng, err := newFromConfig(&engineConfig{allowDirectHandlers: false}, provider)
	if err != nil {
		t.Fatalf("newFromConfig() error = %v", err)
	}
	defer eng.Stop()

	// testWorkflowDeclaredNode is defined in builder_test.go; reuse it.
	wf := Workflow("bundled-ok")
	start := wf.Node("start", node.Start())
	target := wf.Node("portable", testWorkflowDeclaredNode.New(nil))
	wf.Connect(start, target)

	if _, err := eng.AddWorkflow(context.Background(), wf); err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}
}

func TestRegistry_VersionPolicy_WithVersionPolicyOption(t *testing.T) {
	const typ = "test.versioned/sdk-option"
	registry.Register(sdkVersionedHandler{typ: typ, version: 1})

	provider := memory.New()
	eng, err := newFromConfig(&engineConfig{
		allowDirectHandlers: false,
		versionPolicy:       execution.VersionStrict,
		versionPolicySet:    true,
	}, provider)
	if err != nil {
		t.Fatalf("newFromConfig() error = %v", err)
	}
	defer eng.Stop()

	lr, ok := eng.registry.(*execution.Registry)
	if !ok {
		t.Fatal("registry is not *execution.Registry")
	}
	_, err = lr.Get(types.ExecutionID("e1"), "node-a", typ, 99)
	var mismatch *execution.ErrHandlerVersionMismatch
	if !errors.As(err, &mismatch) {
		t.Fatalf("Get() error = %v, want strict mismatch", err)
	}
}
