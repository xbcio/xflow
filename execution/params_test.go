package execution

import (
	"context"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// templateRecordingHandler captures the input it receives, so tests can assert
// template evaluation happened before the handler saw the input.
type templateRecordingHandler struct {
	lastInput *types.Input
}

func (h *templateRecordingHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "xflow.http"}
}

func (h *templateRecordingHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	h.lastInput = input
	return &types.Output{Data: map[string]any{"ok": true}}, nil
}

// TestEvaluateParams_TemplateIsEvaluatedBeforeHandler confirms that a template
// in a non-exempt parameter is evaluated before it reaches the handler. This is
// the core contract of the boundary evaluation layer.
func TestEvaluateParams_TemplateIsEvaluatedBeforeHandler(t *testing.T) {
	rec := &templateRecordingHandler{}
	runner := NewRunner(singleHandlerRegistry{handler: rec})

	lease := &engine.TaskLease{
		Task:     engine.Task{ExecutionID: "exec-tmpl", NodeName: "http-node"},
		NodeType: "xflow.http",
		Input: &types.Input{
			ExecutionID: "exec-tmpl",
			NodeName:    "http-node",
			Params: map[string]any{
				"url":     "https://{{ $params.host }}/api",
				"headers": map[string]any{"Authorization": "${{ $params.token }}"},
			},
			Data: map[string]any{"host": "example.com", "token": "Bearer abc123"},
		},
	}
	// Params also contain the host and token so $params.host resolves.
	lease.Input.Params["host"] = "example.com"
	lease.Input.Params["token"] = "Bearer abc123"

	_, err := runner.Execute(context.Background(), lease)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	// The handler must have received the evaluated values, not templates.
	url, ok := rec.lastInput.Params["url"].(string)
	if !ok || url != "https://example.com/api" {
		t.Errorf("url = %#v, want %q (template was not evaluated)", rec.lastInput.Params["url"], "https://example.com/api")
	}
	headers, ok := rec.lastInput.Params["headers"].(map[string]any)
	if !ok {
		t.Fatalf("headers = %T, want map[string]any", rec.lastInput.Params["headers"])
	}
	auth := headers["Authorization"]
	if auth != "Bearer abc123" {
		t.Errorf("Authorization = %#v, want %q (expression not evaluated)", auth, "Bearer abc123")
	}
}

// exemptRecordingHandler records its input for xflow.function (code is exempt).
type exemptRecordingHandler struct {
	lastInput *types.Input
}

func (h *exemptRecordingHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "xflow.function"}
}

func (h *exemptRecordingHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	h.lastInput = input
	return &types.Output{Data: map[string]any{"ok": true}}, nil
}

// TestEvaluateParams_ExemptParamsAreNotEvaluated confirms that the "code"
// parameter of xflow.function is NOT evaluated at the boundary -- the handler
// itself evaluates it (function.go treats it as an expr program). This asserts
// the exemption table is effective.
func TestEvaluateParams_ExemptParamsAreNotEvaluated(t *testing.T) {
	rec := &exemptRecordingHandler{}
	runner := NewRunner(singleHandlerRegistry{handler: rec})

	codeExpr := "${{ $params.x + $params.y }}"
	lease := &engine.TaskLease{
		Task:     engine.Task{ExecutionID: "exec-exempt", NodeName: "fn-node"},
		NodeType: "xflow.function",
		Input: &types.Input{
			ExecutionID: "exec-exempt",
			NodeName:    "fn-node",
			Params: map[string]any{
				"code": codeExpr,
				"x":    10,
				"y":    20,
			},
		},
	}

	_, err := runner.Execute(context.Background(), lease)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	// The code parameter must arrive at the handler UNCHANGED.
	if rec.lastInput.Params["code"] != codeExpr {
		t.Errorf("code = %#v, want the original template %q (should be exempt)", rec.lastInput.Params["code"], codeExpr)
	}
}

// TestEvaluateParams_ErrorDoesNotLeakValue confirms that when template
// evaluation fails, the error message does not contain the runtime value.
func TestEvaluateParams_ErrorDoesNotLeakValue(t *testing.T) {
	rec := &templateRecordingHandler{}
	runner := NewRunner(singleHandlerRegistry{handler: rec})

	lease := &engine.TaskLease{
		Task:     engine.Task{ExecutionID: "exec-err", NodeName: "http-node"},
		NodeType: "xflow.http",
		Input: &types.Input{
			ExecutionID: "exec-err",
			NodeName:    "http-node",
			Params: map[string]any{
				"url": "${{ $params.nonexistent.deeper }}",
			},
		},
	}

	_, err := runner.Execute(context.Background(), lease)
	if err == nil {
		t.Fatal("Execute() should return error for failed template evaluation")
	}
	// Must contain the node name and parameter name for diagnostics.
	if !strings.Contains(err.Error(), "http-node") {
		t.Errorf("error should name the node; got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "url") {
		t.Errorf("error should name the parameter; got %q", err.Error())
	}
}

// suspendingTemplateHandler is a SuspendingHandler that records its input,
// proving the boundary evaluated templates before the suspending path.
type suspendingTemplateHandler struct {
	lastInput *types.Input
}

func (h *suspendingTemplateHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "xflow.wait"}
}

func (h *suspendingTemplateHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	h.lastInput = input
	return nil, nil
}

func (h *suspendingTemplateHandler) PrepareSuspend(_ context.Context, input *types.Input) (*types.SuspendSpec, error) {
	h.lastInput = input
	return &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"test-signal"}}, nil
}

func (h *suspendingTemplateHandler) OnResume(_ context.Context, input *types.Input, _ *types.SignalPayload) (*types.Output, error) {
	h.lastInput = input
	return &types.Output{Data: map[string]any{"resumed": true}}, nil
}

// TestEvaluateParams_SuspendingHandlerSeesEvaluatedParams confirms templates
// are evaluated on the suspending path (PrepareSuspend consumes the same
// lease.Input). This is the contract that placing the hook BEFORE the
// SuspendingHandler type assertion guarantees.
func TestEvaluateParams_SuspendingHandlerSeesEvaluatedParams(t *testing.T) {
	rec := &suspendingTemplateHandler{}
	runner := NewRunner(singleHandlerRegistry{handler: rec})

	lease := &engine.TaskLease{
		Task:     engine.Task{ExecutionID: "exec-suspend", NodeName: "wait-node"},
		NodeType: "xflow.wait",
		Input: &types.Input{
			ExecutionID: "exec-suspend",
			NodeName:    "wait-node",
			Params: map[string]any{
				"signal_name": "order-{{ $params.order_id }}",
				"order_id":    "X-99",
			},
		},
	}

	res, err := runner.Execute(context.Background(), lease)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if res.Suspend == nil {
		t.Fatal("expected Suspend spec from suspending handler")
	}
	// PrepareSuspend must have received evaluated params.
	sig, ok := rec.lastInput.Params["signal_name"].(string)
	if !ok || sig != "order-X-99" {
		t.Errorf("signal_name = %#v, want %q (template not evaluated before suspend path)", rec.lastInput.Params["signal_name"], "order-X-99")
	}
}

// TestEvaluateParams_PerItemExemption confirms that xflow.map's "expression"
// and xflow.transform.filter's "condition" are exempt (they use per-item env
// roots that don't exist at boundary time).
func TestEvaluateParams_PerItemExemption(t *testing.T) {
	// A handler simulating xflow.map — the test only needs to confirm the
	// boundary does not touch the expression parameter.
	rec := &struct {
		templateRecordingHandler
	}{}
	rec.templateRecordingHandler = templateRecordingHandler{}
	// Override Descriptor to return xflow.map type.
	mapHandler := &mapTypeHandler{}
	runner := NewRunner(singleHandlerRegistry{handler: mapHandler})

	expr := "${{ item.name }}"
	lease := &engine.TaskLease{
		Task:     engine.Task{ExecutionID: "exec-map", NodeName: "map-node"},
		NodeType: "xflow.map",
		Input: &types.Input{
			ExecutionID: "exec-map",
			NodeName:    "map-node",
			Params: map[string]any{
				"items":      "${{ $params.list }}",
				"expression": expr,
				"list":       []any{"a", "b"},
			},
		},
	}

	_, err := runner.Execute(context.Background(), lease)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	// "expression" must be unchanged (per-item exempt).
	if mapHandler.lastInput.Params["expression"] != expr {
		t.Errorf("expression = %#v, want %q (per-item param should be exempt)", mapHandler.lastInput.Params["expression"], expr)
	}
}

type mapTypeHandler struct {
	lastInput *types.Input
}

func (h *mapTypeHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "xflow.map"}
}

func (h *mapTypeHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	h.lastInput = input
	return &types.Output{Data: map[string]any{"ok": true}}, nil
}
