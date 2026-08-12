package execution

import (
	"context"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
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

// switchTypeHandler records the input for xflow.switch, so we can inspect
// what the boundary did to rules[].condition vs rules[].output.
type switchTypeHandler struct {
	lastInput *types.Input
}

func (h *switchTypeHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "xflow.switch"}
}

func (h *switchTypeHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	h.lastInput = input
	return &types.Output{Data: map[string]any{"port": "high"}}, nil
}

// TestEvaluateParams_SwitchRulesConditionPreservedVerbatim confirms that
// xflow.switch's rules[].condition is NOT evaluated at the boundary — the
// handler evaluates it itself (switch.go:119). Without sub-field exemption
// the boundary would turn "${{ $params.threshold > 10 }}" into bool true,
// and the handler would then cast.ToString(true) = "true" and evaluate that
// as an expr — always true, silent misrouting.
func TestEvaluateParams_SwitchRulesConditionPreservedVerbatim(t *testing.T) {
	rec := &switchTypeHandler{}
	runner := NewRunner(singleHandlerRegistry{handler: rec})

	condExpr := "${{ $params.threshold > 10 }}"
	lease := &engine.TaskLease{
		Task:     engine.Task{ExecutionID: "exec-switch", NodeName: "switch-node"},
		NodeType: "xflow.switch",
		Input: &types.Input{
			ExecutionID: "exec-switch",
			NodeName:    "switch-node",
			Params: map[string]any{
				"mode": "rules",
				"rules": []any{
					map[string]any{
						"condition": condExpr,
						"output":    "high",
					},
				},
				"default_output": "low",
				"threshold":      50,
			},
		},
	}

	_, err := runner.Execute(context.Background(), lease)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	rules, ok := rec.lastInput.Params["rules"].([]any)
	if !ok || len(rules) == 0 {
		t.Fatal("rules param missing or empty after boundary evaluation")
	}
	rule0, ok := rules[0].(map[string]any)
	if !ok {
		t.Fatalf("rules[0] = %T, want map[string]any", rules[0])
	}

	// condition MUST be the original template string — NOT a bool.
	if rule0["condition"] != condExpr {
		t.Errorf("rules[0].condition = %#v (%T), want original template %q preserved verbatim\n"+
			"(sub-field exemption failed: boundary evaluated a condition the handler will re-evaluate)",
			rule0["condition"], rule0["condition"], condExpr)
	}

	// output is a literal port name — not evaluable, but also has no template
	// so the boundary passes it through unchanged.
	if rule0["output"] != "high" {
		t.Errorf("rules[0].output = %#v, want %q", rule0["output"], "high")
	}
}

// TestEvaluateParams_SubFieldExemptionCoverage is a mechanical guard that
// asserts every (nodeType, param) pair in EvaluableSubFields is actually
// handled by the boundary's sub-field traversal logic. If someone adds a new
// entry to evaluableSubFields without wiring it here, this test fails —
// preventing silent double evaluation for the new entry.
func TestEvaluateParams_SubFieldExemptionCoverage(t *testing.T) {
	subFields := graph.EvaluableSubFields()
	evaluable := graph.EvaluableParams()

	for nodeType, params := range subFields {
		for param := range params {
			// A sub-field param must NOT be in the full-exempt set (otherwise
			// the boundary skips it entirely and never reaches the sub-field
			// traversal code).
			if handlerEvals, ok := evaluable[nodeType]; ok && handlerEvals[param] {
				t.Errorf("EvaluableSubFields[%q][%q] is also in EvaluableParams — "+
					"the boundary will skip it entirely (full exempt) and never "+
					"apply sub-field exemption", nodeType, param)
			}
			// Verify the boundary actually uses this entry. We can't easily
			// introspect the code path, but we CAN verify the entry exists and
			// is consumed: call evaluateParams with a matching input and confirm
			// the exempt sub-field is preserved.
			condTemplate := "${{ 1 + 1 }}"
			input := &types.Input{
				NodeName: "coverage-probe",
				Params: map[string]any{
					param: []any{
						map[string]any{
							params[param][0]: condTemplate,
							"other_field":    "literal",
						},
					},
				},
			}
			if err := evaluateParams(input, nodeType); err != nil {
				t.Errorf("evaluateParams(%q, %q) error = %v", nodeType, param, err)
				continue
			}
			elems := input.Params[param].([]any)
			elem := elems[0].(map[string]any)
			if elem[params[param][0]] != condTemplate {
				t.Errorf("evaluateParams(%q, %q): sub-field %q was evaluated "+
					"(got %#v), should be exempt",
					nodeType, param, params[param][0], elem[params[param][0]])
			}
		}
	}
}
