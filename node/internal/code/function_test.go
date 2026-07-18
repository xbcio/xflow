package code_test

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

func TestFunction_Factory(t *testing.T) {
	b := node.Function("calculate_tax")
	if b.NodeType() != "xflow.function" {
		t.Fatalf("expected xflow.function, got %s", b.NodeType())
	}
	params := b.RawParams().(map[string]any)
	if params["function_name"] != "calculate_tax" {
		t.Fatalf("expected function_name, got %v", params)
	}
}

func TestExpr_Factory(t *testing.T) {
	b := node.Expr("price * 1.1")
	params := b.RawParams().(map[string]any)
	if params["code"] != "price * 1.1" {
		t.Fatalf("expected code, got %v", params)
	}
}

func TestFunction_InlineExpr(t *testing.T) {
	h, _ := registry.Lookup("xflow.function")
	b := node.Expr("a + b")
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"a": 3.0, "b": 4.0},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["result"] != 7.0 {
		t.Fatalf("expected result=7, got %v", out.Data["result"])
	}
}

func TestFunction_InlineExpr_ReturnsMap(t *testing.T) {
	h, _ := registry.Lookup("xflow.function")
	b := node.Expr(`{"sum": a + b, "product": a * b}`)
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"a": 2.0, "b": 3.0},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["sum"] != 5.0 {
		t.Fatalf("expected sum=5, got %v", out.Data["sum"])
	}
	if out.Data["product"] != 6.0 {
		t.Fatalf("expected product=6, got %v", out.Data["product"])
	}
}

func TestFunction_InlineExprCanReadRuntimeVars(t *testing.T) {
	h, _ := registry.Lookup("xflow.function")
	b := node.Expr(`{"tenant": $runtime.vars.tenant_id}`)
	input := &types.Input{
		Params:  b.RawParams().(map[string]any),
		Runtime: &types.Runtime{Vars: map[string]any{"tenant_id": "tenant-a"}},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["tenant"] != "tenant-a" {
		t.Fatalf("tenant = %v, want tenant-a", out.Data["tenant"])
	}
}

func TestFunction_NamedFunction(t *testing.T) {
	node.RegisterFunc("test_double", func(_ context.Context, input *types.Input) (*types.Output, error) {
		val, _ := input.Data["value"].(float64)
		return &types.Output{Data: map[string]any{"result": val * 2}}, nil
	})

	h, _ := registry.Lookup("xflow.function")
	b := node.Function("test_double")
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"value": 5.0},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["result"] != 10.0 {
		t.Fatalf("expected result=10, got %v", out.Data["result"])
	}
}

func TestFunction_UnregisteredFunction(t *testing.T) {
	h, _ := registry.Lookup("xflow.function")
	b := node.Function("nonexistent_fn")
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{},
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for unregistered function")
	}
}

func TestFunction_NeitherCodeNorName(t *testing.T) {
	h, _ := registry.Lookup("xflow.function")
	input := &types.Input{
		Params: map[string]any{},
		Data:   map[string]any{},
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error when neither code nor function_name provided")
	}
}

// TestFunction_ConfigErrorsArePermanent verifies the A3 code/script classifier
// migration (2026-07-18 remediation §6.4 step 5): config errors that cannot
// self-heal on retry are NewPermanentError, not bare fmt.Errorf (which would
// collapse to transient and retry to MaxAttempts). Classification must survive
// errors.As so it crosses the wire via protocol.error_detail.
func TestFunction_ConfigErrorsArePermanent(t *testing.T) {
	h, _ := registry.Lookup("xflow.function")

	cases := []struct {
		name   string
		params map[string]any
		code   string
	}{
		{"neither_code_nor_name", map[string]any{}, "function.config_required"},
		{"unregistered_function", map[string]any{"function_name": "no_such_fn"}, "function.not_registered"},
		{"expr_eval_error", map[string]any{"code": "price *"}, "function.expr_eval"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			input := &types.Input{Params: c.params, Data: map[string]any{}}
			_, err := h.Execute(context.Background(), input)
			if err == nil {
				t.Fatalf("expected error for %s", c.name)
			}
			if !types.IsPermanent(err) {
				t.Fatalf("%s must be permanent (not retried); got err=%v", c.name, err)
			}
			var ce *types.ClassifiedError
			if !errors.As(err, &ce) || ce.Code != c.code {
				t.Fatalf("expected ClassifiedError code=%q, got %T %v", c.code, err, err)
			}
		})
	}
}

// TestFunction_TimeoutIsTransient verifies a user function hitting a deadline
// is classified transient (retryable) rather than routed to the error port.
func TestFunction_TimeoutIsTransient(t *testing.T) {
	const fnName = "__parity_timeout_fn__"
	registered := false
	node.RegisterFunc(fnName, func(ctx context.Context, _ *types.Input) (*types.Output, error) {
		registered = true
		return nil, context.DeadlineExceeded
	})
	defer func() {
		// RegisterFunc has no unregister; leave the name — it is test-scoped
		// and uniquely named to avoid colliding with real registrations.
		_ = registered
	}()

	h, _ := registry.Lookup("xflow.function")
	b := node.Function(fnName)
	input := &types.Input{Params: b.RawParams().(map[string]any), Data: map[string]any{}}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected transient error for function deadline")
	}
	if types.IsPermanent(err) {
		t.Fatalf("deadline must be transient (retryable); got permanent err=%v", err)
	}
	var ce *types.ClassifiedError
	if !errors.As(err, &ce) || ce.Code != "function.timeout" {
		t.Fatalf("expected ClassifiedError code=function.timeout, got %T %v", err, err)
	}
}

// TestFunction_UserErrorRoutesToErrorPort verifies a user function's
// deterministic error stays on the explicit "error" port (the routable
// business-error / explicit-error-port-output matrix row), NOT a Go error.
func TestFunction_UserErrorRoutesToErrorPort(t *testing.T) {
	const fnName = "__parity_usererr_fn__"
	node.RegisterFunc(fnName, func(_ context.Context, _ *types.Input) (*types.Output, error) {
		return nil, errors.New("business reject")
	})

	h, _ := registry.Lookup("xflow.function")
	b := node.Function(fnName)
	input := &types.Input{Params: b.RawParams().(map[string]any), Data: map[string]any{}}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("user error should route to port, not Go error: %v", err)
	}
	if out.Port != "error" {
		t.Fatalf("expected error port, got %q", out.Port)
	}
}

func TestFunction_ExtraParams(t *testing.T) {
	h, _ := registry.Lookup("xflow.function")
	input := &types.Input{
		Params: map[string]any{
			"code":   "x + multiplier",
			"params": map[string]any{"multiplier": 10.0},
		},
		Data: map[string]any{"x": 5.0},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["result"] != 15.0 {
		t.Fatalf("expected result=15, got %v", out.Data["result"])
	}
}
