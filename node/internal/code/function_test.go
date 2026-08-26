package code_test

import (
	"context"
	"errors"
	"fmt"
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
	b := node.Expr(`{"namespace": $runtime.vars.namespace_id}`)
	input := &types.Input{
		Params:  b.RawParams().(map[string]any),
		Runtime: &types.Runtime{Vars: map[string]any{"namespace_id": "namespace-a"}},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["namespace"] != "namespace-a" {
		t.Fatalf("namespace = %v, want namespace-a", out.Data["namespace"])
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

// TestFunction_WrappedTimeoutIsStillTransient uses the shape a real user
// function's deadline actually has.
//
// TestFunction_TimeoutIsTransient above returns the bare context.DeadlineExceeded
// sentinel, so it passes identically whether function.go compares with
// errors.Is or with ==. Almost nothing returns that sentinel bare: a user
// function that queries a DB or calls an HTTP service gets it back wrapped by
// database/sql, net/http or its own fmt.Errorf("...: %w", err). With ==, every
// one of those falls through to the "error" port branch instead — so a timeout,
// the one condition that is genuinely worth retrying, is committed as a routable
// business error and the message is never reprocessed. That is the exact
// transient-vs-business misclassification engine/doc.go:113 records as having
// bitten the error port before.
func TestFunction_WrappedTimeoutIsStillTransient(t *testing.T) {
	const fnName = "__wrapped_timeout_fn__"
	node.RegisterFunc(fnName, func(_ context.Context, _ *types.Input) (*types.Output, error) {
		// What a real caller returns: the deadline, wrapped with the operation
		// that hit it.
		return nil, fmt.Errorf("query orders: %w", context.DeadlineExceeded)
	})

	h, _ := registry.Lookup("xflow.function")
	b := node.Function(fnName)
	out, err := h.Execute(context.Background(),
		&types.Input{Params: b.RawParams().(map[string]any), Data: map[string]any{}})
	if err == nil {
		t.Fatalf("wrapped deadline produced no error; out = %#v (Port %q): it was "+
			"routed as a business error instead of being classified transient, "+
			"so the message is committed and never retried", out.Data, out.Port)
	}
	if types.IsPermanent(err) {
		t.Fatalf("wrapped deadline must be transient (retryable); got permanent err=%v", err)
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

// TestFunction_SuccessStaysOffTheErrorPort is the other half of the matrix row
// above: the test right before this one is the only place in the package that
// reads Output.Port at all, and it exercises the failure path.
//
// Nothing checks the success path. TestFunction_InlineExpr,
// _InlineExpr_ReturnsMap, _InlineExprCanReadRuntimeVars, _ExtraParams and
// _NamedFunction all assert on out.Data[...] and stop there, so setting
// Port: "error" on executeExpr's two success returns -- or on the value
// executeNamed hands back -- leaves the whole package green while every
// successful function node is committed as a business failure:
// engine/commit.go:446 outputPortRetryError treats Port == "error" with no
// Output.Error as a routable error and synthesises one from Data["error"],
// which an expression result does not have, so the node commits with
// "node returned error port" and the run takes the OnError branch. The Data
// the tests above verify is still perfectly correct; it just never reaches the
// main port.
//
// The expected value is the empty string rather than a named port: the handler
// never sets one on success, and engine/errorpolicy.go:73 leaves RoutePort ""
// for that case, so "" is what the routing layer is actually built around.
func TestFunction_SuccessStaysOffTheErrorPort(t *testing.T) {
	h, _ := registry.Lookup("xflow.function")

	const fnName = "__success_port_fn__"
	node.RegisterFunc(fnName, func(_ context.Context, _ *types.Input) (*types.Output, error) {
		return &types.Output{Data: map[string]any{"ok": true}}, nil
	})

	cases := []struct {
		name  string
		input *types.Input
	}{
		// executeExpr's default branch: a scalar result gets wrapped in
		// {"result": v}.
		{"ExprScalar", &types.Input{
			Params: node.Expr("a + b").RawParams().(map[string]any),
			Data:   map[string]any{"a": 3.0, "b": 4.0},
		}},
		// executeExpr's map branch: a separate return statement.
		{"ExprMap", &types.Input{
			Params: node.Expr(`{"sum": a + b}`).RawParams().(map[string]any),
			Data:   map[string]any{"a": 2.0, "b": 3.0},
		}},
		// executeNamed's success return, which passes the user function's own
		// Output through untouched.
		{"NamedFunction", &types.Input{
			Params: node.Function(fnName).RawParams().(map[string]any),
			Data:   map[string]any{},
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := h.Execute(context.Background(), tc.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if out.Port != "" {
				t.Errorf("successful run returned Port = %q, want \"\": a non-empty "+
					"port on the success path reroutes every successful function "+
					"node away from its main downstream edges, and %q specifically "+
					"makes the engine commit the node as a business failure",
					out.Port, "error")
			}
			if out.Error != nil {
				t.Errorf("successful run returned Error = %v, want nil", out.Error)
			}
		})
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
