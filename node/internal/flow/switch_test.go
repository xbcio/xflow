package flow_test

import (
	"context"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
	"testing"

	"github.com/xbcio/xflow/node"
)

func TestSwitch_Factory_Rules(t *testing.T) {
	b := node.Switch([]node.SwitchRule{
		{Condition: "x > 0", Output: "positive"},
		{Condition: "x < 0", Output: "negative"},
	}, "zero")
	if b.NodeType() != "xflow.switch" {
		t.Fatalf("expected xflow.switch, got %s", b.NodeType())
	}
	params := b.RawParams().(map[string]any)
	if params["default_output"] != "zero" {
		t.Fatalf("expected default_output=zero, got %v", params["default_output"])
	}
}

func TestSwitch_Factory_Expr(t *testing.T) {
	b := node.SwitchExpr("category", "other")
	params := b.RawParams().(map[string]any)
	if params["mode"] != "expression" {
		t.Fatalf("expected mode=expression, got %v", params["mode"])
	}
	if params["expression"] != "category" {
		t.Fatalf("expected expression=category, got %v", params["expression"])
	}
}

func TestSwitch_RulesMode(t *testing.T) {
	h, _ := registry.Lookup("xflow.switch")
	b := node.Switch([]node.SwitchRule{
		{Condition: "status == \"active\"", Output: "active"},
		{Condition: "status == \"inactive\"", Output: "inactive"},
	}, "unknown")
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"status": "active"},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Port != "active" {
		t.Fatalf("expected port \"active\", got %q", out.Port)
	}
}

func TestSwitch_RulesMode_Default(t *testing.T) {
	h, _ := registry.Lookup("xflow.switch")
	b := node.Switch([]node.SwitchRule{
		{Condition: "status == \"active\"", Output: "active"},
	}, "fallback")
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"status": "pending"},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Port != "fallback" {
		t.Fatalf("expected port \"fallback\", got %q", out.Port)
	}
}

func TestSwitch_ExpressionMode(t *testing.T) {
	h, _ := registry.Lookup("xflow.switch")
	b := node.SwitchExpr("category", "other")
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"category": "electronics"},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Port != "electronics" {
		t.Fatalf("expected port \"electronics\", got %q", out.Port)
	}
}

// TestSwitch_RoutingPreservesData covers the half of switch's output that no
// test in this file looks at.
//
// switch is a pure router: it picks a port and passes the batch through
// untouched. Every test above checks only out.Port (or the error), so replacing
// `Data: input.Data` with `Data: nil` at all three success returns —
// executeRules' matched-rule return, executeRules' default-output return, and
// executeExpression's return — leaves the whole file green. The run would still
// route correctly and then hand every downstream node an empty batch: a switch
// in the middle of a workflow becomes a data black hole, and because routing is
// unaffected the failure surfaces far away, in whatever node first reads a
// field that is suddenly missing.
//
// The fixtures deliberately carry a payload field alongside the routing key.
// Asserting only on the routing key would be much weaker: production reads that
// key out of input.Data to decide the port in the first place, so a switch that
// forwarded nothing but the routing key would still satisfy it.
func TestSwitch_RoutingPreservesData(t *testing.T) {
	payload := func(status string) map[string]any {
		return map[string]any{
			"status":   status,
			"category": status,
			// Not read by any rule or expression: only a passthrough can
			// produce it on the far side.
			"order_id": "ord-77",
			"amount":   42.0,
		}
	}

	cases := []struct {
		name     string
		params   any
		data     map[string]any
		wantPort string
	}{
		// executeRules, matched-rule return.
		{"RuleMatched",
			node.Switch([]node.SwitchRule{{Condition: `status == "active"`, Output: "active"}}, "unknown").RawParams(),
			payload("active"), "active"},
		// executeRules, default-output return: a separate statement.
		{"RuleDefault",
			node.Switch([]node.SwitchRule{{Condition: `status == "active"`, Output: "active"}}, "fallback").RawParams(),
			payload("pending"), "fallback"},
		// executeExpression's return.
		{"Expression",
			node.SwitchExpr("category", "other").RawParams(),
			payload("electronics"), "electronics"},
	}

	h, _ := registry.Lookup("xflow.switch")
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := h.Execute(context.Background(), &types.Input{
				Params: tc.params.(map[string]any),
				Data:   tc.data,
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if out.Port != tc.wantPort {
				t.Fatalf("port = %q, want %q", out.Port, tc.wantPort)
			}
			if out.Data == nil {
				t.Fatalf("routed to %q with Data = nil: switch forwarded the batch "+
					"to the right port and emptied it on the way", tc.wantPort)
			}
			if got := out.Data["order_id"]; got != "ord-77" {
				t.Errorf("out.Data[\"order_id\"] = %#v, want %q: the field is read by "+
					"neither the rule nor the expression, so only a genuine "+
					"passthrough carries it to the downstream node", got, "ord-77")
			}
			if got := out.Data["amount"]; got != 42.0 {
				t.Errorf("out.Data[\"amount\"] = %#v, want 42", got)
			}
		})
	}
}

func TestSwitch_ExpressionMode_MissingExpr(t *testing.T) {
	h, _ := registry.Lookup("xflow.switch")
	input := &types.Input{
		Params: map[string]any{"mode": "expression"},
		Data:   map[string]any{},
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for missing expression")
	}
}

func TestSwitch_UnknownMode(t *testing.T) {
	h, _ := registry.Lookup("xflow.switch")
	input := &types.Input{
		Params: map[string]any{"mode": "invalid"},
		Data:   map[string]any{},
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for unknown mode")
	}
}
