package execution

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// bodyParams builds an xflow.map parameter set whose body sub-graph has one
// member carrying the given parameters. Shared by the tests below so they
// cannot disagree about what a body's shape is.
func bodyParams(memberParams map[string]any) map[string]any {
	return map[string]any{
		"items": "$input.rows",
		"body": map[string]any{
			"type": "xflow.subgraph",
			"parameters": map[string]any{
				"nodes": []any{
					map[string]any{
						"name":       "inner",
						"type":       "test.noop",
						"parameters": memberParams,
					},
				},
			},
		},
	}
}

// bodyMemberParams digs the body's single member's parameters back out of an
// evaluated parameter tree, so a test can assert on what the boundary did (or
// did not) do to them.
func bodyMemberParams(t *testing.T, params map[string]any) map[string]any {
	t.Helper()
	body, ok := params["body"].(map[string]any)
	if !ok {
		t.Fatalf("body is %T, want map[string]any", params["body"])
	}
	bp, ok := body["parameters"].(map[string]any)
	if !ok {
		t.Fatalf("body.parameters is %T, want map[string]any", body["parameters"])
	}
	nodes, ok := bp["nodes"].([]any)
	if !ok || len(nodes) != 1 {
		t.Fatalf("body.parameters.nodes is %#v, want a one-element []any", bp["nodes"])
	}
	member, ok := nodes[0].(map[string]any)
	if !ok {
		t.Fatalf("body member is %T, want map[string]any", nodes[0])
	}
	mp, ok := member["parameters"].(map[string]any)
	if !ok {
		t.Fatalf("body member parameters is %T, want map[string]any", member["parameters"])
	}
	return mp
}

// TestEvaluateParams_BodyMemberPerItemRootsSurviveBoundary is the primary
// regression test for the boundary-walks-into-the-body defect.
//
// A body member's parameters are evaluated by the INNER execution, against the
// per-item environment the map adapter injects ($item/$index/$items — see
// execution/subgraph/map_body.go's bodyItemInput). Those roots do not exist in
// the OUTER map node's environment, so evaluating a body member's parameter
// here fails with "unknown name $item".
//
// The consequence is not a bad value, it is a hung execution: the boundary
// error is returned as a system error, which the engine classifies as a
// retriable transient failure, so the outer xflow.map task retries forever and
// its handler is never called once. Measured on backend/providers/local before
// the fix: status stays "running", the map handler logs nothing.
func TestEvaluateParams_BodyMemberPerItemRootsSurviveBoundary(t *testing.T) {
	const tmpl = "${{ $item.id }}"
	input := &types.Input{
		NodeName: "m",
		Data:     map[string]any{"rows": []any{1}},
		Params:   bodyParams(map[string]any{"id": tmpl}),
	}
	if err := evaluateParams(input, "xflow.map"); err != nil {
		t.Fatalf("evaluateParams must not evaluate a body member's parameters -- "+
			"$item/$index/$items are injected by the map adapter for the INNER "+
			"execution and do not exist in the outer node's env, so touching them "+
			"here fails the outer map node before its handler ever runs (the error "+
			"is transient, so the task retries forever and the execution hangs): %v", err)
	}
	if got := bodyMemberParams(t, input.Params)["id"]; got != tmpl {
		t.Fatalf("body member parameter = %#v, want the template preserved verbatim (%q)", got, tmpl)
	}
}

// TestEvaluateParams_BodyMemberNodesRefSurvivesBoundary is the same claim for
// $nodes. A body member's $nodes reference resolves against the INNER graph's
// state (deriveNodesRefs in engine/graph/nodes_refs.go deliberately skips the
// body for exactly this reason), so the outer boundary has no value for it and
// evaluating it here fails the outer node.
func TestEvaluateParams_BodyMemberNodesRefSurvivesBoundary(t *testing.T) {
	const tmpl = "${{ $nodes['inner_upstream'].value }}"
	input := &types.Input{
		NodeName: "m",
		Data:     map[string]any{"rows": []any{1}},
		Params:   bodyParams(map[string]any{"from": tmpl}),
	}
	if err := evaluateParams(input, "xflow.map"); err != nil {
		t.Fatalf("evaluateParams must not evaluate a body member's $nodes reference "+
			"-- it resolves against the inner graph's state, which the outer "+
			"boundary cannot see: %v", err)
	}
	if got := bodyMemberParams(t, input.Params)["from"]; got != tmpl {
		t.Fatalf("body member parameter = %#v, want the template preserved verbatim (%q)", got, tmpl)
	}
}

// TestEvaluateParams_BodyMemberOuterResolvableTemplateNotEvaluated is the
// leak-direction complement. A body member template that HAPPENS to be
// resolvable in the outer env must still be left alone: the body is the inner
// execution's source text, and baking an outer value into it would give a body
// member a value from a scope it never declared a dependency on. Before the fix
// this silently substituted the outer value.
func TestEvaluateParams_BodyMemberOuterResolvableTemplateNotEvaluated(t *testing.T) {
	const tmpl = "${{ $input.outer_only }}"
	input := &types.Input{
		NodeName: "m",
		Data: map[string]any{
			"rows":       []any{1},
			"outer_only": "OUTER-VALUE",
		},
		Params: bodyParams(map[string]any{"leaked": tmpl}),
	}
	if err := evaluateParams(input, "xflow.map"); err != nil {
		t.Fatalf("evaluateParams: %v", err)
	}
	got := bodyMemberParams(t, input.Params)["leaked"]
	if got == "OUTER-VALUE" {
		t.Fatal("the boundary substituted an OUTER value into a body member's " +
			"parameter -- the body is the inner execution's source text and must " +
			"reach it verbatim")
	}
	if got != tmpl {
		t.Fatalf("body member parameter = %#v, want the template preserved verbatim (%q)", got, tmpl)
	}
}

// TestEvaluateParams_MapOwnParamsStillEvaluated is the positive control: the
// body skip must not turn into a whole-node skip. An xflow.map's own
// non-exempt parameters are still evaluated at the boundary.
//
// "items" and "expression" are exempt (the handler evaluates them), so this
// uses batch_size -- a plain literal parameter the handler reads with
// cast.ToIntE.
func TestEvaluateParams_MapOwnParamsStillEvaluated(t *testing.T) {
	params := bodyParams(map[string]any{"id": "plain"})
	params["batch_size"] = "${{ $input.size }}"
	input := &types.Input{
		NodeName: "m",
		Data:     map[string]any{"rows": []any{1}, "size": 7},
		Params:   params,
	}
	if err := evaluateParams(input, "xflow.map"); err != nil {
		t.Fatalf("evaluateParams: %v", err)
	}
	if got := input.Params["batch_size"]; got != 7 {
		t.Fatalf("batch_size = %#v (%T), want 7 -- skipping the body must not "+
			"skip the map node's own parameters", got, got)
	}
}

// TestEvaluateParams_HTTPBodyStillEvaluated is the other side of the skip: the
// exemption keys off the VALUE's shape (a "body" whose type is xflow.subgraph),
// never the parameter NAME. xflow.http's "body" is a request payload and its
// templates must still be evaluated -- that is the shape the boundary layer
// exists to support (DSL-SPECIFICATION.md §4.1).
func TestEvaluateParams_HTTPBodyStillEvaluated(t *testing.T) {
	input := &types.Input{
		NodeName: "h",
		Data:     map[string]any{"order_id": "A-1"},
		Params: map[string]any{
			"url": "https://example.invalid/orders",
			"body": map[string]any{
				"id": "${{ $input.order_id }}",
			},
		},
	}
	if err := evaluateParams(input, "xflow.http"); err != nil {
		t.Fatalf("evaluateParams: %v", err)
	}
	body, ok := input.Params["body"].(map[string]any)
	if !ok {
		t.Fatalf("body is %T, want map[string]any", input.Params["body"])
	}
	if got := body["id"]; got != "A-1" {
		t.Fatalf("http body id = %#v, want \"A-1\" -- an http request payload named "+
			"\"body\" is not a sub-graph body and must still be evaluated", got)
	}
}

// TestEvaluateParams_NonSubgraphBodyShapesStillEvaluated covers the value
// shapes an http body takes that are NOT maps with a subgraph type: a string,
// an array, and a map whose "type" is something else. None may be skipped.
func TestEvaluateParams_NonSubgraphBodyShapesStillEvaluated(t *testing.T) {
	cases := []struct {
		name string
		body any
		want any
	}{
		{"string", "${{ $input.order_id }}", "A-1"},
		{"array", []any{"${{ $input.order_id }}"}, "A-1"},
		{"map with other type", map[string]any{
			"type": "application/json",
			"id":   "${{ $input.order_id }}",
		}, "A-1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := &types.Input{
				NodeName: "h",
				Data:     map[string]any{"order_id": "A-1"},
				Params:   map[string]any{"body": tc.body},
			}
			if err := evaluateParams(input, "xflow.http"); err != nil {
				t.Fatalf("evaluateParams: %v", err)
			}
			var got any
			switch v := input.Params["body"].(type) {
			case string:
				got = v
			case []any:
				got = v[0]
			case map[string]any:
				got = v["id"]
			default:
				t.Fatalf("body is %T after evaluation", input.Params["body"])
			}
			if got != tc.want {
				t.Fatalf("evaluated body = %#v, want %#v", got, tc.want)
			}
		})
	}
}

// TestEvaluateParams_MalformedBodyMemberTemplateNotReported is a documentation
// test for a deliberate consequence of the skip: a malformed template inside a
// body member is not this layer's error to raise. It is already a compile
// error -- validateTemplateForm walks the whole parameter tree including the
// body (walkStrings does not stop at "body"), so a malformed body member
// template never reaches a running workflow at all.
func TestEvaluateParams_MalformedBodyMemberTemplateNotReported(t *testing.T) {
	const malformed = "prefix ${{ $item.id }}"
	input := &types.Input{
		NodeName: "m",
		Data:     map[string]any{"rows": []any{1}},
		Params:   bodyParams(map[string]any{"id": malformed}),
	}
	if err := evaluateParams(input, "xflow.map"); err != nil {
		if strings.Contains(err.Error(), "malformed") {
			t.Fatalf("the boundary reported a malformed body member template; that is "+
				"validateTemplateForm's job at compile time, not this layer's: %v", err)
		}
		t.Fatalf("evaluateParams: %v", err)
	}
	if got := bodyMemberParams(t, input.Params)["id"]; got != malformed {
		t.Fatalf("body member parameter = %#v, want it preserved verbatim", got)
	}
}
