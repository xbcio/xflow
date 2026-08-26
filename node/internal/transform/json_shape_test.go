package transform_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// Every other test in this package builds Input.Params from a builder's
// RawParams() and hands it to the handler directly. That shape is Go-native:
// SortNode.RawParams returns []map[string]any, AggregateNode.RawParams returns
// []map[string]any, RenameNode.RawParams returns map[string]string.
//
// Production never sees those types. A graph is stored as JSON and reloaded
// with json.Unmarshal (rstate.Store.LoadGraph; engine/graph/snapshot.go:560,
// and the comment at engine/graph/node_timeout_test.go:48-53 says so in as
// many words), which yields []any of map[string]any, and map[string]any.
// The boundary evaluation layer does not convert between them either:
// execution/params.go's evaluateParamValue rebuilds map[string]any and []any
// and passes everything else through its default case, so a []map[string]any
// stays []map[string]any and a []any stays []any.
//
// Each of the three parsers has a separate branch per shape with the field
// extraction written out twice:
//
//	parseAggregateOperations  aggregate.go:106 []map[string]any | :120 []any
//	parseSortFields           sort.go:119      []map[string]any | :125 []any
//	parseStringMap            helpers.go:57    map[string]string | :60 map[string]any
//
// The tests exercise the first column. Production only ever reaches the
// second. So the whole right-hand column can be wrong — read the wrong key,
// hard-code a value, return nothing — and every test in this package stays
// green, because the code they run is a different copy.
//
// The tests below run the same builders through an actual json.Marshal +
// json.Unmarshal before executing, which is the shape the handler is given at
// runtime. They are deliberately not a third hand-written literal: a literal
// records what someone believed the JSON looks like, whereas the round trip
// produces it.

// jsonParams renders a builder's params the way a reloaded graph does.
func jsonParams(t *testing.T, b interface{ RawParams() any }) map[string]any {
	t.Helper()
	raw, err := json.Marshal(b.RawParams())
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal params: %v", err)
	}
	return out
}

// assertJSONShape fails if the round trip did not actually produce the
// production types. Without it, a future change to RawParams that already
// emits []any would make the tests below pass for the wrong reason — they
// would be exercising the same branch as everything else again, and the
// coverage they were written for would be gone with no failure.
func assertJSONArrayOfObjects(t *testing.T, params map[string]any, key string) {
	t.Helper()
	arr, ok := params[key].([]any)
	if !ok {
		t.Fatalf("params[%q] after a JSON round trip is %T, want []any: this test "+
			"exists to cover the []any branch, and it is not reaching it", key, params[key])
	}
	if len(arr) == 0 {
		t.Fatalf("params[%q] round-tripped to an empty array", key)
	}
	if _, ok := arr[0].(map[string]any); !ok {
		t.Fatalf("params[%q][0] is %T, want map[string]any", key, arr[0])
	}
}

func TestAggregateParsesOperationsInTheirStoredJSONShape(t *testing.T) {
	b := node.Aggregate("items").
		Count("order_count").
		Sum("amount", "total_amount").
		Average("amount", "average_amount")

	params := jsonParams(t, b)
	assertJSONArrayOfObjects(t, params, "operations")

	h, ok := registry.Lookup("xflow.transform.aggregate")
	if !ok {
		t.Fatal("aggregate handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: params,
		Data: map[string]any{
			"items": []any{
				map[string]any{"amount": 10},
				map[string]any{"amount": 20},
				map[string]any{"amount": 30},
			},
		},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	// The three kinds must stay distinguishable. A parser that reads the wrong
	// key, or hard-codes one kind, produces a well-formed result with the wrong
	// numbers in it — no error, no log line, just a total that is really a count.
	if got := out.Data["order_count"]; got != 3 {
		t.Errorf("order_count = %#v, want 3", got)
	}
	if got := out.Data["total_amount"]; got != float64(60) {
		t.Errorf("total_amount = %#v, want 60: a sum that comes back as the item "+
			"count means the operation kind was not read from the stored JSON", got)
	}
	if got := out.Data["average_amount"]; got != float64(20) {
		t.Errorf("average_amount = %#v, want 20", got)
	}
}

func TestAggregateRejectsAnUnknownKindFromStoredJSON(t *testing.T) {
	h, ok := registry.Lookup("xflow.transform.aggregate")
	if !ok {
		t.Fatal("aggregate handler not registered")
	}
	// The negative control for the test above: it pins that the kind read out of
	// the JSON is the one that selects the operation. Without it, a parser that
	// silently substitutes a valid kind for every entry satisfies the positive
	// assertions whenever the substituted kind happens to be the expected one.
	raw := `{"items":"items","operations":[{"kind":"no-such-kind","as":"x"}]}`
	var params map[string]any
	if err := json.Unmarshal([]byte(raw), &params); err != nil {
		t.Fatal(err)
	}
	_, err := h.Execute(context.Background(), &types.Input{
		Params: params,
		Data:   map[string]any{"items": []any{map[string]any{"amount": 1}}},
	})
	if err == nil {
		t.Fatal("Execute() error = nil for an unsupported operation kind: the kind " +
			"in the stored JSON is not what selects the operation, so a typo in a " +
			"workflow definition produces a silently wrong number instead of a failure")
	}
}

func TestSortParsesFieldsInTheirStoredJSONShape(t *testing.T) {
	b := node.Sort("items", node.SortDesc("score"))

	params := jsonParams(t, b)
	assertJSONArrayOfObjects(t, params, "fields")

	h, ok := registry.Lookup("xflow.transform.sort")
	if !ok {
		t.Fatal("sort handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: params,
		Data: map[string]any{
			"items": []any{
				map[string]any{"score": 1},
				map[string]any{"score": 3},
				map[string]any{"score": 2},
			},
		},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	items, ok := out.Data["items"].([]any)
	if !ok || len(items) != 3 {
		t.Fatalf("out items = %#v, want 3 elements", out.Data["items"])
	}
	// Descending was requested. An ascending result is a fully valid sort, which
	// is why it needs asserting: nothing else about the output distinguishes
	// "desc was dropped" from "desc was honoured".
	want := []int{3, 2, 1}
	for i, w := range want {
		got := items[i].(map[string]any)["score"]
		if got != w {
			t.Fatalf("items[%d].score = %#v, want %v (full order %#v): SortDesc was "+
				"requested but the stored JSON's desc flag did not reach the "+
				"comparator, so every descending sort in production runs ascending",
				i, got, w, items)
		}
	}
}

func TestSortHonoursSecondaryFieldsFromStoredJSON(t *testing.T) {
	// Multi-field sorting has no coverage anywhere in this package: the only
	// other sort test declares a single field, so the tie-break loop can stop
	// after the first comparison and nothing notices.
	b := node.Sort("items", node.SortAsc("dept"), node.SortDesc("score"))

	params := jsonParams(t, b)
	assertJSONArrayOfObjects(t, params, "fields")

	h, ok := registry.Lookup("xflow.transform.sort")
	if !ok {
		t.Fatal("sort handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: params,
		Data: map[string]any{
			// Every dept value collides, so the first field is a total tie and the
			// result is decided entirely by the second. Input order is deliberately
			// neither the answer nor its reverse, so a comparator that gives up on
			// the tie and leaves the stable order alone is distinguishable from one
			// that reverses.
			"items": []any{
				map[string]any{"dept": "eng", "score": 2},
				map[string]any{"dept": "eng", "score": 3},
				map[string]any{"dept": "eng", "score": 1},
			},
		},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	items := out.Data["items"].([]any)
	want := []int{3, 2, 1}
	for i, w := range want {
		got := items[i].(map[string]any)["score"]
		if got != w {
			t.Fatalf("items[%d].score = %#v, want %v (full order %#v): the secondary "+
				"sort field was ignored — a comparator that returns a verdict on the "+
				"first tie leaves the remaining fields dead", i, got, w, items)
		}
	}
}

func TestRenameParsesMappingInItsStoredJSONShape(t *testing.T) {
	b := node.Rename(map[string]string{"old_name": "new_name"})

	params := jsonParams(t, b)
	if _, ok := params["mapping"].(map[string]any); !ok {
		t.Fatalf("params[\"mapping\"] after a JSON round trip is %T, want "+
			"map[string]any: this test exists to cover that branch", params["mapping"])
	}

	h, ok := registry.Lookup("xflow.transform.rename")
	if !ok {
		t.Fatal("rename handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: params,
		Data:   map[string]any{"old_name": "value", "untouched": 7},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v: a mapping that parses to an empty map is "+
			"reported as \"mapping parameter is required\", which is what every "+
			"rename node in production would say if this branch dropped its "+
			"contents", err)
	}
	if got := out.Data["new_name"]; got != "value" {
		t.Errorf("out.Data[\"new_name\"] = %#v, want %q", got, "value")
	}
	if _, still := out.Data["old_name"]; still {
		t.Errorf("out.Data still has old_name: %#v", out.Data)
	}
	if got := out.Data["untouched"]; got != 7 {
		t.Errorf("out.Data[\"untouched\"] = %#v, want 7", got)
	}
}

func TestSetParsesExpressionsInTheirStoredJSONShape(t *testing.T) {
	// set's "expressions" is exempt from boundary evaluation
	// (engine/graph/evaluable_params.go), so the handler receives it exactly as
	// the reloaded graph holds it — map[string]any — and never the
	// map[string]string every existing set test passes in.
	b := node.Set(nil).SetExpr(map[string]string{"doubled": "n * 2"})

	params := jsonParams(t, b)
	if _, ok := params["expressions"].(map[string]any); !ok {
		t.Fatalf("params[\"expressions\"] after a JSON round trip is %T, want "+
			"map[string]any", params["expressions"])
	}

	h, ok := registry.Lookup("xflow.transform.set")
	if !ok {
		t.Fatal("set handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: params,
		Data:   map[string]any{"n": 21},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	// A dropped expressions map is the quiet failure here: set reports no error
	// for an empty map, it just produces none of the computed fields.
	got, present := out.Data["doubled"]
	if !present {
		t.Fatalf("out.Data has no \"doubled\": %#v — the expressions map was "+
			"dropped and set reported success anyway, so every computed field in "+
			"production disappears silently", out.Data)
	}
	if got != float64(42) && got != 42 {
		t.Errorf("out.Data[\"doubled\"] = %#v, want 42", got)
	}
}
