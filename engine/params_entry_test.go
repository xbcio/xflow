package engine

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

func compileParamsGraph(t *testing.T, params map[string]types.ParamDef) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:   "params",
		Params: params,
		Nodes:  []types.NodeDef{{Name: "trigger", Type: "xflow.trigger.kafka", Kind: types.NodeKindTrigger}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

var paramsSchema = map[string]types.ParamDef{
	"amount": {Type: "number", Required: true, Validation: map[string]any{"min": 0.01, "max": 1000000, "pattern": "ignored"}},
	"region": {Type: "string", Default: "us"},
	"flag":   {Type: "boolean"},
	"meta":   {Type: "object"},
	"tags":   {Type: "array"},
}

// submitBoth runs the same params through Submit and Invoke and returns the
// persisted params of each (nil on error) plus the errors.
func submitBoth(t *testing.T, g *graph.Graph, params map[string]any) ([]map[string]any, []error) {
	t.Helper()
	ctx := context.Background()
	var outs []map[string]any
	var errs []error
	for _, invoke := range []bool{false, true} {
		state := newFakeState()
		eng := New(state, &toggleOutboxQueue{})
		var id types.ExecutionID
		var err error
		if invoke {
			id, err = eng.Invoke(ctx, g, "trigger", params)
		} else {
			id, err = eng.Submit(ctx, g, params)
		}
		errs = append(errs, err)
		if err != nil {
			outs = append(outs, nil)
			continue
		}
		snap, gerr := state.GetExecution(ctx, id)
		if gerr != nil {
			t.Fatalf("GetExecution: %v", gerr)
		}
		outs = append(outs, snap.Params)
	}
	return outs, errs
}

func TestEntryParamsInjectsDefaultsAndKeepsExtras(t *testing.T) {
	g := compileParamsGraph(t, paramsSchema)
	in := map[string]any{"amount": 5, "extra": "x"}
	outs, errs := submitBoth(t, g, in)
	for i, err := range errs {
		if err != nil {
			t.Fatalf("path %d: %v", i, err)
		}
		want := map[string]any{"amount": 5, "extra": "x", "region": "us"}
		if !reflect.DeepEqual(outs[i], want) {
			t.Fatalf("path %d params = %v, want %v", i, outs[i], want)
		}
	}
	if _, ok := in["region"]; ok {
		t.Fatal("caller map mutated")
	}
}

func TestEntryParamsAcceptsNumericForms(t *testing.T) {
	g := compileParamsGraph(t, paramsSchema)
	for _, v := range []any{float64(2.5), int(3), int64(4), json.Number("7")} {
		if _, errs := submitBoth(t, g, map[string]any{"amount": v}); errs[0] != nil || errs[1] != nil {
			t.Fatalf("amount %T: %v", v, errs)
		}
	}
}

func TestEntryParamsRejections(t *testing.T) {
	g := compileParamsGraph(t, paramsSchema)
	cases := map[string]map[string]any{
		"missing required": {},
		"number type":      {"amount": "5"},
		"below min":        {"amount": 0},
		"above max":        {"amount": json.Number("1000001")},
		"string type":      {"amount": 1, "region": 3},
		"boolean type":     {"amount": 1, "flag": "true"},
		"object type":      {"amount": 1, "meta": []any{}},
		"array type":       {"amount": 1, "tags": map[string]any{}},
	}
	for name, params := range cases {
		_, errs := submitBoth(t, g, params)
		for i, err := range errs {
			if !errors.Is(err, graph.ErrInvalidParams) {
				t.Fatalf("%s path %d: err = %v, want ErrInvalidParams", name, i, err)
			}
		}
	}
	if _, errs := submitBoth(t, g, map[string]any{"amount": 1, "meta": map[string]any{}, "tags": []string{"a"}, "flag": true}); errs[0] != nil || errs[1] != nil {
		t.Fatalf("valid object/array/bool rejected: %v", errs)
	}
}

func TestEntryParamsNoSchemaUnchanged(t *testing.T) {
	g := compileParamsGraph(t, nil)
	outs, errs := submitBoth(t, g, map[string]any{"anything": "goes"})
	for i := range errs {
		if errs[i] != nil || !reflect.DeepEqual(outs[i], map[string]any{"anything": "goes"}) {
			t.Fatalf("path %d: %v %v", i, outs[i], errs[i])
		}
	}
}

func TestEntryParamsSchemaSurvivesSnapshot(t *testing.T) {
	raw, err := json.Marshal(compileParamsGraph(t, paramsSchema))
	if err != nil {
		t.Fatal(err)
	}
	g := &graph.Graph{}
	if err := json.Unmarshal(raw, g); err != nil {
		t.Fatal(err)
	}
	if _, errs := submitBoth(t, g, map[string]any{}); !errors.Is(errs[0], graph.ErrInvalidParams) {
		t.Fatalf("decoded graph lost schema: %v", errs[0])
	}
}
