package transform_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

func TestSet_AssignsLiteralsAndExpressions(t *testing.T) {
	b := node.Set(map[string]any{"status": "approved"}).
		SetExpr(map[string]string{"total": "price * quantity"})

	if got := b.NodeType(); got != "xflow.transform.set" {
		t.Fatalf("NodeType() = %q, want xflow.transform.set", got)
	}

	h, ok := registry.Lookup("xflow.transform.set")
	if !ok {
		t.Fatal("set handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"price": 12, "quantity": 3, "keep": true},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if out.Data["status"] != "approved" {
		t.Fatalf("status = %v, want approved", out.Data["status"])
	}
	if out.Data["total"] != 36 {
		t.Fatalf("total = %v, want 36", out.Data["total"])
	}
	if out.Data["keep"] != true {
		t.Fatalf("keep = %v, want true", out.Data["keep"])
	}
}

// TestSet_ExpressionsEvaluateInLexicographicOrder covers what a single-expression
// fixture cannot. The test above declares exactly one expression, so the
// evaluation order set.go:72-83 goes out of its way to fix is unobservable there
// — deleting the sort.Strings leaves it green.
//
// Order matters because each evaluated field is written back into the env
// (set.go:91), so a later expression can read an earlier one's result. Without
// the sort, Go's randomized map iteration decides which reads see which writes:
// the same workflow, same input, produces different output between runs. The
// values below are seeded on the input so an out-of-order run does NOT error —
// it silently computes from the stale seed, which is exactly what makes the
// defect hard to notice in production.
func TestSet_ExpressionsEvaluateInLexicographicOrder(t *testing.T) {
	b := node.Set(nil).SetExpr(map[string]string{
		"a": "1",
		"b": "a + 1",
		"c": "b + 1",
	})

	h, ok := registry.Lookup("xflow.transform.set")
	if !ok {
		t.Fatal("set handler not registered")
	}
	const runs = 40
	for i := range runs {
		out, err := h.Execute(context.Background(), &types.Input{
			Params: b.RawParams().(map[string]any),
			// Pre-existing zeros: an expression that runs before its dependency
			// reads these instead of failing.
			Data: map[string]any{"a": 0, "b": 0, "c": 0},
		})
		if err != nil {
			t.Fatalf("run %d: Execute() error = %v", i, err)
		}
		if out.Data["a"] != 1 || out.Data["b"] != 2 || out.Data["c"] != 3 {
			t.Fatalf("run %d: data = %#v, want map[a:1 b:2 c:3] — an expression "+
				"was evaluated before the field it reads, so it resolved against "+
				"the stale input value", i, out.Data)
		}
	}
}
