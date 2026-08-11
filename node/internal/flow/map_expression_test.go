package flow_test

import (
	"context"
	"strings"
	"testing"

	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// expression 形态必须就地求值出结果，而不是发一份 fan-out 描述符。
//
// 这条契约的承重之处在引擎侧：引擎判定「要不要扩展」看的是编译期是否投影出了
// body。expression 形态没有 body 可投影，于是引擎判定「不扩展」，把 handler 的
// 返回值当成这个节点的普通输出直接提交。所以 handler 在这条分支上返回什么，
// 下游读到的就是什么——它必须已经是最终结果，形状还必须与 body 形态一致
// （results/count），否则同一个 xflow.map 的两种写法会给下游两套契约。
func mapExprInput(t *testing.T, params map[string]any, data map[string]any) *types.Input {
	t.Helper()
	return &types.Input{Params: params, Data: data}
}

func TestMapExpression_EvaluatesInlineAndMatchesTheBodyResultShape(t *testing.T) {
	h, _ := registry.Lookup("xflow.map")
	out, err := h.Execute(context.Background(), mapExprInput(t,
		map[string]any{"items": "rows", "expression": "$item * 2"},
		map[string]any{"rows": []any{1, 2, 3}},
	))
	if err != nil {
		t.Fatalf("expression form must run: %v", err)
	}
	for _, marker := range []string{"_loop", "_split", "_map", "batches", "batch_count"} {
		if _, ok := out.Data[marker]; ok {
			t.Errorf("output carries %q: the expression form projects no body, so the engine "+
				"commits this map as an ordinary value — a fan-out descriptor here reaches "+
				"downstream verbatim instead of being expanded", marker)
		}
	}
	results, ok := out.Data["results"].([]any)
	if !ok {
		t.Fatalf(`output has no "results" array (got %#v): the body form promises `+
			`{results, count} and both forms must give downstream one contract`, out.Data)
	}
	if out.Data["count"] != 3 {
		t.Errorf("count = %v, want 3", out.Data["count"])
	}
	want := []any{2, 4, 6}
	for i := range want {
		if results[i] != want[i] {
			t.Errorf("results[%d] = %v, want %v", i, results[i], want[i])
		}
	}
}

// 三个逐项根必须与 body 形态注入的同名同义（execution/subgraph/map_body.go 的
// bodyItemInput）。带 "$" 前缀是刻意的：filter 节点用无前缀的 item/index 做它
// 自己的逐元素条件，写作 item 会与之混淆。
func TestMapExpression_InjectsTheSameThreePerItemRootsAsABody(t *testing.T) {
	h, _ := registry.Lookup("xflow.map")
	out, err := h.Execute(context.Background(), mapExprInput(t,
		map[string]any{"items": "rows", "expression": `string($index) + ":" + $item + "/" + string(len($items))`},
		map[string]any{"rows": []any{"a", "b"}},
	))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	results := out.Data["results"].([]any)
	for i, want := range []any{"0:a/2", "1:b/2"} {
		if results[i] != want {
			t.Errorf("results[%d] = %v, want %v — one of $item/$index/$items did not reach "+
				"the expression", i, results[i], want)
		}
	}
}

// $index 必须是在整个 items 数组里的位置。expression 形态不分批，但 batch_size
// 仍可能被写在参数里；它绝不能影响求值结果。
func TestMapExpression_IgnoresBatchSize(t *testing.T) {
	h, _ := registry.Lookup("xflow.map")
	out, err := h.Execute(context.Background(), mapExprInput(t,
		map[string]any{"items": "rows", "expression": "$index", "batch_size": 2},
		map[string]any{"rows": []any{"a", "b", "c", "d", "e"}},
	))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	results := out.Data["results"].([]any)
	for i := range results {
		if results[i] != i {
			t.Fatalf("results[%d] = %v, want %d — $index restarted per batch, but batch_size "+
				"is a durability policy that must not be observable from the expression",
				i, results[i], i)
		}
	}
}

// continue_on_error 的语义必须与 body 形态的 BatchResultForCommit 一致：
// 默认 false 时任何一项失败就让节点失败；true 且至少一项成功时节点成功，失败项
// 以 {_error, _index} 占位留在原位——正是这一点让「count 恒等于输入长度」成立，
// 也让下游 filter 能把失败与数据区分开。
func TestMapExpression_ContinueOnError(t *testing.T) {
	h, _ := registry.Lookup("xflow.map")
	// 第二项求值失败：字符串不能取 .id。
	params := map[string]any{"items": "rows", "expression": "$item.id"}
	data := map[string]any{"rows": []any{map[string]any{"id": 7}, "not-an-object"}}

	if _, err := h.Execute(context.Background(), mapExprInput(t, params, data)); err == nil {
		t.Fatal("a failed item must fail the node by default: continue_on_error defaults to " +
			"false in the body form, and a silent partial result is exactly the outcome that " +
			"setting exists to prevent")
	}

	params["continue_on_error"] = true
	out, err := h.Execute(context.Background(), mapExprInput(t, params, data))
	if err != nil {
		t.Fatalf("continue_on_error=true with one success must not fail the node: %v", err)
	}
	results := out.Data["results"].([]any)
	if len(results) != 2 || out.Data["count"] != 2 {
		t.Fatalf("results/count = %d/%v, want 2/2: a failed item occupies its slot rather "+
			"than being dropped", len(results), out.Data["count"])
	}
	failed, ok := results[1].(map[string]any)
	if !ok {
		t.Fatalf("results[1] = %#v, want an {_error, _index} placeholder", results[1])
	}
	if _, has := failed["_error"]; !has {
		t.Errorf("placeholder has no _error key: downstream cannot tell a failure from data")
	}
	if failed["_index"] != 1 {
		t.Errorf("placeholder _index = %v, want 1", failed["_index"])
	}
}

// 全部失败时两种设置都必须失败——这也是 BatchResultForCommit 的规则
// （continue_on_error 只在「至少一项成功」时放行）。
func TestMapExpression_AllItemsFailingFailsTheNodeEvenWithContinueOnError(t *testing.T) {
	h, _ := registry.Lookup("xflow.map")
	_, err := h.Execute(context.Background(), mapExprInput(t,
		map[string]any{"items": "rows", "expression": "$item.id", "continue_on_error": true},
		map[string]any{"rows": []any{"x", "y"}},
	))
	if err == nil {
		t.Fatal("every item failed yet the node succeeded: continue_on_error means " +
			"'tolerate partial failure', not 'never fail'")
	}
	if !strings.Contains(err.Error(), "xflow.map") {
		t.Errorf("error %q does not name the node type", err)
	}
}

// 两种形态互斥。同时写了 body 与 expression 时不能悄悄二选一跑一个——那会让作者
// 以为另一半也生效了。编译期已经拒绝这个形状，handler 侧再兜一次底，因为 handler
// 也被未经编译的调用方直接调用（本文件即是）。
func TestMapExpression_BodyAndExpressionTogetherIsRejected(t *testing.T) {
	h, _ := registry.Lookup("xflow.map")
	_, err := h.Execute(context.Background(), mapExprInput(t,
		map[string]any{
			"items":      "rows",
			"expression": "$item",
			"body":       map[string]any{"type": "xflow.subgraph"},
		},
		map[string]any{"rows": []any{1}},
	))
	if err == nil {
		t.Fatal("body and expression together were accepted: one of the two silently does " +
			"nothing and the author has no way to tell which")
	}
}
