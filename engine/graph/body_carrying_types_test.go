package graph

import (
	"testing"

	"github.com/xbcio/xflow/types"
)

// bodyCarryingNodeTypes 与 bannedBodyMemberTypes 之间的派生关系必须成立：
// 能带 body 的类型都是 fan-out 节点，都不许出现在别人的 body 里。
//
// 这条钉的是 DERIVATION 而不是当下的成员表。把 bannedBodyMemberTypes 写回字面量
// 常量（看起来更直白）会让「新增一种带 body 的节点类型」这个一行改动，静默地
// 把它变成可嵌套的——递归 fan-out 让子执行树无界，正是 v1 明确不做的事。
// 那种回退不会有任何编译错误，只有这条测试会红。
func TestEveryBodyCarryingTypeIsBannedFromBodies(t *testing.T) {
	if len(bodyCarryingNodeTypes) == 0 {
		t.Fatal("bodyCarryingNodeTypes is empty: no node type can carry a body at all, " +
			"which would make projectNodeBodies dead code")
	}
	for nodeType := range bodyCarryingNodeTypes {
		if !bannedBodyMemberTypes[nodeType] {
			t.Errorf("node type %q can carry a body but is not in bannedBodyMemberTypes: "+
				"it may therefore be nested inside another node's body, and the "+
				"sub-execution tree the durable layer and package hash are sized for "+
				"becomes unbounded", nodeType)
		}
	}
}

// xflow.http 也声明了一个叫 "body" 的参数（node/internal/action/http.go），
// 但那是 HTTP 请求体，不是子图。编译器据以判断「要不要投影」的必须是显式类型集，
// 不能嗅探参数名——嗅参数名会把每个 HTTP 节点的请求体送进子图编译。
//
// 这里给的 body 是一个合法的 HTTP 请求体、同时是一个非法的子图（没有 nodes、
// 也不是 xflow.subgraph）。若判据退化成嗅参数名，Compile 会在 validateNodeBody
// 里报错；判据正确时它干净通过，且节点不带投影出来的包。
func TestHTTPBodyParameterIsNotProjectedAsASubgraph(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "wf",
		Nodes: []types.NodeDef{
			{Name: "call", Type: "xflow.http", Parameters: map[string]any{
				"method": "POST",
				"url":    "https://example.invalid/v1/things",
				"body":   map[string]any{"name": "thing", "count": 3},
			}},
		},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("an xflow.http node's request body must not be compiled as a sub-graph, "+
			"but compilation failed: %v", err)
	}
	idx, ok := g.NodeIndex("call")
	if !ok {
		t.Fatal("compiled graph has no node \"call\"")
	}
	if body := g.BodyAt(idx); body != nil {
		t.Fatalf("the http node came out with a projected body package (%+v): its "+
			"\"body\" parameter is a request payload and must stay an opaque parameter", body)
	}
	// 参数本身必须原样留在节点上——投影会把 body 从 Parameters 里搬走，
	// 若这里丢了，HTTP 节点运行期就发不出请求体了。
	params := g.NodeAt(idx).Parameters
	if _, present := params["body"]; !present {
		t.Fatal("the http node's \"body\" parameter was consumed during compilation: " +
			"the request payload must reach the handler intact")
	}
}
