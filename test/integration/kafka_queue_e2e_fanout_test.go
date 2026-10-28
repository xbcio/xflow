//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// e2eFanoutDef is start -> fan (xflow.map, body form) -> done. The body is a
// one-node subgraph that squares $item.id, so each result can be traced back to
// the item it came from.
func e2eFanoutDef(batchSize int) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "kafka-e2e-fanout",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "fan", Type: "xflow.map", Parameters: map[string]any{
				"items":      "$input.items",
				"batch_size": batchSize,
				"body": map[string]any{
					"type": "xflow.subgraph",
					"parameters": map[string]any{
						"nodes": []any{map[string]any{
							"name": "sq", "type": "xflow.function",
							"parameters": map[string]any{"code": `{"id": $item.id, "sq": $item.id * $item.id}`},
						}},
						"connections": map[string]any{},
					},
				},
			}},
			{Name: "done", Type: "xflow.end"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "fan", Input: "main"}}}},
			"fan":   {"main": {Targets: []types.Connection{{Node: "done", Input: "main"}}}},
		},
	}
}

// TestKafkaQueueE2E_FanOut maps 24 items in batches of 4. The six batch tasks
// must travel through the batch topic (not the default one), and the map node's
// flattened results must keep one slot per item, in input order.
func TestKafkaQueueE2E_FanOut(t *testing.T) {
	const items, batchSize = 24, 4
	env := newKafkaE2EEnv(t)
	node := startKafkaE2ENode(t, env, kafkaE2EOpts{bodies: true})
	topics := env.topics()
	batchBefore := topicRecordCount(t, env, topics[1])

	input := make([]any, items)
	for i := range input {
		input[i] = map[string]any{"id": i}
	}
	g, err := graph.Compile(e2eFanoutDef(batchSize))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	ctx := env.ctx(t, 60*time.Second)
	id, err := node.eng.Submit(ctx, g, map[string]any{"items": input})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	res := waitForCompletion(ctx, t, node.b.State(), id, "fan")
	if res.Status != types.ExecutionStatusSuccess {
		t.Fatalf("status = %s, want success; fan output: %v", res.Status, res.Output["fan"])
	}

	out, ok := res.Output["fan"].(map[string]any)
	if !ok {
		t.Fatalf("fan output is %T (%v), want map", res.Output["fan"], res.Output["fan"])
	}
	results, _ := out["results"].([]any)
	if e2eInt(out["count"]) != items || len(results) != items {
		t.Fatalf("count=%v len(results)=%d, want %d; output: %v", out["count"], len(results), items, out)
	}
	for i, r := range results {
		m, ok := r.(map[string]any)
		if !ok || e2eInt(m["id"]) != i || e2eInt(m["sq"]) != i*i {
			t.Fatalf("results[%d] = %v, want id=%d sq=%d (input order)", i, r, i, i*i)
		}
	}

	// ceil(24/4) = 6 batch tasks, each one record on the batch topic.
	got := topicRecordCount(t, env, topics[1]) - batchBefore
	if want := int64((items + batchSize - 1) / batchSize); got != want {
		t.Fatalf("batch topic received %d records, want %d", got, want)
	}
	t.Logf("fan-out ok: %d items in %d batches through %s", items, got, topics[1])
}
