//go:build integration

package integration

import (
	"context"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

const (
	blobE2EItems     = 30 // items the map node fans out
	blobE2EPadLength = 200
)

// blobE2EMarker is the text every item carries. The workflow builds it at run
// time (repeat), so it appears nowhere in the stored graph, only in the items
// themselves; any Redis value that holds it holds a copy of the items.
var blobE2EMarker = strings.Repeat("Z", blobE2EPadLength)

// blobE2EWorkflow is start -> fan (map) -> done. The items are produced by the
// map node's own expression, so no input or upstream output holds them: the only
// places the array can live are the batch tasks and the blob. The body returns
// the item's id and its square and leaves the pad out, so the node outputs do
// not carry the marker either.
func blobE2EWorkflow(batchSize int) *types.WorkflowDef {
	link := func(to string) types.PortConnections {
		return types.PortConnections{Targets: []types.Connection{{Node: to, Input: "main"}}}
	}
	return &types.WorkflowDef{
		Name: "blob-e2e",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "fan", Type: "xflow.map", Parameters: map[string]any{
				"items":      `map(1..30, {{"id": #, "pad": repeat("Z", 200)}})`,
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
			"start": {"main": link("fan")},
			"fan":   {"main": link("done")},
		},
	}
}

// blobE2EHolders returns the keys of the run's namespace, and of its asynq
// queues, whose value contains marker. Values are read by type instead of
// through DUMP: DUMP may compress a repetitive string, which would hide the
// marker from a substring search.
func blobE2EHolders(ctx context.Context, rdb *redis.Client, ns namespace.Namespace, marker string) ([]string, error) {
	var holders []string
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, "*"+string(ns)+"*", 500).Result()
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			text, err := blobE2EKeyText(ctx, rdb, key)
			if err != nil {
				return nil, err
			}
			if strings.Contains(text, marker) {
				holders = append(holders, key)
			}
		}
		if cursor = next; cursor == 0 {
			return holders, nil
		}
	}
}
