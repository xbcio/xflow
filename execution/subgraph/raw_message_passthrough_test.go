package subgraph

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"
)

// This file asserts one property of the group path: a value placed in a group's
// input reaches a map body member as the SAME Go value, not as whatever
// encoding/json would rebuild it into.
//
// The Kafka trigger's ValueAsJSON depends on exactly this. It puts the message
// payload into the item as a json.RawMessage so that no one escapes and
// un-escapes it, and that is only sound while nothing between the trigger and
// the body marshals the item and reads it back -- a round trip would turn the
// RawMessage into a map[string]any, which is a different type for every
// downstream expression and costs more than the escaping it replaced.
//
// A benchmark cannot check this. Handing a guest spliced bytes and measuring it
// proves the guest is faster given those bytes; it says nothing about whether
// the framework delivers them. This does.

// rawItemsFanout is the xflow.map node's handler, emitting whatever items it was
// given rather than a fixed list, so the caller controls the values under test.
type rawItemsFanout struct{}

func (rawItemsFanout) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "xflow.map"}
}

func (rawItemsFanout) Execute(_ context.Context, in *types.Input) (*types.Output, error) {
	items, _ := in.Data["items"].([]any)
	batches := make([][]any, 0, len(items))
	for _, it := range items {
		batches = append(batches, []any{it})
	}
	return &types.Output{Data: map[string]any{
		"items":       items,
		"batches":     batches,
		"batch_size":  1,
		"total":       len(items),
		"batch_count": len(batches),
	}}, nil
}

// rawTypeProbe records the concrete type of the value under $item's "value" key.
type rawTypeProbe struct {
	mu    sync.Mutex
	kinds []string
}

func (h *rawTypeProbe) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.rawprobe"}
}

func (h *rawTypeProbe) Execute(_ context.Context, in *types.Input) (*types.Output, error) {
	item, _ := in.Data["$item"].(map[string]any)
	kind := "absent"
	switch v := item["value"].(type) {
	case json.RawMessage:
		kind = "raw:" + string(v)
	case string:
		kind = "string"
	case map[string]any:
		kind = "object"
	default:
		if v != nil {
			kind = "other"
		}
	}
	h.mu.Lock()
	h.kinds = append(h.kinds, kind)
	h.mu.Unlock()
	return &types.Output{Data: map[string]any{"kind": kind}}, nil
}

func (h *rawTypeProbe) seen() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.kinds...)
}

func buildRawProbePackage() *graph.SubgraphPackage {
	return &graph.SubgraphPackage{
		Version:   1,
		GroupName: "grp",
		EntryNode: "m",
		Def: &types.WorkflowDef{
			Name: "grp",
			Nodes: []types.NodeDef{
				{Name: "m", Type: "xflow.map", Version: 1, Parameters: map[string]any{
					"items": "$input.items",
					"body": map[string]any{
						"type": "xflow.subgraph",
						"parameters": map[string]any{
							"nodes": []any{
								map[string]any{"name": "probe", "type": "test.rawprobe"},
							},
						},
					},
				}},
				{Name: "__collector_m_main", Type: graph.NodeTypeGroupExit, Version: 1},
			},
			Connections: types.Connections{
				"m": {"main": types.PortConnections{Targets: []types.Connection{{Node: "__collector_m_main"}}}},
			},
		},
		Exits: []graph.SubgraphPackageExit{
			{CollectorNode: "__collector_m_main", SrcNode: "m", Port: "main"},
		},
		Requirements: []graph.Requirement{
			{NodeType: "xflow.map", NodeVersion: 1},
		},
	}
}

// TestGroupMapBody_ReceivesRawMessageUnchanged is the wiring assertion behind
// the Kafka trigger's ValueAsJSON.
//
// Both arms are checked. The spliced arm must arrive as json.RawMessage holding
// the original bytes -- "some non-string type" is not enough, because a
// map[string]any also fails the string check while being exactly the degradation
// this rules out. The string arm must still arrive as a string, so a framework
// change that started re-encoding everything could not pass by making both arms
// agree on the wrong answer.
func TestGroupMapBody_ReceivesRawMessageUnchanged(t *testing.T) {
	const record = `{"request":{"method":"GET"},"response":{"status":200}}`

	reg := execution.NewRegistry()
	reg.RegisterGlobal("xflow.map", rawItemsFanout{})
	probe := &rawTypeProbe{}
	reg.RegisterGlobal("test.rawprobe", probe)

	ex := NewExecutor(reg, NewPackageCache(PackageCacheConfig{
		MaxEntries: 4, MaxPackageBytes: 1 << 20,
	}), func() Backend { return local.New(local.WithRegistry(reg), local.WithConcurrency(1)) },
		WithMapConcurrencyLimiter(NewMapConcurrencyLimiter(1, 1)))

	pkg := buildRawProbePackage()
	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		t.Fatalf("compute package hash: %v", err)
	}

	items := []any{
		map[string]any{"topic": "t", "value": json.RawMessage(record)},
		map[string]any{"topic": "t", "value": record},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	res, err := ex.Execute(ctx, Request{
		Package:     pkg,
		PackageHash: hash,
		Input:       &types.Input{Data: map[string]any{"items": items}},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.Outcome != OutcomeSuccess {
		t.Fatalf("outcome = %v (error: %s), want success", res.Outcome, res.Error)
	}

	kinds := probe.seen()
	if len(kinds) != 2 {
		t.Fatalf("body ran %d time(s) (%v), want 2", len(kinds), kinds)
	}
	var sawRaw, sawString bool
	for _, k := range kinds {
		switch k {
		case "raw:" + record:
			sawRaw = true
		case "string":
			sawString = true
		}
	}
	if !sawRaw {
		t.Fatalf("the spliced item did not reach the map body as an intact json.RawMessage (saw %v); "+
			"something between the group input and the body is round-tripping items through "+
			"encoding/json, which makes the Kafka trigger's ValueAsJSON both slower and a "+
			"type change for every downstream expression", kinds)
	}
	if !sawString {
		t.Fatalf("the string item did not reach the map body as a string (saw %v); the arms "+
			"agree, so this test can no longer tell a preserved value from a re-encoded one", kinds)
	}
}
