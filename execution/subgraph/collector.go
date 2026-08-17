package subgraph

import (
	"context"
	"strings"
	"sync"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"
)

// exitMapping holds the source identity for a collector node.
type exitMapping struct {
	srcNode string
	port    string
}

// Collector captures boundary output data from xflow.group_exit nodes
// during an inner sub-graph execution.
type Collector struct {
	mu       sync.Mutex
	mappings map[string]exitMapping
	captured []graph.SubgraphExitResult
}

// NewCollector creates a collector that knows which collector nodes to expect
// based on the sub-graph package's exit definitions.
func NewCollector(pkg *graph.SubgraphPackage) *Collector {
	m := make(map[string]exitMapping, len(pkg.Exits))
	for _, exit := range pkg.Exits {
		m[exit.CollectorNode] = exitMapping{
			srcNode: exit.SrcNode,
			port:    exit.Port,
		}
	}
	return &Collector{mappings: m}
}

// Register installs per-execution handlers for each collector node so the
// inner engine dispatches to this collector when the package's exit nodes
// fire.
func Register(reg *execution.Registry, id types.ExecutionID, c *Collector) {
	for nodeName := range c.mappings {
		reg.RegisterExecutionHandler(id, nodeName, &exitHandler{
			collector: c,
			nodeName:  nodeName,
		})
	}
}

// Exits returns the collected boundary outputs after the inner execution
// completes. Safe to call after the inner engine has finished.
func (c *Collector) Exits() []graph.SubgraphExitResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]graph.SubgraphExitResult, len(c.captured))
	copy(out, c.captured)
	return out
}

// exitHandler implements types.ActionHandler for xflow.group_exit collector
// nodes.
type exitHandler struct {
	collector *Collector
	nodeName  string
}

func (h *exitHandler) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type: graph.NodeTypeGroupExit,
	}
}

func (h *exitHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	data := withoutExecutionRoots(input.Data)

	mapping, ok := h.collector.mappings[h.nodeName]
	if !ok {
		return &types.Output{Data: data}, nil
	}

	result := graph.SubgraphExitResult{
		NodeName: mapping.srcNode,
		Port:     mapping.port,
		Data:     data,
	}

	h.collector.mu.Lock()
	h.collector.captured = append(h.collector.captured, result)
	h.collector.mu.Unlock()

	return &types.Output{Data: data}, nil
}

// withoutExecutionRoots strips the execution-scope expression roots from a
// boundary output.
//
// engine's applyExecutionScope merges the scope into EVERY node's Input.Data,
// which is what makes $item/$index/$items reachable from any body member rather
// than only from its entry. A collector node is a member too, so its Data
// arrives carrying them — and it echoes its Data out as both the exit result and
// its own stored output. That echo is not the body's product, and for $items it
// is quadratic: $items is the map node's WHOLE items array, so every item's
// result carries a copy of every item. Measured on 2.7 KB items, one batch's
// collected results totalled 1.2 MB at 20 items, 27.7 MB at 100, and 681 MB at
// 500 — the last of which lands in one Redis string, and Redis is
// single-threaded, so writing it stalls every other command on the instance
// (lease lookups, claim finalization) and the pipeline stops draining.
//
// Matched by the "$" prefix rather than an enumerated list. The prefix is
// reserved — a node output cannot introduce one through the DSL (see
// engine/input.go's applyExecutionScope) — so nothing a handler legitimately
// produces is at risk, and a root added later is excluded without a second
// table to keep in sync.
//
// A nil or root-free map is returned as-is: the common case is a body whose
// members produce no "$" key at all, and the map it already holds is not shared
// with the engine (buildInput's cloneMap made it per-node).
func withoutExecutionRoots(data map[string]any) map[string]any {
	rooted := false
	for k := range data {
		if strings.HasPrefix(k, "$") {
			rooted = true
			break
		}
	}
	if !rooted {
		return data
	}
	out := make(map[string]any, len(data))
	for k, v := range data {
		if strings.HasPrefix(k, "$") {
			continue
		}
		out[k] = v
	}
	return out
}
