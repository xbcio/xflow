package groupnode

import (
	"sync"

	"github.com/xbcio/xflow/engine"
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
// during an inner group execution.
type Collector struct {
	mu       sync.Mutex
	mappings map[string]exitMapping
	captured []engine.GroupExitResult
}

// NewCollector creates a collector that knows which collector nodes to expect
// based on the group package's exit definitions.
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
// inner engine dispatches to this collector when group exit nodes fire.
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
func (c *Collector) Exits() []engine.GroupExitResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]engine.GroupExitResult, len(c.captured))
	copy(out, c.captured)
	return out
}
