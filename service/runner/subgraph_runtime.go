package runner

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"
)

// SubgraphRuntime adapts a batch lease to the runner's execution surface. It is
// the map counterpart of GroupRuntime: the control plane hands it one batch of
// a map expansion, it runs that batch, and the result commits through the
// expansion barrier rather than the ordinary node path.
//
// Body sub-graph execution is not wired yet — this preserves the pass-through
// shape ExecuteBatch has always had. What changed with this type is WHERE the
// batch runs: on the runner the map node's runnerSelector chose, instead of
// wherever the control plane happens to live.
type SubgraphRuntime struct {
	registry *execution.Registry
}

// NewSubgraphRuntime creates a runtime that will resolve body member handlers
// from the given registry once body execution lands.
func NewSubgraphRuntime(reg *execution.Registry) *SubgraphRuntime {
	return &SubgraphRuntime{registry: reg}
}

// Execute runs one batch and returns the result the control plane commits
// through CommitSubgraphResult.
func (r *SubgraphRuntime) Execute(_ context.Context, lease *engine.TaskLease) (engine.TaskResult, error) {
	if lease == nil || lease.SubgraphPayload == nil {
		return engine.TaskResult{}, fmt.Errorf("subgraph runtime: nil lease or payload")
	}
	items := lease.SubgraphPayload.Items
	return engine.TaskResult{Output: &types.Output{Data: map[string]any{
		"items": items,
		"count": len(items),
	}}}, nil
}
