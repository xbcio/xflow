package runner

import (
	"context"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution/subgraph"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// groupExecBatchDeadline bounds one trigger-group batch's local execution.
// Chosen well above the per-partition flush cadence (seconds, not the
// sub-second per-message emit path) since a batch may run several member
// nodes' worth of real I/O. Revisit once real batch timing data exists (see
// spec 2026-08-07 §7.3 for the analogous per-record timeout rationale).
const groupExecBatchDeadline = 30 * time.Second

// groupExecTriggerRuntime is the Runtime a trigger-group activation installs
// on TriggerActivateInput (see TriggerActivationHandler.activateGroup). It
// embeds *protocol.HTTPEntrySeedRuntime for the entry-seed admission round trip
// (SeedExecutionFromEntry) and inherits its fail-closed TriggerRuntime stubs
// (Emit/Dedup/TryLock/State) unchanged — this runtime, like
// HTTPEntrySeedRuntime alone, only supports the entry-seed admission path.
//
// What it ADDS is ExecuteGroup: running the group's real member nodes locally
// via GroupRuntime and returning the REAL boundary exits, so a Kafka trigger's
// batch flush (node/trigger/kafka/aggregate.go) can admit actual member
// execution results instead of synthesizing exits from the raw batch (spec
// 2026-08-07 §3.3-§3.4).
type groupExecTriggerRuntime struct {
	*protocol.HTTPEntrySeedRuntime
	runtime     *GroupRuntime
	pkg         *graph.SubgraphPackage
	packageHash string
}

var _ types.EntrySeedRuntime = (*groupExecTriggerRuntime)(nil)
var _ types.GroupExecRuntime = (*groupExecTriggerRuntime)(nil)
var _ types.TriggerRuntime = (*groupExecTriggerRuntime)(nil)

// ExecuteGroup runs g.pkg with input as the entry node's seed data and
// converts the resulting engine.GroupResult to the caller-facing
// types.GroupExecResult. A non-nil error means the group could not be
// executed at all (e.g. package compile/validation failure via PackageCache);
// GroupExecResult.Outcome carries the group's own success/failed/timeout/
// canceled verdict once it did run.
func (g *groupExecTriggerRuntime) ExecuteGroup(ctx context.Context, input map[string]any) (types.GroupExecResult, error) {
	res, err := g.runtime.ExecuteRequest(ctx, subgraph.Request{
		Package:         g.pkg,
		PackageHash:     g.packageHash,
		Input:           &types.Input{Data: input},
		Deadline:        time.Now().Add(groupExecBatchDeadline),
		SuspendDisabled: true,
	})
	if err != nil {
		return types.GroupExecResult{}, err
	}
	exits := make([]types.BoundaryExit, len(res.Exits))
	for i, ex := range res.Exits {
		exits[i] = types.BoundaryExit{NodeName: ex.NodeName, Port: ex.Port, Data: ex.Data}
	}
	return types.GroupExecResult{
		Outcome: string(res.Outcome),
		Exits:   exits,
		Error:   res.Error,
	}, nil
}
