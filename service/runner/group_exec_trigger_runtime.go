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
// converts the resulting subgraph.Result to the caller-facing
// types.GroupExecResult. A non-nil error means the group could not be
// executed at all (e.g. package compile/validation failure via PackageCache);
// GroupExecResult.Outcome carries the group's own success/failed/timeout/
// canceled verdict once it did run.
func (g *groupExecTriggerRuntime) ExecuteGroup(ctx context.Context, input map[string]any) (types.GroupExecResult, error) {
	// ExecuteSubgraph, not ExecuteRequest: the latter maps onto
	// engine.GroupResult, the control plane's wire shape, which carries no
	// failure classification (protocol.GroupResultWire has no such field). This
	// path never crosses the wire — the group runs in this process, for this
	// Kafka batch — so it reads the executor's own result and keeps the
	// classification the failing member set.
	res, err := g.runtime.ExecuteSubgraph(ctx, subgraph.Request{
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
		// "Deterministic" here and "Permanent" upstream name the same property
		// from the two ends: the producer says the failure will not change on a
		// retry, the consumer reads that as "redelivering this batch is
		// pointless". The flag is set by the member node that failed, so it
		// survives whatever text that member's error happens to have.
		//
		// A timeout or a cancel is environmental, so it is never deterministic
		// and subgraph.Result never marks it — no filtering is needed here.
		Deterministic: res.Permanent,
	}, nil
}
