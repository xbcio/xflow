package runner

import (
	"context"
	"errors"
	"fmt"
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
		return types.GroupExecResult{}, classifyGroupExecError(err)
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

// classifyGroupExecError marks the deterministic error shapes ExecuteSubgraph
// can return with types.ErrPermanent, so the Kafka batch path can tell a broken
// package apart from a blip.
//
// The classification has to happen HERE rather than at the consumer.
// node/trigger/kafka does not import execution/subgraph and so cannot name
// these types at all; types.ErrPermanent is the vocabulary both layers already
// share, and is the same property GroupExecResult.Deterministic reports for the
// case where the group DID run.
//
// Why an allowlist and not a blanket "every error out of ExecuteSubgraph is
// permanent": that blanket happens to be true today — Executor.Execute has
// exactly one error return, the package cache's Resolve
// (execution/subgraph/subgraph.go:145-148), and each shape Resolve produces is
// a property of the package itself, so redelivering the batch re-runs the
// identical package and fails identically. But it would become a lie the first
// time Execute grows a second error return, and nothing would say so. An
// unrecognised error keeps the transient reading, which is the one whose only
// cost is a retry.
func classifyGroupExecError(err error) error {
	var validation *subgraph.PackageValidationError
	if errors.Is(err, subgraph.ErrPackageMissing) || errors.As(err, &validation) {
		return fmt.Errorf("%w: %w", types.ErrPermanent, err)
	}
	return err
}
