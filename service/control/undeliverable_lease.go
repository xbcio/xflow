package control

import (
	"context"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

// LeaseTooLargeErrorCode is the permanent-error code a task fails with when
// its lease encodes past the runner transport's message limit, so it could
// never be delivered to a runner.
const LeaseTooLargeErrorCode = "dispatch.lease_too_large"

// failUndeliverableLease commits cause as the result of a lease pollTask has
// already finalized but the transport cannot deliver, then releases the
// runner capacity the lease holds, mirroring reportResult.
//
// Leaving such a lease alone does not lose it; it loops. The Redis directory
// replays a finalized lease on every poll of the owning session that does not
// list it as active, so each poll fails the same way. Once the lease expires
// the sweeper re-enqueues the task and the next BuildTaskLease produces the
// same oversize lease, and nothing on that path consults MaxAttempts. cause
// must therefore be permanent (types.NewPermanentError): retry declines it and
// the node's OnError decides the workflow outcome, as for any node failure.
func (c *Core) failUndeliverableLease(ctx context.Context, runnerID, sessionID string, lease *engine.TaskLease, cause error) error {
	if tid := lease.Namespace; tid != "" {
		ctx = namespace.WithNamespace(ctx, tid)
	}
	var (
		outcome engine.CommitOutcome
		err     error
	)
	if isGroupTask(&lease.Task) {
		res := engine.GroupResult{Outcome: engine.GroupOutcomeFailed, Error: cause.Error(), Attempt: lease.Attempt}
		if p := lease.GroupPayload; p != nil {
			res.ProtocolVersion = p.ProtocolVersion
			res.GroupExecID = p.GroupExecID
		}
		outcome, err = c.commitGroupResult(ctx, lease, res)
	} else {
		outcome, err = c.engine.CommitTaskResultWithOutcome(ctx, lease, engine.TaskResult{Error: cause})
	}
	if outcome.ReleasesLeasedCapacity() {
		removeSeen := outcome == engine.CommitOutcomeAccepted || outcome == engine.CommitOutcomeDuplicateTerminal || outcome == engine.CommitOutcomeExecutionInactive
		if releaseErr := c.runners.ReleaseLeased(ctx, ReleaseLeasedRequest{
			RunnerID:     runnerID,
			SessionID:    sessionID,
			AssignmentID: BuildAssignmentID(&lease.Task),
			LeaseID:      lease.LeaseID,
			LeaseToken:   lease.LeaseToken,
			RemoveSeen:   removeSeen,
		}); releaseErr != nil && err == nil {
			err = releaseErr
		}
	}
	return err
}
