package control

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// LeaseTooLargeErrorCode is the permanent-error code a task fails with when
// its lease encodes past the runner transport's message limit, so it could
// never be delivered to a runner.
const LeaseTooLargeErrorCode = "dispatch.lease_too_large"

// failOversizeLease answers a poll whose lease encodes to size bytes, past the
// MaxRegisterRunnerBodyBytes limit of the transport named by limitName (for
// example "runner gRPC message limit"). The lease is already finalized in the
// directory, so letting the transport refuse it would not lose it: it would be
// redelivered, fail the same way, and loop (see failUndeliverableLease). The
// lease is failed permanently instead and the returned no-task answer is what
// the transport sends, so one oversize task does not also end the runner's
// session.
func (c *Core) failOversizeLease(ctx context.Context, req protocol.PollTaskRequest, resp protocol.PollTaskResponse, size int, limitName string) protocol.PollTaskResponse {
	lease := resp.Lease
	cause := types.NewPermanentError(LeaseTooLargeErrorCode, fmt.Sprintf(
		"task lease encodes to %d bytes, above the %d-byte %s", size, MaxRegisterRunnerBodyBytes, limitName))
	logArgs := []any{
		"ns", string(lease.Namespace),
		"exec", string(lease.Task.ExecutionID),
		"node", lease.Task.NodeName,
		"node_idx", lease.Task.NodeIdx,
		"attempt", lease.Attempt,
		"lease", string(lease.LeaseID),
		"runner", req.RunnerID,
		"bytes", size,
		"limit", MaxRegisterRunnerBodyBytes,
	}
	if c.logger != nil {
		c.logger.Error("task lease exceeds "+limitName+"; failing task", logArgs...)
	}
	if err := c.failUndeliverableLease(ctx, req.RunnerID, req.SessionID, lease, cause); err != nil && c.logger != nil {
		// The lease stays finalized, so the next replay or reclaim comes back
		// through this branch and retries the failure.
		c.logger.Error("fail oversize task lease", append(logArgs, "err", err)...)
	}
	return protocol.PollTaskResponse{Wait: c.pollWait, Control: resp.Control}
}

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
