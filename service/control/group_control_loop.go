package control

import (
	"context"
	"errors"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// groupLeaseEngine is the optional interface for engines that support group
// lease lifecycle. The concrete *engine.Engine implements all four methods.
type groupLeaseEngine interface {
	BuildGroupLease(ctx context.Context, t *engine.Task) (*engine.TaskLease, *engine.GroupLeasePayload, error)
	RecoverGroupLease(ctx context.Context, execID types.ExecutionID, unitIdx int) (*engine.TaskLease, *engine.GroupLeasePayload, error)
	CommitGroupResult(ctx context.Context, lease *engine.TaskLease, res engine.GroupResult) (engine.CommitOutcome, error)
	RenewGroupLease(ctx context.Context, lease *engine.TaskLease, extend time.Duration) (bool, error)
}

// isGroupTask returns true when the task type is a group execution type.
func isGroupTask(t *engine.Task) bool {
	return t != nil && t.Type == engine.TaskTypeGroupExec
}

// dispatchGroupLease handles the BuildGroupLease + FinalizeClaim flow for group
// tasks, mirroring the node BuildTaskLease path but attaching GroupPayload.
func (c *Core) dispatchGroupLease(ctx context.Context, claim Claim) (protocol.PollTaskResponse, error) {
	ge, ok := c.engine.(groupLeaseEngine)
	if !ok {
		_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimRequeue)
		return protocol.PollTaskResponse{}, errors.New("engine does not support group leases")
	}

	lease, payload, err := ge.BuildGroupLease(ctx, &claim.Assignment.Task)
	switch {
	case err == nil:
		lease.GroupPayload = payload
		lease.Namespace = claim.Assignment.Namespace
		if err := c.runners.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
			_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimRequeue)
			return protocol.PollTaskResponse{}, normalizeRunnerError(err, c.logger, "poll")
		}
		return protocol.PollTaskResponse{Lease: lease}, nil

	case errors.Is(err, engine.ErrGroupLeaseAlreadyActive):
		recovered, recoverErr := c.recoverGroupLease(ctx, ge, &claim.Assignment.Task)
		if recoverErr == nil {
			recovered.Namespace = claim.Assignment.Namespace
			if finalizeErr := c.runners.FinalizeClaim(ctx, claim.ClaimID, recovered); finalizeErr != nil {
				_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimRequeue)
				return protocol.PollTaskResponse{}, normalizeRunnerError(finalizeErr, c.logger, "poll")
			}
			return protocol.PollTaskResponse{Lease: recovered}, nil
		}
		if errors.Is(recoverErr, engine.ErrExecutionInactive) {
			_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimDrop)
			return protocol.PollTaskResponse{}, nil
		}
		_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimRequeue)
		return protocol.PollTaskResponse{}, recoverErr

	case errors.Is(err, engine.ErrExecutionInactive):
		_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimDrop)
		return protocol.PollTaskResponse{}, nil

	default:
		_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimRequeue)
		return protocol.PollTaskResponse{}, err
	}
}

// recoverGroupLease reads the authoritative group lease from the backend and
// rebuilds the TaskLease + GroupLeasePayload without mutating state.
func (c *Core) recoverGroupLease(ctx context.Context, ge groupLeaseEngine, task *engine.Task) (*engine.TaskLease, error) {
	lease, payload, err := ge.RecoverGroupLease(ctx, task.ExecutionID, task.UnitIdx)
	if err != nil {
		return nil, err
	}
	lease.GroupPayload = payload
	return lease, nil
}

// replayGroupLease handles the durable replay path for group tasks when the
// directory returns a finalized lease (claim.Lease != nil) that has
// Input == nil and is a group task. This triggers recovery to rebuild the
// GroupPayload for the runner.
func (c *Core) replayGroupLease(ctx context.Context, lease *engine.TaskLease) (*engine.TaskLease, error) {
	ge, ok := c.engine.(groupLeaseEngine)
	if !ok {
		return lease, nil
	}
	recovered, payload, err := ge.RecoverGroupLease(ctx, lease.Task.ExecutionID, lease.Task.UnitIdx)
	if err != nil {
		return nil, err
	}
	// Preserve the finalized lease identity; attach the rebuilt payload.
	lease.GroupPayload = payload
	// If the lease fields are empty (old finalization), fill from recovered.
	if lease.LeaseToken == "" {
		lease.LeaseToken = recovered.LeaseToken
	}
	if lease.LeaseID == "" {
		lease.LeaseID = recovered.LeaseID
	}
	return lease, nil
}

// commitGroupResult delegates a group execution result to the engine's
// CommitGroupResult. It handles outcome→error mapping identically to
// CommitTaskResultWithOutcome for consistency with the report path.
func (c *Core) commitGroupResult(ctx context.Context, lease *engine.TaskLease, res engine.GroupResult) (engine.CommitOutcome, error) {
	ge, ok := c.engine.(groupLeaseEngine)
	if !ok {
		return "", errors.New("engine does not support group result commit")
	}
	return ge.CommitGroupResult(ctx, lease, res)
}

// renewLease handles the /v1/runners/lease/renew endpoint. It resolves the
// finalized lease from the directory, validates session, then delegates to
// either RenewGroupLease or (future) RenewNodeLease based on task type.
func (c *Core) renewLease(ctx context.Context, req protocol.RenewLeaseRequest, info TransportInfo) (protocol.RenewLeaseResponse, error) {
	if req.RunnerID == "" || req.SessionID == "" {
		return protocol.RenewLeaseResponse{}, ErrRunnerSessionRequired
	}
	if err := c.runners.ValidateSession(ctx, req.RunnerID, req.SessionID); err != nil {
		return protocol.RenewLeaseResponse{}, normalizeRunnerError(err, c.logger, "renew_lease")
	}

	lookup, hasLookup := c.runners.(LeaseLookup)
	if !hasLookup {
		return protocol.RenewLeaseResponse{}, errors.New("directory does not support lease lookup")
	}
	resolved, found, err := lookup.LookupLease(ctx, req.RunnerID, req.SessionID, LeaseLookupKey{
		LeaseID:    engine.LeaseID(req.LeaseID),
		LeaseToken: engine.LeaseToken(req.LeaseToken),
	})
	if err != nil {
		return protocol.RenewLeaseResponse{}, normalizeRunnerError(err, c.logger, "renew_lease")
	}
	if !found {
		return protocol.RenewLeaseResponse{Renewed: false, Error: "lease not found"}, nil
	}

	extend := time.Duration(req.Extend) * time.Millisecond
	if extend <= 0 {
		extend = 30 * time.Second
	}

	ge, hasGroup := c.engine.(groupLeaseEngine)
	if hasGroup && isGroupTask(&resolved.Task) {
		renewed, err := ge.RenewGroupLease(ctx, resolved, extend)
		if err != nil {
			return protocol.RenewLeaseResponse{}, normalizeRunnerError(err, c.logger, "renew_lease")
		}
		resp := protocol.RenewLeaseResponse{Renewed: renewed}
		if renewed {
			resp.Deadline = time.Now().UTC().Add(extend)
		}
		return resp, nil
	}

	// Node lease renewal is a future capability (T15). For now, report not renewed.
	return protocol.RenewLeaseResponse{Renewed: false, Error: "node lease renewal not yet supported"}, nil
}
