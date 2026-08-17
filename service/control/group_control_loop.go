package control

import (
	"context"
	"errors"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/observability/tracing"
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

// nodeLeaseEngine is the optional interface for engines that can extend a live
// node lease. It is separate from groupLeaseEngine because the two renew
// different state machines: a group renews its unit lease, a node renews its
// own. The concrete *engine.Engine implements both.
type nodeLeaseEngine interface {
	RenewTaskLease(ctx context.Context, lease *engine.TaskLease, extend time.Duration) (bool, error)
}

// nodeTimeoutCommitter is the optional interface for engines that can commit a
// node timeout from the server side. The concrete *engine.Engine implements it.
// It is tested separately from nodeLeaseEngine because the backstop is a new
// code path — forcing every existing fake to implement it would be churn with
// no verification value.
type nodeTimeoutCommitter interface {
	CommitTaskTimeout(ctx context.Context, lease *engine.TaskLease, cause error) error
}

// isGroupTask returns true when the task type is a group execution type.
func isGroupTask(t *engine.Task) bool {
	return t != nil && t.Type == engine.TaskTypeGroupExec
}

// isBatchTask returns true when the task is one batch of a map expansion.
func isBatchTask(t *engine.Task) bool {
	return t != nil && t.Type == engine.TaskTypeNodeBatch
}

// subgraphLeaseEngine is the optional interface for engines that can build a
// batch lease. The concrete *engine.Engine implements it.
type subgraphLeaseEngine interface {
	BuildSubgraphLease(ctx context.Context, t *engine.Task) (*engine.TaskLease, *engine.SubgraphLeasePayload, error)
}

// dispatchSubgraphLease handles the BuildSubgraphLease + FinalizeClaim flow for
// batch tasks, mirroring dispatchGroupLease.
//
// It has no already-active recovery branch, unlike dispatchGroupLease. That is
// not an omission: BuildSubgraphLease never calls AcquireTaskLease — the batch
// borrows the parent map node's fence rather than claiming one of its own — so
// there is no "already active" state for a batch to collide with. A replay
// rebuilds the same lease from the task payload (see RecoverTaskLease).
func (c *Core) dispatchSubgraphLease(ctx context.Context, claim Claim) (protocol.PollTaskResponse, error) {
	se, ok := c.engine.(subgraphLeaseEngine)
	if !ok {
		_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimRequeue)
		return protocol.PollTaskResponse{}, errors.New("engine does not support batch leases")
	}

	// A batch's lease needs a dispatch span for the same reason a node's does:
	// the runner starts xflow.task.execute from the carrier injected here, so
	// without it the whole remote body execution has no remote parent.
	dispatchCtx, dispatchSpan := c.startDispatchSpan(ctx, &claim.Assignment.Task)
	lease, payload, err := se.BuildSubgraphLease(dispatchCtx, &claim.Assignment.Task)
	switch {
	case err == nil:
		lease.SubgraphPayload = payload
		lease.TraceCarrier = tracing.InjectCarrier(dispatchCtx)
		lease.Namespace = claim.Assignment.Namespace
		if err := c.runners.FinalizeClaim(dispatchCtx, claim.ClaimID, lease); err != nil {
			dispatchSpan.RecordError(err)
			dispatchSpan.End()
			_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimRequeue)
			return protocol.PollTaskResponse{}, normalizeRunnerError(err, c.logger, "poll")
		}
		dispatchSpan.End()
		return protocol.PollTaskResponse{Lease: lease}, nil

	case errors.Is(err, engine.ErrExecutionInactive):
		dispatchSpan.End()
		_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimDrop)
		return protocol.PollTaskResponse{}, nil

	default:
		dispatchSpan.RecordError(err)
		dispatchSpan.End()
		_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimRequeue)
		return protocol.PollTaskResponse{}, err
	}
}

// dispatchGroupLease handles the BuildGroupLease + FinalizeClaim flow for group
// tasks, mirroring the node BuildTaskLease path but attaching GroupPayload.
func (c *Core) dispatchGroupLease(ctx context.Context, claim Claim) (protocol.PollTaskResponse, error) {
	ge, ok := c.engine.(groupLeaseEngine)
	if !ok {
		_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimRequeue)
		return protocol.PollTaskResponse{}, errors.New("engine does not support group leases")
	}

	// See dispatchSubgraphLease: the carrier injected from this span's context is
	// what parents the runner's whole group execution to the workflow trace.
	dispatchCtx, dispatchSpan := c.startDispatchSpan(ctx, &claim.Assignment.Task)
	lease, payload, err := ge.BuildGroupLease(dispatchCtx, &claim.Assignment.Task)
	switch {
	case err == nil:
		lease.GroupPayload = payload
		lease.TraceCarrier = tracing.InjectCarrier(dispatchCtx)
		lease.Namespace = claim.Assignment.Namespace
		if err := c.runners.FinalizeClaim(dispatchCtx, claim.ClaimID, lease); err != nil {
			dispatchSpan.RecordError(err)
			dispatchSpan.End()
			_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimRequeue)
			return protocol.PollTaskResponse{}, normalizeRunnerError(err, c.logger, "poll")
		}
		dispatchSpan.End()
		return protocol.PollTaskResponse{Lease: lease}, nil

	case errors.Is(err, engine.ErrGroupLeaseAlreadyActive):
		recovered, recoverErr := c.recoverGroupLease(dispatchCtx, ge, &claim.Assignment.Task)
		if recoverErr == nil {
			// The recovered lease's original carrier died with the process that
			// built it, so it gets this dispatch's instead: the runner about to
			// receive it is starting a fresh execute span either way.
			recovered.TraceCarrier = tracing.InjectCarrier(dispatchCtx)
			recovered.Namespace = claim.Assignment.Namespace
			if finalizeErr := c.runners.FinalizeClaim(dispatchCtx, claim.ClaimID, recovered); finalizeErr != nil {
				dispatchSpan.RecordError(finalizeErr)
				dispatchSpan.End()
				_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimRequeue)
				return protocol.PollTaskResponse{}, normalizeRunnerError(finalizeErr, c.logger, "poll")
			}
			dispatchSpan.End()
			return protocol.PollTaskResponse{Lease: recovered}, nil
		}
		dispatchSpan.End()
		if errors.Is(recoverErr, engine.ErrExecutionInactive) {
			_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimDrop)
			return protocol.PollTaskResponse{}, nil
		}
		_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimRequeue)
		return protocol.PollTaskResponse{}, recoverErr

	case errors.Is(err, engine.ErrExecutionInactive):
		dispatchSpan.End()
		_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimDrop)
		return protocol.PollTaskResponse{}, nil

	default:
		dispatchSpan.RecordError(err)
		dispatchSpan.End()
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
// either RenewGroupLease or RenewTaskLease based on task type.
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

	// Refuse to renew past the execution deadline, and terminate the task on the
	// spot. Both halves are required.
	//
	// Refusing alone is not enough: one TTL later the sweeper treats the lease
	// as a crashed runner and puts the task back to pending (ReclaimLease, the
	// node branch at engine/lease.go:408-441 — RevokeLeaseWithOutbox → Enqueue,
	// or the legacy RevokeLease → Enqueue fallback), which is the retry a
	// timeout must never get. A well-behaved runner never reaches this branch
	// -- it reports its own terminal result -- but a runner that ignores the
	// deadline is exactly the case this backstop exists for, and that is the
	// case the sweeper would otherwise pick up.
	//
	// This does mean the server commits a node it is not executing. That was the
	// path this design tried to avoid, but "no retry on timeout" forces it. The
	// trade is that the failover recovery chain (ListExpiredLeases -> reclaim*)
	// is untouched: the risk is confined to one new branch here rather than a
	// fork in crash recovery.
	//
	// KNOWN LIMIT: gRPC runners never renew at all because
	// service/runner/runner.go:110 gates renewal behind a leaseRenewClient type
	// assertion the gRPC client does not satisfy. So this backstop is HTTP-only.
	// That matches the project's stated direction (HTTP is the primary runner
	// transport); fixing gRPC is out of scope.
	//
	// The branch is node-only: CommitTaskTimeout is a node commit path and
	// refuses group leases (ErrGroupLeaseNotSupported). A group's deadline lives
	// in GroupLeasePayload.Deadline, not on TaskLease.ExecutionDeadline, so today
	// no group lease reaches this branch. The isGroupTask guard makes that
	// invariant explicit: if someone stamps ExecutionDeadline on a group lease
	// in the future, the branch is skipped and the group renewal path below
	// handles it (or a future group-aware timeout commit does).
	if !isGroupTask(&resolved.Task) && !resolved.ExecutionDeadline.IsZero() && !resolved.ExecutionDeadline.After(time.Now()) {
		// Record the server-detected timeout before committing: the source label
		// distinguishes runner-detected (well-behaved runner reports its own
		// terminal result before renewal) from server-detected (this branch).
		// A persistently non-zero server count means some runner has not
		// implemented or upgraded the deadline. node_type only -- never node
		// name, execution ID, params, or output.
		c.observeNodeTimeout(ctx, resolved.NodeType)
		if committer, ok := c.engine.(nodeTimeoutCommitter); ok {
			cause := types.NewPermanentError("node.timeout", "node execution exceeded its deadline")
			if err := committer.CommitTaskTimeout(ctx, resolved, cause); err != nil {
				return protocol.RenewLeaseResponse{}, normalizeRunnerError(err, c.logger, "renew_lease")
			}
		}
		return protocol.RenewLeaseResponse{Renewed: false, Error: "execution deadline exceeded"}, nil
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

	// Node leases need renewal for the same reason group leases do: the engine
	// stamps every lease with its default TTL, and nothing clamps a node's own
	// timeout (xflow.http's options.timeout, xflow.script's params.timeout)
	// against it. Without this branch a handler configured to run longer than the
	// TTL gets swept and redelivered while its runner is still executing it.
	ne, hasNode := c.engine.(nodeLeaseEngine)
	if !hasNode {
		return protocol.RenewLeaseResponse{Renewed: false, Error: "engine does not support node lease renewal"}, nil
	}
	renewed, err := ne.RenewTaskLease(ctx, resolved, extend)
	if err != nil {
		return protocol.RenewLeaseResponse{}, normalizeRunnerError(err, c.logger, "renew_lease")
	}
	resp := protocol.RenewLeaseResponse{Renewed: renewed}
	if renewed {
		resp.Deadline = time.Now().UTC().Add(extend)
	}
	return resp, nil
}
