package engine

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/types"
)

// Pause suspends dispatch of a running execution: its status becomes
// types.ExecutionStatusPaused and nothing in its ready set is delivered until
// Resume. Work already leased to a runner runs to completion and commits its
// result, including the commit that happens to finish the execution.
//
// The transition itself belongs to the store, because the enforcement point is
// the store's dispatch transaction: a check made here, before claiming, races
// the very pause it is deciding — a claim that read "running" can still be
// granted after the pause landed. What this method adds is the capability
// lookup and the two verdicts the store cannot give: an execution that is not
// there is reported as not found rather than as unpausable, since the caller
// must be able to tell a typo from a queued execution, and an execution that is
// already paused is the store's idempotent success.
//
// Existence is read before the transition, and only for that verdict: whether
// the pause is allowed stays the store's compare-and-set decision, never this
// read's.
func (e *Engine) Pause(ctx context.Context, id types.ExecutionID) error {
	pauser, err := e.executionPauser()
	if err != nil {
		return err
	}
	if err := e.confirmExecutionRecorded(ctx, id); err != nil {
		return err
	}
	if err := pauser.PauseExecution(ctx, id); err != nil {
		return fmt.Errorf("pause execution %q: %w", id, err)
	}
	return nil
}

// Resume returns a paused execution to dispatch and delivers what the pause
// held.
//
// The status is the whole release — the pause never moved the ready set — so
// the flush below is about promptness rather than correctness: without it the
// held work waits for the outbox dispatcher's next drain, and with it the
// operator sees the execution move as soon as the call returns. Because the
// flush is recoverable by that same dispatcher (the execution is running again,
// so discovery finds it), a failed flush does not fail the resume: the state
// change the caller asked for has happened, and reporting an error would invite
// a retry that the execution would refuse as not-paused.
func (e *Engine) Resume(ctx context.Context, id types.ExecutionID) error {
	pauser, err := e.executionPauser()
	if err != nil {
		return err
	}
	if err := e.confirmExecutionRecorded(ctx, id); err != nil {
		return err
	}
	if err := pauser.ResumeExecution(ctx, id); err != nil {
		return fmt.Errorf("resume execution %q: %w", id, err)
	}
	if err := e.FlushOutbox(ctx, id); err != nil && e.logger != nil {
		e.logger.Error("resumed execution's outbox delivery deferred",
			"execution_id", string(id), "err", err)
	}
	return nil
}

// executionPauser resolves the store's pause capability.
func (e *Engine) executionPauser() (ExecutionPauser, error) {
	pauser, ok := e.state.(ExecutionPauser)
	if !ok {
		return nil, ErrPauseUnsupported
	}
	return pauser, nil
}

// confirmExecutionRecorded answers whether the execution exists at all, which
// is the one question neither pause nor resume can answer from the store's
// refusal: a single sentinel covers "absent" and "in the wrong status" on
// purpose (see ExecutionPauser), so a caller wanting 404 for a typo and 409 for
// a queued submission has to ask separately.
//
// The narrow status read is the cheap form of that question on the backends
// that have it, and it is deliberately not used to decide anything else — a
// second check here of "is it running" would be a check racing the transition
// it is deciding, which is exactly what the store's compare-and-set exists to
// avoid.
func (e *Engine) confirmExecutionRecorded(ctx context.Context, id types.ExecutionID) error {
	_, found, err := e.executionStatus(ctx, id)
	if err != nil {
		return fmt.Errorf("read status for %q: %w", id, err)
	}
	if !found {
		return fmt.Errorf("execution %q: %w", id, ErrExecutionNotFound)
	}
	return nil
}
