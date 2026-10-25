package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/xbcio/xflow/types"
)

// ExpireOutcome reports what one ExpireExecution call did.
type ExpireOutcome string

const (
	// ExpireOutcomeExpired means this call drove the execution into timeout and
	// ran the cascade that stops what it still had running.
	ExpireOutcomeExpired ExpireOutcome = "expired"
	// ExpireOutcomeResumed means the execution was already in timeout for its
	// deadline (an earlier call wrote it and did not finish), so this call only
	// completed the cascade it had left behind.
	ExpireOutcomeResumed ExpireOutcome = "resumed"
	// ExpireOutcomeNotDue means the execution has no deadline, or its deadline
	// has not been reached. Nothing was written.
	ExpireOutcomeNotDue ExpireOutcome = "not_due"
	// ExpireOutcomeAlreadyTerminal means the execution had already concluded,
	// or timed out for a reason other than its deadline. Nothing was written.
	ExpireOutcomeAlreadyTerminal ExpireOutcome = "already_terminal"
	// ExpireOutcomeCanceling means a cancel owns the execution. Nothing was
	// written, and the deadline entry is left for the cancel to withdraw.
	ExpireOutcomeCanceling ExpireOutcome = "canceling"
	// ExpireOutcomeGone means the execution (or its graph) no longer exists.
	ExpireOutcomeGone ExpireOutcome = "gone"
)

// deadlineReached reports whether an execution deadline has been reached at
// now. A zero deadline is no budget and is never reached; the boundary instant
// counts as reached. It is a pure function so the boundary can be pinned
// without a clock.
func deadlineReached(now, deadline time.Time) bool {
	return !deadline.IsZero() && !now.Before(deadline)
}

// ExpireExecution drives an execution whose settings.timeout deadline has been
// reached into the timeout terminal state and stops what it still had running.
//
// It is the one entry point every deadline observer calls: the maintenance
// sweep that lists ExecutionDeadlineExpirer entries, and the lease and commit
// paths that notice the deadline while a task is in flight. Every outcome other
// than a store failure comes back as (outcome, nil), including "someone else
// already handled this" (it never returns ErrExecutionInactive), so all of them
// can call it unconditionally.
//
// The order is terminal first. Both backends refuse to overwrite a terminal
// status and accept only canceled once an execution is canceling, so the
// canceling-then-cascade-then-final-write order Cancel uses cannot end in
// timeout. Instead the timeout status is written first, as one fenced
// compare-and-set, and the cascade runs after it. The cascade is idempotent and
// does not need the execution to be active (it loads the graph without the
// activeness check), so a crash anywhere after the status write is repaired by
// calling ExpireExecution again: it finds the execution already timed out for
// its deadline (ExpireOutcomeResumed), repeats the cascade, and does not repeat
// the completion hook, which the winning call fires right after the status
// write. The deadline entry (ExecutionDeadlineExpirer) is
// acknowledged only after the cascade succeeded, which is what keeps a
// half-finished expiry discoverable.
//
// The reason string is what tells this mechanism's timeout from the other one
// (ConcurrencyQueueTimeoutError): an execution already in timeout for another
// reason is not this call's to finish or announce again.
//
// A cancel that got to the execution first wins: an execution that is canceling
// is left alone (ExpireOutcomeCanceling) and ends canceled.
//
// Two concurrent calls on one execution can each observe their own write as the
// winning one and both announce completion, the same narrow window Cancel has.
//
// Slot release and queue promotion are done by the store's terminal write; an
// execution that expires while still queued leaves the line there.
func (e *Engine) ExpireExecution(ctx context.Context, id types.ExecutionID) (outcome ExpireOutcome, err error) {
	ctx, span := e.startExecutionSpan(ctx, id, "xflow.execution.expire",
		"execution_id", string(id),
	)
	defer span.End()
	defer func() {
		if err != nil {
			span.RecordError(err)
			return
		}
		span.Set("outcome", string(outcome))
		// Reading the trace carrier for the span above memoizes it per
		// execution. Expired and Resumed evict at the end of the cascade; every
		// other result leaves the execution to someone else (or to nobody), and
		// on an engine that does not own it nothing else would ever evict the
		// entry, so it is dropped here. Eviction only drops this engine's
		// process-local caches: dropping something still in use costs a reload.
		if outcome != ExpireOutcomeExpired && outcome != ExpireOutcomeResumed {
			e.EvictExecution(id)
		}
	}()

	status, found, err := e.executionStatus(ctx, id)
	if err != nil {
		return "", fmt.Errorf("read status of execution %q: %w", id, err)
	}
	if !found {
		return ExpireOutcomeGone, nil
	}
	switch status {
	case types.ExecutionStatusSuccess, types.ExecutionStatusFailed, types.ExecutionStatusCanceled:
		return e.expireAlreadyTerminal(ctx, id)
	case types.ExecutionStatusCanceling:
		return ExpireOutcomeCanceling, nil
	}

	snap, err := e.state.GetExecution(ctx, id)
	if err != nil {
		return "", fmt.Errorf("get execution %q: %w", id, err)
	}
	if snap == nil {
		return ExpireOutcomeGone, nil
	}
	resumed := false
	if status == types.ExecutionStatusTimeout {
		if snap.Error != ExecutionDeadlineExceededError {
			return e.expireAlreadyTerminal(ctx, id)
		}
		resumed = true
	} else if !deadlineReached(time.Now(), snap.Deadline) {
		return ExpireOutcomeNotDue, nil
	}

	g, err := e.loadGraph(ctx, id)
	if err != nil {
		return "", err
	}
	if g == nil {
		return ExpireOutcomeGone, nil
	}

	if !resumed {
		if err := e.state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusTimeout, ExecutionDeadlineExceededError); err != nil {
			return "", fmt.Errorf("mark execution %q timed out: %w", id, err)
		}
		// The write is fenced at the store, so what it did is read back rather
		// than assumed: a cancel or a normal conclusion may have won the race.
		after, err := e.state.GetExecution(ctx, id)
		if err != nil {
			return "", fmt.Errorf("get execution %q after timeout write: %w", id, err)
		}
		switch {
		case after == nil:
			return ExpireOutcomeGone, nil
		case after.Status == types.ExecutionStatusCanceling:
			return ExpireOutcomeCanceling, nil
		case after.Status != types.ExecutionStatusTimeout || after.Error != ExecutionDeadlineExceededError:
			return e.expireAlreadyTerminal(ctx, id)
		}
		// Announced as soon as the terminal status is durable, before the
		// cascade, like Cancel announces after its terminal write. A cascade
		// that fails on a transient store error is repaired by a later call,
		// which is Resumed and does not announce again, so announcing after the
		// cascade would lose the hook for good.
		e.notifyExecutionComplete(ctx, id, types.ExecutionStatusTimeout)
	}

	if err := e.cancelSuspendedNodes(ctx, id, g); err != nil {
		return "", fmt.Errorf("expire execution %q: %w", id, err)
	}
	e.cancelSubWorkflowChildren(ctx, id, g)
	if err := e.finalizeParkedSubWorkflowNodes(ctx, id, g); err != nil {
		return "", fmt.Errorf("expire execution %q: %w", id, err)
	}

	outcome = ExpireOutcomeExpired
	if resumed {
		// The hook already fired for the call that wrote the status; only the
		// parent's conclusion may have been missed.
		e.notifySubWorkflowParent(ctx, id)
		outcome = ExpireOutcomeResumed
	}
	e.EvictExecution(id)
	if err := e.ackDeadlineExpired(ctx, id); err != nil {
		return "", err
	}
	return outcome, nil
}

// expireAlreadyTerminal settles a call that found the execution concluded by
// something other than this deadline. There is nothing to write or cascade, but
// the deadline entry may still be listed, so it is acknowledged.
func (e *Engine) expireAlreadyTerminal(ctx context.Context, id types.ExecutionID) (ExpireOutcome, error) {
	if err := e.ackDeadlineExpired(ctx, id); err != nil {
		return "", err
	}
	return ExpireOutcomeAlreadyTerminal, nil
}

// ackDeadlineExpired withdraws the execution's deadline entry when the backend
// keeps one. A backend without the capability has nothing to withdraw.
func (e *Engine) ackDeadlineExpired(ctx context.Context, id types.ExecutionID) error {
	expirer, ok := e.state.(ExecutionDeadlineExpirer)
	if !ok {
		return nil
	}
	if err := expirer.AckDeadlineExpired(ctx, id); err != nil {
		return fmt.Errorf("acknowledge expired execution %q: %w", id, err)
	}
	return nil
}
