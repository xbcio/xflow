package xflow

import (
	"context"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// Wait blocks until the execution reaches a terminal state or ctx is canceled.
//
// Backends that implement event watching wake promptly; otherwise Wait polls.
// The returned Result contains the final execution status and latest node
// outputs. In cyclic mode, repeated nodes expose only their latest output.
func (e *Engine) Wait(ctx context.Context, id types.ExecutionID) (types.Result, error) {
	if e.waiter != nil {
		return e.waiter.WaitDone(ctx, id)
	}
	// Fallback: poll StateStore. Check immediately once before entering the
	// ticker loop so an already-terminal execution returns without waiting a
	// full 500ms tick.
	const maxConsecutiveErrors = 5
	var consecutiveErrs int
	var lastErr error
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		snap, err := e.eng.State().GetExecution(ctx, id)
		if err != nil {
			consecutiveErrs++
			lastErr = err
			if consecutiveErrs >= maxConsecutiveErrors {
				return types.Result{}, fmt.Errorf("xflow: Wait: persistent backend error after %d attempts: %w", consecutiveErrs, lastErr)
			}
		} else {
			consecutiveErrs = 0
			if snap != nil && isTerminalStatus(snap.Status) {
				detail, err := e.eng.Inspect(ctx, id)
				if err != nil {
					return types.Result{}, err
				}
				return resultFromDetail(detail), nil
			}
			// A non-existent execution returns snap==nil with no error; we keep
			// polling (until ctx is canceled) because the caller may be waiting
			// for an execution that is about to be created.
		}
		select {
		case <-ctx.Done():
			return types.Result{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func resultFromDetail(detail engine.ExecutionDetail) types.Result {
	out := make(map[string]any, len(detail.Nodes))
	for _, n := range detail.Nodes {
		if n.Output != nil {
			out[n.Name] = n.Output
		}
	}
	if len(out) == 0 {
		out = nil
	}
	return types.Result{
		ExecutionID: detail.ExecutionID,
		Status:      detail.Status,
		Output:      out,
		Error:       detail.Error,
	}
}

// Signal delivers a named signal to a suspended node within the execution.
//
// Signal names are defined by suspending nodes. For built-in approval nodes the
// per-approver form is "NodeName/approval/approver". If a signal arrives before
// the node suspends, the backend stores it and consumes it when the node reaches
// the matching wait point.
func (e *Engine) Signal(ctx context.Context, id types.ExecutionID, name string, data map[string]any) error {
	if e.executionMode == ExecutionModeTransient {
		return ErrTransientSignalsUnsupported
	}
	return e.eng.DeliverSignal(ctx, id, name, data)
}

// RevokeSignal revokes a pre-delivered signal that has not yet been consumed.
//
// It cannot revoke a signal that already resumed a node. Use it for UI flows
// where a user retracts an early signal before the workflow reaches the wait
// point.
func (e *Engine) RevokeSignal(ctx context.Context, id types.ExecutionID, name string) error {
	if e.executionMode == ExecutionModeTransient {
		return ErrTransientSignalsUnsupported
	}
	return e.eng.RevokeSignal(ctx, id, name)
}

// Cancel cancels a running execution and releases suspended nodes.
//
// Cancel is best-effort for work already leased to a runner: the execution is
// marked canceled and suspended waits are released, while stale task commits are
// fenced by the engine/state store.
func (e *Engine) Cancel(ctx context.Context, id types.ExecutionID) error {
	return e.eng.Cancel(ctx, id)
}

// Inspect returns execution and node status details for audit and UI flows.
//
// When nodeNames are omitted, Inspect loads the stored graph and returns every
// node's current status and latest output. In cyclic mode this is still a
// latest-state view, not a per-activation history.
func (e *Engine) Inspect(ctx context.Context, id types.ExecutionID, nodeNames ...string) (engine.ExecutionDetail, error) {
	if e.executionMode == ExecutionModeTransient {
		return engine.ExecutionDetail{}, ErrTransientInspectionUnavailable
	}
	return e.eng.Inspect(ctx, id, nodeNames...)
}

func isTerminalStatus(s types.ExecutionStatus) bool { return types.IsTerminalExecutionStatus(s) }
