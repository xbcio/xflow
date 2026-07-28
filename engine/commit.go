package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// CommitTaskResult validates a runner lease token, persists the task result,
// and advances scheduling. Stale tokens are rejected so an older assignment
// cannot overwrite or advance state after a newer lease has been issued.
func (e *Engine) CommitTaskResult(ctx context.Context, lease *TaskLease, result TaskResult) error {
	_, err := e.CommitTaskResultWithOutcome(ctx, lease, result)
	return err
}

// CommitTaskResultWithOutcome validates a runner lease token, persists the
// task result, advances scheduling, and returns a structured outcome for
// control-plane cleanup.
func (e *Engine) CommitTaskResultWithOutcome(ctx context.Context, lease *TaskLease, result TaskResult) (outcome CommitOutcome, err error) {
	defer func() {
		e.notifyCommitOutcome(ctx, outcome)
	}()

	if lease == nil {
		return CommitOutcomeStaleToken, ErrInvalidLeaseToken
	}
	t := &lease.Task
	g, active, err := e.loadActiveGraph(ctx, t.ExecutionID)
	if err != nil {
		return CommitOutcomeTransientError, err
	}
	if !active {
		return CommitOutcomeExecutionInactive, nil
	}

	if result.Suspend != nil && e.suspendDisabled {
		if err := e.CommitTaskFailure(ctx, lease, e.suspendDisabledErr); err != nil {
			return CommitOutcomeTransientError, err
		}
		return CommitOutcomeAccepted, nil
	}

	if !g.AllowCycles() && result.Suspend == nil && (result.Output == nil || !isLoopSplitOutput(result.Output.Data)) {
		return e.commitAcyclicTaskResult(ctx, lease, g, result)
	}
	if result.Suspend != nil {
		return e.commitSuspendedTaskResult(ctx, lease, result)
	}
	return e.commitLegacyTaskResult(ctx, lease, g, result)
}

// CommitTaskFailure forces a leased task to fail the whole execution without
// applying the node's OnError strategy. Backend adapters use this for runtime
// failures that make the execution mode itself incompatible with the task.
func (e *Engine) CommitTaskFailure(ctx context.Context, lease *TaskLease, failure error) error {
	if lease == nil {
		return ErrInvalidLeaseToken
	}
	if failure == nil {
		failure = errors.New("task failed")
	}
	t := &lease.Task
	g, active, err := e.loadActiveGraph(ctx, t.ExecutionID)
	if err != nil {
		return err
	}
	if !active {
		return nil
	}
	if !g.AllowCycles() {
		return e.commitAcyclicFailure(ctx, lease, failure)
	}
	outcome, err := e.commitLegacyNode(ctx, lease, types.NodeStatusFailed, nil, "", failure.Error(), true)
	if err != nil {
		return err
	}
	if outcome == CommitOutcomeStaleToken {
		return ErrInvalidLeaseToken
	}
	return nil
}

// commitLegacyTaskResult handles cyclic and experimental loop/split outputs.
// Ordinary terminal transitions use a backend-owned token fence directly,
// avoiding a long-lived committing state. Suspend and expansion retain the
// explicit claim protocol because they have additional coordination state.
func (e *Engine) commitLegacyTaskResult(ctx context.Context, lease *TaskLease, g *graph.Graph, result TaskResult) (CommitOutcome, error) {
	task := &lease.Task
	meta := g.NodeAt(task.NodeIdx)
	if result.Error != nil || (result.Output != nil && result.Output.Error != nil) {
		var businessErr *types.Error
		if result.Output != nil {
			businessErr = result.Output.Error
		}
		return e.commitLegacyNodeError(ctx, lease, meta, result.Error, result.Output, businessErr)
	}
	if retryErr := outputPortRetryError(result.Output); retryErr != nil {
		retried, err := e.tryRetryWithAttempt(ctx, task, meta, retryErr, lease.Attempt, lease.LeaseToken)
		if err != nil {
			return CommitOutcomeTransientError, fmt.Errorf("retry node %q/%q: %w", task.ExecutionID, task.NodeName, err)
		}
		if retried {
			// Mirror commitLegacyNodeError / the acyclic path: record a retry
			// evidence receipt so the runtime evidence buffer does not drop a
			// retry event on the cyclic error-port retry branch.
			e.publishRetryReceipt(ctx, task, lease.Attempt)
			return CommitOutcomeAccepted, nil
		}
		// Retry budget exhausted: the explicit error-port output is a terminal
		// failure. Apply the node's OnError strategy rather than committing it
		// as a success on the error port.
		return e.commitLegacyNodeError(ctx, lease, meta, retryErr, result.Output, nil)
	}

	data := map[string]any{}
	if result.Output != nil && result.Output.Data != nil {
		data = result.Output.Data
	}
	if isLoopSplitOutput(data) {
		node, claimed, err := e.state.ClaimTaskLease(ctx, lease)
		if err != nil {
			return CommitOutcomeTransientError, err
		}
		if !claimed {
			return CommitOutcomeStaleToken, ErrInvalidLeaseToken
		}
		if types.IsTerminalNodeStatus(node.Status) {
			return CommitOutcomeDuplicateTerminal, nil
		}
		if err := e.expandLoopSplit(ctx, lease, g, data); err != nil {
			return CommitOutcomeTransientError, err
		}
		return CommitOutcomeAccepted, nil
	}

	port := "main"
	if result.Output != nil && result.Output.Port != "" {
		port = result.Output.Port
	}
	return e.commitLegacyNode(ctx, lease, types.NodeStatusSuccess, data, port, "", false)
}

func (e *Engine) commitLegacyNodeError(ctx context.Context, lease *TaskLease, meta graph.NodeMeta, systemErr error, output *types.Output, businessErr *types.Error) (CommitOutcome, error) {
	if retried, err := e.tryRetryWithAttempt(ctx, &lease.Task, meta, systemErr, lease.Attempt, lease.LeaseToken); err != nil {
		return CommitOutcomeTransientError, fmt.Errorf("retry node %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	} else if retried {
		e.publishRetryReceipt(ctx, &lease.Task, lease.Attempt)
		return CommitOutcomeAccepted, nil
	}
	outcome := ApplyOnError(meta.OnError, systemErr, businessErr, output)
	errorPort := outcome.RoutePort == "error" && businessErr == nil
	cls := buildEffectiveClassification(systemErr, businessErr, errorPort)
	return e.commitLegacyNodeWithClassification(ctx, lease, outcome.NodeStatus, outcome.Output, outcome.RoutePort, outcome.ErrorMessage, outcome.ExecFatal, cls)
}

func (e *Engine) commitLegacyNode(ctx context.Context, lease *TaskLease, status types.NodeStatus, output map[string]any, port, errMsg string, fatal bool) (CommitOutcome, error) {
	return e.commitLegacyNodeWithClassification(ctx, lease, status, output, port, errMsg, fatal, EffectiveClassification{})
}

// commitLegacyNodeWithClassification applies a fenced terminal transition for a
// cyclic (or experimental loop/split) node and its deterministic downstream
// intent in one backend transaction. cls is the EffectiveClassification bound
// to this commit (empty for non-error commits); it is only carried on the
// read-only commit receipt and never changes control flow.
func (e *Engine) commitLegacyNodeWithClassification(ctx context.Context, lease *TaskLease, status types.NodeStatus, output map[string]any, port, errMsg string, fatal bool, cls EffectiveClassification) (CommitOutcome, error) {
	task := &lease.Task
	g, active, err := e.loadActiveGraph(ctx, task.ExecutionID)
	if err != nil {
		return CommitOutcomeTransientError, err
	}
	if !active {
		return CommitOutcomeExecutionInactive, nil
	}

	// Experimental Loop/Split workflows may be acyclic even though their parent
	// reaches an intermediate waiting state. Once the fenced child generation
	// completes, use the normal atomic commit/outbox path so downstream work is
	// not left to the legacy direct scheduler.
	if !g.AllowCycles() {
		return e.commitAcyclicNode(ctx, lease, status, output, port, errMsg, fatal)
	}

	committer, ok := e.state.(LegacyNodeCommitter)
	if !ok {
		return CommitOutcomeTransientError, ErrAtomicCommitUnsupported
	}

	// Compute the cyclic downstream intent deterministically BEFORE the commit
	// so the terminal transition and its downstream delivery intents (or the
	// execution finalization) are persisted in ONE fenced transaction. This
	// closes the #7 window where a crash between the terminal commit and a
	// separate queue.Enqueue permanently lost downstream cyclic tasks.
	//
	// A fatal abort of a cyclic node has no downstream: the whole execution
	// fails. Express that failure through the cyclic-completion mechanism
	// (CyclicComplete + finalStatus=failed) so the terminal node write AND the
	// execution finalization land in ONE fenced transition, exactly like the
	// acyclic Fatal path. This code is only reached for genuinely cyclic graphs
	// — acyclic and experimental loop/split graphs are redirected to
	// commitAcyclicNode above, where Fatal is already finalized atomically.
	//
	// Previously a fatal cyclic node committed terminal here and the engine
	// finalized the execution with a SEPARATE UpdateExecutionStatus below. A
	// crash (or error) between the two left the execution stuck Running with a
	// terminal failed node; the lease sweeper only scans non-terminal nodes, so
	// nothing could ever recover it (H1). Folding the finalization into the
	// fenced commit makes it atomic and makes crash recovery idempotent (a
	// replay observes DuplicateTerminal with the execution already failed).
	var plan cyclicPlan
	if fatal {
		plan = cyclicPlan{complete: true, finalStatus: types.ExecutionStatusFailed, finalError: errMsg}
	} else {
		plan = e.planCyclicDownstream(g, &lease.Task, port)
	}

	req := CommitNodeRequest{
		ExecutionID:  task.ExecutionID,
		NodeName:     task.NodeName,
		NodeIdx:      task.NodeIdx,
		ActivationID: task.ActivationID,
		AutoDepth:    task.AutoDepth,
		LeaseID:      lease.LeaseID,
		LeaseToken:   lease.LeaseToken,
		Attempt:      lease.Attempt,
		Status:       status,
		Output:       output,
		StoreOutput:  true,
		Port:         port,
		Error:        errMsg,
		// Fatal is intentionally NOT set for the cyclic path: the backend cyclic
		// finalization is driven by CyclicComplete (above), and the Fatal flag is
		// the acyclic-path finalization signal (it is also the guard the backend
		// uses to skip cyclic downstream). Carrying the fatal intent via
		// CyclicComplete keeps the whole transition inside one fenced commit.
		Fatal:             false,
		CyclicOutbox:      plan.entries,
		CyclicComplete:    plan.complete,
		CyclicFinalStatus: plan.finalStatus,
		CyclicFinalError:  plan.finalError,
	}
	result, err := committer.CommitLeasedNode(ctx, req)
	if err != nil {
		return CommitOutcomeTransientError, err
	}
	// Publish read-only runtime evidence receipts for the accepted terminal
	// case, mirroring the acyclic atomic path (Tasks 3/4). Non-blocking and
	// nil-buffer-safe; never changes commit control flow or return values. The
	// cyclic "advance" is the activation of the downstream cyclic task: the
	// durable outbox entry persisted within this same fenced commit IS the
	// advance, so an advance receipt is published when downstream intents were
	// actually applied (result.OutboxIDs carries the cyclic outbox entry IDs
	// because the cyclic request sets no AdvanceTask).
	if result.Outcome == CommitOutcomeAccepted {
		e.publishCommitReceipt(ctx, req, result, cls)
		if result.Applied && len(result.OutboxIDs) > 0 {
			e.publishAdvanceReceipt(ctx, task, AdvanceNodeResult{Applied: true, OutboxIDs: result.OutboxIDs})
		}
	}
	switch result.Outcome {
	case CommitOutcomeAccepted:
		e.notifyNodeComplete(ctx, task.ExecutionID, task.NodeName, status)
		if result.ExecutionDone {
			// The cyclic branch terminated (fatal abort, natural completion, or
			// MaxAutoDepth) and the backend finalized the execution status in the
			// SAME fenced transition as the terminal node write. Yield the
			// completion notification to an in-flight Cancel, matching
			// completeExecution's cancel-aware behavior.
			if !e.isCancelingOrCanceled(ctx, task.ExecutionID) {
				e.notifyExecutionComplete(ctx, task.ExecutionID, result.ExecutionStatus)
				e.EvictExecution(task.ExecutionID)
			}
			return CommitOutcomeAccepted, nil
		}
		// Downstream intents are durably persisted; deliver them now. A crash
		// before this flush is recovered by the outbox dispatcher.
		if err := e.FlushOutbox(ctx, task.ExecutionID); err != nil {
			return CommitOutcomeTransientError, err
		}
		return CommitOutcomeAccepted, nil
	case CommitOutcomeDuplicateTerminal:
		// The node was already committed terminal by a prior attempt that may
		// have crashed before delivering its downstream intents. Replay the
		// outbox so those intents are not stranded (fixes the retry-does-not-
		// re-enqueue gap in #7). FlushOutbox is a no-op when nothing is pending.
		if err := e.FlushOutbox(ctx, task.ExecutionID); err != nil {
			return CommitOutcomeTransientError, err
		}
		return CommitOutcomeDuplicateTerminal, nil
	case CommitOutcomeStaleToken:
		return CommitOutcomeStaleToken, ErrInvalidLeaseToken
	case CommitOutcomeExecutionInactive:
		return CommitOutcomeExecutionInactive, nil
	default:
		return CommitOutcomeTransientError, fmt.Errorf("commit legacy node %q/%q returned %q", task.ExecutionID, task.NodeName, result.Outcome)
	}
}

// commitSuspendedTaskResult claims the active lease, then delegates the full
// output/signal/status transition to a token-fenced state-store primitive.
func (e *Engine) commitSuspendedTaskResult(ctx context.Context, lease *TaskLease, result TaskResult) (CommitOutcome, error) {
	node, claimed, err := e.state.ClaimTaskLease(ctx, lease)
	if err != nil {
		return CommitOutcomeTransientError, err
	}
	if !claimed {
		return CommitOutcomeStaleToken, ErrInvalidLeaseToken
	}
	if types.IsTerminalNodeStatus(node.Status) {
		return CommitOutcomeDuplicateTerminal, nil
	}

	storeOutput := true
	var output map[string]any
	oldSignalName := ""
	if result.Output != nil && result.Output.Resuspend {
		storeOutput = result.Output.Data != nil
		output = result.Output.Data
		if lease.Task.Payload == nil {
			return CommitOutcomeTransientError, fmt.Errorf("resuspend result for %s without resume payload", lease.Task.NodeName)
		}
		oldSignalName = lease.Task.Payload.Name
	} else if lease.Input != nil {
		output = cloneMap(lease.Input.Data)
	}

	suspender, ok := e.state.(DurableLeaseSuspender)
	if !ok {
		return CommitOutcomeTransientError, ErrAtomicCommitUnsupported
	}
	committed, err := suspender.SuspendTaskLeaseWithOutbox(ctx, lease, output, storeOutput, result.Suspend, oldSignalName)
	if err != nil {
		return CommitOutcomeTransientError, err
	}
	if !committed {
		return CommitOutcomeStaleToken, ErrInvalidLeaseToken
	}
	if err := e.FlushOutbox(ctx, lease.Task.ExecutionID); err != nil {
		return CommitOutcomeTransientError, fmt.Errorf("deliver suspend continuation for %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	e.notifyNodeSuspended(ctx, &lease.Task)
	return CommitOutcomeAccepted, nil
}

func outputPortRetryError(output *types.Output) error {
	if output == nil || output.Port != "error" || output.Error != nil {
		return nil
	}
	if output.Data != nil {
		if msg, ok := output.Data["error"].(string); ok && msg != "" {
			return errors.New(msg)
		}
	}
	return errors.New("node returned error port")
}
