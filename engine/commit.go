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
			return CommitOutcomeAccepted, nil
		}
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
		return CommitOutcomeAccepted, nil
	}
	outcome := ApplyOnError(meta.OnError, systemErr, businessErr, output)
	return e.commitLegacyNode(ctx, lease, outcome.NodeStatus, outcome.Output, outcome.RoutePort, outcome.ErrorMessage, outcome.ExecFatal)
}

func (e *Engine) commitLegacyNode(ctx context.Context, lease *TaskLease, status types.NodeStatus, output map[string]any, port, errMsg string, fatal bool) (CommitOutcome, error) {
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
	// separate queue.Enqueue permanently lost downstream cyclic tasks. A fatal
	// abort has no downstream: the whole execution fails.
	var plan cyclicPlan
	if !fatal {
		plan = e.planCyclicDownstream(g, &lease.Task, port)
	}

	result, err := committer.CommitLeasedNode(ctx, CommitNodeRequest{
		ExecutionID:       task.ExecutionID,
		NodeName:          task.NodeName,
		NodeIdx:           task.NodeIdx,
		ActivationID:      task.ActivationID,
		AutoDepth:         task.AutoDepth,
		LeaseID:           lease.LeaseID,
		LeaseToken:        lease.LeaseToken,
		Attempt:           lease.Attempt,
		Status:            status,
		Output:            output,
		StoreOutput:       true,
		Port:              port,
		Error:             errMsg,
		Fatal:             fatal,
		CyclicOutbox:      plan.entries,
		CyclicComplete:    plan.complete,
		CyclicFinalStatus: plan.finalStatus,
		CyclicFinalError:  plan.finalError,
	})
	if err != nil {
		return CommitOutcomeTransientError, err
	}
	switch result.Outcome {
	case CommitOutcomeAccepted:
		e.notifyNodeComplete(ctx, task.ExecutionID, task.NodeName, status)
		if fatal {
			if err := e.state.UpdateExecutionStatus(ctx, task.ExecutionID, types.ExecutionStatusFailed, errMsg); err != nil {
				return CommitOutcomeTransientError, fmt.Errorf("mark execution %q failed: %w", task.ExecutionID, err)
			}
			e.notifyExecutionComplete(ctx, task.ExecutionID, types.ExecutionStatusFailed)
			e.EvictExecution(task.ExecutionID)
			return CommitOutcomeAccepted, nil
		}
		if result.ExecutionDone {
			// The cyclic branch terminated (or exceeded MaxAutoDepth) and the
			// backend finalized the execution status in the same fenced
			// transition. Yield the completion notification to an in-flight
			// Cancel, matching completeExecution's cancel-aware behavior.
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
