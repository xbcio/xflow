package engine

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// commitAcyclicTaskResult applies an ordinary (non-suspend) DAG result through
// the backend-owned atomic commit primitive. It deliberately leaves cyclic and
// suspend flows on their dedicated protocols: neither has static terminal
// counting semantics yet.
func (e *Engine) commitAcyclicTaskResult(ctx context.Context, lease *TaskLease, g *graph.Graph, result TaskResult) (CommitOutcome, error) {
	task := &lease.Task
	meta := g.NodeAt(task.NodeIdx)

	if result.Error != nil || (result.Output != nil && result.Output.Error != nil) {
		var businessErr *types.Error
		if result.Output != nil {
			businessErr = result.Output.Error
		}
		return e.commitAcyclicNodeError(ctx, lease, meta, result.Error, result.Output, businessErr)
	}

	if retryErr := outputPortRetryError(result.Output); retryErr != nil {
		retried, err := e.tryRetryWithAttempt(ctx, task, meta, retryErr, lease.Attempt, lease.LeaseToken)
		if err != nil {
			return CommitOutcomeTransientError, fmt.Errorf("retry node %q/%q: %w", task.ExecutionID, task.NodeName, err)
		}
		if retried {
			e.publishRetryReceipt(ctx, task, lease.Attempt)
			return CommitOutcomeAccepted, nil
		}
		// Retry budget exhausted: the explicit error-port output is a terminal
		// failure. Apply the node's OnError strategy rather than committing it
		// as a success on the error port.
		return e.commitAcyclicNodeError(ctx, lease, meta, retryErr, result.Output, nil)
	}

	data := make(map[string]any)
	if result.Output != nil && result.Output.Data != nil {
		data = result.Output.Data
	}
	// Loop/Split expansion has a separate sub-execution protocol and must not
	// be counted as an ordinary static DAG terminal node.
	if isLoopSplitOutput(data) {
		return CommitOutcomeTransientError, fmt.Errorf("atomic commit is not available for loop/split output from %q", task.NodeName)
	}

	port := "main"
	if result.Output != nil && result.Output.Port != "" {
		port = result.Output.Port
	}
	return e.commitAcyclicNode(ctx, lease, types.NodeStatusSuccess, data, port, "", false)
}

func (e *Engine) commitAcyclicNodeError(ctx context.Context, lease *TaskLease, meta graph.NodeMeta, systemErr error, output *types.Output, businessErr *types.Error) (CommitOutcome, error) {
	if retried, err := e.tryRetryWithAttempt(ctx, &lease.Task, meta, systemErr, lease.Attempt, lease.LeaseToken); err != nil {
		return CommitOutcomeTransientError, fmt.Errorf("retry node %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	} else if retried {
		e.publishRetryReceipt(ctx, &lease.Task, lease.Attempt)
		return CommitOutcomeAccepted, nil
	}

	outcome := ApplyOnError(meta.OnError, systemErr, businessErr, output)
	errorPort := outcome.RoutePort == "error" && businessErr == nil
	cls := buildEffectiveClassification(systemErr, businessErr, errorPort)
	return e.commitAcyclicNodeWithClassification(ctx, lease, outcome.NodeStatus, outcome.Output, outcome.RoutePort, outcome.ErrorMessage, outcome.ExecFatal, cls)
}

func (e *Engine) commitAcyclicNode(ctx context.Context, lease *TaskLease, status types.NodeStatus, output map[string]any, port, errMsg string, fatal bool) (CommitOutcome, error) {
	return e.commitAcyclicNodeWithClassification(ctx, lease, status, output, port, errMsg, fatal, EffectiveClassification{})
}

func (e *Engine) commitAcyclicNodeWithClassification(ctx context.Context, lease *TaskLease, status types.NodeStatus, output map[string]any, port, errMsg string, fatal bool, cls EffectiveClassification) (CommitOutcome, error) {
	task := &lease.Task
	var advanceTask *Task
	if !fatal {
		advanceTask = &Task{
			ExecutionID:  task.ExecutionID,
			NodeName:     task.NodeName,
			NodeIdx:      task.NodeIdx,
			UnitIdx:      task.UnitIdx,
			Type:         TaskTypeNodeAdvance,
			ActivationID: task.ActivationID,
			AutoDepth:    task.AutoDepth,
		}
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
		Fatal:        fatal,
		AdvanceTask:  advanceTask,
	}
	result, err := e.commitNode(ctx, req)
	if err != nil {
		return CommitOutcomeTransientError, fmt.Errorf("atomic commit node %q/%q: %w", task.ExecutionID, task.NodeName, err)
	}
	e.publishCommitReceipt(ctx, req, result, cls)
	return e.finishAtomicCommit(ctx, req, result)
}

// publishCommitReceipt publishes a read-only commit evidence event after the
// authoritative CommitNode mutation returned. Non-blocking; never changes the
// commit outcome. cls is the EffectiveClassification bound to this commit
// request (empty for non-error commits).
func (e *Engine) publishCommitReceipt(ctx context.Context, req CommitNodeRequest, result CommitNodeResult, cls EffectiveClassification) {
	if e.evidenceBuffer == nil {
		return
	}
	if cls.Source == "" {
		cls.Source = ErrorSourceUnclassified
	}
	publishRuntimeEvidence(e.evidenceBuffer, RuntimeEvidenceEvent{
		Version:       1,
		EventID:       newRuntimeEventID(),
		Type:          RuntimeEvidenceCommit,
		ExecutionID:   req.ExecutionID,
		NodeName:      req.NodeName,
		NodeIdx:       req.NodeIdx,
		ActivationID:  req.ActivationID,
		Attempt:       req.Attempt,
		CommitOutcome: result.Outcome,
		Applied:       result.Applied,
		OutboxIDs:     result.OutboxIDs,
		ErrorSource:   cls.Source,
		Classified:    cls.Classified,
		ErrorKind:     cls.Kind,
		Retryable:     cls.Retryable,
		Permanent:     cls.Permanent,
		ErrorCode:     cls.Code,
		NodeStatus:    req.Status,
		RoutePort:     req.Port,
	})
}

func (e *Engine) commitAcyclicFailure(ctx context.Context, lease *TaskLease, failure error) error {
	if failure == nil {
		failure = fmt.Errorf("task failed")
	}
	task := &lease.Task
	req := CommitNodeRequest{
		ExecutionID:  task.ExecutionID,
		NodeName:     task.NodeName,
		NodeIdx:      task.NodeIdx,
		ActivationID: task.ActivationID,
		AutoDepth:    task.AutoDepth,
		LeaseID:      lease.LeaseID,
		LeaseToken:   lease.LeaseToken,
		Attempt:      lease.Attempt,
		Status:       types.NodeStatusFailed,
		StoreOutput:  true,
		Error:        failure.Error(),
		Fatal:        true,
	}
	result, err := e.commitNode(ctx, req)
	if err != nil {
		return fmt.Errorf("atomic fail node %q/%q: %w", task.ExecutionID, task.NodeName, err)
	}
	e.publishCommitReceipt(ctx, req, result, EffectiveClassification{})
	outcome, err := e.finishAtomicCommit(ctx, req, result)
	if err != nil {
		return err
	}
	if outcome == CommitOutcomeStaleToken {
		return ErrInvalidLeaseToken
	}
	return nil
}

func (e *Engine) finishAtomicCommit(ctx context.Context, req CommitNodeRequest, result CommitNodeResult) (CommitOutcome, error) {
	switch result.Outcome {
	case CommitOutcomeAccepted, CommitOutcomeDuplicateTerminal:
		if err := e.afterAtomicCommit(ctx, req, result); err != nil {
			return CommitOutcomeTransientError, fmt.Errorf("deliver atomic commit outbox for %q/%q: %w", req.ExecutionID, req.NodeName, err)
		}
		return result.Outcome, nil
	case CommitOutcomeStaleToken:
		return result.Outcome, ErrInvalidLeaseToken
	case CommitOutcomeExecutionInactive:
		return result.Outcome, nil
	default:
		return CommitOutcomeTransientError, fmt.Errorf("atomic commit node %q/%q returned %q", req.ExecutionID, req.NodeName, result.Outcome)
	}
}

// tryRetryWithAttempt is the retry path used once a lease was already
// validated by the caller. It never rereads lease state and therefore cannot
// turn a backend read failure into a spurious attempt count.
func (e *Engine) tryRetryWithAttempt(ctx context.Context, task *Task, meta graph.NodeMeta, cause error, attempt int, token LeaseToken) (bool, error) {
	if cause == nil {
		return false, nil
	}
	settings := retryFor(meta)
	if settings == nil || types.IsPermanent(cause) || attempt >= settings.MaxAttempts {
		return false, nil
	}
	return e.scheduleRetry(ctx, task, attempt, settings, token)
}
