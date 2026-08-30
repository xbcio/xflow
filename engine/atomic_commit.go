package engine

import (
	"context"
	"errors"
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
	// Expansion has a separate sub-execution protocol and must not be counted as
	// an ordinary static DAG terminal node. CommitTaskResultWithOutcome routes
	// an expanding success away from this path before it gets here, so this is a
	// backstop against a future caller reaching commitAcyclicTaskResult
	// directly — not a second, independent decision about what expands.
	if expandsIntoSubExecutions(g, task.NodeIdx) {
		return CommitOutcomeTransientError, fmt.Errorf("atomic commit is not available for the expansion output of %q", task.NodeName)
	}

	port := "main"
	if result.Output != nil && result.Output.Port != "" {
		port = result.Output.Port
	}
	return e.commitAcyclicNode(ctx, lease, types.NodeStatusSuccess, data, port, "", false)
}

func (e *Engine) commitAcyclicNodeError(ctx context.Context, lease *TaskLease, meta graph.NodeMeta, systemErr error, output *types.Output, businessErr *types.Error) (CommitOutcome, error) {
	return e.commitNodeErrorOutcome(ctx, lease, meta, systemErr, output, businessErr, e.commitAcyclicNodeWithClassification)
}

// nodeErrorCommitFunc is the shape of the terminal-transition step that
// commitNodeErrorOutcome hands its classified result to: commitAcyclicNodeWithClassification
// on the acyclic path, commitLegacyNodeWithClassification on the cyclic/legacy
// path. The two committers differ (atomic commit vs. the legacy fenced
// scheduler), which is exactly why this is a parameter rather than a third
// copy of the function it varies.
type nodeErrorCommitFunc func(ctx context.Context, lease *TaskLease, status types.NodeStatus, output map[string]any, port, errMsg string, fatal bool, cls EffectiveClassification) (CommitOutcome, error)

// commitNodeErrorOutcome is the failure-handling pipeline shared by
// commitAcyclicNodeError and commitLegacyNodeError: retry → publishRetryReceipt
// → ApplyOnError → buildEffectiveClassification → hand off to the caller's
// terminal committer. engine/expansion_criterion_test.go documents that the
// acyclic and legacy paths must be convergent here — a failure routed through
// commitLegacyTaskResult and one routed through commitAcyclicTaskResult must
// run the identical retry/OnError/classification sequence — so this function,
// not two independently maintained copies of it, is what keeps that true.
func (e *Engine) commitNodeErrorOutcome(ctx context.Context, lease *TaskLease, meta graph.NodeMeta, systemErr error, output *types.Output, businessErr *types.Error, commit nodeErrorCommitFunc) (CommitOutcome, error) {
	if retried, err := e.tryRetryWithAttempt(ctx, &lease.Task, meta, systemErr, lease.Attempt, lease.LeaseToken); err != nil {
		return CommitOutcomeTransientError, fmt.Errorf("retry node %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	} else if retried {
		e.publishRetryReceipt(ctx, &lease.Task, lease.Attempt)
		return CommitOutcomeAccepted, nil
	}

	outcome := ApplyOnError(meta.OnError, systemErr, businessErr, output)
	errorPort := outcome.RoutePort == "error" && businessErr == nil
	cls := buildEffectiveClassification(systemErr, businessErr, errorPort)
	return commit(ctx, lease, outcome.NodeStatus, outcome.Output, outcome.RoutePort, outcome.ErrorMessage, outcome.ExecFatal, cls)
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
			// Carried so the advance branch does not read the node back to
			// learn the port we are about to write in the same commit.
			Port: &port,
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
	if fatal && status == types.NodeStatusFailed && e.nodeFailureObserver != nil {
		e.nodeFailureObserver.ObserveNodeFailure(task.ExecutionID, ObservedNodeFailure{
			NodeName: task.NodeName,
			Err:      errors.New(errMsg),
			Attempt:  lease.Attempt,
			Fatal:    true,
			// Taken from cls, not from the error handed to the observer: that
			// error is rebuilt from the committed MESSAGE, so both the
			// ErrPermanent sentinel and the *ClassifiedError type are already
			// gone by this line. cls was derived one frame up from the original
			// error (buildEffectiveClassification), which is the last place the
			// classification still exists.
			Permanent: cls.Permanent != nil && *cls.Permanent,
		})
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
