package xflow

import (
	"errors"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/service/apiserver"
)

// IsRetryableRegistrationError reports whether a failure from AddWorkflow or
// ReplaceWorkflow is one the same call can clear on a retry.
//
// Neither call retries on its own (see ReplaceWorkflow for the contract), which
// leaves a host that wants to recover from a contended or briefly unreachable
// backend to decide what is worth retrying. The answer is not "everything": a
// definition the compiler rejects, a key another definition occupies, and a
// registry that cannot replace atomically return the same answer however many
// times they are asked. This predicate separates those from the failures that
// change with time — a context deadline, a dropped connection, an indeterminate
// mutation, a rival writer's stale-revision conflict.
//
// It is deliberately biased the way the SDK's own contract is: an unrecognised
// error is retryable. Giving up on a recoverable backend failure produces the
// half-dead process that contract exists to prevent, while retrying a permanent
// one costs a log line and a backed-off attempt.
//
// The classes:
//
//   - NOT retryable, because only the definition or the call can change the
//     answer: a *apiserver.WorkflowCompileError; the key conflict AddWorkflow
//     answers a different definition with (backend.ErrWorkflowConflict); a
//     registry that does not support atomic replacement
//     (backend.ErrWorkflowReplaceUnsupported); and the definition problems
//     AddWorkflow and ReplaceWorkflow refuse themselves — a nil workflow, a
//     workflow carrying local node handlers, a workflow that does not build.
//   - Retryable: everything else, including context.DeadlineExceeded from a
//     registration that timed out against a contended backend, any transport or
//     backend error, an indeterminate mutation, and a
//     *backend.WorkflowReplaceConflictError, which names a racing writer that a
//     re-read and a retry resolves.
//
// The last case is why this function tests the typed replace conflict before
// the bare sentinel: the typed error unwraps to ErrWorkflowConflict, and the
// two need opposite answers.
//
// Registration failures are also counted in
// xflow_workflow_registration_total{operation,outcome} on the same taxonomy, so
// a host can alert on the non-retryable classes without logging them itself.
func IsRetryableRegistrationError(err error) bool {
	if err == nil {
		return false
	}
	var compileErr *apiserver.WorkflowCompileError
	if errors.As(err, &compileErr) {
		return false
	}
	var refused definitionRefused
	if errors.As(err, &refused) {
		return false
	}
	var replaceConflict *backend.WorkflowReplaceConflictError
	if errors.As(err, &replaceConflict) {
		return true
	}
	if errors.Is(err, backend.ErrWorkflowConflict) {
		return false
	}
	if errors.Is(err, backend.ErrWorkflowReplaceUnsupported) {
		return false
	}
	return true
}

// definitionRefused marks the failures AddWorkflow and ReplaceWorkflow raise
// before anything reaches the registry: the definition is not one this Server
// can register, so presenting the same one again cannot change the answer.
// Error and Unwrap delegate verbatim, so the message a caller already logs and
// any cause it already matched are unchanged.
type definitionRefused struct{ err error }

func (e definitionRefused) Error() string { return e.err.Error() }
func (e definitionRefused) Unwrap() error { return e.err }
