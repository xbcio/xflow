package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/xbcio/xflow/types"
)

// ExecutionInactiveKind classifies why an execution was found inactive. It is
// the difference between a benign late delivery and lost work, and the
// dispatch sink (service/control.Dispatcher) is its only consumer.
type ExecutionInactiveKind string

const (
	// ExecutionInactiveGone means no live state exists for the execution and
	// nothing says it ever reached a terminal state, and that absence is
	// conclusive enough to call the delivery lost work: either the backend
	// keeps state forever (so absence really is absence), or the delivery's
	// measured age — from the durable intent's creation, or its deliverable
	// stamp when it carries no creation time — is no longer than the backend's
	// evidence window (so a terminal transition during that span, had there
	// been one, would still be readable).
	//
	// It is also what a task for an execution that never existed at all (wrong
	// namespace, synthetic payload) looks like, and the torn case where the
	// status key still says the execution is live while the graph key is
	// missing (routing is broken on a demonstrably live execution; that shape
	// is not a timing question, so the window does not apply).
	//
	// The provability qualifier is the whole point: a "no evidence" verdict
	// alone does not distinguish an execution that expired under its queued
	// work from one that finished long ago whose terminal marker has itself
	// aged out. The latter only becomes possible once the wait exceeds the
	// evidence window, and such deliveries are Unattributed instead. See
	// ExecutionRetentionReader for the window and ExecutionTerminalReader for
	// the marker that keeps the benign case out of this bucket entirely.
	ExecutionInactiveGone ExecutionInactiveKind = "gone"

	// ExecutionInactiveTerminal means the execution's recorded state exists
	// and is terminal (success, failed, canceled, timeout), or a terminal
	// marker left by a terminal transition outlived the status record. This is
	// a late or duplicate delivery for an execution the control plane already
	// finished; dropping it is correct and must not be alerted on.
	ExecutionInactiveTerminal ExecutionInactiveKind = "terminal"

	// ExecutionInactiveUnattributed means no live state and no terminal
	// evidence was found, but the absence is not conclusive: the delivery's
	// measured age exceeded the backend's evidence window, it carries no stamp
	// to measure that age, or the backend could not confirm the retention its
	// writes actually used.
	//
	// Beyond the window, a task for an execution that finished long ago is
	// indistinguishable from a task for one that expired under its queued
	// work, and a backlog longer than the retention produces both at once. The
	// verdict says "cannot attribute this drop", not "work was lost" and not
	// "this was benign"; a delivery layer must count it separately and must
	// not page on it as confirmed loss. See ExecutionRetentionReader.
	ExecutionInactiveUnattributed ExecutionInactiveKind = "unattributed"
)

// ExecutionInactiveError reports why TaskRouting found an execution inactive.
//
// It unwraps to ErrExecutionInactive so every existing
// errors.Is(err, ErrExecutionInactive) call site (core.go's claim handling,
// group_control_loop.go, apiserver/module_control.go, execution/dispatcher.go,
// and the engine's own tests) keeps its exact current behavior. Callers that
// need the classification — a delivery layer deciding whether a dropped task
// is lost work or a benign late arrival — opt in with errors.As.
type ExecutionInactiveError struct {
	ExecutionID types.ExecutionID
	Kind        ExecutionInactiveKind
	// Status is the status observed for the execution: the terminal status for
	// Kind=terminal, or the surviving non-terminal status in the torn case
	// where the graph key is missing while the status key still lives. It is
	// empty when nothing was found.
	Status types.ExecutionStatus
}

func (e *ExecutionInactiveError) Error() string {
	switch {
	case e == nil:
		return ErrExecutionInactive.Error()
	case e.Kind == ExecutionInactiveTerminal:
		return fmt.Sprintf("execution %q inactive: terminal (%s)", e.ExecutionID, e.Status)
	case e.Kind == ExecutionInactiveUnattributed:
		return fmt.Sprintf("execution %q inactive: unattributed (no evidence, delivery wait not provably inside the retention window)", e.ExecutionID)
	case e.Status != "":
		return fmt.Sprintf("execution %q inactive: gone (last status %s)", e.ExecutionID, e.Status)
	default:
		return fmt.Sprintf("execution %q inactive: gone (no state)", e.ExecutionID)
	}
}

// Unwrap keeps the coarse sentinel contract: every existing errors.Is(err,
// ErrExecutionInactive) caller keeps working unchanged.
func (e *ExecutionInactiveError) Unwrap() error { return ErrExecutionInactive }

// ExecutionInactiveClassifyError reports that the activeness verdict was
// inactive but the state read that would classify it failed. It deliberately
// does NOT unwrap to ErrExecutionInactive: a store read error is not evidence
// that the execution is gone, and folding it into the sentinel would make
// every existing inactive-handling caller treat an outage as "work is over"
// and drop claims it should retry.
//
// The delivery layer counts it separately (reason=classify_error) and returns
// it unmarked, so a retry-capable transport retries it. See the mode note on
// Dispatcher.HandleTask for what a fire-and-forget transport does with it.
type ExecutionInactiveClassifyError struct {
	ExecutionID types.ExecutionID
	Err         error
}

func (e *ExecutionInactiveClassifyError) Error() string {
	if e == nil {
		return "classify inactive execution: unknown error"
	}
	return fmt.Sprintf("classify inactive execution %q: %v", e.ExecutionID, e.Err)
}

func (e *ExecutionInactiveClassifyError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// executionStatus answers loadActiveGraph's activeness question in full: the
// status and whether anything was found. It is the reusable core executionActive
// was extracted from; executionActive keeps the ExecutionStatusReader
// preference so the hot path pays the same single narrow read it always did.
func (e *Engine) executionStatus(ctx context.Context, id types.ExecutionID) (types.ExecutionStatus, bool, error) {
	if reader, ok := e.state.(ExecutionStatusReader); ok {
		return reader.GetExecutionStatus(ctx, id)
	}
	snap, err := e.state.GetExecution(ctx, id)
	if err != nil {
		return "", false, err
	}
	if snap == nil {
		return "", false, nil
	}
	return snap.Status, true, nil
}

// inactiveExecutionError classifies an inactive verdict the caller already has
// (loadActiveGraph returned !active with no error). It is only called on the
// inactive path — healthy routing pays nothing for it. Read cost on that path:
// the status read, the terminal-marker read when the status is absent, and the
// retention read when both are absent. loadActiveGraph does not hand any of
// them over: on its cache-hit branch and on a cache miss that still loads a
// graph it already asked its own status question, but that read answers only
// "non-terminal?" and its value is discarded, so the classifier's status read
// is a second narrow GET on exactly the common inactive shapes. It is kept
// self-contained deliberately — the routing path has no partial verdict to
// thread through, and this path is the rare one.
//
// Classification order matters. A live terminal status is authoritative. When
// no status exists, the terminal marker decides: it is written by every
// terminal transition with the execution's active retention, so it outlives
// the status key exactly far enough to tell "finished a while ago, this is a
// late duplicate" from "expired under its queued work".
//
// When neither exists, the verdict is split by provability, because "no
// evidence" alone is not a loss signal at the scale this classification
// exists for. The marker has a finite lifetime (the active retention), and a
// backlog can exceed it: an execution that finished two hours ago leaves no
// trace once a one-retention marker expires, and its late duplicate then looks
// exactly like an execution that expired under its queued work. So:
//
//   - the backend's retention window is read (ExecutionRetentionReader);
//     backends that never expire state have no window and absence is
//     conclusive;
//   - a backend that cannot confirm the retention its writes used forces
//     Unattributed outright (ExecutionRetentionUnknown): no age can be
//     compared against a window that is not provable, and guessing the
//     fallback is how a benign late duplicate becomes a reported loss;
//   - a delivery whose measured age is within that window is Gone — had the
//     execution terminalized after the intent was created, the marker would
//     still be readable, so its absence means the execution did not finish.
//     Age is measured from the durable intent's creation (IntentCreatedAt),
//     falling back to the deliverable stamp for payloads that carry no
//     creation time: a delayed intent's availability can postdate the very
//     terminal transition it should have observed, so availability cannot
//     bound the window;
//   - a delivery whose age passed the window, or that carries no stamp to
//     measure the age, is Unattributed. It is neither provable loss nor
//     provable health, and the count must be read as "cannot tell", not as
//     loss.
//
// A read error while classifying is NOT folded into the sentinel (the D1
// draft did that and it silently acked real losses during a Redis blip): it
// becomes an ExecutionInactiveClassifyError, which existing callers treat as
// an outstanding error, not as "inactive".
func (e *Engine) inactiveExecutionError(ctx context.Context, id types.ExecutionID, ageAnchor time.Time) error {
	status, found, err := e.executionStatus(ctx, id)
	if err != nil {
		return &ExecutionInactiveClassifyError{ExecutionID: id, Err: err}
	}
	if found {
		if types.IsTerminalExecutionStatus(status) {
			return &ExecutionInactiveError{ExecutionID: id, Kind: ExecutionInactiveTerminal, Status: status}
		}
		// The status says the execution is live but routing still refused:
		// loadActiveGraph's cache-miss branch returns inactive on a nil graph
		// without consulting the status, so this is the torn case where the
		// graph key is missing while the status key survives. The work cannot
		// be routed, so it is counted as gone, with the surviving status
		// attached for diagnosis (it cannot be a metric label). This is not a
		// timing question — positive evidence says the execution was live and
		// the route is broken — so the retention window does not apply.
		return &ExecutionInactiveError{ExecutionID: id, Kind: ExecutionInactiveGone, Status: status}
	}
	if reader, ok := e.state.(ExecutionTerminalReader); ok {
		terminalStatus, terminalFound, terminalErr := reader.GetExecutionTerminalStatus(ctx, id)
		if terminalErr != nil {
			return &ExecutionInactiveClassifyError{ExecutionID: id, Err: terminalErr}
		}
		if terminalFound {
			return &ExecutionInactiveError{ExecutionID: id, Kind: ExecutionInactiveTerminal, Status: terminalStatus}
		}
	}
	window, known, err := e.executionEvidenceWindow(ctx, id)
	if err != nil {
		return &ExecutionInactiveClassifyError{ExecutionID: id, Err: err}
	}
	if !known {
		// The backend cannot say how long this execution's own evidence could
		// have lived, so "no evidence" is not attributable at any age: the
		// fallback could exceed the retention the terminal writes actually
		// used, and comparing against it would report a benign late delivery
		// as lost work. Under-reporting Gone is the deliberate trade.
		return &ExecutionInactiveError{ExecutionID: id, Kind: ExecutionInactiveUnattributed}
	}
	if window > 0 {
		if ageAnchor.IsZero() {
			return &ExecutionInactiveError{ExecutionID: id, Kind: ExecutionInactiveUnattributed}
		}
		if time.Since(ageAnchor) > window {
			return &ExecutionInactiveError{ExecutionID: id, Kind: ExecutionInactiveUnattributed}
		}
	}
	return &ExecutionInactiveError{ExecutionID: id, Kind: ExecutionInactiveGone}
}

// provabilityAnchor returns the instant a delivery's age is measured from for
// the provability test in inactiveExecutionError: the durable intent's
// creation when the task carries it, otherwise the deliverable stamp (which
// for every non-delayed intent is the same creation instant, and for a legacy
// payload is the only age evidence that exists).
//
// IntentCreatedAt wins for delayed intents because their two stamps diverge:
// DeliverableAt is the availability instant, which can be hours after the
// intent was created. A terminal marker lives one retention from the terminal
// transition, and the transition cannot precede the intent's creation, so
// only the creation instant bounds the no-evidence verdict soundly; measuring
// from a future availability would report the wait as roughly zero and
// misreport a benign cancelled execution's late wakeup as gone.
func (t *Task) provabilityAnchor() time.Time {
	if t == nil {
		return time.Time{}
	}
	if !t.IntentCreatedAt.IsZero() {
		return t.IntentCreatedAt
	}
	return t.DeliverableAt
}

// executionEvidenceWindow resolves the backend's evidence window for the
// execution into the three states the classifier needs: a positive window to
// compare the delivery's age against, zero when the backend bounds nothing
// (an in-memory store, or a backend that opted out of the optional
// interface), and known=false when the backend cannot confirm the retention
// its writes used (ExecutionRetentionUnknown). Zero keeps the classifier's
// verdict Gone exactly as it was before the window existed; unknown forces
// Unattributed because there is no provable window to test against.
func (e *Engine) executionEvidenceWindow(ctx context.Context, id types.ExecutionID) (window time.Duration, known bool, err error) {
	reader, ok := e.state.(ExecutionRetentionReader)
	if !ok {
		return 0, true, nil
	}
	window, err = reader.GetExecutionRetention(ctx, id)
	if err != nil {
		return 0, false, err
	}
	if window < 0 {
		return 0, false, nil
	}
	return window, true, nil
}
