package engine

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// SignalOutcome classifies what Engine.DeliverSignalResult did with a signal.
type SignalOutcome string

const (
	// SignalOutcomeResumed means the signal woke a node that was suspended on
	// it, and a resume task was persisted for that node.
	SignalOutcomeResumed SignalOutcome = "resumed"
	// SignalOutcomeBuffered means the backend stored the signal without waking
	// anything and the execution is still live. The cause is not distinguished:
	// the target node is not suspended on that name yet (or never will be), a
	// multi-signal quorum is not complete, or the execution is still queued for a
	// concurrency slot. A node that later suspends on the name consumes the
	// stored signal; a name no node waits for is never consumed.
	SignalOutcomeBuffered SignalOutcome = "buffered"
	// SignalOutcomeInactive means the execution is finished or no longer exists,
	// so the signal reached no node and never will. Delivery treats this as a
	// benign no-op rather than an error.
	SignalOutcomeInactive SignalOutcome = "inactive"
)

// SignalDelivery is what DeliverSignalResult reports about one delivery.
type SignalDelivery struct {
	// Outcome is what happened to the signal. It is never empty when the call
	// returned a nil error.
	Outcome SignalOutcome
}

// DeliverSignal routes an external signal to the appropriate suspended node
// and enqueues a resume task if the node is ready. It reports only failure;
// use DeliverSignalResult to learn whether the signal woke anything.
func (e *Engine) DeliverSignal(ctx context.Context, id types.ExecutionID, name string, data map[string]any) error {
	_, err := e.deliverSignal(ctx, id, name, data, false)
	return err
}

// DeliverSignalResult delivers a signal like DeliverSignal and reports what
// became of it. When the backend supports DurableSignalDeliverer, the signal
// consumption and resume-task persistence happen in one atomic transition so a
// crash between them cannot lose the signal; otherwise the legacy two-step path
// is used.
//
// A nil error means the signal was accepted, whatever the outcome: a signal no
// node waits for yet is stored, not rejected. The outcome is the only thing
// that tells a delivery that woke a node from one that was stored or dropped.
//
// Telling buffered from inactive costs one execution status read after a
// delivery that woke nothing; DeliverSignal does not pay it. If that read fails
// the signal is reported as buffered, because the backend did store it, and the
// failure is logged. The returned SignalDelivery is the zero value when err is
// non-nil.
func (e *Engine) DeliverSignalResult(ctx context.Context, id types.ExecutionID, name string, data map[string]any) (SignalDelivery, error) {
	return e.deliverSignal(ctx, id, name, data, true)
}

// deliverSignal is the shared body of DeliverSignal and DeliverSignalResult.
// classify controls whether a stored signal is split into buffered and
// inactive, which needs a status read the plain DeliverSignal has no use for.
func (e *Engine) deliverSignal(ctx context.Context, id types.ExecutionID, name string, data map[string]any, classify bool) (delivery SignalDelivery, err error) {
	// The signal NAME is an attribute; the payload never is. A signal carries
	// caller business data (and, on some paths, credentials or tokens in it),
	// and spans are exported to a backend with a far broader audience than one
	// execution. durable/resumed say what happened to the signal without
	// describing what it contained.
	ctx, span := e.startExecutionSpan(ctx, id, "xflow.signal.deliver",
		"execution_id", string(id),
		"signal_name", name,
		"durable", false,
		"resumed", false,
	)
	defer span.End()
	defer func() {
		if err != nil {
			span.RecordError(err)
		}
	}()

	var outcome SignalOutcome
	if durable, ok := e.state.(DurableSignalDeliverer); ok {
		span.Set("durable", true)
		outcome, err = e.deliverSignalDurable(ctx, id, name, data, durable)
	} else {
		outcome, err = e.deliverSignalLegacy(ctx, id, name, data)
	}
	// Set before the error check: a resume that was persisted but whose flush
	// failed still resumed, and the span has always said so.
	span.Set("resumed", outcome == SignalOutcomeResumed)
	if err != nil {
		return SignalDelivery{}, err
	}
	if classify && outcome == SignalOutcomeBuffered {
		outcome = e.classifyStoredSignal(ctx, id, name)
	}
	return SignalDelivery{Outcome: outcome}, nil
}

// classifyStoredSignal decides whether a signal the backend stored without
// waking anything went to a live execution (buffered) or to one that is over
// (inactive). Neither backend reports this from the delivery itself: a
// delivery with no waiter stores the signal under the execution's key whether
// or not the execution still runs, so the status is read separately.
//
// The read comes after the delivery and is a snapshot: an execution that ends
// a moment later still reports buffered. That is the honest answer for the
// instant the signal was stored. A failed read does not fail the delivery,
// which already took effect; it reports buffered and logs the failure.
func (e *Engine) classifyStoredSignal(ctx context.Context, id types.ExecutionID, name string) SignalOutcome {
	active, err := e.executionActive(ctx, id)
	if err != nil {
		if e.logger != nil {
			e.logger.Warn("classify stored signal failed; reporting it as buffered",
				"execution_id", string(id), "signal_name", name, "err", err)
		}
		return SignalOutcomeBuffered
	}
	if !active {
		return SignalOutcomeInactive
	}
	return SignalOutcomeBuffered
}

// deliverSignalDurable peeks the resume target, resolves graph metadata, then
// atomically consumes the signal and persists the resume task in one outbox
// transition. A crash after consumption but before FlushOutbox leaves the
// resume intent durable; the outbox dispatcher redelivers it.
//
// The outcome is SignalOutcomeResumed when a resume intent was persisted (and,
// on the successful path, delivered). SignalOutcomeInactive is returned here
// only when a waiter was found on an execution that turned out to be over. A
// delivery that stored the signal, or has not reached its quorum, returns
// SignalOutcomeBuffered; DeliverSignalResult then separates that from a signal
// stored for an execution that is over.
func (e *Engine) deliverSignalDurable(ctx context.Context, id types.ExecutionID, name string, data map[string]any, durable DurableSignalDeliverer) (SignalOutcome, error) {
	resumeNode, err := durable.PeekResumeTarget(ctx, id, name)
	if err != nil {
		return "", err
	}

	var intent ResumeIntent
	if resumeNode != "" {
		g, active, err := e.loadActiveGraph(ctx, id)
		if err != nil {
			return "", fmt.Errorf("load graph for signal %q on %q: %w", name, id, err)
		}
		if !active {
			// The execution completed, was canceled, or was cleaned up while
			// the signal was in flight. Signal delivery is a control/user-facing
			// operation (unlike runner-facing lease/commit paths), so a terminal
			// target is a benign no-op — the signal simply has nowhere to resume.
			// Returning ErrExecutionInactive here would surface as an HTTP 500 on
			// the control plane's signal endpoint for an already-finished workflow.
			return SignalOutcomeInactive, nil
		}
		nodeIdx, ok := g.NodeIndex(resumeNode)
		if !ok {
			return "", fmt.Errorf("signal %q targeted unknown node %q", name, resumeNode)
		}
		unitIdx := g.UnitIndexForNode(nodeIdx)
		if unitIdx < 0 {
			return "", fmt.Errorf("cannot resume at %q: supply node has no schedulable unit", resumeNode)
		}
		// ActivationID is intentionally left zero: the durable backend reads the
		// live activation_id from node meta inside the atomic Lua transaction,
		// closing the TOCTOU window where a concurrent re-suspend under a new
		// activation could make the Go-side snapshot stale.
		intent = ResumeIntent{NodeName: resumeNode, NodeIdx: nodeIdx, UnitIdx: unitIdx}
	}

	node, _, committed, err := durable.DeliverSignalWithOutbox(ctx, id, name, data, intent)
	if err != nil {
		return "", err
	}

	if e.hooks != nil {
		safeHook(ctx, e.logger, func(hookCtx context.Context) {
			e.hooks.OnSignalDelivered(hookCtx, id, name, data)
		})
	}

	if !committed || node == "" {
		// Signal stored or multi-signal quorum not yet reached; no resume to deliver.
		return SignalOutcomeBuffered, nil
	}
	// Resume task is durably persisted in the outbox; flush delivers it now.
	// The intent survives a failed flush, so the resume is scheduled either way.
	return SignalOutcomeResumed, e.FlushOutbox(ctx, id)
}

// deliverSignalLegacy is the non-atomic two-step path used by backends that do
// not implement DurableSignalDeliverer (e.g. transient). A crash between signal
// consumption and enqueue can lose the resume — acceptable for non-durable
// backends whose state is already lost on crash.
//
// The outcome is SignalOutcomeResumed when a resume task was actually enqueued,
// SignalOutcomeInactive when a waiter was woken on an execution that turned out
// to be over, and SignalOutcomeBuffered when the signal was stored (target not
// yet suspended, or quorum not reached).
func (e *Engine) deliverSignalLegacy(ctx context.Context, id types.ExecutionID, name string, data map[string]any) (SignalOutcome, error) {
	resumeNode, payload, err := e.state.DeliverSignal(ctx, id, name, data)
	if err != nil {
		return "", err
	}

	if e.hooks != nil {
		safeHook(ctx, e.logger, func(hookCtx context.Context) {
			e.hooks.OnSignalDelivered(hookCtx, id, name, data)
		})
	}

	if resumeNode == "" {
		// Signal stored; node not yet suspended.
		return SignalOutcomeBuffered, nil
	}

	g, active, err := e.loadActiveGraph(ctx, id)
	if err != nil {
		return "", fmt.Errorf("load graph for signal %q on %q: %w", name, id, err)
	}
	if !active {
		// Terminal/cleaned-up target: benign no-op for the control/user-facing
		// signal path (see deliverSignalDurable for rationale). The signal was
		// already consumed above; the node is terminal so there is no resume to
		// enqueue.
		return SignalOutcomeInactive, nil
	}
	nodeIdx, ok := g.NodeIndex(resumeNode)
	if !ok {
		return "", fmt.Errorf("signal %q targeted unknown node %q", name, resumeNode)
	}
	activationID, err := e.currentActivationID(ctx, id, resumeNode)
	if err != nil {
		return "", fmt.Errorf("read signal target %q/%q: %w", id, resumeNode, err)
	}

	acquired, err := e.state.AcquireResumeLock(ctx, id, resumeNode)
	if err != nil {
		return "", err
	}
	if !acquired {
		// The lock is held by a concurrent resume (another signal or timer).
		// The signal was already consumed above; that holder will enqueue the
		// resume task. Return a retryable error so upper layers can surface the
		// contention rather than silently succeeding with no resume enqueued.
		return "", fmt.Errorf("resume lock contended for %q/%q: signal consumed but resume delegated to lock holder", id, resumeNode)
	}

	if payload == nil {
		payload = &types.SignalPayload{
			Triggered: types.SignalReceived,
			Name:      name,
			Data:      data,
		}
	}
	if err := e.queue.Enqueue(ctx, &Task{
		ExecutionID:  id,
		NodeName:     resumeNode,
		NodeIdx:      nodeIdx,
		UnitIdx:      g.UnitIndexForNode(nodeIdx),
		Type:         TaskTypeNodeResume,
		Payload:      payload,
		ActivationID: activationID,
		AutoDepth:    0,
	}); err != nil {
		return "", err
	}
	return SignalOutcomeResumed, nil
}

// TimeoutNode directly enqueues a resume task with TimeoutFired trigger for a
// suspended node. Unlike DeliverSignal, this bypasses signal name matching —
// used by the Timeout Monitor when a node's deadline expires.
func (e *Engine) TimeoutNode(ctx context.Context, id types.ExecutionID, nodeName string) error {
	g, active, err := e.loadActiveGraph(ctx, id)
	if err != nil {
		return fmt.Errorf("load graph for timeout on %q: %w", id, err)
	}
	if !active {
		return ErrExecutionInactive
	}
	nodeIdx, ok := g.NodeIndex(nodeName)
	if !ok {
		return fmt.Errorf("timeout targeted unknown node %q", nodeName)
	}
	activationID, err := e.currentActivationID(ctx, id, nodeName)
	if err != nil {
		return fmt.Errorf("read timeout target %q/%q: %w", id, nodeName, err)
	}

	acquired, err := e.state.AcquireResumeLock(ctx, id, nodeName)
	if err != nil {
		return err
	}
	if !acquired {
		// Another resume path (signal delivery or concurrent timeout) already
		// holds the lock and will enqueue the resume task. Return a retryable
		// error to surface the contention.
		return fmt.Errorf("resume lock contended for timeout %q/%q: delegated to lock holder", id, nodeName)
	}

	return e.queue.Enqueue(ctx, &Task{
		ExecutionID: id,
		NodeName:    nodeName,
		NodeIdx:     nodeIdx,
		UnitIdx:     g.UnitIndexForNode(nodeIdx),
		Type:        TaskTypeNodeResume,
		Payload: &types.SignalPayload{
			Triggered: types.TimeoutFired,
			Name:      "_timeout",
		},
		ActivationID: activationID,
		AutoDepth:    0,
	})
}

// RevokeSignal atomically revokes a previously delivered signal that has not
// yet been consumed by a suspended node. Returns ErrSignalConsumed if the
// signal was already consumed or does not exist.
func (e *Engine) RevokeSignal(ctx context.Context, id types.ExecutionID, signalName string) error {
	revoked, err := e.state.RevokeSignal(ctx, id, signalName)
	if err != nil {
		return err
	}
	if !revoked {
		return ErrSignalConsumed
	}
	if e.hooks != nil {
		safeHook(ctx, e.logger, func(ctx context.Context) {
			e.hooks.OnSignalRevoked(ctx, id, signalName)
		})
	}
	return nil
}

func (e *Engine) currentActivationID(ctx context.Context, id types.ExecutionID, nodeName string) (int, error) {
	ns, err := e.state.GetNode(ctx, id, nodeName)
	if err != nil {
		return 0, fmt.Errorf("get node %q/%q: %w", id, nodeName, err)
	}
	if ns == nil {
		return 0, fmt.Errorf("%w: node %q/%q not found", ErrExecutionInactive, id, nodeName)
	}
	return ns.ActivationID, nil
}

func cloneSignalPayload(payload *types.SignalPayload) *types.SignalPayload {
	if payload == nil {
		return nil
	}
	cp := *payload
	cp.Data = cloneMap(payload.Data)
	if payload.All != nil {
		cp.All = make(map[string]map[string]any, len(payload.All))
		for name, data := range payload.All {
			cp.All[name] = cloneMap(data)
		}
	}
	return &cp
}

// resolveResumeIntent resolves a node name into a ResumeIntent, rejecting supply
// nodes which have no schedulable unit (UnitIndexForNode returns -1).
func resolveResumeIntent(g *graph.Graph, nodeName string) (ResumeIntent, error) {
	nodeIdx, ok := g.NodeIndex(nodeName)
	if !ok {
		return ResumeIntent{}, fmt.Errorf("cannot resume at %q: unknown node", nodeName)
	}
	unitIdx := g.UnitIndexForNode(nodeIdx)
	if unitIdx < 0 {
		return ResumeIntent{}, fmt.Errorf("cannot resume at %q: supply node has no schedulable unit", nodeName)
	}
	return ResumeIntent{NodeName: nodeName, NodeIdx: nodeIdx, UnitIdx: unitIdx}, nil
}
