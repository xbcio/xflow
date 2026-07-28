package engine

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/types"
)

// DeliverSignal routes an external signal to the appropriate suspended node
// and enqueues a resume task if the node is ready. When the backend supports
// DurableSignalDeliverer, the signal consumption and resume-task persistence
// happen in one atomic transition so a crash between them cannot lose the
// signal; otherwise the legacy two-step path is used.
func (e *Engine) DeliverSignal(ctx context.Context, id types.ExecutionID, name string, data map[string]any) error {
	if durable, ok := e.state.(DurableSignalDeliverer); ok {
		return e.deliverSignalDurable(ctx, id, name, data, durable)
	}
	return e.deliverSignalLegacy(ctx, id, name, data)
}

// deliverSignalDurable peeks the resume target, resolves graph metadata, then
// atomically consumes the signal and persists the resume task in one outbox
// transition. A crash after consumption but before FlushOutbox leaves the
// resume intent durable; the outbox dispatcher redelivers it.
func (e *Engine) deliverSignalDurable(ctx context.Context, id types.ExecutionID, name string, data map[string]any, durable DurableSignalDeliverer) error {
	resumeNode, err := durable.PeekResumeTarget(ctx, id, name)
	if err != nil {
		return err
	}

	var intent ResumeIntent
	if resumeNode != "" {
		g, active, err := e.loadActiveGraph(ctx, id)
		if err != nil {
			return fmt.Errorf("load graph for signal %q on %q: %w", name, id, err)
		}
		if !active {
			// The execution completed, was canceled, or was cleaned up while
			// the signal was in flight. Signal delivery is a control/user-facing
			// operation (unlike runner-facing lease/commit paths), so a terminal
			// target is a benign no-op — the signal simply has nowhere to resume.
			// Returning ErrExecutionInactive here would surface as an HTTP 500 on
			// the control plane's signal endpoint for an already-finished workflow.
			return nil
		}
		nodeIdx, ok := g.NodeIndex(resumeNode)
		if !ok {
			return fmt.Errorf("signal %q targeted unknown node %q", name, resumeNode)
		}
		// ActivationID is intentionally left zero: the durable backend reads the
		// live activation_id from node meta inside the atomic Lua transaction,
		// closing the TOCTOU window where a concurrent re-suspend under a new
		// activation could make the Go-side snapshot stale.
		intent = ResumeIntent{NodeName: resumeNode, NodeIdx: nodeIdx, UnitIdx: g.UnitIndexForNode(nodeIdx)}
	}

	node, _, committed, err := durable.DeliverSignalWithOutbox(ctx, id, name, data, intent)
	if err != nil {
		return err
	}

	if e.hooks != nil {
		safeHook(ctx, e.logger, func(hookCtx context.Context) {
			e.hooks.OnSignalDelivered(hookCtx, id, name, data)
		})
	}

	if !committed || node == "" {
		// Signal stored or multi-signal quorum not yet reached; no resume to deliver.
		return nil
	}
	// Resume task is durably persisted in the outbox; flush delivers it now.
	return e.FlushOutbox(ctx, id)
}

// deliverSignalLegacy is the non-atomic two-step path used by backends that do
// not implement DurableSignalDeliverer (e.g. transient). A crash between signal
// consumption and enqueue can lose the resume — acceptable for non-durable
// backends whose state is already lost on crash.
func (e *Engine) deliverSignalLegacy(ctx context.Context, id types.ExecutionID, name string, data map[string]any) error {
	resumeNode, payload, err := e.state.DeliverSignal(ctx, id, name, data)
	if err != nil {
		return err
	}

	if e.hooks != nil {
		safeHook(ctx, e.logger, func(hookCtx context.Context) {
			e.hooks.OnSignalDelivered(hookCtx, id, name, data)
		})
	}

	if resumeNode == "" {
		// Signal stored; node not yet suspended.
		return nil
	}

	g, active, err := e.loadActiveGraph(ctx, id)
	if err != nil {
		return fmt.Errorf("load graph for signal %q on %q: %w", name, id, err)
	}
	if !active {
		// Terminal/cleaned-up target: benign no-op for the control/user-facing
		// signal path (see deliverSignalDurable for rationale). The signal was
		// already consumed above; the node is terminal so there is no resume to
		// enqueue.
		return nil
	}
	nodeIdx, ok := g.NodeIndex(resumeNode)
	if !ok {
		return fmt.Errorf("signal %q targeted unknown node %q", name, resumeNode)
	}
	activationID, err := e.currentActivationID(ctx, id, resumeNode)
	if err != nil {
		return fmt.Errorf("read signal target %q/%q: %w", id, resumeNode, err)
	}

	acquired, err := e.state.AcquireResumeLock(ctx, id, resumeNode)
	if err != nil {
		return err
	}
	if !acquired {
		// The lock is held by a concurrent resume (another signal or timer).
		// The signal was already consumed above; that holder will enqueue the
		// resume task. Return a retryable error so upper layers can surface the
		// contention rather than silently succeeding with no resume enqueued.
		return fmt.Errorf("resume lock contended for %q/%q: signal consumed but resume delegated to lock holder", id, resumeNode)
	}

	if payload == nil {
		payload = &types.SignalPayload{
			Triggered: types.SignalReceived,
			Name:      name,
			Data:      data,
		}
	}
	return e.queue.Enqueue(ctx, &Task{
		ExecutionID:  id,
		NodeName:     resumeNode,
		NodeIdx:      nodeIdx,
		UnitIdx:      g.UnitIndexForNode(nodeIdx),
		Type:         TaskTypeNodeResume,
		Payload:      payload,
		ActivationID: activationID,
		AutoDepth:    0,
	})
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
