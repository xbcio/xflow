package xflow

import (
	"fmt"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// registerNodeDefinitions registers node/trigger handlers declared via
// WithNodes. Used by both local and cluster: local needs it so LocalNode-free
// typed nodes resolve in-process; cluster workers need it to resolve types
// for workflows submitted by other processes.
func (e *Engine) registerNodeDefinitions(defs []types.Handler) error {
	if len(defs) == 0 {
		return nil
	}
	lr, ok := e.registry.(engine.HandlerRegistrar)
	for _, def := range defs {
		if def == nil {
			continue
		}
		switch h := def.(type) {
		case types.ActionHandler:
			if !ok {
				return fmt.Errorf("registry does not support node definition registration")
			}
			lr.RegisterGlobal(h.Descriptor().Type, h)
			e.globalHandlers[h.Descriptor().Type] = h
			if th, isTrigger := def.(types.TriggerHandler); isTrigger {
				registry.RegisterTrigger(th)
			}
		case types.TriggerHandler:
			registry.RegisterTrigger(h)
		default:
			return fmt.Errorf("node definition %q is not an action or trigger handler", def.Descriptor().Type)
		}
	}
	return nil
}

// registerWorkflowHandlersTracked registers portable typed node/trigger
// handlers declared inline on a WorkflowBuilder (via Node). It returns a
// rollback func that restores the process registry's prior action-handler state
// for every node type it (over)wrote, so a later failure in AddWorkflow leaves
// no handler pollution behind.
//
// Trigger handlers registered via the process-global node registry
// (registry.RegisterTrigger) cannot be unregistered — that API is append-only —
// so they are not rolled back; re-registering the identical handler on a retry
// is idempotent. Caller must hold e.mu, and the returned rollback must also run
// under e.mu.
func (e *Engine) registerWorkflowHandlersTracked(wf *WorkflowBuilder) (func(), error) {
	noop := func() {}
	if wf == nil {
		return noop, nil
	}
	actions := wf.workflowHandlers()
	triggers := wf.workflowTriggerHandlers()
	if len(actions) == 0 && len(triggers) == 0 {
		return noop, nil
	}
	lr, ok := e.registry.(engine.HandlerRegistrar)
	if !ok {
		return nil, fmt.Errorf("registry does not support workflow handler registration")
	}
	type prevGlobal struct {
		h       types.ActionHandler
		existed bool
	}
	saved := make(map[string]prevGlobal, len(actions))
	for nodeType, h := range actions {
		old, existed := e.globalHandlers[nodeType]
		saved[nodeType] = prevGlobal{h: old, existed: existed}
		e.globalHandlers[nodeType] = h
		lr.RegisterGlobal(nodeType, h)
	}
	for _, h := range triggers {
		registry.RegisterTrigger(h)
	}
	rollback := func() {
		for nodeType, p := range saved {
			if p.existed {
				e.globalHandlers[nodeType] = p.h
				lr.RegisterGlobal(nodeType, p.h)
			} else {
				// The underlying registry cannot unregister a node type; drop the
				// mirror entry so bookkeeping stays accurate. The orphaned handler
				// is inert (no persisted workflow references it) and is idempotently
				// overwritten on a later retry.
				delete(e.globalHandlers, nodeType)
			}
		}
	}
	return rollback, nil
}

// registerDirectHandlersTracked registers LocalNode direct handlers from a
// WorkflowBuilder and returns a rollback func that restores directHandlerNames
// and the process registry's prior node-name handler for every name it wrote.
// Local-mode only: cluster rejects any workflow using direct handlers because
// Go function values cannot be serialized or executed by another process.
// Caller must hold e.mu; the returned rollback must also run under e.mu.
func (e *Engine) registerDirectHandlersTracked(wf *WorkflowBuilder) (func(), error) {
	noop := func() {}
	if len(wf.directHandlers()) == 0 {
		return noop, nil
	}
	lr, ok := e.registry.(engine.HandlerRegistrar)
	if !cfgAllowsDirectHandlers(e) || !ok {
		names := make([]string, 0, len(wf.directHandlers()))
		for n := range wf.directHandlers() {
			names = append(names, n)
		}
		return nil, fmt.Errorf("nodes %v use direct action handlers (local mode only); with cluster, define custom nodes with node.Define and register consumer capabilities with xflow.WithNodes", names)
	}
	wfName := wf.name
	if wfName == "" {
		wfName = "<unnamed>"
	}
	type prevDirect struct {
		owner        string
		ownerExisted bool
		handler      types.ActionHandler
		handlerSet   bool
	}
	saved := make(map[string]prevDirect, len(wf.directHandlers()))
	for nodeName, h := range wf.directHandlers() {
		if _, recorded := saved[nodeName]; !recorded {
			owner, ownerExisted := e.directHandlerNames[nodeName]
			prevH, handlerSet := e.directHandlers[nodeName]
			saved[nodeName] = prevDirect{owner: owner, ownerExisted: ownerExisted, handler: prevH, handlerSet: handlerSet}
		}
		// LocalNode handlers are stored in a process-global map keyed by node
		// name (execution.Registry.nodeHandlers). Two workflows that declare a
		// LocalNode with the same name therefore collide and the later
		// registration silently shadows the earlier one. Surface it as a
		// warning rather than leaving it to fail at runtime.
		//
		// Re-registering the same node name from the same workflow (e.g. an
		// idempotent AddWorkflow retry) is not a shadow: skip the warning.
		if existing, exists := e.directHandlerNames[nodeName]; exists && existing != wfName {
			msg := fmt.Sprintf("direct handler name %q is already registered by workflow %q; the new registration from workflow %q will shadow it", nodeName, existing, wfName)
			if e.logger != nil {
				e.logger.Warn(msg)
			}
		}
		e.directHandlerNames[nodeName] = wfName
		e.directHandlers[nodeName] = h
		lr.RegisterNodeHandler(nodeName, h)
	}
	rollback := func() {
		for nodeName, p := range saved {
			if p.ownerExisted {
				e.directHandlerNames[nodeName] = p.owner
			} else {
				delete(e.directHandlerNames, nodeName)
			}
			if p.handlerSet {
				e.directHandlers[nodeName] = p.handler
				lr.RegisterNodeHandler(nodeName, p.handler)
			} else {
				delete(e.directHandlers, nodeName)
				// Cannot unregister from the underlying registry; leave the inert
				// handler (idempotently overwritten on retry).
			}
		}
	}
	return rollback, nil
}
