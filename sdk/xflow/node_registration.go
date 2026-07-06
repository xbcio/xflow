package xflow

import (
	"fmt"

	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/nodes/node"
	"github.com/xbcio/xflow/types"
)

// registerNodeDefinitions registers node/trigger handlers declared via
// WithNodes. Used by both local and cluster: local needs it so LocalNode-free
// typed nodes resolve in-process; cluster workers need it to resolve types
// for workflows submitted by other processes.
func (e *Engine) registerNodeDefinitions(defs []node.Handler) error {
	if len(defs) == 0 {
		return nil
	}
	lr, ok := e.registry.(*execution.Registry)
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
			if th, isTrigger := def.(types.TriggerHandler); isTrigger {
				node.RegisterTrigger(th)
			}
		case types.TriggerHandler:
			node.RegisterTrigger(h)
		default:
			return fmt.Errorf("node definition %q is not an action or trigger handler", def.Descriptor().Type)
		}
	}
	return nil
}

// registerDirectHandlers registers LocalNode handlers from a WorkflowBuilder.
// Local-mode only: cluster rejects any workflow using direct handlers because
// Go function values cannot be serialized or executed by another process.
func (e *Engine) registerDirectHandlers(wf *WorkflowBuilder) error {
	if len(wf.directHandlers()) == 0 {
		return nil
	}
	lr, ok := e.registry.(*execution.Registry)
	if !cfgAllowsDirectHandlers(e) || !ok {
		names := make([]string, 0, len(wf.directHandlers()))
		for n := range wf.directHandlers() {
			names = append(names, n)
		}
		return fmt.Errorf("nodes %v use direct action handlers (local mode only); with cluster, define custom nodes with node.Define and register consumer capabilities with xflow.WithNodes", names)
	}
	for nodeName, h := range wf.directHandlers() {
		lr.RegisterNodeHandler(nodeName, h)
	}
	return nil
}

// registerWorkflowHandlers registers portable typed node/trigger handlers
// declared inline on a WorkflowBuilder (via Node). Shared by both modes.
func (e *Engine) registerWorkflowHandlers(wf *WorkflowBuilder) error {
	if wf == nil || len(wf.workflowHandlers()) == 0 {
		return nil
	}
	lr, ok := e.registry.(*execution.Registry)
	if !ok {
		return fmt.Errorf("registry does not support workflow handler registration")
	}
	for nodeType, h := range wf.workflowHandlers() {
		lr.RegisterGlobal(nodeType, h)
	}
	for _, h := range wf.workflowTriggerHandlers() {
		node.RegisterTrigger(h)
	}
	return nil
}
