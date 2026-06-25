package execution

import (
	"fmt"
	"sync"

	"github.com/xbcio/xflow/nodes/node"
	"github.com/xbcio/xflow/types"
)

// Registry resolves handlers for embedded execution.
//
// Resolution order is:
//  1. execution-scoped direct handler
//  2. node-name direct handler
//  3. explicitly registered node-type handler
//  4. global node registry, versioned first
type Registry struct {
	mu                sync.RWMutex
	executionHandlers map[string]types.ActionHandler
	nodeHandlers      map[string]types.ActionHandler
	globalHandlers    map[string]types.ActionHandler
}

// NewRegistry creates a handler registry suitable for embedded runners.
func NewRegistry() *Registry {
	return &Registry{
		executionHandlers: make(map[string]types.ActionHandler),
		nodeHandlers:      make(map[string]types.ActionHandler),
		globalHandlers:    make(map[string]types.ActionHandler),
	}
}

// RegisterExecutionHandler binds a handler to one execution and node name.
func (r *Registry) RegisterExecutionHandler(id types.ExecutionID, nodeName string, h types.ActionHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.executionHandlers[string(id)+"/"+nodeName] = h
}

// RegisterNodeHandler binds a handler to a node name across executions.
func (r *Registry) RegisterNodeHandler(nodeName string, h types.ActionHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodeHandlers[nodeName] = h
}

// RegisterGlobal binds a handler to a node type across executions.
func (r *Registry) RegisterGlobal(nodeType string, h types.ActionHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.globalHandlers[nodeType] = h
}

// Get resolves a handler for the given execution node.
func (r *Registry) Get(id types.ExecutionID, nodeName string, nodeType string, version int) (types.ActionHandler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if h, ok := r.executionHandlers[string(id)+"/"+nodeName]; ok {
		return h, nil
	}
	if h, ok := r.nodeHandlers[nodeName]; ok {
		return h, nil
	}
	if h, ok := r.globalHandlers[nodeType]; ok {
		return h, nil
	}
	if version > 0 {
		if h, ok := node.LookupVersion(nodeType, version); ok {
			return h, nil
		}
	}
	if h, ok := node.Lookup(nodeType); ok {
		return h, nil
	}

	return nil, fmt.Errorf("no handler registered for node type %q", nodeType)
}
