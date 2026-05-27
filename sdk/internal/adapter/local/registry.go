package local

import (
	"fmt"
	"sync"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// LocalRegistry implements engine.HandlerRegistry.
// It supports two lookup modes:
//   - closure: per-execution, per-node direct handler (e.g. inline functions)
//   - global:  type-based lookup from the node registry
type LocalRegistry struct {
	mu       sync.RWMutex
	closures map[string]node.TaskHandler // key: execID+"/"+nodeName
	globals  map[string]node.TaskHandler // key: nodeType
}

func NewLocalRegistry() *LocalRegistry {
	return &LocalRegistry{
		closures: make(map[string]node.TaskHandler),
		globals:  make(map[string]node.TaskHandler),
	}
}

// RegisterClosure binds a specific handler to a (executionID, nodeName) pair.
func (r *LocalRegistry) RegisterClosure(id types.ExecutionID, nodeName string, h node.TaskHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closures[string(id)+"/"+nodeName] = h
}

// RegisterGlobal registers a handler for a node type, available to all executions.
func (r *LocalRegistry) RegisterGlobal(nodeType string, h node.TaskHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.globals[nodeType] = h
}

// RegisterNodeHandler registers a handler for a specific node name (not execution-scoped).
func (r *LocalRegistry) RegisterNodeHandler(nodeName string, h node.TaskHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closures["*/"+nodeName] = h
}

// Get resolves a handler: closure lookup first, then global type lookup,
// then the global node.Registry.
func (r *LocalRegistry) Get(id types.ExecutionID, nodeName string, nodeType string) (node.TaskHandler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if h, ok := r.closures[string(id)+"/"+nodeName]; ok {
		return h, nil
	}
	if h, ok := r.closures["*/"+nodeName]; ok {
		return h, nil
	}
	if h, ok := r.globals[nodeType]; ok {
		return h, nil
	}
	if h, ok := node.Lookup(nodeType); ok {
		return h, nil
	}

	return nil, fmt.Errorf("no handler registered for node type %q", nodeType)
}
