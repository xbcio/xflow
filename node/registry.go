package node

import (
	"fmt"
	"sync"
)

type registry struct {
	mu       sync.RWMutex
	handlers map[string]TaskHandler
}

var globalRegistry = &registry{
	handlers: make(map[string]TaskHandler),
}

// Register registers a handler in the global registry.
// h must implement DescriptorProvider; panics otherwise.
// Safe for concurrent use from multiple init() functions.
func Register(h TaskHandler) {
	p, ok := h.(DescriptorProvider)
	if !ok {
		panic(fmt.Sprintf("node.Register: handler %T must implement DescriptorProvider", h))
	}
	t := p.Descriptor().Type
	if t == "" {
		panic(fmt.Sprintf("node.Register: handler %T has empty Descriptor().Type", h))
	}
	globalRegistry.mu.Lock()
	defer globalRegistry.mu.Unlock()
	globalRegistry.handlers[t] = h
}

// Lookup finds a handler by node type, returning (handler, found).
func Lookup(nodeType string) (TaskHandler, bool) {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()
	h, ok := globalRegistry.handlers[nodeType]
	return h, ok
}
