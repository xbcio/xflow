package node

import (
	"fmt"
	"sync"
)

type registry struct {
	mu       sync.RWMutex
	handlers map[string]TaskHandler            // type -> latest handler
	versioned map[string]map[int]TaskHandler   // type -> version -> handler
}

var globalRegistry = &registry{
	handlers:  make(map[string]TaskHandler),
	versioned: make(map[string]map[int]TaskHandler),
}

// Register registers a handler in the global registry.
// h must implement DescriptorProvider; panics otherwise.
// If h embeds BaseNode, its version is used; otherwise defaults to v1.
// The latest registered version becomes the default for Lookup.
func Register(h TaskHandler) {
	p, ok := h.(DescriptorProvider)
	if !ok {
		panic(fmt.Sprintf("node.Register: handler %T must implement DescriptorProvider", h))
	}
	t := p.Descriptor().Type
	if t == "" {
		panic(fmt.Sprintf("node.Register: handler %T has empty Descriptor().Type", h))
	}

	version := 1
	if v, ok := h.(interface{ NodeVersion() int }); ok {
		version = v.NodeVersion()
	}

	globalRegistry.mu.Lock()
	defer globalRegistry.mu.Unlock()

	if globalRegistry.versioned[t] == nil {
		globalRegistry.versioned[t] = make(map[int]TaskHandler)
	}
	globalRegistry.versioned[t][version] = h

	// latest version wins as default
	if cur, exists := globalRegistry.handlers[t]; exists {
		if cv, ok := cur.(interface{ NodeVersion() int }); ok {
			if version >= cv.NodeVersion() {
				globalRegistry.handlers[t] = h
			}
			return
		}
	}
	globalRegistry.handlers[t] = h
}

// Lookup finds the latest version of a handler by node type.
func Lookup(nodeType string) (TaskHandler, bool) {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()
	h, ok := globalRegistry.handlers[nodeType]
	return h, ok
}

// LookupVersion finds a specific version of a handler by node type.
func LookupVersion(nodeType string, version int) (TaskHandler, bool) {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()
	versions, ok := globalRegistry.versioned[nodeType]
	if !ok {
		return nil, false
	}
	h, ok := versions[version]
	return h, ok
}

// Versions returns all registered versions for a node type.
func Versions(nodeType string) []int {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()
	versions, ok := globalRegistry.versioned[nodeType]
	if !ok {
		return nil
	}
	result := make([]int, 0, len(versions))
	for v := range versions {
		result = append(result, v)
	}
	return result
}
