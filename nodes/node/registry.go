package node

import (
	"fmt"
	"sync"

	"github.com/xbcio/xflow/types"
)

type registry struct {
	mu         sync.RWMutex
	handlers   map[string]ActionHandler         // type -> latest handler
	versioned  map[string]map[int]ActionHandler // type -> version -> handler
	triggers   map[string]types.TriggerHandler
	triggerVer map[string]map[int]types.TriggerHandler
}

var globalRegistry = &registry{
	handlers:   make(map[string]ActionHandler),
	versioned:  make(map[string]map[int]ActionHandler),
	triggers:   make(map[string]types.TriggerHandler),
	triggerVer: make(map[string]map[int]types.TriggerHandler),
}

// Register registers a handler in the global registry.
// If h embeds BaseNode, its version is used; otherwise defaults to v1.
// The latest registered version becomes the default for Lookup.
func Register(h ActionHandler) {
	t := h.Descriptor().Type
	if t == "" {
		panic(fmt.Sprintf("node.Register: handler %T has empty Descriptor().Type", h))
	}

	version := 1
	if v, ok := h.(interface{ NodeVersion() int }); ok {
		version = v.NodeVersion()
	}

	globalRegistry.mu.Lock()
	defer globalRegistry.mu.Unlock()

	registerActionLocked(t, version, h)
	if th, ok := h.(types.TriggerHandler); ok {
		registerTriggerLocked(t, version, th)
	}
}

func RegisterTrigger(h types.TriggerHandler) {
	t := h.Descriptor().Type
	if t == "" {
		panic(fmt.Sprintf("node.RegisterTrigger: handler %T has empty Descriptor().Type", h))
	}
	version := 1
	if v, ok := h.(interface{ NodeVersion() int }); ok {
		version = v.NodeVersion()
	}
	globalRegistry.mu.Lock()
	defer globalRegistry.mu.Unlock()
	registerTriggerLocked(t, version, h)
	if ah, ok := h.(ActionHandler); ok {
		registerActionLocked(t, version, ah)
	}
}

func registerActionLocked(nodeType string, version int, h ActionHandler) {
	if globalRegistry.versioned[nodeType] == nil {
		globalRegistry.versioned[nodeType] = make(map[int]ActionHandler)
	}
	globalRegistry.versioned[nodeType][version] = h

	// latest version wins as default
	if cur, exists := globalRegistry.handlers[nodeType]; exists {
		if cv, ok := cur.(interface{ NodeVersion() int }); ok {
			if version >= cv.NodeVersion() {
				globalRegistry.handlers[nodeType] = h
			}
			return
		}
	}
	globalRegistry.handlers[nodeType] = h
}

func registerTriggerLocked(nodeType string, version int, h types.TriggerHandler) {
	if globalRegistry.triggerVer[nodeType] == nil {
		globalRegistry.triggerVer[nodeType] = make(map[int]types.TriggerHandler)
	}
	globalRegistry.triggerVer[nodeType][version] = h
	if cur, exists := globalRegistry.triggers[nodeType]; exists {
		if cv, ok := cur.(interface{ NodeVersion() int }); ok {
			if version >= cv.NodeVersion() {
				globalRegistry.triggers[nodeType] = h
			}
			return
		}
	}
	globalRegistry.triggers[nodeType] = h
}

// Lookup finds the latest version of a handler by node type.
func Lookup(nodeType string) (ActionHandler, bool) {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()
	h, ok := globalRegistry.handlers[nodeType]
	return h, ok
}

func LookupTrigger(nodeType string) (types.TriggerHandler, bool) {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()
	h, ok := globalRegistry.triggers[nodeType]
	return h, ok
}

// LookupVersion finds a specific version of a handler by node type.
func LookupVersion(nodeType string, version int) (ActionHandler, bool) {
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
