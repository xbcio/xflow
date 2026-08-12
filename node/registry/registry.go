package registry

import (
	"fmt"
	"sync"

	"github.com/xbcio/xflow/types"
)

type registry struct {
	mu         sync.RWMutex
	handlers   map[string]types.ActionHandler         // type -> latest handler
	versioned  map[string]map[int]types.ActionHandler // type -> version -> handler
	triggers   map[string]types.TriggerHandler
	triggerVer map[string]map[int]types.TriggerHandler
}

var globalRegistry = &registry{
	handlers:   make(map[string]types.ActionHandler),
	versioned:  make(map[string]map[int]types.ActionHandler),
	triggers:   make(map[string]types.TriggerHandler),
	triggerVer: make(map[string]map[int]types.TriggerHandler),
}

// Register registers a handler in the global registry.
// If h embeds BaseNode, its version is used; otherwise defaults to v1.
// The latest registered version becomes the default for Lookup.
func Register(h types.ActionHandler) {
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
	if ah, ok := h.(types.ActionHandler); ok {
		registerActionLocked(t, version, ah)
	}
}

func registerActionLocked(nodeType string, version int, h types.ActionHandler) {
	if globalRegistry.versioned[nodeType] == nil {
		globalRegistry.versioned[nodeType] = make(map[int]types.ActionHandler)
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
func Lookup(nodeType string) (types.ActionHandler, bool) {
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
func LookupVersion(nodeType string, version int) (types.ActionHandler, bool) {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()
	versions, ok := globalRegistry.versioned[nodeType]
	if !ok {
		return nil, false
	}
	h, ok := versions[version]
	return h, ok
}

// LookupTriggerVersion finds a specific version of a trigger handler by node
// type. Used by the SDK's trigger runtime when resolving versioned
// trigger node definitions; returns false on miss so the caller can apply a
// policy decision (strict / warn fallback / silent fallback).
func LookupTriggerVersion(nodeType string, version int) (types.TriggerHandler, bool) {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()
	versions, ok := globalRegistry.triggerVer[nodeType]
	if !ok {
		return nil, false
	}
	h, ok := versions[version]
	return h, ok
}

// TriggerVersions returns all registered trigger versions for a node type.
func TriggerVersions(nodeType string) []int {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()
	versions, ok := globalRegistry.triggerVer[nodeType]
	if !ok {
		return nil
	}
	result := make([]int, 0, len(versions))
	for v := range versions {
		result = append(result, v)
	}
	return result
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

// Types returns all registered action node types in the global registry.
func Types() []string {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()
	result := make([]string, 0, len(globalRegistry.versioned))
	for t := range globalRegistry.versioned {
		result = append(result, t)
	}
	return result
}

// TriggerTypes returns all registered trigger node types in the global registry.
// Trigger-only types (those registered via RegisterTrigger that do not also
// implement ActionHandler) appear here but not in Types().
func TriggerTypes() []string {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()
	result := make([]string, 0, len(globalRegistry.triggerVer))
	for t := range globalRegistry.triggerVer {
		result = append(result, t)
	}
	return result
}

// HasExact returns true when the global registry has a handler for the exact
// (nodeType, version) pair. It does NOT fall back to latest version.
func HasExact(nodeType string, version int) bool {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()
	versions, ok := globalRegistry.versioned[nodeType]
	if !ok {
		return false
	}
	_, found := versions[version]
	return found
}
