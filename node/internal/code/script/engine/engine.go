// Package engine defines the language-agnostic engine abstraction and
// (language, runtime) registry for the xflow.script node. Concrete engines
// live in subpackages (js, wasm) and self-register via init().
package engine

import (
	"context"
	"sync"
)

// Engine executes a script of one (language, runtime) family.
type Engine interface {
	// Name is the human-readable identifier, e.g. "js/goja", "wasm/wazero".
	Name() string
	// Execute runs code with the given globals (already including $credentials
	// and $credential resolved by the node layer) and host helpers.
	// It returns the raw completion value (js) or decoded stdout JSON (wasm).
	Execute(ctx context.Context, code string, globals map[string]any, h Helpers) (any, error)
}

// Helpers is the language-agnostic set of NON-SECURITY utilities exposed to
// scripts (base64, etc). It carries no credential capability.
type Helpers interface {
	Base64Encode(s string) string
	Base64Decode(s string) (string, error)
}

type registryKey struct{ language, runtime string }

var (
	registryMu sync.RWMutex
	registry   = map[registryKey]func() Engine{}
)

// Register adds an engine factory under (language, runtime). Called from
// engine subpackage init().
func Register(language, runtime string, factory func() Engine) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry[registryKey{language, runtime}] = factory
}

// Lookup returns an engine instance for (language, runtime).
func Lookup(language, runtime string) (Engine, bool) {
	registryMu.RLock()
	factory, ok := registry[registryKey{language, runtime}]
	registryMu.RUnlock()
	if !ok {
		return nil, false
	}
	return factory(), true
}
