// Package engine defines the language-agnostic engine abstraction and
// (language, runtime) registry for the xflow.script node. Concrete engines
// live in subpackages (js, wasm) and self-register via init().
package engine

import (
	"context"
	"sync"
)

// Source is one script handed to an engine: the text to run, plus the identity
// of those bytes when the caller knows it.
//
// Code alone is not a workable identity for an engine that memoises compiled
// artifacts. A wasm module arrives base64-encoded at ~9 MB, and a Go map keyed
// by that string must confirm on every hit that the stored key EQUALS the
// lookup key — runtime.memequal walks the full 9 MB unless the two strings
// happen to share a backing array. Production never shares one: the activation
// path encodes its own copy while the message path gets another from the
// artifact cache, whose documented behaviour lets concurrent misses each
// produce a fresh encoding. Measured on an M3, one such lookup costs 198 µs
// against 22 ns when the headers coincide, and it was 32% of all wasm time in a
// production profile (SAS cross-env collection, batch=500).
//
// Digest is what removes that cost: a content-addressable name is ~70 bytes, so
// the same lookup compares 70 bytes instead of 9 MB. It travels as a field
// rather than an optional interface because an engine that silently loses it
// degrades by four orders of magnitude with nothing failing — exactly the class
// of drop this codebase keeps paying for. A caller that genuinely has no digest
// (inline code, tests) leaves it empty and the engine falls back to keying by
// content, which is the behaviour that predates this field.
type Source struct {
	// Code is the script text: JS source, or base64 of a wasm module.
	Code string
	// Digest names Code's bytes in canonical "sha256:<hex>" form — the artifact
	// store digest. Empty means "unknown", never "no digest exists". An engine
	// must treat a non-empty Digest as naming exactly the bytes in Code; the
	// artifact store is what guarantees that, since Code was fetched BY this
	// digest.
	Digest string
}

// Code builds a Source with no known identity. It is the constructor for inline
// scripts and tests; the artifact path sets Digest and pays far less per call.
func Code(code string) Source { return Source{Code: code} }

// Engine executes a script of one (language, runtime) family.
type Engine interface {
	// Name is the human-readable identifier, e.g. "js/goja", "wasm/wazero".
	Name() string
	// Execute runs src with the given globals (already including $credentials
	// and $credential resolved by the node layer) and host helpers.
	// It returns the raw completion value (js) or decoded stdout JSON (wasm).
	Execute(ctx context.Context, src Source, globals map[string]any, h Helpers) (any, error)
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
