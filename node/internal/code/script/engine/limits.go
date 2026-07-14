package engine

import "fmt"

// Process-wide resource limits for the script node. Kept as package constants
// (not DSL params) — the operational profile is well-defined and DSL surface
// stays tight. Adjust here when telemetry justifies a different default.
const (
	// DefaultMaxOutputBytes caps the JSON-encoded script result size to defend
	// against runaway scripts that build huge strings/arrays before returning.
	// Enforced at the node layer after engine.Execute returns.
	DefaultMaxOutputBytes = 1 << 20 // 1 MiB

	// DefaultGojaStackSize caps goja's call stack depth. Deep recursion past
	// this returns a runtime error instead of crashing the process.
	DefaultGojaStackSize = 2048

	// DefaultWasmMemoryPages caps the wasm linear memory at 256 pages × 64KiB
	// = 16 MiB. Guests requesting more via memory.grow trap at the wazero
	// boundary.
	DefaultWasmMemoryPages = 256

	// DefaultProgramCacheSize bounds the goja compiled-program LRU cache.
	// Beyond this, the least-recently-used entry is evicted on insert.
	DefaultProgramCacheSize = 256

	// DefaultQJSMemoryLimit caps the QuickJS heap (bytes). Passed to
	// JS_SetMemoryLimit; allocations beyond the cap raise an out-of-memory
	// exception inside the runtime.
	DefaultQJSMemoryLimit = 16 << 20 // 16 MiB

	// DefaultQJSMaxStackSize caps QuickJS's internal C-stack budget (bytes).
	DefaultQJSMaxStackSize = 256 << 10 // 256 KiB
)

// OutputSizeError is returned by the node layer when the encoded script
// result exceeds DefaultMaxOutputBytes. It carries the actual and allowed
// sizes for diagnostic logging; the value itself is never echoed.
type OutputSizeError struct {
	Size, Limit int
}

func (e *OutputSizeError) Error() string {
	return fmt.Sprintf("script: result size %d bytes exceeds limit %d bytes", e.Size, e.Limit)
}
