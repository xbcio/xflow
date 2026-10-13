package script

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Observer receives script execution observations. Implementations must be
// non-blocking and must avoid high-cardinality labels such as execution IDs or
// raw script source.
type Observer interface {
	OnScriptExecute(ctx context.Context, language, runtime, outcome string, duration time.Duration)
	OnScriptOutputBytes(ctx context.Context, language, runtime string, size int)
}

// BatchFallbackObserver is an optional extension of Observer, discovered by
// type assertion on the installed observer: it reports one batch execution
// that had to run record-by-record because the engine implements no
// engine.BatchEngine.
//
// It is a separate interface rather than a third method on Observer so that
// existing implementations keep compiling; observability/metrics mirrors it
// for the same reason the other metrics adapters do.
type BatchFallbackObserver interface {
	OnScriptBatchSerialFallback(ctx context.Context, language, runtime string)
}

type noopObserver struct{}

func (noopObserver) OnScriptExecute(context.Context, string, string, string, time.Duration) {}
func (noopObserver) OnScriptOutputBytes(context.Context, string, string, int)               {}

var (
	observerMu        sync.RWMutex
	observer          Observer = noopObserver{}
	observerInstalled bool     // true when a non-noop observer is installed
)

// SetObserver installs the global script observer. Pass nil to restore the
// no-op default.
//
// Call once during process initialization. A second non-nil call panics:
// two callers racing to set a process-wide observer means one of them will
// silently lose all its observations, which is worse than failing loudly.
// Pass nil explicitly to remove the observer before installing a new one
// (tests use this as their teardown path).
func SetObserver(o Observer) {
	observerMu.Lock()
	defer observerMu.Unlock()
	if o == nil {
		observer = noopObserver{}
		observerInstalled = false
		return
	}
	if observerInstalled {
		panic("script.SetObserver: observer already installed; call SetObserver(nil) first")
	}
	observer = o
	observerInstalled = true
}

func observeExecute(ctx context.Context, language, runtime, outcome string, duration time.Duration) {
	observerMu.RLock()
	o := observer
	observerMu.RUnlock()
	o.OnScriptExecute(ctx, language, runtime, outcome, duration)
}

func observeOutputBytes(ctx context.Context, language, runtime string, size int) {
	observerMu.RLock()
	o := observer
	observerMu.RUnlock()
	o.OnScriptOutputBytes(ctx, language, runtime, size)
}

// batchSerialFallbackKey is the once-per-process warning key: one warning per
// (language, runtime) is enough to point an operator at the placement, while a
// batch trigger would otherwise re-log it for every batch it delivers.
type batchSerialFallbackKey struct{ language, runtime string }

var batchSerialFallbackWarned sync.Map

// observeBatchSerialFallback counts one batch execution that fell back to
// engine.ExecuteBatchSerial and warns about the runtime once per
// (language, runtime) per process.
//
// The fallback is not an error — the results are the same, only the cost
// differs, and for wazero's legacy command runtime the difference is the
// pooled-instance fast path (runtime=wazero-reactor) not being used at all. It
// was silent, so a node paying 18x per record looked identical to one on the
// pooled runtime. The counter is the durable half of that signal; the warning
// is the one a developer sees while wiring a Kafka batch trigger.
//
// The counter is emitted on every fallback (it is the volume), the log line
// only on the first one (it is the placement). Both labels come from the node's
// parameters, so the label set is bounded by what the DSL can spell.
func observeBatchSerialFallback(ctx context.Context, language, runtime string) {
	observerMu.RLock()
	o := observer
	observerMu.RUnlock()
	if b, ok := o.(BatchFallbackObserver); ok {
		b.OnScriptBatchSerialFallback(ctx, language, runtime)
	}
	if _, loaded := batchSerialFallbackWarned.LoadOrStore(batchSerialFallbackKey{language: language, runtime: runtime}, struct{}{}); loaded {
		return
	}
	attrs := []any{"language", language, "runtime", runtime}
	if language == wasmScriptLanguage {
		// The only runtime with a pooled batch path, so the hint is wasm-only;
		// on js the same fallback is the intended path, just serial.
		attrs = append(attrs, "hint", "runtime=wazero-reactor evaluates a batch through pooled instances")
	}
	slog.Warn("script: runtime has no batch fast path; executing the batch serially", attrs...)
}
