package wasm

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

// Observer receives wasm reactor pool observations. Implementations must be
// non-blocking and must never use content, hashes, or execution IDs as labels:
// the first two are not an operator's to read from a metric and the third
// would be unbounded cardinality.
type Observer interface {
	// OnPoolSwap reports one config application. result is "applied",
	// "rejected", or "source_error". ruleCount is the number of rules in the
	// new content (count only — never the content itself), or -1 if the shape
	// was not recognized. revision is the SupplyResource revision.
	OnPoolSwap(ctx context.Context, result string, ruleCount int, revision uint64, d time.Duration)
	// OnConfigAge reports how long the active content has been in service.
	// This is the only signal that exposes a source which stopped updating.
	OnConfigAge(ctx context.Context, age time.Duration)
	// OnInstanceCount reports pool occupancy. state is "ready" or "doomed".
	OnInstanceCount(ctx context.Context, state string, n int)
	// OnInstanceRecycled reports an instance teardown. cause is "timeout",
	// "eval_error", "shutdown", or "pool_swapped".
	OnInstanceRecycled(ctx context.Context, cause string)
	// OnBorrowWait reports how long a caller waited for a free instance. A
	// rising value means poolSize is too small for the offered concurrency.
	OnBorrowWait(ctx context.Context, d time.Duration)
	// OnModuleCompile reports module compilation cache outcome: "hit" or
	// "miss".
	OnModuleCompile(ctx context.Context, result string)
}

type noopObserver struct{}

func (noopObserver) OnPoolSwap(context.Context, string, int, uint64, time.Duration) {}
func (noopObserver) OnConfigAge(context.Context, time.Duration)                     {}
func (noopObserver) OnInstanceCount(context.Context, string, int)                  {}
func (noopObserver) OnInstanceRecycled(context.Context, string)                    {}
func (noopObserver) OnBorrowWait(context.Context, time.Duration)                   {}
func (noopObserver) OnModuleCompile(context.Context, string)                       {}

var (
	observerMu sync.RWMutex
	observer   Observer = noopObserver{}
)

// SetObserver installs the global wasm observer. Pass nil to restore the no-op
// default. Call once during process initialization — this lock is NOT on the
// message path (see obs()).
func SetObserver(o Observer) {
	observerMu.Lock()
	defer observerMu.Unlock()
	if o == nil {
		observer = noopObserver{}
		return
	}
	observer = o
}

// obs snapshots the observer. It IS called from swap/borrow paths, so keep it
// a bare RLock over a pointer read and nothing else. The Execute path's config
// lookup was deliberately made lock-free; do not undo that by adding work
// here (see the baseline note in pool.go's callers).
func obs() Observer {
	observerMu.RLock()
	o := observer
	observerMu.RUnlock()
	return o
}

// ruleCount counts entries in the content's "rules" array. It reports a COUNT
// and never the content. -1 means the shape was not recognized, which is
// itself worth alerting on: content the host cannot even count is probably not
// what the guest expects either.
func ruleCount(cfg []byte) int {
	var probe struct {
		Rules []json.RawMessage `json:"rules"`
	}
	if err := json.Unmarshal(cfg, &probe); err != nil {
		return -1
	}
	return len(probe.Rules)
}
