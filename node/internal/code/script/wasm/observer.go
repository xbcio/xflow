package wasm

import (
	"context"
	"encoding/json"
	"sync/atomic"
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
func (noopObserver) OnInstanceCount(context.Context, string, int)                   {}
func (noopObserver) OnInstanceRecycled(context.Context, string)                     {}
func (noopObserver) OnBorrowWait(context.Context, time.Duration)                    {}
func (noopObserver) OnModuleCompile(context.Context, string)                        {}

// observer holds the installed Observer. It is an atomic pointer, not a
// RWMutex-guarded variable, because obs() sits on the per-message path
// (OnConfigAge in Execute, OnBorrowWait in borrow) at ~12000 msg/s.
//
// Measured on an M3 at 8-way parallelism: an RWMutex read-and-call costs
// 69.5 ns/op, the atomic load 2.2 ns/op — 32x. A read-mostly RWMutex is not
// free under contention; every reader still writes the shared reader counter,
// so the cache line ping-pongs between cores. That is the same tail-latency
// cost that moved configFromSource off a lock, and it would be undone here.
//
// The pointer indirection exists because Observer is an interface: storing a
// two-word interface value atomically requires boxing it behind one pointer.
var observer atomic.Pointer[Observer]

func init() {
	var o Observer = noopObserver{}
	observer.Store(&o)
}

// SetObserver installs the global wasm observer. Pass nil to restore the no-op
// default. Call once during process initialization.
func SetObserver(o Observer) {
	if o == nil {
		o = noopObserver{}
	}
	observer.Store(&o)
}

// obs returns the installed observer. It IS called from the per-message path,
// so it must stay a single atomic load and nothing else. The Execute path's
// config lookup was deliberately made lock-free; do not undo that by adding a
// lock here.
func obs() Observer {
	return *observer.Load()
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
