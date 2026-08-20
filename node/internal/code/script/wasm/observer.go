package wasm

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
)

// Observer receives wasm reactor pool observations. Implementations must be
// non-blocking and must never use content, hashes, or execution IDs as labels:
// the first two are not an operator's to read from a metric and the third
// would be unbounded cardinality.
type Observer interface {
	// OnPoolSwap reports one config application. result is "applied" or
	// "rejected". ruleCount is the number of rules in the new content (count
	// only — never the content itself), or -1 if the shape was not recognized.
	// revision is the SupplyResource revision.
	OnPoolSwap(ctx context.Context, result string, ruleCount int, revision uint64, d time.Duration)
	// OnConfigAge reports how long the active content has been in service.
	// This is the only signal that exposes a source which stopped updating.
	OnConfigAge(ctx context.Context, age time.Duration)
	// OnInstanceCount reports the host's TOTAL resident pool instances, summed
	// across every engine. state is "ready" — there is no other value.
	//
	// The sum is what makes it meaningful: the metric behind it is a gauge
	// carrying only state, so each report replaces the series. A per-engine
	// report therefore means "the last engine to swap", which understates
	// residency by the module count.
	//
	// There is deliberately no "doomed" state. A doomed instance is an EVENT,
	// not a population: it is torn down and replaced immediately, so it has no
	// residency to report. It used to be reported here as a constant 1, which —
	// against a gauge that never returns to zero — made the series a "has any
	// instance ever been doomed" boolean rather than a count. Doom events are
	// counted properly by OnInstanceRecycled.
	OnInstanceCount(ctx context.Context, state string, n int)
	// OnInstanceRecycled reports an instance teardown. cause is "timeout",
	// "eval_error", "max_evals", "shutdown", or "pool_swapped".
	OnInstanceRecycled(ctx context.Context, cause string)
	// OnBorrowWait reports how long a caller waited for a free instance. A
	// rising value means poolSize is too small for the offered concurrency.
	OnBorrowWait(ctx context.Context, d time.Duration)
	// OnEval reports one eval: the stdin byte count handed to the guest and how
	// long the instance was occupied running it.
	//
	// The two travel together deliberately. Eval cost is dominated by the guest
	// rebuilding the stdin object inside the sandbox — measured at ~4.9 MB/s and
	// linear across an 85x size range — so stdin size is the term that predicts
	// duration. Reported apart, a slow eval cannot be told from a large one, and
	// a size regression (a redundant copy of the record reaching the payload)
	// looks identical to the engine getting slower.
	//
	// stdinBytes is a COUNT, never content: the payload carries live traffic.
	OnEval(ctx context.Context, stdinBytes int, d time.Duration)
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
func (noopObserver) OnEval(context.Context, int, time.Duration)                     {}
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

// observerMu guards the observerInstalled flag. It protects only the
// write path; the hot-path read (obs()) goes straight to the atomic pointer.
var observerMu sync.Mutex

// observerInstalled tracks whether a non-noop observer is currently installed,
// so a second non-nil SetObserver call can be caught and panicked. It is
// guarded by observerMu.
var observerInstalled bool

func init() {
	var o Observer = noopObserver{}
	observer.Store(&o)
}

// SetObserver installs the global wasm observer. Pass nil to restore the no-op
// default.
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
		var noop Observer = noopObserver{}
		observer.Store(&noop)
		observerInstalled = false
		return
	}
	if observerInstalled {
		panic("wasm.SetObserver: observer already installed; call SetObserver(nil) first")
	}
	observer.Store(&o)
	observerInstalled = true
}

// obs returns the installed observer. It IS called from the per-message path,
// so it must stay a single atomic load and nothing else. The Execute path's
// config lookup was deliberately made lock-free; do not undo that by adding a
// lock here.
func obs() Observer {
	return *observer.Load()
}

// ruleCount counts the rule entries in the content. It reports a COUNT and
// never the content. -1 means the shape was not recognized, which is itself
// worth alerting on: content the host cannot even count is probably not what
// the guest expects either.
//
// Two shapes are recognized: a flat {"rules":[...]} and the phase-split
// {"pre_analysis":[...],"post_decode":[...]} that a guest with more than one
// evaluation stage publishes. The split shape is counted as the sum, because
// the gauge answers "how many rules are serving" and both phases serve.
//
// Counting only "rules" made the -1 branch unreachable for any well-formed
// JSON: an absent key unmarshals to a nil slice rather than an error, so a
// phase-split payload reported a confident 0 — a value that is also legal
// ("the source says there are no rules") and therefore indistinguishable from
// the real thing. A gauge that cannot tell "no rules" from "I do not
// understand this content" is worse than no gauge, since it reads healthy.
func ruleCount(cfg []byte) int {
	var probe struct {
		Rules       []json.RawMessage `json:"rules"`
		PreAnalysis []json.RawMessage `json:"pre_analysis"`
		PostDecode  []json.RawMessage `json:"post_decode"`
	}
	if err := json.Unmarshal(cfg, &probe); err != nil {
		return -1
	}
	// Distinguish "recognized, and it holds zero rules" from "none of the known
	// keys is present". Only a key that EXISTS licenses a count; absent keys
	// leave the shape unrecognized, which is the -1 case.
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(cfg, &keys); err != nil {
		return -1
	}
	_, hasRules := keys["rules"]
	_, hasPre := keys["pre_analysis"]
	_, hasPost := keys["post_decode"]
	if !hasRules && !hasPre && !hasPost {
		return -1
	}
	return len(probe.Rules) + len(probe.PreAnalysis) + len(probe.PostDecode)
}
