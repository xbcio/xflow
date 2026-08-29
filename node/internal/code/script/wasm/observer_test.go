package wasm

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
	"github.com/xbcio/xflow/node/supply"
)

// recordingObserver captures every call made to it, for assertions in tests
// that exercise pool.go's instrumentation points.
//
// The mutex is load-bearing, not defensive boilerplate: swapConfig tears the
// superseded pool down on a goroutine of its own (the `go e.drainPool` at the
// end of swapConfig), and that goroutine calls OnInstanceRecycled concurrently
// with the test body. Without the lock this is a data race on the slices.
// Read the slices through the accessors below, never directly — a bare
// len(rec.recycled) in a test body is the same race read from the other side.
type recordingObserver struct {
	mu         sync.Mutex
	swaps      []swapCall
	ages       []time.Duration
	instances  []instanceCall
	recycled   []string
	borrowWait []time.Duration
	evals      []evalCall
	compiles   []string
}

type evalCall struct {
	workflow   string
	node       string
	stdinBytes int
	d          time.Duration
}

type swapCall struct {
	result    string
	ruleCount int
	revision  uint64
	d         time.Duration
}

type instanceCall struct {
	state string
	n     int
}

func (r *recordingObserver) OnPoolSwap(_ context.Context, result string, ruleCount int, revision uint64, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.swaps = append(r.swaps, swapCall{result, ruleCount, revision, d})
}
func (r *recordingObserver) OnConfigAge(_ context.Context, age time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ages = append(r.ages, age)
}
func (r *recordingObserver) OnInstanceCount(_ context.Context, state string, n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.instances = append(r.instances, instanceCall{state, n})
}
func (r *recordingObserver) OnInstanceRecycled(_ context.Context, cause string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recycled = append(r.recycled, cause)
}
func (r *recordingObserver) OnBorrowWait(_ context.Context, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.borrowWait = append(r.borrowWait, d)
}
func (r *recordingObserver) OnEval(_ context.Context, workflow, node string, stdinBytes int, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evals = append(r.evals, evalCall{workflow, node, stdinBytes, d})
}
func (r *recordingObserver) OnModuleCompile(_ context.Context, result string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.compiles = append(r.compiles, result)
}

// OnEngineCount is a stub: no test in this package currently asserts on it.
// Task 4 owns the real observer implementation.
func (r *recordingObserver) OnEngineCount(context.Context, int) {}

// Snapshot accessors. Each returns a copy so a caller can range over the
// result while the drain goroutine keeps appending.

func (r *recordingObserver) swapCalls() []swapCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]swapCall(nil), r.swaps...)
}
func (r *recordingObserver) ageCalls() []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Duration(nil), r.ages...)
}
func (r *recordingObserver) instanceCalls() []instanceCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]instanceCall(nil), r.instances...)
}
func (r *recordingObserver) recycledCauses() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.recycled...)
}
func (r *recordingObserver) borrowWaits() []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Duration(nil), r.borrowWait...)
}
func (r *recordingObserver) evalCalls() []evalCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]evalCall(nil), r.evals...)
}
func (r *recordingObserver) compileResults() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.compiles...)
}

// SetObserver must install and later restore the no-op default: a test that
// leaves a custom observer installed would leak into every later test sharing
// the process-wide sharedReactorHost.
func TestSetObserverInstallsAndRestoresDefault(t *testing.T) {
	rec := &recordingObserver{}
	SetObserver(rec)
	defer SetObserver(nil)

	if obs() != Observer(rec) {
		t.Fatal("obs() must return the installed observer")
	}

	SetObserver(nil)
	if _, ok := obs().(noopObserver); !ok {
		t.Fatalf("SetObserver(nil) must restore noopObserver, got %T", obs())
	}
}

// ruleCount must report a plain count, never content — and -1 for a shape it
// does not recognize, which is itself worth alerting on.
//
// The "unrecognized" cases carry the weight here. This function once counted
// only a "rules" key, and an absent key unmarshals to a nil slice rather than
// an error — so every phase-split payload reported a confident 0, a value that
// is ALSO legal ("the source says there are no rules"). The gauge read healthy
// while the host understood none of the content. -1 must be reachable for
// well-formed JSON, not just for bytes that fail to parse.
func TestRuleCount(t *testing.T) {
	cases := []struct {
		name string
		cfg  string
		want int
	}{
		{"empty rules", `{"rules":[]}`, 0},
		{"three rules", `{"rules":[{"id":"a"},{"id":"b"},{"id":"c"}]}`, 3},
		// The phase-split shape a guest with more than one evaluation stage
		// publishes. Counted as the sum: both phases serve.
		{"phase split", `{"revision":7,"pre_analysis":[{"id":1},{"id":2}],"post_decode":[{"id":3}]}`, 3},
		{"phase split empty", `{"revision":7,"pre_analysis":[],"post_decode":[]}`, 0},
		{"one phase only", `{"pre_analysis":[{"id":1}]}`, 1},
		// Well-formed JSON carrying none of the known keys. This is the case
		// that must NOT read as zero.
		{"no known key", `{}`, -1},
		{"unknown shape", `{"revision":7,"entries":[{"id":1}]}`, -1},
		{"malformed json", `not json`, -1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ruleCount([]byte(c.cfg)); got != c.want {
				t.Fatalf("ruleCount(%q) = %d, want %d", c.cfg, got, c.want)
			}
		})
	}
}

// A source-driven Execute call on a Fresh/Stale pool must sample ConfigAge
// into the observer on every call: this is the only signal that exposes a
// source that stopped updating, since gen/revision stay frozen while a source
// keeps failing.
func TestExecuteSamplesConfigAgeForSourceDrivenModule(t *testing.T) {
	rec := &recordingObserver{}
	SetObserver(rec)
	defer SetObserver(nil)

	code := testReactorCode(t)
	reg := supply.NewRegistry()
	if err := RegisterSupplyConsumer(code, "rules", reg); err != nil {
		t.Fatalf("register: %v", err)
	}
	ctx := context.Background()
	if err := reg.Apply(ctx, supply.Snapshot{
		Name: "rules", Content: []byte(`{"rules":[]}`), Hash: "h1", Revision: 1,
		FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}

	if _, err := sharedReactorEngine.Execute(ctx, engine.Code(code), map[string]any{"x": 1.0}, engine.DefaultHelpers()); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// Exactly one: the sole OnConfigAge call site (reactor.go) sits on the
	// source-driven branch of Execute and samples once per call, and this test
	// makes one call. "at least one" was the old check, and it is green for a
	// sampler that fires ten thousand times — which is the failure mode the
	// call site's own comment is guarding against, since it justifies per-call
	// sampling by its cost.
	ages := rec.ageCalls()
	if len(ages) != 1 {
		t.Fatalf("OnConfigAge notifications = %d, want exactly 1 (one Execute, one "+
			"per-call sample)", len(ages))
	}
	// And it must be the content's age, not a zero placeholder: the whole point
	// of the series is that it climbs when a source stops updating.
	if ages[0] <= 0 {
		t.Fatalf("reported config age = %s, want a positive age measured from the "+
			"snapshot's FetchedAt", ages[0])
	}
}
