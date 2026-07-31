package wasm

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
	"github.com/xbcio/xflow/node/supply"
)

// recordingObserver captures every call made to it, for assertions in tests
// that exercise pool.go's instrumentation points.
type recordingObserver struct {
	swaps      []swapCall
	ages       []time.Duration
	instances  []instanceCall
	recycled   []string
	borrowWait []time.Duration
	compiles   []string
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
	r.swaps = append(r.swaps, swapCall{result, ruleCount, revision, d})
}
func (r *recordingObserver) OnConfigAge(_ context.Context, age time.Duration) {
	r.ages = append(r.ages, age)
}
func (r *recordingObserver) OnInstanceCount(_ context.Context, state string, n int) {
	r.instances = append(r.instances, instanceCall{state, n})
}
func (r *recordingObserver) OnInstanceRecycled(_ context.Context, cause string) {
	r.recycled = append(r.recycled, cause)
}
func (r *recordingObserver) OnBorrowWait(_ context.Context, d time.Duration) {
	r.borrowWait = append(r.borrowWait, d)
}
func (r *recordingObserver) OnModuleCompile(_ context.Context, result string) {
	r.compiles = append(r.compiles, result)
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
// cannot parse, which is itself worth alerting on.
func TestRuleCount(t *testing.T) {
	cases := []struct {
		name string
		cfg  string
		want int
	}{
		{"empty rules", `{"rules":[]}`, 0},
		{"three rules", `{"rules":[{"id":"a"},{"id":"b"},{"id":"c"}]}`, 3},
		{"no rules key", `{}`, 0},
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

	if _, err := sharedReactorEngine.Execute(ctx, code, map[string]any{"x": 1.0}, engine.DefaultHelpers()); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(rec.ages) == 0 {
		t.Fatal("expected at least one OnConfigAge notification")
	}
}

