package runner

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node/supply"
)

// --- test doubles ---

// mutableFetcher serves content a test can change between calls, with a hash
// that follows the content. stubFetcher derives its hash from the resource
// name, so it cannot express the one thing a poll test has to arrange: the
// server's content changing while the runner is up, with no hint delivered.
type mutableFetcher struct {
	mu    sync.Mutex
	body  map[string]string
	rev   uint64
	calls atomic.Int32
}

func newMutableFetcher() *mutableFetcher {
	return &mutableFetcher{body: map[string]string{}}
}

// set publishes content server-side: the next Fetch returns it, under a hash
// that differs from the previous one.
func (f *mutableFetcher) set(resource, content string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.body[resource] = content
	f.rev++
}

func (f *mutableFetcher) Fetch(_ context.Context, resource string) ([]byte, string, uint64, error) {
	f.calls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	content, ok := f.body[resource]
	if !ok {
		return nil, "", 0, ErrSupplyNotFound
	}
	return []byte(content), "sha256:" + content, f.rev, nil
}

// fakePollTicker is the hand-driven tick source a test installs on the gate, so
// a poll round needs no wall clock. It also records the period RunPoller asked
// for, which is where the interval resolution and the jitter become observable.
type fakePollTicker struct {
	ch chan time.Time

	mu      sync.Mutex
	period  time.Duration
	stopped bool
}

func newFakePollTicker() *fakePollTicker {
	return &fakePollTicker{ch: make(chan time.Time, 8)}
}

func (t *fakePollTicker) C() <-chan time.Time { return t.ch }

func (t *fakePollTicker) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stopped = true
}

func (t *fakePollTicker) tick() { t.ch <- time.Now() }

func (t *fakePollTicker) recordedPeriod() (time.Duration, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.period, t.period != 0
}

func (t *fakePollTicker) wasStopped() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stopped
}

// installFakePoller points the gate at a hand-driven ticker. It must be called
// before the poller goroutine starts, so the field write happens-before the
// read in RunPoller.
func installFakePoller(g *SupplyGate) *fakePollTicker {
	t := newFakePollTicker()
	g.newPollTicker = func(d time.Duration) pollTicker {
		t.mu.Lock()
		t.period = d
		t.mu.Unlock()
		return t
	}
	return t
}

func waitForSupplyContent(t *testing.T, reg *supply.Registry, k supply.Key, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got, ok := reg.Get(k); ok && string(got.Content) == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	got, ok := reg.Get(k)
	t.Fatalf("supply %v content = %q (cached=%v), want %q within 2s", k, got.Content, ok, want)
}

func waitForFetchCalls(t *testing.T, f *mutableFetcher, want int32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f.calls.Load() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("fetch calls = %d, want at least %d within 2s", f.calls.Load(), want)
}

// --- interval resolution ---

// The interval knob resolves to something pollable in every case, including the
// two that look like "turn it off".
func TestResolveSupplyPollInterval(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{"unset", 0, DefaultSupplyPollInterval},
		{"negative is not a disable switch", -time.Second, DefaultSupplyPollInterval},
		{"below the floor is raised", 5 * time.Second, MinSupplyPollInterval},
		{"at the floor", MinSupplyPollInterval, MinSupplyPollInterval},
		{"configured", 5 * time.Minute, 5 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveSupplyPollInterval(tc.in, quietLogger()); got != tc.want {
				t.Fatalf("resolveSupplyPollInterval(%v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// The config surfaces call this to refuse a value the operator cannot have
// meant: polling is always on, so an interval below the floor is an error
// rather than a silently raised cadence.
func TestValidateSupplyPollInterval(t *testing.T) {
	for _, d := range []time.Duration{0, MinSupplyPollInterval, DefaultSupplyPollInterval} {
		if err := ValidateSupplyPollInterval(d); err != nil {
			t.Fatalf("ValidateSupplyPollInterval(%v) = %v, want nil", d, err)
		}
	}
	for _, d := range []time.Duration{-time.Second, time.Second, MinSupplyPollInterval - time.Millisecond} {
		if err := ValidateSupplyPollInterval(d); err == nil {
			t.Fatalf("ValidateSupplyPollInterval(%v) = nil, want an error naming the floor", d)
		}
	}
}

// --- poll loop ---

// The TTL poll is the convergence path for a supply whose hint was lost: no
// hint is delivered here at all, and one tick must still apply the server's new
// content. The supply has no registered consumer — it is read only through
// $supplies — which is the case a consumer-count-pruned poll would strand.
func TestRunPollerConvergesAfterOneTickWithAllHintsLost(t *testing.T) {
	reg := supply.NewRegistry()
	f := newMutableFetcher()
	f.set("shared-rules", `v1`)
	g := NewSupplyGate(f, reg, quietLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := g.Admit(ctx, "wf", []engine.SupplyRequirement{
		{Node: "rules", Resource: "shared-rules", RequireReady: true},
	}); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if n := reg.ConsumerCount(defaultKey("rules")); n != 0 {
		t.Fatalf("test setup: consumer count = %d, want 0 (expression-only supply)", n)
	}

	// Republished while this process is up, with nothing to notify it.
	f.set("shared-rules", `v2`)

	ticker := installFakePoller(g)
	done := make(chan struct{})
	go func() { defer close(done); g.RunPoller(ctx, time.Hour) }()
	ticker.tick()
	waitForSupplyContent(t, reg, defaultKey("rules"), `v2`)

	// Unconditional per-key GET: the next tick fetches again even though the
	// content is unchanged, because discovering "unchanged" is what a tick is
	// for (D3).
	before := f.calls.Load()
	ticker.tick()
	waitForFetchCalls(t, f, before+1)

	cancel()
	<-done
}

// A runner that has admitted no supply hosts none: a tick costs nothing.
func TestRunPollerWithoutHostedSuppliesFetchesNothing(t *testing.T) {
	f := newMutableFetcher()
	g := NewSupplyGate(f, supply.NewRegistry(), quietLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ticker := installFakePoller(g)
	done := make(chan struct{})
	go func() { defer close(done); g.RunPoller(ctx, time.Hour) }()
	ticker.tick()
	// The tick is buffered, so reaching this point does not prove the poll
	// round ran; give the loop a window in which a fetch would have happened.
	time.Sleep(50 * time.Millisecond)
	if n := f.calls.Load(); n != 0 {
		t.Fatalf("fetch calls = %d, want 0 with nothing hosted", n)
	}
	cancel()
	<-done
}

// A tick that lands while a hint's fetch for the same key is still in flight
// must not fire a second one: both are asking the server for the same current
// content, and two results for one key could apply out of order.
//
// The tick is driven synchronously on the test goroutine while the hint's fetch
// is confirmed blocked inside Fetch, so the ordering is not a race the test
// hopes to win — the marker is guaranteed to be held before pollHosted runs.
func TestRunPollerDedupesAgainstAnInFlightHint(t *testing.T) {
	reg := supply.NewRegistry()
	release := make(chan struct{})
	started := make(chan struct{})
	f := &blockingFetcher{content: []byte(`v1`)}
	g := NewSupplyGate(f, reg, quietLogger())
	ctx := context.Background()

	if err := g.Admit(ctx, "wf", []engine.SupplyRequirement{
		{Node: "rules", Resource: "rules", RequireReady: true},
	}); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	// From here on every fetch blocks until released.
	f.onStart = func() { close(started); <-release }

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		g.ApplyHints(ctx, hints("rules", "sha256:changed"))
	}()
	<-started

	g.pollHosted(ctx)
	if n := f.calls.Load(); n != 2 {
		t.Fatalf("fetch calls = %d, want 2 (Admit plus the hint's); the tick must "+
			"dedupe against the fetch already in flight for this key", n)
	}

	close(release)
	wg.Wait()
	if n := f.calls.Load(); n != 2 {
		t.Fatalf("fetch calls = %d, want 2: neither path may fetch twice for one key", n)
	}
}

// The poll runs under the supply's own namespace, not the session context's.
// One runner hosts several namespaces at once, so RunPoller is started from a
// context that carries none; a fetch that inherited it would read the default
// namespace's content and publish it under the supply's real key.
func TestRunPollerFetchCarriesTheKeysNamespace(t *testing.T) {
	reg := supply.NewRegistry()
	f := &nsRecordingFetcher{stubFetcher: stubFetcher{
		content: map[string][]byte{"shared-rules": []byte(`v1`)},
	}}
	g := NewSupplyGate(f, reg, quietLogger())

	activationCtx := namespace.WithNamespace(context.Background(), "tenant-a")
	if err := g.Admit(activationCtx, "wf", []engine.SupplyRequirement{
		{Node: "rules", Resource: "shared-rules", RequireReady: true},
	}); err != nil {
		t.Fatalf("Admit: %v", err)
	}

	g.pollHosted(context.Background())

	got := f.namespaces()
	if len(got) != 2 {
		t.Fatalf("fetches = %d, want 2 (Admit plus one poll)", len(got))
	}
	for i, ns := range got {
		if ns != "tenant-a" {
			t.Fatalf("fetch %d ran under namespace %q, want tenant-a", i, ns)
		}
	}
	if _, ok := reg.Get(supply.KeyOf("tenant-a", "rules")); !ok {
		t.Fatal("the polled content did not land under (tenant-a, rules)")
	}
}

// A tick refreshes every hosted key, each under its own namespace, in a
// deterministic order — the same node name in two namespaces is two supplies.
func TestPollHostedRefreshesEveryNamespaceOfTheSameName(t *testing.T) {
	reg := supply.NewRegistry()
	f := newMutableFetcher()
	f.set("a-rules", `default-copy`)
	f.set("b-rules", `tenant-b-copy`)
	g := NewSupplyGate(f, reg, quietLogger())
	ctx := context.Background()

	if err := g.Admit(namespace.WithNamespace(ctx, "tenant-b"), "wf", []engine.SupplyRequirement{
		{Node: "rules", Resource: "b-rules", RequireReady: true},
	}); err != nil {
		t.Fatalf("Admit (tenant-b): %v", err)
	}
	if err := g.Admit(ctx, "wf", []engine.SupplyRequirement{
		{Node: "rules", Resource: "a-rules", RequireReady: true},
	}); err != nil {
		t.Fatalf("Admit (default): %v", err)
	}

	want := []supply.Key{
		supply.KeyOf(namespace.Default, "rules"),
		supply.KeyOf("tenant-b", "rules"),
	}
	if got := g.hostedKeys(); !reflect.DeepEqual(got, want) {
		t.Fatalf("hostedKeys() = %v, want %v (sorted by namespace then name, so a "+
			"tick's fetch order is deterministic)", got, want)
	}

	// Republish both; one tick must converge both keys independently.
	f.set("a-rules", `default-v2`)
	f.set("b-rules", `tenant-b-v2`)
	g.pollHosted(ctx)

	waitForSupplyContent(t, reg, supply.KeyOf(namespace.Default, "rules"), `default-v2`)
	waitForSupplyContent(t, reg, supply.KeyOf("tenant-b", "rules"), `tenant-b-v2`)
}

// Cancelling the context is how Run ends the poller on a reconnect; the ticker
// must not be left running.
func TestRunPollerStopsOnContextCancel(t *testing.T) {
	f := newMutableFetcher()
	g := NewSupplyGate(f, supply.NewRegistry(), quietLogger())
	ticker := installFakePoller(g)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); g.RunPoller(ctx, time.Hour) }()
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunPoller did not return after its context was cancelled")
	}
	if !ticker.wasStopped() {
		t.Fatal("RunPoller must stop its ticker on return")
	}
}

// The period is spread by ±10% so a fleet rolled out together does not fire its
// ticks together, and the resolution rules above survive the jitter.
func TestRunPollerJittersTheConfiguredInterval(t *testing.T) {
	for _, tc := range []struct {
		name       string
		configured time.Duration
		base       time.Duration
	}{
		{"unset uses the default", 0, DefaultSupplyPollInterval},
		{"below the floor uses the floor", 5 * time.Second, MinSupplyPollInterval},
		{"configured", 90 * time.Second, 90 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewSupplyGate(newMutableFetcher(), supply.NewRegistry(), quietLogger())
			ticker := installFakePoller(g)

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { defer close(done); g.RunPoller(ctx, tc.configured) }()
			defer func() { cancel(); <-done }()

			var period time.Duration
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				if p, ok := ticker.recordedPeriod(); ok {
					period = p
					break
				}
				time.Sleep(time.Millisecond)
			}
			if period == 0 {
				t.Fatal("RunPoller never asked for a ticker within 2s")
			}
			spread := tc.base / 10
			if period < tc.base-spread || period > tc.base+spread {
				t.Fatalf("ticker period = %v, want within ±10%% of %v", period, tc.base)
			}
		})
	}
}

// RunPoller is called from Run on whatever gate the config carries, including
// none: it must be nil-safe like every other SupplyGate entry point.
func TestRunPollerNilGateIsANoop(t *testing.T) {
	var g *SupplyGate
	g.RunPoller(context.Background(), time.Second) // must not panic or block
}

// The poller is only worth anything if Run starts it: the keys come from Admit
// (an activation), but the loop is Run's to launch. With no hint delivered,
// only a poller Run actually started can converge this supply.
func TestRunnerRunStartsTheSupplyPoller(t *testing.T) {
	reg := supply.NewRegistry()
	f := newMutableFetcher()
	f.set("shared-rules", `v1`)
	g := NewSupplyGate(f, reg, quietLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := g.Admit(ctx, "wf", []engine.SupplyRequirement{
		{Node: "rules", Resource: "shared-rules", RequireReady: true},
	}); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	ticker := installFakePoller(g)

	client := &registerKeyClient{}
	r := New(client, execution.NewRegistry(), Config{
		RunnerID:           "runner-1",
		Concurrency:        1,
		HeartbeatInterval:  5 * time.Millisecond,
		PollWait:           time.Millisecond,
		SupplyGate:         g,
		SupplyRegistry:     reg,
		SupplyPollInterval: time.Hour,
	})
	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(ctx) }()
	client.waitForBeats(t, 1)

	f.set("shared-rules", `v2`)
	ticker.tick()
	waitForSupplyContent(t, reg, defaultKey("rules"), `v2`)

	cancel()
	<-errCh
}
