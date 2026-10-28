package rstate

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

// registrySAddCounter counts client SADDs into the namespace registry and can
// refuse them. Matching on the key matters: an execution also issues SADDs
// elsewhere, and only the registry write is under test.
type registrySAddCounter struct {
	mu   sync.Mutex
	n    int
	fail bool
}

func (c *registrySAddCounter) hook() server.Hook {
	return func(p *server.Peer, cmd string, args ...string) bool {
		if !strings.EqualFold(cmd, "sadd") || len(args) == 0 || args[0] != namespaceSetKey {
			return false
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		c.n++
		if c.fail {
			p.WriteError("injected registry failure")
			return true
		}
		return false
	}
}

func (c *registrySAddCounter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

func (c *registrySAddCounter) setFail(v bool) {
	c.mu.Lock()
	c.fail = v
	c.mu.Unlock()
}

// throttleClock is the injected clock: tests advance it instead of sleeping.
type throttleClock struct{ now time.Time }

func (c *throttleClock) Now() time.Time          { return c.now }
func (c *throttleClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

func newThrottleTestStore(t *testing.T) (*Store, *registrySAddCounter, *throttleClock, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	counter := &registrySAddCounter{}
	mr.Server().SetPreHook(counter.hook())
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	clock := &throttleClock{now: time.Date(2026, 10, 28, 9, 0, 0, 0, time.UTC)}
	state := New(rdb, nil, time.Hour)
	state.nsClock = clock.Now
	return state, counter, clock, mr
}

// seedDistinctEntry admits one new execution (a fresh admission key each call)
// into ns through the real entry-seed path.
func seedDistinctEntry(t *testing.T, state *Store, ns namespace.Namespace, key string) {
	t.Helper()
	g := seedMetricsGraph(t, "throttle-"+key)
	gm := g.Groups()[0]
	sinkIdx, ok := g.NodeIndex("sink")
	if !ok {
		t.Fatal("sink node not found in the compiled graph")
	}
	exits := []engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"x": 1}}}
	resp, err := state.SeedExecutionFromEntry(namespace.WithNamespace(context.Background(), ns), engine.SeedExecutionFromEntryRequest{
		AdmissionKey:    engine.AdmissionKey(key),
		WorkflowID:      "wf-throttle",
		WorkflowVersion: "v1",
		Namespace:       ns,
		EntryUnitID:     gm.Name,
		EntryUnitIdx:    gm.UnitIdx,
		Graph:           g,
		Outcome:         engine.GroupOutcomeSuccess,
		Exits:           exits,
		Downstream: []engine.DownstreamArrival{{
			NodeName: "sink", NodeIdx: sinkIdx, UnitIdx: g.UnitIndexForNode(sinkIdx),
			ArrivalCount: 1, ActiveCount: 1,
		}},
		ResultHash: engine.ComputeResultHash(engine.GroupOutcomeSuccess, exits),
	})
	if err != nil || resp.State != engine.AdmissionStateAccepted {
		t.Fatalf("SeedExecutionFromEntry(%s, %s) = %+v, %v, want accepted", ns, key, resp, err)
	}
}

func registryHas(mr *miniredis.Miniredis, ns namespace.Namespace) bool {
	ok, _ := mr.SIsMember(namespaceSetKey, string(ns))
	return ok
}

// TestEntrySeedRegistersTheNamespaceOncePerWindow is the cost contract of the
// entry-seed registration: it is on the collection hot path, so a namespace is
// SADDed once per window per Store, not once per seed.
func TestEntrySeedRegistersTheNamespaceOncePerWindow(t *testing.T) {
	t.Run("same namespace, many seeds, one SADD", func(t *testing.T) {
		state, counter, _, mr := newThrottleTestStore(t)
		const seeds = 8
		for i := 0; i < seeds; i++ {
			seedDistinctEntry(t, state, "tenant-x", "same-ns-"+string(rune('a'+i)))
		}
		if got := counter.count(); got != 1 {
			t.Fatalf("registry SADDs after %d seeds in one namespace = %d, want 1", seeds, got)
		}
		if !registryHas(mr, "tenant-x") {
			t.Fatal("tenant-x was not registered")
		}
	})

	t.Run("a seed after the window registers again", func(t *testing.T) {
		state, counter, clock, _ := newThrottleTestStore(t)
		seedDistinctEntry(t, state, "tenant-x", "w1")
		clock.Advance(namespaceRegisterInterval - time.Second)
		seedDistinctEntry(t, state, "tenant-x", "w2")
		if got := counter.count(); got != 1 {
			t.Fatalf("SADDs just inside the window = %d, want 1", got)
		}
		clock.Advance(time.Second)
		seedDistinctEntry(t, state, "tenant-x", "w3")
		if got := counter.count(); got != 2 {
			t.Fatalf("SADDs once the window elapsed = %d, want 2", got)
		}
		seedDistinctEntry(t, state, "tenant-x", "w4")
		if got := counter.count(); got != 2 {
			t.Fatalf("SADDs right after the refresh = %d, want 2 (a new window started)", got)
		}
	})

	t.Run("namespaces are throttled independently", func(t *testing.T) {
		state, counter, _, mr := newThrottleTestStore(t)
		seedDistinctEntry(t, state, "tenant-x", "ind-x1")
		seedDistinctEntry(t, state, "tenant-y", "ind-y1")
		seedDistinctEntry(t, state, "tenant-x", "ind-x2")
		seedDistinctEntry(t, state, "tenant-y", "ind-y2")
		if got := counter.count(); got != 2 {
			t.Fatalf("SADDs for two namespaces = %d, want 2 (one each)", got)
		}
		if !registryHas(mr, "tenant-x") || !registryHas(mr, "tenant-y") {
			t.Fatal("both namespaces must be registered")
		}
	})

	// The reason the record expires instead of being permanent: a Redis that was
	// flushed or failed over loses the registry, and the next seed after the
	// window must put the namespace back or its outbox is never discovered.
	t.Run("a wiped registry heals after the window", func(t *testing.T) {
		state, counter, clock, mr := newThrottleTestStore(t)
		seedDistinctEntry(t, state, "tenant-x", "heal1")
		mr.Del(namespaceSetKey)
		clock.Advance(namespaceRegisterInterval)
		seedDistinctEntry(t, state, "tenant-x", "heal2")
		if got := counter.count(); got != 2 {
			t.Fatalf("SADDs = %d, want 2", got)
		}
		if !registryHas(mr, "tenant-x") {
			t.Fatal("tenant-x was not re-registered after the registry was wiped")
		}
	})

	// A window must only be opened by a SADD that worked, or one transient Redis
	// error would leave the namespace unregistered for a whole minute.
	t.Run("a failed SADD does not open the window", func(t *testing.T) {
		state, counter, _, mr := newThrottleTestStore(t)
		counter.setFail(true)
		seedDistinctEntry(t, state, "tenant-x", "fail1")
		if registryHas(mr, "tenant-x") {
			t.Fatal("namespace registered although the SADD was refused")
		}
		counter.setFail(false)
		seedDistinctEntry(t, state, "tenant-x", "fail2")
		if got := counter.count(); got != 2 {
			t.Fatalf("SADDs = %d, want 2 (the retry must not be suppressed by the failed attempt)", got)
		}
		if !registryHas(mr, "tenant-x") {
			t.Fatal("tenant-x was not registered by the retry")
		}
	})
}

// TestRegisterNamespaceThrottledIsSafeUnderConcurrency covers the worst case the
// doc comment admits: concurrent first calls may each SADD, never more than one
// per caller, and the namespace ends up registered. Run with -race.
func TestRegisterNamespaceThrottledIsSafeUnderConcurrency(t *testing.T) {
	state, counter, _, mr := newThrottleTestStore(t)
	const callers = 16
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = state.registerNamespaceThrottled(context.Background(), "tenant-x")
		}()
	}
	wg.Wait()
	if got := counter.count(); got < 1 || got > callers {
		t.Fatalf("SADDs = %d, want between 1 and %d", got, callers)
	}
	before := counter.count()
	_ = state.registerNamespaceThrottled(context.Background(), "tenant-x")
	if counter.count() != before {
		t.Fatal("a call after the window opened must not SADD")
	}
	if !registryHas(mr, "tenant-x") {
		t.Fatal("tenant-x was not registered")
	}
}
