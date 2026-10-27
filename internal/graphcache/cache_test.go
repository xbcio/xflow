package graphcache

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// fakeClock keeps the idle sweep deterministic: Put only sweeps once per
// SweepInterval, so every test that exercises expiry moves this clock instead
// of sleeping.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newGraph() *graph.Graph { return &graph.Graph{} }

// TestCacheIdleTTL pins the primary bound: an entry no task has read within
// IdleTTL is dropped, while an entry that keeps being read survives even though
// it was inserted at the same time.
//
// This is the shape of the leak it exists for: a replica that executed one node
// of an execution never reads that graph again, while the replica still driving
// the execution re-reads it on every task.
func TestCacheIdleTTL(t *testing.T) {
	clk := newFakeClock()
	c := New(Options{IdleTTL: 30 * time.Minute, SweepInterval: time.Minute, Now: clk.now})

	c.Put("exec-untouched", newGraph(), 0)
	c.Put("exec-active", newGraph(), 0)

	// The active execution keeps producing tasks.
	clk.advance(10 * time.Minute)
	if _, ok := c.Get("exec-active"); !ok {
		t.Fatal("✓ entry must be present before its TTL lapses")
	}

	// 35 minutes after insertion the untouched entry is expired; the sweep runs
	// on this Put because SweepInterval has elapsed.
	clk.advance(25 * time.Minute)
	c.Put("exec-new", newGraph(), 0)

	if _, ok := c.Get("exec-untouched"); ok {
		t.Error("✓ entry untouched for longer than IdleTTL must be evicted")
	}
	if _, ok := c.Get("exec-active"); !ok {
		t.Error("✓ a Get within the window must extend the entry's life")
	}
	if _, ok := c.Get("exec-new"); !ok {
		t.Error("✓ freshly inserted entry must survive the sweep")
	}
	if got := c.Stats().EvictedIdle; got != 1 {
		t.Errorf("evicted by idle = %d, want 1", got)
	}
}

// TestCacheEntryCap covers the entry-count safety net: over the cap, the
// least-recently-used entries go first, and Get refreshes recency.
func TestCacheEntryCap(t *testing.T) {
	clk := newFakeClock()
	c := New(Options{MaxEntries: 3, Now: clk.now})

	for _, id := range []types.ExecutionID{"a", "b", "c"} {
		c.Put(id, newGraph(), 0)
		clk.advance(time.Second)
	}
	// Touch "a" so it is no longer the oldest, then overflow the cap.
	if _, ok := c.Get("a"); !ok {
		t.Fatal("a must be cached")
	}
	clk.advance(time.Second)
	c.Put("d", newGraph(), 0)

	if got := c.Stats().Entries; got != 3 {
		t.Fatalf("entries = %d, want 3 (cap)", got)
	}
	if _, ok := c.Get("b"); ok {
		t.Error("✓ the least recently used entry (b) is the one evicted")
	}
	for _, id := range []types.ExecutionID{"a", "c", "d"} {
		if _, ok := c.Get(id); !ok {
			t.Errorf("✓ %s must survive: recency, not insertion order, decides", id)
		}
	}
	if got := c.Stats().EvictedCapacity; got != 1 {
		t.Errorf("evicted by capacity = %d, want 1", got)
	}
}

// TestCacheByteCap covers the byte budget: the caller's size estimate is what
// the cap counts, and unknown sizes fall back to DefaultEntryBytes.
func TestCacheByteCap(t *testing.T) {
	clk := newFakeClock()
	c := New(Options{MaxBytes: 100, Now: clk.now})

	c.Put("a", newGraph(), 60)
	clk.advance(time.Second)
	c.Put("b", newGraph(), 60) // 120 > 100: one entry must go

	stats := c.Stats()
	if stats.Bytes > 100 {
		t.Errorf("bytes = %d, want <= 100", stats.Bytes)
	}
	if stats.Entries != 1 {
		t.Fatalf("entries = %d, want 1", stats.Entries)
	}
	if _, ok := c.Get("a"); ok {
		t.Error("✓ the older entry is evicted to satisfy the byte budget")
	}
	if _, ok := c.Get("b"); !ok {
		t.Error("✓ the entry just written survives its own trim")
	}

	// Unknown size is charged the conservative default so the budget still binds.
	c2 := New(Options{MaxBytes: 2 * DefaultEntryBytes, Now: clk.now})
	c2.Put("x", newGraph(), 0)
	if got := c2.Stats().Bytes; got != DefaultEntryBytes {
		t.Errorf("unknown-size entry charged %d bytes, want %d", got, DefaultEntryBytes)
	}
}

// TestCacheDeleteDoesNotNotify pins the lifetime rule the engine relies on:
// explicit Delete is the terminal-transition path (the caller already holds the
// id and does its own cleanup), while sweep/capacity evictions notify so the
// caller can drop anything it keys by the same id.
func TestCacheDeleteDoesNotNotify(t *testing.T) {
	clk := newFakeClock()
	var notified []types.ExecutionID
	c := New(Options{
		MaxEntries:    1,
		IdleTTL:       time.Minute,
		SweepInterval: time.Minute,
		Now:           clk.now,
		OnEvict:       func(id types.ExecutionID) { notified = append(notified, id) },
	})

	c.Put("explicit", newGraph(), 0)
	c.Delete("explicit")
	if len(notified) != 0 {
		t.Fatalf("Delete notified %v, want no notification", notified)
	}
	if got := c.Stats().EvictedExplicit; got != 1 {
		t.Errorf("explicit evictions = %d, want 1", got)
	}
	c.Delete("never-cached") // idempotent

	// Capacity eviction notifies.
	c.Put("first", newGraph(), 0)
	clk.advance(time.Second) // tied `used` would pick the victim by map order
	c.Put("second", newGraph(), 0)
	if len(notified) != 1 || notified[0] != "first" {
		t.Fatalf("capacity eviction notified %v, want [first]", notified)
	}
}

// TestCacheZeroOptionsIsUnbounded keeps the previous behaviour available:
// embedders that pass no limits keep every entry, as the plain maps did.
func TestCacheZeroOptionsIsUnbounded(t *testing.T) {
	clk := newFakeClock()
	c := New(Options{Now: clk.now})
	for i := 0; i < 100; i++ {
		c.Put(types.ExecutionID(fmt.Sprintf("exec-%d", i)), newGraph(), 0)
	}
	clk.advance(24 * time.Hour)
	c.Put("exec-late", newGraph(), 0)

	if got := c.Stats().Entries; got != 101 {
		t.Fatalf("entries = %d, want 101 (no bounds configured)", got)
	}
}

// TestCacheOnSweepReportsPostSweepState covers the reporting hook: it fires at
// most once per interval, with the state left behind by the sweep.
func TestCacheOnSweepReportsPostSweepState(t *testing.T) {
	clk := newFakeClock()
	var reports []Stats
	c := New(Options{
		IdleTTL:       time.Minute,
		SweepInterval: time.Minute,
		Now:           clk.now,
		OnSweep:       func(st Stats) { reports = append(reports, st) },
	})

	c.Put("a", newGraph(), 100) // first Put sweeps (empty)
	clk.advance(2 * time.Minute)
	c.Put("b", newGraph(), 100) // sweeps: "a" is idle-expired

	if len(reports) != 2 {
		t.Fatalf("sweep reports = %d, want 2", len(reports))
	}
	last := reports[len(reports)-1]
	if last.Entries != 1 || last.Bytes != 100 {
		t.Errorf("post-sweep state = %d entries / %d bytes, want 1 / 100", last.Entries, last.Bytes)
	}
	if last.EvictedIdle != 1 {
		t.Errorf("post-sweep evicted_idle = %d, want 1", last.EvictedIdle)
	}

	// A Put inside the interval must not report again.
	c.Put("c", newGraph(), 100)
	if len(reports) != 2 {
		t.Errorf("sweep reports = %d after a put inside the interval, want 2", len(reports))
	}
}

// TestCacheConcurrentAccess is the race-detector test: the task path reads and
// writes from every worker goroutine while the sweep and the capacity trim
// mutate the same map.
func TestCacheConcurrentAccess(t *testing.T) {
	c := New(Options{MaxEntries: 16, IdleTTL: time.Hour, SweepInterval: time.Nanosecond})

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			id := types.ExecutionID(fmt.Sprintf("exec-%d", w%4))
			for i := 0; i < 200; i++ {
				c.Put(id, newGraph(), int64(64+i))
				c.Get(id)
				if i%17 == 0 {
					c.Delete(id)
				}
				_ = c.Stats()
			}
		}(w)
	}
	wg.Wait()

	if got := c.Stats().Entries; got > 16 {
		t.Fatalf("entries = %d, want <= 16", got)
	}
}
