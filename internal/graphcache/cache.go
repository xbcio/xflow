// Package graphcache bounds the process-local caches of decoded execution
// graphs.
//
// Both caches that hold a decoded *graph.Graph — engine.Engine's dispatch cache
// and rstate.Store's load cache — are populated by any replica that executes a
// single node of an execution, but evicted only by the replica that applies the
// execution's terminal transition. With several replicas sharing one task
// queue, that leaves every non-finalizing replica pinning the graphs it loaded
// until the process restarts. Measured on a six-replica deployment, one process
// held 52k-69k graphs (1.0-1.1 GB, ~69% of its heap) and grew ~4 MB/min while
// the system was otherwise idle.
//
// Cache adds the two bounds those maps never had:
//
//   - IdleTTL drops entries that have not been read for the configured window.
//     This is the bound that matches the leak: a replica's copy of a graph is
//     only useful while tasks of that execution keep arriving, and a
//     non-finalizing replica stops seeing them as soon as the execution ends.
//   - MaxEntries/MaxBytes are coarse safety nets for bursts. A miss is correct
//     — the graph is re-read from Redis and decoded again — so early eviction
//     costs latency, never correctness.
//
// The zero Options means "unbounded", which preserves the previous behaviour
// for embedders that construct the caches without limits.
package graphcache

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

const (
	// DefaultMaxEntries is a secondary guard on top of DefaultMaxBytes: it bounds
	// the map and per-entry overhead, which the byte estimate does not model. At
	// the ~21 KB graphs observed in production the byte cap binds first.
	DefaultMaxEntries = 5000
	// DefaultMaxBytes bounds one cache's estimated heap footprint. With ~21 KB
	// graphs that is roughly 6,000 entries — far above the working set of a few
	// dozen concurrent executions, so the idle TTL stays the binding constraint
	// in steady state.
	DefaultMaxBytes = 128 << 20
	// DefaultIdleTTL is deliberately larger than the longest gap between two
	// tasks of one execution (a long-running node produces no graph reads while
	// it runs), so an execution that is still making progress is not evicted
	// between its nodes.
	DefaultIdleTTL = 30 * time.Minute
	// DefaultSweepInterval throttles the idle sweep, which runs opportunistically
	// on Put so the cache needs no goroutine and no lifecycle.
	DefaultSweepInterval = time.Minute
	// DefaultEntryBytes sizes an entry whose caller cannot measure the graph.
	// It over-estimates the observed 21 KB average on purpose: over-estimating
	// evicts earlier, which only costs a re-decode.
	DefaultEntryBytes = 64 << 10
)

// Options configures a Cache. The zero value is unbounded.
type Options struct {
	// MaxEntries caps the number of cached graphs. 0 disables the cap.
	MaxEntries int
	// MaxBytes caps the estimated heap footprint. 0 disables the cap.
	MaxBytes int64
	// IdleTTL drops entries untouched for this long. 0 disables idle expiry.
	IdleTTL time.Duration
	// SweepInterval throttles the idle sweep. 0 means DefaultSweepInterval,
	// capped at IdleTTL so an entry never outlives its deadline by more than
	// one interval.
	SweepInterval time.Duration
	// OnEvict, when set, is called for every entry dropped by the idle sweep or
	// by a capacity cap. It is called without the cache lock held, but it must
	// not call back into this Cache. Explicit Delete does not call it: the
	// caller already knows which id it removed.
	OnEvict func(types.ExecutionID)
	// OnSweep, when set, is called after each idle sweep (at most once per
	// SweepInterval) with the cache's post-sweep state. It is the reporting
	// hook: the cache itself has no logger and no goroutine, so whoever
	// constructs it decides where the numbers go.
	OnSweep func(Stats)
	// Now overrides the clock. Tests only; nil means time.Now.
	Now func() time.Time
}

// Stats is a point-in-time view of one cache.
type Stats struct {
	Entries         int
	Bytes           int64
	EvictedIdle     int64
	EvictedCapacity int64
	EvictedExplicit int64
}

type entry struct {
	// used is unix nanos and is written under the read lock, so it must stay
	// the first field: the first word of an allocated struct is the one Go
	// guarantees 64-bit aligned for atomic access.
	used int64
	g    *graph.Graph
	size int64
}

// Cache is a bounded, concurrency-safe map of execution id to decoded graph.
type Cache struct {
	opts Options

	mu        sync.RWMutex
	entries   map[types.ExecutionID]*entry
	bytes     int64
	lastSweep time.Time

	evictedIdle     int64
	evictedCapacity int64
	evictedExplicit int64
}

// New builds a cache. A nil-receiver is never returned; callers that want the
// old unbounded behaviour pass Options{}.
func New(opts Options) *Cache {
	if opts.SweepInterval <= 0 {
		// Never sweep less often than the TTL: otherwise an entry can outlive
		// its deadline by a whole interval before anything notices.
		opts.SweepInterval = DefaultSweepInterval
		if opts.IdleTTL > 0 && opts.IdleTTL < opts.SweepInterval {
			opts.SweepInterval = opts.IdleTTL
		}
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Cache{
		opts:    opts,
		entries: make(map[types.ExecutionID]*entry),
	}
}

// Get returns the cached graph and refreshes its idle deadline.
//
// The refresh is an atomic store, not a list reordering, so Get stays on the
// read lock: it runs once per dispatched task and must not serialize the
// engine's workers.
func (c *Cache) Get(id types.ExecutionID) (*graph.Graph, bool) {
	c.mu.RLock()
	e, ok := c.entries[id]
	c.mu.RUnlock()
	if !ok {
		return nil, false
	}
	atomic.StoreInt64(&e.used, c.opts.Now().UnixNano())
	return e.g, true
}

// Put inserts or replaces id's graph. size is the caller's estimate of the
// graph's heap footprint; <= 0 falls back to DefaultEntryBytes.
//
// The idle sweep and the capacity trims run here, at most once per
// SweepInterval, so the cache needs neither a goroutine nor a Close method.
func (c *Cache) Put(id types.ExecutionID, g *graph.Graph, size int64) {
	if g == nil {
		return
	}
	if size <= 0 {
		size = DefaultEntryBytes
	}
	now := c.opts.Now()

	var evicted []types.ExecutionID
	swept := false
	c.mu.Lock()
	if old, ok := c.entries[id]; ok {
		c.bytes -= old.size
	}
	c.entries[id] = &entry{g: g, size: size, used: now.UnixNano()}
	c.bytes += size

	if c.opts.IdleTTL > 0 && now.Sub(c.lastSweep) >= c.opts.SweepInterval {
		c.lastSweep = now
		evicted = c.sweepLocked(now)
		swept = true
	}
	if c.overCapacityLocked() {
		evicted = append(evicted, c.trimLocked()...)
	}
	c.mu.Unlock()

	c.notify(evicted)
	if swept && c.opts.OnSweep != nil {
		c.opts.OnSweep(c.Stats())
	}
}

// Delete drops one entry explicitly; it is the terminal-transition path and
// does not fire OnEvict.
func (c *Cache) Delete(id types.ExecutionID) {
	c.mu.Lock()
	if e, ok := c.entries[id]; ok {
		delete(c.entries, id)
		c.bytes -= e.size
		c.evictedExplicit++
	}
	c.mu.Unlock()
}

// Stats reports the cache's current size and eviction counters.
func (c *Cache) Stats() Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return Stats{
		Entries:         len(c.entries),
		Bytes:           c.bytes,
		EvictedIdle:     c.evictedIdle,
		EvictedCapacity: c.evictedCapacity,
		EvictedExplicit: c.evictedExplicit,
	}
}

// sweepLocked drops entries untouched for longer than IdleTTL and returns their
// ids. Caller holds the write lock.
func (c *Cache) sweepLocked(now time.Time) []types.ExecutionID {
	cutoff := now.Add(-c.opts.IdleTTL).UnixNano()
	var evicted []types.ExecutionID
	for id, e := range c.entries {
		if atomic.LoadInt64(&e.used) < cutoff {
			delete(c.entries, id)
			c.bytes -= e.size
			c.evictedIdle++
			evicted = append(evicted, id)
		}
	}
	return evicted
}

func (c *Cache) overCapacityLocked() bool {
	if c.opts.MaxEntries > 0 && len(c.entries) > c.opts.MaxEntries {
		return true
	}
	return c.opts.MaxBytes > 0 && c.bytes > c.opts.MaxBytes
}

// trimLocked evicts least-recently-used entries until the cache is back under
// both caps. Sorting is acceptable here: it only runs when a cap is exceeded,
// and the entry count is in the thousands.
func (c *Cache) trimLocked() []types.ExecutionID {
	type candidate struct {
		id   types.ExecutionID
		used int64
	}
	cands := make([]candidate, 0, len(c.entries))
	for id, e := range c.entries {
		cands = append(cands, candidate{id: id, used: atomic.LoadInt64(&e.used)})
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].used < cands[j].used })

	var evicted []types.ExecutionID
	for _, cand := range cands {
		if !c.overCapacityLocked() {
			break
		}
		e, ok := c.entries[cand.id]
		if !ok {
			continue
		}
		delete(c.entries, cand.id)
		c.bytes -= e.size
		c.evictedCapacity++
		evicted = append(evicted, cand.id)
	}
	return evicted
}

func (c *Cache) notify(ids []types.ExecutionID) {
	if c.opts.OnEvict == nil {
		return
	}
	for _, id := range ids {
		c.opts.OnEvict(id)
	}
}
