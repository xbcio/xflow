package control

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

// runnerNodeTypesCacheTTL bounds how long one fleet-wide descriptor read
// serves GET /v1/node-types. Reading is O(runners x envelope size) — on Redis
// an HGETALL of every runner's copy plus a decode of each — and an editor
// page load issues it per request. Descriptors change only on register or
// remove, and a runner's liveness window is DefaultRunnerLiveTTL (30s), so a
// couple of seconds of staleness is invisible next to it.
const runnerNodeTypesCacheTTL = 2 * time.Second

// runnerNodeTypesCache holds the latest unfiltered live descriptor read of
// the whole fleet. The clock is the caller's now — the same instant that
// decides liveness — so a test that injects a clock drives expiry with it
// too. A read is reused while 0 <= now-fetchedAt < ttl; a clock that moves
// backwards forces a re-read. Concurrent misses share one directory read.
// Errors are never cached.
//
// Cached records are shared between callers and must be treated as
// read-only: AggregateRunnerDescriptors only reads them and copies what it
// returns.
type runnerNodeTypesCache struct {
	ttl time.Duration

	mu        sync.Mutex
	valid     bool
	fetchedAt time.Time
	records   []RunnerDescriptorRecord
	inflight  *runnerNodeTypesFetch
}

type runnerNodeTypesFetch struct {
	done    chan struct{}
	records []RunnerDescriptorRecord
	err     error
}

func newRunnerNodeTypesCache(ttl time.Duration) *runnerNodeTypesCache {
	return &runnerNodeTypesCache{ttl: ttl}
}

// get returns the cached fleet read when it is fresh at now, or performs
// fetch. onRefresh runs once per successful fetch, before any caller sees the
// new records, so per-fleet side effects (the conflict log and gauge) happen
// once per read rather than once per request. The shared fetch runs detached
// from the leading caller's cancellation, so one abandoned request cannot fail
// the others waiting on it; every caller still returns on its own ctx.
func (c *runnerNodeTypesCache) get(
	ctx context.Context,
	now time.Time,
	fetch func(context.Context) ([]RunnerDescriptorRecord, error),
	onRefresh func(context.Context, []RunnerDescriptorRecord),
) ([]RunnerDescriptorRecord, error) {
	if c == nil {
		records, err := fetch(ctx)
		if err == nil && onRefresh != nil {
			onRefresh(ctx, records)
		}
		return records, err
	}
	c.mu.Lock()
	if c.valid && !now.Before(c.fetchedAt) && now.Sub(c.fetchedAt) < c.ttl {
		records := c.records
		c.mu.Unlock()
		return records, nil
	}
	f := c.inflight
	if f == nil {
		f = &runnerNodeTypesFetch{done: make(chan struct{})}
		c.inflight = f
		go c.refresh(context.WithoutCancel(ctx), now, f, fetch, onRefresh)
	}
	c.mu.Unlock()

	select {
	case <-f.done:
		return f.records, f.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *runnerNodeTypesCache) refresh(
	ctx context.Context,
	now time.Time,
	f *runnerNodeTypesFetch,
	fetch func(context.Context) ([]RunnerDescriptorRecord, error),
	onRefresh func(context.Context, []RunnerDescriptorRecord),
) {
	records, err := fetch(ctx)
	if err == nil {
		records = internRunnerDescriptorJSON(records)
		if onRefresh != nil {
			onRefresh(ctx, records)
		}
	}
	c.mu.Lock()
	c.inflight = nil
	if err == nil {
		c.valid, c.fetchedAt, c.records = true, now, records
	}
	c.mu.Unlock()
	f.records, f.err = records, err
	close(f.done)
}

// internRunnerDescriptorJSON makes every descriptor with the same (type,
// version, hash) share one JSON buffer. Every runner in a pool stores its own copy of what is
// usually an identical set, so the cached read holds one copy per distinct
// descriptor instead of one per runner. The hash is the sha256 of exactly
// these canonical bytes (validateRunnerDescriptors), so sharing by hash never
// changes a value.
func internRunnerDescriptorJSON(records []RunnerDescriptorRecord) []RunnerDescriptorRecord {
	type key struct {
		nodeType string
		version  int
		hash     string
	}
	shared := make(map[key]json.RawMessage)
	for i := range records {
		for j := range records[i].Descriptors {
			d := &records[i].Descriptors[j]
			if d.Hash == "" {
				continue
			}
			k := key{nodeType: d.Type, version: d.Version, hash: d.Hash}
			if buf, ok := shared[k]; ok {
				d.JSON = buf
				continue
			}
			shared[k] = d.JSON
		}
	}
	return records
}
