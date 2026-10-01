package control

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	backendlocal "github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/namespace"
)

// countingDescriptorDirectory counts LiveRunnerDescriptors reads of the
// memory directory it wraps.
type countingDescriptorDirectory struct {
	*MemoryRunnerDirectory
	reads atomic.Int32
}

func (d *countingDescriptorDirectory) LiveRunnerDescriptors(ctx context.Context, now time.Time) ([]RunnerDescriptorRecord, error) {
	d.reads.Add(1)
	return d.MemoryRunnerDirectory.LiveRunnerDescriptors(ctx, now)
}

// LiveRunnerNodeTypes reuses one fleet read for every namespace within the
// cache TTL, re-reads once it passes, and gauges conflicts once per read.
func TestControlPlaneLiveRunnerNodeTypesCachesTheFleetRead(t *testing.T) {
	dir := &countingDescriptorDirectory{MemoryRunnerDirectory: NewMemoryRunnerDirectory()}
	cp, err := NewControlPlane(Config{Backend: backendlocal.New(), RunnerDirectory: dir})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	observer := &countingConflictObserver{}
	cp.runnerDescriptorConflicts = newRunnerDescriptorConflictReporter(nil, observer)
	base := time.Now().UTC()
	registerNodeTypesRunner(t, dir, "runner-a", "tenant-a", base, contractDescriptor("acme.a", 1, "A1"))
	now := base.Add(time.Second)

	for _, ns := range []namespace.Namespace{"tenant-a", "tenant-b", "tenant-a"} {
		if _, err := cp.LiveRunnerNodeTypes(context.Background(), ns, now); err != nil {
			t.Fatalf("LiveRunnerNodeTypes(%s): %v", ns, err)
		}
	}
	if got := dir.reads.Load(); got != 1 {
		t.Fatalf("directory reads = %d, want 1 within the TTL", got)
	}
	if got := observer.calls.Load(); got != 1 {
		t.Fatalf("conflict reports = %d, want 1 per read", got)
	}

	// A registration inside the TTL is not visible yet; past it, it is.
	registerNodeTypesRunner(t, dir, "runner-b", "tenant-a", base, contractDescriptor("acme.b", 1, "B1"))
	got, err := cp.LiveRunnerNodeTypes(context.Background(), "tenant-a", now.Add(runnerNodeTypesCacheTTL-time.Millisecond))
	if err != nil || len(got) != 1 {
		t.Fatalf("inside the TTL = %+v, %v; want the cached acme.a only", got, err)
	}
	got, err = cp.LiveRunnerNodeTypes(context.Background(), "tenant-a", now.Add(runnerNodeTypesCacheTTL))
	if err != nil || len(got) != 2 {
		t.Fatalf("at the TTL = %+v, %v; want acme.a and acme.b", got, err)
	}
	if got := dir.reads.Load(); got != 2 {
		t.Fatalf("directory reads = %d, want 2 after the TTL", got)
	}

	// A clock that moves backwards re-reads instead of trusting the cache.
	if _, err := cp.LiveRunnerNodeTypes(context.Background(), "tenant-a", now); err != nil {
		t.Fatal(err)
	}
	if got := dir.reads.Load(); got != 3 {
		t.Fatalf("directory reads = %d, want 3 after the clock went back", got)
	}
}

type countingConflictObserver struct{ calls atomic.Int32 }

func (o *countingConflictObserver) OnRunnerDescriptorConflicts(context.Context, string, int) {
	o.calls.Add(1)
}

func TestRunnerNodeTypesCacheDoesNotCacheErrors(t *testing.T) {
	cache := newRunnerNodeTypesCache(time.Minute)
	now := time.Unix(1_900_000_000, 0)
	var reads int
	fail := true
	fetch := func(context.Context) ([]RunnerDescriptorRecord, error) {
		reads++
		if fail {
			return nil, errors.New("redis down")
		}
		return []RunnerDescriptorRecord{aggRecord("r1", "p", now, nil, aggDescriptor("acme.a", 1, "h"))}, nil
	}
	var refreshed int
	onRefresh := func(context.Context, []RunnerDescriptorRecord) { refreshed++ }
	if _, err := cache.get(context.Background(), now, fetch, onRefresh); err == nil {
		t.Fatal("want the fetch error")
	}
	fail = false
	records, err := cache.get(context.Background(), now, fetch, onRefresh)
	if err != nil || len(records) != 1 {
		t.Fatalf("after recovery = %+v, %v", records, err)
	}
	if reads != 2 || refreshed != 1 {
		t.Fatalf("reads = %d, refreshed = %d; want 2 and 1", reads, refreshed)
	}
}

// Concurrent misses share one fetch, and every caller sees its result.
func TestRunnerNodeTypesCacheSharesConcurrentMisses(t *testing.T) {
	cache := newRunnerNodeTypesCache(time.Minute)
	now := time.Unix(1_900_000_000, 0)
	release := make(chan struct{})
	var reads, refreshed atomic.Int32
	fetch := func(context.Context) ([]RunnerDescriptorRecord, error) {
		reads.Add(1)
		<-release
		return []RunnerDescriptorRecord{
			aggRecord("r1", "p", now, []namespace.Namespace{"*"}, aggDescriptor("acme.a", 1, "h")),
			aggRecord("r2", "p", now, []namespace.Namespace{"*"}, aggDescriptor("acme.a", 1, "h")),
		}, nil
	}
	onRefresh := func(context.Context, []RunnerDescriptorRecord) { refreshed.Add(1) }

	const callers = 16
	var started, wg sync.WaitGroup
	started.Add(callers)
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			started.Done()
			records, err := cache.get(context.Background(), now, fetch, onRefresh)
			if err != nil || len(records) != 2 {
				t.Errorf("get = %d records, %v", len(records), err)
				return
			}
			for _, a := range AggregateRunnerDescriptors(FilterRunnerDescriptorRecords(records, "tenant-a")) {
				if a.Hash != "h" || len(a.JSON) == 0 {
					t.Errorf("aggregated %+v", a)
				}
			}
		}()
	}
	started.Wait()
	time.Sleep(10 * time.Millisecond)
	close(release)
	wg.Wait()
	// Any caller that arrived after the fetch finished hit the cache.
	if got := reads.Load(); got != 1 {
		t.Fatalf("fetches = %d, want 1", got)
	}
	if got := refreshed.Load(); got != 1 {
		t.Fatalf("refreshes = %d, want 1", got)
	}
}

// A caller whose context ends stops waiting, without failing the shared
// fetch: the result still lands in the cache for the next caller.
func TestRunnerNodeTypesCacheCallerCancellation(t *testing.T) {
	cache := newRunnerNodeTypesCache(time.Minute)
	now := time.Unix(1_900_000_000, 0)
	release := make(chan struct{})
	var reads atomic.Int32
	fetch := func(ctx context.Context) ([]RunnerDescriptorRecord, error) {
		reads.Add(1)
		<-release
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return []RunnerDescriptorRecord{aggRecord("r1", "p", now, nil, aggDescriptor("acme.a", 1, "h"))}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := cache.get(ctx, now, fetch, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled caller err = %v, want context.Canceled", err)
	}
	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for {
		records, err := cache.get(context.Background(), now, fetch, nil)
		if err != nil {
			t.Fatalf("get after cancel: %v", err)
		}
		if len(records) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("shared fetch never landed")
		}
	}
	if got := reads.Load(); got != 1 {
		t.Fatalf("fetches = %d, want the detached fetch reused", got)
	}
}

// Identical descriptors across runners share one buffer in the cached read.
func TestInternRunnerDescriptorJSON(t *testing.T) {
	at := time.Unix(1, 0)
	records := internRunnerDescriptorJSON([]RunnerDescriptorRecord{
		aggRecord("r1", "p", at, nil, aggDescriptor("acme.a", 1, "h"), aggDescriptor("acme.b", 1, "h")),
		aggRecord("r2", "p", at, nil, aggDescriptor("acme.a", 1, "h"), aggDescriptor("acme.b", 1, "other")),
	})
	a1, a2 := records[0].Descriptors[0].JSON, records[1].Descriptors[0].JSON
	if &a1[0] != &a2[0] {
		t.Error("acme.a@1/h is not shared between runners")
	}
	b1, b2 := records[0].Descriptors[1].JSON, records[1].Descriptors[1].JSON
	if &b1[0] == &b2[0] {
		t.Error("acme.b with different hashes share a buffer")
	}
	if &a1[0] == &b1[0] {
		t.Error("different types with the same hash share a buffer")
	}
}
