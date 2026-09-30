package control

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
)

func aggRecord(runnerID, pool string, at time.Time, namespaces []namespace.Namespace, descriptors ...RunnerNodeDescriptor) RunnerDescriptorRecord {
	return RunnerDescriptorRecord{
		RunnerID: runnerID, PoolName: pool, PoolID: pool, RegisteredAt: at,
		Namespaces: namespaces, Descriptors: descriptors,
	}
}

func aggDescriptor(nodeType string, version int, hash string) RunnerNodeDescriptor {
	return RunnerNodeDescriptor{Type: nodeType, Version: version, Hash: hash, JSON: json.RawMessage(`{"Type":"` + nodeType + `","Docs":"` + hash + `"}`)}
}

func TestAggregateRunnerDescriptorsMergesIdenticalHashes(t *testing.T) {
	base := time.Unix(1_900_000_000, 0)
	records := []RunnerDescriptorRecord{
		aggRecord("r1", "pool-b", base, []namespace.Namespace{"tenant-b"}, aggDescriptor("acme.a", 1, "h1")),
		aggRecord("r2", "pool-a", base.Add(time.Second), []namespace.Namespace{"tenant-a", "default"}, aggDescriptor("acme.a", 1, "h1")),
		aggRecord("r3", "", base, nil, aggDescriptor("acme.a", 1, "h1")),
		aggRecord("r4", "pool-a", base, []namespace.Namespace{"tenant-a"}, aggDescriptor("acme.a", 1, "h1")),
	}
	got := AggregateRunnerDescriptors(records)
	if len(got) != 1 {
		t.Fatalf("aggregated = %+v, want one entry", got)
	}
	a := got[0]
	if a.Type != "acme.a" || a.Version != 1 || a.Hash != "h1" || a.DistinctHashes != 1 {
		t.Fatalf("entry = %+v", a)
	}
	if !reflect.DeepEqual(a.Pools, []string{"pool-a", "pool-b"}) {
		t.Fatalf("pools = %v, want sorted union without the pool-less runner", a.Pools)
	}
	if !reflect.DeepEqual(a.Namespaces, []namespace.Namespace{"default", "tenant-a", "tenant-b"}) {
		t.Fatalf("namespaces = %v", a.Namespaces)
	}
}

func TestAggregateRunnerDescriptorsPoolLessOnly(t *testing.T) {
	got := AggregateRunnerDescriptors([]RunnerDescriptorRecord{
		aggRecord("r1", "", time.Unix(1, 0), []namespace.Namespace{"default"}, aggDescriptor("acme.a", 1, "h")),
	})
	if len(got) != 1 || got[0].Pools != nil {
		t.Fatalf("aggregated = %+v, want one entry with no pools", got)
	}
}

func TestAggregateRunnerDescriptorsConflictLatestWins(t *testing.T) {
	base := time.Unix(1_900_000_000, 0)
	records := []RunnerDescriptorRecord{
		aggRecord("r1", "pool-a", base, []namespace.Namespace{"default"}, aggDescriptor("acme.a", 1, "old")),
		aggRecord("r2", "pool-b", base.Add(time.Minute), []namespace.Namespace{"default"}, aggDescriptor("acme.a", 1, "new")),
		aggRecord("r3", "pool-a", base.Add(-time.Minute), []namespace.Namespace{"default"}, aggDescriptor("acme.a", 1, "older")),
	}
	got := AggregateRunnerDescriptors(records)
	if len(got) != 1 || got[0].Hash != "new" || got[0].DistinctHashes != 3 {
		t.Fatalf("aggregated = %+v, want the latest registration to win with 3 hashes", got)
	}
	if string(got[0].JSON) != string(aggDescriptor("acme.a", 1, "new").JSON) {
		t.Fatalf("winner JSON = %s", got[0].JSON)
	}
	if !reflect.DeepEqual(got[0].Pools, []string{"pool-a", "pool-b"}) {
		t.Fatalf("pools = %v, want every reporting pool", got[0].Pools)
	}
}

func TestAggregateRunnerDescriptorsTieBreaksOnRunnerID(t *testing.T) {
	at := time.Unix(1_900_000_000, 0)
	a := aggRecord("runner-b", "p", at, nil, aggDescriptor("acme.a", 1, "from-b"))
	b := aggRecord("runner-a", "p", at, nil, aggDescriptor("acme.a", 1, "from-a"))
	for _, order := range [][]RunnerDescriptorRecord{{a, b}, {b, a}} {
		got := AggregateRunnerDescriptors(order)
		if len(got) != 1 || got[0].Hash != "from-a" {
			t.Fatalf("aggregated = %+v, want the smaller runner ID to win regardless of order", got)
		}
	}
}

func TestAggregateRunnerDescriptorsStableSort(t *testing.T) {
	at := time.Unix(1, 0)
	records := []RunnerDescriptorRecord{
		aggRecord("r2", "p", at, nil, aggDescriptor("acme.b", 1, "b1"), aggDescriptor("acme.a", 2, "a2")),
		aggRecord("r1", "p", at, nil, aggDescriptor("acme.a", 1, "a1"), aggDescriptor("acme.a", 10, "a10")),
	}
	want := []string{"acme.a/1", "acme.a/2", "acme.a/10", "acme.b/1"}
	first := AggregateRunnerDescriptors(records)
	for i := 0; i < 20; i++ {
		got := AggregateRunnerDescriptors(records)
		if !reflect.DeepEqual(got, first) {
			t.Fatalf("aggregation not deterministic:\n%+v\n%+v", got, first)
		}
	}
	var keys []string
	for _, a := range first {
		keys = append(keys, a.Type+"/"+itoa(a.Version))
	}
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("order = %v, want %v", keys, want)
	}
	if AggregateRunnerDescriptors(nil) == nil || len(AggregateRunnerDescriptors(nil)) != 0 {
		t.Fatal("empty input must aggregate to an empty, non-nil slice")
	}
}

func itoa(v int) string {
	raw, _ := json.Marshal(v)
	return string(raw)
}

func TestAggregateRunnerDescriptorsDoesNotAliasInput(t *testing.T) {
	d := aggDescriptor("acme.a", 1, "h")
	records := []RunnerDescriptorRecord{aggRecord("r1", "p", time.Unix(1, 0), nil, d)}
	got := AggregateRunnerDescriptors(records)
	got[0].JSON[0] = 'X'
	if records[0].Descriptors[0].JSON[0] != '{' {
		t.Fatal("aggregated JSON aliases the input record")
	}
}

func TestFilterRunnerDescriptorRecordsByNamespace(t *testing.T) {
	at := time.Unix(1, 0)
	records := []RunnerDescriptorRecord{
		aggRecord("r-a", "pool-a", at, []namespace.Namespace{"tenant-a"}, aggDescriptor("acme.x", 1, "from-a")),
		aggRecord("r-b", "pool-b", at.Add(time.Hour), []namespace.Namespace{"tenant-b"}, aggDescriptor("acme.x", 1, "from-b")),
		aggRecord("r-any", "pool-any", at, []namespace.Namespace{"*"}, aggDescriptor("acme.y", 1, "any")),
	}
	filtered := FilterRunnerDescriptorRecords(records, "tenant-a")
	got := AggregateRunnerDescriptors(filtered)
	if len(got) != 2 {
		t.Fatalf("aggregated = %+v, want acme.x and acme.y", got)
	}
	// tenant-b's later registration must not win for tenant-a.
	if got[0].Type != "acme.x" || got[0].Hash != "from-a" || !reflect.DeepEqual(got[0].Pools, []string{"pool-a"}) {
		t.Fatalf("acme.x = %+v, want tenant-a's own descriptor and pool only", got[0])
	}
	if got[1].Type != "acme.y" || !reflect.DeepEqual(got[1].Pools, []string{"pool-any"}) {
		t.Fatalf("acme.y = %+v, want the wildcard runner visible", got[1])
	}
	if other := FilterRunnerDescriptorRecords(records, "tenant-c"); len(other) != 1 || other[0].RunnerID != "r-any" {
		t.Fatalf("tenant-c records = %+v, want only the wildcard runner", other)
	}
}

type recordingConflictObserver struct {
	mu  sync.Mutex
	got map[string]int
}

func (o *recordingConflictObserver) OnRunnerDescriptorConflicts(_ context.Context, nodeType string, conflicts int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.got == nil {
		o.got = map[string]int{}
	}
	o.got[nodeType] = conflicts
}

func TestReportRunnerDescriptorConflicts(t *testing.T) {
	at := time.Unix(1, 0)
	aggregated := AggregateRunnerDescriptors([]RunnerDescriptorRecord{
		aggRecord("r1", "p", at, nil, aggDescriptor("acme.a", 1, "x"), aggDescriptor("acme.a", 2, "x"), aggDescriptor("acme.b", 1, "same")),
		aggRecord("r2", "p", at, nil, aggDescriptor("acme.a", 1, "y"), aggDescriptor("acme.a", 2, "y"), aggDescriptor("acme.b", 1, "same")),
	})
	logger := &recordingLogger{}
	observer := &recordingConflictObserver{}
	ReportRunnerDescriptorConflicts(context.Background(), logger, observer, aggregated)

	if !reflect.DeepEqual(observer.got, map[string]int{"acme.a": 2, "acme.b": 0}) {
		t.Fatalf("conflict gauges = %v, want acme.a=2 acme.b=0", observer.got)
	}
	warns := logger.withMsg("runner_descriptor_conflict")
	if len(warns) != 2 || warns[0].level != "warn" || warns[0].field("node_type") != "acme.a" {
		t.Fatalf("warns = %+v, want one per conflicting version", warns)
	}
	// Nil logger and observer are no-ops.
	ReportRunnerDescriptorConflicts(context.Background(), nil, nil, aggregated)
}

// TestAggregateRunnerDescriptorsConcurrentWithDirectory reads a directory
// while it is being re-registered, exercising the directory-to-aggregator path
// under -race.
func TestAggregateRunnerDescriptorsConcurrentWithDirectory(t *testing.T) {
	dir := NewMemoryRunnerDirectory()
	base := time.Unix(1_900_000_000, 0).UTC()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = dir.Register(context.Background(), RegisterRunnerRequest{
				RunnerID: "runner-1", Capacity: 1, InstanceUID: "i", Now: base,
				Descriptors: []RunnerNodeDescriptor{aggDescriptor("acme.a", 1, "h")},
			})
		}()
		go func() {
			defer wg.Done()
			records, err := dir.LiveRunnerDescriptors(context.Background(), base)
			if err != nil {
				t.Errorf("LiveRunnerDescriptors: %v", err)
				return
			}
			for _, a := range AggregateRunnerDescriptors(records) {
				if a.Hash != "h" {
					t.Errorf("aggregated %+v", a)
				}
			}
		}()
	}
	wg.Wait()
}
