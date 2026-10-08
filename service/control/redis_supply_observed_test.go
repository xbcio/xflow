package control

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func newRedisSupplyObservedForTest(t *testing.T) (*RedisSupplyObserved, *redis.Client) {
	t.Helper()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	return NewRedisSupplyObserved(rdb, DefaultSupplyObservedRetention, nil), rdb
}

func TestRedisSupplyObservedRecordThenSnapshotRoundTrips(t *testing.T) {
	sink, _ := newRedisSupplyObservedForTest(t)

	sink.Record("runner-a", map[string]string{"rules": "hash-a"})
	sink.Record("runner-b", map[string]string{"rules": "hash-b", "models": "hash-m"})

	got := sink.Snapshot()
	if len(got) != 2 {
		t.Fatalf("Snapshot returned %d runners, want 2: %#v", len(got), got)
	}
	if got["runner-a"]["rules"] != "hash-a" {
		t.Errorf("runner-a = %#v, want rules=hash-a", got["runner-a"])
	}
	if got["runner-b"]["rules"] != "hash-b" || got["runner-b"]["models"] != "hash-m" {
		t.Errorf("runner-b = %#v, want both supplies", got["runner-b"])
	}
}

func TestRedisSupplyObservedRecordReplacesNotMerges(t *testing.T) {
	sink, _ := newRedisSupplyObservedForTest(t)

	sink.Record("runner-a", map[string]string{"rules": "hash-1", "models": "hash-m"})
	sink.Record("runner-a", map[string]string{"rules": "hash-2"})

	report := sink.Snapshot()["runner-a"]
	if report["rules"] != "hash-2" {
		t.Errorf("rules = %q, want the latest report hash-2", report["rules"])
	}
	if _, ok := report["models"]; ok {
		t.Errorf("report = %#v: a report replaces, never merges", report)
	}
}

func TestRedisSupplyObservedEmptyReportClears(t *testing.T) {
	sink, rdb := newRedisSupplyObservedForTest(t)

	sink.Record("runner-a", map[string]string{"rules": "hash-1"})
	sink.Record("runner-a", nil)

	if _, ok := sink.Snapshot()["runner-a"]; ok {
		t.Fatal("a runner that reported an empty set must not linger in the snapshot")
	}
	members, err := rdb.SMembers(context.Background(), sink.indexKey()).Result()
	if err != nil {
		t.Fatalf("SMembers: %v", err)
	}
	if len(members) != 0 {
		t.Errorf("index still holds %v after an empty report", members)
	}
}

func TestRedisSupplyObservedExpiresReport(t *testing.T) {
	mr, rdb := newRedisRunnerDirectoryTestClient(t)
	sink := NewRedisSupplyObserved(rdb, 90*time.Second, nil)

	sink.Record("runner-a", map[string]string{"rules": "hash-1"})
	// Prove the report existed before asserting it is gone: a sink that never
	// wrote anything would otherwise pass this test.
	if before := sink.Snapshot(); len(before) != 1 {
		t.Fatalf("precondition failed: Snapshot = %#v", before)
	}

	mr.FastForward(91 * time.Second)

	if after := sink.Snapshot(); len(after) != 0 {
		t.Fatalf("Snapshot = %#v after retention elapsed, want empty", after)
	}
}

func TestRedisSupplyObservedSnapshotPrunesStaleIndexMembers(t *testing.T) {
	// The index SET has no TTL of its own, so an expired payload leaves a
	// dangling member. Snapshot must drop it AND clean it up, or the index
	// grows without bound across a fleet's lifetime of runner ids.
	mr, rdb := newRedisRunnerDirectoryTestClient(t)
	sink := NewRedisSupplyObserved(rdb, 90*time.Second, nil)

	sink.Record("gone", map[string]string{"rules": "hash-1"})
	mr.FastForward(91 * time.Second)
	if got := sink.Snapshot(); len(got) != 0 {
		t.Fatalf("Snapshot = %#v, want empty", got)
	}

	members, err := rdb.SMembers(context.Background(), sink.indexKey()).Result()
	if err != nil {
		t.Fatalf("SMembers: %v", err)
	}
	if len(members) != 0 {
		t.Errorf("index still holds %v after the payload expired", members)
	}
}

func TestRedisSupplyObservedSnapshotIsEmptyWhenNothingReported(t *testing.T) {
	sink, _ := newRedisSupplyObservedForTest(t)
	got := sink.Snapshot()
	if got == nil || len(got) != 0 {
		t.Fatalf("Snapshot = %#v, want an empty map", got)
	}
}

func TestRedisSupplyObservedSharesDataAcrossInstances(t *testing.T) {
	// Two RedisSupplyObserved values over one Redis stand in for two server
	// replicas: this is the entire reason the sink is not a process-local map.
	// Without it, "has everyone applied revision N" flips with whichever
	// replica serves the read.
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	replicaA := NewRedisSupplyObserved(rdb, DefaultSupplyObservedRetention, nil)
	replicaB := NewRedisSupplyObserved(rdb, DefaultSupplyObservedRetention, nil)

	replicaA.Record("runner-a", map[string]string{"rules": "hash-a"})

	got := replicaB.Snapshot()
	if got["runner-a"]["rules"] != "hash-a" {
		t.Errorf("replica B saw %#v, want the report replica A accepted", got)
	}
}

func TestRedisSupplyObservedLogsFailuresAndDegrades(t *testing.T) {
	// The sink interface has no error return, so a failure must still leave a
	// trail, and a read must degrade to nil — "no observation" — rather than a
	// partial fleet view that reads as a converged-elsewhere answer.
	mr, rdb := newRedisRunnerDirectoryTestClient(t)
	recorder := &supplyHintWarnRecorder{}
	sink := NewRedisSupplyObserved(rdb, DefaultSupplyObservedRetention, recorder)

	mr.Close()

	sink.Record("runner-a", map[string]string{"rules": "hash-a"})
	if got := sink.Snapshot(); got != nil {
		t.Errorf("Snapshot after Redis loss = %#v, want nil", got)
	}
	if len(recorder.msgs) < 2 {
		t.Fatalf("recorded %d warnings %#v, want one per failed call", len(recorder.msgs), recorder.msgs)
	}
	for i, msg := range recorder.msgs {
		if !strings.HasPrefix(msg, "supply observed:") {
			t.Errorf("warning %d = %q, want a supply observed failure line", i, msg)
		}
		if recorder.errArgString(i) == "" {
			t.Errorf("warning %d carries no error detail: %#v", i, recorder.args[i])
		}
	}
}
