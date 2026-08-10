package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/protocol"
)

func TestMemoryMetricsStoreEvicts_PayloadYoungerThanRetention_IsReturned(t *testing.T) {
	now := time.Unix(1754000000, 0)
	store := NewMemoryMetricsStoreWith(90*time.Second, func() time.Time { return now })

	stamped := protocol.MetricsPayloadStamp(now, []byte("payload"))
	if err := store.Put(context.Background(), "runner-a", stamped); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Advance 80s — within retention (90s).
	now = now.Add(80 * time.Second)

	got, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if _, ok := got["runner-a"]; !ok {
		t.Fatal("payload younger than retention was evicted; want retained")
	}
}

func TestMemoryMetricsStoreEvicts_PayloadOlderThanRetention_IsEvicted(t *testing.T) {
	now := time.Unix(1754000000, 0)
	store := NewMemoryMetricsStoreWith(90*time.Second, func() time.Time { return now })

	stamped := protocol.MetricsPayloadStamp(now, []byte("payload"))
	if err := store.Put(context.Background(), "runner-a", stamped); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Advance past retention.
	now = now.Add(91 * time.Second)

	got, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if _, ok := got["runner-a"]; ok {
		t.Fatal("payload older than retention was returned; want evicted")
	}

	// Prove the entry was actually removed from the underlying map (memory leak
	// is the entire point of this fix).
	store.mu.Lock()
	mapLen := len(store.vals)
	store.mu.Unlock()
	if mapLen != 0 {
		t.Fatalf("map still has %d entries after eviction; want 0", mapLen)
	}
}

func TestMemoryMetricsStoreEvicts_ExactBoundary_Survives(t *testing.T) {
	now := time.Unix(1754000000, 0)
	retention := 90 * time.Second
	store := NewMemoryMetricsStoreWith(retention, func() time.Time { return now })

	stamped := protocol.MetricsPayloadStamp(now, []byte("payload"))
	if err := store.Put(context.Background(), "runner-b", stamped); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Exactly at the boundary the stamp equals the cutoff, and the eviction
	// test is a strict Before, so the entry survives. Pinning the boundary
	// keeps a future refactor from silently turning it into <=, which would
	// drop a payload one scrape earlier than the Redis TTL does.
	now = now.Add(retention)

	got, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if _, ok := got["runner-b"]; !ok {
		t.Fatal("entry at exactly the retention boundary was evicted; want retained")
	}

	// One nanosecond past the boundary it must be gone.
	now = now.Add(1)
	got, err = store.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if _, ok := got["runner-b"]; ok {
		t.Fatal("entry 1ns past the retention boundary was not evicted")
	}
}

func TestMemoryMetricsStoreEvicts_PutEvictsStaleEntries(t *testing.T) {
	now := time.Unix(1754000000, 0)
	store := NewMemoryMetricsStoreWith(90*time.Second, func() time.Time { return now })

	stamped := protocol.MetricsPayloadStamp(now, []byte("old"))
	if err := store.Put(context.Background(), "runner-old", stamped); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Advance past retention, then Put a new entry.
	now = now.Add(91 * time.Second)
	fresh := protocol.MetricsPayloadStamp(now, []byte("fresh"))
	if err := store.Put(context.Background(), "runner-new", fresh); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// The stale entry should have been evicted by Put.
	store.mu.Lock()
	mapLen := len(store.vals)
	_, oldPresent := store.vals["runner-old"]
	store.mu.Unlock()
	if oldPresent {
		t.Fatal("stale entry not evicted by Put")
	}
	if mapLen != 1 {
		t.Fatalf("map length = %d; want 1 (only runner-new)", mapLen)
	}
}

func TestMemoryMetricsStoreKeepsUnstampableValue(t *testing.T) {
	now := time.Unix(1754000000, 0)
	store := NewMemoryMetricsStoreWith(90*time.Second, func() time.Time { return now })

	// A value too short to carry the 8-byte stamp. Gather counts this as
	// decode_error; evicting it here would swallow that signal, so the store
	// must retain it however old the clock gets.
	if err := store.Put(context.Background(), "runner-corrupt", []byte("no")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	now = now.Add(time.Hour)

	got, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if _, ok := got["runner-corrupt"]; !ok {
		t.Fatal("unstampable value was evicted; want retained so Gather can count it as decode_error")
	}
}

func TestMemoryMetricsStoreDefaultRetention(t *testing.T) {
	store := NewMemoryMetricsStore()
	if store.retention != DefaultMetricsRetention {
		t.Fatalf("default retention = %v, want %v", store.retention, DefaultMetricsRetention)
	}
}
