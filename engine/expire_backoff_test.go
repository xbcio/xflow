package engine

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

var backoffEpoch = time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)

func TestExpireBackoffUnknownKeyIsAllowed(t *testing.T) {
	b := NewExpireBackoff()
	if !b.Allow("exec-1", backoffEpoch) {
		t.Fatal("Allow() on a key with no failure = false, want true")
	}
	if got := b.Failures("exec-1"); got != 0 {
		t.Fatalf("Failures() = %d, want 0", got)
	}
}

func TestExpireBackoffDelayDoublesUpToTheCap(t *testing.T) {
	b := NewExpireBackoff()
	want := []time.Duration{
		time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second,
		16 * time.Second, 32 * time.Second, 64 * time.Second, 128 * time.Second,
		256 * time.Second, expireBackoffMax, expireBackoffMax, expireBackoffMax,
	}
	now := backoffEpoch
	for i, delay := range want {
		if got := b.Failure("exec-1", now); got != i+1 {
			t.Fatalf("Failure() #%d = %d, want %d", i+1, got, i+1)
		}
		if b.Allow("exec-1", now.Add(delay-time.Nanosecond)) {
			t.Fatalf("failure %d: Allow() just before the %v window ends = true, want false", i+1, delay)
		}
		if !b.Allow("exec-1", now.Add(delay)) {
			t.Fatalf("failure %d: Allow() at the end of the %v window = false, want true", i+1, delay)
		}
		now = now.Add(delay)
	}
}

func TestExpireBackoffDelayDoesNotOverflow(t *testing.T) {
	if got := expireBackoffDelay(1 << 30); got != expireBackoffMax {
		t.Fatalf("expireBackoffDelay(huge) = %v, want %v", got, expireBackoffMax)
	}
}

func TestExpireBackoffKeysAreIndependent(t *testing.T) {
	b := NewExpireBackoff()
	b.Failure("exec-1", backoffEpoch)
	if b.Allow("exec-1", backoffEpoch) {
		t.Fatal("exec-1 Allow() inside its window = true, want false")
	}
	if !b.Allow("exec-2", backoffEpoch) {
		t.Fatal("exec-2 Allow() = false, want it unaffected by exec-1")
	}
}

func TestExpireBackoffSuccessClearsTheRecord(t *testing.T) {
	b := NewExpireBackoff()
	b.Failure("exec-1", backoffEpoch)
	b.Failure("exec-1", backoffEpoch)
	b.Success("exec-1")
	if got := b.Len(); got != 0 {
		t.Fatalf("Len() after Success = %d, want 0", got)
	}
	if !b.Allow("exec-1", backoffEpoch) {
		t.Fatal("Allow() after Success = false, want true")
	}
	if got := b.Failure("exec-1", backoffEpoch); got != 1 {
		t.Fatalf("Failure() after Success = %d, want the count restarted at 1", got)
	}
	b.Success("never-failed")
}

func TestExpireBackoffZeroValueIsUsable(t *testing.T) {
	var b ExpireBackoff
	if !b.Allow("exec-1", backoffEpoch) {
		t.Fatal("zero value Allow() = false, want true")
	}
	b.Success("exec-1")
	if got := b.Failure("exec-1", backoffEpoch); got != 1 {
		t.Fatalf("zero value Failure() = %d, want 1", got)
	}
}

// TestExpireBackoffEvictsTheEarliestDueAtCapacity pins the overflow rule: the
// new key is always tracked, and the record that is due soonest makes room.
func TestExpireBackoffEvictsTheEarliestDueAtCapacity(t *testing.T) {
	b := &ExpireBackoff{maxEntries: 3}
	b.Failure("a", backoffEpoch)                    // due at +1s
	b.Failure("b", backoffEpoch.Add(time.Second))   // due at +2s
	b.Failure("c", backoffEpoch.Add(2*time.Second)) // due at +3s
	b.Failure("d", backoffEpoch.Add(3*time.Second)) // evicts a
	if got := b.Len(); got != 3 {
		t.Fatalf("Len() at capacity = %d, want 3", got)
	}
	if got := b.Failures("a"); got != 0 {
		t.Fatalf("Failures(a) = %d, want the earliest-due record evicted", got)
	}
	for _, key := range []string{"b", "c", "d"} {
		if got := b.Failures(key); got != 1 {
			t.Fatalf("Failures(%s) = %d, want 1", key, got)
		}
	}
	if b.Allow("d", backoffEpoch.Add(3*time.Second)) {
		t.Fatal("the newly added key is not backing off, want it tracked")
	}
}

func TestExpireBackoffExistingKeyAtCapacityEvictsNothing(t *testing.T) {
	b := &ExpireBackoff{maxEntries: 2}
	b.Failure("a", backoffEpoch)
	b.Failure("b", backoffEpoch)
	if got := b.Failure("a", backoffEpoch); got != 2 {
		t.Fatalf("Failure(a) = %d, want 2", got)
	}
	if got := b.Len(); got != 2 {
		t.Fatalf("Len() = %d, want both keys kept", got)
	}
	if got := b.Failures("b"); got != 1 {
		t.Fatalf("Failures(b) = %d, want 1", got)
	}
}

func TestExpireBackoffEvictionTieBreaksOnKey(t *testing.T) {
	b := &ExpireBackoff{maxEntries: 2}
	b.Failure("z", backoffEpoch)
	b.Failure("m", backoffEpoch)
	b.Failure("new", backoffEpoch)
	if got := b.Failures("m"); got != 0 {
		t.Fatalf("Failures(m) = %d, want the smaller key of the tie evicted", got)
	}
	if got := b.Failures("z"); got != 1 {
		t.Fatalf("Failures(z) = %d, want 1", got)
	}
}

func TestExpireBackoffDefaultCapacityBoundsMemory(t *testing.T) {
	b := NewExpireBackoff()
	for i := 0; i < expireBackoffMaxEntries+50; i++ {
		b.Failure(fmt.Sprintf("exec-%d", i), backoffEpoch.Add(time.Duration(i)*time.Millisecond))
	}
	if got := b.Len(); got != expireBackoffMaxEntries {
		t.Fatalf("Len() = %d, want it held at %d", got, expireBackoffMaxEntries)
	}
}

func TestExpireBackoffIsSafeForConcurrentUse(t *testing.T) {
	b := &ExpireBackoff{maxEntries: 16}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				key := fmt.Sprintf("exec-%d", (g*7+i)%40)
				now := backoffEpoch.Add(time.Duration(i) * time.Second)
				b.Allow(key, now)
				b.Failure(key, now)
				b.Failures(key)
				b.Len()
				if i%3 == 0 {
					b.Success(key)
				}
			}
		}(g)
	}
	wg.Wait()
	if got := b.Len(); got > 16 {
		t.Fatalf("Len() = %d, want at most the capacity 16", got)
	}
}
