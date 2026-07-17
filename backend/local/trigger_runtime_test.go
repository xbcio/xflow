package local

import (
	"context"
	"testing"
	"time"
)

func TestTriggerLockSupportsRenewal(t *testing.T) {
	primitives := newTriggerPrimitives()

	lock, ok, err := primitives.TryLock(context.Background(), "test-lock", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("TryLock() error = %v", err)
	}
	if !ok {
		t.Fatal("TryLock() = false, want true")
	}

	renewable, ok := lock.(interface {
		Renew(context.Context, time.Duration) (bool, error)
	})
	if !ok {
		t.Fatal("lock does not support renewal")
	}

	renewed, err := renewable.Renew(context.Background(), time.Minute)
	if err != nil {
		t.Fatalf("Renew() error = %v", err)
	}
	if !renewed {
		t.Fatal("Renew() = false, want true")
	}

	_, ok, err = primitives.TryLock(context.Background(), "test-lock", time.Minute)
	if err != nil {
		t.Fatalf("TryLock(second) error = %v", err)
	}
	if ok {
		t.Fatal("second TryLock() = true, want false")
	}
}

func TestTriggerLockStaleRenewAndReleaseDoNotAffectCurrentOwner(t *testing.T) {
	primitives := newTriggerPrimitives()

	firstLock, ok, err := primitives.TryLock(context.Background(), "test-lock-stale", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("TryLock(first) error = %v", err)
	}
	if !ok {
		t.Fatal("TryLock(first) = false, want true")
	}

	firstRenewable, ok := firstLock.(interface {
		Renew(context.Context, time.Duration) (bool, error)
	})
	if !ok {
		t.Fatal("first lock does not support renewal")
	}

	waitForMemoryCondition(t, time.Second, func() bool {
		primitives.mu.Lock()
		defer primitives.mu.Unlock()
		record, ok := primitives.locks["test-lock-stale"]
		return !ok || !time.Now().Before(record.expires)
	})

	secondLock, ok, err := primitives.TryLock(context.Background(), "test-lock-stale", time.Minute)
	if err != nil {
		t.Fatalf("TryLock(second) error = %v", err)
	}
	if !ok {
		t.Fatal("TryLock(second) = false, want true")
	}

	renewed, err := firstRenewable.Renew(context.Background(), time.Minute)
	if err != nil {
		t.Fatalf("Renew(first stale token) error = %v", err)
	}
	if renewed {
		t.Fatal("Renew(first stale token) = true, want false")
	}

	if err := firstLock.Release(context.Background()); err != nil {
		t.Fatalf("Release(first stale token) error = %v", err)
	}

	_, ok, err = primitives.TryLock(context.Background(), "test-lock-stale", time.Minute)
	if err != nil {
		t.Fatalf("TryLock(third) error = %v", err)
	}
	if ok {
		t.Fatal("third TryLock() = true after stale renew/release")
	}

	if err := secondLock.Release(context.Background()); err != nil {
		t.Fatalf("Release(second) error = %v", err)
	}
}

func waitForMemoryCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if condition() {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("condition was not met before timeout")
		}
	}
}
