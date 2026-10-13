package local

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

func TestTriggerLockSupportsRenewal(t *testing.T) {
	primitives := newTriggerPrimitives(newMemoryState())

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
	primitives := newTriggerPrimitives(newMemoryState())

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

// TestTriggerPrimitivesAnswerExecutionStatus pins the optional capability a
// trigger asks for when it must know whether the run it last admitted has
// finished. It reads the store the engine writes, so there is one status in the
// process rather than a second opinion that could drift from it.
func TestTriggerPrimitivesAnswerExecutionStatus(t *testing.T) {
	ctx := context.Background()
	state := newMemoryState()
	primitives := newTriggerPrimitives(state)
	const id = types.ExecutionID("exec-status-1")

	// An execution the store has never seen is not an error: the caller must be
	// able to tell "the run is gone" from "I cannot see the run".
	if status, found, err := primitives.GetExecutionStatus(ctx, id); err != nil || found || status != "" {
		t.Fatalf("GetExecutionStatus(absent) = %q, %v, %v; want not found and no error", status, found, err)
	}

	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{ID: id, Status: types.ExecutionStatusRunning}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	status, found, err := primitives.GetExecutionStatus(ctx, id)
	if err != nil || !found || status != types.ExecutionStatusRunning {
		t.Fatalf("GetExecutionStatus(running) = %q, %v, %v; want %q", status, found, err, types.ExecutionStatusRunning)
	}

	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus() error = %v", err)
	}
	status, found, err = primitives.GetExecutionStatus(ctx, id)
	if err != nil || !found || !types.IsTerminalExecutionStatus(status) {
		t.Fatalf("GetExecutionStatus(finished) = %q, %v, %v; want a terminal status, which is what "+
			"tells a trigger the previous run is done", status, found, err)
	}
}

// TestTriggerPrimitivesWithoutAStoreSaySo: the capability is optional, and a
// value built without a store must answer with ErrExecutionStatusUnavailable
// rather than report a status it does not know — a caller that read a zero
// status as "not running any more" would start a second run on top of the first.
func TestTriggerPrimitivesWithoutAStoreSaySo(t *testing.T) {
	status, found, err := newTriggerPrimitives(nil).GetExecutionStatus(context.Background(), "exec-status-1")
	if !errors.Is(err, types.ErrExecutionStatusUnavailable) {
		t.Fatalf("GetExecutionStatus() = %q, %v, %v; want types.ErrExecutionStatusUnavailable", status, found, err)
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
