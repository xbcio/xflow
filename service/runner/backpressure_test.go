package runner

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestBackpressure_AcquireRelease(t *testing.T) {
	bp := NewEmitBackpressure(3)

	ctx := context.Background()

	// Acquire within limit should succeed immediately.
	release1, err := bp.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire 1: %v", err)
	}
	if bp.Inflight() != 1 {
		t.Fatalf("expected inflight=1, got %d", bp.Inflight())
	}

	release2, err := bp.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire 2: %v", err)
	}
	if bp.Inflight() != 2 {
		t.Fatalf("expected inflight=2, got %d", bp.Inflight())
	}

	// Release one slot.
	release1()
	if bp.Inflight() != 1 {
		t.Fatalf("expected inflight=1 after release, got %d", bp.Inflight())
	}

	// Double-release is safe (idempotent).
	release1()
	if bp.Inflight() != 1 {
		t.Fatalf("expected inflight=1 after double release, got %d", bp.Inflight())
	}

	release2()
	if bp.Inflight() != 0 {
		t.Fatalf("expected inflight=0 after all released, got %d", bp.Inflight())
	}

	// PauseCount should be 0 since we never blocked.
	if bp.PauseCount() != 0 {
		t.Fatalf("expected pauseCount=0, got %d", bp.PauseCount())
	}
}

func TestBackpressure_BlocksAtLimit(t *testing.T) {
	bp := NewEmitBackpressure(2)
	ctx := context.Background()

	// Fill the window.
	r1, _ := bp.Acquire(ctx)
	r2, _ := bp.Acquire(ctx)

	if bp.Inflight() != 2 {
		t.Fatalf("expected inflight=2, got %d", bp.Inflight())
	}

	// Third acquire should block.
	acquired := make(chan struct{})
	var r3 func()
	go func() {
		var err error
		r3, err = bp.Acquire(ctx)
		if err != nil {
			t.Errorf("Acquire 3: %v", err)
		}
		close(acquired)
	}()

	// Give the goroutine time to block.
	time.Sleep(50 * time.Millisecond)
	select {
	case <-acquired:
		t.Fatal("Acquire 3 should have blocked")
	default:
	}

	// Release one slot — the blocked acquire should proceed.
	r1()

	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("Acquire 3 did not unblock after release")
	}

	// inflight should still be 2 (slot transferred from r1 to r3).
	if bp.Inflight() != 2 {
		t.Fatalf("expected inflight=2 after transfer, got %d", bp.Inflight())
	}

	r2()
	r3()
	if bp.Inflight() != 0 {
		t.Fatalf("expected inflight=0 after all released, got %d", bp.Inflight())
	}
}

func TestBackpressure_ContextCancel(t *testing.T) {
	bp := NewEmitBackpressure(1)
	ctx := context.Background()

	// Fill the window.
	_, _ = bp.Acquire(ctx)

	// Cancel context while waiting.
	cancelCtx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	var acquireErr error
	go func() {
		defer wg.Done()
		_, acquireErr = bp.Acquire(cancelCtx)
	}()

	// Let the goroutine block.
	time.Sleep(50 * time.Millisecond)

	cancel()
	wg.Wait()

	if acquireErr != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", acquireErr)
	}

	// Inflight should remain 1 (original acquire).
	if bp.Inflight() != 1 {
		t.Fatalf("expected inflight=1, got %d", bp.Inflight())
	}
}

func TestBackpressure_PauseCount(t *testing.T) {
	bp := NewEmitBackpressure(1)
	ctx := context.Background()

	// Fill the window.
	r1, _ := bp.Acquire(ctx)

	if bp.PauseCount() != 0 {
		t.Fatalf("expected pauseCount=0 initially, got %d", bp.PauseCount())
	}

	// Two goroutines will block.
	var wg sync.WaitGroup
	releases := make([]func(), 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			rel, err := bp.Acquire(ctx)
			if err != nil {
				t.Errorf("Acquire %d: %v", idx, err)
				return
			}
			releases[idx] = rel
		}(i)
	}

	// Wait for both goroutines to register as waiters.
	time.Sleep(50 * time.Millisecond)

	if bp.PauseCount() != 2 {
		t.Fatalf("expected pauseCount=2, got %d", bp.PauseCount())
	}

	// Release slots one by one.
	r1()
	time.Sleep(20 * time.Millisecond)
	// One waiter unblocked; release its slot to unblock the second.
	for i := 0; i < 2; i++ {
		if releases[i] != nil {
			releases[i]()
			break
		}
	}

	wg.Wait()

	// PauseCount does not decrement — it is a monotonic counter.
	if bp.PauseCount() != 2 {
		t.Fatalf("expected pauseCount=2 (monotonic), got %d", bp.PauseCount())
	}
}
