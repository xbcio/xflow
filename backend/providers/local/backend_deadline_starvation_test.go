package local

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

var errDeadlineStoreDown = errors.New("store unavailable")

// failingExpirer fails ExpireExecution for the executions it is told are broken
// and hands every other one to the real engine, so a stuck entry can be made
// without breaking the store the healthy ones are expired in.
type failingExpirer struct {
	engine.ExecutionExpirer
	broken func(types.ExecutionID) bool

	mu    sync.Mutex
	calls map[types.ExecutionID]int
}

func (f *failingExpirer) ExpireExecution(ctx context.Context, id types.ExecutionID) (engine.ExpireOutcome, error) {
	f.mu.Lock()
	if f.calls == nil {
		f.calls = make(map[types.ExecutionID]int)
	}
	f.calls[id]++
	f.mu.Unlock()
	if f.broken(id) {
		return "", errDeadlineStoreDown
	}
	return f.ExecutionExpirer.ExpireExecution(ctx, id)
}

func (f *failingExpirer) callsFor(id types.ExecutionID) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[id]
}

// TestBackendDeadlineSweepIsNotStarvedByFailingEntries is the starvation
// regression. The memory index lists the oldest deadlines first and keeps an
// entry until it is acknowledged, so a page of executions that never expire
// would hold every slot of every tick: 110 of them ahead of one healthy
// execution with a batch of 100 must not keep that one from being expired.
func TestBackendDeadlineSweepIsNotStarvedByFailingEntries(t *testing.T) {
	ctx := context.Background()
	b := New()
	eng := engine.New(b.state, b.Queue())
	g := nodeStateGraph(t)
	base := time.Now().UTC().Add(-time.Hour)

	const brokenCount = deadlineSweepBatch + 10
	create := func(id types.ExecutionID, deadline time.Time) {
		t.Helper()
		if err := b.state.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID: id, Graph: g, Status: types.ExecutionStatusRunning, Deadline: deadline,
		}); err != nil {
			t.Fatalf("CreateExecution(%s) error = %v", id, err)
		}
	}
	for i := 0; i < brokenCount; i++ {
		create(types.ExecutionID(fmt.Sprintf("broken-%03d", i)), base.Add(time.Duration(i)*time.Millisecond))
	}
	const fresh = types.ExecutionID("fresh")
	create(fresh, base.Add(time.Minute))

	expirer := &failingExpirer{
		ExecutionExpirer: eng,
		broken:           func(id types.ExecutionID) bool { return strings.HasPrefix(string(id), "broken-") },
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.runDeadlineSweeper(runCtx, expirer, 5*time.Millisecond)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("runDeadlineSweeper did not return after its context was canceled")
		}
	})

	limit := time.Now().Add(10 * time.Second)
	for {
		snap, err := b.state.GetExecution(ctx, fresh)
		if err != nil || snap == nil {
			t.Fatalf("GetExecution(fresh) = %+v, %v", snap, err)
		}
		if snap.Status == types.ExecutionStatusTimeout {
			if snap.Error != engine.ExecutionDeadlineExceededError {
				t.Fatalf("fresh execution error = %q, want %q", snap.Error, engine.ExecutionDeadlineExceededError)
			}
			break
		}
		if time.Now().After(limit) {
			t.Fatalf("the healthy execution was never expired: %d failing entries ahead of it starved the sweep", brokenCount)
		}
		time.Sleep(2 * time.Millisecond)
	}

	// A failing entry is set aside for a window, not retried on every tick: the
	// healthy execution was reached within a few ticks, far inside the first
	// one-second window, so none of them has been attempted twice.
	for i := 0; i < brokenCount; i++ {
		id := types.ExecutionID(fmt.Sprintf("broken-%03d", i))
		if got := expirer.callsFor(id); got > 1 {
			t.Fatalf("%s was attempted %d times, want it left alone inside its backoff window", id, got)
		}
	}
	if got := expirer.callsFor("broken-000"); got != 1 {
		t.Fatalf("broken-000 was attempted %d times, want it tried once", got)
	}
}
