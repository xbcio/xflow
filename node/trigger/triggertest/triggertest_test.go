package triggertest_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// TestFakeRuntimeSetEmitFuncConcurrentWithEmit is the guard for the reason
// SetEmitFunc exists at all: the pre-split call sites assigned the field
// directly, which races with a trigger goroutine delivering. Run with -race.
func TestFakeRuntimeSetEmitFuncConcurrentWithEmit(t *testing.T) {
	rt := triggertest.NewFakeRuntime()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_, _ = rt.Emit(context.Background(), "wf", "n", &types.TriggerEvent{ID: "e"})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			rt.SetEmitFunc(func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
				return "x", nil
			})
		}
	}()
	wg.Wait()
	if got := rt.EmitCount(); got != 200 {
		t.Fatalf("EmitCount() = %d, want 200", got)
	}
}

// TestFakeRuntimeWaitForEmitCountTimesOut proves WaitForEmitCount actually
// waits rather than reading the current value once — a probe that returns
// immediately would make every "want >= N" assertion vacuous.
func TestFakeRuntimeWaitForEmitCountTimesOut(t *testing.T) {
	rt := triggertest.NewFakeRuntime()
	start := time.Now()
	if rt.WaitForEmitCount(1, 100*time.Millisecond) {
		t.Fatal("WaitForEmitCount returned true with no emits")
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("returned after %s, want >= 100ms (must actually wait)", elapsed)
	}
}

func TestFakeRuntimeEventsIsACopy(t *testing.T) {
	rt := triggertest.NewFakeRuntime()
	rt.RecordEmit(&types.TriggerEvent{ID: "a"})
	events := rt.Events()
	rt.RecordEmit(&types.TriggerEvent{ID: "b"})
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1 (snapshot must not grow)", len(events))
	}
}
