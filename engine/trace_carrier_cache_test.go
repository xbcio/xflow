package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/types"
)

var testCarrier = map[string]string{
	"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	"tracestate":  "vendor=opaque",
}

// TestExecutionTraceCarrier_CachesNarrowRead pins the cache contract: the
// carrier is read from the backend once, repeated dispatches reuse the memo,
// the returned map is a copy (so a caller cannot poison later dispatches), and
// EvictExecution drops the memo so the next call reads again.
func TestExecutionTraceCarrier_CachesNarrowRead(t *testing.T) {
	ctx := context.Background()
	id := types.ExecutionID("exec-carrier-cache")
	state := newRecordingExecutionState()
	if err := state.CreateExecution(ctx, &ExecutionSnapshot{
		ID: id, Status: types.ExecutionStatusRunning, TraceCarrier: testCarrier,
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	eng := New(state, &fakeQueue{})

	first, err := eng.ExecutionTraceCarrier(ctx, id)
	if err != nil {
		t.Fatalf("ExecutionTraceCarrier() error = %v", err)
	}
	if !sameCarrier(first, testCarrier) {
		t.Fatalf("ExecutionTraceCarrier() = %#v, want %#v", first, testCarrier)
	}
	if full, _, carrier := state.readCounts(); full != 0 || carrier != 1 {
		t.Fatalf("first call used GetExecution %d times and the carrier read %d times, want 0 and 1", full, carrier)
	}

	// A caller mutating the returned map must not change what later dispatches
	// see: the engine hands out a copy, as GetExecution's per-call decode did.
	first["traceparent"] = "mutated"
	second, err := eng.ExecutionTraceCarrier(ctx, id)
	if err != nil {
		t.Fatalf("ExecutionTraceCarrier() second call error = %v", err)
	}
	if second["traceparent"] != testCarrier["traceparent"] {
		t.Fatalf("second call returned %q, want the cached original %q: the cached map must not be handed out for mutation",
			second["traceparent"], testCarrier["traceparent"])
	}
	if full, _, carrier := state.readCounts(); full != 0 || carrier != 1 {
		t.Fatalf("second call used GetExecution %d times and the carrier read %d times, want 0 and 1 (cached)", full, carrier)
	}

	eng.EvictExecution(id)
	if _, err := eng.ExecutionTraceCarrier(ctx, id); err != nil {
		t.Fatalf("ExecutionTraceCarrier() after EvictExecution error = %v", err)
	}
	if full, _, carrier := state.readCounts(); full != 0 || carrier != 2 {
		t.Fatalf("read after EvictExecution used GetExecution %d times and the carrier read %d times, want 0 and 2 (memo cleared)",
			full, carrier)
	}
}

// TestExecutionTraceCarrier_CachesMissing pins the sentinel: an execution
// submitted without tracing has no carrier, and that answer is cached too so
// every dispatch does not re-read it. An absent execution caches the same way.
func TestExecutionTraceCarrier_CachesMissing(t *testing.T) {
	ctx := context.Background()
	state := newRecordingExecutionState()
	noTracing := types.ExecutionID("exec-carrier-no-tracing")
	if err := state.CreateExecution(ctx, &ExecutionSnapshot{
		ID: noTracing, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	eng := New(state, &fakeQueue{})

	for i := 0; i < 2; i++ {
		got, err := eng.ExecutionTraceCarrier(ctx, noTracing)
		if err != nil {
			t.Fatalf("ExecutionTraceCarrier(no tracing) call %d error = %v", i+1, err)
		}
		if got != nil {
			t.Fatalf("ExecutionTraceCarrier(no tracing) call %d = %#v, want nil", i+1, got)
		}
	}
	if _, _, carrier := state.readCounts(); carrier != 1 {
		t.Fatalf("carrier-less execution was read %d times across two dispatches, want 1: the empty answer is a sentinel", carrier)
	}

	gone := types.ExecutionID("exec-carrier-gone")
	for i := 0; i < 2; i++ {
		got, err := eng.ExecutionTraceCarrier(ctx, gone)
		if err != nil {
			t.Fatalf("ExecutionTraceCarrier(absent) call %d error = %v", i+1, err)
		}
		if got != nil {
			t.Fatalf("ExecutionTraceCarrier(absent) call %d = %#v, want nil", i+1, got)
		}
	}
	if _, _, carrier := state.readCounts(); carrier != 2 {
		t.Fatalf("carrier reads after two absent-execution dispatches = %d, want 2 (one per execution id, one sentinel each)", carrier)
	}
}

// TestExecutionTraceCarrier_FallsBackToSnapshot pins the capability-absent path:
// with only GetExecution available the first ask reads the snapshot, and the
// answer is memoized exactly like the narrow read — a backend without the
// capability must not pay a full snapshot read per background span. The same
// copy and eviction rules apply.
func TestExecutionTraceCarrier_FallsBackToSnapshot(t *testing.T) {
	ctx := context.Background()
	id := types.ExecutionID("exec-carrier-fallback")
	state := &snapshotOnlyState{fakeState: newFakeState()}
	if err := state.CreateExecution(ctx, &ExecutionSnapshot{
		ID: id, Status: types.ExecutionStatusRunning, TraceCarrier: testCarrier,
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	eng := New(state, &fakeQueue{})

	first, err := eng.ExecutionTraceCarrier(ctx, id)
	if err != nil {
		t.Fatalf("ExecutionTraceCarrier() error = %v", err)
	}
	if !sameCarrier(first, testCarrier) {
		t.Fatalf("ExecutionTraceCarrier() = %#v, want %#v", first, testCarrier)
	}
	if reads := state.reads(); reads != 1 {
		t.Fatalf("snapshot reads after first call = %d, want 1", reads)
	}

	first["traceparent"] = "mutated"
	second, err := eng.ExecutionTraceCarrier(ctx, id)
	if err != nil {
		t.Fatalf("ExecutionTraceCarrier() second call error = %v", err)
	}
	if second["traceparent"] != testCarrier["traceparent"] {
		t.Fatalf("second call returned %q, want the cached original %q: the cached map must not be handed out for mutation",
			second["traceparent"], testCarrier["traceparent"])
	}
	if reads := state.reads(); reads != 1 {
		t.Fatalf("snapshot reads after second call = %d, want 1 (cached)", reads)
	}

	eng.EvictExecution(id)
	if _, err := eng.ExecutionTraceCarrier(ctx, id); err != nil {
		t.Fatalf("ExecutionTraceCarrier() after EvictExecution error = %v", err)
	}
	if reads := state.reads(); reads != 2 {
		t.Fatalf("snapshot reads after EvictExecution = %d, want 2 (memo cleared)", reads)
	}
}

// TestExecutionTraceCarrier_ErrorIsNotCached pins that a transient backend
// failure is reported as-is and leaves no entry behind, so the next dispatch
// retries instead of serving a fabricated "no carrier".
func TestExecutionTraceCarrier_ErrorIsNotCached(t *testing.T) {
	ctx := context.Background()
	id := types.ExecutionID("exec-carrier-error")
	state := newRecordingExecutionState()
	if err := state.CreateExecution(ctx, &ExecutionSnapshot{
		ID: id, Status: types.ExecutionStatusRunning, TraceCarrier: testCarrier,
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	eng := New(state, &fakeQueue{})

	boom := errors.New("carrier read failed")
	state.setCarrierError(boom)
	if _, err := eng.ExecutionTraceCarrier(ctx, id); !errors.Is(err, boom) {
		t.Fatalf("ExecutionTraceCarrier() error = %v, want the backend error", err)
	}

	state.setCarrierError(nil)
	got, err := eng.ExecutionTraceCarrier(ctx, id)
	if err != nil {
		t.Fatalf("ExecutionTraceCarrier() after recovery error = %v", err)
	}
	if !sameCarrier(got, testCarrier) {
		t.Fatalf("ExecutionTraceCarrier() after recovery = %#v, want %#v", got, testCarrier)
	}
	if _, _, carrier := state.readCounts(); carrier != 2 {
		t.Fatalf("carrier reads = %d, want 2: a failed read must not be cached as a missing carrier", carrier)
	}
}

func sameCarrier(got, want map[string]string) bool {
	if len(got) != len(want) {
		return false
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}
