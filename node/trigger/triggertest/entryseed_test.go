package triggertest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// TestFakeEntrySeedRuntimeAdmitsEachKeyOnce asserts the property the fake exists
// for: N callers sharing one fake observe one admission per key and a duplicate
// for every later attempt, which is what "two replicas, one execution per tick"
// is made of.
func TestFakeEntrySeedRuntimeAdmitsEachKeyOnce(t *testing.T) {
	rt := triggertest.NewFakeEntrySeedRuntime()
	req := types.EntrySeedRequest{AdmissionKey: "trigger:wf:entry:cron:2026-10-12T00:00:00Z"}

	first, err := rt.SeedExecutionFromEntry(context.Background(), req)
	if err != nil {
		t.Fatalf("first seed: %v", err)
	}
	if !first.Accepted || first.Duplicate {
		t.Fatalf("first seed = %+v, want accepted and not duplicate", first)
	}
	second, err := rt.SeedExecutionFromEntry(context.Background(), req)
	if err != nil {
		t.Fatalf("second seed: %v", err)
	}
	if !second.Accepted || !second.Duplicate {
		t.Fatalf("second seed = %+v, want duplicate acceptance", second)
	}
	if second.ExecutionID != first.ExecutionID {
		t.Fatalf("duplicate execution id = %q, want %q", second.ExecutionID, first.ExecutionID)
	}
	if got := rt.AcceptedCount(); got != 1 {
		t.Fatalf("AcceptedCount() = %d, want 1", got)
	}
}

// TestFakeEntrySeedRuntimeResponderSeesEveryAttempt pins the retry observation:
// a responder that refuses the first attempt and accepts the second must be
// called twice, because a "Refused is not retried" test and a "transport errors
// are retried with an upper bound" test both count these calls.
func TestFakeEntrySeedRuntimeResponderSeesEveryAttempt(t *testing.T) {
	rt := triggertest.NewFakeEntrySeedRuntime()
	var attempts []int
	rt.SetResponder(func(_ types.EntrySeedRequest, attempt int) (types.EntrySeedResponse, error) {
		attempts = append(attempts, attempt)
		if attempt == 1 {
			return types.EntrySeedResponse{Refused: true}, nil
		}
		return types.EntrySeedResponse{}, nil
	})
	req := types.EntrySeedRequest{AdmissionKey: "key"}

	if resp, err := rt.SeedExecutionFromEntry(context.Background(), req); err != nil || !resp.Refused {
		t.Fatalf("first seed = (%+v, %v), want refusal", resp, err)
	}
	if resp, err := rt.SeedExecutionFromEntry(context.Background(), req); err != nil || !resp.Accepted {
		t.Fatalf("second seed = (%+v, %v), want acceptance", resp, err)
	}
	if len(attempts) != 2 || attempts[0] != 1 || attempts[1] != 2 {
		t.Fatalf("responder attempts = %v, want [1 2]", attempts)
	}
}

// TestFakeEntrySeedRuntimeFailsClosedOnLegacyPath guards the reason the legacy
// methods are stubs: a trigger that fell back onto Emit would otherwise be
// indistinguishable, in a test, from one that admitted through the control
// plane.
func TestFakeEntrySeedRuntimeFailsClosedOnLegacyPath(t *testing.T) {
	rt := triggertest.NewFakeEntrySeedRuntime()
	if _, err := rt.Emit(context.Background(), "wf", "n", &types.TriggerEvent{}); err == nil {
		t.Fatal("Emit succeeded, want failure on the legacy path")
	}
	if _, err := rt.Dedup(context.Background(), "k", time.Minute); err == nil {
		t.Fatal("Dedup succeeded, want failure on the legacy path")
	}
	if _, _, err := rt.TryLock(context.Background(), "k", time.Minute); err == nil {
		t.Fatal("TryLock succeeded, want failure on the legacy path")
	}
}

// TestFakeEntrySeedRuntimeStateRoundTrips asserts the state seam exists and is
// scoped: the watermark a trigger writes under one scope must not be readable
// under another.
func TestFakeEntrySeedRuntimeStateRoundTrips(t *testing.T) {
	rt := triggertest.NewFakeEntrySeedRuntime()
	ctx := context.Background()
	state := rt.State(ctx, "cron:wf:node")
	if state == nil {
		t.Fatal("State returned nil")
	}
	if err := state.Set(ctx, "last_scheduled", []byte("t1"), time.Hour); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := state.Get(ctx, "last_scheduled")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "t1" {
		t.Fatalf("Get = %q, want %q", got, "t1")
	}
	other, err := rt.State(ctx, "cron:other:node").Get(ctx, "last_scheduled")
	if err != nil {
		t.Fatalf("Get from another scope: %v", err)
	}
	if other != nil {
		t.Fatalf("cross-scope Get = %q, want nil", other)
	}
}

// TestFakeEntrySeedRuntimeWaitForSeedCountTimesOut proves the wait helper is
// bounded rather than blocking a hung test forever.
func TestFakeEntrySeedRuntimeWaitForSeedCountTimesOut(t *testing.T) {
	rt := triggertest.NewFakeEntrySeedRuntime()
	start := time.Now()
	if rt.WaitForSeedCount(1, 20*time.Millisecond) {
		t.Fatal("WaitForSeedCount reported a seed that was never made")
	}
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Fatalf("WaitForSeedCount returned after %v, want at least the timeout", elapsed)
	}
}

// TestFakeEntrySeedRuntimeResponderErrorIsReturned keeps the transport-failure
// row of the response table drivable: a responder's error must reach the caller
// instead of being swallowed by the fake's own admission bookkeeping.
func TestFakeEntrySeedRuntimeResponderErrorIsReturned(t *testing.T) {
	rt := triggertest.NewFakeEntrySeedRuntime()
	boom := errors.New("boom")
	rt.SetResponder(func(types.EntrySeedRequest, int) (types.EntrySeedResponse, error) {
		return types.EntrySeedResponse{}, boom
	})
	if _, err := rt.SeedExecutionFromEntry(context.Background(), types.EntrySeedRequest{AdmissionKey: "k"}); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
	if got := rt.AcceptedCount(); got != 0 {
		t.Fatalf("AcceptedCount() = %d, want 0 after a transport failure", got)
	}
}
