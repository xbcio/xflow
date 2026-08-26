package timer

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// Neither the deduplication window nor the identity it protects was asserted
// anywhere in this package. The existing tests wait for an emit and stop there,
// and the fake took the TTL as `_ time.Duration` and threw it away.

func TestTimerDedupWindowOutlastsTheInterval(t *testing.T) {
	const interval = 10 * time.Millisecond
	rt := triggertest.NewFakeRuntime()
	tr := New().Every(interval)
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "timer",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.WaitDedup(2 * time.Second) {
		t.Fatal("timer did not attempt dedup")
	}
	calls := rt.DedupCalls()
	if len(calls) == 0 {
		t.Fatal("no dedup call was recorded")
	}

	// The window must outlive the interval it protects. The identity buckets by
	// interval, so a key that expires within one bucket is gone before the next
	// replica's tick for that same bucket arrives, and both replicas run it.
	if got := calls[0].TTL; got != 2*interval {
		t.Fatalf("dedup TTL = %v, want %v (twice the interval)", got, 2*interval)
	}
	if calls[0].TTL <= interval {
		t.Fatalf("dedup TTL %v does not outlast the %v interval: the key expires "+
			"inside the very bucket it is meant to guard", calls[0].TTL, interval)
	}

	if !strings.HasPrefix(calls[0].Key, "trigger:wf-1:timer:") {
		t.Fatalf("dedup key = %q, want it namespaced by workflow and node; without "+
			"that prefix two workflows on the same schedule cancel each other out",
			calls[0].Key)
	}
	if !rt.WaitEmit(2 * time.Second) {
		t.Fatal("timer did not emit after a successful dedup")
	}
	// The key is the event ID and nothing else — if they can drift apart, the
	// window is guarding an identity no consumer ever sees.
	if want := "trigger:wf-1:timer:" + string(rt.Events()[0].ID); calls[0].Key != want {
		t.Fatalf("dedup key = %q, want %q", calls[0].Key, want)
	}
}

func TestTimerTriggerEventTruncatesToTheInterval(t *testing.T) {
	// Not zero coverage: TestTimerTriggerEventUsesDeterministicIntervalBucket
	// already asserts that two ticks in one bucket share an ID and two ticks in
	// different buckets do not. What it cannot see is the bucket getting NARROWER
	// — with a 10s interval and ticks 700ms apart, truncating to the second
	// satisfies both of its relations. A narrowed bucket still looks deterministic
	// while replicas that tick a second apart stop agreeing on one identity, so
	// this asserts the width itself against a fixed instant.
	//
	// Driven by an explicit instant rather than a ticker: the truncation under
	// test is what a clock-driven assertion cannot pin without racing a bucket
	// boundary.
	const interval = time.Minute
	tick := time.Date(2026, 8, 26, 13, 47, 29, 500_000_000, time.UTC)
	event := newTimerTriggerEvent("wf-1", "timer", interval, tick)

	scheduled := time.Date(2026, 8, 26, 13, 47, 0, 0, time.UTC)
	wantID := fmt.Sprintf("wf-1/timer/%d", scheduled.UnixNano())
	if event.ID != wantID {
		t.Fatalf("event ID = %q, want %q. This ID is the deduplication identity: "+
			"without the truncation each replica computes a distinct nanosecond and "+
			"every tick runs once per replica", event.ID, wantID)
	}
	if got := event.Data["scheduled_time"]; got != scheduled.Format(time.RFC3339Nano) {
		t.Fatalf("scheduled_time = %v, want %v", got, scheduled.Format(time.RFC3339Nano))
	}
	// Time is the real tick, not the bucket — a workflow reading it is asking
	// when this actually ran.
	if !event.Time.Equal(tick) {
		t.Fatalf("event Time = %v, want the untruncated tick %v", event.Time, tick)
	}
	if event.Kind != "timer" || event.Source != "timer" {
		t.Fatalf("event kind/source = %q/%q, want timer/timer", event.Kind, event.Source)
	}
}
