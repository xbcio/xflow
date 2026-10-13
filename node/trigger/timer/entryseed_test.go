package timer

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/internal/seedadmit"
	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// seedInput builds the activation a runner delivers for a remote-hosted timer:
// entry_seed plus the entry-unit identity the admission is keyed on.
func seedInput(rt types.TriggerRuntime, interval string) *types.TriggerActivateInput {
	return &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "entry",
		Params: map[string]any{
			"interval":         interval,
			"entry_seed":       true,
			"entry_unit_id":    "entry",
			"workflow_version": "v1",
		},
		Runtime: rt,
	}
}

// TestTimerEntrySeedAdmitsTicks asserts the fire key of every admitted tick:
// trigger:<workflow>:<entry unit>:timer:<scheduled unix second>, with the id
// derived from the same scheduled time the event itself carries. Two replicas
// firing one tick agree on that key or the tick runs twice; two ticks sharing
// it would silently drop one.
func TestTimerEntrySeedAdmitsTicks(t *testing.T) {
	rt := triggertest.NewFakeEntrySeedRuntime()
	tr := New().Every(20 * time.Millisecond)
	sub, err := tr.Activate(context.Background(), seedInput(rt, "20ms"))
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.WaitForSeedCount(2, 2*time.Second) {
		t.Fatalf("admissions = %d, want at least 2", rt.SeedCount())
	}
	distinct := map[string]struct{}{}
	for _, call := range rt.Seeds() {
		key := call.Request.AdmissionKey
		distinct[key] = struct{}{}
		if !strings.HasPrefix(key, "trigger:wf-1:entry:timer:") {
			t.Fatalf("fire key = %q, want the trigger:<wf>:<entry>:timer:<id> shape", key)
		}
		id := key[strings.LastIndex(key, ":")+1:]
		if _, err := strconv.ParseInt(id, 10, 64); err != nil {
			t.Fatalf("fire key id = %q, want the scheduled tick as unix seconds", id)
		}
		scheduled, ok := exitScheduledTime(call.Request)
		if !ok {
			t.Fatalf("exit carries no scheduled_time: %#v", call.Request.Exits)
		}
		if want := strconv.FormatInt(scheduled.Unix(), 10); id != want {
			t.Fatalf("fire key id = %q, want %q (the event's own scheduled time)", id, want)
		}
	}
	// Each distinct fire key is admitted once; a repeat of a key is the
	// server's deduplication doing its job. The counts are compared against the
	// distinct keys rather than the attempt count because two ticker deliveries
	// can land in one truncated bucket when the process is delayed — that is a
	// property of the clock, not of the trigger.
	if rt.AcceptedCount() != len(distinct) {
		t.Fatalf("accepted = %d but distinct fire keys = %d; each key must be admitted exactly once",
			rt.AcceptedCount(), len(distinct))
	}
	if emits, dedups, locks := rt.LegacyCalls(); emits != 0 || dedups != 0 || locks != 0 {
		t.Fatalf("legacy calls = emit:%d dedup:%d trylock:%d, want none on the entry-seed path",
			emits, dedups, locks)
	}
}

// TestTimerEntrySeedKeepsLegacyPathForAnEngineRuntime asserts the capability
// gate: the same entry_seed params against the embedded engine's runtime must
// keep emitting locally, because there is no control plane to admit to.
func TestTimerEntrySeedKeepsLegacyPathForAnEngineRuntime(t *testing.T) {
	rt := triggertest.NewFakeRuntime()
	tr := New().Every(10 * time.Millisecond)
	sub, err := tr.Activate(context.Background(), seedInput(rt, "10ms"))
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.WaitEmit(time.Second) {
		t.Fatal("timer did not emit through the legacy path")
	}
	if got := rt.DedupCalls(); len(got) == 0 {
		t.Fatal("legacy path did not deduplicate")
	}
}

// TestTimerEntrySeedRefusalIsNotRetried pins the refusal row of the response
// table: the workflow's own concurrency limit declined the tick, so the tick is
// dropped and the next one is a fresh attempt. Retrying it would turn a
// throttled workflow into a backlog of stale ticks.
//
// The interval is long enough that only one tick can be in play while the test
// watches, and the retry window is shrunk so a retry — if the trigger made one
// — would land well inside that same window rather than being mistaken for the
// next tick.
func TestTimerEntrySeedRefusalIsNotRetried(t *testing.T) {
	restore := shrinkRetryWindow(t)
	defer restore()

	rt := triggertest.NewFakeEntrySeedRuntime()
	rt.SetResponder(func(types.EntrySeedRequest, int) (types.EntrySeedResponse, error) {
		return types.EntrySeedResponse{Refused: true}, nil
	})
	tr := New().Every(500 * time.Millisecond)
	sub, err := tr.Activate(context.Background(), seedInput(rt, "500ms"))
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.WaitForSeedCount(1, 2*time.Second) {
		t.Fatal("timer did not admit")
	}
	// Long enough for three retries at the shrunk window, short enough that the
	// next tick (500ms after the first) cannot have arrived.
	settle(t, 200*time.Millisecond)
	if got := rt.SeedCount(); got != 1 {
		t.Fatalf("admissions = %d, want 1: a refused tick is dropped, not retried", got)
	}
}

// TestTimerEntrySeedTransportFailureIsBounded pins the last row of the response
// table: a transport failure is retried a bounded number of times and then the
// tick is dropped. The bound is what keeps a control plane that is down from
// leaving a goroutine retrying a tick forever.
func TestTimerEntrySeedTransportFailureIsBounded(t *testing.T) {
	restore := shrinkRetryWindow(t)
	defer restore()

	rt := triggertest.NewFakeEntrySeedRuntime()
	rt.SetResponder(func(types.EntrySeedRequest, int) (types.EntrySeedResponse, error) {
		return types.EntrySeedResponse{}, context.DeadlineExceeded
	})
	tr := New().Every(500 * time.Millisecond)
	sub, err := tr.Activate(context.Background(), seedInput(rt, "500ms"))
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.WaitForSeedCount(1, 2*time.Second) {
		t.Fatal("timer did not attempt an admission")
	}
	settle(t, 200*time.Millisecond)
	for _, call := range rt.Seeds() {
		if call.Attempt > 4 {
			t.Fatalf("fire key %q was attempted %d times, want at most 1+3",
				call.Request.AdmissionKey, call.Attempt)
		}
	}
	// The bound is only meaningful if it is reached: a trigger that gave up
	// after the first failure would satisfy "at most 4" while dropping ticks a
	// single retry would have admitted.
	if calls := rt.Seeds(); len(calls) > 0 && calls[len(calls)-1].Attempt != 4 {
		t.Fatalf("attempts for the first fire = %d, want 4 (one attempt plus three retries)",
			calls[len(calls)-1].Attempt)
	}
}

// shrinkRetryWindow replaces the admission retry policy with a short one for
// the duration of a test and returns the restore function.
func shrinkRetryWindow(t *testing.T) func() {
	t.Helper()
	previous := admitRetry
	admitRetry = seedadmit.Retry{MaxRetries: 3, Delay: time.Millisecond}
	return func() { admitRetry = previous }
}

// settle waits for the trigger's own goroutines to reach a quiet point. It is a
// sleep rather than a poll because the property under test is the ABSENCE of a
// call: there is nothing to wait on.
func settle(t *testing.T, d time.Duration) {
	t.Helper()
	time.Sleep(d)
}

// TestTimerEntrySeedSubscriptionCloseStopsAdmitting asserts the subscription
// really stops the loop: a closed activation that kept admitting would keep
// firing a workflow nothing is hosting.
func TestTimerEntrySeedSubscriptionCloseStopsAdmitting(t *testing.T) {
	rt := triggertest.NewFakeEntrySeedRuntime()
	tr := New().Every(10 * time.Millisecond)
	sub, err := tr.Activate(context.Background(), seedInput(rt, "10ms"))
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if !rt.WaitForSeedCount(1, 2*time.Second) {
		t.Fatal("timer did not admit")
	}
	if err := sub.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	settled := rt.SeedCount()
	// The loop may already be inside an admit when Close lands; give it a window
	// to finish and assert it does not admit again.
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if rt.SeedCount() > settled+1 {
			t.Fatalf("admissions after Close = %d, want at most %d", rt.SeedCount(), settled+1)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// exitScheduledTime reads back the scheduled_time a tick's boundary exit
// carries, which is the value the fire key must be derived from.
func exitScheduledTime(req types.EntrySeedRequest) (time.Time, bool) {
	if len(req.Exits) == 0 {
		return time.Time{}, false
	}
	trigger, ok := req.Exits[0].Data["trigger"].(map[string]any)
	if !ok {
		return time.Time{}, false
	}
	data, ok := trigger["data"].(map[string]any)
	if !ok {
		return time.Time{}, false
	}
	raw, _ := data["scheduled_time"].(string)
	if raw == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}
