package cron

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// The cron package's only test that reaches the firing path installs a runtime
// whose Dedup blocks forever, so Emit is never called and neither the event nor
// the dedup arguments are ever read. Everything the firing decides — the minute
// truncation that gives replicas a shared identity, the 2-minute window, the key
// that namespaces it — was unasserted.

func TestCronTriggerEventTruncatesToTheMinute(t *testing.T) {
	// Driven by an explicit instant rather than the wall clock: the truncation
	// under test is exactly what a clock-driven assertion cannot pin without
	// racing a minute boundary.
	fired := time.Date(2026, 8, 26, 13, 47, 29, 500_000_000, time.UTC)
	event := newCronTriggerEvent("wf-1", "nightly", fired)

	const wantScheduled = "2026-08-26T13:47:00Z"
	if got := event.ID; got != "wf-1/nightly/"+wantScheduled {
		t.Fatalf("event ID = %q, want wf-1/nightly/%s. This ID is the deduplication "+
			"identity: truncating to the hour folds every firing in an hour into one "+
			"and all but the first are dropped; not truncating at all gives each "+
			"replica a distinct sub-second ID and every firing runs once per replica",
			got, wantScheduled)
	}
	if got := event.Data["scheduled_time"]; got != wantScheduled {
		t.Fatalf("scheduled_time = %v, want %s", got, wantScheduled)
	}
	// Time is the real firing instant, not the truncated one — a workflow that
	// reads it is asking when this actually ran.
	if !event.Time.Equal(fired) {
		t.Fatalf("event Time = %v, want the untruncated firing instant %v", event.Time, fired)
	}
	if event.Kind != "cron" || event.Source != "nightly" {
		t.Fatalf("event kind/source = %q/%q, want cron/nightly", event.Kind, event.Source)
	}
}

func TestCronDedupWindowSpansTwoScheduleBuckets(t *testing.T) {
	rt := triggertest.NewFakeRuntime()
	tr := New().Cron("@every 1s")
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "cron",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.WaitDedup(5 * time.Second) {
		t.Fatal("cron did not attempt dedup")
	}
	calls := rt.DedupCalls()
	if len(calls) == 0 {
		t.Fatal("no dedup call was recorded")
	}

	// The identity buckets by minute, so the window has to outlive a minute for
	// a replica that fires late in the bucket to still see the early firing's
	// key. Anything at or below one minute is deduplication switched off.
	if got := calls[0].TTL; got != 2*time.Minute {
		t.Fatalf("dedup TTL = %v, want 2m: the schedule buckets by the minute, so a "+
			"window not longer than a minute lets a second replica re-run the same "+
			"firing", got)
	}
	if !strings.HasPrefix(calls[0].Key, "trigger:wf-1:cron:wf-1/cron/") {
		t.Fatalf("dedup key = %q, want it namespaced by workflow and node and "+
			"carrying the scheduled bucket; without the bucket the window covers the "+
			"whole node and the next scheduled run is dropped as a duplicate",
			calls[0].Key)
	}
}
