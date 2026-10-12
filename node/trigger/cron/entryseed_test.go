package cron

import (
	"context"
	"testing"
	"time"

	cronlib "github.com/robfig/cron/v3"
	"github.com/xbcio/xflow/node/trigger/internal/seedadmit"
	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// seedInput builds the activation a runner delivers for a remote-hosted cron:
// entry_seed plus the entry-unit identity the admission is keyed on.
func seedInput(rt types.TriggerRuntime, expression string) *types.TriggerActivateInput {
	return &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "entry",
		Params: map[string]any{
			"expression":       expression,
			"entry_seed":       true,
			"entry_unit_id":    "entry",
			"workflow_version": "v1",
		},
		Runtime: rt,
	}
}

// minuteSchedule is the schedule the fire-only tests drive by hand; its cadence
// is not what they assert, only its Next() (used for the admit window).
func minuteSchedule(t *testing.T) cronlib.Schedule {
	t.Helper()
	sched, err := cronlib.ParseStandard("*/1 * * * *")
	if err != nil {
		t.Fatalf("parse schedule: %v", err)
	}
	return sched
}

// TestCronEntrySeedFireKey pins the fire key of one firing:
// trigger:<workflow>:<entry unit>:cron:<scheduled minute in UTC RFC3339>.
//
// The minute comes from the FIRING INSTANT truncated, which is the same
// truncation the event ID uses. Two replicas of this schedule must derive the
// same key from the same minute or the firing runs twice; a key that drifted
// from the event ID would make the two deduplicators disagree about which
// firing an event is.
func TestCronEntrySeedFireKey(t *testing.T) {
	rt := triggertest.NewFakeEntrySeedRuntime()
	tr := New()
	in := seedInput(rt, "*/1 * * * *")
	firedAt := time.Date(2026, 10, 12, 3, 4, 30, 0, time.UTC)

	tr.admitScheduledFire(context.Background(), in, rt, minuteSchedule(t), firedAt)

	calls := rt.Seeds()
	if len(calls) != 1 {
		t.Fatalf("admissions = %d, want 1", len(calls))
	}
	want := "trigger:wf-1:entry:cron:2026-10-12T03:04:00Z"
	if got := calls[0].Request.AdmissionKey; got != want {
		t.Fatalf("fire key = %q, want %q", got, want)
	}
	if calls[0].Request.EntryUnitID != "entry" || calls[0].Request.WorkflowVersion != "v1" {
		t.Fatalf("request identity = %q/%q, want entry/v1",
			calls[0].Request.EntryUnitID, calls[0].Request.WorkflowVersion)
	}
}

// TestCronEntrySeedTwoReplicasFireOnce is the property the fire key exists for:
// two replicas of the same activation, firing the same scheduled minute, must
// produce exactly ONE execution. Both admit; the server's admission-key
// deduplication is what folds the second into a duplicate.
func TestCronEntrySeedTwoReplicasFireOnce(t *testing.T) {
	rt := triggertest.NewFakeEntrySeedRuntime()
	in := seedInput(rt, "*/1 * * * *")
	sched := minuteSchedule(t)
	firedAt := time.Date(2026, 10, 12, 3, 4, 2, 0, time.UTC)

	// Two independent Node values: replica index is not part of the fire key in
	// this path, so the two must collide on the server.
	New().admitScheduledFire(context.Background(), in, rt, sched, firedAt)
	New().admitScheduledFire(context.Background(), in, rt, sched, firedAt)

	if got := rt.SeedCount(); got != 2 {
		t.Fatalf("admissions = %d, want 2 (both replicas submit)", got)
	}
	if got := rt.AcceptedCount(); got != 1 {
		t.Fatalf("accepted = %d, want 1 (the second replica is a duplicate)", got)
	}
}

// TestCronEntrySeedRefusalIsNotRetried pins the refusal row: the firing is
// dropped and the next scheduled minute is a fresh attempt. Retrying it would
// turn a throttled workflow into a backlog of stale ticks.
func TestCronEntrySeedRefusalIsNotRetried(t *testing.T) {
	restore := shrinkRetryWindow(t)
	defer restore()

	rt := triggertest.NewFakeEntrySeedRuntime()
	rt.SetResponder(func(types.EntrySeedRequest, int) (types.EntrySeedResponse, error) {
		return types.EntrySeedResponse{Refused: true}, nil
	})
	tr := New()
	in := seedInput(rt, "*/1 * * * *")
	tr.admitScheduledFire(context.Background(), in, rt, minuteSchedule(t), time.Now())

	if got := rt.SeedCount(); got != 1 {
		t.Fatalf("admissions = %d, want 1: a refused firing is dropped, not retried", got)
	}
	if emits, dedups, _ := rt.LegacyCalls(); emits != 0 || dedups != 0 {
		t.Fatalf("legacy calls = emit:%d dedup:%d, want none on the entry-seed path", emits, dedups)
	}
}

// TestCronEntrySeedTransportFailureIsBounded pins the last row: a transport
// failure is retried a bounded number of times and then the firing is dropped.
func TestCronEntrySeedTransportFailureIsBounded(t *testing.T) {
	restore := shrinkRetryWindow(t)
	defer restore()

	rt := triggertest.NewFakeEntrySeedRuntime()
	rt.SetResponder(func(types.EntrySeedRequest, int) (types.EntrySeedResponse, error) {
		return types.EntrySeedResponse{}, context.DeadlineExceeded
	})
	tr := New()
	tr.admitScheduledFire(context.Background(), seedInput(rt, "*/1 * * * *"), rt, minuteSchedule(t), time.Now())

	calls := rt.Seeds()
	if len(calls) != 4 {
		t.Fatalf("attempts = %d, want 4 (one attempt plus three retries)", len(calls))
	}
}

// TestCronEntrySeedActivateAdmitsRealFirings exercises the hosted path end to
// end: the schedule is real, the callback runs on cronlib's own clock, and the
// admission lands. The cadence is @every 1s, the fastest a five-field standard
// parser expresses, so this is the one test here that waits on the clock.
func TestCronEntrySeedActivateAdmitsRealFirings(t *testing.T) {
	rt := triggertest.NewFakeEntrySeedRuntime()
	tr := New().Cron("@every 1s")
	sub, err := tr.Activate(context.Background(), seedInput(rt, "@every 1s"))
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.WaitForSeedCount(1, 3*time.Second) {
		t.Fatal("cron did not admit a firing")
	}
	if got := rt.SeedCount(); got > 0 && rt.AcceptedCount() == 0 {
		t.Fatalf("admissions = %d but none was accepted", got)
	}
}

// TestCronEntrySeedKeepsLegacyPathForAnEngineRuntime asserts the capability
// gate: the same entry_seed params against the embedded engine's runtime must
// keep deduplicating and emitting locally.
func TestCronEntrySeedKeepsLegacyPathForAnEngineRuntime(t *testing.T) {
	rt := triggertest.NewFakeRuntime()
	tr := New().Cron("@every 1s")
	sub, err := tr.Activate(context.Background(), seedInput(rt, "@every 1s"))
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.WaitEmit(3 * time.Second) {
		t.Fatal("cron did not emit through the legacy path")
	}
	if len(rt.DedupCalls()) == 0 {
		t.Fatal("legacy path did not deduplicate")
	}
}

// TestCronEntrySeedFireIDMatchesEventID asserts the two identities a firing has
// cannot drift: the same instant must produce the event ID's minute and the
// fire key's id.
func TestCronEntrySeedFireIDMatchesEventID(t *testing.T) {
	firedAt := time.Date(2026, 10, 12, 3, 4, 59, 999, time.UTC)
	event := newCronTriggerEvent("wf-1", "entry", firedAt)
	scheduled, _ := event.Data["scheduled_time"].(string)
	if got, want := cronFireID(firedAt), scheduled; got != want {
		t.Fatalf("cronFireID() = %q, event scheduled_time = %q; they must name the same minute", got, want)
	}
	if event.ID != "wf-1/entry/"+scheduled {
		t.Fatalf("event ID = %q, want it built from the same minute", event.ID)
	}
	// A non-UTC schedule truncates to the same instant: every IANA zone differs
	// from UTC by whole minutes, so the minute boundary is a global one.
	shanghai, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatalf("load location: %v", err)
	}
	if got := cronFireID(firedAt.In(shanghai)); got != cronFireID(firedAt) {
		t.Fatalf("fire id in Asia/Shanghai = %q, in UTC = %q; the minute must not depend on the zone",
			got, cronFireID(firedAt))
	}
}

// TestCronEntrySeedScheduleIsTheStandardParser guards the parse the seed path
// performs explicitly: cronlib's default parser (which AddFunc uses on the
// legacy path) is the standard one, so switching to an explicit ParseStandard
// must not narrow or widen the accepted expression set.
func TestCronEntrySeedScheduleIsTheStandardParser(t *testing.T) {
	for _, expression := range []string{"*/5 * * * *", "@hourly", "@every 1m"} {
		if _, err := cronlib.ParseStandard(expression); err != nil {
			t.Fatalf("ParseStandard(%q): %v", expression, err)
		}
	}
	if _, err := cronlib.ParseStandard("0 0 0 0 0 0"); err == nil {
		t.Fatal("a six-field expression parsed; the standard parser must reject it")
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
