package cron

import (
	"context"
	"strings"
	"testing"
	"time"

	cronlib "github.com/robfig/cron/v3"
	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// The cron package's only test that reaches the firing path installs a runtime
// whose Dedup blocks forever, so Emit is never called and neither the event nor
// the dedup arguments are ever read. Everything the firing decides — the
// scheduled instant that gives replicas a shared identity, the window a
// duplicate can still be caught in, the key that namespaces it — was unasserted.

func TestCronTriggerEventCarriesTheScheduledInstant(t *testing.T) {
	// Driven by explicit instants rather than the wall clock: which instant a
	// firing is attributed to is exactly what a clock-driven assertion cannot pin
	// without racing a boundary.
	scheduled := time.Date(2026, 8, 26, 13, 47, 0, 0, time.UTC)
	observed := time.Date(2026, 8, 26, 13, 47, 29, 500_000_000, time.UTC)
	event := newCronTriggerEvent("wf-1", "nightly", cronFire{scheduled: scheduled, observed: observed})

	const wantScheduled = "2026-08-26T13:47:00Z"
	if got := event.ID; got != "wf-1/nightly/"+wantScheduled {
		t.Fatalf("event ID = %q, want wf-1/nightly/%s. This ID is the deduplication "+
			"identity: it must name the instant the SCHEDULE chose, because that is the "+
			"only instant two replicas of one schedule necessarily agree on — each of "+
			"them observes the firing at a moment of its own",
			got, wantScheduled)
	}
	if got := event.Data["scheduled_time"]; got != wantScheduled {
		t.Fatalf("scheduled_time = %v, want %s", got, wantScheduled)
	}
	// Time is the real firing instant, not the scheduled one — a workflow that
	// reads it is asking when this actually ran.
	if !event.Time.Equal(observed) {
		t.Fatalf("event Time = %v, want the observation instant %v", event.Time, observed)
	}
	if event.Kind != "cron" || event.Source != "nightly" {
		t.Fatalf("event kind/source = %q/%q, want cron/nightly", event.Kind, event.Source)
	}
}

// TestCronTriggerEventKeepsSubMinuteFiringsDistinct covers the case minute
// truncation could not express: @every 1s is the fastest cadence the standard
// parser parses, and under the old identity every firing in a minute collapsed
// onto one key, so the deduper dropped 59 of every 60.
func TestCronTriggerEventKeepsSubMinuteFiringsDistinct(t *testing.T) {
	base := time.Date(2026, 8, 26, 13, 47, 0, 0, time.UTC)
	seen := map[string]bool{}
	for i := 0; i < 60; i++ {
		scheduled := base.Add(time.Duration(i) * time.Second)
		event := newCronTriggerEvent("wf-1", "tick", cronFire{scheduled: scheduled, observed: scheduled})
		if seen[event.ID] {
			t.Fatalf("firing at %s produced the duplicate identity %q; a second of "+
				"schedule must be a second of identity", scheduled, event.ID)
		}
		seen[event.ID] = true
	}
	if len(seen) != 60 {
		t.Fatalf("distinct identities = %d, want 60", len(seen))
	}
}

func TestCronDedupWindowOutlivesTheCadence(t *testing.T) {
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

	// The window is the catch-up window, not the cadence: a catch-up firing of
	// an instant whose original firing was admitted but never recorded arrives
	// up to one catch-up window later, and it can only be folded if the
	// original's key is still there to fold it against. The cadence sets a floor
	// under that (see TestFireDedupTTLBounds), which a one-second cadence does
	// not reach here.
	if got, want := calls[0].TTL, defaultCatchupWindow; got != want {
		t.Fatalf("dedup TTL = %v, want %v", got, want)
	}
	if !strings.HasPrefix(calls[0].Key, "trigger:wf-1:cron:wf-1/cron/") {
		t.Fatalf("dedup key = %q, want it namespaced by workflow and node and "+
			"carrying the scheduled instant; without the instant the window covers the "+
			"whole node and the next scheduled run is dropped as a duplicate",
			calls[0].Key)
	}
}

// TestFireDedupTTLBounds pins both halves of the rule: twice the distance to the
// next firing, so two replicas that observe one instant seconds apart collide,
// and never less than the catch-up window, so a catch-up collides with the
// original firing it repeats.
func TestFireDedupTTLBounds(t *testing.T) {
	minute := mustSchedule(t, "*/1 * * * *")
	hourly := mustSchedule(t, "0 * * * *")
	// An instant on a minute boundary: the hourly schedule's next firing is
	// then a full hour away and the cadence floor is legible.
	at := time.Date(2026, 10, 12, 3, 0, 0, 0, time.UTC)
	cfg := func(window time.Duration) catchupConfig {
		return catchupConfig{policy: misfireSkip, window: window, max: defaultMaxCatchup}
	}

	cases := []struct {
		name     string
		sched    cronlib.Schedule
		cfg      catchupConfig
		want     time.Duration
		explains string
	}{
		{"a window under the cadence leaves the cadence floor", minute, cfg(time.Second), 2 * time.Minute, "cadence"},
		{"the window raises the floor when it is longer", minute, cfg(time.Hour), time.Hour, "catch-up window"},
		{"an hourly schedule still takes a window longer than it", hourly, cfg(3 * time.Hour), 3 * time.Hour, "catch-up window"},
		{"two cadences beat a window shorter than them", hourly, cfg(30 * time.Minute), 2 * time.Hour, "cadence"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := fireDedupTTL(tc.sched, at, tc.cfg); got != tc.want {
				t.Fatalf("fireDedupTTL = %v, want %v (the %s)", got, tc.want, tc.explains)
			}
		})
	}
}
