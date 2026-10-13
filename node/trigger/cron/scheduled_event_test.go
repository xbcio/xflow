package cron

import (
	"testing"
	"time"
)

// TestScheduledEventSpellsTheInstantAndMarksItACatchUp pins what a backfilled
// firing looks like to the workflow that receives it.
//
// Two things are load-bearing. The identity is the instant formatted this
// package's way: the schedule API planned the range from its own parse of the
// same expression, and it compares the instant it planned against the firing it
// got back — an ID built with a different spelling would not be the firing of
// that instant. And `catchup` is true: a backfill is by definition a firing that
// is not happening at its own instant, which is the one fact a workflow that
// bills a period or closes a book has to be able to branch on.
func TestScheduledEventSpellsTheInstantAndMarksItACatchUp(t *testing.T) {
	n := New()
	instant := time.Date(2026, 8, 26, 13, 47, 15, 0, time.UTC)

	event, err := n.ScheduledEvent("wf-1", "nightly", map[string]any{"expression": "0 2 * * *"}, instant)
	if err != nil {
		t.Fatalf("ScheduledEvent: %v", err)
	}
	wantID := "wf-1/nightly/" + instant.Format(time.RFC3339Nano)
	if event.ID != wantID {
		t.Errorf("event ID = %q, want %q", event.ID, wantID)
	}
	if got := event.Data["scheduled_time"]; got != instant.Format(time.RFC3339Nano) {
		t.Errorf("scheduled_time = %v, want the instant %v", got, instant.Format(time.RFC3339Nano))
	}
	if catchup, _ := event.Data["catchup"].(bool); !catchup {
		t.Errorf("catchup = %v, want true: a backfill never fires at its own instant", event.Data["catchup"])
	}
	// Time is when the firing is admitted — now — while the instant it belongs to
	// is carried as scheduled_time. A backfilled firing whose Time were the
	// instant would misreport when it ran.
	if !event.Time.After(instant) {
		t.Errorf("event Time = %v, want the admission moment, not the instant %v", event.Time, instant)
	}
	if event.Kind != "cron" || event.Source != "nightly" {
		t.Errorf("event kind/source = %q/%q, want cron/nightly", event.Kind, event.Source)
	}

	// The node's own parameters are not part of a cron firing's identity: the
	// instant decides it, whatever the expression was parsed from. A node whose
	// expression was overridden and one that was not agree about which firing
	// 13:47:15 is, which is what lets a backfill plan a range from the effective
	// schedule and still recognise the result.
	overridden, err := n.ScheduledEvent("wf-1", "nightly", map[string]any{"expression": "*/5 * * * *"}, instant)
	if err != nil {
		t.Fatalf("ScheduledEvent: %v", err)
	}
	if overridden.ID != event.ID {
		t.Errorf("the same instant produced %q and %q; the expression is leaking into the identity",
			event.ID, overridden.ID)
	}
}
