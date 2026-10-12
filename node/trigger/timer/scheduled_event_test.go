package timer

import (
	"strconv"
	"testing"
	"time"
)

// TestScheduledEventSpellsTheTickTheNodeIsRunning pins the identity a backfilled
// tick carries, and that it comes from the EFFECTIVE interval.
//
// The interval is what makes this a parameter-dependent identity: the tick is
// the instant truncated to the interval's grid, so the same instant is two
// different firings under two different intervals. An operator who retimed a
// node and then backfilled it means the schedule the node is running, and the
// backfill passes the parameters it was activated with for exactly this reason.
func TestScheduledEventSpellsTheTickTheNodeIsRunning(t *testing.T) {
	n := New()
	// Mid-bucket on purpose: an instant already on the grid would look the same
	// under an interval that is a multiple of it.
	instant := time.Date(2026, 8, 26, 13, 47, 15, 0, time.UTC)

	event, err := n.ScheduledEvent("wf-1", "poll", map[string]any{"interval": "5s"}, instant)
	if err != nil {
		t.Fatalf("ScheduledEvent: %v", err)
	}
	wantID := "wf-1/poll/" + strconv.FormatInt(instant.UnixNano(), 10)
	if event.ID != wantID {
		t.Errorf("event ID = %q, want %q -- the identity must be the tick the live trigger would fire, not a fresh one",
			event.ID, wantID)
	}
	if got := event.Data["scheduled_time"]; got != instant.Format(time.RFC3339Nano) {
		t.Errorf("scheduled_time = %v, want %v", got, instant.Format(time.RFC3339Nano))
	}
	// Time is when the firing is admitted, which for a backfill is now — the
	// instant itself is in the past and is carried as scheduled_time.
	if !event.Time.After(instant) {
		t.Errorf("event Time = %v, want the admission moment, not the instant %v", event.Time, instant)
	}

	// The same instant under the definition's own interval is a different
	// firing: 13:47:15 truncates to 13:47:00 on a minute grid.
	other, err := n.ScheduledEvent("wf-1", "poll", map[string]any{"interval": "1m"}, instant)
	if err != nil {
		t.Fatalf("ScheduledEvent: %v", err)
	}
	if other.ID == event.ID {
		t.Errorf("both intervals produced %q; the interval is not reaching the identity", event.ID)
	}

	// A node whose interval cannot be read has no identity to offer, and saying
	// so is the only honest answer: firing it anyway would mint an ID no live
	// trigger would ever produce.
	for name, params := range map[string]map[string]any{
		"missing":  nil,
		"unparsed": {"interval": "every so often"},
		"zero":     {"interval": "0s"},
	} {
		if _, err := n.ScheduledEvent("wf-1", "poll", params, instant); err == nil {
			t.Errorf("%s interval: ScheduledEvent succeeded, want an error", name)
		}
	}
}
