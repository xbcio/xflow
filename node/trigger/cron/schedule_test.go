package cron

import (
	"testing"
	"time"

	cronlib "github.com/robfig/cron/v3"
)

// mustSchedule parses an expression with the same parser both activation paths
// use, so a cursor test cannot assert against a schedule shape the node would
// never build.
func mustSchedule(t *testing.T, expression string) cronlib.Schedule {
	t.Helper()
	sched, err := cronlib.ParseStandard(expression)
	if err != nil {
		t.Fatalf("ParseStandard(%q): %v", expression, err)
	}
	return sched
}

// TestScheduleCursorAttributesFiringsToTheScheduledInstant is the core of the
// fix the cursor exists for: a firing's identity comes from the schedule, not
// from when the callback happened to run.
func TestScheduleCursorAttributesFiringsToTheScheduledInstant(t *testing.T) {
	minute := mustSchedule(t, "*/1 * * * *")
	everySecond := mustSchedule(t, "@every 1s")

	cases := []struct {
		name      string
		sched     cronlib.Schedule
		activated time.Time
		observed  time.Time
		want      time.Time
	}{
		{
			name:      "an on-time callback keeps its own minute",
			sched:     minute,
			activated: time.Date(2026, 10, 12, 3, 3, 30, 0, time.UTC),
			observed:  time.Date(2026, 10, 12, 3, 4, 0, 4_000_000, time.UTC),
			want:      time.Date(2026, 10, 12, 3, 4, 0, 0, time.UTC),
		},
		{
			name:      "activation mid-minute does not shift the first firing",
			sched:     minute,
			activated: time.Date(2026, 10, 12, 3, 4, 59, 999_000_000, time.UTC),
			observed:  time.Date(2026, 10, 12, 3, 5, 0, 2_000_000, time.UTC),
			want:      time.Date(2026, 10, 12, 3, 5, 0, 0, time.UTC),
		},
		{
			name: "a late callback is still the firing that was due, not the next one",
			// This is the cross-boundary case: a callback for 03:04 that lands at
			// 03:04:59.5 must not be published as 03:05 — attributing it forward
			// would lose 03:04 and admit 03:05 twice.
			sched:     minute,
			activated: time.Date(2026, 10, 12, 3, 3, 0, 0, time.UTC),
			observed:  time.Date(2026, 10, 12, 3, 4, 59, 500_000_000, time.UTC),
			want:      time.Date(2026, 10, 12, 3, 4, 0, 0, time.UTC),
		},
		{
			name: "a stall skips the firings the library skipped",
			// cronlib reschedules from its wake time, so a process that was stalled
			// from 03:04 to 03:07 runs this callback for the schedule's latest due
			// instant. Reporting 03:04 here would publish a moment that every
			// healthy replica has already published under the same key, and would
			// leave 03:07 unpublished.
			sched:     minute,
			activated: time.Date(2026, 10, 12, 3, 3, 0, 0, time.UTC),
			observed:  time.Date(2026, 10, 12, 3, 7, 30, 0, time.UTC),
			want:      time.Date(2026, 10, 12, 3, 7, 0, 0, time.UTC),
		},
		{
			name:      "a sub-second cadence keeps its own instant",
			sched:     everySecond,
			activated: time.Date(2026, 10, 12, 3, 4, 0, 400_000_000, time.UTC),
			observed:  time.Date(2026, 10, 12, 3, 4, 1, 10_000_000, time.UTC),
			want:      time.Date(2026, 10, 12, 3, 4, 1, 0, time.UTC),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cursor := newScheduleCursor(tc.sched, tc.activated)
			got := cursor.next(tc.observed)
			if !got.scheduled.Equal(tc.want) {
				t.Fatalf("scheduled = %s, want %s", got.scheduled, tc.want)
			}
			if !got.observed.Equal(tc.observed) {
				t.Fatalf("observed = %s, want the instant the callback ran (%s)", got.observed, tc.observed)
			}
		})
	}
}

// TestScheduleCursorAdvancesWithoutRepeating asserts the walk is monotone: each
// firing consumes exactly one scheduled instant, so a caller that fires the
// cursor repeatedly cannot publish the same instant twice — which the control
// plane would fold into a duplicate, silently losing the later firing.
func TestScheduleCursorAdvancesWithoutRepeating(t *testing.T) {
	cursor := newScheduleCursor(mustSchedule(t, "*/1 * * * *"), time.Date(2026, 10, 12, 3, 3, 30, 0, time.UTC))

	seen := map[time.Time]bool{}
	at := time.Date(2026, 10, 12, 3, 4, 0, 1_000_000, time.UTC)
	for i := 0; i < 5; i++ {
		fire := cursor.next(at)
		if seen[fire.scheduled] {
			t.Fatalf("firing %d re-reported %s", i, fire.scheduled)
		}
		seen[fire.scheduled] = true
		if want := at.Truncate(time.Minute); !fire.scheduled.Equal(want) {
			t.Fatalf("firing %d scheduled = %s, want %s", i, fire.scheduled, want)
		}
		at = at.Add(time.Minute)
	}
}

// TestScheduleCursorStaysAlignedAcrossReplicas asserts the property the identity
// exists for: two replicas that activated at different moments and observed the
// same firing at different moments report the same scheduled instant.
func TestScheduleCursorStaysAlignedAcrossReplicas(t *testing.T) {
	sched := mustSchedule(t, "*/1 * * * *")
	early := newScheduleCursor(sched, time.Date(2026, 10, 12, 3, 3, 1, 0, time.UTC))
	late := newScheduleCursor(sched, time.Date(2026, 10, 12, 3, 3, 40, 0, time.UTC))

	// The early replica fires 03:04 on time; the late one runs its callback for
	// the same minute nine seconds afterwards, and once more half a minute later
	// still inside the same minute.
	a := early.next(time.Date(2026, 10, 12, 3, 4, 0, 3_000_000, time.UTC))
	b := late.next(time.Date(2026, 10, 12, 3, 4, 9, 0, time.UTC))
	if !a.scheduled.Equal(b.scheduled) {
		t.Fatalf("replicas disagreed: %s vs %s; one firing would run twice",
			a.scheduled, b.scheduled)
	}

	// A third replica that was stalled through 03:04 and 03:05 catches back up to
	// the instant every awake replica is on.
	stalled := newScheduleCursor(sched, time.Date(2026, 10, 12, 3, 3, 55, 0, time.UTC))
	c := stalled.next(time.Date(2026, 10, 12, 3, 6, 20, 0, time.UTC))
	want := time.Date(2026, 10, 12, 3, 6, 0, 0, time.UTC)
	if !c.scheduled.Equal(want) {
		t.Fatalf("stalled replica scheduled = %s, want %s", c.scheduled, want)
	}
}

// TestFiringWindowBoundsAdmissionByTheNextFiring pins the window an admission
// retry lives in: the distance to the next firing, computed from the schedule
// rather than from the retry's own observation.
func TestFiringWindowBoundsAdmissionByTheNextFiring(t *testing.T) {
	if got, want := firingWindow(mustSchedule(t, "*/5 * * * *"), time.Date(2026, 10, 12, 3, 5, 0, 0, time.UTC)), 5*time.Minute; got != want {
		t.Fatalf("window = %v, want %v", got, want)
	}
	if got, want := firingWindow(mustSchedule(t, "@every 1s"), time.Date(2026, 10, 12, 3, 5, 0, 0, time.UTC)), time.Second; got != want {
		t.Fatalf("window = %v, want %v", got, want)
	}
}
