package cron

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// instant parses a wall-clock instant in loc, so a table case can name the
// moment it means in the zone the schedule is evaluated in.
func instant(t *testing.T, loc *time.Location, value string) time.Time {
	t.Helper()
	parsed, err := time.ParseInLocation("2006-01-02 15:04:05", value, loc)
	if err != nil {
		t.Fatalf("parse %q in %s: %v", value, loc, err)
	}
	return parsed
}

// TestMissedFires is the table for the catch-up planner: what one activation
// decides from the record an earlier one left, with no clock and no store in
// play. Each row names one property — the policy's choice, the window, the cap,
// a cadence, a DST gap — rather than an expression of the implementation.
func TestMissedFires(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load location: %v", err)
	}
	utc := time.UTC
	cfg := func(policy misfirePolicy, window time.Duration, max int) catchupConfig {
		return catchupConfig{policy: policy, window: window, max: max}
	}
	hour := time.Hour

	cases := []struct {
		name          string
		expression    string
		loc           *time.Location
		last          time.Time
		recorded      bool
		now           time.Time
		cfg           catchupConfig
		wantFires     []time.Time
		wantMissed    int
		wantWatermark time.Time
		wantTruncated bool
	}{
		{
			name:       "a trigger that has never recorded a firing catches up nothing",
			expression: "*/1 * * * *",
			loc:        utc,
			recorded:   false,
			now:        instant(t, utc, "2026-10-12 03:10:00"),
			cfg:        cfg(misfireFireAll, hour, 100),
			// The whole hour is behind it, and none of it was ever this
			// trigger's: a first activation is not a catch-up.
		},
		{
			name:       "nothing was due since the record",
			expression: "*/1 * * * *",
			loc:        utc,
			last:       instant(t, utc, "2026-10-12 03:04:00"),
			recorded:   true,
			now:        instant(t, utc, "2026-10-12 03:04:30"),
			cfg:        cfg(misfireFireAll, hour, 100),
		},
		{
			name:          "skip acknowledges the instants and fires none of them",
			expression:    "*/1 * * * *",
			loc:           utc,
			last:          instant(t, utc, "2026-10-12 03:00:00"),
			recorded:      true,
			now:           instant(t, utc, "2026-10-12 03:04:30"),
			cfg:           cfg(misfireSkip, hour, 100),
			wantMissed:    4,
			wantWatermark: instant(t, utc, "2026-10-12 03:04:00"),
		},
		{
			name:          "fire_once keeps the newest instant",
			expression:    "*/1 * * * *",
			loc:           utc,
			last:          instant(t, utc, "2026-10-12 03:00:00"),
			recorded:      true,
			now:           instant(t, utc, "2026-10-12 03:04:30"),
			cfg:           cfg(misfireFireOnce, hour, 100),
			wantFires:     []time.Time{instant(t, utc, "2026-10-12 03:04:00")},
			wantMissed:    3,
			wantWatermark: instant(t, utc, "2026-10-12 03:04:00"),
		},
		{
			name:       "fire_all keeps the order the schedule named",
			expression: "*/1 * * * *",
			loc:        utc,
			last:       instant(t, utc, "2026-10-12 03:00:00"),
			recorded:   true,
			now:        instant(t, utc, "2026-10-12 03:04:30"),
			cfg:        cfg(misfireFireAll, hour, 100),
			wantFires: []time.Time{
				instant(t, utc, "2026-10-12 03:01:00"),
				instant(t, utc, "2026-10-12 03:02:00"),
				instant(t, utc, "2026-10-12 03:03:00"),
				instant(t, utc, "2026-10-12 03:04:00"),
			},
			wantWatermark: instant(t, utc, "2026-10-12 03:04:00"),
		},
		{
			name:       "the cap keeps the newest and counts the rest as missed",
			expression: "*/1 * * * *",
			loc:        utc,
			last:       instant(t, utc, "2026-10-12 02:30:00"),
			recorded:   true,
			now:        instant(t, utc, "2026-10-12 03:00:30"),
			cfg:        cfg(misfireFireAll, hour, 5),
			wantFires: []time.Time{
				instant(t, utc, "2026-10-12 02:56:00"),
				instant(t, utc, "2026-10-12 02:57:00"),
				instant(t, utc, "2026-10-12 02:58:00"),
				instant(t, utc, "2026-10-12 02:59:00"),
				instant(t, utc, "2026-10-12 03:00:00"),
			},
			wantMissed:    25,
			wantWatermark: instant(t, utc, "2026-10-12 03:00:00"),
		},
		{
			name:       "the window bounds the look-back, not the cap",
			expression: "*/1 * * * *",
			loc:        utc,
			last:       instant(t, utc, "2026-10-12 00:00:00"),
			recorded:   true,
			now:        instant(t, utc, "2026-10-12 03:00:00"),
			cfg:        cfg(misfireFireAll, 10*time.Minute, 100),
			wantFires: []time.Time{
				instant(t, utc, "2026-10-12 02:51:00"),
				instant(t, utc, "2026-10-12 02:52:00"),
				instant(t, utc, "2026-10-12 02:53:00"),
				instant(t, utc, "2026-10-12 02:54:00"),
				instant(t, utc, "2026-10-12 02:55:00"),
				instant(t, utc, "2026-10-12 02:56:00"),
				instant(t, utc, "2026-10-12 02:57:00"),
				instant(t, utc, "2026-10-12 02:58:00"),
				instant(t, utc, "2026-10-12 02:59:00"),
				instant(t, utc, "2026-10-12 03:00:00"),
			},
			wantWatermark: instant(t, utc, "2026-10-12 03:00:00"),
		},
		{
			name:       "a sub-minute cadence keeps every instant it named",
			expression: "@every 1s",
			loc:        utc,
			last:       instant(t, utc, "2026-10-12 03:04:00"),
			recorded:   true,
			now:        instant(t, utc, "2026-10-12 03:04:05").Add(500 * time.Millisecond),
			cfg:        cfg(misfireFireAll, hour, 3),
			wantFires: []time.Time{
				instant(t, utc, "2026-10-12 03:04:03"),
				instant(t, utc, "2026-10-12 03:04:04"),
				instant(t, utc, "2026-10-12 03:04:05"),
			},
			wantMissed:    2,
			wantWatermark: instant(t, utc, "2026-10-12 03:04:05"),
		},
		{
			name:       "a daily schedule across days fires one instant per day",
			expression: "0 3 * * *",
			loc:        utc,
			last:       instant(t, utc, "2026-10-06 03:00:00"),
			recorded:   true,
			now:        instant(t, utc, "2026-10-09 05:00:00"),
			cfg:        cfg(misfireFireAll, 96*hour, 100),
			wantFires: []time.Time{
				instant(t, utc, "2026-10-07 03:00:00"),
				instant(t, utc, "2026-10-08 03:00:00"),
				instant(t, utc, "2026-10-09 03:00:00"),
			},
			wantWatermark: instant(t, utc, "2026-10-09 03:00:00"),
		},
		{
			name:       "the instant a DST gap removed is not invented",
			expression: "30 2 * * *",
			loc:        ny,
			last:       instant(t, ny, "2026-03-06 02:30:00"),
			recorded:   true,
			now:        instant(t, ny, "2026-03-09 03:00:00"),
			cfg:        cfg(misfireFireAll, 96*hour, 100),
			// 2026-03-08 02:30 never happened in New York; the schedule's own
			// two instants across the gap are what a catch-up may offer.
			wantFires: []time.Time{
				instant(t, ny, "2026-03-07 02:30:00"),
				instant(t, ny, "2026-03-09 02:30:00"),
			},
			wantWatermark: instant(t, ny, "2026-03-09 02:30:00"),
		},
		{
			name:       "a record written by a clock that ran ahead offers nothing",
			expression: "*/1 * * * *",
			loc:        utc,
			last:       instant(t, utc, "2026-10-12 03:10:00"),
			recorded:   true,
			now:        instant(t, utc, "2026-10-12 03:04:00"),
			cfg:        cfg(misfireFireAll, hour, 100),
		},
		{
			name:       "a schedule that never comes due offers nothing",
			expression: "0 0 31 2 *",
			loc:        utc,
			last:       instant(t, utc, "2026-10-06 00:00:00"),
			recorded:   true,
			now:        instant(t, utc, "2026-10-12 00:00:00"),
			cfg:        cfg(misfireFireAll, hour, 100),
		},
		{
			name:       "a window holding more instants than the walk examines is skipped",
			expression: "@every 1s",
			loc:        utc,
			last:       instant(t, utc, "2026-09-12 02:00:00"),
			recorded:   true,
			now:        instant(t, utc, "2026-10-12 03:00:00"),
			cfg:        cfg(misfireFireAll, 60*24*hour, 100),
			// A month of seconds is 2.6 million instants: more than the walk
			// examines, and far more than any catch-up would fire.
			wantTruncated: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := missedFires(mustSchedule(t, tc.expression), tc.last, tc.recorded, tc.now, tc.cfg)
			if plan.truncated != tc.wantTruncated {
				t.Fatalf("truncated = %v, want %v", plan.truncated, tc.wantTruncated)
			}
			if tc.wantTruncated {
				if len(plan.fires) != 0 || !plan.watermark.IsZero() {
					t.Fatalf("a truncated plan fired %v and reached %s; it must do neither",
						plan.fires, plan.watermark)
				}
				return
			}
			if len(plan.fires) != len(tc.wantFires) {
				t.Fatalf("fires = %v, want %v", plan.fires, tc.wantFires)
			}
			for i, want := range tc.wantFires {
				if !plan.fires[i].Equal(want) {
					t.Fatalf("fire %d = %s, want %s", i, plan.fires[i], want)
				}
			}
			if plan.missed != tc.wantMissed {
				t.Fatalf("missed = %d, want %d", plan.missed, tc.wantMissed)
			}
			if plan.watermark.IsZero() != tc.wantWatermark.IsZero() || !plan.watermark.Equal(tc.wantWatermark) {
				t.Fatalf("watermark = %s, want %s", plan.watermark, tc.wantWatermark)
			}
		})
	}
}

// TestApplyCatchupFiresWhatThePlanChose pins the execution of a plan: order,
// the catchup flag, and where the record ends up.
func TestApplyCatchupFiresWhatThePlanChose(t *testing.T) {
	ctx := context.Background()
	in := &types.TriggerActivateInput{WorkflowID: "wf-1", NodeName: "nightly"}
	now := time.Date(2026, 10, 12, 3, 10, 30, 0, time.UTC)

	cases := []struct {
		name          string
		plan          catchupPlan
		wantFired     []time.Time
		wantWatermark time.Time
		wantRecorded  bool
	}{
		{
			name: "fire_all admits every chosen instant, oldest first",
			plan: catchupPlan{
				policy: misfireFireAll,
				fires: []time.Time{
					now.Add(-3 * time.Minute).Truncate(time.Minute),
					now.Add(-2 * time.Minute).Truncate(time.Minute),
					now.Add(-time.Minute).Truncate(time.Minute),
				},
				watermark: now.Truncate(time.Minute),
			},
			wantFired: []time.Time{
				now.Add(-3 * time.Minute).Truncate(time.Minute),
				now.Add(-2 * time.Minute).Truncate(time.Minute),
				now.Add(-time.Minute).Truncate(time.Minute),
			},
			wantWatermark: now.Truncate(time.Minute),
			wantRecorded:  true,
		},
		{
			name: "skip fires nothing and still moves the record",
			plan: catchupPlan{
				policy:    misfireSkip,
				missed:    43,
				watermark: now.Truncate(time.Minute),
			},
			wantWatermark: now.Truncate(time.Minute),
			wantRecorded:  true,
		},
		{
			name: "a plan that reached no instant moves no record",
			plan: catchupPlan{policy: misfireSkip},
		},
		{
			name: "a truncated plan does nothing at all",
			plan: catchupPlan{
				policy:    misfireFireAll,
				truncated: true,
			},
			// The walk never reached an instant it could name, so neither the
			// firings nor the record may claim one.
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt := triggertest.NewFakeEntrySeedRuntime()
			state := watermarkState(t, rt)
			var fired []cronFire
			applyCatchup(ctx, in, tc.plan, now, state, func(fire cronFire) { fired = append(fired, fire) })

			if len(fired) != len(tc.wantFired) {
				t.Fatalf("fired %d instants, want %d", len(fired), len(tc.wantFired))
			}
			for i, want := range tc.wantFired {
				if !fired[i].scheduled.Equal(want) {
					t.Fatalf("firing %d = %s, want %s (oldest first)", i, fired[i].scheduled, want)
				}
				if !fired[i].catchup {
					t.Fatalf("firing %d is not marked as a catch-up", i)
				}
				if !fired[i].observed.Equal(now) {
					t.Fatalf("firing %d observed = %s, want the activation instant %s", i, fired[i].observed, now)
				}
			}

			recorded, ok := readWatermark(ctx, state)
			if ok != tc.wantRecorded {
				t.Fatalf("record present = %v, want %v (value %s)", ok, tc.wantRecorded, recorded)
			}
			if ok && !recorded.Equal(tc.wantWatermark) {
				t.Fatalf("record = %s, want %s", recorded, tc.wantWatermark)
			}
		})
	}
}

// TestActivateCatchesUpOnTheEmbeddedEnginePath drives the local path end to
// end: a record left by an earlier activation, a trigger that was down across
// several scheduled instants, and one Activate with misfire=fire_all. The
// firings must reach the runtime before the live schedule starts, which is what
// makes them catch-up firings rather than a race with the trigger's own clock.
func TestActivateCatchesUpOnTheEmbeddedEnginePath(t *testing.T) {
	ctx := context.Background()
	rt := newEmbeddedRuntime()
	now := time.Now().UTC()

	in := &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "nightly",
		Params: map[string]any{
			"expression":     "*/1 * * * *",
			"misfire":        "fire_all",
			"max_catchup":    float64(3),
			"catchup_window": "1h",
		},
		Runtime: rt,
	}
	// Five minutes of schedule went by while nothing was hosting it.
	if err := writeWatermark(ctx, rt.State(ctx, watermarkScope("wf-1", "nightly")), now.Add(-5*time.Minute)); err != nil {
		t.Fatalf("writeWatermark: %v", err)
	}

	sub, err := New().Activate(ctx, in)
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	defer func() { _ = sub.Close(ctx) }()

	events := rt.Events()
	if len(events) != 3 {
		t.Fatalf("emitted %d events, want 3 (the cap)", len(events))
	}
	want := []string{
		now.Add(-2 * time.Minute).Truncate(time.Minute).Format(time.RFC3339Nano),
		now.Add(-time.Minute).Truncate(time.Minute).Format(time.RFC3339Nano),
		now.Truncate(time.Minute).Format(time.RFC3339Nano),
	}
	for i, event := range events {
		if got := event.Data["scheduled_time"]; got != want[i] {
			t.Fatalf("event %d scheduled_time = %v, want %s", i, got, want[i])
		}
		if event.Data["catchup"] != true {
			t.Fatalf("event %d is not marked as a catch-up: %v", i, event.Data)
		}
	}

	// The record moved past the instants the catch-up fired, so the next
	// activation starts from where this one finished.
	recorded, ok := readWatermark(ctx, rt.State(ctx, watermarkScope("wf-1", "nightly")))
	if !ok {
		t.Fatal("the catch-up left no record")
	}
	if wantLast := now.Truncate(time.Minute); !recorded.Equal(wantLast) {
		t.Fatalf("record = %s, want %s", recorded, wantLast)
	}
}

// TestActivateSkipsCatchupWithoutARecord asserts the other half of the local
// path: a trigger whose runtime keeps no state still fires on its own clock, and
// an activation with no record ahead of it runs no catch-up. It is the case the
// deployment that has never run this schedule is in.
func TestActivateSkipsCatchupWithoutARecord(t *testing.T) {
	ctx := context.Background()
	rt := triggertest.NewFakeRuntime()

	sub, err := New().Activate(ctx, &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "nightly",
		Params: map[string]any{
			"expression":  "*/1 * * * *",
			"misfire":     "fire_all",
			"max_catchup": float64(3),
		},
		Runtime: rt,
	})
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	defer func() { _ = sub.Close(ctx) }()

	if got := rt.EmitCount(); got != 0 {
		t.Fatalf("emitted %d events at activation, want none: with no record there is nothing to catch up", got)
	}
}

// TestCatchupKeepsTheIdentityOfTheInstantItRepeats is the property that makes a
// catch-up safe: it is the same firing, arriving late, so it carries the same
// identity as the punctual firing of that instant would have. A catch-up that
// invented an identity of its own would run the same moment twice — the record
// says the instant was not fired, and the deduplicator is the only other thing
// that knows, and only if the keys match.
func TestCatchupKeepsTheIdentityOfTheInstantItRepeats(t *testing.T) {
	scheduled := time.Date(2026, 10, 12, 3, 4, 0, 0, time.UTC)
	punctual := newCronTriggerEvent("wf-1", "entry", cronFire{
		scheduled: scheduled,
		observed:  scheduled.Add(3 * time.Millisecond),
	})
	late := newCronTriggerEvent("wf-1", "entry", cronFire{
		scheduled: scheduled,
		observed:  scheduled.Add(40 * time.Minute),
		catchup:   true,
	})

	if punctual.ID != late.ID {
		t.Fatalf("catch-up event ID = %q, punctual = %q; the same instant must be one firing",
			late.ID, punctual.ID)
	}
	if punctual.Data["scheduled_time"] != late.Data["scheduled_time"] {
		t.Fatalf("scheduled_time = %v, want %v", late.Data["scheduled_time"], punctual.Data["scheduled_time"])
	}
	if punctual.Data["catchup"] != false || late.Data["catchup"] != true {
		t.Fatalf("catchup = %v (punctual) / %v (late), want false / true",
			punctual.Data["catchup"], late.Data["catchup"])
	}
}

// TestActivateSkipsWithoutFiringOnTheDefaultPolicy drives the default end to
// end: a trigger that missed firings and names no policy must behave exactly as
// it did before catch-up existed — no extra firings — while still moving its
// record past the instants it did not fire, so the next activation does not
// re-offer them.
func TestActivateSkipsWithoutFiringOnTheDefaultPolicy(t *testing.T) {
	ctx := context.Background()
	rt := newEmbeddedRuntime()
	now := time.Now().UTC()
	scope := watermarkScope("wf-1", "nightly")
	if err := writeWatermark(ctx, rt.State(ctx, scope), now.Add(-5*time.Minute)); err != nil {
		t.Fatalf("writeWatermark: %v", err)
	}

	sub, err := New().Activate(ctx, &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "nightly",
		Params:     map[string]any{"expression": "*/1 * * * *"},
		Runtime:    rt,
	})
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	defer func() { _ = sub.Close(ctx) }()

	if got := rt.EmitCount(); got != 0 {
		t.Fatalf("emitted %d events, want none under the default policy", got)
	}
	recorded, ok := readWatermark(ctx, rt.State(ctx, scope))
	if !ok {
		t.Fatal("the activation left no record")
	}
	if want := now.Truncate(time.Minute); !recorded.Equal(want) {
		t.Fatalf("record = %s, want %s: the skipped instants are behind the trigger", recorded, want)
	}
}

// embeddedRuntime is the runtime the LOCAL path needs and neither shared fake
// provides: triggertest.FakeRuntime records Emit and Dedup but returns nil from
// State, so no activation could have a record to resume from, while
// triggertest.FakeEntrySeedRuntime keeps state but fails closed on Emit and
// Dedup — correctly, for the runtime it stands in for. The production embedded
// runtime has both halves, so the test composes them.
type embeddedRuntime struct {
	*triggertest.FakeRuntime
	mu    sync.Mutex
	state map[string]map[string][]byte
}

func newEmbeddedRuntime() *embeddedRuntime {
	return &embeddedRuntime{FakeRuntime: triggertest.NewFakeRuntime(), state: map[string]map[string][]byte{}}
}

var _ types.TriggerRuntime = (*embeddedRuntime)(nil)

func (r *embeddedRuntime) State(_ context.Context, scope string) types.TriggerState {
	return embeddedState{rt: r, scope: scope}
}

type embeddedState struct {
	rt    *embeddedRuntime
	scope string
}

func (s embeddedState) Get(_ context.Context, key string) ([]byte, error) {
	s.rt.mu.Lock()
	defer s.rt.mu.Unlock()
	return append([]byte(nil), s.rt.state[s.scope][key]...), nil
}

func (s embeddedState) Set(_ context.Context, key string, value []byte, _ time.Duration) error {
	s.rt.mu.Lock()
	defer s.rt.mu.Unlock()
	scope := s.rt.state[s.scope]
	if scope == nil {
		scope = map[string][]byte{}
		s.rt.state[s.scope] = scope
	}
	scope[key] = append([]byte(nil), value...)
	return nil
}

func (s embeddedState) Delete(_ context.Context, key string) error {
	s.rt.mu.Lock()
	defer s.rt.mu.Unlock()
	delete(s.rt.state[s.scope], key)
	return nil
}
