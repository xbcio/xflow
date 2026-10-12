package cron

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// watermarkState is the watermark handle the fake entry-seed runtime keeps for
// the activation seedInput builds (workflow wf-1, node entry).
func watermarkState(t *testing.T, rt *triggertest.FakeEntrySeedRuntime) types.TriggerState {
	t.Helper()
	return rt.State(context.Background(), watermarkScope("wf-1", "entry"))
}

// TestWatermarkScopeIsPerWorkflowAndNode pins the scope the record lives in. Two
// cron nodes of one workflow, and one node of two workflows, must not share a
// watermark: a shared one would let whichever fired last move the other's
// position, and the other would then read a position from a different schedule.
func TestWatermarkScopeIsPerWorkflowAndNode(t *testing.T) {
	cases := []struct {
		workflow types.WorkflowID
		node     string
	}{
		{"wf-1", "nightly"},
		{"wf-1", "hourly"},
		{"wf-2", "nightly"},
	}
	seen := map[string]bool{}
	for _, tc := range cases {
		scope := watermarkScope(tc.workflow, tc.node)
		if seen[scope] {
			t.Fatalf("scope %q is shared by two (workflow, node) pairs", scope)
		}
		seen[scope] = true
	}
	if got, want := watermarkScope("wf-1", "nightly"), "cron:wf-1:nightly"; got != want {
		t.Fatalf("scope = %q, want %q", got, want)
	}
}

// TestWatermarkRoundTrip pins the encoding: what a firing writes is what the next
// activation reads, and it is the same rendering the fire key uses, so an
// operator can match a watermark against an admission key by eye.
func TestWatermarkRoundTrip(t *testing.T) {
	rt := triggertest.NewFakeEntrySeedRuntime()
	state := watermarkState(t, rt)
	ctx := context.Background()

	// Absent reads as "no record", not as an error and not as the epoch: an
	// activation that never fired has no position, and treating that as zero
	// would offer a catch-up of the entire schedule history.
	if _, ok := readWatermark(ctx, state); ok {
		t.Fatal("a fresh state reported a watermark")
	}

	scheduled := time.Date(2026, 10, 12, 3, 4, 0, 0, time.UTC)
	if err := writeWatermark(ctx, state, scheduled); err != nil {
		t.Fatalf("writeWatermark: %v", err)
	}
	got, ok := readWatermark(ctx, state)
	if !ok {
		t.Fatal("the written watermark did not read back")
	}
	if !got.Equal(scheduled) {
		t.Fatalf("watermark = %s, want %s", got, scheduled)
	}
	raw, err := state.Get(ctx, watermarkKey)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(raw) != cronFireID(scheduled) {
		t.Fatalf("stored value = %q, want the fire key's rendering %q", raw, cronFireID(scheduled))
	}
}

// TestWatermarkReadIsTotal covers the three ways the record can be unusable, all
// of which must read as "no record": a runtime without trigger state, a value
// another build wrote, and an empty value. Each of them costs the knowledge of
// which instants were already accounted for, and none of them may cost a firing:
// a firing's identity comes from the schedule, not from this record.
func TestWatermarkReadIsTotal(t *testing.T) {
	ctx := context.Background()

	if _, ok := readWatermark(ctx, nil); ok {
		t.Fatal("a nil state handed back a watermark")
	}

	rt := triggertest.NewFakeEntrySeedRuntime()
	state := watermarkState(t, rt)
	if err := state.Set(ctx, watermarkKey, []byte("not-a-time"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if at, ok := readWatermark(ctx, state); ok {
		t.Fatalf("a corrupt watermark read as %s", at)
	}
	if err := state.Set(ctx, watermarkKey, nil, 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if at, ok := readWatermark(ctx, state); ok {
		t.Fatalf("an empty watermark read as %s", at)
	}

	// Writing through a nil handle is a no-op, not a panic: the trigger still
	// fires on a runtime that keeps no state.
	if err := writeWatermark(ctx, nil, time.Now()); err != nil {
		t.Fatalf("writeWatermark(nil) = %v, want nil", err)
	}
}

// TestFiringAdvancesTheWatermark asserts the write half of the record on the
// real admission path, and pins WHICH outcomes move it. All three are instants
// behind the trigger: an admitted firing produced an execution, a refused one was
// the workflow's own limit declining, and a dropped one lost its retry budget.
// The tick is not retried in any of them, so a watermark that withheld the
// instant would only make a later activation re-offer it.
func TestFiringAdvancesTheWatermark(t *testing.T) {
	cases := []struct {
		name     string
		respond  func(types.EntrySeedRequest, int) (types.EntrySeedResponse, error)
		schedule string
	}{
		{name: "admitted", schedule: "*/1 * * * *"},
		{name: "refused", schedule: "*/1 * * * *", respond: func(types.EntrySeedRequest, int) (types.EntrySeedResponse, error) {
			return types.EntrySeedResponse{Refused: true}, nil
		}},
		{name: "dropped", schedule: "*/1 * * * *", respond: func(types.EntrySeedRequest, int) (types.EntrySeedResponse, error) {
			return types.EntrySeedResponse{}, context.DeadlineExceeded
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			restore := shrinkRetryWindow(t)
			defer restore()

			rt := triggertest.NewFakeEntrySeedRuntime()
			if tc.respond != nil {
				rt.SetResponder(tc.respond)
			}
			in := seedInput(rt, tc.schedule)
			state := watermarkState(t, rt)
			scheduled := time.Date(2026, 10, 12, 3, 4, 0, 0, time.UTC)

			New().admitScheduledFire(context.Background(), in, rt, minuteSchedule(t),
				cronFire{scheduled: scheduled, observed: scheduled.Add(2 * time.Second)}, state)

			got, ok := readWatermark(context.Background(), state)
			if !ok {
				t.Fatal("the firing left no watermark")
			}
			if !got.Equal(scheduled) {
				t.Fatalf("watermark = %s, want %s", got, scheduled)
			}
		})
	}
}

// TestActivationResumesFromTheWatermark asserts the read half across
// activations: the record written by one activation is what the next one opens,
// including the scheduling-relevant part — a later activation must be able to
// answer "which instants are behind me" without having observed them.
func TestActivationResumesFromTheWatermark(t *testing.T) {
	rt := triggertest.NewFakeEntrySeedRuntime()
	ctx := context.Background()
	in := seedInput(rt, "*/1 * * * *")
	scheduled := time.Date(2026, 10, 12, 3, 4, 0, 0, time.UTC)

	// First activation: one firing, then the process goes away.
	first := openActivationWatermark(ctx, in)
	if first == nil {
		t.Fatal("the fake runtime offered no watermark state")
	}
	recordFiring(ctx, first, in, scheduled)

	// Second activation of the same workflow and node: the watermark it opens is
	// the one the first left.
	second := openActivationWatermark(ctx, in)
	got, ok := readWatermark(ctx, second)
	if !ok {
		t.Fatal("the second activation saw no watermark")
	}
	if !got.Equal(scheduled) {
		t.Fatalf("resumed watermark = %s, want %s", got, scheduled)
	}
}
