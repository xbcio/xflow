package control

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// recordingEmitter stands in for a trigger host: it records the instants it was
// asked to fire and answers with what it was told to.
type recordingEmitter struct {
	instants []time.Time
	// fired holds the identities it has already admitted, so a second emission
	// of the same instant reports duplicate, exactly as a real deduplicator does.
	fired map[string]bool
	err   error
}

func newRecordingEmitter() *recordingEmitter {
	return &recordingEmitter{fired: map[string]bool{}}
}

func (e *recordingEmitter) EmitScheduleInstant(_ context.Context, _ namespace.Namespace, _ types.WorkflowID, node string, instant time.Time) (types.ExecutionID, bool, error) {
	e.instants = append(e.instants, instant)
	if e.err != nil {
		return "", false, e.err
	}
	key := fmt.Sprintf("%s/%s", node, instant.UTC().Format(time.RFC3339Nano))
	if e.fired[key] {
		return "", true, nil
	}
	e.fired[key] = true
	return types.ExecutionID("exec-" + instant.UTC().Format("150405")), false, nil
}

// backfillFixture is a manager over the shared schedule test graph, wired to an
// emitter.
func backfillFixture(t *testing.T, emitter ScheduleEmitter) *scheduleFixture {
	t.Helper()
	return newScheduleFixture(t, scheduleTestGraph(t), ScheduleManagerConfig{Emitter: emitter})
}

// retimeToEveryFifteen gives the fixture's cron node a cadence a test can reason
// about: the definition fires once a day at 02:00, which is too coarse to plan a
// range over.
func retimeToEveryFifteen(t *testing.T, f *scheduleFixture) {
	t.Helper()
	if _, err := f.mgr.Patch(context.Background(), f.wf, "nightly", "alice", SchedulePatch{Fields: map[string]*string{
		ScheduleFieldExpression: strPtr("*/15 * * * *"),
	}}, nil); err != nil {
		t.Fatalf("retime: %v", err)
	}
}

// TestScheduleBackfillPlansAndFiresEveryInstant is the positive path: the
// instants are the schedule's own, oldest first, and each one is reported with
// the execution it started.
func TestScheduleBackfillPlansAndFiresEveryInstant(t *testing.T) {
	emitter := newRecordingEmitter()
	f := backfillFixture(t, emitter)
	retimeToEveryFifteen(t, f)

	from := time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC)
	to := time.Date(2026, 5, 4, 9, 45, 0, 0, time.UTC)
	result, err := f.mgr.Backfill(context.Background(), f.wf, "nightly", ScheduleBackfillRequest{From: from, To: to})
	if err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	want := []time.Time{
		time.Date(2026, 5, 4, 9, 15, 0, 0, time.UTC),
		time.Date(2026, 5, 4, 9, 30, 0, 0, time.UTC),
		time.Date(2026, 5, 4, 9, 45, 0, 0, time.UTC),
	}
	if len(result.Planned) != len(want) {
		t.Fatalf("planned = %v, want %v -- from is exclusive and to inclusive", result.Planned, want)
	}
	for i, instant := range want {
		if !result.Planned[i].Equal(instant) {
			t.Errorf("planned[%d] = %v, want %v", i, result.Planned[i], instant)
		}
	}
	if len(result.Emitted) != 3 || len(result.Skipped) != 0 {
		t.Fatalf("emitted=%d skipped=%d, want 3/0", len(result.Emitted), len(result.Skipped))
	}
	for i, e := range result.Emitted {
		if !e.Instant.Equal(want[i]) {
			t.Errorf("emitted[%d].instant = %v, want %v -- the instants are fired oldest first", i, e.Instant, want[i])
		}
		if e.ExecutionID == "" {
			t.Errorf("emitted[%d] carries no execution id", i)
		}
	}
	if len(emitter.instants) != 3 {
		t.Fatalf("emitter saw %d instants, want 3", len(emitter.instants))
	}
}

// TestScheduleBackfillIsIdempotentPerInstant pins the dedup contract: running
// the same range twice fires each instant once, and the second run reports every
// instant as a duplicate rather than a success or a failure.
func TestScheduleBackfillIsIdempotentPerInstant(t *testing.T) {
	emitter := newRecordingEmitter()
	f := backfillFixture(t, emitter)
	retimeToEveryFifteen(t, f)

	from := time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC)
	to := time.Date(2026, 5, 4, 9, 30, 0, 0, time.UTC)
	first, err := f.mgr.Backfill(context.Background(), f.wf, "nightly", ScheduleBackfillRequest{From: from, To: to})
	if err != nil {
		t.Fatalf("first backfill: %v", err)
	}
	if len(first.Emitted) != 2 {
		t.Fatalf("first run emitted %d, want 2", len(first.Emitted))
	}
	second, err := f.mgr.Backfill(context.Background(), f.wf, "nightly", ScheduleBackfillRequest{From: from, To: to})
	if err != nil {
		t.Fatalf("second backfill: %v", err)
	}
	if len(second.Emitted) != 0 {
		t.Errorf("the second run emitted %d instants again", len(second.Emitted))
	}
	if len(second.Skipped) != 2 {
		t.Fatalf("second run skipped %d, want 2 -- an instant that already fired is reported, not hidden",
			len(second.Skipped))
	}
	for _, skip := range second.Skipped {
		if skip.Reason != ScheduleBackfillDuplicate {
			t.Errorf("skip reason = %q, want %q", skip.Reason, ScheduleBackfillDuplicate)
		}
	}
}

// TestScheduleBackfillDryRunFiresNothing pins that a dry run is a plan and
// nothing else.
func TestScheduleBackfillDryRunFiresNothing(t *testing.T) {
	emitter := newRecordingEmitter()
	f := backfillFixture(t, emitter)
	retimeToEveryFifteen(t, f)

	result, err := f.mgr.Backfill(context.Background(), f.wf, "nightly", ScheduleBackfillRequest{
		From:   time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC),
		To:     time.Date(2026, 5, 4, 9, 45, 0, 0, time.UTC),
		DryRun: true,
	})
	if err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	if len(result.Planned) != 3 || !result.DryRun {
		t.Fatalf("result = %+v, want a three-instant plan", result)
	}
	if len(result.Emitted) != 0 || len(result.Skipped) != 0 || len(emitter.instants) != 0 {
		t.Errorf("a dry run fired something: emitted=%d skipped=%d emitter=%d",
			len(result.Emitted), len(result.Skipped), len(emitter.instants))
	}
}

// TestScheduleBackfillRefusesRangesItCannotHonour pins the four refusals: an
// inverted range, a range reaching into the future, a range over the cap, and a
// missing schedule.
func TestScheduleBackfillRefusesRangesItCannotHonour(t *testing.T) {
	emitter := newRecordingEmitter()
	f := backfillFixture(t, emitter)
	ctx := context.Background()
	now := f.now

	cases := []struct {
		name string
		req  ScheduleBackfillRequest
	}{
		{"inverted", ScheduleBackfillRequest{From: now, To: now.Add(-time.Hour)}},
		{"empty", ScheduleBackfillRequest{From: now.Add(-time.Hour), To: now.Add(-time.Hour)}},
		{"future", ScheduleBackfillRequest{From: now.Add(-time.Hour), To: now.Add(time.Hour)}},
	}
	for _, tc := range cases {
		_, err := f.mgr.Backfill(ctx, f.wf, "nightly", tc.req)
		if !errors.Is(err, ErrScheduleBackfillRange) {
			t.Errorf("%s range = %v, want ErrScheduleBackfillRange", tc.name, err)
		}
	}

	// One instance over the cap, with the cap enforced as a refusal rather than
	// a truncation: a plan of the oldest thousand would misreport what was asked
	// for. The cron node is retimed to every minute so the arithmetic is exact.
	if _, err := f.mgr.Patch(ctx, f.wf, "nightly", "alice", SchedulePatch{Fields: map[string]*string{
		ScheduleFieldExpression: strPtr("* * * * *"),
	}}, nil); err != nil {
		t.Fatalf("retime: %v", err)
	}
	over := ScheduleBackfillRequest{From: now.Add(-time.Duration(MaxScheduleBackfillInstants+1) * time.Minute), To: now}
	if _, err := f.mgr.Backfill(ctx, f.wf, "nightly", over); !errors.Is(err, ErrScheduleBackfillRange) {
		t.Errorf("range of %d instants = %v, want ErrScheduleBackfillRange", MaxScheduleBackfillInstants+1, err)
	}
	if len(emitter.instants) != 0 {
		t.Errorf("a refused range fired %d instants", len(emitter.instants))
	}
	// The dry run of the same range is refused the same way: the cap bounds the
	// plan, not the firing.
	if _, err := f.mgr.Backfill(ctx, f.wf, "nightly", ScheduleBackfillRequest{
		From: over.From, To: over.To, DryRun: true,
	}); !errors.Is(err, ErrScheduleBackfillRange) {
		t.Errorf("dry run over the cap = %v, want ErrScheduleBackfillRange", err)
	}
	// Exactly at the cap is allowed.
	atCap := ScheduleBackfillRequest{From: now.Add(-time.Duration(MaxScheduleBackfillInstants) * time.Minute), To: now}
	result, err := f.mgr.Backfill(ctx, f.wf, "nightly", atCap)
	if err != nil {
		t.Fatalf("a range of exactly %d instants was refused: %v", MaxScheduleBackfillInstants, err)
	}
	if len(result.Planned) != MaxScheduleBackfillInstants {
		t.Errorf("planned %d instants, want %d", len(result.Planned), MaxScheduleBackfillInstants)
	}

	if _, err := f.mgr.Backfill(ctx, f.wf, "poll", ScheduleBackfillRequest{From: now.Add(-time.Hour), To: now}); err != nil {
		t.Errorf("a timer could not be backfilled: %v", err)
	}
	if _, err := f.mgr.Backfill(ctx, f.wf, "hook", ScheduleBackfillRequest{From: now.Add(-time.Hour), To: now}); !errors.Is(err, ErrScheduleNotFound) {
		t.Errorf("backfilling a non-schedule = %v, want ErrScheduleNotFound", err)
	}
}

// TestScheduleBackfillWithoutAnEmitterIsRefused pins the cluster-mode answer:
// the plan is still produced, and the firing is refused with its own error so
// the API can say "this deployment cannot do that" instead of failing as if the
// range were wrong.
func TestScheduleBackfillWithoutAnEmitterIsRefused(t *testing.T) {
	f := newScheduleFixture(t, scheduleTestGraph(t), ScheduleManagerConfig{})
	result, err := f.mgr.Backfill(context.Background(), f.wf, "nightly", ScheduleBackfillRequest{
		From: f.now.Add(-24 * time.Hour),
		To:   f.now,
	})
	if !errors.Is(err, ErrScheduleNoEmitter) {
		t.Fatalf("Backfill = %v, want ErrScheduleNoEmitter", err)
	}
	if len(result.Planned) == 0 {
		t.Error("the plan was discarded with the refusal; a dry run of the same range should be possible")
	}
	if _, err := f.mgr.Backfill(context.Background(), f.wf, "nightly", ScheduleBackfillRequest{
		From: f.now.Add(-24 * time.Hour), To: f.now, DryRun: true,
	}); err != nil {
		t.Errorf("dry run without an emitter = %v, want success", err)
	}
}

// TestScheduleBackfillReportsPerInstantOutcomes pins that one instant's failure
// does not stop the range, and that a refusal is told apart from a failure.
func TestScheduleBackfillReportsPerInstantOutcomes(t *testing.T) {
	emitter := newRecordingEmitter()
	emitter.err = engine.ErrConcurrencyLimit
	f := backfillFixture(t, emitter)
	retimeToEveryFifteen(t, f)

	result, err := f.mgr.Backfill(context.Background(), f.wf, "nightly", ScheduleBackfillRequest{
		From: time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 5, 4, 9, 45, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	if len(result.Skipped) != 3 || len(result.Emitted) != 0 {
		t.Fatalf("skipped=%d emitted=%d, want 3/0", len(result.Skipped), len(result.Emitted))
	}
	for _, skip := range result.Skipped {
		if skip.Reason != ScheduleBackfillRefused {
			t.Errorf("reason = %q, want %q for a concurrency refusal", skip.Reason, ScheduleBackfillRefused)
		}
		if skip.Detail == "" {
			t.Error("a refused instant carries no detail for the operator")
		}
	}
	// Every instant was attempted: one failure does not abandon the rest of the
	// range, and nothing is retried.
	if len(emitter.instants) != 3 {
		t.Errorf("the emitter saw %d instants, want 3", len(emitter.instants))
	}

	emitter.err = errors.New("state store is down")
	result, err = f.mgr.Backfill(context.Background(), f.wf, "nightly", ScheduleBackfillRequest{
		From: time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 5, 4, 9, 15, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	if len(result.Skipped) != 1 || result.Skipped[0].Reason != ScheduleBackfillFailed {
		t.Errorf("skipped = %+v, want one %q", result.Skipped, ScheduleBackfillFailed)
	}
}

// TestScheduleBackfillPlansTheEffectiveSchedule pins that the overlay decides
// what a backfill plans: an operator who retimed a node and then backfilled its
// range means the schedule they retimed it to.
func TestScheduleBackfillPlansTheEffectiveSchedule(t *testing.T) {
	emitter := newRecordingEmitter()
	f := backfillFixture(t, emitter)

	// The definition fires at 02:00 daily; the overlay says every 15 minutes.
	retimeToEveryFifteen(t, f)
	from := time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC)
	to := time.Date(2026, 5, 4, 9, 45, 0, 0, time.UTC)
	result, err := f.mgr.Backfill(context.Background(), f.wf, "nightly", ScheduleBackfillRequest{
		From: from, To: to, DryRun: true,
	})
	if err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	if len(result.Planned) != 3 {
		t.Fatalf("planned %v, want the overlay's quarter-hourly instants", result.Planned)
	}

	// Without the overlay the same range holds none of the definition's
	// instants, which is what proves the plan follows the overlay rather than
	// the definition.
	if _, err := f.mgr.DeleteOverride(context.Background(), f.wf, "nightly", "alice", nil); err != nil {
		t.Fatalf("DeleteOverride: %v", err)
	}
	result, err = f.mgr.Backfill(context.Background(), f.wf, "nightly", ScheduleBackfillRequest{
		From: from, To: to, DryRun: true,
	})
	if err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	if len(result.Planned) != 0 {
		t.Errorf("planned %v, want none: the definition fires once a day at 02:00", result.Planned)
	}
}
