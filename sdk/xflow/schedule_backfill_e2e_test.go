package xflow

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/trigger"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/types"
)

// The end-to-end shape of a backfill: a paused schedule that missed a window of
// its own instants, and an operator asking for those instants by name. Every
// layer is real — the compiled graph, the manager's plan, the trigger kind that
// spells each firing, the host that admits it, and the deduplicator that makes a
// repeated request idempotent — because the failure this guards against is the
// seam between them: a plan that is reported as backfilled while nothing ran, or
// a second request that runs everything twice.

// firingLog records what each run of the schedule received: the instant the
// firing belongs to and whether it was marked a catch-up.
type firingLog struct {
	mu      sync.Mutex
	firings []recordedFiring
}

type recordedFiring struct {
	scheduled time.Time
	catchup   bool
}

func (l *firingLog) record(f recordedFiring) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.firings = append(l.firings, f)
}

func (l *firingLog) snapshot() []recordedFiring {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]recordedFiring(nil), l.firings...)
}

// loggingTickHandler runs the action node a schedule's run is observed through:
// it records the trigger payload and signals on the channel.
func loggingTickHandler(fired chan struct{}, log *firingLog) *node.Definition {
	return node.Define("test.backfill.tick", func(_ context.Context, in *types.Input) (*types.Output, error) {
		log.record(readFiring(in.Data["trigger"]))
		select {
		case fired <- struct{}{}:
		default:
		}
		return &types.Output{Data: map[string]any{"ok": true}}, nil
	})
}

// readFiring reads the two facts a backfill asserts on out of the trigger
// payload. The local engine hands the event through as the object it admitted; a
// payload that has been through an encoding step arrives in the event's map form
// (see TriggerEvent.ToMap), and both are accepted so the assertion is about the
// values rather than about which path the engine took.
func readFiring(payload any) recordedFiring {
	var scheduled string
	var catchup bool
	switch v := payload.(type) {
	case *types.TriggerEvent:
		scheduled, _ = v.Data["scheduled_time"].(string)
		catchup, _ = v.Data["catchup"].(bool)
	case map[string]any:
		data, _ := v["data"].(map[string]any)
		scheduled, _ = data["scheduled_time"].(string)
		catchup, _ = data["catchup"].(bool)
	}
	instant, _ := time.Parse(time.RFC3339Nano, scheduled)
	return recordedFiring{scheduled: instant, catchup: catchup}
}

// waitForFires waits for n further runs of the schedule.
func waitForFires(t *testing.T, fired <-chan struct{}, n int, within time.Duration, why string) {
	t.Helper()
	deadline := time.After(within)
	for i := 0; i < n; i++ {
		select {
		case <-fired:
		case <-deadline:
			t.Fatalf("only %d of %d firings arrived within %s: %s", i, n, within, why)
		}
	}
}

func TestLocalScheduleBackfillRunsAPausedWindowAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	fired := make(chan struct{}, 64)
	log := &firingLog{}
	tick := loggingTickHandler(fired, log)
	overrides := memstore.New()
	eng, err := NewLocal(WithNodes(tick), WithScheduleOverrides(overrides))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Stop()

	wf := Workflow("schedule_backfill_e2e")
	start := wf.Node("tick", trigger.Cron().Cron("@every 1s"))
	run := wf.Node("run", tick.New(nil))
	wf.Connect(start, run)

	id, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatal(err)
	}
	// The one process both serves the API and hosts the trigger wires itself as
	// both the reloader and the emitter, over the same overlay store the engine
	// reads.
	mgr := control.NewScheduleManager(control.ScheduleManagerConfig{
		Overrides: overrides, Reloader: eng, Emitter: eng,
	})
	rec, err := eng.workflowRegistry.GetWorkflow(ctx, id)
	if err != nil {
		t.Fatalf("resolve the registered workflow: %v", err)
	}
	ref := control.ScheduleWorkflow{Namespace: namespace.Default, ID: id, Graph: rec.Graph}

	waitForFire(t, fired, 5*time.Second, "the schedule never fired before the pause")

	// The window being backfilled opens at the pause. Everything after it is a
	// moment the schedule named while it was not firing.
	pausedAt := time.Now()
	if _, err := mgr.Pause(ctx, ref, "tick", "operator", nil); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	assertSilent(t, fired, 300*time.Millisecond, 2*time.Second, "while paused")

	// Plan first: the API's dry run is how an operator sees what a range holds
	// before asking for it.
	req := control.ScheduleBackfillRequest{From: pausedAt, To: time.Now()}
	plan, err := mgr.Backfill(ctx, ref, "tick", control.ScheduleBackfillRequest{
		From: req.From, To: req.To, DryRun: true,
	})
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if len(plan.Planned) < 2 {
		t.Fatalf("planned %v: the test needs at least two instants to have passed while paused", plan.Planned)
	}
	if len(plan.Emitted) != 0 {
		t.Fatalf("a dry run emitted %d instants", len(plan.Emitted))
	}

	before := len(log.snapshot())
	result, err := mgr.Backfill(ctx, ref, "tick", req)
	if err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	if len(result.Emitted) != len(result.Planned) || len(result.Skipped) != 0 {
		t.Fatalf("emitted=%d skipped=%d planned=%d, want every planned instant emitted: %+v",
			len(result.Emitted), len(result.Skipped), len(result.Planned), result.Skipped)
	}

	// The paused window really ran — this is the assertion that fails if the
	// backfill's identity collides with the one the paused host claimed for the
	// same instants, in which case every instant would have been reported as a
	// duplicate and nothing would have run.
	waitForFires(t, fired, len(result.Planned), 10*time.Second, "the backfilled instants did not run")
	ran := log.snapshot()[before:]
	if len(ran) != len(result.Planned) {
		t.Fatalf("%d of %d backfilled instants ran", len(ran), len(result.Planned))
	}
	// Each run is the run of the instant that was planned, and it says so: a
	// backfilled firing is marked a catch-up, which is the fact a workflow that
	// bills a period branches on.
	//
	// The runs are matched to the plan as a SET rather than by index. The plan
	// is ordered (oldest first) and the emission follows it, but the executions
	// it starts do not run serially: the local pool runs them concurrently, so
	// two adjacent instants can finish in either order. What must hold is that
	// every planned instant ran exactly once — an ordered comparison would fail
	// on the scheduler's interleaving rather than on anything about the
	// backfill, which is the flake this shape removes.
	matched := make([]bool, len(result.Planned))
	for _, f := range ran {
		if !f.catchup {
			t.Errorf("the run for %s is not marked a catch-up", f.scheduled)
		}
		found := -1
		for i, instant := range result.Planned {
			if !matched[i] && f.scheduled.Equal(instant) {
				found = i
				break
			}
		}
		if found < 0 {
			t.Errorf("a run was for %s, which is not one of the planned instants %v (or ran twice)",
				f.scheduled, result.Planned)
			continue
		}
		matched[found] = true
	}
	for i, ok := range matched {
		if !ok {
			t.Errorf("the planned instant %s never ran", result.Planned[i])
		}
	}

	// Asking again for the same range is a no-op per instant: the instants that
	// already ran are recognised and reported, and nothing is started twice.
	repeated, err := mgr.Backfill(ctx, ref, "tick", req)
	if err != nil {
		t.Fatalf("second Backfill: %v", err)
	}
	if len(repeated.Emitted) != 0 {
		t.Errorf("the repeated backfill emitted %d instants again", len(repeated.Emitted))
	}
	if len(repeated.Skipped) != len(result.Planned) {
		t.Errorf("the repeated backfill reported %d skipped, want all %d: %+v",
			len(repeated.Skipped), len(result.Planned), repeated.Skipped)
	}
	for _, skip := range repeated.Skipped {
		if skip.Reason != control.ScheduleBackfillDuplicate {
			t.Errorf("skip reason for %s = %q, want %q", skip.Instant, skip.Reason, control.ScheduleBackfillDuplicate)
		}
	}
	assertNoFireWithin(t, fired, 700*time.Millisecond,
		"on a repeated backfill of a range that already ran")
	if got := len(log.snapshot()); got != before+len(result.Planned) {
		t.Errorf("%d runs in total, want %d -- a repeated backfill ran something twice",
			got, before+len(result.Planned))
	}

	// And the schedule still works: backfilling a pause does not leave the
	// subscription behind.
	if _, err := mgr.Resume(ctx, ref, "tick", "operator", nil); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	waitForFire(t, fired, 5*time.Second, "the schedule did not resume after the backfill")
}
