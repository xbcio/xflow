package xflow

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/trigger"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/types"
)

// The end-to-end shape of a schedule override, in the deployment the whole
// feature exists for: a workflow hosted in this process, an operator retiming or
// pausing one of its cron nodes through the schedule API, and the trigger
// actually changing what it does. Every layer below is real — the compiled
// graph, the overlay store, the manager, the reload notification, and the cron
// node's own scheduling — because the failure this guards against is precisely
// the seam between them: an override the API reports as in effect while the
// trigger keeps running the old schedule.

// tickHandler builds the action node a schedule's run is observed through: it
// signals on a channel every time it executes.
func tickHandler(fired chan struct{}) *node.Definition {
	return node.Define("test.schedule.tick", func(context.Context, *types.Input) (*types.Output, error) {
		select {
		case fired <- struct{}{}:
		default:
		}
		return &types.Output{Data: map[string]any{"ok": true}}, nil
	})
}

// waitForFire waits for one run of the schedule.
func waitForFire(t *testing.T, fired <-chan struct{}, within time.Duration, why string) {
	t.Helper()
	select {
	case <-fired:
	case <-time.After(within):
		t.Fatalf("the schedule did not fire within %s: %s", within, why)
	}
}

// assertSilent drains anything already in flight (the settle grace) and then
// requires the schedule to stay quiet for the whole window.
func assertSilent(t *testing.T, fired <-chan struct{}, settle, window time.Duration, why string) {
	t.Helper()
	time.Sleep(settle)
	for {
		select {
		case <-fired:
			continue
		default:
		}
		break
	}
	deadline := time.After(window)
	for {
		select {
		case <-fired:
			t.Fatalf("the schedule fired %s", why)
		case <-deadline:
			return
		}
	}
}

func TestLocalScheduleOverlayTakesEffectWithoutReregistering(t *testing.T) {
	ctx := context.Background()
	fired := make(chan struct{}, 64)
	tick := tickHandler(fired)
	overrides := memstore.New()
	eng, err := NewLocal(WithNodes(tick), WithScheduleOverrides(overrides))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Stop()

	wf := Workflow("schedule_overlay_e2e")
	start := wf.Node("tick", trigger.Cron().Cron("@every 1s"))
	run := wf.Node("run", tick.New(nil))
	wf.Connect(start, run)

	id, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatal(err)
	}
	// The manager is handed the SAME overlay store the engine reads, and the
	// engine itself as the reloader: a process that serves the API and hosts its
	// own triggers wires exactly this.
	mgr := control.NewScheduleManager(control.ScheduleManagerConfig{Overrides: overrides, Reloader: eng})
	rec, err := eng.workflowRegistry.GetWorkflow(ctx, id)
	if err != nil {
		t.Fatalf("resolve the registered workflow: %v", err)
	}
	ref := control.ScheduleWorkflow{Namespace: namespace.Default, ID: id, Graph: rec.Graph}

	// The definition's own cadence fires.
	waitForFire(t, fired, 5*time.Second, "the @every 1s schedule did not fire at all")

	// Retime to an hour. Nothing about this re-registers the workflow: the
	// overlay is stored and the host is told to reload.
	view, err := mgr.Patch(ctx, ref, "tick", "operator", control.SchedulePatch{Fields: map[string]*string{
		"expression": strPtr("@every 1h"),
	}}, nil)
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}
	if view.Expression != "@every 1h" || !view.Overridden {
		t.Fatalf("view = %+v, want the override in effect", view)
	}
	// next_fire moves with the override: it is derived from the effective
	// schedule, not from the definition.
	if view.NextFire == nil || view.NextFire.Sub(time.Now()) > time.Hour {
		t.Fatalf("next_fire = %v, want the retimed schedule's next instant", view.NextFire)
	}
	assertSilent(t, fired, 300*time.Millisecond, 2*time.Second,
		"after being retimed to @every 1h -- the reload did not reach the hosted trigger")

	// And back: the operator's change is reversible without a new version.
	if _, err := mgr.Patch(ctx, ref, "tick", "operator", control.SchedulePatch{Fields: map[string]*string{
		"expression": strPtr("@every 1s"),
	}}, nil); err != nil {
		t.Fatalf("Patch back: %v", err)
	}
	waitForFire(t, fired, 5*time.Second, "the schedule did not resume the fast cadence after being retimed back")

	// Pause: the host keeps hosting the schedule (so its watermark keeps
	// moving) and drops every firing.
	if _, err := mgr.Pause(ctx, ref, "tick", "operator", nil); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	assertSilent(t, fired, 300*time.Millisecond, 3*time.Second,
		"while paused")

	// Resume: from the next instant, with the paused window never replayed.
	if _, err := mgr.Resume(ctx, ref, "tick", "operator", nil); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	waitForFire(t, fired, 5*time.Second, "the schedule did not resume")

	// Deleting the overlay returns the node to the definition's cadence, which
	// here is also fast: the reload that follows must re-activate it rather than
	// leave the trigger without a subscription.
	if _, err := mgr.Patch(ctx, ref, "tick", "operator", control.SchedulePatch{Fields: map[string]*string{
		"expression": strPtr("@every 1h"),
	}}, nil); err != nil {
		t.Fatalf("Patch: %v", err)
	}
	assertSilent(t, fired, 300*time.Millisecond, 2*time.Second, "after being retimed to @every 1h again")
	if _, err := mgr.DeleteOverride(ctx, ref, "tick", "operator", nil); err != nil {
		t.Fatalf("DeleteOverride: %v", err)
	}
	waitForFire(t, fired, 5*time.Second, "the schedule did not return to its definition's cadence")
}

// assertNoFireWithin requires an EMPTY channel to stay empty for the window. It
// is the opposite of assertSilent: nothing is drained first, because the firing
// it is looking for is one that would arrive immediately.
func assertNoFireWithin(t *testing.T, fired <-chan struct{}, window time.Duration, why string) {
	t.Helper()
	select {
	case <-fired:
		t.Fatalf("the schedule fired %s", why)
	case <-time.After(window):
	}
}

// alignAfterGridBoundary returns just after an @every 1s grid boundary, so the
// next instant the live schedule will fire is most of a second away. Without it
// a resume could legitimately fire inside the window the check below watches.
func alignAfterGridBoundary() {
	now := time.Now().UTC()
	time.Sleep(time.Until(now.Truncate(time.Second).Add(time.Second)) + 60*time.Millisecond)
}

// TestLocalPausedScheduleDoesNotReplayThePausedWindow pins the ruling that
// resuming is not a catch-up: what an operator paused stays paused, and asking
// for those instants is what an explicit backfill is for.
//
// The misfire policy is what makes this testable. Under the default (skip) a
// frozen watermark and an advancing one look the same on resume, because neither
// replays anything; under fire_all they diverge completely. So the schedule is
// set to fire_all, paused across several of its instants, and resumed just after
// a grid boundary: an implementation that let the watermark freeze would replay
// the paused window as a burst right there, and one that keeps acknowledging
// instants while paused fires only on the next instant it reaches.
func TestLocalPausedScheduleDoesNotReplayThePausedWindow(t *testing.T) {
	ctx := context.Background()
	fired := make(chan struct{}, 64)
	tick := tickHandler(fired)
	overrides := memstore.New()
	eng, err := NewLocal(WithNodes(tick), WithScheduleOverrides(overrides))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Stop()

	wf := Workflow("schedule_overlay_no_replay")
	start := wf.Node("tick", trigger.Cron().Cron("@every 1s").Misfire("fire_all"))
	run := wf.Node("run", tick.New(nil))
	wf.Connect(start, run)

	id, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatal(err)
	}
	mgr := control.NewScheduleManager(control.ScheduleManagerConfig{Overrides: overrides, Reloader: eng})
	rec, err := eng.workflowRegistry.GetWorkflow(ctx, id)
	if err != nil {
		t.Fatalf("resolve the registered workflow: %v", err)
	}
	ref := control.ScheduleWorkflow{Namespace: namespace.Default, ID: id, Graph: rec.Graph}

	waitForFire(t, fired, 5*time.Second, "the schedule never fired before the pause")
	if _, err := mgr.Pause(ctx, ref, "tick", "operator", nil); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	// Several of the schedule's instants pass while it is paused. They are
	// acknowledged, not queued.
	assertSilent(t, fired, 300*time.Millisecond, 3*time.Second, "while paused")

	alignAfterGridBoundary()
	if _, err := mgr.Resume(ctx, ref, "tick", "operator", nil); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	assertNoFireWithin(t, fired, 500*time.Millisecond,
		"immediately after resuming, which is where a replay of the paused window would land")
	waitForFire(t, fired, 3*time.Second, "the schedule did not resume after the pause")
}

// TestLocalPausedScheduleStaysPausedAcrossReRegistration pins the activation
// half of pause: a paused schedule that is activated FROM SCRATCH — a restarted
// process, a subscription that was dropped, a workflow registered again — must
// come back paused. The overlay outlives the subscription that was reading it,
// which is the whole reason it is stored rather than held in memory.
func TestLocalPausedScheduleStaysPausedAcrossReRegistration(t *testing.T) {
	ctx := context.Background()
	fired := make(chan struct{}, 64)
	tick := tickHandler(fired)
	overrides := memstore.New()
	eng, err := NewLocal(WithNodes(tick), WithScheduleOverrides(overrides))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Stop()

	wf := Workflow("schedule_overlay_restart")
	start := wf.Node("tick", trigger.Cron().Cron("@every 1s"))
	run := wf.Node("run", tick.New(nil))
	wf.Connect(start, run)

	id, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatal(err)
	}
	mgr := control.NewScheduleManager(control.ScheduleManagerConfig{Overrides: overrides, Reloader: eng})
	rec, err := eng.workflowRegistry.GetWorkflow(ctx, id)
	if err != nil {
		t.Fatalf("resolve the registered workflow: %v", err)
	}
	ref := control.ScheduleWorkflow{Namespace: namespace.Default, ID: id, Graph: rec.Graph}

	waitForFire(t, fired, 5*time.Second, "the schedule never fired before the pause")
	if _, err := mgr.Pause(ctx, ref, "tick", "operator", nil); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	assertSilent(t, fired, 300*time.Millisecond, 2*time.Second, "while paused")

	// The hosted subscriptions are gone; the overlay is not. This is what a
	// restart looks like from the store's point of view.
	if err := eng.triggerRuntime.RemoveWorkflow(ctx, id); err != nil {
		t.Fatalf("RemoveWorkflow: %v", err)
	}
	if _, err := eng.AddWorkflow(ctx, wf); err != nil {
		t.Fatalf("re-register: %v", err)
	}
	assertSilent(t, fired, 300*time.Millisecond, 2*time.Second,
		"after a paused schedule was activated from scratch -- the pause was lost with the process")

	// And the paused state is still the one the API reports, so resuming is the
	// way back.
	view, err := mgr.Get(ctx, ref, "tick")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !view.Paused {
		t.Fatalf("view = %+v, want it to still be paused", view)
	}
	if _, err := mgr.Resume(ctx, ref, "tick", "operator", nil); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	waitForFire(t, fired, 5*time.Second, "the schedule did not resume after the pause")
}
