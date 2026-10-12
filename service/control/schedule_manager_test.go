package control

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/types"
)

// scheduleTestGraph compiles a workflow with two schedules, one non-schedule
// trigger, one grouped schedule and one ordinary action node, so every
// selection rule has something to reject.
//
// The cron node's timezone is a template rather than a literal: the view is
// supposed to report what the trigger will run in, and a definition that reads
// its zone from $config is the case where "what the definition says" and "what
// the trigger does" differ.
func scheduleTestGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "nightly",
		Context: &types.WorkflowContext{
			Config: map[string]any{"tz": "Asia/Shanghai"},
		},
		Settings: &types.WorkflowSettings{Timezone: "Europe/Berlin"},
		Nodes: []types.NodeDef{
			{
				Name: "nightly", Type: NodeTypeCronTrigger, Version: 1, Kind: types.NodeKindTrigger,
				Parameters: map[string]any{"expression": "0 2 * * *", "timezone": "${{ $config.tz }}"},
			},
			{
				Name: "poll", Type: NodeTypeTimerTrigger, Version: 1, Kind: types.NodeKindTrigger,
				Parameters: map[string]any{"interval": "1m"},
			},
			{
				Name: "hook", Type: webhookTriggerType, Version: 1, Kind: types.NodeKindTrigger,
				Parameters: map[string]any{"path": "hook"},
			},
			{
				// A schedule inside a co-location group: the group is the entry
				// unit, so this node has no activation of its own to retime.
				Name: "grouped", Type: NodeTypeCronTrigger, Version: 1, Kind: types.NodeKindTrigger,
				Parameters: map[string]any{"expression": "*/5 * * * *"},
			},
			{Name: "job", Type: "xflow.http", Version: 1, Kind: types.NodeKindAction},
		},
		Groups: []types.GroupDef{{Name: "edge", Members: []string{"grouped", "job"}}},
		Connections: types.Connections{
			"hook":    {"main": types.PortConnections{Targets: []types.Connection{{Node: "job", Input: "main"}}}},
			"grouped": {"main": types.PortConnections{Targets: []types.Connection{{Node: "job", Input: "main"}}}},
			"poll":    {"main": types.PortConnections{Targets: []types.Connection{{Node: "job", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

// scheduleWebhookOnlyGraph compiles a workflow with no schedule at all, for the
// "there is nothing here" paths.
func scheduleWebhookOnlyGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "hooks",
		Nodes: []types.NodeDef{
			{Name: "hook", Type: webhookTriggerType, Version: 1, Kind: types.NodeKindTrigger},
			{Name: "job", Type: "xflow.http", Version: 1, Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"hook": {"main": types.PortConnections{Targets: []types.Connection{{Node: "job", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

type recordingReloader struct {
	calls []string
	err   error
}

func (r *recordingReloader) ReloadSchedule(_ context.Context, ns namespace.Namespace, workflowID types.WorkflowID, node string) error {
	r.calls = append(r.calls, fmt.Sprintf("%s/%s/%s", ns, workflowID, node))
	return r.err
}

type fixedStateReader struct {
	last time.Time
	ok   bool
	err  error
}

func (r fixedStateReader) LastScheduled(context.Context, namespace.Namespace, types.WorkflowID, string) (time.Time, bool, error) {
	return r.last, r.ok, r.err
}

// scheduleFixture is a manager over a fresh memstore, plus the compile clock the
// views are computed against.
type scheduleFixture struct {
	mgr  *ScheduleManager
	wf   ScheduleWorkflow
	fake *memstore.Store
	now  time.Time
}

func newScheduleFixture(t *testing.T, g *graph.Graph, cfg ScheduleManagerConfig) *scheduleFixture {
	t.Helper()
	mem := memstore.New()
	cfg.Overrides = mem
	now := time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)
	cfg.Now = func() time.Time { return now }
	mgr := NewScheduleManager(cfg)
	if mgr == nil {
		t.Fatal("NewScheduleManager returned nil with a store configured")
	}
	return &scheduleFixture{
		mgr:  mgr,
		fake: mem,
		now:  now,
		wf: ScheduleWorkflow{
			Namespace: namespace.Default,
			ID:        types.WorkflowID("wf-1"),
			Graph:     g,
		},
	}
}

func (f *scheduleFixture) node(t *testing.T, name string) ScheduleView {
	t.Helper()
	view, err := f.mgr.Get(context.Background(), f.wf, name)
	if err != nil {
		t.Fatalf("Get(%s): %v", name, err)
	}
	return view
}

func (f *scheduleFixture) patch(t *testing.T, node string, fields map[string]*string) ScheduleView {
	t.Helper()
	view, err := f.mgr.Patch(context.Background(), f.wf, node, "alice", SchedulePatch{Fields: fields}, nil)
	if err != nil {
		t.Fatalf("Patch(%s): %v", node, err)
	}
	return view
}

func strPtr(s string) *string { return &s }

// TestScheduleListDerivesFromTheGraph pins which nodes are schedules, what the
// view reports before any overlay exists, and that the order is stable.
func TestScheduleListDerivesFromTheGraph(t *testing.T) {
	f := newScheduleFixture(t, scheduleTestGraph(t), ScheduleManagerConfig{})
	views, err := f.mgr.List(context.Background(), f.wf)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	names := make([]string, 0, len(views))
	for _, v := range views {
		names = append(names, v.Node)
	}
	// "grouped" is a group member and "hook" is not a schedule; only the two
	// independent schedules are listed, ordered by node name.
	if len(names) != 2 || names[0] != "nightly" || names[1] != "poll" {
		t.Fatalf("listed nodes = %v, want [nightly poll]", names)
	}

	nightly := f.node(t, "nightly")
	if nightly.Kind != ScheduleKindCron {
		t.Errorf("kind = %q, want %q", nightly.Kind, ScheduleKindCron)
	}
	if nightly.Expression != "0 2 * * *" {
		t.Errorf("expression = %q, want the definition's", nightly.Expression)
	}
	// The template is rendered, not echoed: the view answers what the trigger
	// will run in.
	if nightly.Timezone != "Asia/Shanghai" {
		t.Errorf("timezone = %q, want the rendered $config value", nightly.Timezone)
	}
	// The cron node's documented defaults, resolved: an operator asking "what
	// happens to a missed instant" must not have to know that empty means skip.
	if nightly.Misfire != "skip" || nightly.Overlap != "allow" {
		t.Errorf("policies = (%q, %q), want (skip, allow)", nightly.Misfire, nightly.Overlap)
	}
	if nightly.Overridden || nightly.Paused || nightly.Orphaned {
		t.Errorf("an untouched schedule is not overridden/paused/orphaned: %+v", nightly)
	}
	wantNext := time.Date(2026, 5, 5, 2, 0, 0, 0, mustLocation(t, "Asia/Shanghai"))
	if nightly.NextFire == nil || !nightly.NextFire.Equal(wantNext) {
		t.Errorf("next_fire = %v, want %v", nightly.NextFire, wantNext)
	}

	poll := f.node(t, "poll")
	if poll.Kind != ScheduleKindTimer {
		t.Errorf("timer kind = %q", poll.Kind)
	}
	// A timer has no zone, no misfire policy and no overlap policy, and the view
	// says so by leaving them empty rather than by inventing defaults.
	if poll.Timezone != "" || poll.Misfire != "" || poll.Overlap != "" {
		t.Errorf("timer reported cron-only fields: %+v", poll)
	}
	if poll.NextFire == nil || !poll.NextFire.Equal(time.Date(2026, 5, 4, 12, 1, 0, 0, time.UTC)) {
		t.Errorf("timer next_fire = %v, want the grid's next minute", poll.NextFire)
	}
}

// TestScheduleListIgnoresNonScheduleNodes pins that a workflow with no schedule
// lists nothing, and that addressing one is not-found rather than an empty view.
func TestScheduleListIgnoresNonScheduleNodes(t *testing.T) {
	f := newScheduleFixture(t, scheduleWebhookOnlyGraph(t), ScheduleManagerConfig{})
	views, err := f.mgr.List(context.Background(), f.wf)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(views) != 0 {
		t.Fatalf("views = %+v, want none", views)
	}
	for _, name := range []string{"hook", "job", "missing"} {
		_, err := f.mgr.Get(context.Background(), f.wf, name)
		if !errors.Is(err, ErrScheduleNotFound) {
			t.Errorf("Get(%s) error = %v, want ErrScheduleNotFound", name, err)
		}
	}
}

// TestSchedulePatchOverridesAndReverts walks the three patch states: setting a
// value, leaving an unnamed field alone, and reverting with an explicit null.
func TestSchedulePatchOverridesAndReverts(t *testing.T) {
	f := newScheduleFixture(t, scheduleTestGraph(t), ScheduleManagerConfig{})
	ctx := context.Background()

	view := f.patch(t, "nightly", map[string]*string{
		ScheduleFieldExpression: strPtr("0 3 * * *"),
		ScheduleFieldMisfire:    strPtr("fire_once"),
	})
	if !view.Overridden || view.Revision != 1 {
		t.Fatalf("after patch: overridden=%v revision=%d, want true/1", view.Overridden, view.Revision)
	}
	if view.Expression != "0 3 * * *" || view.Misfire != "fire_once" {
		t.Errorf("patched view = %+v", view)
	}
	// The timezone was not named, so it keeps following the definition.
	if view.Timezone != "Asia/Shanghai" {
		t.Errorf("timezone = %q, want the definition's rendered value", view.Timezone)
	}
	wantNext := time.Date(2026, 5, 5, 3, 0, 0, 0, mustLocation(t, "Asia/Shanghai"))
	if view.NextFire == nil || !view.NextFire.Equal(wantNext) {
		t.Errorf("next_fire = %v, want the patched schedule's %v", view.NextFire, wantNext)
	}

	// Only the expression is reverted; misfire keeps what the previous patch
	// set. This is the case DELETE .../override cannot express.
	view = f.patch(t, "nightly", map[string]*string{ScheduleFieldExpression: nil})
	if view.Expression != "0 2 * * *" {
		t.Errorf("expression = %q, want the definition's value back", view.Expression)
	}
	if view.Misfire != "fire_once" {
		t.Errorf("misfire = %q, want the earlier override kept", view.Misfire)
	}
	if view.Revision != 2 {
		t.Errorf("revision = %d, want 2 -- every write bumps it", view.Revision)
	}

	// The whole overlay goes back to the definition.
	after, err := f.mgr.DeleteOverride(ctx, f.wf, "nightly", "bob", nil)
	if err != nil {
		t.Fatalf("DeleteOverride: %v", err)
	}
	if after.Overridden || after.Expression != "0 2 * * *" || after.Misfire != "skip" {
		t.Errorf("after delete: %+v", after)
	}
	if _, err := f.mgr.DeleteOverride(ctx, f.wf, "nightly", "bob", nil); !errors.Is(err, ErrScheduleNotFound) {
		t.Errorf("deleting twice = %v, want ErrScheduleNotFound", err)
	}
}

// TestSchedulePatchRefusesFieldsTheKindCannotCarry pins the capability check:
// an override the trigger would ignore must not be stored as if it applied.
func TestSchedulePatchRefusesFieldsTheKindCannotCarry(t *testing.T) {
	f := newScheduleFixture(t, scheduleTestGraph(t), ScheduleManagerConfig{})
	cases := []struct {
		node   string
		fields map[string]*string
		field  string
	}{
		{"poll", map[string]*string{ScheduleFieldTimezone: strPtr("UTC")}, ScheduleFieldTimezone},
		{"poll", map[string]*string{ScheduleFieldMisfire: strPtr("skip")}, ScheduleFieldMisfire},
		{"poll", map[string]*string{ScheduleFieldOverlap: strPtr("skip")}, ScheduleFieldOverlap},
		{"nightly", map[string]*string{ScheduleFieldExpression: strPtr("not a cron")}, ScheduleFieldExpression},
		{"poll", map[string]*string{ScheduleFieldExpression: strPtr("-1m")}, ScheduleFieldExpression},
		{"nightly", map[string]*string{ScheduleFieldTimezone: strPtr("Mars/Olympus")}, ScheduleFieldTimezone},
		{"nightly", map[string]*string{ScheduleFieldMisfire: strPtr("fire_later")}, ScheduleFieldMisfire},
		{"nightly", map[string]*string{ScheduleFieldOverlap: strPtr("queue")}, ScheduleFieldOverlap},
		{"nightly", map[string]*string{"paused": strPtr("true")}, "paused"},
	}
	for _, tc := range cases {
		_, err := f.mgr.Patch(context.Background(), f.wf, tc.node, "alice", SchedulePatch{Fields: tc.fields}, nil)
		var fieldErr *ScheduleFieldError
		if !errors.As(err, &fieldErr) {
			t.Errorf("patch %v on %s = %v, want *ScheduleFieldError", tc.fields, tc.node, err)
			continue
		}
		if fieldErr.Field != tc.field {
			t.Errorf("patch %v on %s blamed %q, want %q", tc.fields, tc.node, fieldErr.Field, tc.field)
		}
	}
	// Nothing was stored by any of the refusals.
	views, err := f.mgr.List(context.Background(), f.wf)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, v := range views {
		if v.Overridden {
			t.Errorf("a refused patch left an overlay behind: %+v", v)
		}
	}
}

// TestSchedulePatchIfMatch pins the conditional-write contract: 0 is
// create-only, a stale revision conflicts, and the current one writes.
func TestSchedulePatchIfMatch(t *testing.T) {
	f := newScheduleFixture(t, scheduleTestGraph(t), ScheduleManagerConfig{})
	ctx := context.Background()
	patch := SchedulePatch{Fields: map[string]*string{ScheduleFieldExpression: strPtr("0 4 * * *")}}

	if _, err := f.mgr.Patch(ctx, f.wf, "nightly", "alice", patch, uint64Ptr(7)); !errors.Is(err, store.ErrRevisionConflict) {
		t.Errorf("create with a non-zero If-Match = %v, want ErrRevisionConflict", err)
	}
	if _, err := f.mgr.Patch(ctx, f.wf, "nightly", "alice", patch, uint64Ptr(0)); err != nil {
		t.Fatalf("create with If-Match 0: %v", err)
	}
	if _, err := f.mgr.Patch(ctx, f.wf, "nightly", "alice", patch, uint64Ptr(0)); !errors.Is(err, store.ErrRevisionConflict) {
		t.Errorf("second create with If-Match 0 = %v, want ErrRevisionConflict", err)
	}
	if _, err := f.mgr.Patch(ctx, f.wf, "nightly", "alice", patch, uint64Ptr(9)); !errors.Is(err, store.ErrRevisionConflict) {
		t.Errorf("stale If-Match = %v, want ErrRevisionConflict", err)
	}
	view, err := f.mgr.Patch(ctx, f.wf, "nightly", "alice", patch, uint64Ptr(1))
	if err != nil {
		t.Fatalf("matching If-Match: %v", err)
	}
	if view.Revision != 2 {
		t.Errorf("revision = %d, want 2", view.Revision)
	}
	// The revision the view reports is the one a next conditional write needs.
	if _, err := f.mgr.Patch(ctx, f.wf, "nightly", "alice", patch, &view.Revision); err != nil {
		t.Errorf("the revision returned by the view was refused: %v", err)
	}
}

// TestSchedulePatchEmptyIsARead pins that a patch naming no field writes
// nothing: there is no change to make, and a revision bump would invalidate
// another writer's If-Match for free.
func TestSchedulePatchEmptyIsARead(t *testing.T) {
	reloader := &recordingReloader{}
	f := newScheduleFixture(t, scheduleTestGraph(t), ScheduleManagerConfig{Reloader: reloader})
	ctx := context.Background()

	if _, err := f.mgr.Patch(ctx, f.wf, "nightly", "alice", SchedulePatch{}, nil); err != nil {
		t.Fatalf("empty patch: %v", err)
	}
	if _, err := f.mgr.Patch(ctx, f.wf, "nightly", "alice", SchedulePatch{}, uint64Ptr(0)); err != nil {
		t.Fatalf("empty patch with If-Match 0: %v", err)
	}
	if _, err := f.mgr.Patch(ctx, f.wf, "nightly", "alice", SchedulePatch{}, uint64Ptr(3)); !errors.Is(err, store.ErrRevisionConflict) {
		t.Errorf("empty patch with a stale If-Match = %v, want ErrRevisionConflict", err)
	}
	if len(reloader.calls) != 0 {
		t.Errorf("an empty patch notified the host: %v", reloader.calls)
	}
	if _, err := f.mgr.Get(ctx, f.wf, "nightly"); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if views, _ := f.mgr.List(ctx, f.wf); views[0].Overridden {
		t.Errorf("an empty patch created an overlay: %+v", views[0])
	}
}

// TestSchedulePauseAndResumeAreIdempotent pins the pause semantics: pausing is
// an action, not a resource edit, so repeating it changes no revision, and the
// pause flag is what the view reports.
func TestSchedulePauseAndResumeAreIdempotent(t *testing.T) {
	reloader := &recordingReloader{}
	f := newScheduleFixture(t, scheduleTestGraph(t), ScheduleManagerConfig{Reloader: reloader})
	ctx := context.Background()

	paused, err := f.mgr.Pause(ctx, f.wf, "nightly", "alice", nil)
	if err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if !paused.Paused || !paused.Overridden || paused.Revision != 1 {
		t.Fatalf("paused view = %+v", paused)
	}
	again, err := f.mgr.Pause(ctx, f.wf, "nightly", "alice", nil)
	if err != nil {
		t.Fatalf("Pause twice: %v", err)
	}
	if again.Revision != 1 {
		t.Errorf("repeated pause bumped the revision to %d", again.Revision)
	}
	if len(reloader.calls) != 1 {
		t.Errorf("reload calls = %v, want one -- a no-op pause changes nothing to reload", reloader.calls)
	}
	if _, err := f.mgr.Pause(ctx, f.wf, "nightly", "alice", uint64Ptr(4)); !errors.Is(err, store.ErrRevisionConflict) {
		t.Errorf("pause with a stale If-Match = %v, want ErrRevisionConflict", err)
	}

	resumed, err := f.mgr.Resume(ctx, f.wf, "nightly", "alice", nil)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if resumed.Paused {
		t.Errorf("resumed view is still paused: %+v", resumed)
	}
	if resumed.Expression != "0 2 * * *" {
		t.Errorf("resume changed the expression to %q", resumed.Expression)
	}
	// Resuming what is not paused is a no-op too.
	if _, err := f.mgr.Resume(ctx, f.wf, "nightly", "alice", nil); err != nil {
		t.Fatalf("Resume twice: %v", err)
	}
	if again, _ := f.mgr.Get(ctx, f.wf, "nightly"); again.Revision != resumed.Revision {
		t.Errorf("repeated resume bumped the revision to %d", again.Revision)
	}
}

// TestSchedulePauseSurvivesAPatch pins that pausing and patching share one row:
// a pause is not a second overlay that a later patch could silently drop.
func TestSchedulePauseSurvivesAPatch(t *testing.T) {
	f := newScheduleFixture(t, scheduleTestGraph(t), ScheduleManagerConfig{})
	ctx := context.Background()
	if _, err := f.mgr.Pause(ctx, f.wf, "nightly", "alice", nil); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	view := f.patch(t, "nightly", map[string]*string{ScheduleFieldExpression: strPtr("*/10 * * * *")})
	if !view.Paused {
		t.Errorf("the patch cleared the pause: %+v", view)
	}
	if view.Expression != "*/10 * * * *" {
		t.Errorf("expression = %q", view.Expression)
	}
	if view.UpdatedBy != "alice" {
		t.Errorf("updated_by = %q, want the actor", view.UpdatedBy)
	}
}

// TestScheduleOverlayOutlivesItsNode pins the re-registration ruling: an
// overlay is keyed by node NAME, so it survives a new version that still
// declares the node, and is reported as orphaned — never deleted — when the
// node is gone or has become something else.
func TestScheduleOverlayOutlivesItsNode(t *testing.T) {
	f := newScheduleFixture(t, scheduleTestGraph(t), ScheduleManagerConfig{})
	ctx := context.Background()
	if _, err := f.mgr.Patch(ctx, f.wf, "nightly", "alice", SchedulePatch{Fields: map[string]*string{
		ScheduleFieldExpression: strPtr("0 5 * * *"),
	}}, nil); err != nil {
		t.Fatalf("Patch: %v", err)
	}

	// Re-registration: same node name, a freshly compiled graph.
	f.wf.Graph = scheduleTestGraph(t)
	view := f.node(t, "nightly")
	if !view.Overridden || view.Expression != "0 5 * * *" {
		t.Errorf("the overlay did not survive re-registration: %+v", view)
	}

	// The node is gone from the new graph: the overlay is listed as orphaned,
	// and is neither applied nor removed.
	f.wf.Graph = scheduleWebhookOnlyGraph(t)
	views, err := f.mgr.List(ctx, f.wf)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(views) != 1 {
		t.Fatalf("views = %+v, want the orphaned overlay", views)
	}
	orphan := views[0]
	if !orphan.Orphaned || orphan.Node != "nightly" || orphan.Expression != "0 5 * * *" {
		t.Errorf("orphaned view = %+v", orphan)
	}
	if orphan.Kind != "" || orphan.NextFire != nil {
		t.Errorf("an orphaned overlay claims a kind or a next fire: %+v", orphan)
	}
	if got, err := f.mgr.Get(ctx, f.wf, "nightly"); err != nil || !got.Orphaned {
		t.Errorf("Get on an orphaned node = (%+v, %v)", got, err)
	}
	// Retiming an orphan is refused: there is no kind left to validate against.
	if _, err := f.mgr.Patch(ctx, f.wf, "nightly", "alice", SchedulePatch{Fields: map[string]*string{
		ScheduleFieldExpression: strPtr("0 6 * * *"),
	}}, nil); !errors.Is(err, ErrScheduleNotFound) {
		t.Errorf("Patch on an orphan = %v, want ErrScheduleNotFound", err)
	}
	if _, err := f.mgr.Pause(ctx, f.wf, "nightly", "alice", nil); !errors.Is(err, ErrScheduleNotFound) {
		t.Errorf("Pause on an orphan = %v, want ErrScheduleNotFound", err)
	}
	// Deleting is the one thing an orphan must allow, or it can never be cleaned.
	if _, err := f.mgr.DeleteOverride(ctx, f.wf, "nightly", "bob", nil); err != nil {
		t.Fatalf("DeleteOverride on an orphan: %v", err)
	}
	if views, _ := f.mgr.List(ctx, f.wf); len(views) != 0 {
		t.Errorf("views = %+v after deleting the orphan", views)
	}
}

// TestScheduleReloadFailureIsDistinctFromARejectedWrite pins the contract the
// API needs to tell an operator what to do next: the write happened.
func TestScheduleReloadFailureIsDistinctFromARejectedWrite(t *testing.T) {
	reloader := &recordingReloader{err: errors.New("host is not hosting this workflow")}
	f := newScheduleFixture(t, scheduleTestGraph(t), ScheduleManagerConfig{Reloader: reloader})
	ctx := context.Background()

	view, err := f.mgr.Patch(ctx, f.wf, "nightly", "alice", SchedulePatch{Fields: map[string]*string{
		ScheduleFieldExpression: strPtr("0 7 * * *"),
	}}, nil)
	if !errors.Is(err, ErrScheduleReload) {
		t.Fatalf("Patch = %v, want ErrScheduleReload", err)
	}
	if view.Expression != "0 7 * * *" || view.Revision != 1 {
		t.Errorf("the stored view was not returned with the reload failure: %+v", view)
	}
	// Stored, not rolled back: the operator's next read sees it.
	got := f.node(t, "nightly")
	if got.Expression != "0 7 * * *" {
		t.Errorf("get after a reload failure = %+v", got)
	}
}

// TestScheduleNoReloaderIsSupported pins that cluster mode (an overlay with no
// in-process host) is a working configuration rather than a failure.
func TestScheduleNoReloaderIsSupported(t *testing.T) {
	f := newScheduleFixture(t, scheduleTestGraph(t), ScheduleManagerConfig{})
	if _, err := f.mgr.Patch(context.Background(), f.wf, "nightly", "alice", SchedulePatch{Fields: map[string]*string{
		ScheduleFieldExpression: strPtr("0 8 * * *"),
	}}, nil); err != nil {
		t.Fatalf("Patch without a reloader: %v", err)
	}
}

// TestScheduleWatermarkIsReported pins the optional state reader: a host that
// can answer supplies last_scheduled, one that cannot leaves it null.
func TestScheduleWatermarkIsReported(t *testing.T) {
	last := time.Date(2026, 5, 4, 2, 0, 0, 0, time.UTC)
	f := newScheduleFixture(t, scheduleTestGraph(t), ScheduleManagerConfig{
		State: fixedStateReader{last: last, ok: true},
	})
	view := f.node(t, "nightly")
	if view.LastScheduled == nil || !view.LastScheduled.Equal(last) {
		t.Errorf("last_scheduled = %v, want %v", view.LastScheduled, last)
	}

	// No watermark recorded yet is not the same as the zero instant.
	unknown := newScheduleFixture(t, scheduleTestGraph(t), ScheduleManagerConfig{
		State: fixedStateReader{},
	})
	if got := unknown.node(t, "nightly").LastScheduled; got != nil {
		t.Errorf("last_scheduled = %v, want nil", got)
	}

	// A state read that fails is reported, not silently downgraded to null.
	broken := newScheduleFixture(t, scheduleTestGraph(t), ScheduleManagerConfig{
		State: fixedStateReader{err: errors.New("state store is down")},
	})
	if _, err := broken.mgr.Get(context.Background(), broken.wf, "nightly"); err == nil {
		t.Error("a failing state read was swallowed")
	}
}

// TestScheduleNamespacesAreIsolated pins that overlays are scoped by the
// principal's namespace: the same workflow id in another namespace sees none.
func TestScheduleNamespacesAreIsolated(t *testing.T) {
	f := newScheduleFixture(t, scheduleTestGraph(t), ScheduleManagerConfig{})
	ctx := context.Background()
	if _, err := f.mgr.Patch(ctx, f.wf, "nightly", "alice", SchedulePatch{Fields: map[string]*string{
		ScheduleFieldExpression: strPtr("0 9 * * *"),
	}}, nil); err != nil {
		t.Fatalf("Patch: %v", err)
	}

	other := f.wf
	other.Namespace = namespace.Namespace("tenant-b")
	view, err := f.mgr.Get(ctx, other, "nightly")
	if err != nil {
		t.Fatalf("Get in another namespace: %v", err)
	}
	if view.Overridden || view.Expression != "0 2 * * *" {
		t.Errorf("another namespace saw the overlay: %+v", view)
	}
	// An overlay only exists in the namespace that wrote it.
	if _, err := f.mgr.DeleteOverride(ctx, other, "nightly", "mallory", nil); !errors.Is(err, ErrScheduleNotFound) {
		t.Errorf("delete from another namespace = %v, want ErrScheduleNotFound", err)
	}
	// An invalid scope is refused by the store rather than defaulted.
	bad := f.wf
	bad.Namespace = ""
	if _, err := f.mgr.Patch(ctx, bad, "nightly", "alice", SchedulePatch{Fields: map[string]*string{
		ScheduleFieldExpression: strPtr("0 9 * * *"),
	}}, nil); !errors.Is(err, store.ErrInvalidNamespace) {
		t.Errorf("empty namespace = %v, want ErrInvalidNamespace", err)
	}
}

// TestScheduleManagerNeedsAStore pins the refusal shape: a deployment with no
// overlay store gets an error, not a nil dereference.
func TestScheduleManagerNeedsAStore(t *testing.T) {
	if NewScheduleManager(ScheduleManagerConfig{}) != nil {
		t.Fatal("NewScheduleManager without a store must return nil")
	}
	var mgr *ScheduleManager
	if _, err := mgr.List(context.Background(), ScheduleWorkflow{}); err == nil {
		t.Error("a nil manager must refuse rather than panic")
	}
}

// TestMergeScheduleOverrideParams pins the host-side mapping: the overlay's
// expression lands on the parameter its OWN kind reads, and only that one.
func TestMergeScheduleOverrideParams(t *testing.T) {
	params := map[string]any{"expression": "0 2 * * *", "timezone": "UTC", "misfire": "skip"}
	cron := MergeScheduleOverrideParams(ScheduleKindCron, params, &store.ScheduleOverride{
		Expression: strPtr("0 3 * * *"),
		Timezone:   strPtr("Europe/Berlin"),
		Misfire:    strPtr("fire_all"),
		Overlap:    strPtr("skip"),
	})
	if cron["expression"] != "0 3 * * *" || cron["timezone"] != "Europe/Berlin" ||
		cron["misfire"] != "fire_all" || cron["overlap"] != "skip" {
		t.Errorf("cron params = %v", cron)
	}
	// The caller's map is not written into: it may be the compiled graph's own.
	if params["expression"] != "0 2 * * *" || params["timezone"] != "UTC" || params["misfire"] != "skip" {
		t.Errorf("the input params were mutated: %v", params)
	}
	if _, ok := params["overlap"]; ok {
		t.Errorf("the input params grew an overlap key: %v", params)
	}

	timer := MergeScheduleOverrideParams(ScheduleKindTimer, map[string]any{"interval": "1m"}, &store.ScheduleOverride{
		Expression: strPtr("5m"),
		Timezone:   strPtr("Europe/Berlin"),
		Misfire:    strPtr("fire_all"),
		Overlap:    strPtr("skip"),
	})
	if timer["interval"] != "5m" {
		t.Errorf("timer interval = %v, want the override", timer["interval"])
	}
	if _, ok := timer["expression"]; ok {
		t.Errorf("the timer grew an expression key: %v", timer)
	}
	for _, key := range []string{"timezone", "misfire", "overlap"} {
		if _, ok := timer[key]; ok {
			t.Errorf("a cron-only field reached the timer: %v", timer)
		}
	}

	// No overlay is the identity: the graph's own params must come through
	// untouched, not through a copy that could lose a key.
	identity := MergeScheduleOverrideParams(ScheduleKindCron, params, nil)
	if len(identity) != len(params) || identity["expression"] != "0 2 * * *" {
		t.Errorf("nil overlay changed the params: %v", identity)
	}
}

// TestScheduleExpressionNext pins the instant a rule produces, including the
// timer's grid: a tick's identity is the interval's own truncation, not "last
// firing plus interval", which is what makes a timer's next fire knowable.
func TestScheduleExpressionNext(t *testing.T) {
	from := time.Date(2026, 5, 4, 12, 0, 30, 0, time.UTC)

	cron, err := ParseScheduleExpression(ScheduleKindCron, "*/15 * * * *", "UTC")
	if err != nil {
		t.Fatalf("parse cron: %v", err)
	}
	if got, want := cron.Next(from), time.Date(2026, 5, 4, 12, 15, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("cron next = %v, want %v", got, want)
	}

	timer, err := ParseScheduleExpression(ScheduleKindTimer, "5m", "")
	if err != nil {
		t.Fatalf("parse timer: %v", err)
	}
	if got, want := timer.Next(from), time.Date(2026, 5, 4, 12, 5, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("timer next = %v, want %v", got, want)
	}
	// On an exact boundary the next instant is the following one, never the
	// instant passed in: a schedule does not fire twice for one moment.
	if got, want := timer.Next(time.Date(2026, 5, 4, 12, 5, 0, 0, time.UTC)),
		time.Date(2026, 5, 4, 12, 10, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("timer next on a boundary = %v, want %v", got, want)
	}

	if _, err := ParseScheduleExpression(ScheduleKindCron, "0 2 * * *", "Mars/Olympus"); err == nil {
		t.Error("an unknown zone parsed")
	}
	if _, err := ParseScheduleExpression(ScheduleKindCron, "", "UTC"); err == nil {
		t.Error("an empty expression parsed")
	}
}

// TestScheduleViewToleratesAnInvalidDefinedExpression pins that a definition
// the trigger will refuse at activation does not blind the whole listing.
func TestScheduleViewToleratesAnInvalidDefinedExpression(t *testing.T) {
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "broken",
		Nodes: []types.NodeDef{
			{Name: "bad", Type: NodeTypeCronTrigger, Version: 1, Kind: types.NodeKindTrigger,
				Parameters: map[string]any{"expression": "definitely not cron"}},
			{Name: "job", Type: "xflow.http", Version: 1, Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"bad": {"main": types.PortConnections{Targets: []types.Connection{{Node: "job", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	f := newScheduleFixture(t, g, ScheduleManagerConfig{})
	view := f.node(t, "bad")
	if view.Expression != "definitely not cron" || view.NextFire != nil {
		t.Errorf("view = %+v, want the expression reported and no next fire", view)
	}
	if view.Timezone != "UTC" {
		t.Errorf("timezone = %q, want the documented UTC fallback", view.Timezone)
	}
}

func mustLocation(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatalf("LoadLocation(%s): %v", name, err)
	}
	return loc
}

func uint64Ptr(v uint64) *uint64 { return &v }
