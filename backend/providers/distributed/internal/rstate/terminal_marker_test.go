package rstate

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TestTransientTerminalMarkerOutlivesShortenedStatus pins the terminal marker's
// reason for existing: a transient execution's status key is shortened to the
// completion TTL the moment it finishes, while the marker keeps the execution's
// ACTIVE retention. A backlogged task consumed between the two would otherwise
// read "no status" as "work never ran" on an execution that completed normally.
func TestTransientTerminalMarkerOutlivesShortenedStatus(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	state := New(rdb, nil, probeActiveTTL)
	ctx := context.Background()
	g := probeTransientGraph(t, "probe-terminal-marker", &types.WorkflowDef{
		Nodes: []types.NodeDef{{Name: "only", Kind: types.NodeKindTrigger}},
	})
	id := types.ExecutionID("probe-terminal-marker")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	lease := &engine.TaskLease{
		LeaseID: "L1", LeaseToken: "T1",
		IssuedAt: time.Now().UTC().Truncate(time.Millisecond), TTL: time.Minute,
		Task: engine.Task{ExecutionID: id, NodeName: "only", NodeIdx: 0, Type: engine.TaskTypeNodeExec},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v", acquired, err)
	}
	lease.Attempt = 1
	if _, claimed, err := state.ClaimTaskLease(ctx, lease); err != nil || !claimed {
		t.Fatalf("ClaimTaskLease() claimed=%v err=%v", claimed, err)
	}
	res, err := state.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID: id, NodeName: "only", Status: types.NodeStatusSuccess,
		LeaseID: "L1", LeaseToken: "T1", Attempt: 1,
		StoreOutput: true, Output: map[string]any{"k": "v"},
	})
	if err != nil {
		t.Fatalf("CommitNode() error = %v", err)
	}
	if !res.ExecutionDone || !types.IsTerminalExecutionStatus(res.ExecutionStatus) {
		t.Fatalf("CommitNode() outcome=%v status=%q, want terminal", res.Outcome, res.ExecutionStatus)
	}

	ns := namespace.FromContext(ctx)
	marker, err := rdb.Get(ctx, terminalMarkKey(ns, id)).Result()
	if err != nil {
		t.Fatalf("terminal marker missing after a terminal commit: %v", err)
	}
	if marker != string(types.ExecutionStatusSuccess) {
		t.Fatalf("marker = %q, want success", marker)
	}
	markerTTL, err := rdb.TTL(ctx, terminalMarkKey(ns, id)).Result()
	if err != nil {
		t.Fatalf("TTL marker: %v", err)
	}
	if markerTTL <= probeCompletionTTL {
		t.Fatalf("marker TTL = %v, want more than the %v completion TTL (it must carry the active retention)",
			markerTTL, probeCompletionTTL)
	}
	statusTTL, err := rdb.TTL(ctx, execKey(ns, id, "status")).Result()
	if err != nil {
		t.Fatalf("TTL status: %v", err)
	}
	if statusTTL > probeCompletionTTL+5*time.Second {
		t.Fatalf("status TTL = %v, want the %v completion TTL: shortening must still apply to the status key",
			statusTTL, probeCompletionTTL)
	}

	// The classification window: after the completion TTL lapses the status key
	// is gone, but the marker still answers "this execution finished".
	srv.FastForward(probeCompletionTTL + 5*time.Second)
	if exists := rdb.Exists(ctx, execKey(ns, id, "status")).Val(); exists != 0 {
		t.Fatalf("status key still exists after fast-forward; this test cannot exercise the expired-status window")
	}
	status, found, err := state.GetExecutionTerminalStatus(ctx, id)
	if err != nil || !found {
		t.Fatalf("GetExecutionTerminalStatus() = (%q, %v, %v) after the status key expired, want the retained terminal status",
			status, found, err)
	}
	if status != types.ExecutionStatusSuccess {
		t.Fatalf("terminal status = %q, want success", status)
	}

	// And it is not immortal: past the active retention there is nothing left,
	// which is the honest "gone" answer.
	srv.FastForward(probeActiveTTL)
	if status, found, err := state.GetExecutionTerminalStatus(ctx, id); err != nil || found {
		t.Fatalf("GetExecutionTerminalStatus() = (%q, %v, %v) after the active TTL, want not found", status, found, err)
	}
}

// TestTerminalMarkerWrittenByUpdateExecutionStatus covers the non-commit
// terminal paths (cancel, timeout, external status writes): they too must leave
// the marker, or a canceled execution's late tasks would be classified gone.
func TestTerminalMarkerWrittenByUpdateExecutionStatus(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	state := New(rdb, nil, probeActiveTTL)
	ctx := context.Background()
	g := probeTransientGraph(t, "probe-cancel-marker", &types.WorkflowDef{
		Nodes: []types.NodeDef{{Name: "only", Kind: types.NodeKindTrigger}},
	})
	id := types.ExecutionID("probe-cancel-marker")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceled, "canceled by test"); err != nil {
		t.Fatalf("UpdateExecutionStatus() error = %v", err)
	}

	ns := namespace.FromContext(ctx)
	if got, err := rdb.Get(ctx, terminalMarkKey(ns, id)).Result(); err != nil || got != string(types.ExecutionStatusCanceled) {
		t.Fatalf("terminal marker = (%q, %v), want canceled", got, err)
	}

	// A non-terminal update must not plant a marker: the execution is still
	// live, and "status missing" for it really is abnormal.
	id2 := types.ExecutionID("probe-running-marker")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id2, Graph: g, Status: types.ExecutionStatusRunning}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	if err := state.UpdateExecutionStatus(ctx, id2, types.ExecutionStatusRunning, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus() error = %v", err)
	}
	if exists := rdb.Exists(ctx, terminalMarkKey(ns, id2)).Val(); exists != 0 {
		t.Fatal("a non-terminal status update wrote a terminal marker")
	}
}

// TestDurableExecutionHasNoTerminalMarker pins the deliberate scope: a durable
// execution's status key already keeps the full active retention (nothing
// shortens it), so no marker is written — the marker exists to survive
// shortening, not to duplicate a key that expires at the same instant.
func TestDurableExecutionHasNoTerminalMarker(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	state := New(rdb, nil, time.Hour)
	ctx := context.Background()
	id := types.ExecutionID("probe-durable-no-marker")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: testGraphOneNode(), Status: types.ExecutionStatusRunning}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus() error = %v", err)
	}
	ns := namespace.FromContext(ctx)
	if exists := rdb.Exists(ctx, terminalMarkKey(ns, id)).Val(); exists != 0 {
		t.Fatal("durable execution wrote a terminal marker; the marker is only for executions whose status key is shortened")
	}
	if status, found, err := state.GetExecutionTerminalStatus(ctx, id); err != nil || found {
		t.Fatalf("GetExecutionTerminalStatus() = (%q, %v, %v), want not found", status, found, err)
	}
}

// TestGetExecutionRetentionMatchesTheBoundItsWritesUse pins the contract the
// engine's classifier depends on: the reported window is the execution's
// ACTIVE retention — the transient active TTL for a transient execution (never
// the shortened completion TTL), and execTTL for a durable one — so the
// provability bound cannot drift from the TTLs the status and marker writes
// actually use.
func TestGetExecutionRetentionMatchesTheBoundItsWritesUse(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	ctx := context.Background()

	transient := New(rdb, nil, probeActiveTTL)
	g := probeTransientGraph(t, "probe-retention-transient", &types.WorkflowDef{
		Nodes: []types.NodeDef{{Name: "only", Kind: types.NodeKindTrigger}},
	})
	id := types.ExecutionID("probe-retention-transient")
	if err := transient.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	if window, err := transient.GetExecutionRetention(ctx, id); err != nil || window != probeActiveTTL {
		t.Fatalf("GetExecutionRetention() = (%v, %v), want the %v active TTL", window, err, probeActiveTTL)
	}

	// The status key shortens at completion; the reported window must not, or
	// a task classified after the shortening would be judged against a bound
	// shorter than the marker's actual lifetime.
	if err := transient.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceled, "canceled by test"); err != nil {
		t.Fatalf("UpdateExecutionStatus() error = %v", err)
	}
	if window, err := transient.GetExecutionRetention(ctx, id); err != nil || window != probeActiveTTL {
		t.Fatalf("GetExecutionRetention() after terminalization = (%v, %v), want the %v active TTL "+
			"(the completion TTL shortening must not leak into the evidence window)", window, err, probeActiveTTL)
	}

	durable := New(rdb, nil, time.Hour)
	id2 := types.ExecutionID("probe-retention-durable")
	if err := durable.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id2, Graph: testGraphOneNode(), Status: types.ExecutionStatusRunning}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	if window, err := durable.GetExecutionRetention(ctx, id2); err != nil || window != time.Hour {
		t.Fatalf("GetExecutionRetention() = (%v, %v), want the 1h execTTL", window, err)
	}
}

// TestEngineClassificationBacklogBeyondMarkerWindow is the end-to-end timing
// case behind the tri-state: a transient execution terminalizes, then both its
// shortened status key and its terminal marker age out (miniredis FastForward
// past the active retention). A task stamped far earlier — the backlog profile
// this work exists for — must classify unattributed, not gone; the same absent
// state with a delivery stamped inside the window must still classify gone.
// The delayed-intent shape (timer wakeup: availability now, creation two
// retentions ago) must also classify unattributed, which is what keeps a
// cancelled execution's late wakeup out of the gone counter. That pins the
// classifier's window against the real TTLs, in both age directions.
func TestEngineClassificationBacklogBeyondMarkerWindow(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	state := New(rdb, nil, probeActiveTTL)
	ctx := context.Background()
	g := probeTransientGraph(t, "probe-backlog-window", &types.WorkflowDef{
		Nodes: []types.NodeDef{{Name: "only", Kind: types.NodeKindTrigger}},
	})
	id := types.ExecutionID("probe-backlog-window")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceled, "canceled by test"); err != nil {
		t.Fatalf("UpdateExecutionStatus() error = %v", err)
	}

	// Both the status key (shortened to the completion TTL) and the marker
	// (active retention) are gone now, so the classifier has no evidence left.
	srv.FastForward(probeActiveTTL + time.Minute)
	if _, found, err := state.GetExecutionTerminalStatus(ctx, id); err != nil || found {
		t.Fatalf("GetExecutionTerminalStatus() = (_, %v, %v) after the active retention, want not found", found, err)
	}

	eng := engine.New(state, nil)

	backlog := &engine.Task{
		ExecutionID:   id,
		NodeName:      "only",
		NodeIdx:       0,
		Type:          engine.TaskTypeNodeExec,
		DeliverableAt: time.Now().Add(-2 * probeActiveTTL),
	}
	_, err = eng.TaskRouting(ctx, backlog)
	var inactive *engine.ExecutionInactiveError
	if !errors.As(err, &inactive) {
		t.Fatalf("TaskRouting(backlog) error = %v, want *engine.ExecutionInactiveError", err)
	}
	if inactive.Kind != engine.ExecutionInactiveUnattributed {
		t.Fatalf("Kind = %q, want %q: a wait of %v against the %v retention window cannot be "+
			"reported as provable loss", inactive.Kind, engine.ExecutionInactiveUnattributed, 2*probeActiveTTL, probeActiveTTL)
	}

	recent := &engine.Task{
		ExecutionID:   id,
		NodeName:      "only",
		NodeIdx:       0,
		Type:          engine.TaskTypeNodeExec,
		DeliverableAt: time.Now().Add(-time.Minute),
	}
	_, err = eng.TaskRouting(ctx, recent)
	if !errors.As(err, &inactive) {
		t.Fatalf("TaskRouting(recent) error = %v, want *engine.ExecutionInactiveError", err)
	}
	if inactive.Kind != engine.ExecutionInactiveGone {
		t.Fatalf("Kind = %q, want %q: a delivery whose age is inside the window with no evidence "+
			"is a loss claim (a terminal transition during that span would still be readable)",
			inactive.Kind, engine.ExecutionInactiveGone)
	}

	// The delayed-intent shape with the real TTLs: a suspend timer armed two
	// retentions before it fires. Its availability stamp reads now, so only the
	// creation anchor can tell that the cancellation — and the marker's whole
	// lifetime — is two windows old.
	delayed := &engine.Task{
		ExecutionID:     id,
		NodeName:        "only",
		NodeIdx:         0,
		Type:            engine.TaskTypeNodeResume,
		DeliverableAt:   time.Now(),
		IntentCreatedAt: time.Now().Add(-2 * probeActiveTTL),
	}
	_, err = eng.TaskRouting(ctx, delayed)
	if !errors.As(err, &inactive) {
		t.Fatalf("TaskRouting(delayed) error = %v, want *engine.ExecutionInactiveError", err)
	}
	if inactive.Kind != engine.ExecutionInactiveUnattributed {
		t.Fatalf("Kind = %q, want %q: measuring a delayed intent's age from its availability "+
			"reads two hours as an instant and misreports a benign cancelled timer as gone",
			inactive.Kind, engine.ExecutionInactiveUnattributed)
	}
}

// TestGetExecutionRetentionKeepsAShortOverrideAfterTerminalization pins R4: a
// per-invocation TTL override shorter than the deployment default is kept only
// in the process-local execTTLs map, which evictExecutionCaches deletes the
// moment the execution terminalizes (and which no restart or replica change
// carries). If GetExecutionRetention fell back to the longer global execTTL
// after that, the classifier would call a delivery inside the override window
// gone even though the terminal record could only ever have lived the shorter
// override lifetime. The persisted retention record must answer with the TTL
// the writes actually used, after the cached override is gone.
func TestGetExecutionRetentionKeepsAShortOverrideAfterTerminalization(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	const globalTTL = time.Hour
	override := 2 * time.Minute
	state := New(rdb, nil, globalTTL)
	ctx := context.Background()
	id := types.ExecutionID("probe-retention-override")
	if err := state.CreateExecution(
		engine.WithExecutionTTL(ctx, override),
		&engine.ExecutionSnapshot{ID: id, Graph: testGraphOneNode(), Status: types.ExecutionStatusRunning},
	); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	if window, err := state.GetExecutionRetention(ctx, id); err != nil || window != override {
		t.Fatalf("GetExecutionRetention() = (%v, %v), want the %v override", window, err, override)
	}

	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceled, "canceled by test"); err != nil {
		t.Fatalf("UpdateExecutionStatus() error = %v", err)
	}
	// Premise check: terminalization must have dropped the cached override,
	// otherwise this test would not exercise the fallback path at all.
	state.ttlMu.RLock()
	_, cached := state.execTTLs[id]
	state.ttlMu.RUnlock()
	if cached {
		t.Fatal("test premise broken: the per-execution override survived terminalization")
	}
	if window, err := state.GetExecutionRetention(ctx, id); err != nil || window != override {
		t.Fatalf("GetExecutionRetention() after terminalization = (%v, %v), want the persisted %v override, "+
			"not the %v global fallback: a benign delivery that outlived the override's real evidence "+
			"window must not be classified as a loss claim", window, err, override, globalTTL)
	}

	// The consequence the record exists for: let the status key expire (it was
	// written with the override), then classify a delivery whose age sits
	// between the override and the global TTL. It must be unattributed; the
	// pre-fix fallback would have reported the global window and called a
	// benign drop "gone".
	srv.FastForward(override + time.Second)
	eng := engine.New(state, nil)
	classify := func(intentAge time.Duration) engine.ExecutionInactiveKind {
		t.Helper()
		_, err := eng.TaskRouting(ctx, &engine.Task{
			ExecutionID:     id,
			NodeName:        "only",
			NodeIdx:         0,
			Type:            engine.TaskTypeNodeExec,
			DeliverableAt:   time.Now().Add(-intentAge),
			IntentCreatedAt: time.Now().Add(-intentAge),
		})
		var inactive *engine.ExecutionInactiveError
		if !errors.As(err, &inactive) {
			t.Fatalf("TaskRouting(%v) error = %v, want *engine.ExecutionInactiveError", intentAge, err)
		}
		return inactive.Kind
	}
	if kind := classify(30 * time.Minute); kind != engine.ExecutionInactiveUnattributed {
		t.Fatalf("Kind = %q, want %q: a 30m-old delivery must be measured against the %v override it "+
			"was written with, not the %v fallback (the terminal record could not have lived that long)",
			kind, engine.ExecutionInactiveUnattributed, override, globalTTL)
	}
	// The other direction, while the record still backs the window: a delivery
	// younger than the override is a loss claim even though the status key is
	// already gone. Removing the fallback must not weaken this verdict.
	if kind := classify(time.Minute); kind != engine.ExecutionInactiveGone {
		t.Fatalf("Kind = %q, want %q: a delivery inside the %v override it was written with is the "+
			"provable loss claim the window exists for", kind, engine.ExecutionInactiveGone, override)
	}

	// The record is not immortal, and once it is gone the retention its writes
	// used cannot be confirmed: the fallback must NOT answer, because it can
	// name a longer TTL than the terminal evidence lived by, and a delivery
	// inside that inflated window would be a false loss claim. Both the
	// resolution and the classification go to "unknown"/unattributed — the
	// deliberate under-report, and the residual this record's absence signal
	// closes.
	srv.FastForward(2 * globalTTL)
	if window, err := state.GetExecutionRetention(ctx, id); err != nil || window != engine.ExecutionRetentionUnknown {
		t.Fatalf("GetExecutionRetention() after the record expired = (%v, %v), want %v (unknown)",
			window, err, engine.ExecutionRetentionUnknown)
	}
	if kind := classify(30 * time.Minute); kind != engine.ExecutionInactiveUnattributed {
		t.Fatalf("Kind = %q, want %q: with the record gone the %v fallback is not provable either",
			kind, engine.ExecutionInactiveUnattributed, globalTTL)
	}
	// The sharpest shape: a one-minute-old delivery is inside the fallback's
	// window, so the old resolution would have claimed it lost. Without a
	// confirmable window no age is inside anything.
	if kind := classify(time.Minute); kind != engine.ExecutionInactiveUnattributed {
		t.Fatalf("Kind = %q, want %q: an unprovable window must not be compared against, "+
			"even for a wait the fallback would have called provable loss",
			kind, engine.ExecutionInactiveUnattributed)
	}
}

// TestGetExecutionRetentionNeverExceedsTheWriteFallback pins the record's
// upper bound in the other direction: an override LONGER than the deployment
// default cannot be reported once the process-local map is gone, because a
// terminal write on a replica without that map resolves through getExecTTL to
// the default — the recorded window must not exceed what the evidence could
// actually have lived, or a benign late delivery inside the inflated window
// becomes a reported loss.
func TestGetExecutionRetentionNeverExceedsTheWriteFallback(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	const globalTTL = 2 * time.Minute
	override := time.Hour
	state := New(rdb, nil, globalTTL)
	ctx := context.Background()
	id := types.ExecutionID("probe-retention-long-override")
	if err := state.CreateExecution(
		engine.WithExecutionTTL(ctx, override),
		&engine.ExecutionSnapshot{ID: id, Graph: testGraphOneNode(), Status: types.ExecutionStatusRunning},
	); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceled, "canceled by test"); err != nil {
		t.Fatalf("UpdateExecutionStatus() error = %v", err)
	}
	// Premise check: terminalization dropped the cached override, so the
	// persisted record is what answers now.
	state.ttlMu.RLock()
	_, cached := state.execTTLs[id]
	state.ttlMu.RUnlock()
	if cached {
		t.Fatal("test premise broken: the per-execution override survived terminalization")
	}
	if window, err := state.GetExecutionRetention(ctx, id); err != nil || window != globalTTL {
		t.Fatalf("GetExecutionRetention() = (%v, %v), want the %v write fallback: the %v override "+
			"cannot be claimed for evidence a terminal write on another replica would have "+
			"written with the default", window, err, globalTTL, override)
	}
}
