package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// recordingGroupObserver is a GroupObserver test double that records every
// call verbatim (not just "was it called"), so tests can assert exact call
// counts and the literal string arguments the engine classified each event
// into.
type recordingGroupObserver struct {
	mu              sync.Mutex
	leaseAcquired   int
	leaseExpired    int
	renewResults    []string
	renewDurations  []time.Duration
	commitOutcomes  []string
	commitDurations []time.Duration
}

func (r *recordingGroupObserver) OnGroupLeaseAcquired(_ context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.leaseAcquired++
}

func (r *recordingGroupObserver) OnGroupLeaseExpired(_ context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.leaseExpired++
}

func (r *recordingGroupObserver) OnGroupLeaseRenew(_ context.Context, result string, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.renewResults = append(r.renewResults, result)
	r.renewDurations = append(r.renewDurations, d)
}

func (r *recordingGroupObserver) OnGroupCommit(_ context.Context, outcome string, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commitOutcomes = append(r.commitOutcomes, outcome)
	r.commitDurations = append(r.commitDurations, d)
}

var _ GroupObserver = (*recordingGroupObserver)(nil)

// newObserverGroupLeaseEngine builds an engine over fakeGroupLeaseState (the
// same double group_lease_test.go's BuildGroupLease/CommitGroupResult tests
// use) with the given options applied, so observer wiring tests exercise the
// exact remote-lease code path (BuildGroupLease / CommitGroupResult /
// RenewGroupLease) rather than a bespoke double that could diverge from it.
func newObserverGroupLeaseEngine(t *testing.T, opts ...Option) (*Engine, *graph.Graph, types.ExecutionID) {
	t.Helper()
	def := &types.WorkflowDef{
		Name:    "test-grouped-observer",
		Version: "1",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "http.request", Version: 1, Parameters: map[string]any{"url": "http://a"}},
			{Name: "B", Type: "code.python", Version: 2, Parameters: map[string]any{"script": "pass"}},
			{Name: "C", Type: "http.request", Version: 1, Parameters: map[string]any{"url": "http://c"}},
			{Name: "D", Type: "db.query", Version: 1},
		},
		Groups: []types.GroupDef{
			{Name: "grp1", Members: []string{"A", "B", "C"}},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}},
			"B": {"main": {Targets: []types.Connection{{Node: "C", Input: "main"}}}},
			"C": {"result": {Targets: []types.Connection{{Node: "D", Input: "main"}}}},
		},
	}

	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	execID := types.ExecutionID("exec-group-observer-1")
	state := &fakeGroupLeaseState{fakeState: newFakeState()}
	state.fakeState.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID:     execID,
		Graph:  g,
		Status: types.ExecutionStatusRunning,
	})

	q := &fakeQueue{}
	eng := New(state, q, opts...)
	eng.cacheExecutionGraph(execID, g)

	return eng, g, execID
}

// TestNotifyGroupLeaseAcquired_RemotePath drives BuildGroupLease (the
// remote-runner lease path, service/control/group_control_loop.go's only
// caller) and asserts OnGroupLeaseAcquired fires exactly once per genuine
// acquisition — not on the second call, which fails with
// ErrGroupLeaseAlreadyActive and must not double-count.
func TestNotifyGroupLeaseAcquired_RemotePath(t *testing.T) {
	obs := &recordingGroupObserver{}
	eng, g, execID := newObserverGroupLeaseEngine(t, WithGroupObserver(obs))
	ctx := context.Background()

	gm := g.Groups()[0]
	task := &Task{
		ExecutionID: execID,
		NodeName:    gm.Name,
		NodeIdx:     gm.EntryIdx,
		UnitIdx:     gm.UnitIdx,
		Type:        TaskTypeGroupExec,
	}

	if _, _, err := eng.BuildGroupLease(ctx, task); err != nil {
		t.Fatalf("BuildGroupLease: %v", err)
	}
	if obs.leaseAcquired != 1 {
		t.Fatalf("leaseAcquired = %d, want 1", obs.leaseAcquired)
	}

	if _, _, err := eng.BuildGroupLease(ctx, task); err != ErrGroupLeaseAlreadyActive {
		t.Fatalf("second BuildGroupLease error = %v, want ErrGroupLeaseAlreadyActive", err)
	}
	if obs.leaseAcquired != 1 {
		t.Fatalf("leaseAcquired after failed second acquire = %d, want still 1 (no double count)", obs.leaseAcquired)
	}
}

// TestNotifyGroupLeaseAcquired_LocalPath drives executeGroup (the
// in-process GroupExecutor path) through handleSystemTask, the same entry
// point service dispatch uses, and asserts OnGroupLeaseAcquired fires once.
func TestNotifyGroupLeaseAcquired_LocalPath(t *testing.T) {
	g := buildSingleGroupGraph(t)
	groupUnitIdx := findGroupUnit(t, g)

	obs := &recordingGroupObserver{}
	fake := &fakeGroupExecutor{exits: []GroupExit{{NodeName: "g.sink", Port: "main", Data: map[string]any{"ok": true}}}}
	state := &fakeStateWithGroup{fakeState: newFakeState(), groupState: &fakeGroupState{}}
	execID := types.ExecutionID("exec-observer-local-acquired")
	state.fakeState.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID:     execID,
		Graph:  g,
		Status: types.ExecutionStatusRunning,
	})

	q := &fakeQueue{}
	eng := New(state, q, WithGroupExecutor(fake), WithGroupObserver(obs))
	eng.cacheExecutionGraph(execID, g)

	handled, err := eng.handleSystemTask(context.Background(), &Task{
		ExecutionID: execID,
		NodeName:    "g",
		UnitIdx:     groupUnitIdx,
		Type:        TaskTypeGroupExec,
	}, true)
	if err != nil {
		t.Fatalf("handleSystemTask: %v", err)
	}
	if !handled {
		t.Fatal("expected handled=true for TaskTypeGroupExec")
	}
	if obs.leaseAcquired != 1 {
		t.Fatalf("leaseAcquired = %d, want 1", obs.leaseAcquired)
	}
}

// contendedGroupState is a GroupStateStore test double whose AcquireGroupLease
// always loses the race — the backend's way of saying another executor already
// owns this unit. Distinct from fakeGroupState, which always acquires.
type contendedGroupState struct {
	*fakeState
}

func (c *contendedGroupState) AcquireGroupLease(_ context.Context, _ *GroupLease) (bool, error) {
	return false, nil
}

func (c *contendedGroupState) RenewGroupLease(_ context.Context, _ types.ExecutionID, _ int, _ LeaseToken, _ time.Time) (bool, error) {
	return true, nil
}

func (c *contendedGroupState) CommitGroup(_ context.Context, _ GroupCommitRequest) (GroupCommitResult, error) {
	return GroupCommitResult{Outcome: CommitOutcomeAccepted, Applied: true}, nil
}

// TestNotifyGroupLeaseAcquired_LocalPathContendedDoesNotFire is the negative
// control for the local (executeGroup) acquisition path: when the backend hands
// back acquired=false the unit belongs to somebody else, so this executor must
// not report an acquisition it never made. The remote path has its own negative
// control inside TestNotifyGroupLeaseAcquired_RemotePath (the second, failing
// BuildGroupLease); the two paths guard separate call sites and a mutation in
// one is invisible to the other's test.
func TestNotifyGroupLeaseAcquired_LocalPathContendedDoesNotFire(t *testing.T) {
	g := buildSingleGroupGraph(t)
	groupUnitIdx := findGroupUnit(t, g)

	obs := &recordingGroupObserver{}
	fake := &fakeGroupExecutor{exits: []GroupExit{{NodeName: "g.sink", Port: "main", Data: map[string]any{"ok": true}}}}
	state := &contendedGroupState{fakeState: newFakeState()}
	execID := types.ExecutionID("exec-observer-local-contended")
	state.fakeState.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID:     execID,
		Graph:  g,
		Status: types.ExecutionStatusRunning,
	})

	q := &fakeQueue{}
	eng := New(state, q, WithGroupExecutor(fake), WithGroupObserver(obs))
	eng.cacheExecutionGraph(execID, g)

	handled, err := eng.handleSystemTask(context.Background(), &Task{
		ExecutionID: execID,
		NodeName:    "g",
		UnitIdx:     groupUnitIdx,
		Type:        TaskTypeGroupExec,
	}, true)
	if err != nil {
		t.Fatalf("handleSystemTask: %v", err)
	}
	if !handled {
		t.Fatal("expected handled=true for TaskTypeGroupExec")
	}
	if obs.leaseAcquired != 0 {
		t.Fatalf("leaseAcquired = %d, want exactly 0 — executeGroup reported an "+
			"acquisition the backend refused", obs.leaseAcquired)
	}
	// The discard must happen before execution, not after: an observer count of
	// zero would also hold if the notify moved below a group that ran anyway.
	if fake.calls != 0 {
		t.Fatalf("ExecuteGroup calls = %d, want 0 — a lost lease race must discard "+
			"the task without executing the group", fake.calls)
	}
}

// findGroupUnit locates the single group unit's index in g. Shared by tests
// that drive the local executeGroup path through handleSystemTask.
func findGroupUnit(t *testing.T, g *graph.Graph) int {
	t.Helper()
	for i := 0; i < g.UnitCount(); i++ {
		if g.UnitKindAt(i) == graph.UnitGroup {
			return i
		}
	}
	t.Fatal("fixture regressed: no group unit found")
	return -1
}

// reclaimerGroupState is a GroupLeaseReclaimer test double with a
// controllable RevokeGroupLeaseWithOutbox return, embedding fakeState so
// ReclaimLease's subsequent FlushOutbox call succeeds against a real (if
// empty) outbox.
type reclaimerGroupState struct {
	*fakeState
	revoked bool
}

func (r *reclaimerGroupState) RevokeGroupLeaseWithOutbox(_ context.Context, _ types.ExecutionID, _ int, _ LeaseToken, _ OutboxEntry) (bool, error) {
	return r.revoked, nil
}

// expirerGroupState is a GroupLeaseExpirer test double (the non-atomic
// fallback path: expire, then a plain queue.Enqueue) with a controllable
// ExpireGroupLease return.
type expirerGroupState struct {
	*fakeState
	expired bool
}

func (e *expirerGroupState) ExpireGroupLease(_ context.Context, _ types.ExecutionID, _ int, _ LeaseToken) (bool, error) {
	return e.expired, nil
}

// TestNotifyGroupLeaseExpired_ReclaimerPath covers reclaimGroupLease's
// preferred (GroupLeaseReclaimer) branch: OnGroupLeaseExpired must fire only
// when the backend actually revoked the lease, never on the "already handled
// by someone else" false branch.
func TestNotifyGroupLeaseExpired_ReclaimerPath(t *testing.T) {
	for _, tc := range []struct {
		name    string
		revoked bool
		want    int
	}{
		{"revoked", true, 1},
		{"not revoked (already handled)", false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			obs := &recordingGroupObserver{}
			execID := types.ExecutionID("exec-reclaim-" + tc.name)
			state := &reclaimerGroupState{fakeState: newFakeState(), revoked: tc.revoked}
			state.fakeState.CreateExecution(context.Background(), &ExecutionSnapshot{
				ID:     execID,
				Status: types.ExecutionStatusRunning,
			})
			q := &fakeQueue{}
			eng := New(state, q, WithGroupObserver(obs))

			handled, err := eng.ReclaimLease(context.Background(), ExpiredLease{
				ExecutionID: execID,
				UnitIdx:     0,
				LeaseToken:  "tok",
				TaskType:    TaskTypeGroupExec,
			})
			if err != nil {
				t.Fatalf("ReclaimLease: %v", err)
			}
			if handled != tc.revoked {
				t.Fatalf("handled = %v, want %v", handled, tc.revoked)
			}
			if obs.leaseExpired != tc.want {
				t.Fatalf("leaseExpired = %d, want %d", obs.leaseExpired, tc.want)
			}
		})
	}
}

// TestNotifyGroupLeaseExpired_ExpirerPath covers reclaimGroupLease's fallback
// (GroupLeaseExpirer-only) branch, mirroring the reclaimer-path test.
func TestNotifyGroupLeaseExpired_ExpirerPath(t *testing.T) {
	for _, tc := range []struct {
		name    string
		expired bool
		want    int
	}{
		{"expired", true, 1},
		{"not expired (already handled)", false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			obs := &recordingGroupObserver{}
			execID := types.ExecutionID("exec-expire-" + tc.name)
			state := &expirerGroupState{fakeState: newFakeState(), expired: tc.expired}
			state.fakeState.CreateExecution(context.Background(), &ExecutionSnapshot{
				ID:     execID,
				Status: types.ExecutionStatusRunning,
			})
			q := &fakeQueue{}
			eng := New(state, q, WithGroupObserver(obs))

			handled, err := eng.ReclaimLease(context.Background(), ExpiredLease{
				ExecutionID: execID,
				UnitIdx:     0,
				LeaseToken:  "tok",
				TaskType:    TaskTypeGroupExec,
			})
			if err != nil {
				t.Fatalf("ReclaimLease: %v", err)
			}
			if handled != tc.expired {
				t.Fatalf("handled = %v, want %v", handled, tc.expired)
			}
			if obs.leaseExpired != tc.want {
				t.Fatalf("leaseExpired = %d, want %d", obs.leaseExpired, tc.want)
			}
		})
	}
}

// renewGroupState is a GroupStateStore test double with a controllable
// RenewGroupLease return (and error), used to drive all three
// OnGroupLeaseRenew result classifications.
type renewGroupState struct {
	*fakeState
	renewed bool
	renewErr error
}

func (r *renewGroupState) AcquireGroupLease(_ context.Context, _ *GroupLease) (bool, error) {
	return true, nil
}

func (r *renewGroupState) RenewGroupLease(_ context.Context, _ types.ExecutionID, _ int, _ LeaseToken, _ time.Time) (bool, error) {
	return r.renewed, r.renewErr
}

func (r *renewGroupState) CommitGroup(_ context.Context, _ GroupCommitRequest) (GroupCommitResult, error) {
	return GroupCommitResult{Outcome: CommitOutcomeAccepted, Applied: true}, nil
}

// TestRenewGroupLease_NotifiesClassifiedResult pins the literal result
// strings RenewGroupLease classifies its backend's (bool, error) return
// into: "ok", "not_renewed", "error" — and never "fenced", per the decision
// that the backend contract cannot distinguish a fenced token from the other
// two not-renewed causes.
func TestRenewGroupLease_NotifiesClassifiedResult(t *testing.T) {
	cases := []struct {
		name    string
		renewed bool
		err     error
		want    string
	}{
		{"ok", true, nil, "ok"},
		{"not_renewed", false, nil, "not_renewed"},
		{"error", false, fmt.Errorf("boom"), "error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obs := &recordingGroupObserver{}
			state := &renewGroupState{fakeState: newFakeState(), renewed: tc.renewed, renewErr: tc.err}
			q := &fakeQueue{}
			eng := New(state, q, WithGroupObserver(obs))

			lease := &TaskLease{
				Task:       Task{ExecutionID: "exec-renew", UnitIdx: 0},
				LeaseToken: "tok",
			}
			renewed, err := eng.RenewGroupLease(context.Background(), lease, 30*time.Second)
			if renewed != tc.renewed {
				t.Fatalf("renewed = %v, want %v", renewed, tc.renewed)
			}
			if (err != nil) != (tc.err != nil) {
				t.Fatalf("err = %v, want err!=nil = %v", err, tc.err != nil)
			}

			if len(obs.renewResults) != 1 {
				t.Fatalf("renewResults len = %d, want 1: %v", len(obs.renewResults), obs.renewResults)
			}
			if obs.renewResults[0] != tc.want {
				t.Fatalf("renewResults[0] = %q, want %q", obs.renewResults[0], tc.want)
			}
			if len(obs.renewDurations) != 1 {
				t.Fatalf("renewDurations len = %d, want 1", len(obs.renewDurations))
			}
			if obs.renewDurations[0] < 0 {
				t.Fatalf("renewDuration = %v, want >= 0", obs.renewDurations[0])
			}
		})
	}
}

// buildGroupGraphWithOnError is buildSingleGroupGraph (group_schedule_test.go)
// parameterized on the group's OnError strategy, needed to drive
// groupOnErrorFatal's fatal/non-fatal classification deterministically.
func buildGroupGraphWithOnError(t *testing.T, onError string) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "grouped-onerror",
		Nodes: []types.NodeDef{
			{Name: "g.source", Type: "test.action", Kind: types.NodeKindAction},
			{Name: "g.sink", Type: "test.action", Kind: types.NodeKindAction},
			{Name: "out", Type: "test.action", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"g.source": {"main": {Targets: []types.Connection{{Node: "g.sink", Input: "main"}}}},
			"g.sink":   {"main": {Targets: []types.Connection{{Node: "out", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "g", Members: []string{"g.source", "g.sink"}, OnError: onError}},
	})
	if err != nil {
		t.Fatalf("buildGroupGraphWithOnError: %v", err)
	}
	return g
}

// configurableGroupExecutor is a GroupExecutor test double whose reported
// error (and requested-fatal flag) is set per test case. commitGroup
// recomputes fatal from the group's OnError strategy whenever execErr != nil
// (see engine/group_exec.go's commitGroup), so the fatal field here only
// matters for the execErr == nil case.
type configurableGroupExecutor struct {
	exits []GroupExit
	err   error
}

func (f *configurableGroupExecutor) ExecuteGroup(_ context.Context, _ *Task, _ graph.GroupMeta) ([]GroupExit, bool, error) {
	return f.exits, false, f.err
}

// runGroupCommitScenario drives one local-path group execution to commit and
// returns the recording observer plus handleSystemTask's error.
func runGroupCommitScenario(t *testing.T, execID types.ExecutionID, onError string, execErr error) (*recordingGroupObserver, error) {
	t.Helper()
	g := buildGroupGraphWithOnError(t, onError)
	groupUnitIdx := findGroupUnit(t, g)

	obs := &recordingGroupObserver{}
	exec := &configurableGroupExecutor{
		exits: []GroupExit{{NodeName: "g.sink", Port: "main", Data: map[string]any{"ok": true}}},
		err:   execErr,
	}
	state := &fakeStateWithGroup{fakeState: newFakeState(), groupState: &fakeGroupState{}}
	state.fakeState.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID:     execID,
		Graph:  g,
		Status: types.ExecutionStatusRunning,
	})

	q := &fakeQueue{}
	eng := New(state, q, WithGroupExecutor(exec), WithGroupObserver(obs))
	eng.cacheExecutionGraph(execID, g)

	_, err := eng.handleSystemTask(context.Background(), &Task{
		ExecutionID: execID,
		NodeName:    "g",
		UnitIdx:     groupUnitIdx,
		Type:        TaskTypeGroupExec,
	}, true)
	return obs, err
}

// TestNotifyGroupCommit_Success pins the "success" outcome literal for a
// clean group execution.
func TestNotifyGroupCommit_Success(t *testing.T) {
	obs, err := runGroupCommitScenario(t, "exec-commit-success", "", nil)
	if err != nil {
		t.Fatalf("handleSystemTask: %v", err)
	}
	if len(obs.commitOutcomes) != 1 || obs.commitOutcomes[0] != "success" {
		t.Fatalf("commitOutcomes = %v, want [success]", obs.commitOutcomes)
	}
	if len(obs.commitDurations) != 1 {
		t.Fatalf("commitDurations len = %d, want 1", len(obs.commitDurations))
	}
	if obs.commitDurations[0] < 0 {
		t.Fatalf("commitDuration = %v, want >= 0", obs.commitDurations[0])
	}
}

// TestNotifyGroupCommit_FailedTolerated pins the "failed_tolerated" literal:
// a failing group whose OnError=continue does not fail the execution.
func TestNotifyGroupCommit_FailedTolerated(t *testing.T) {
	obs, err := runGroupCommitScenario(t, "exec-commit-tolerated", string(types.OnErrorContinue), fmt.Errorf("boom"))
	if err != nil {
		t.Fatalf("handleSystemTask: %v", err)
	}
	if len(obs.commitOutcomes) != 1 || obs.commitOutcomes[0] != "failed_tolerated" {
		t.Fatalf("commitOutcomes = %v, want [failed_tolerated]", obs.commitOutcomes)
	}
}

// TestNotifyGroupCommit_FailedFatal pins the "failed_fatal" literal: a
// failing group whose OnError=stop (the default) fails the execution.
func TestNotifyGroupCommit_FailedFatal(t *testing.T) {
	obs, err := runGroupCommitScenario(t, "exec-commit-fatal", string(types.OnErrorStop), fmt.Errorf("boom"))
	if err != nil {
		t.Fatalf("handleSystemTask: %v", err)
	}
	if len(obs.commitOutcomes) != 1 || obs.commitOutcomes[0] != "failed_fatal" {
		t.Fatalf("commitOutcomes = %v, want [failed_fatal]", obs.commitOutcomes)
	}
}

// TestCommitGroupResult_IssuedAtCarriedToCommitDuration is a dedicated
// regression test for a real production bug: CommitGroupResult (the ONLY
// path a remote runner's group result takes — see BuildGroupLease's
// provenance comment on the internal GroupLease literal in group_lease.go)
// used to build that literal without copying over lease.IssuedAt from the
// outer TaskLease, leaving it at the zero time.Time. commitGroup then
// computed time.Since(zero) — on the order of 56 years — and reported that
// as the group's exec duration on every single remote commit, while every
// local-executor test (executeGroup sets IssuedAt directly) stayed green.
//
// This test would have caught it: it drives the exact remote path
// (BuildGroupLease -> CommitGroupResult) and asserts the observed duration is
// small, not merely non-zero.
func TestCommitGroupResult_IssuedAtCarriedToCommitDuration(t *testing.T) {
	obs := &recordingGroupObserver{}
	eng, g, execID := newObserverGroupLeaseEngine(t, WithGroupObserver(obs))
	ctx := context.Background()

	gm := g.Groups()[0]
	task := &Task{
		ExecutionID: execID,
		NodeName:    gm.Name,
		NodeIdx:     gm.EntryIdx,
		UnitIdx:     gm.UnitIdx,
		Type:        TaskTypeGroupExec,
	}

	lease, _, err := eng.BuildGroupLease(ctx, task)
	if err != nil {
		t.Fatalf("BuildGroupLease: %v", err)
	}
	if lease.IssuedAt.IsZero() {
		t.Fatal("fixture regressed: BuildGroupLease did not set IssuedAt")
	}

	boundaryOutputs := gm.BoundaryOutputs
	if len(boundaryOutputs) == 0 {
		t.Fatalf("fixture regressed: group %q has no boundary outputs, so no exit can be accepted", gm.Name)
	}
	bo := boundaryOutputs[0]
	srcName := g.NodeName(bo.Src.NodeIdx)

	_, err = eng.CommitGroupResult(ctx, lease, GroupResult{
		Outcome:     GroupOutcomeSuccess,
		GroupExecID: "test-exec-issuedat",
		Attempt:     1,
		Exits: []GroupExitResult{{
			NodeName: srcName,
			Port:     bo.Src.Port,
			Data:     map[string]any{"result": "ok"},
		}},
	})
	if err != nil {
		t.Fatalf("CommitGroupResult: %v", err)
	}

	if len(obs.commitDurations) != 1 {
		t.Fatalf("commitDurations len = %d, want 1", len(obs.commitDurations))
	}
	d := obs.commitDurations[0]
	if d < 0 || d >= time.Minute {
		t.Fatalf("commit duration = %v, want < 1 minute — a zero-value IssuedAt "+
			"would report roughly 56 years here (the exact regression this test "+
			"pins)", d)
	}
}

// TestGroupObserver_NilObserverDoesNotPanic drives every notify* path with no
// GroupObserver configured (the field stays nil, matching all existing
// production callers that predate this wiring). Every notifyGroupX method
// must no-op silently rather than panic on a nil interface.
func TestGroupObserver_NilObserverDoesNotPanic(t *testing.T) {
	eng, g, execID := newObserverGroupLeaseEngine(t) // no WithGroupObserver
	ctx := context.Background()

	gm := g.Groups()[0]
	task := &Task{
		ExecutionID: execID,
		NodeName:    gm.Name,
		NodeIdx:     gm.EntryIdx,
		UnitIdx:     gm.UnitIdx,
		Type:        TaskTypeGroupExec,
	}

	lease, _, err := eng.BuildGroupLease(ctx, task)
	if err != nil {
		t.Fatalf("BuildGroupLease: %v", err)
	}
	if _, err := eng.RenewGroupLease(ctx, lease, 30*time.Second); err != nil {
		t.Fatalf("RenewGroupLease: %v", err)
	}

	boundaryOutputs := gm.BoundaryOutputs
	if len(boundaryOutputs) == 0 {
		t.Fatalf("fixture regressed: group %q has no boundary outputs", gm.Name)
	}
	bo := boundaryOutputs[0]
	srcName := g.NodeName(bo.Src.NodeIdx)

	if _, err := eng.CommitGroupResult(ctx, lease, GroupResult{
		Outcome: GroupOutcomeSuccess,
		Exits: []GroupExitResult{{
			NodeName: srcName,
			Port:     bo.Src.Port,
			Data:     map[string]any{"result": "ok"},
		}},
	}); err != nil {
		t.Fatalf("CommitGroupResult: %v", err)
	}

	// Also exercise the ReclaimLease path with a nil observer.
	reclaimState := &reclaimerGroupState{fakeState: newFakeState(), revoked: true}
	reclaimState.fakeState.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID:     "exec-nil-observer-reclaim",
		Status: types.ExecutionStatusRunning,
	})
	reclaimEng := New(reclaimState, &fakeQueue{}) // no WithGroupObserver
	if _, err := reclaimEng.ReclaimLease(ctx, ExpiredLease{
		ExecutionID: "exec-nil-observer-reclaim",
		UnitIdx:     0,
		LeaseToken:  "tok",
		TaskType:    TaskTypeGroupExec,
	}); err != nil {
		t.Fatalf("ReclaimLease: %v", err)
	}
}
