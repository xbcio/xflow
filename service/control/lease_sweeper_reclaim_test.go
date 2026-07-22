package control

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

// fakeReclaimEngine implements execution.Engine and lets tests control the
// (reclaimed, err) tuple returned by ReclaimLease.
type fakeReclaimEngine struct {
	mu              sync.Mutex
	reclaimCalls    int
	reclaimAfterDir bool
	results         map[string]struct {
		reclaimed bool
		err       error
	}
}

func (e *fakeReclaimEngine) ReclaimLease(_ context.Context, lease engine.ExpiredLease) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.reclaimCalls++
	key := string(lease.ExecutionID) + "/" + lease.NodeName
	if r, ok := e.results[key]; ok {
		return r.reclaimed, r.err
	}
	return true, nil
}

func (e *fakeReclaimEngine) BuildTaskLease(context.Context, *engine.Task) (*engine.TaskLease, error) {
	return nil, nil
}

func (e *fakeReclaimEngine) CommitTaskResult(context.Context, *engine.TaskLease, engine.TaskResult) error {
	return nil
}

func (e *fakeReclaimEngine) TaskRouting(context.Context, *engine.Task) (engine.TaskRouting, error) {
	return engine.TaskRouting{}, nil
}

// spyExpiredLeaseReleaser wraps a real directory and records whether directory
// cleanup happens before the engine reclaim.
type spyExpiredLeaseReleaser struct {
	inner    ExpiredLeaseReleaser
	eng      *fakeReclaimEngine
	mu       sync.Mutex
	calls    int
	before   bool
	lastOut  ExpiredDirectoryLeaseOutcome
	lastErr  error
	forceOut ExpiredDirectoryLeaseOutcome
	forceErr error
}

func (s *spyExpiredLeaseReleaser) ReleaseExpiredLease(ctx context.Context, req ExpiredDirectoryLeaseRequest) (ExpiredDirectoryLeaseOutcome, error) {
	s.mu.Lock()
	s.calls++
	if s.eng != nil {
		s.eng.mu.Lock()
		before := s.eng.reclaimCalls == 0
		s.eng.mu.Unlock()
		if before {
			s.before = true
		}
	}
	if s.forceErr != nil || s.forceOut != "" {
		s.lastOut, s.lastErr = s.forceOut, s.forceErr
		s.mu.Unlock()
		return s.forceOut, s.forceErr
	}
	s.mu.Unlock()
	out, err := s.inner.ReleaseExpiredLease(ctx, req)
	s.mu.Lock()
	s.lastOut, s.lastErr = out, err
	s.mu.Unlock()
	return out, err
}

type recordingSweepObserver struct {
	mu           sync.Mutex
	reclaims     int
	applied      int
	races        int
	errs         int
	resultLabels []string
}

func (o *recordingSweepObserver) OnSweepReclaim(context.Context, string, string, int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.reclaims++
}

func (o *recordingSweepObserver) OnSweepReclaimApplied(context.Context, string, string, int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.applied++
}

func (o *recordingSweepObserver) OnSweepRace(context.Context, string, string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.races++
}

func (o *recordingSweepObserver) OnSweepError(context.Context, string, string, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.errs++
}

func (o *recordingSweepObserver) OnSweepListExpired(context.Context, int, time.Duration, error) {}

func (o *recordingSweepObserver) OnSweepReclaimResult(_ context.Context, result string, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.resultLabels = append(o.resultLabels, result)
}

func (o *recordingSweepObserver) OnSweepRepair(context.Context, int, time.Duration, error) {}

func newSweeperWithFakeEngineAndDir(t *testing.T, dir ExpiredLeaseReleaser) (*LeaseSweeper, *fakeReclaimEngine, *spyExpiredLeaseReleaser) {
	t.Helper()
	eng := &fakeReclaimEngine{results: make(map[string]struct {
		reclaimed bool
		err       error
	})}
	spy := &spyExpiredLeaseReleaser{inner: dir, eng: eng}
	return NewLeaseSweeper(&fakeLeaseLister{}, eng, LeaseSweeperConfig{RunnerDirectory: spy}), eng, spy
}

func seedExpiredLease(t *testing.T, dir *MemoryRunnerDirectory, leaseToken engine.LeaseToken) engine.ExpiredLease {
	t.Helper()
	return seedExpiredLeaseWithDirToken(t, dir, leaseToken, leaseToken)
}

func seedExpiredLeaseWithDirToken(t *testing.T, dir *MemoryRunnerDirectory, leaseToken, dirToken engine.LeaseToken) engine.ExpiredLease {
	t.Helper()
	ctx := context.Background()
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-sweep",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"xflow.function"}},
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	task := engine.Task{
		ExecutionID:  "exec-sweep",
		NodeName:     "node-a",
		NodeIdx:      0,
		Type:         engine.TaskTypeNodeExec,
		ActivationID: 1,
	}
	assignment := Assignment{
		AssignmentID: BuildAssignmentID(&task),
		Task:         task,
		Routing:      engine.TaskRouting{NodeType: "xflow.function"},
	}
	if _, err := dir.EnqueueAssignment(ctx, assignment); err != nil {
		t.Fatalf("EnqueueAssignment() error = %v", err)
	}
	claim, ok, err := dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:     session.RunnerID,
		SessionID:    session.SessionID,
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	})
	if err != nil || !ok {
		t.Fatalf("ClaimForRunner() ok=%v err=%v", ok, err)
	}
	lease := engine.TaskLease{
		LeaseID:    "lease-sweep",
		LeaseToken: dirToken,
		Task:       task,
	}
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, &lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	return engine.ExpiredLease{
		ExecutionID:  task.ExecutionID,
		NodeName:     task.NodeName,
		NodeIdx:      task.NodeIdx,
		LeaseID:      lease.LeaseID,
		LeaseToken:   leaseToken,
		IssuedAt:     time.Now().UTC().Add(-time.Minute),
		TTL:          time.Second,
		ActivationID: task.ActivationID,
		TaskType:     task.Type,
		Payload:      task.Payload,
	}
}

func TestSweepOnceDirectoryCleanupBeforeReclaim(t *testing.T) {
	dir := NewMemoryRunnerDirectory()
	lease := seedExpiredLease(t, dir, "token-ok")
	state := &fakeLeaseLister{expired: []engine.ExpiredLease{lease}}
	sw, eng, spy := newSweeperWithFakeEngineAndDir(t, dir)
	sw.state = state

	if got := sw.SweepOnce(context.Background()); got != 1 {
		t.Fatalf("SweepOnce() = %d, want 1", got)
	}
	if eng.reclaimCalls != 1 {
		t.Fatalf("reclaim calls = %d, want 1", eng.reclaimCalls)
	}
	if spy.calls != 1 {
		t.Fatalf("directory release calls = %d, want 1", spy.calls)
	}
	if !spy.before {
		t.Fatal("directory cleanup must happen before engine reclaim")
	}
}

func TestSweepOnceDirectoryTokenMismatchFailsClosed(t *testing.T) {
	dir := NewMemoryRunnerDirectory()
	lease := seedExpiredLeaseWithDirToken(t, dir, "stale-token", "current-token")
	state := &fakeLeaseLister{expired: []engine.ExpiredLease{lease}}
	sw, eng, spy := newSweeperWithFakeEngineAndDir(t, dir)
	sw.state = state

	if got := sw.SweepOnce(context.Background()); got != 0 {
		t.Fatalf("SweepOnce() = %d, want 0 on token mismatch", got)
	}
	if eng.reclaimCalls != 0 {
		t.Fatalf("token mismatch must not reclaim; got %d", eng.reclaimCalls)
	}
	if spy.lastOut != ExpiredDirectoryLeaseTokenMismatch {
		t.Fatalf("directory outcome = %v, want token_mismatch", spy.lastOut)
	}
}

func TestSweepOnceDirectoryErrorFailsClosed(t *testing.T) {
	dir := NewMemoryRunnerDirectory()
	lease := seedExpiredLease(t, dir, "token-ok")
	state := &fakeLeaseLister{expired: []engine.ExpiredLease{lease}}
	sw, eng, spy := newSweeperWithFakeEngineAndDir(t, dir)
	sw.state = state
	spy.forceOut = ""
	spy.forceErr = errors.New("directory unavailable")

	if got := sw.SweepOnce(context.Background()); got != 0 {
		t.Fatalf("SweepOnce() = %d, want 0 on directory error", got)
	}
	if eng.reclaimCalls != 0 {
		t.Fatalf("directory error must not reclaim; got %d", eng.reclaimCalls)
	}
}

func TestSweepOnceNoDirectoryStillReclaims(t *testing.T) {
	lease := engine.ExpiredLease{
		ExecutionID:  "exec-1",
		NodeName:     "node-a",
		LeaseID:      "lease-1",
		LeaseToken:   "token-1",
		IssuedAt:     time.Now().UTC().Add(-time.Minute),
		TTL:          time.Second,
		ActivationID: 1,
	}
	state := &fakeLeaseLister{expired: []engine.ExpiredLease{lease}}
	eng := &fakeReclaimEngine{results: make(map[string]struct {
		reclaimed bool
		err       error
	})}
	sw := NewLeaseSweeper(state, eng, LeaseSweeperConfig{})

	if got := sw.SweepOnce(context.Background()); got != 1 {
		t.Fatalf("SweepOnce() = %d, want 1 without directory", got)
	}
	if eng.reclaimCalls != 1 {
		t.Fatalf("ReclaimLease calls = %d, want 1", eng.reclaimCalls)
	}
}

func TestSweepOnceHandlesReclaimTuples(t *testing.T) {
	lease := engine.ExpiredLease{
		ExecutionID:  "exec-1",
		NodeName:     "node-a",
		LeaseID:      "lease-1",
		LeaseToken:   "token-1",
		IssuedAt:     time.Now().UTC().Add(-time.Minute),
		TTL:          time.Second,
		ActivationID: 1,
	}

	cases := []struct {
		name         string
		reclaimed    bool
		err          error
		wantCount    int
		wantReclaims int
		wantApplied  int
		wantRaces    int
		wantErrs     int
		wantResult   string
	}{
		{
			name:       "race",
			reclaimed:  false,
			err:        nil,
			wantCount:  0,
			wantRaces:  1,
			wantResult: "race",
		},
		{
			name:       "state_error",
			reclaimed:  false,
			err:        errors.New("state down"),
			wantCount:  0,
			wantErrs:   1,
			wantResult: "error",
		},
		{
			name:         "ok",
			reclaimed:    true,
			err:          nil,
			wantCount:    1,
			wantReclaims: 1,
			wantResult:   "reclaimed",
		},
		{
			name:        "flush_error",
			reclaimed:   true,
			err:         errors.New("flush failed"),
			wantCount:   1,
			wantApplied: 1,
			wantResult:  "applied_pending",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := &fakeLeaseLister{expired: []engine.ExpiredLease{lease}}
			key := string(lease.ExecutionID) + "/" + lease.NodeName
			eng := &fakeReclaimEngine{results: make(map[string]struct {
				reclaimed bool
				err       error
			})}
			eng.results[key] = struct {
				reclaimed bool
				err       error
			}{reclaimed: tc.reclaimed, err: tc.err}
			obs := &recordingSweepObserver{}
			sw := NewLeaseSweeper(state, eng, LeaseSweeperConfig{Observer: obs})

			if got := sw.SweepOnce(context.Background()); got != tc.wantCount {
				t.Fatalf("SweepOnce() = %d, want %d", got, tc.wantCount)
			}
			if obs.reclaims != tc.wantReclaims {
				t.Fatalf("OnSweepReclaim calls = %d, want %d", obs.reclaims, tc.wantReclaims)
			}
			if obs.applied != tc.wantApplied {
				t.Fatalf("OnSweepReclaimApplied calls = %d, want %d", obs.applied, tc.wantApplied)
			}
			if obs.races != tc.wantRaces {
				t.Fatalf("OnSweepRace calls = %d, want %d", obs.races, tc.wantRaces)
			}
			if obs.errs != tc.wantErrs {
				t.Fatalf("OnSweepError calls = %d, want %d", obs.errs, tc.wantErrs)
			}
			if len(obs.resultLabels) != 1 || obs.resultLabels[0] != tc.wantResult {
				t.Fatalf("reclaim result labels = %v, want [%s]", obs.resultLabels, tc.wantResult)
			}
		})
	}
}
