package control

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

type fakeLeaseLister struct {
	mu       sync.Mutex
	expired  []engine.ExpiredLease
	lastCall time.Time
	err      error
}

func (f *fakeLeaseLister) ListExpiredLeases(_ context.Context, before time.Time) ([]engine.ExpiredLease, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastCall = before
	if f.err != nil {
		return nil, f.err
	}
	out := append([]engine.ExpiredLease(nil), f.expired...)
	f.expired = nil
	return out, nil
}

type fakeReclaimer struct {
	mu        sync.Mutex
	reclaimed []engine.ExpiredLease
	results   map[string]bool
	err       error
}

func (r *fakeReclaimer) ReclaimLease(_ context.Context, lease engine.ExpiredLease) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reclaimed = append(r.reclaimed, lease)
	if r.err != nil {
		return false, r.err
	}
	if r.results != nil {
		if ok, found := r.results[lease.NodeName]; found {
			return ok, nil
		}
	}
	return true, nil
}

func (r *fakeReclaimer) BuildTaskLease(context.Context, *engine.Task) (*engine.TaskLease, error) {
	return nil, nil
}

func (r *fakeReclaimer) CommitTaskResult(context.Context, *engine.TaskLease, engine.TaskResult) error {
	return nil
}

func (r *fakeReclaimer) TaskRouting(context.Context, *engine.Task) (engine.TaskRouting, error) {
	return engine.TaskRouting{}, nil
}

type fakeElector struct {
	leader atomic.Bool
}

func (f *fakeElector) Campaign(context.Context) error { return nil }
func (f *fakeElector) IsLeader() bool                 { return f.leader.Load() }
func (f *fakeElector) Resign(context.Context) error   { return nil }
func (f *fakeElector) Notify() <-chan bool            { ch := make(chan bool, 1); return ch }

type fakeObserver struct {
	mu       sync.Mutex
	reclaims []string
	races    []string
	errs     []string
}

func (o *fakeObserver) OnSweepReclaim(execID, nodeName string, _ int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.reclaims = append(o.reclaims, execID+"/"+nodeName)
}

func (o *fakeObserver) OnSweepRace(execID, nodeName string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.races = append(o.races, execID+"/"+nodeName)
}

func (o *fakeObserver) OnSweepError(execID, nodeName string, _ error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.errs = append(o.errs, execID+"/"+nodeName)
}

func TestLeaseSweeperReclaimsExpiredLeases(t *testing.T) {
	state := &fakeLeaseLister{
		expired: []engine.ExpiredLease{
			{ExecutionID: types.ExecutionID("e1"), NodeName: "a", LeaseToken: "t1"},
			{ExecutionID: types.ExecutionID("e1"), NodeName: "b", LeaseToken: "t2"},
		},
	}
	reclaimer := &fakeReclaimer{}
	obs := &fakeObserver{}
	sw := NewLeaseSweeper(state, reclaimer, LeaseSweeperConfig{Observer: obs})

	got := sw.SweepOnce(context.Background())

	if got != 2 {
		t.Fatalf("SweepOnce reclaimed = %d, want 2", got)
	}
	if len(reclaimer.reclaimed) != 2 {
		t.Fatalf("ReclaimLease called %d times, want 2", len(reclaimer.reclaimed))
	}
	if len(obs.reclaims) != 2 {
		t.Fatalf("OnSweepReclaim fired %d times, want 2", len(obs.reclaims))
	}
}

func TestLeaseSweeperLosesRaceWithCommit(t *testing.T) {
	state := &fakeLeaseLister{
		expired: []engine.ExpiredLease{
			{ExecutionID: types.ExecutionID("e1"), NodeName: "a", LeaseToken: "t1"},
		},
	}
	reclaimer := &fakeReclaimer{results: map[string]bool{"a": false}}
	obs := &fakeObserver{}
	sw := NewLeaseSweeper(state, reclaimer, LeaseSweeperConfig{Observer: obs})

	got := sw.SweepOnce(context.Background())

	if got != 0 {
		t.Fatalf("SweepOnce reclaimed = %d, want 0", got)
	}
	if len(obs.races) != 1 {
		t.Fatalf("OnSweepRace fired %d times, want 1", len(obs.races))
	}
}

func TestLeaseSweeperReportsErrors(t *testing.T) {
	state := &fakeLeaseLister{
		expired: []engine.ExpiredLease{
			{ExecutionID: types.ExecutionID("e1"), NodeName: "a", LeaseToken: "t1"},
		},
	}
	reclaimer := &fakeReclaimer{err: errors.New("backend down")}
	obs := &fakeObserver{}
	sw := NewLeaseSweeper(state, reclaimer, LeaseSweeperConfig{Observer: obs})

	if sw.SweepOnce(context.Background()) != 0 {
		t.Fatal("expected zero reclaims on backend failure")
	}
	if len(obs.errs) != 1 {
		t.Fatalf("OnSweepError fired %d times, want 1", len(obs.errs))
	}
}

func TestLeaseSweeperHonorsGrace(t *testing.T) {
	state := &fakeLeaseLister{}
	reclaimer := &fakeReclaimer{}
	sw := NewLeaseSweeper(state, reclaimer, LeaseSweeperConfig{Grace: 5 * time.Second})

	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	sw.clock = func() time.Time { return now }
	sw.SweepOnce(context.Background())

	state.mu.Lock()
	defer state.mu.Unlock()
	want := now.Add(-5 * time.Second)
	if !state.lastCall.Equal(want) {
		t.Fatalf("ListExpiredLeases called with before=%v, want %v", state.lastCall, want)
	}
}

func TestLeaseSweeperRunStopsOnContextCancel(t *testing.T) {
	state := &fakeLeaseLister{}
	reclaimer := &fakeReclaimer{}
	sw := NewLeaseSweeper(state, reclaimer, LeaseSweeperConfig{Period: 5 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		sw.Run(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after context cancel")
	}
}

func TestSweepOnceSkipsWhenNotLeader(t *testing.T) {
	state := &fakeLeaseLister{
		expired: []engine.ExpiredLease{{
			ExecutionID: "exec-1",
			NodeName:    "n",
			IssuedAt:    time.Now().Add(-time.Minute),
			TTL:         time.Second,
			LeaseToken:  "token-1",
		}},
	}
	reclaimer := &fakeReclaimer{}
	elector := &fakeElector{} // leader=false by default
	sw := NewLeaseSweeper(state, reclaimer, LeaseSweeperConfig{Elector: elector})

	n := sw.SweepOnce(context.Background())
	if n != 0 {
		t.Fatalf("SweepOnce() = %d, want 0 when not leader", n)
	}
	if len(reclaimer.reclaimed) != 0 {
		t.Fatalf("ReclaimLease called %d times, want 0 when not leader", len(reclaimer.reclaimed))
	}
}

func TestSweepOnceRunsWhenLeader(t *testing.T) {
	state := &fakeLeaseLister{
		expired: []engine.ExpiredLease{{
			ExecutionID: "exec-1",
			NodeName:    "n",
			IssuedAt:    time.Now().Add(-time.Minute),
			TTL:         time.Second,
			LeaseToken:  "token-1",
		}},
	}
	reclaimer := &fakeReclaimer{}
	elector := &fakeElector{}
	elector.leader.Store(true)
	sw := NewLeaseSweeper(state, reclaimer, LeaseSweeperConfig{Elector: elector})

	n := sw.SweepOnce(context.Background())
	if n != 1 {
		t.Fatalf("SweepOnce() = %d, want 1 when leader", n)
	}
}

func TestSweepOnceRunsWhenElectorNil(t *testing.T) {
	state := &fakeLeaseLister{
		expired: []engine.ExpiredLease{{
			ExecutionID: "exec-1",
			NodeName:    "n",
			IssuedAt:    time.Now().Add(-time.Minute),
			TTL:         time.Second,
			LeaseToken:  "token-1",
		}},
	}
	reclaimer := &fakeReclaimer{}
	sw := NewLeaseSweeper(state, reclaimer, LeaseSweeperConfig{}) // Elector unset

	n := sw.SweepOnce(context.Background())
	if n != 1 {
		t.Fatalf("SweepOnce() = %d, want 1 when Elector is nil (backward-compat default)", n)
	}
}
