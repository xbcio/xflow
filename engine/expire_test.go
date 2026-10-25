package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// expireLog is the ordered record of the store and hook calls an ExpireExecution
// makes. One shared log is what lets a test assert ORDER across the status
// write, the cascade, the completion hook and the acknowledgement.
//
// The log follows one execution (track): a sub-workflow cascade cancels the
// child through the same store and hooks, and the child's own writes must not
// interleave with the parent's order.
type expireLog struct {
	track  types.ExecutionID
	mu     sync.Mutex
	events []string
}

func (l *expireLog) add(id types.ExecutionID, event string) {
	if id != l.track {
		return
	}
	l.mu.Lock()
	l.events = append(l.events, event)
	l.mu.Unlock()
}

func (l *expireLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

func (l *expireLog) clear() {
	l.mu.Lock()
	l.events = nil
	l.mu.Unlock()
}

// index returns the position of the first occurrence of event, or -1.
func (l *expireLog) index(event string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, got := range l.events {
		if got == event {
			return i
		}
	}
	return -1
}

func (l *expireLog) count(event string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, got := range l.events {
		if got == event {
			n++
		}
	}
	return n
}

// expireState is the store ExpireExecution runs against in these tests. It is
// the sub-workflow link fake plus the three behaviours the real backends have
// and fakeState does not: the status compare-and-set (a terminal status is
// never overwritten, canceling accepts only canceled), the execution-level
// reason recorded for any status, and a fenced CancelSuspendedNode. Every call
// ExpireExecution makes through it lands in log, in order.
type expireState struct {
	*subWorkflowLinkState
	log *expireLog

	faultMu            sync.Mutex
	listSuspendedErr   error
	beforeTimeoutWrite func()
}

var _ ExecutionTraceCarrierReader = (*expireState)(nil)

// GetExecution returns a copy under the store lock. fakeState hands out its
// stored pointer, which the concurrent-expire test would race on.
func (s *expireState) GetExecution(_ context.Context, id types.ExecutionID) (*ExecutionSnapshot, error) {
	f := s.fakeState
	f.mu.Lock()
	defer f.mu.Unlock()
	snap := f.executions[id]
	if snap == nil {
		return nil, nil
	}
	cp := *snap
	return &cp, nil
}

func (s *expireState) GetExecutionTraceCarrier(ctx context.Context, id types.ExecutionID) (map[string]string, bool, error) {
	snap, err := s.GetExecution(ctx, id)
	if err != nil || snap == nil || len(snap.TraceCarrier) == 0 {
		return nil, false, err
	}
	return snap.TraceCarrier, true, nil
}

// UpdateExecutionStatus mirrors the real terminal write. A one-shot
// beforeTimeoutWrite runs just before the first timeout write, which is how a
// test lands a cancel or a normal conclusion between ExpireExecution's
// pre-read and its write.
func (s *expireState) UpdateExecutionStatus(_ context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) error {
	s.log.add(id, "write:"+string(status))
	if status == types.ExecutionStatusTimeout {
		s.faultMu.Lock()
		race := s.beforeTimeoutWrite
		s.beforeTimeoutWrite = nil
		s.faultMu.Unlock()
		if race != nil {
			race()
		}
	}
	f := s.fakeState
	f.mu.Lock()
	defer f.mu.Unlock()
	exec, ok := f.executions[id]
	if !ok || types.IsTerminalExecutionStatus(exec.Status) {
		return nil
	}
	if exec.Status == types.ExecutionStatusCanceling && status != types.ExecutionStatusCanceled {
		return nil
	}
	exec.Status = status
	if errMsg != "" {
		exec.Error = errMsg
	}
	f.releaseConcurrencySlotLocked(id, status)
	f.publishLocked(ExecutionEvent{ExecutionID: id, Status: status, Error: errMsg})
	return nil
}

// ListSuspendedNodes is the first call of the cascade; the one-shot fault is how
// a test makes the cascade fail after the status write landed.
func (s *expireState) ListSuspendedNodes(ctx context.Context, id types.ExecutionID) ([]string, error) {
	s.log.add(id, "cascade:list-suspended")
	s.faultMu.Lock()
	err := s.listSuspendedErr
	s.listSuspendedErr = nil
	s.faultMu.Unlock()
	if err != nil {
		return nil, err
	}
	return s.fakeState.ListSuspendedNodes(ctx, id)
}

// CancelSuspendedNode mirrors the fenced compare-and-set both real backends
// implement (see cancelResumeState for why the fallback path is not enough).
func (s *expireState) CancelSuspendedNode(_ context.Context, id types.ExecutionID, name string) (bool, error) {
	f := s.fakeState
	f.mu.Lock()
	defer f.mu.Unlock()
	key := string(id) + "/" + name
	ns := f.nodes[key]
	if ns == nil || ns.Status != types.NodeStatusSuspended {
		return false, nil
	}
	cp := *ns
	cp.Status = types.NodeStatusCanceled
	f.nodes[key] = &cp
	delete(f.suspended, key)
	return true, nil
}

func (s *expireState) failListSuspended(err error) {
	s.faultMu.Lock()
	s.listSuspendedErr = err
	s.faultMu.Unlock()
}

func (s *expireState) onceBeforeTimeoutWrite(fn func()) {
	s.faultMu.Lock()
	s.beforeTimeoutWrite = fn
	s.faultMu.Unlock()
}

// expireAckState adds the optional ExecutionDeadlineExpirer capability on top of
// expireState and records every acknowledgement in the shared log.
type expireAckState struct {
	*expireState
}

var _ ExecutionDeadlineExpirer = (*expireAckState)(nil)

func (s *expireAckState) ListDeadlineExceededExecutions(context.Context, time.Time, int) ([]DeadlineExpiredExecution, error) {
	return nil, nil
}

func (s *expireAckState) AckDeadlineExpired(_ context.Context, id types.ExecutionID) error {
	s.log.add(id, "ack")
	return nil
}

// expireHooks records the lifecycle hooks an expiry may or may not fire.
type expireHooks struct {
	BaseHooks
	log *expireLog

	mu       sync.Mutex
	complete []types.ExecutionStatus
}

func (h *expireHooks) OnExecutionComplete(_ context.Context, id types.ExecutionID, status types.ExecutionStatus) {
	h.log.add(id, "hook:complete")
	if id != h.log.track {
		return
	}
	h.mu.Lock()
	h.complete = append(h.complete, status)
	h.mu.Unlock()
}

func (h *expireHooks) OnNodeTimeout(_ context.Context, id types.ExecutionID, _ string) {
	h.log.add(id, "hook:node-timeout")
}

func (h *expireHooks) completions() []types.ExecutionStatus {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]types.ExecutionStatus(nil), h.complete...)
}

// expireRig is one execution with a suspended node, driven through an engine
// whose store and hooks feed one expireLog.
type expireRig struct {
	eng   *Engine
	state *expireState
	queue *fakeQueue
	log   *expireLog
	hooks *expireHooks
	g     *graph.Graph
	id    types.ExecutionID
}

// newExpireRig assembles the engine. withAck wraps the store in the optional
// ExecutionDeadlineExpirer capability; without it the engine sees a backend
// that keeps no deadline index.
func newExpireRig(t *testing.T, withAck bool) *expireRig {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    "expire",
		Version: "1",
		Nodes:   []types.NodeDef{{Name: "wait", Type: "test.suspend"}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	log := &expireLog{}
	st := &expireState{subWorkflowLinkState: newSubWorkflowLinkState(), log: log}
	hooks := &expireHooks{log: log}
	var store StateStore = st
	if withAck {
		store = &expireAckState{expireState: st}
	}
	queue := &fakeQueue{}
	eng := New(store, queue, WithHooks(hooks))
	testRegistries.Store(eng, &fakeRegistry{handlers: map[string]types.ActionHandler{
		"test.suspend": &fakeSuspendHandler{},
	}})
	t.Cleanup(func() { testRegistries.Delete(eng) })
	return &expireRig{eng: eng, state: st, queue: queue, log: log, hooks: hooks, g: g}
}

// start submits the graph, runs its node into suspension and clears the log, so
// a test asserts only what ExpireExecution itself did.
func (r *expireRig) start(t *testing.T) {
	t.Helper()
	id, err := r.eng.Submit(context.Background(), r.g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	r.adopt(t, id)
}

// adopt takes over an execution Submit already created (the span test submits
// under a parent span) and suspends its node.
func (r *expireRig) adopt(t *testing.T, id types.ExecutionID) {
	t.Helper()
	r.id = id
	r.log.track = id
	r.state.InitInDegrees(id, r.g)
	tasks := r.queue.Drain()
	if len(tasks) != 1 {
		t.Fatalf("submission enqueued %d tasks, want 1", len(tasks))
	}
	executeTask(t, r.eng, tasks[0])
	suspended, err := r.state.ListSuspendedNodes(context.Background(), id)
	if err != nil || len(suspended) != 1 || suspended[0] != "wait" {
		t.Fatalf("suspended nodes = %v (err %v), want [wait]: the fixture must park real work", suspended, err)
	}
	r.log.clear()
}

// setDeadline writes the execution deadline straight into the stored snapshot,
// so a test does not depend on the wall clock or on a graph timeout.
func (r *expireRig) setDeadline(d time.Time) {
	r.state.fakeState.mu.Lock()
	r.state.fakeState.executions[r.id].Deadline = d
	r.state.fakeState.mu.Unlock()
}

// force overwrites the stored status and reason, the way another actor's
// terminal write would have left them.
func (r *expireRig) force(status types.ExecutionStatus, reason string) {
	r.state.fakeState.mu.Lock()
	r.state.fakeState.executions[r.id].Status = status
	r.state.fakeState.executions[r.id].Error = reason
	r.state.fakeState.mu.Unlock()
}

func (r *expireRig) snapshot(t *testing.T) *ExecutionSnapshot {
	t.Helper()
	snap, err := r.state.GetExecution(context.Background(), r.id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution() = (%v, %v)", snap, err)
	}
	return snap
}

func (r *expireRig) nodeStatus(t *testing.T) types.NodeStatus {
	t.Helper()
	node, err := r.state.GetNode(context.Background(), r.id, "wait")
	if err != nil || node == nil {
		t.Fatalf("GetNode() = (%v, %v)", node, err)
	}
	return node.Status
}

// expire runs ExpireExecution and requires the outcome and a nil error.
func (r *expireRig) expire(t *testing.T, want ExpireOutcome) {
	t.Helper()
	got, err := r.eng.ExpireExecution(context.Background(), r.id)
	if err != nil {
		t.Fatalf("ExpireExecution() error = %v", err)
	}
	if got != want {
		t.Fatalf("ExpireExecution() = %q, want %q (log %v)", got, want, r.log.snapshot())
	}
}

var pastDeadline = time.Now().Add(-time.Hour)

func TestDeadlineReached(t *testing.T) {
	d := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name     string
		now      time.Time
		deadline time.Time
		want     bool
	}{
		{"zero deadline is no budget", d, time.Time{}, false},
		{"zero deadline with a zero clock", time.Time{}, time.Time{}, false},
		{"before the deadline", d.Add(-time.Nanosecond), d, false},
		{"exactly at the deadline counts as reached", d, d, true},
		{"after the deadline", d.Add(time.Nanosecond), d, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := deadlineReached(tc.now, tc.deadline); got != tc.want {
				t.Fatalf("deadlineReached(%s, %s) = %v, want %v", tc.now, tc.deadline, got, tc.want)
			}
		})
	}
}

func TestExpireExecutionGone(t *testing.T) {
	rig := newExpireRig(t, true)
	got, err := rig.eng.ExpireExecution(context.Background(), "exec-missing")
	if err != nil || got != ExpireOutcomeGone {
		t.Fatalf("ExpireExecution(missing) = (%q, %v), want (%q, nil)", got, err, ExpireOutcomeGone)
	}
}

// TestExpireExecutionConcludedExecutionsAreAcknowledged pins that an execution
// another actor already concluded is reported, left untouched, and has its
// deadline entry withdrawn: nothing is left to expire, and the entry would
// otherwise be listed forever.
func TestExpireExecutionConcludedExecutionsAreAcknowledged(t *testing.T) {
	for _, status := range []types.ExecutionStatus{
		types.ExecutionStatusSuccess, types.ExecutionStatusFailed, types.ExecutionStatusCanceled,
	} {
		t.Run(string(status), func(t *testing.T) {
			rig := newExpireRig(t, true)
			rig.start(t)
			rig.setDeadline(pastDeadline)
			rig.force(status, "")

			rig.expire(t, ExpireOutcomeAlreadyTerminal)

			if got := rig.snapshot(t).Status; got != status {
				t.Fatalf("status = %q, want %q kept", got, status)
			}
			if got := rig.log.snapshot(); len(got) != 1 || got[0] != "ack" {
				t.Fatalf("calls = %v, want exactly [ack]", got)
			}
			if got := rig.nodeStatus(t); got != types.NodeStatusSuspended {
				t.Fatalf("node status = %q, want suspended: nothing may cascade", got)
			}
		})
	}
}

// TestExpireExecutionCancelingIsLeftToCancel pins the precedence rule: a cancel
// that owns the execution wins, so nothing is written and the entry stays for
// the cancel's own terminal write to withdraw.
func TestExpireExecutionCancelingIsLeftToCancel(t *testing.T) {
	rig := newExpireRig(t, true)
	rig.start(t)
	rig.setDeadline(pastDeadline)
	rig.force(types.ExecutionStatusCanceling, "")

	rig.expire(t, ExpireOutcomeCanceling)

	if got := rig.snapshot(t).Status; got != types.ExecutionStatusCanceling {
		t.Fatalf("status = %q, want canceling kept", got)
	}
	if got := rig.log.snapshot(); len(got) != 0 {
		t.Fatalf("calls = %v, want none: no write, no cascade, no ack", got)
	}
}

// TestExpireExecutionNotDue pins that only a reached deadline expires an
// execution: no deadline at all (no settings.timeout) and a deadline still in
// the future both leave everything alone.
func TestExpireExecutionNotDue(t *testing.T) {
	cases := []struct {
		name     string
		deadline time.Time
	}{
		{"no deadline", time.Time{}},
		{"deadline in the future", time.Now().Add(time.Hour)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newExpireRig(t, true)
			rig.start(t)
			rig.setDeadline(tc.deadline)

			rig.expire(t, ExpireOutcomeNotDue)

			if got := rig.snapshot(t).Status; got == types.ExecutionStatusTimeout {
				t.Fatalf("status = %q, want it untouched", got)
			}
			if got := rig.log.snapshot(); len(got) != 0 {
				t.Fatalf("calls = %v, want none", got)
			}
			if got := rig.nodeStatus(t); got != types.NodeStatusSuspended {
				t.Fatalf("node status = %q, want suspended", got)
			}
			if got := rig.hooks.completions(); len(got) != 0 {
				t.Fatalf("completion hooks = %v, want none", got)
			}
		})
	}
}

// TestExpireExecutionOtherTimeoutIsNotThisMechanisms pins the reason check: an
// execution that timed out in the concurrency queue is concluded by another
// mechanism, so it is neither cascaded nor announced a second time.
func TestExpireExecutionOtherTimeoutIsNotThisMechanisms(t *testing.T) {
	rig := newExpireRig(t, true)
	rig.start(t)
	rig.setDeadline(pastDeadline)
	rig.force(types.ExecutionStatusTimeout, ConcurrencyQueueTimeoutError)

	rig.expire(t, ExpireOutcomeAlreadyTerminal)

	if got := rig.snapshot(t); got.Status != types.ExecutionStatusTimeout || got.Error != ConcurrencyQueueTimeoutError {
		t.Fatalf("snapshot = (%q, %q), want the queue timeout kept", got.Status, got.Error)
	}
	if got := rig.log.snapshot(); len(got) != 1 || got[0] != "ack" {
		t.Fatalf("calls = %v, want exactly [ack]", got)
	}
}

// TestExpireExecutionWithoutDeadlineIndexSkipsAck pins that the acknowledgement
// is a capability, not a requirement: a backend without ExecutionDeadlineExpirer
// still expires an execution.
func TestExpireExecutionWithoutDeadlineIndexSkipsAck(t *testing.T) {
	rig := newExpireRig(t, false)
	rig.start(t)
	rig.setDeadline(pastDeadline)

	rig.expire(t, ExpireOutcomeExpired)

	if rig.log.count("ack") != 0 {
		t.Fatalf("calls = %v, want no ack without the capability", rig.log.snapshot())
	}
}

// TestExpireExecutionExpiresTheWholeExecution is the full flow on one
// execution: the timeout status lands with the deadline reason, the suspended
// node is stopped, the completion hook fires once with timeout, the per-node
// timeout hook never fires, the graph cache entry is dropped, and the deadline
// entry is withdrawn last.
func TestExpireExecutionExpiresTheWholeExecution(t *testing.T) {
	rig := newExpireRig(t, true)
	rig.start(t)
	rig.setDeadline(pastDeadline)
	if _, cached := rig.eng.graphs.Get(rig.id); !cached {
		t.Fatal("precondition: the execution's graph should be cached before the expiry")
	}

	rig.expire(t, ExpireOutcomeExpired)

	snap := rig.snapshot(t)
	if snap.Status != types.ExecutionStatusTimeout || snap.Error != ExecutionDeadlineExceededError {
		t.Fatalf("snapshot = (%q, %q), want (timeout, %q)", snap.Status, snap.Error, ExecutionDeadlineExceededError)
	}
	if got := rig.nodeStatus(t); got != types.NodeStatusCanceled {
		t.Fatalf("node status = %q, want canceled", got)
	}
	if got := rig.hooks.completions(); len(got) != 1 || got[0] != types.ExecutionStatusTimeout {
		t.Fatalf("completion hooks = %v, want exactly [timeout]", got)
	}
	if n := rig.log.count("hook:node-timeout"); n != 0 {
		t.Fatalf("OnNodeTimeout fired %d times, want 0: it belongs to the suspended-node park timer", n)
	}
	if _, cached := rig.eng.graphs.Get(rig.id); cached {
		t.Fatal("the graph cache entry survived the expiry, want it evicted")
	}
	write, hook := rig.log.index("write:timeout"), rig.log.index("hook:complete")
	cascade, ack := rig.log.index("cascade:list-suspended"), rig.log.index("ack")
	if write < 0 || hook < 0 || cascade < 0 || ack < 0 {
		t.Fatalf("missing calls in %v", rig.log.snapshot())
	}
	if !(write < cascade && cascade < ack) {
		t.Fatalf("order = %v, want the timeout write, then the cascade, then the ack", rig.log.snapshot())
	}
	if !(write < hook) {
		t.Fatalf("order = %v, want the completion hook after the durable timeout write", rig.log.snapshot())
	}
	if n := rig.log.count("write:timeout"); n != 1 {
		t.Fatalf("timeout was written %d times, want once", n)
	}
}

// TestExpireExecutionCascadeFailureIsResumable pins the recovery contract: a
// cascade that fails after the status write leaves the execution in timeout,
// keeps the deadline entry listed (no ack), and does not lose or repeat the
// completion hook. The next call is Resumed, finishes the cascade, announces
// nothing again and withdraws the entry.
func TestExpireExecutionCascadeFailureIsResumable(t *testing.T) {
	ctx := context.Background()
	rig := newExpireRig(t, true)
	rig.start(t)
	rig.setDeadline(pastDeadline)
	injected := errors.New("injected: list suspended nodes failed")
	rig.state.failListSuspended(injected)

	got, err := rig.eng.ExpireExecution(ctx, rig.id)
	if !errors.Is(err, injected) {
		t.Fatalf("ExpireExecution() = (%q, %v), want an error wrapping %v", got, err, injected)
	}
	if snap := rig.snapshot(t); snap.Status != types.ExecutionStatusTimeout || snap.Error != ExecutionDeadlineExceededError {
		t.Fatalf("snapshot = (%q, %q), want the timeout status kept", snap.Status, snap.Error)
	}
	if n := rig.log.count("ack"); n != 0 {
		t.Fatalf("acked %d times after a failed cascade, want 0: the entry must stay listed", n)
	}
	if got := rig.nodeStatus(t); got != types.NodeStatusSuspended {
		t.Fatalf("node status = %q, want suspended: the failed cascade did not reach it", got)
	}
	if got := rig.hooks.completions(); len(got) != 1 {
		t.Fatalf("completion hooks after the failed call = %v, want exactly one", got)
	}

	rig.expire(t, ExpireOutcomeResumed)

	if got := rig.nodeStatus(t); got != types.NodeStatusCanceled {
		t.Fatalf("node status = %q, want canceled after the resumed cascade", got)
	}
	if got := rig.hooks.completions(); len(got) != 1 {
		t.Fatalf("completion hooks after the resume = %v, want still exactly one", got)
	}
	if n := rig.log.count("ack"); n != 1 {
		t.Fatalf("acked %d times after the resume, want 1", n)
	}
	if n := rig.log.count("write:timeout"); n != 1 {
		t.Fatalf("timeout was written %d times, want once: the resume must not rewrite it", n)
	}
	if rig.log.index("cascade:list-suspended") >= rig.log.index("ack") {
		t.Fatalf("order = %v, want the ack after the cascade", rig.log.snapshot())
	}
}

// TestExpireExecutionYieldsToACancelThatLandsFirst pins the precedence rule at
// its tightest point: the cancel's canceling write lands between Expire's
// status pre-read and its own write. The store refuses the timeout write, the
// read-back sees canceling, and Expire reports it without cascading, announcing
// or acknowledging, so the execution can still end canceled.
func TestExpireExecutionYieldsToACancelThatLandsFirst(t *testing.T) {
	rig := newExpireRig(t, true)
	rig.start(t)
	rig.setDeadline(pastDeadline)
	rig.state.onceBeforeTimeoutWrite(func() { rig.force(types.ExecutionStatusCanceling, "") })

	rig.expire(t, ExpireOutcomeCanceling)

	if got := rig.snapshot(t).Status; got != types.ExecutionStatusCanceling {
		t.Fatalf("status = %q, want canceling: the timeout must not displace a cancel", got)
	}
	if rig.log.count("cascade:list-suspended") != 0 || rig.log.count("ack") != 0 || rig.log.count("hook:complete") != 0 {
		t.Fatalf("calls = %v, want only the refused write", rig.log.snapshot())
	}

	// The cancel carries on from canceling and the execution ends canceled.
	if err := rig.eng.Cancel(context.Background(), rig.id); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if got := rig.snapshot(t).Status; got != types.ExecutionStatusCanceled {
		t.Fatalf("status after Cancel = %q, want canceled", got)
	}
}

// TestExpireExecutionYieldsToAConclusionThatLandsFirst is the same race against
// a normal completion: the execution succeeded between the pre-read and the
// write, so the timeout is refused and reported as already terminal.
func TestExpireExecutionYieldsToAConclusionThatLandsFirst(t *testing.T) {
	rig := newExpireRig(t, true)
	rig.start(t)
	rig.setDeadline(pastDeadline)
	rig.state.onceBeforeTimeoutWrite(func() { rig.force(types.ExecutionStatusSuccess, "") })

	rig.expire(t, ExpireOutcomeAlreadyTerminal)

	if got := rig.snapshot(t).Status; got != types.ExecutionStatusSuccess {
		t.Fatalf("status = %q, want success kept", got)
	}
	if rig.log.count("cascade:list-suspended") != 0 || rig.log.count("hook:complete") != 0 {
		t.Fatalf("calls = %v, want no cascade and no announcement", rig.log.snapshot())
	}
	if n := rig.log.count("ack"); n != 1 {
		t.Fatalf("acked %d times, want 1: nothing is left to expire", n)
	}
}

// TestExpireExecutionConcurrentCallsConverge runs two expiries of one execution
// at once (the sweeper and a lease path can both observe the deadline). Under
// -race it pins that the shared state is only touched through the store's
// fences: the execution ends in timeout, the cascade is idempotent, and every
// announcement carries timeout.
func TestExpireExecutionConcurrentCallsConverge(t *testing.T) {
	rig := newExpireRig(t, true)
	rig.start(t)
	rig.setDeadline(pastDeadline)

	const callers = 2
	outcomes := make([]ExpireOutcome, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			outcomes[i], errs[i] = rig.eng.ExpireExecution(context.Background(), rig.id)
		}(i)
	}
	wg.Wait()

	expired := 0
	for i := 0; i < callers; i++ {
		if errs[i] != nil {
			t.Fatalf("call %d error = %v", i, errs[i])
		}
		switch outcomes[i] {
		case ExpireOutcomeExpired:
			expired++
		case ExpireOutcomeResumed:
		default:
			t.Fatalf("call %d outcome = %q, want expired or resumed", i, outcomes[i])
		}
	}
	if expired == 0 {
		t.Fatalf("outcomes = %v, want at least one call to win the write", outcomes)
	}
	if got := rig.snapshot(t).Status; got != types.ExecutionStatusTimeout {
		t.Fatalf("status = %q, want timeout", got)
	}
	if got := rig.nodeStatus(t); got != types.NodeStatusCanceled {
		t.Fatalf("node status = %q, want canceled", got)
	}
	got := rig.hooks.completions()
	if len(got) < 1 || len(got) > callers {
		t.Fatalf("completion hooks = %v, want one per winning call", got)
	}
	for _, status := range got {
		if status != types.ExecutionStatusTimeout {
			t.Fatalf("completion hooks = %v, want only timeout", got)
		}
	}
}

// TestExpireExecutionCascadesToSubWorkflowChildren covers the sub-workflow half
// of the cascade on an execution with a parked sub-workflow node: the child
// the node waits on is canceled, the parked node is finalized as canceled
// rather than concluded from the dying child, the parent's completion hook
// fires once with timeout, and the child's own cancellation does not announce
// anything on the parent's behalf.
func TestExpireExecutionCascadesToSubWorkflowChildren(t *testing.T) {
	ctx := context.Background()
	g := resolvedSubWorkflowParent(t)
	parentID := types.ExecutionID("exec-parent")
	log := &expireLog{track: parentID}
	hooks := &expireHooks{log: log}
	state := &expireState{subWorkflowLinkState: newSubWorkflowLinkState(), log: log}
	if err := state.CreateExecution(ctx, &ExecutionSnapshot{
		ID:       parentID,
		Graph:    g,
		Status:   types.ExecutionStatusRunning,
		Deadline: pastDeadline,
	}); err != nil {
		t.Fatalf("seed parent execution: %v", err)
	}
	queue := &fakeQueue{}
	eng := New(&expireAckState{expireState: state}, queue,
		WithHooks(hooks), WithSubWorkflowResolver(subWorkflowStubResolver(t)))
	idx, ok := g.NodeIndex("sub")
	if !ok {
		t.Fatal(`the graph has no node named "sub"`)
	}
	handled, err := eng.handleSystemTask(ctx, &Task{
		ExecutionID:  parentID,
		NodeName:     "sub",
		NodeIdx:      idx,
		UnitIdx:      g.UnitIndexForNode(idx),
		Type:         TaskTypeSubWorkflow,
		ActivationID: 1,
	}, true)
	if err != nil || !handled {
		t.Fatalf("dispatch the sub-workflow node: handled=%v err=%v", handled, err)
	}
	link, found, err := state.GetSubWorkflowLink(ctx, parentID, "sub")
	if err != nil || !found {
		t.Fatalf("read the sub-workflow link: found=%v err=%v", found, err)
	}
	log.clear()

	got, err := eng.ExpireExecution(ctx, parentID)
	if err != nil || got != ExpireOutcomeExpired {
		t.Fatalf("ExpireExecution() = (%q, %v), want (%q, nil)", got, err, ExpireOutcomeExpired)
	}

	parent, err := state.GetExecution(ctx, parentID)
	if err != nil || parent == nil || parent.Status != types.ExecutionStatusTimeout || parent.Error != ExecutionDeadlineExceededError {
		t.Fatalf("parent = %+v (err %v), want timeout with the deadline reason", parent, err)
	}
	child, err := state.GetExecution(ctx, link.ChildExecutionID)
	if err != nil || child == nil || child.Status != types.ExecutionStatusCanceled {
		t.Fatalf("child = %+v (err %v), want canceled by the cascade", child, err)
	}
	node, err := state.GetNode(ctx, parentID, "sub")
	if err != nil || node == nil || node.Status != types.NodeStatusCanceled {
		t.Fatalf("parked node = %+v (err %v), want canceled: the cascade must finalize the wait", node, err)
	}
	if node.ChildExecutionID != link.ChildExecutionID {
		t.Fatalf("canceled node child = %q, want %q kept", node.ChildExecutionID, link.ChildExecutionID)
	}
	if got := hooks.completions(); len(got) != 1 || got[0] != types.ExecutionStatusTimeout {
		t.Fatalf("parent completion hooks = %v, want exactly [timeout]", got)
	}
	if write, ack := log.index("write:timeout"), log.index("ack"); write < 0 || ack < 0 || write > ack {
		t.Fatalf("order = %v, want the timeout write before the ack", log.snapshot())
	}
}

// TestExpireExecutionSpan pins the span contract: one xflow.execution.expire
// span, parented to the submission trace the execution carries (a background
// sweeper has no span of its own), carrying the execution id and the outcome
// and nothing else.
func TestExpireExecutionSpan(t *testing.T) {
	rec, tracer := installEngineSpanRecorder(t)
	rig := newExpireRig(t, true)
	id, parentSC, _ := submitUnderParentSpan(t, rig.eng, rig.g, tracer)
	rig.adopt(t, id)
	rig.setDeadline(pastDeadline)

	rig.expire(t, ExpireOutcomeExpired)

	span := findEndedSpan(t, rec, "xflow.execution.expire")
	assertSpanParent(t, span, parentSC)
	if kv, ok := spanAttribute(span, "execution_id"); !ok || kv.Value.AsString() != string(id) {
		t.Errorf("execution_id attribute = %v, want %q", kv.Value.String(), id)
	}
	if kv, ok := spanAttribute(span, "outcome"); !ok || kv.Value.AsString() != string(ExpireOutcomeExpired) {
		t.Errorf("outcome attribute = %v, want %q", kv.Value.String(), ExpireOutcomeExpired)
	}
	// namespace is stamped by the tracer on every engine span; anything beyond
	// it and the two attributes above would be new data on a trace export.
	for _, kv := range span.Attributes() {
		switch string(kv.Key) {
		case "execution_id", "outcome", "namespace":
		default:
			t.Errorf("span carries unexpected attribute %q", kv.Key)
		}
	}
	assertNoSensitiveSpanAttributes(t, span)
}
