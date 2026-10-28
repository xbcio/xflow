package engine

import (
	"context"
	"errors"
	"slices"
	"strings"
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

// requireExactly fails unless the log holds exactly want, in that order.
func (l *expireLog) requireExactly(t *testing.T, want ...string) {
	t.Helper()
	if got := l.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("calls = %v, want exactly %v", got, want)
	}
}

// requireInOrder fails unless every event is in the log and their first
// occurrences come in the given order.
func (l *expireLog) requireInOrder(t *testing.T, want ...string) {
	t.Helper()
	prev := -1
	for _, event := range want {
		i := l.index(event)
		if i < 0 {
			t.Fatalf("calls = %v, missing %q", l.snapshot(), event)
		}
		if i <= prev {
			t.Fatalf("calls = %v, want %v in that order", l.snapshot(), want)
		}
		prev = i
	}
}

// requireLast fails unless event is the final entry in the log.
func (l *expireLog) requireLast(t *testing.T, event string) {
	t.Helper()
	if got := l.snapshot(); len(got) == 0 || got[len(got)-1] != event {
		t.Fatalf("calls = %v, want %q as the last call", got, event)
	}
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
	listSuspendedErrBy map[types.ExecutionID]error
	linkReadErr        error
	upsertNodeErr      error
	ackErr             error
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
	if byID, ok := s.listSuspendedErrBy[id]; ok {
		err = byID
		delete(s.listSuspendedErrBy, id)
	}
	s.faultMu.Unlock()
	if err != nil {
		return nil, err
	}
	return s.fakeState.ListSuspendedNodes(ctx, id)
}

// UpsertNode logs the node writes of the tracked execution, which is what makes
// the sub-workflow half of the cascade (finalizeParkedSubWorkflowNodes, the only
// node write an expiry makes with this store) visible in the ordered log. The
// one-shot fault fails the first such write, so a test can break that step
// after the status write landed.
func (s *expireState) UpsertNode(ctx context.Context, n *NodeSnapshot) error {
	s.log.add(n.ExecutionID, "upsert-node:"+string(n.Status))
	if n.ExecutionID == s.log.track {
		s.faultMu.Lock()
		err := s.upsertNodeErr
		s.upsertNodeErr = nil
		s.faultMu.Unlock()
		if err != nil {
			return err
		}
	}
	return s.subWorkflowLinkState.UpsertNode(ctx, n)
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

// GetSubWorkflowLink answers a one-shot injected error before reading the link,
// which is how a test breaks the first step of the child cascade.
func (s *expireState) GetSubWorkflowLink(ctx context.Context, id types.ExecutionID, nodeName string) (SubWorkflowLink, bool, error) {
	s.faultMu.Lock()
	err := s.linkReadErr
	s.linkReadErr = nil
	s.faultMu.Unlock()
	if err != nil {
		return SubWorkflowLink{}, false, err
	}
	return s.subWorkflowLinkState.GetSubWorkflowLink(ctx, id, nodeName)
}

func (s *expireState) failLinkRead(err error) {
	s.faultMu.Lock()
	s.linkReadErr = err
	s.faultMu.Unlock()
}

func (s *expireState) failListSuspended(err error) {
	s.faultMu.Lock()
	s.listSuspendedErr = err
	s.faultMu.Unlock()
}

// failListSuspendedFor fails the first ListSuspendedNodes of one execution,
// whichever execution makes it. The one-shot fault above is consumed by the
// first execution to list, which is the parent in a sub-workflow cascade; this
// one reaches a child's own cancel.
func (s *expireState) failListSuspendedFor(id types.ExecutionID, err error) {
	s.faultMu.Lock()
	if s.listSuspendedErrBy == nil {
		s.listSuspendedErrBy = make(map[types.ExecutionID]error)
	}
	s.listSuspendedErrBy[id] = err
	s.faultMu.Unlock()
}

func (s *expireState) failUpsertNode(err error) {
	s.faultMu.Lock()
	s.upsertNodeErr = err
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
	s.faultMu.Lock()
	err := s.ackErr
	s.faultMu.Unlock()
	return err
}

// failAck makes every AckDeadlineExpired answer err until it is called with nil.
func (s *expireState) failAck(err error) {
	s.faultMu.Lock()
	s.ackErr = err
	s.faultMu.Unlock()
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
	r.state.mu.Lock()
	r.state.executions[r.id].Deadline = d
	r.state.mu.Unlock()
}

// force overwrites the stored status and reason, the way another actor's
// terminal write would have left them.
func (r *expireRig) force(status types.ExecutionStatus, reason string) {
	r.state.mu.Lock()
	r.state.executions[r.id].Status = status
	r.state.executions[r.id].Error = reason
	r.state.mu.Unlock()
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
	if write >= cascade || cascade >= ack {
		t.Fatalf("order = %v, want the timeout write, then the cascade, then the ack", rig.log.snapshot())
	}
	if write >= hook {
		t.Fatalf("order = %v, want the completion hook after the durable timeout write", rig.log.snapshot())
	}
	// The hook is announced BEFORE the cascade, not after it. A Resumed call
	// never announces again, so a hook placed after a cascade that can fail on a
	// store error would be lost for good the first time that happens.
	if hook >= cascade {
		t.Fatalf("order = %v, want the completion hook before the cascade", rig.log.snapshot())
	}
	// The ack is the very last call: the entry may only be withdrawn once
	// everything the expiry owes has been done.
	rig.log.requireLast(t, "ack")
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
	// The hook comes before the cascade (a Resumed call does not announce again,
	// so it must not wait on steps that can fail), and the ack comes after every
	// step of the cascade, the sub-workflow half included: the parked node's
	// finalization is the "upsert-node" write.
	log.requireInOrder(t, "write:timeout", "hook:complete", "cascade:list-suspended", "upsert-node:canceled", "ack")
	log.requireLast(t, "ack")
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

// TestExpireExecutionYieldsToAQueueTimeoutThatLandsFirst is the same race
// against the other timeout mechanism. The concurrency queue's timeout lands
// between the pre-read and the write, so the read-back sees status timeout. The
// status alone cannot tell it from this mechanism's own write: only the reason
// can, and it is not the deadline's. Expire must report AlreadyTerminal without
// cascading and without announcing a second completion, and acknowledge once.
func TestExpireExecutionYieldsToAQueueTimeoutThatLandsFirst(t *testing.T) {
	rig := newExpireRig(t, true)
	rig.start(t)
	rig.setDeadline(pastDeadline)
	rig.state.onceBeforeTimeoutWrite(func() {
		rig.force(types.ExecutionStatusTimeout, ConcurrencyQueueTimeoutError)
	})

	rig.expire(t, ExpireOutcomeAlreadyTerminal)

	if got := rig.snapshot(t); got.Status != types.ExecutionStatusTimeout || got.Error != ConcurrencyQueueTimeoutError {
		t.Fatalf("snapshot = (%q, %q), want the queue timeout kept", got.Status, got.Error)
	}
	rig.log.requireExactly(t, "write:timeout", "ack")
	if got := rig.hooks.completions(); len(got) != 0 {
		t.Fatalf("completion hooks = %v, want none: the queue timeout is not this call's to announce", got)
	}
	if got := rig.nodeStatus(t); got != types.NodeStatusSuspended {
		t.Fatalf("node status = %q, want suspended: nothing may cascade", got)
	}
}

// traceMemoized reports whether the engine still holds a memoized trace carrier
// for id. ExpireExecution's own span reads the carrier to find its parent, and
// that read is what leaves the entry behind on an engine that does not own the
// execution.
func traceMemoized(e *Engine, id types.ExecutionID) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	_, ok := e.traceCarriers[id]
	return ok
}

func graphCached(e *Engine, id types.ExecutionID) bool {
	_, ok := e.graphs.Get(id)
	return ok
}

// primeEvictProbe leaves the engine holding nothing for the execution except
// what the call under test adds: the trace memo and the graph cache entry are
// dropped, then the graph is loaded again so a missing eviction is visible as a
// surviving entry.
func primeEvictProbe(t *testing.T, rig *expireRig) {
	t.Helper()
	rig.eng.EvictExecution(rig.id)
	if _, err := rig.eng.loadGraph(context.Background(), rig.id); err != nil {
		t.Fatalf("loadGraph() error = %v", err)
	}
	if !graphCached(rig.eng, rig.id) || traceMemoized(rig.eng, rig.id) {
		t.Fatal("precondition: want the graph cached and no trace memo before the call")
	}
}

// TestExpireExecutionGoneWritesNothingAndLeavesNoMemo pins the Gone result: an
// execution that does not exist is not written, cascaded or acknowledged, and the
// span's read of its trace carrier does not leave a memo entry on an engine that
// will never see the execution again.
func TestExpireExecutionGoneWritesNothingAndLeavesNoMemo(t *testing.T) {
	rig := newExpireRig(t, true)
	const missing = types.ExecutionID("exec-missing")
	rig.log.track = missing

	got, err := rig.eng.ExpireExecution(context.Background(), missing)
	if err != nil || got != ExpireOutcomeGone {
		t.Fatalf("ExpireExecution(missing) = (%q, %v), want (%q, nil)", got, err, ExpireOutcomeGone)
	}

	rig.log.requireExactly(t)
	if traceMemoized(rig.eng, missing) {
		t.Fatal("the trace carrier memo survived a Gone result, want it evicted")
	}
}

// TestExpireExecutionAlreadyTerminalLeavesNoMemo pins the same hygiene for an
// execution somebody else concluded: nothing is written or cascaded, the
// deadline entry is acknowledged exactly once, and neither the graph nor the
// trace memo is kept for an execution this engine has nothing more to do with.
func TestExpireExecutionAlreadyTerminalLeavesNoMemo(t *testing.T) {
	cases := []struct {
		name   string
		status types.ExecutionStatus
		reason string
	}{
		{"concluded", types.ExecutionStatusSuccess, ""},
		{"timed out in the concurrency queue", types.ExecutionStatusTimeout, ConcurrencyQueueTimeoutError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newExpireRig(t, true)
			rig.start(t)
			rig.setDeadline(pastDeadline)
			rig.force(tc.status, tc.reason)
			primeEvictProbe(t, rig)

			rig.expire(t, ExpireOutcomeAlreadyTerminal)

			rig.log.requireExactly(t, "ack")
			if traceMemoized(rig.eng, rig.id) {
				t.Fatal("the trace carrier memo survived an AlreadyTerminal result, want it evicted")
			}
			if graphCached(rig.eng, rig.id) {
				t.Fatal("the graph cache entry survived an AlreadyTerminal result, want it evicted")
			}
		})
	}
}

// expireSubWorkflowRig is a parent execution with its sub-workflow node parked
// on a real child (the dispatch ran), driven through an engine whose store and
// hooks feed one expireLog. The log tracks the parent; a test that expires the
// child points log.track at the child's id.
//
// A graph with several sub-workflow nodes parks each on its own child: nodes
// lists them in graph order and links holds every link by node name, while link
// is the one of the node named "sub".
type expireSubWorkflowRig struct {
	eng      *Engine
	state    *expireState
	log      *expireLog
	hooks    *expireHooks
	parentID types.ExecutionID
	link     SubWorkflowLink
	nodes    []string
	links    map[string]SubWorkflowLink
}

func newExpireSubWorkflowRig(t *testing.T) *expireSubWorkflowRig {
	t.Helper()
	return newExpireSubWorkflowRigOf(t, resolvedSubWorkflowParent(t))
}

func newExpireSubWorkflowRigOf(t *testing.T, g *graph.Graph) *expireSubWorkflowRig {
	t.Helper()
	ctx := context.Background()
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
	eng := New(&expireAckState{expireState: state}, &fakeQueue{},
		WithHooks(hooks), WithSubWorkflowResolver(subWorkflowStubResolver(t)))
	rig := &expireSubWorkflowRig{
		eng: eng, state: state, log: log, hooks: hooks, parentID: parentID,
		links: make(map[string]SubWorkflowLink),
	}
	for _, idx := range g.SubWorkflowNodeIndexes() {
		name := g.NodeName(idx)
		handled, err := eng.handleSystemTask(ctx, &Task{
			ExecutionID:  parentID,
			NodeName:     name,
			NodeIdx:      idx,
			UnitIdx:      g.UnitIndexForNode(idx),
			Type:         TaskTypeSubWorkflow,
			ActivationID: 1,
		}, true)
		if err != nil || !handled {
			t.Fatalf("dispatch the sub-workflow node %q: handled=%v err=%v", name, handled, err)
		}
		link, found, err := state.GetSubWorkflowLink(ctx, parentID, name)
		if err != nil || !found {
			t.Fatalf("read the sub-workflow link of %q: found=%v err=%v", name, found, err)
		}
		rig.nodes = append(rig.nodes, name)
		rig.links[name] = link
	}
	if len(rig.nodes) == 0 {
		t.Fatal("the graph has no sub-workflow node")
	}
	rig.link = rig.links["sub"]
	log.clear()
	return rig
}

func (r *expireSubWorkflowRig) nodeStatus(t *testing.T) types.NodeStatus {
	t.Helper()
	node, err := r.state.GetNode(context.Background(), r.parentID, "sub")
	if err != nil || node == nil {
		t.Fatalf("GetNode(sub) = (%v, %v)", node, err)
	}
	return node.Status
}

func (r *expireSubWorkflowRig) expire(t *testing.T, id types.ExecutionID, want ExpireOutcome) {
	t.Helper()
	got, err := r.eng.ExpireExecution(context.Background(), id)
	if err != nil || got != want {
		t.Fatalf("ExpireExecution(%q) = (%q, %v), want (%q, nil) (log %v)", id, got, err, want, r.log.snapshot())
	}
}

// TestExpireExecutionSubWorkflowCascadeFailureIsResumable is the cascade
// failure test for the last step of the cascade. Finalizing the parked
// sub-workflow node fails after the status write landed: the error must come
// back (not be swallowed into an ack that would stop the entry being listed),
// the status stays timeout, and nothing is acknowledged. The next call is
// Resumed, finishes the finalization, acknowledges once, and the completion hook
// still has fired exactly once overall.
func TestExpireExecutionSubWorkflowCascadeFailureIsResumable(t *testing.T) {
	ctx := context.Background()
	rig := newExpireSubWorkflowRig(t)
	injected := errors.New("injected: finalize parked node failed")
	rig.state.failUpsertNode(injected)

	got, err := rig.eng.ExpireExecution(ctx, rig.parentID)
	if !errors.Is(err, injected) {
		t.Fatalf("ExpireExecution() = (%q, %v), want an error wrapping %v", got, err, injected)
	}
	parent, err := rig.state.GetExecution(ctx, rig.parentID)
	if err != nil || parent == nil || parent.Status != types.ExecutionStatusTimeout || parent.Error != ExecutionDeadlineExceededError {
		t.Fatalf("parent = %+v (err %v), want timeout with the deadline reason kept", parent, err)
	}
	if n := rig.log.count("ack"); n != 0 {
		t.Fatalf("acked %d times after a failed finalization, want 0: the entry must stay listed", n)
	}
	if got := rig.nodeStatus(t); got != types.NodeStatusWaiting {
		t.Fatalf("parked node = %q, want waiting: the failed finalization did not reach it", got)
	}
	if got := rig.hooks.completions(); len(got) != 1 {
		t.Fatalf("completion hooks after the failed call = %v, want exactly one", got)
	}

	rig.expire(t, rig.parentID, ExpireOutcomeResumed)

	if got := rig.nodeStatus(t); got != types.NodeStatusCanceled {
		t.Fatalf("parked node = %q, want canceled after the resumed cascade", got)
	}
	if n := rig.log.count("ack"); n != 1 {
		t.Fatalf("acked %d times after the resume, want 1", n)
	}
	if got := rig.hooks.completions(); len(got) != 1 || got[0] != types.ExecutionStatusTimeout {
		t.Fatalf("completion hooks after the resume = %v, want still exactly [timeout]", got)
	}
	if n := rig.log.count("write:timeout"); n != 1 {
		t.Fatalf("timeout was written %d times, want once: the resume must not rewrite it", n)
	}
	rig.log.requireLast(t, "ack")
}

// TestExpireExecutionResumedNotifiesTheParent pins that a Resumed call tells the
// parent of a sub-workflow child. The call that wrote the child's timeout also
// notified the parent, but that notification is in memory only: a crash between
// the status write and the notification loses it, and the child is later seen
// only as already in timeout. The Resumed call is then the one place left to
// deliver it quickly, without waiting for the parent's wait lease to expire.
// It must not repeat the completion hook, which the first call already fired.
func TestExpireExecutionResumedNotifiesTheParent(t *testing.T) {
	ctx := context.Background()
	rig := newExpireSubWorkflowRig(t)
	childID := rig.link.ChildExecutionID
	// The earlier call that was lost: the child is in timeout for its deadline,
	// the parent has not been told.
	if err := rig.state.UpdateExecutionStatus(ctx, childID, types.ExecutionStatusTimeout, ExecutionDeadlineExceededError); err != nil {
		t.Fatalf("seed the child timeout: %v", err)
	}
	if got := rig.nodeStatus(t); got != types.NodeStatusWaiting {
		t.Fatalf("precondition: parked node = %q, want waiting", got)
	}
	rig.log.track = childID
	rig.log.clear()

	rig.expire(t, childID, ExpireOutcomeResumed)

	node, err := rig.state.GetNode(ctx, rig.parentID, "sub")
	if err != nil || node == nil || node.Status != types.NodeStatusFailed {
		t.Fatalf("parked node = %+v (err %v), want failed: the Resumed call must tell the parent its child timed out", node, err)
	}
	if !strings.Contains(node.Error, "timed out") || !strings.Contains(node.Error, string(childID)) {
		t.Fatalf("parked node error = %q, want the child's timeout and identity", node.Error)
	}
	if n := rig.log.count("hook:complete"); n != 0 {
		t.Fatalf("the Resumed call announced completion %d times, want 0", n)
	}
	rig.log.requireLast(t, "ack")
}

// TestExpireExecutionEarlyReturnsLeaveNoMemo pins that no result which returns
// before the cascade keeps the engine's per-execution memo. The call's own span
// reads the execution's trace carrier and memoizes it, and an engine that does
// not own the execution (a sweeper's, or a runner's) has nothing else that would
// ever evict it. Eviction only drops this engine's process-local caches, so a
// result that evicts something still in use costs one reload, nothing more.
func TestExpireExecutionEarlyReturnsLeaveNoMemo(t *testing.T) {
	cases := []struct {
		name    string
		prepare func(rig *expireRig)
		want    ExpireOutcome
		calls   []string
	}{
		{
			name:    "canceling",
			prepare: func(rig *expireRig) { rig.force(types.ExecutionStatusCanceling, "") },
			want:    ExpireOutcomeCanceling,
		},
		{
			name: "canceling lands between the pre-read and the write",
			prepare: func(rig *expireRig) {
				rig.state.onceBeforeTimeoutWrite(func() { rig.force(types.ExecutionStatusCanceling, "") })
			},
			want:  ExpireOutcomeCanceling,
			calls: []string{"write:timeout"},
		},
		{
			name:    "deadline not reached",
			prepare: func(rig *expireRig) { rig.setDeadline(time.Now().Add(time.Hour)) },
			want:    ExpireOutcomeNotDue,
		},
		{
			name: "graph missing",
			prepare: func(rig *expireRig) {
				rig.eng.EvictExecution(rig.id)
				rig.state.fakeState.mu.Lock()
				rig.state.fakeState.executions[rig.id].Graph = nil
				rig.state.fakeState.mu.Unlock()
			},
			want: ExpireOutcomeGone,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newExpireRig(t, true)
			rig.start(t)
			rig.setDeadline(pastDeadline)
			primeEvictProbe(t, rig)
			tc.prepare(rig)

			rig.expire(t, tc.want)

			rig.log.requireExactly(t, tc.calls...)
			if traceMemoized(rig.eng, rig.id) {
				t.Fatalf("the trace carrier memo survived a %q result, want it evicted", tc.want)
			}
			if graphCached(rig.eng, rig.id) {
				t.Fatalf("the graph cache entry survived a %q result, want it evicted", tc.want)
			}
		})
	}
}

// TestExpireExecutionChildCancelFailureIsResumable pins that a child execution
// that could not be stopped fails the expiry like any other cascade step. The
// child's own cancel fails after it marked itself canceling; swallowing that
// would acknowledge the deadline entry with a child still running, and nothing
// would ever list the parent again. The error comes back, the entry stays
// listed, and the completion hook is not repeated. After the fault clears the
// next call is Resumed and finishes the child.
func TestExpireExecutionChildCancelFailureIsResumable(t *testing.T) {
	ctx := context.Background()
	rig := newExpireSubWorkflowRig(t)
	childID := rig.link.ChildExecutionID
	injected := errors.New("injected: child list suspended nodes failed")
	rig.state.failListSuspendedFor(childID, injected)

	got, err := rig.eng.ExpireExecution(ctx, rig.parentID)
	if !errors.Is(err, injected) {
		t.Fatalf("ExpireExecution() = (%q, %v), want an error wrapping %v", got, err, injected)
	}
	parent, err := rig.state.GetExecution(ctx, rig.parentID)
	if err != nil || parent == nil || parent.Status != types.ExecutionStatusTimeout || parent.Error != ExecutionDeadlineExceededError {
		t.Fatalf("parent = %+v (err %v), want timeout with the deadline reason kept", parent, err)
	}
	child, err := rig.state.GetExecution(ctx, childID)
	if err != nil || child == nil || child.Status != types.ExecutionStatusCanceling {
		t.Fatalf("child = %+v (err %v), want canceling: its cancel failed half way", child, err)
	}
	if n := rig.log.count("ack"); n != 0 {
		t.Fatalf("acked %d times after a failed child cancel, want 0: the entry must stay listed", n)
	}
	if got := rig.nodeStatus(t); got != types.NodeStatusWaiting {
		t.Fatalf("parked node = %q, want waiting: the failed cascade did not reach it", got)
	}
	if got := rig.hooks.completions(); len(got) != 1 {
		t.Fatalf("completion hooks after the failed call = %v, want exactly one", got)
	}

	rig.expire(t, rig.parentID, ExpireOutcomeResumed)

	child, err = rig.state.GetExecution(ctx, childID)
	if err != nil || child == nil || child.Status != types.ExecutionStatusCanceled {
		t.Fatalf("child = %+v (err %v), want canceled by the resumed cascade", child, err)
	}
	if got := rig.nodeStatus(t); got != types.NodeStatusCanceled {
		t.Fatalf("parked node = %q, want canceled after the resumed cascade", got)
	}
	if n := rig.log.count("ack"); n != 1 {
		t.Fatalf("acked %d times after the resume, want 1", n)
	}
	if got := rig.hooks.completions(); len(got) != 1 || got[0] != types.ExecutionStatusTimeout {
		t.Fatalf("completion hooks after the resume = %v, want still exactly [timeout]", got)
	}
	if n := rig.log.count("write:timeout"); n != 1 {
		t.Fatalf("timeout was written %d times, want once: the resume must not rewrite it", n)
	}
	rig.log.requireLast(t, "ack")
}

// TestExpireExecutionChildAlreadyConcludedStillAcknowledges is the other side
// of the contract above: a child that is already finished, already being
// canceled, or gone is the state the cascade wants, not a failure. Returning an
// error for it would keep the entry listed forever, because the next call finds
// the same child in the same state.
func TestExpireExecutionChildAlreadyConcludedStillAcknowledges(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name      string
		prepare   func(t *testing.T, rig *expireSubWorkflowRig, childID types.ExecutionID)
		wantChild types.ExecutionStatus // empty: the child no longer exists
	}{
		{
			name: "child already succeeded",
			prepare: func(t *testing.T, rig *expireSubWorkflowRig, childID types.ExecutionID) {
				if err := rig.state.UpdateExecutionStatus(ctx, childID, types.ExecutionStatusSuccess, ""); err != nil {
					t.Fatalf("conclude the child: %v", err)
				}
			},
			wantChild: types.ExecutionStatusSuccess,
		},
		{
			name: "child already canceled",
			prepare: func(t *testing.T, rig *expireSubWorkflowRig, childID types.ExecutionID) {
				if err := rig.state.UpdateExecutionStatus(ctx, childID, types.ExecutionStatusCanceled, ""); err != nil {
					t.Fatalf("cancel the child: %v", err)
				}
			},
			wantChild: types.ExecutionStatusCanceled,
		},
		{
			name: "child already canceling",
			prepare: func(t *testing.T, rig *expireSubWorkflowRig, childID types.ExecutionID) {
				if err := rig.state.UpdateExecutionStatus(ctx, childID, types.ExecutionStatusCanceling, ""); err != nil {
					t.Fatalf("start the child's cancel: %v", err)
				}
			},
			wantChild: types.ExecutionStatusCanceled,
		},
		{
			name: "child gone",
			prepare: func(t *testing.T, rig *expireSubWorkflowRig, childID types.ExecutionID) {
				rig.state.fakeState.mu.Lock()
				delete(rig.state.fakeState.executions, childID)
				rig.state.fakeState.mu.Unlock()
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newExpireSubWorkflowRig(t)
			childID := rig.link.ChildExecutionID
			tc.prepare(t, rig, childID)

			rig.expire(t, rig.parentID, ExpireOutcomeExpired)

			if n := rig.log.count("ack"); n != 1 {
				t.Fatalf("acked %d times, want 1: a concluded child must not hold the entry", n)
			}
			if got := rig.nodeStatus(t); got != types.NodeStatusCanceled {
				t.Fatalf("parked node = %q, want canceled", got)
			}
			if got := rig.hooks.completions(); len(got) != 1 || got[0] != types.ExecutionStatusTimeout {
				t.Fatalf("completion hooks = %v, want exactly [timeout]", got)
			}
			child, err := rig.state.GetExecution(ctx, childID)
			if err != nil {
				t.Fatalf("read the child: %v", err)
			}
			switch {
			case tc.wantChild == "" && child != nil:
				t.Fatalf("child = %+v, want it gone", child)
			case tc.wantChild != "" && (child == nil || child.Status != tc.wantChild):
				t.Fatalf("child = %+v, want status %q", child, tc.wantChild)
			}
			rig.log.requireLast(t, "ack")
		})
	}
}

// TestExpireExecutionAckFailureIsReturned pins that a failed acknowledgement
// reaches the caller on every path that acknowledges. Swallowing it would report
// success for an entry that is still listed, and the sweeper's only signal that
// the call must be repeated is the error. After the fault clears, the replay
// acknowledges and, for an execution this mechanism timed out, does not repeat
// the completion hook.
func TestExpireExecutionAckFailureIsReturned(t *testing.T) {
	cases := []struct {
		name       string
		prepare    func(rig *expireRig)
		replay     ExpireOutcome
		wantHooks  int
		wantStatus types.ExecutionStatus
	}{
		{
			name:       "expired",
			prepare:    func(*expireRig) {},
			replay:     ExpireOutcomeResumed,
			wantHooks:  1,
			wantStatus: types.ExecutionStatusTimeout,
		},
		{
			name: "resumed",
			prepare: func(rig *expireRig) {
				rig.force(types.ExecutionStatusTimeout, ExecutionDeadlineExceededError)
			},
			replay:     ExpireOutcomeResumed,
			wantStatus: types.ExecutionStatusTimeout,
		},
		{
			name:       "already terminal",
			prepare:    func(rig *expireRig) { rig.force(types.ExecutionStatusSuccess, "") },
			replay:     ExpireOutcomeAlreadyTerminal,
			wantStatus: types.ExecutionStatusSuccess,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newExpireRig(t, true)
			rig.start(t)
			rig.setDeadline(pastDeadline)
			tc.prepare(rig)
			injected := errors.New("injected: acknowledge failed")
			rig.state.failAck(injected)

			got, err := rig.eng.ExpireExecution(context.Background(), rig.id)
			if !errors.Is(err, injected) {
				t.Fatalf("ExpireExecution() = (%q, %v), want an error wrapping %v", got, err, injected)
			}
			if got != "" {
				t.Fatalf("outcome = %q on error, want empty", got)
			}
			if n := rig.log.count("ack"); n != 1 {
				t.Fatalf("ack attempted %d times, want 1", n)
			}

			rig.state.failAck(nil)
			rig.expire(t, tc.replay)

			if n := rig.log.count("ack"); n != 2 {
				t.Fatalf("ack attempted %d times after the replay, want 2", n)
			}
			if got := rig.snapshot(t).Status; got != tc.wantStatus {
				t.Fatalf("status = %q, want %q", got, tc.wantStatus)
			}
			if got := rig.hooks.completions(); len(got) != tc.wantHooks {
				t.Fatalf("completion hooks = %v, want %d: a replay must not announce again", got, tc.wantHooks)
			}
			rig.log.requireLast(t, "ack")
		})
	}
}

// TestExpireExecutionEvictsOnlyAfterTheCascadeSucceeds pins the eviction timing.
// A cascade that failed leaves the execution to be replayed, so its graph must
// still be cached: evicting before or despite the failure costs the replay a
// reload at best and, with the eviction ahead of the cascade, drops the entry
// the cascade is about to use. The replay that succeeds then drops both the
// graph and the trace memo.
func TestExpireExecutionEvictsOnlyAfterTheCascadeSucceeds(t *testing.T) {
	rig := newExpireRig(t, true)
	rig.start(t)
	rig.setDeadline(pastDeadline)
	primeEvictProbe(t, rig)
	injected := errors.New("injected: list suspended nodes failed")
	rig.state.failListSuspended(injected)

	if got, err := rig.eng.ExpireExecution(context.Background(), rig.id); !errors.Is(err, injected) {
		t.Fatalf("ExpireExecution() = (%q, %v), want an error wrapping %v", got, err, injected)
	}
	if !graphCached(rig.eng, rig.id) {
		t.Fatal("the graph cache entry was dropped by a failed cascade, want it kept for the replay")
	}

	rig.expire(t, ExpireOutcomeResumed)

	if graphCached(rig.eng, rig.id) {
		t.Fatal("the graph cache entry survived the resumed expiry, want it evicted")
	}
	if traceMemoized(rig.eng, rig.id) {
		t.Fatal("the trace carrier memo survived the resumed expiry, want it evicted")
	}
}

// TestExpireExecutionEvictsAfterASuccessfulCascade pins that both outcomes that
// ran the cascade drop the graph and the trace memo: an engine that does not own
// the execution (a sweeper's) would otherwise keep them for good.
func TestExpireExecutionEvictsAfterASuccessfulCascade(t *testing.T) {
	cases := []struct {
		name    string
		prepare func(rig *expireRig)
		want    ExpireOutcome
	}{
		{"expired", func(*expireRig) {}, ExpireOutcomeExpired},
		{
			name: "resumed",
			prepare: func(rig *expireRig) {
				rig.force(types.ExecutionStatusTimeout, ExecutionDeadlineExceededError)
			},
			want: ExpireOutcomeResumed,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newExpireRig(t, true)
			rig.start(t)
			rig.setDeadline(pastDeadline)
			tc.prepare(rig)
			primeEvictProbe(t, rig)

			rig.expire(t, tc.want)

			if graphCached(rig.eng, rig.id) {
				t.Fatalf("the graph cache entry survived a %q result, want it evicted", tc.want)
			}
			if traceMemoized(rig.eng, rig.id) {
				t.Fatalf("the trace carrier memo survived a %q result, want it evicted", tc.want)
			}
			if got := rig.nodeStatus(t); got != types.NodeStatusCanceled {
				t.Fatalf("node status = %q, want canceled: the cascade must have run", got)
			}
		})
	}
}

// TestExpireExecutionTimeoutWithoutAReasonIsNotThisMechanisms pins the other
// edge of the reason check. A timeout carrying no reason at all is not the
// deadline's own write (that one always carries ExecutionDeadlineExceededError),
// so it is neither cascaded nor announced; only the entry is acknowledged.
func TestExpireExecutionTimeoutWithoutAReasonIsNotThisMechanisms(t *testing.T) {
	rig := newExpireRig(t, true)
	rig.start(t)
	rig.setDeadline(pastDeadline)
	rig.force(types.ExecutionStatusTimeout, "")

	rig.expire(t, ExpireOutcomeAlreadyTerminal)

	if got := rig.snapshot(t); got.Status != types.ExecutionStatusTimeout || got.Error != "" {
		t.Fatalf("snapshot = (%q, %q), want the reasonless timeout kept", got.Status, got.Error)
	}
	rig.log.requireExactly(t, "ack")
	if got := rig.hooks.completions(); len(got) != 0 {
		t.Fatalf("completion hooks = %v, want none", got)
	}
	if got := rig.nodeStatus(t); got != types.NodeStatusSuspended {
		t.Fatalf("node status = %q, want suspended: nothing may cascade", got)
	}
}

// TestExpireExecutionExpiredNotifiesTheParent pins that the call that writes a
// sub-workflow child's timeout tells the parent, so the parked node concludes
// at once instead of waiting for its wait lease to expire. The Resumed variant
// is TestExpireExecutionResumedNotifiesTheParent.
func TestExpireExecutionExpiredNotifiesTheParent(t *testing.T) {
	ctx := context.Background()
	rig := newExpireSubWorkflowRig(t)
	childID := rig.link.ChildExecutionID
	rig.state.fakeState.mu.Lock()
	rig.state.fakeState.executions[childID].Deadline = pastDeadline
	rig.state.fakeState.mu.Unlock()
	if got := rig.nodeStatus(t); got != types.NodeStatusWaiting {
		t.Fatalf("precondition: parked node = %q, want waiting", got)
	}
	rig.log.track = childID
	rig.log.clear()

	rig.expire(t, childID, ExpireOutcomeExpired)

	node, err := rig.state.GetNode(ctx, rig.parentID, "sub")
	if err != nil || node == nil || node.Status != types.NodeStatusFailed {
		t.Fatalf("parked node = %+v (err %v), want failed: the expiry must tell the parent its child timed out", node, err)
	}
	if !strings.Contains(node.Error, "timed out") || !strings.Contains(node.Error, string(childID)) {
		t.Fatalf("parked node error = %q, want the child's timeout and identity", node.Error)
	}
	if got := rig.hooks.completions(); len(got) != 1 || got[0] != types.ExecutionStatusTimeout {
		t.Fatalf("completion hooks = %v, want exactly [timeout]", got)
	}
	rig.log.requireLast(t, "ack")
}

// TestExpireExecutionLinkReadFailureIsResumable pins that a sub-workflow link
// that cannot be read fails the expiry too: the child it names cannot be found,
// let alone stopped, so acknowledging would strand it. The replay reads the link,
// stops the child and acknowledges once.
func TestExpireExecutionLinkReadFailureIsResumable(t *testing.T) {
	ctx := context.Background()
	rig := newExpireSubWorkflowRig(t)
	childID := rig.link.ChildExecutionID
	injected := errors.New("injected: read sub-workflow link failed")
	rig.state.failLinkRead(injected)

	got, err := rig.eng.ExpireExecution(ctx, rig.parentID)
	if !errors.Is(err, injected) {
		t.Fatalf("ExpireExecution() = (%q, %v), want an error wrapping %v", got, err, injected)
	}
	if n := rig.log.count("ack"); n != 0 {
		t.Fatalf("acked %d times after an unreadable link, want 0: the entry must stay listed", n)
	}
	child, err := rig.state.GetExecution(ctx, childID)
	if err != nil || child == nil || types.IsTerminalExecutionStatus(child.Status) {
		t.Fatalf("child = %+v (err %v), want it still running: its link could not be read", child, err)
	}

	rig.expire(t, rig.parentID, ExpireOutcomeResumed)

	child, err = rig.state.GetExecution(ctx, childID)
	if err != nil || child == nil || child.Status != types.ExecutionStatusCanceled {
		t.Fatalf("child = %+v (err %v), want canceled by the resumed cascade", child, err)
	}
	if n := rig.log.count("ack"); n != 1 {
		t.Fatalf("acked %d times after the resume, want 1", n)
	}
	if got := rig.hooks.completions(); len(got) != 1 {
		t.Fatalf("completion hooks = %v, want exactly one", got)
	}
}

// TestExpireExecutionLinkReadFailureDoesNotStopTheOtherChildren pins that a link
// that cannot be read costs only its own child: the cascade records the failure
// and goes on to the next sub-workflow node, so a child further down the walk is
// still stopped by the call that failed. Returning at the first unreadable link
// would leave that child running with the entry still listed only because of the
// other one. The one-shot fault fails the first link read, which is the first
// sub-workflow node in graph order (rig.nodes lists them in that order), so the
// test follows rig.nodes rather than naming a node. The replay finds the
// remaining child, acknowledges once, and does not repeat the completion hook.
func TestExpireExecutionLinkReadFailureDoesNotStopTheOtherChildren(t *testing.T) {
	ctx := context.Background()
	def := subWorkflowParentDef()
	def.Nodes = append(def.Nodes, types.NodeDef{
		Name:       "sub2",
		Type:       types.SubWorkflowNodeType,
		Parameters: map[string]any{"workflow": "child"},
	})
	g, err := graph.CompileWithResolver(ctx, def, subWorkflowStubResolver(t))
	if err != nil {
		t.Fatalf("CompileWithResolver: %v", err)
	}
	rig := newExpireSubWorkflowRigOf(t, g)
	if len(rig.nodes) != 2 {
		t.Fatalf("parked nodes = %v, want two", rig.nodes)
	}
	unread, other := rig.nodes[0], rig.nodes[1]
	unreadChild := rig.links[unread].ChildExecutionID
	otherChild := rig.links[other].ChildExecutionID
	injected := errors.New("injected: read sub-workflow link failed")
	rig.state.failLinkRead(injected)

	got, err := rig.eng.ExpireExecution(ctx, rig.parentID)
	if !errors.Is(err, injected) {
		t.Fatalf("ExpireExecution() = (%q, %v), want an error wrapping %v", got, err, injected)
	}
	child, err := rig.state.GetExecution(ctx, otherChild)
	if err != nil || child == nil || child.Status != types.ExecutionStatusCanceled {
		t.Fatalf("child of %q = %+v (err %v), want canceled: an unreadable link must not stop the others", other, child, err)
	}
	child, err = rig.state.GetExecution(ctx, unreadChild)
	if err != nil || child == nil || child.Status != types.ExecutionStatusRunning {
		t.Fatalf("child of %q = %+v (err %v), want it still running: its link could not be read", unread, child, err)
	}
	if n := rig.log.count("ack"); n != 0 {
		t.Fatalf("acked %d times after an unreadable link, want 0: the entry must stay listed", n)
	}
	if got := rig.hooks.completions(); len(got) != 1 {
		t.Fatalf("completion hooks after the failed call = %v, want exactly one", got)
	}

	rig.expire(t, rig.parentID, ExpireOutcomeResumed)

	for _, name := range rig.nodes {
		child, err := rig.state.GetExecution(ctx, rig.links[name].ChildExecutionID)
		if err != nil || child == nil || child.Status != types.ExecutionStatusCanceled {
			t.Fatalf("child of %q = %+v (err %v), want canceled after the replay", name, child, err)
		}
	}
	if n := rig.log.count("ack"); n != 1 {
		t.Fatalf("acked %d times after the replay, want 1", n)
	}
	if got := rig.hooks.completions(); len(got) != 1 || got[0] != types.ExecutionStatusTimeout {
		t.Fatalf("completion hooks after the replay = %v, want still exactly [timeout]", got)
	}
	if n := rig.log.count("write:timeout"); n != 1 {
		t.Fatalf("timeout was written %d times, want once: the replay must not rewrite it", n)
	}
	rig.log.requireLast(t, "ack")
}

// TestExpireExecutionOneFailingChildDoesNotStopTheOthers pins the cascade's
// join: the children of a parent's sub-workflow nodes are each cancelled even
// when an earlier one could not be, and the error that comes back names every
// failure. Stopping at the first failure would leave the later children running
// with nothing left to list the parent again for them once the entry were
// acknowledged, and would hide the second failure behind the first. The replay
// after the faults clear finishes both children, acknowledges once, and does not
// repeat the completion hook.
func TestExpireExecutionOneFailingChildDoesNotStopTheOthers(t *testing.T) {
	cases := []struct {
		name string
		fail []string // parent node names whose child's own cancel fails
	}{
		{name: "the first child fails", fail: []string{"sub"}},
		{name: "the second child fails", fail: []string{"sub2"}},
		{name: "both children fail", fail: []string{"sub", "sub2"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			def := subWorkflowParentDef()
			def.Nodes = append(def.Nodes, types.NodeDef{
				Name:       "sub2",
				Type:       types.SubWorkflowNodeType,
				Parameters: map[string]any{"workflow": "child"},
			})
			g, err := graph.CompileWithResolver(ctx, def, subWorkflowStubResolver(t))
			if err != nil {
				t.Fatalf("CompileWithResolver: %v", err)
			}
			rig := newExpireSubWorkflowRigOf(t, g)
			if got := rig.nodes; len(got) != 2 || got[0] != "sub" || got[1] != "sub2" {
				t.Fatalf("parked nodes = %v, want [sub sub2]", got)
			}
			injected := make(map[string]error, len(tc.fail))
			for _, name := range tc.fail {
				injected[name] = errors.New("injected: child list suspended nodes failed for " + name)
				rig.state.failListSuspendedFor(rig.links[name].ChildExecutionID, injected[name])
			}

			got, err := rig.eng.ExpireExecution(ctx, rig.parentID)
			if err == nil {
				t.Fatalf("ExpireExecution() = (%q, nil), want the child cancel failures", got)
			}
			for name, want := range injected {
				if !errors.Is(err, want) {
					t.Fatalf("ExpireExecution() error = %v, want it to wrap the failure of %q (%v)", err, name, want)
				}
			}
			for _, name := range rig.nodes {
				child, err := rig.state.GetExecution(ctx, rig.links[name].ChildExecutionID)
				if err != nil || child == nil {
					t.Fatalf("read the child of %q: %+v, %v", name, child, err)
				}
				want := types.ExecutionStatusCanceled
				if injected[name] != nil {
					want = types.ExecutionStatusCanceling
				}
				if child.Status != want {
					t.Fatalf("child of %q = %q after the failed call, want %q: one failing child must not stop the others", name, child.Status, want)
				}
			}
			if n := rig.log.count("ack"); n != 0 {
				t.Fatalf("acked %d times after a failed child cancel, want 0: the entry must stay listed", n)
			}
			if got := rig.hooks.completions(); len(got) != 1 {
				t.Fatalf("completion hooks after the failed call = %v, want exactly one", got)
			}

			rig.expire(t, rig.parentID, ExpireOutcomeResumed)

			for _, name := range rig.nodes {
				child, err := rig.state.GetExecution(ctx, rig.links[name].ChildExecutionID)
				if err != nil || child == nil || child.Status != types.ExecutionStatusCanceled {
					t.Fatalf("child of %q = %+v (err %v), want canceled after the replay", name, child, err)
				}
				node, err := rig.state.GetNode(ctx, rig.parentID, name)
				if err != nil || node == nil || node.Status != types.NodeStatusCanceled {
					t.Fatalf("parked node %q = %+v (err %v), want canceled after the replay", name, node, err)
				}
			}
			if n := rig.log.count("ack"); n != 1 {
				t.Fatalf("acked %d times after the replay, want 1", n)
			}
			if got := rig.hooks.completions(); len(got) != 1 || got[0] != types.ExecutionStatusTimeout {
				t.Fatalf("completion hooks after the replay = %v, want still exactly [timeout]", got)
			}
			if n := rig.log.count("write:timeout"); n != 1 {
				t.Fatalf("timeout was written %d times, want once: the replay must not rewrite it", n)
			}
			rig.log.requireLast(t, "ack")
		})
	}
}

// TestExpireExecutionKeepsTheGraphWhenAChildCancelFails pins the eviction
// timing for the one cascade step that has its own error path: a child that
// could not be stopped. The call fails, the entry stays listed and the replay
// will run the cascade again, so the graph it needs must still be cached; the
// replay that stops the child is the call that evicts it and acknowledges.
func TestExpireExecutionKeepsTheGraphWhenAChildCancelFails(t *testing.T) {
	ctx := context.Background()
	rig := newExpireSubWorkflowRig(t)
	childID := rig.link.ChildExecutionID
	// The dispatch left the graph cached; drop it and load it again so the
	// entry under test is the one this probe put there.
	rig.eng.EvictExecution(rig.parentID)
	if _, err := rig.eng.loadGraph(ctx, rig.parentID); err != nil {
		t.Fatalf("loadGraph() error = %v", err)
	}
	if !graphCached(rig.eng, rig.parentID) {
		t.Fatal("precondition: want the graph cached before the call")
	}
	injected := errors.New("injected: child list suspended nodes failed")
	rig.state.failListSuspendedFor(childID, injected)

	got, err := rig.eng.ExpireExecution(ctx, rig.parentID)
	if !errors.Is(err, injected) {
		t.Fatalf("ExpireExecution() = (%q, %v), want an error wrapping %v", got, err, injected)
	}
	if !graphCached(rig.eng, rig.parentID) {
		t.Fatal("the graph cache entry was dropped by a failed child cancel, want it kept for the replay")
	}
	if n := rig.log.count("ack"); n != 0 {
		t.Fatalf("acked %d times after a failed child cancel, want 0", n)
	}

	rig.expire(t, rig.parentID, ExpireOutcomeResumed)

	if graphCached(rig.eng, rig.parentID) {
		t.Fatal("the graph cache entry survived the resumed expiry, want it evicted")
	}
	if n := rig.log.count("ack"); n != 1 {
		t.Fatalf("acked %d times after the replay, want 1", n)
	}
	rig.log.requireLast(t, "ack")
}
