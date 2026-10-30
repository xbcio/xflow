package xflow

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// These tests cover a sequential approval gate that suspends again after every
// signal. Signals that arrive while the gate is awake (a resume still running)
// or before its first suspension are stored, and the next suspension finds
// several at once. Each of them is a valid signal that returned no error, so
// each must end in the gate's decisions or its ignored trail, and the approval
// must still complete. The scenarios are ordered by hooks, never by timing.

const approvalBufGate = "gate"

var approvalBufChain = []string{"supervisor", "dept-head", "admin"}

// approvalBufHold holds chosen starts of the gate node open. Holding the first
// resume stretches the window in which the node is neither suspended nor
// parked, which is exactly the window a slow runner round trip creates.
type approvalBufHold struct {
	engine.BaseHooks

	mu        sync.Mutex
	starts    int
	suspended int
	release   map[int]chan struct{} // start number (1 = first run) -> release
	blocked   chan int
}

func newApprovalBufHold(holdOn ...int) *approvalBufHold {
	h := &approvalBufHold{release: map[int]chan struct{}{}, blocked: make(chan int, 8)}
	for _, n := range holdOn {
		h.release[n] = make(chan struct{})
	}
	return h
}

func (h *approvalBufHold) OnNodeStart(ctx context.Context, _ types.ExecutionID, name string) {
	if name != approvalBufGate {
		return
	}
	h.mu.Lock()
	h.starts++
	n := h.starts
	gate := h.release[n]
	h.mu.Unlock()
	if gate == nil {
		return
	}
	h.blocked <- n
	select {
	case <-gate:
	case <-ctx.Done():
	}
}

func (h *approvalBufHold) OnNodeSuspended(_ context.Context, _ types.ExecutionID, name string) {
	if name != approvalBufGate {
		return
	}
	h.mu.Lock()
	h.suspended++
	h.mu.Unlock()
}

func (h *approvalBufHold) suspensions() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.suspended
}

func (h *approvalBufHold) open(n int) { close(h.release[n]) }

func (h *approvalBufHold) waitBlocked(t *testing.T, n int) {
	t.Helper()
	select {
	case got := <-h.blocked:
		if got != n {
			t.Fatalf("hook blocked on start %d, want %d", got, n)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("start %d of the gate never began", n)
	}
}

// approvalBufBackend builds an engine on one backend. The cluster variant runs
// on miniredis, so a resume costs a queue poll (about a second).
type approvalBufBackend struct {
	name string
	make func(t *testing.T, hooks engine.Hooks) *Engine
}

func approvalBufBackends() []approvalBufBackend {
	return []approvalBufBackend{
		{"local", func(t *testing.T, hooks engine.Hooks) *Engine {
			eng, err := NewLocal(WithHooks(hooks))
			if err != nil {
				t.Fatal(err)
			}
			return eng
		}},
		{"cluster", func(t *testing.T, hooks engine.Hooks) *Engine {
			mr, err := miniredis.Run()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(mr.Close)
			eng, err := NewCluster(ClusterConfig{RedisAddr: mr.Addr()}, WithConcurrency(4), WithHooks(hooks))
			if err != nil {
				t.Fatal(err)
			}
			return eng
		}},
	}
}

func startApprovalBufGate(t *testing.T, eng *Engine) types.ExecutionID {
	t.Helper()
	ctx := context.Background()
	wf := Workflow("approval-signal-buffer")
	start := wf.Node("start", node.Start())
	gate := wf.Node(approvalBufGate, node.Approval(approvalBufChain, node.ApprovalSequential))
	wf.Connect(start, gate)
	wfID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}
	id, err := eng.Invoke(ctx, wfID, EntryNode("start"), nil)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	return id
}

func sendApprovalBufSignal(t *testing.T, eng *Engine, id types.ExecutionID, actor string) {
	t.Helper()
	data := map[string]any{types.VerifiedActorKey: actor, "action": "approve"}
	if err := eng.Signal(context.Background(), id, approvalBufGate+"/approval/"+actor, data); err != nil {
		t.Fatalf("Signal(%s) = %v", actor, err)
	}
}

// approvalBufLen counts a ledger list. The local backend returns the slice the
// node built, the cluster backend one decoded from JSON.
func approvalBufLen(v any) int {
	switch l := v.(type) {
	case []any:
		return len(l)
	case []map[string]any:
		return len(l)
	}
	return 0
}

func approvalBufCount(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	}
	return 0
}

// assertApprovalBufAccounted waits for the execution to finish, then checks the
// gate's record. accepted is the number of valid signals Signal returned nil
// for; every one of them must appear as a decision or an ignored entry.
func assertApprovalBufAccounted(t *testing.T, eng *Engine, id types.ExecutionID, accepted int) {
	t.Helper()
	// A cluster resume takes about a second and a chain of three can need
	// several of them, so the bound is generous and is only a hang guard.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := eng.Wait(ctx, id)
	if err != nil || res.Status != types.ExecutionStatusSuccess {
		t.Fatalf("approval did not complete: status=%v err=%v", res.Status, err)
	}
	d, err := eng.Inspect(context.Background(), id, approvalBufGate)
	if err != nil || len(d.Nodes) != 1 {
		t.Fatalf("Inspect(%s): nodes=%d err=%v", approvalBufGate, len(d.Nodes), err)
	}
	out := d.Nodes[0].Output
	decisions := approvalBufLen(out["decisions"])
	ignored := approvalBufCount(out["ignored_count"])
	if got := decisions + ignored; got != accepted {
		t.Fatalf("accepted %d signals, the gate recorded decisions=%d ignored=%d: %d lost",
			accepted, decisions, ignored, accepted-got)
	}
	if decisions != len(approvalBufChain) {
		t.Fatalf("decisions = %d, want one per approver (%d)", decisions, len(approvalBufChain))
	}
}

// waitApprovalBufDecisions blocks until the gate's published record holds at
// least n decisions. It reads the record rather than sleeping, so the next
// signal is sent only after the previous approver's decision is on the ledger.
func waitApprovalBufDecisions(t *testing.T, eng *Engine, id types.ExecutionID, n int) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		d, err := eng.Inspect(context.Background(), id, approvalBufGate)
		if err == nil && len(d.Nodes) == 1 {
			if approvalBufLen(d.Nodes[0].Output["decisions"]) >= n {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("the gate never recorded %d decisions", n)
}

// TestSequentialApprovalKeepsSignalsStoredDuringResume holds the first resume
// open, so the gate has been woken but has not suspended again, and delivers
// three signals in that window. All three are stored. The next suspension finds
// them together and may consume only the one it needs; the others must reach
// the gate on the suspensions after it.
func TestSequentialApprovalKeepsSignalsStoredDuringResume(t *testing.T) {
	for _, b := range approvalBufBackends() {
		t.Run(b.name, func(t *testing.T) {
			hold := newApprovalBufHold(2) // start 1 = first run, start 2 = first resume
			eng := b.make(t, hold)
			t.Cleanup(eng.Stop)
			id := startApprovalBufGate(t, eng)
			deadline := time.Now().Add(10 * time.Second)
			for hold.suspensions() < 1 {
				if time.Now().After(deadline) {
					t.Fatal("the gate never suspended")
				}
				time.Sleep(10 * time.Millisecond)
			}

			sendApprovalBufSignal(t, eng, id, "dept-head") // out of turn: wakes the gate, which the hook holds
			hold.waitBlocked(t, 2)
			sendApprovalBufSignal(t, eng, id, "supervisor") // stored
			sendApprovalBufSignal(t, eng, id, "dept-head")  // stored
			sendApprovalBufSignal(t, eng, id, "admin")      // stored
			hold.open(2)

			assertApprovalBufAccounted(t, eng, id, 4)
		})
	}
}

// TestSequentialApprovalKeepsSignalsStoredBeforeFirstSuspend holds the gate's
// first run and delivers two valid signals before it has ever suspended. The
// first suspension finds both. The second approver's decision must still be
// recorded, and the last approver completes the chain afterwards.
func TestSequentialApprovalKeepsSignalsStoredBeforeFirstSuspend(t *testing.T) {
	for _, b := range approvalBufBackends() {
		t.Run(b.name, func(t *testing.T) {
			hold := newApprovalBufHold(1)
			eng := b.make(t, hold)
			t.Cleanup(eng.Stop)
			id := startApprovalBufGate(t, eng)
			hold.waitBlocked(t, 1)

			sendApprovalBufSignal(t, eng, id, "supervisor")
			sendApprovalBufSignal(t, eng, id, "dept-head")
			hold.open(1)

			// The admin decision is out of turn until dept-head's is recorded.
			waitApprovalBufDecisions(t, eng, id, 2)
			sendApprovalBufSignal(t, eng, id, "admin")

			assertApprovalBufAccounted(t, eng, id, 3)
		})
	}
}
