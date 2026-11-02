package statestoretest

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// SuspendSignalStore is a backend that can park a claimed lease on a signal
// wait, consume signals that arrived before the wait, and expose the resume
// intents it recorded for them.
type SuspendSignalStore interface {
	engine.StateStore
	engine.AtomicStateStore
	engine.DurableLeaseSuspender
	engine.DurableSignalDeliverer
}

// RunSuspendSignalBufferContract pins how a suspension consumes signals that
// were stored before it: it takes only as many as the wait needs and leaves the
// rest stored for the next suspension.
//
// The distributed backend once took every stored signal of the wait in one
// suspension, delivered only the first in the resume payload, and deleted the
// others. A node that suspends again after each signal (sequential approval)
// accumulates stored signals while its resume is still running, so the extras
// vanished without reaching the node's decisions or its ignored list. The
// memory backend stops at the first satisfied wait, which is the behaviour this
// contract fixes for both.
func RunSuspendSignalBufferContract(t *testing.T, manifest *Manifest, newStore func(*testing.T) SuspendSignalStore) {
	t.Helper()
	manifest.Report(t)
	// The factory's return type already requires engine.DurableLeaseSuspender,
	// so only the capabilities the interface set cannot express are declared.
	manifest.RequireDeclared(t, CapabilityAtomicStateStore, CapabilityDurableSignalDeliverer)

	const waiter = "finish"
	g := ContractGraph()
	nodeIdx, ok := g.NodeIndex(waiter)
	if !ok {
		t.Fatalf("ContractGraph has no node %q", waiter)
	}
	unitIdx := g.UnitIndexForNode(nodeIdx)

	// start creates the execution and returns a function that leases the
	// waiter the way a resume does: a fresh lease on the already-suspended
	// node. Every call uses its own lease ID because the resume intent a
	// suspension records is keyed by it.
	start := func(t *testing.T) (SuspendSignalStore, types.ExecutionID, func(*testing.T, *types.SignalPayload) *engine.TaskLease) {
		t.Helper()
		ctx := context.Background()
		s := newStore(t)
		id := types.ExecutionID("e-" + t.Name())
		if err := s.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID: id, Graph: g, Status: types.ExecutionStatusRunning}); err != nil {
			t.Fatalf("create execution: %v", err)
		}
		round := 0
		lease := func(t *testing.T, resume *types.SignalPayload) *engine.TaskLease {
			t.Helper()
			round++
			name := engine.LeaseID(fmt.Sprintf("%s-lease-%d", id, round))
			l := &engine.TaskLease{
				LeaseID:    name,
				LeaseToken: engine.LeaseToken("token-" + string(name)),
				IssuedAt:   time.Now().UTC().Truncate(time.Millisecond),
				TTL:        time.Minute,
				Task: engine.Task{
					ExecutionID:  id,
					NodeName:     waiter,
					NodeIdx:      nodeIdx,
					UnitIdx:      unitIdx,
					Type:         engine.TaskTypeNodeExec,
					ActivationID: 1,
				},
			}
			if resume != nil {
				l.Task.Type = engine.TaskTypeNodeResume
				l.Task.Payload = resume
			}
			if _, acquired, err := s.AcquireTaskLease(ctx, l); err != nil || !acquired {
				t.Fatalf("round %d: AcquireTaskLease acquired=%v err=%v", round, acquired, err)
			}
			node, err := s.GetNode(ctx, id, waiter)
			if err != nil || node == nil {
				t.Fatalf("round %d: GetNode node=%v err=%v", round, node, err)
			}
			l.Attempt = node.Attempt
			if _, claimed, err := s.ClaimTaskLease(ctx, l); err != nil || !claimed {
				t.Fatalf("round %d: ClaimTaskLease claimed=%v err=%v", round, claimed, err)
			}
			return l
		}
		return s, id, lease
	}

	ctx := context.Background()

	deliver := func(t *testing.T, s SuspendSignalStore, id types.ExecutionID, name string) {
		t.Helper()
		// No waiter exists yet, so the signal is stored for a later suspension.
		if _, _, err := s.DeliverSignal(ctx, id, name, map[string]any{"by": name}); err != nil {
			t.Fatalf("DeliverSignal(%q): %v", name, err)
		}
	}
	suspend := func(t *testing.T, s SuspendSignalStore, l *engine.TaskLease, spec *types.SuspendSpec, oldSignal string) {
		t.Helper()
		committed, err := s.SuspendTaskLeaseWithOutbox(ctx, l, nil, false, false, spec, oldSignal)
		if err != nil || !committed {
			t.Fatalf("SuspendTaskLeaseWithOutbox committed=%v err=%v, want true/nil", committed, err)
		}
	}
	// drain returns the resume payloads recorded since the last drain and
	// acknowledges them, so each suspension is judged on its own intents.
	drain := func(t *testing.T, s SuspendSignalStore, id types.ExecutionID) []*types.SignalPayload {
		t.Helper()
		entries, err := s.ListOutbox(ctx, id, time.Now().Add(time.Hour), 16)
		if err != nil {
			t.Fatalf("ListOutbox: %v", err)
		}
		var out []*types.SignalPayload
		for _, e := range entries {
			if e.Task.Type != engine.TaskTypeNodeResume || e.Task.Payload == nil {
				t.Fatalf("outbox entry %+v is not a resume with a payload", e)
			}
			if err := s.AckOutbox(ctx, id, e.ID); err != nil {
				t.Fatalf("AckOutbox(%q): %v", e.ID, err)
			}
			out = append(out, e.Task.Payload)
		}
		return out
	}
	wantSingle := func(t *testing.T, got []*types.SignalPayload, name string) *types.SignalPayload {
		t.Helper()
		if len(got) != 1 || got[0].Name != name || got[0].Data["by"] != name {
			t.Fatalf("resume intents = %+v, want exactly one for signal %q", got, name)
		}
		return got[0]
	}
	wantAll := func(t *testing.T, p *types.SignalPayload, names ...string) {
		t.Helper()
		have := make([]string, 0, len(p.All))
		for name, data := range p.All {
			have = append(have, name)
			if data["by"] != name {
				t.Errorf("All[%q] = %v, want the payload that signal carried", name, data)
			}
		}
		sort.Strings(have)
		want := slices.Sorted(slices.Values(names))
		if !slices.Equal(have, want) {
			t.Fatalf("resume payload All = %v, want exactly %v", have, want)
		}
	}
	assertSuspended := func(t *testing.T, s SuspendSignalStore, id types.ExecutionID) {
		t.Helper()
		nodes, err := s.ListSuspendedNodes(ctx, id)
		if err != nil || !slices.Contains(nodes, waiter) {
			t.Fatalf("suspended nodes = %v err=%v, want %q still waiting", nodes, err, waiter)
		}
	}
	// Two signals stored before the first suspension satisfy a single-signal
	// wait one at a time. The suspension delivers the first, in spec order, and
	// leaves the second stored; the next suspension of the same wait then
	// delivers it. Each signal reaches the node exactly once, none vanishes.
	t.Run("SingleSignalWaitConsumesOneAndKeepsTheRest", func(t *testing.T) {
		s, id, lease := start(t)
		spec := &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"first", "second"}}
		deliver(t, s, id, "first")
		deliver(t, s, id, "second")

		suspend(t, s, lease(t, nil), spec, "")
		resumed := wantSingle(t, drain(t, s, id), "first")

		suspend(t, s, lease(t, resumed), spec, resumed.Name)
		resumed = wantSingle(t, drain(t, s, id), "second")

		suspend(t, s, lease(t, resumed), spec, resumed.Name)
		if extra := drain(t, s, id); len(extra) != 0 {
			t.Fatalf("a third suspension produced resume intents %+v; both stored signals were "+
				"already delivered, so none may be delivered again", extra)
		}
		assertSuspended(t, s, id)
	})

	// The same rule holds when the wait is driven through SuspendOrConsume
	// rather than through a claimed lease. That call returns the payload
	// directly, and a consumer that resumes per signal (sequential approval)
	// reads the triggering Name/Data rather than All: a signal consumed past the
	// quorum reaches neither the decisions of that resume nor the stored signals
	// of the next one, so it vanishes for that consumer. Collecting only up to
	// the quorum and leaving the rest stored is what keeps every arrival
	// individually resumable.
	t.Run("SuspendOrConsumeMultiStopsAtQuorumAndKeepsTheRest", func(t *testing.T) {
		s, id, _ := start(t)
		spec := &types.SuspendSpec{
			Mode: types.ModeMultiSignal, Signals: []string{"one", "two", "three"}, Quorum: 2,
		}
		deliver(t, s, id, "one")
		deliver(t, s, id, "two")
		deliver(t, s, id, "three")

		payload, err := s.SuspendOrConsume(ctx, id, waiter, spec)
		if err != nil {
			t.Fatalf("SuspendOrConsume() error = %v", err)
		}
		if payload == nil {
			t.Fatal("SuspendOrConsume() parked, want the satisfied quorum collected")
		}
		wantAll(t, payload, "one", "two")

		// Re-suspending finds only the signal the first call left behind, so a
		// quorum of two is not met and the call parks with it still stored.
		again, err := s.SuspendOrConsume(ctx, id, waiter, spec)
		if err != nil {
			t.Fatalf("SuspendOrConsume(second) error = %v", err)
		}
		if again != nil {
			t.Fatalf("SuspendOrConsume(second) = %+v, want nil: one stored signal is "+
				"below the quorum of two", again)
		}
		assertSuspended(t, s, id)

		// One more arrival completes the next quorum together with the signal
		// the first call left behind, instead of finding it consumed and gone.
		node, resumed, err := s.DeliverSignal(ctx, id, "one", map[string]any{"by": "one"})
		if err != nil || node != waiter || resumed == nil {
			t.Fatalf("DeliverSignal(one) = (node %q, payload %+v, err %v), want a resume of %q",
				node, resumed, err, waiter)
		}
		wantAll(t, resumed, "three", "one")
	})

	// With a quorum of two, a suspension stops once two stored signals have
	// been collected. The third stays stored and counts toward the next
	// collection instead of being consumed and dropped.
	t.Run("MultiSignalWaitStopsAtQuorumAndKeepsTheRest", func(t *testing.T) {
		s, id, lease := start(t)
		spec := &types.SuspendSpec{
			Mode: types.ModeMultiSignal, Signals: []string{"one", "two", "three"}, Quorum: 2,
		}
		deliver(t, s, id, "one")
		deliver(t, s, id, "two")
		deliver(t, s, id, "three")

		suspend(t, s, lease(t, nil), spec, "")
		got := drain(t, s, id)
		if len(got) != 1 {
			t.Fatalf("resume intents = %+v, want exactly one for the satisfied quorum", got)
		}
		// The backends differ on which member Name reports, so only the set of
		// collected signals is part of the contract.
		wantAll(t, got[0], "one", "two")

		suspend(t, s, lease(t, got[0]), spec, got[0].Name)
		if extra := drain(t, s, id); len(extra) != 0 {
			t.Fatalf("resume intents = %+v after re-suspending with one stored signal "+
				"left, want none: a quorum of two is not met", extra)
		}
		assertSuspended(t, s, id)

		// The signal the first suspension left behind is still collected: one
		// more arrival completes the next quorum together with it.
		_, _, committed, err := s.DeliverSignalWithOutbox(ctx, id, "one",
			map[string]any{"by": "one"},
			engine.ResumeIntent{NodeName: waiter, NodeIdx: nodeIdx, UnitIdx: unitIdx})
		if err != nil || !committed {
			t.Fatalf("DeliverSignalWithOutbox committed=%v err=%v, want true/nil", committed, err)
		}
		next := drain(t, s, id)
		if len(next) != 1 {
			t.Fatalf("resume intents = %+v, want exactly one for the second quorum", next)
		}
		wantAll(t, next[0], "three", "one")
	})
}
