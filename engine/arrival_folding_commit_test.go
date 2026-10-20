package engine

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/xbcio/xflow/types"
)

// foldingOrderLog records the callbacks a folded commit emits, in delivery
// order, across the three independent observation surfaces a commit touches:
// the lifecycle hook, the skip observer, and (read separately, below) the
// runtime evidence buffer. A per-surface assertion cannot prove that a skip is
// reported before the node is announced complete; one shared log can.
type foldingOrderLog struct {
	mu     sync.Mutex
	events []string
}

func (l *foldingOrderLog) add(event string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, event)
}

func (l *foldingOrderLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

func (l *foldingOrderLog) count(event string) int {
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

type foldingOrderHooks struct {
	BaseHooks
	log *foldingOrderLog
}

func (h foldingOrderHooks) OnNodeComplete(_ context.Context, _ types.ExecutionID, name string, _ types.NodeStatus) {
	h.log.add("complete:" + name)
}

type foldingOrderSkipObserver struct {
	log *foldingOrderLog
}

func (o foldingOrderSkipObserver) OnNodeSkip(_ context.Context, flow, node string, count int) {
	o.log.add(fmt.Sprintf("skip:%s:%s:%d", flow, node, count))
}

// drainEvidence reads everything published so far without blocking. Every
// publish happens synchronously inside the commit call under test, and the
// buffer is sized for the whole fixture, so an empty read means "done".
func drainEvidence(buf *RuntimeEvidenceBuffer) []RuntimeEvidenceEvent {
	var events []RuntimeEvidenceEvent
	for {
		select {
		case ev := <-buf.Events():
			events = append(events, ev)
		default:
			return events
		}
	}
}

// TestFoldedCommitReportsSkipOnceBeforeNodeComplete pins the two things a skip
// resolved by the commit itself has to get right: it is reported exactly once,
// by the transition that resolved it, and it is reported before that source
// node's completion is announced.
//
// The order is not cosmetic. A consumer that reacts to OnNodeComplete by
// reading the skip series must already see the skip this node caused; the
// cyclic path publishes its advance receipt before notifyNodeComplete for the
// same reason, and folding makes the acyclic commit an advance too.
//
// Exactly once is the harder half. A skip produced by the commit is also a
// durable TaskTypeNodeSkip intent, which FlushOutbox consumes inside the same
// CommitTaskResult call. If that consuming commit reported the upstream
// transition's skip list — or if the flush replayed the source advance — the
// operator-visible series would double-count a branch that was skipped once.
func TestFoldedCommitReportsSkipOnceBeforeNodeComplete(t *testing.T) {
	ctx := context.Background()
	log := &foldingOrderLog{}
	buf := NewRuntimeEvidenceBuffer(32)
	queue := &fakeQueue{}
	eng := New(newFakeState(), queue,
		WithHooks(foldingOrderHooks{log: log}),
		WithNodeSkipObserver(foldingOrderSkipObserver{log: log}),
		WithRuntimeEvidenceBuffer(buf),
	)

	id, lease := startSkipDiamond(t, ctx, eng, queue)
	if _, err := eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{
		Output: &types.Output{Port: "main", Data: map[string]any{"out": "start"}},
	}); err != nil {
		t.Fatalf("CommitTaskResultWithOutcome(start) error = %v", err)
	}
	drainEvidence(buf)
	if got := log.snapshot(); !reflect.DeepEqual(got, []string{"complete:start"}) {
		t.Fatalf("events after an all-execute commit = %v, want [complete:start]", got)
	}

	// split commits on "main", stranding its "other" destination. The commit
	// folds that arrival, resolves on_other as skip, and its flush then consumes
	// the skip intent — all inside this one call.
	commitOn(t, ctx, eng, queue, id, "split", "main")

	// split's folded commit reports the skip before its own completion, then its
	// flush consumes the durable skip intent, which commits on_other as skipped
	// and announces that completion in turn.
	want := []string{
		"complete:start",
		"skip:advance:on_other:1",
		"complete:split",
		"complete:on_other",
	}
	if got := log.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("events after the folded skip commit = %v, want %v "+
			"(skip exactly once, and before split's completion)", got, want)
	}

	// The evidence stream has to show the folded commit for split followed by
	// ITS advance receipt. A second advance event for split would mean the old
	// AdvanceNode carrier is still running alongside the fold; a missing one
	// would mean the folded transition stopped being observable as an advance.
	var commitIdx, advanceIdx int = -1, -1
	events := drainEvidence(buf)
	for i, ev := range events {
		if ev.NodeName != "split" {
			continue
		}
		switch ev.Type {
		case RuntimeEvidenceCommit:
			if commitIdx >= 0 {
				t.Fatalf("split published %d commit receipts, want 1", 2)
			}
			commitIdx = i
		case RuntimeEvidenceAdvance:
			if advanceIdx >= 0 {
				t.Fatalf("split published %d advance receipts, want 1 (the commit "+
					"already folded the advance, so a second one is a duplicate)", 2)
			}
			advanceIdx = i
		}
	}
	if commitIdx < 0 {
		t.Fatalf("no commit receipt for split; events = %+v", events)
	}
	if advanceIdx < 0 {
		t.Fatalf("no advance receipt for split; events = %+v", events)
	}
	if commitIdx > advanceIdx {
		t.Fatalf("advance receipt (index %d) precedes commit receipt (index %d); "+
			"the advance receipt must describe an applied commit", advanceIdx, commitIdx)
	}

	// Drive the remaining branch to be sure the single skip is not re-reported
	// by a later transition.
	commitOn(t, ctx, eng, queue, id, "on_main", "main")
	if got := log.count("skip:advance:on_other:1"); got != 1 {
		t.Fatalf("on_other was reported skipped %d times across the execution, "+
			"want exactly 1: events = %v", got, log.snapshot())
	}
}

// TestFoldedCommitWritesNoDownstreamWhileCanceling pins the cancel fence on the
// folded path. The deterministic "canceling" status below is the state a
// concurrent Cancel produces; the backend transition is what has to refuse to
// schedule into it, because a folded arrival is scheduling work exactly like an
// advance intent and must stop under the same conditions.
//
// A commit racing a Cancel still lands its node terminal write — that is the
// commit's own fence, not the downstream one — so the assertion is that the
// node reached success while the destination unit has no scheduling marker and
// no outbox intent. Without that pair the test could pass by the commit simply
// failing.
func TestFoldedCommitWritesNoDownstreamWhileCanceling(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)

	g := compileCommitProbeGraph(t)
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() after submit error = %v", err)
	}
	nextIdx, ok := g.NodeIndex("next")
	if !ok {
		t.Fatal(`compiled graph has no "next" node`)
	}
	task := queue.Drain()[0]
	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil || lease == nil {
		t.Fatalf("BuildTaskLease() = %v, %v", lease, err)
	}

	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceling, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus() error = %v", err)
	}
	outcome, err := eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{
		Output: &types.Output{Data: map[string]any{"out": 1}},
	})
	if err != nil {
		t.Fatalf("CommitTaskResultWithOutcome() error = %v", err)
	}
	if outcome != CommitOutcomeAccepted {
		t.Fatalf("outcome = %s, want %s", outcome, CommitOutcomeAccepted)
	}

	node, err := state.GetNode(ctx, id, "start")
	if err != nil || node == nil || node.Status != types.NodeStatusSuccess {
		t.Fatalf("committed node = %+v, %v, want success", node, err)
	}
	state.mu.Lock()
	pending := len(state.atomicOutbox[id])
	marker := state.atomicSchedule[fakeAtomicCounterKey(id, nextIdx)]
	state.mu.Unlock()
	if pending != 0 {
		t.Fatalf("outbox entries after a canceling commit = %d, want 0", pending)
	}
	if marker != "" {
		t.Fatalf("downstream scheduling marker = %q, want none: the folded arrival "+
			"must not schedule work during a Cancel", marker)
	}
	if delivered := queue.Drain(); len(delivered) != 0 {
		t.Fatalf("delivered after a canceling commit = %+v, want nothing", taskNames(delivered))
	}
}
