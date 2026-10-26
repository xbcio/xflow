package rstate

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// parkOnSignal drives the execution's one queued task to a suspension: the node
// is leased and its result is a suspend on the signal "go", which is exactly
// what a handler's PrepareSuspend produces. The park happens before the race,
// so the race starts from an execution that has a waiter to wake.
func (r *deadlineRaceRig) parkOnSignal(ctx context.Context, id types.ExecutionID) {
	r.t.Helper()
	lease := r.lease(ctx, id)
	err := r.eng.CommitTaskResult(ctx, lease, engine.TaskResult{
		Suspend: &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"go"}},
	})
	if err != nil {
		r.t.Fatalf("CommitTaskResult(suspend) error = %v", err)
	}
	if got := r.snapshot(ctx, id).Status; got != types.ExecutionStatusRunning {
		r.t.Fatalf("execution after the park = %q, want running", got)
	}
	nodes, err := r.state.ListSuspendedNodes(ctx, id)
	if err != nil || len(nodes) != 1 {
		r.t.Fatalf("ListSuspendedNodes after the park = (%v, %v), want exactly one", nodes, err)
	}
	if n, err := r.rdb.Exists(ctx, waiterKey(namespace.Default, id, "go")).Result(); err != nil || n != 1 {
		r.t.Fatalf("waiter after the park = (%d, %v), want it registered", n, err)
	}
}

// signalRaceRound is what one execution of the signal race looked like once it
// had settled. The race is replayed once and recorded here, because the store of
// every round is closed as soon as the round ends; the subtests then read the
// same recordings, so the invariants and the known-leak observation can never
// disagree about which run they describe.
type signalRaceRound struct {
	raced       bool // false for the baseline expiry that has no signal beside it
	status      types.ExecutionStatus
	reason      string
	nodeStatus  types.NodeStatus
	outcome     engine.ExpireOutcome
	commit      engine.CommitOutcome
	announced   []types.ExecutionStatus
	drainIssue  string
	lateLease   error // BuildTaskLease for a resume that reaches a worker after the end
	afterLate   types.ExecutionStatus
	lateSignal  error   // Engine.DeliverSignal after the end
	leftover    []error // BuildTaskLease for each resume the race itself left queued
	lateQueued  int
	lateOutbox  int64 // outbox entries the late signal added
	waiterAfter int64 // EXISTS waiter:go once the execution has ended
	signalErr   error
	expireErr   error
}

// outboxSize counts the resume deliveries the store still holds for an execution.
func (r *deadlineRaceRig) outboxSize(ctx context.Context, id types.ExecutionID) int64 {
	ready, err := r.rdb.ZCard(ctx, outboxReadyKey(namespace.Default, id)).Result()
	if err != nil {
		r.t.Fatalf("ZCARD outbox ready of %s: %v", id, err)
	}
	body, err := r.rdb.HLen(ctx, outboxBodyKey(namespace.Default, id)).Result()
	if err != nil {
		r.t.Fatalf("HLEN outbox body of %s: %v", id, err)
	}
	return ready + body
}

// settleSignalRound records everything the subtests look at once the execution
// has stopped changing, and then pushes a signal and a resume at it that arrive
// too late.
func (r *deadlineRaceRig) settleSignalRound(ctx context.Context, g *graph.Graph, id types.ExecutionID, rec *signalRaceRound) {
	r.t.Helper()
	snap := r.snapshot(ctx, id)
	rec.status, rec.reason = snap.Status, snap.Error
	node, err := r.state.GetNode(ctx, id, "park")
	if err != nil || node == nil {
		r.t.Fatalf("GetNode(park) = (%+v, %v)", node, err)
	}
	rec.nodeStatus = node.Status
	rec.announced = r.hooks.completionsOf(id)
	rec.drainIssue = r.drainProblem(g.Name(), id)
	rec.waiterAfter = r.existsWaiter(ctx, id)

	// A flush that ran beside the signal may enqueue its resume after the
	// signal's own goroutine looked at the queue. Such a task was queued while
	// the execution was still alive; what matters is that a worker is refused.
	for _, task := range r.queue.take(id) {
		_, err := r.eng.BuildTaskLease(ctx, task)
		rec.leftover = append(rec.leftover, err)
	}
	before := r.outboxSize(ctx, id)
	rec.lateSignal = r.eng.DeliverSignal(ctx, id, "go", map[string]any{"late": true})
	rec.lateQueued = len(r.queue.take(id))
	rec.lateOutbox = r.outboxSize(ctx, id) - before
	late := &engine.Task{
		ExecutionID: id, NodeName: "park", NodeIdx: 0, ActivationID: 1,
		Type:    engine.TaskTypeNodeResume,
		Payload: &types.SignalPayload{Triggered: types.SignalReceived, Name: "go"},
	}
	_, rec.lateLease = r.eng.BuildTaskLease(ctx, late)
	rec.afterLate = r.snapshot(ctx, id).Status
}

func (r *deadlineRaceRig) existsWaiter(ctx context.Context, id types.ExecutionID) int64 {
	n, err := r.rdb.Exists(ctx, waiterKey(namespace.Default, id, "go")).Result()
	if err != nil {
		r.t.Fatalf("EXISTS waiter of %s: %v", id, err)
	}
	return n
}

// runSignalRace replays the race raceRounds() times, after one baseline round
// in which the expiry has no signal beside it, and returns what each looked like.
func runSignalRace(t *testing.T) []signalRaceRound {
	ctx := context.Background()
	var rounds []signalRaceRound
	for round := -1; round < raceRounds(); round++ {
		rig := newDeadlineRaceRig(t)
		g := raceGraph(t, "deadline-race-park", 0, types.NodeDef{Name: "park", Type: "test.race_park"})
		id := rig.submit(ctx, g)
		rig.parkOnSignal(ctx, id)

		rec := signalRaceRound{raced: round >= 0}
		if !rec.raced {
			rec.outcome, rec.expireErr = rig.eng.ExpireExecution(ctx, id)
		} else {
			raceTogether(
				func() {
					if rec.signalErr = rig.eng.DeliverSignal(ctx, id, "go", map[string]any{"by": "race"}); rec.signalErr != nil {
						return
					}
					// What a queue worker does with the resume the signal left behind.
					for _, task := range rig.queue.take(id) {
						lease, err := rig.eng.BuildTaskLease(ctx, task)
						if err != nil {
							if !errors.Is(err, engine.ErrExecutionInactive) {
								rec.signalErr = err
							}
							continue
						}
						rec.commit, rec.signalErr = rig.eng.CommitTaskResultWithOutcome(ctx, lease, engine.TaskResult{
							Output: &types.Output{Data: map[string]any{"woken": true}},
						})
					}
				},
				func() { rec.outcome, rec.expireErr = rig.eng.ExpireExecution(ctx, id) },
			)
		}
		rig.settleSignalRound(ctx, g, id, &rec)
		rig.close()
		rounds = append(rounds, rec)
	}
	return rounds
}

// TestExpireRacingASignalNeverResumesATimedOutExecutionOnRedis replays the race
// between the deadline sweep and the signal that wakes the execution's only
// suspended node, on a real Engine over the Redis store. The woken node is
// carried all the way: the resume task the signal queued is leased and committed
// on the same goroutine that delivered the signal, so the two sides really
// contend for the execution rather than for a queue entry.
//
// Success and timeout are both legitimate endings, because the deadline is
// already behind the clock. What is not legitimate is a timed-out execution
// that a woken node still advances.
func TestExpireRacingASignalNeverResumesATimedOutExecutionOnRedis(t *testing.T) {
	rounds := runSignalRace(t)

	t.Run("invariants", func(t *testing.T) {
		var succeeded, timedOut int
		for i, rec := range rounds {
			if rec.signalErr != nil {
				t.Fatalf("round %d: signal and resume error = %v", i, rec.signalErr)
			}
			if rec.expireErr != nil {
				t.Fatalf("round %d: ExpireExecution() error = %v", i, rec.expireErr)
			}
			switch rec.status {
			case types.ExecutionStatusSuccess:
				succeeded++
				if !rec.raced {
					t.Fatalf("round %d: an expiry with no signal beside it ended in success", i)
				}
				if rec.nodeStatus != types.NodeStatusSuccess {
					t.Fatalf("round %d: execution succeeded but the woken node is %q", i, rec.nodeStatus)
				}
				if rec.commit != engine.CommitOutcomeAccepted {
					t.Fatalf("round %d: execution succeeded but the resume commit was %q", i, rec.commit)
				}
				if rec.outcome != engine.ExpireOutcomeAlreadyTerminal {
					t.Fatalf("round %d: the resume won but ExpireExecution reported %q", i, rec.outcome)
				}
			case types.ExecutionStatusTimeout:
				timedOut++
				if rec.reason != engine.ExecutionDeadlineExceededError {
					t.Fatalf("round %d: timeout reason = %q, want %q", i, rec.reason, engine.ExecutionDeadlineExceededError)
				}
				if rec.outcome != engine.ExpireOutcomeExpired {
					t.Fatalf("round %d: the expiry won but ExpireExecution reported %q, want expired", i, rec.outcome)
				}
				if rec.nodeStatus == types.NodeStatusSuccess {
					t.Fatalf("round %d: execution timed out but the woken node completed", i)
				}
				if rec.commit == engine.CommitOutcomeAccepted {
					t.Fatalf("round %d: execution timed out but a resume commit was accepted", i)
				}
			default:
				t.Fatalf("round %d: execution ended %q, want success or timeout", i, rec.status)
			}
			if len(rec.announced) == 0 {
				t.Fatalf("round %d: execution ended %q but its completion was never announced", i, rec.status)
			}
			for _, status := range rec.announced {
				if status != rec.status {
					t.Fatalf("round %d: execution ended %q but its completion was announced as %v", i, rec.status, rec.announced)
				}
			}
			if rec.drainIssue != "" {
				t.Fatalf("round %d: %s", i, rec.drainIssue)
			}
			// A signal or a resume that arrives after the end changes nothing.
			if !errors.Is(rec.lateLease, engine.ErrExecutionInactive) {
				t.Fatalf("round %d: late resume lease error = %v, want ErrExecutionInactive", i, rec.lateLease)
			}
			if rec.lateSignal != nil || rec.lateQueued != 0 || rec.lateOutbox != 0 {
				t.Fatalf("round %d: late DeliverSignal = (%v), queued %d, outbox +%d, want a silent no-op",
					i, rec.lateSignal, rec.lateQueued, rec.lateOutbox)
			}
			if rec.status == types.ExecutionStatusTimeout {
				for _, err := range rec.leftover {
					if !errors.Is(err, engine.ErrExecutionInactive) {
						t.Fatalf("round %d: a resume left queued by the race was leased on a timed-out execution: error = %v", i, err)
					}
				}
			}
			if rec.afterLate != rec.status {
				t.Fatalf("round %d: execution went from %q to %q after a late signal", i, rec.status, rec.afterLate)
			}
		}
		if timedOut == 0 {
			t.Fatalf("no round ended in timeout; the baseline expiry alone should have")
		}
		t.Logf("%d rounds: %d ended success, %d ended timeout", len(rounds), succeeded, timedOut)
	})

	// waiter_key_leak_as_it_stands pins a KNOWN, UNDECIDED defect so that fixing
	// it is a visible, deliberate change. A node parked through the durable
	// suspend path in single-signal mode has no stored waiter spec (the Lua only
	// writes one in multi-signal mode), so cleanupExecutionIndexes, which finds
	// the signal-named waiter key by reading that spec, cannot delete it. The key
	// outlives the timed-out execution until its TTL. A plain Cancel leaves it
	// too. The engine refuses a late signal for an inactive execution, which is
	// what the invariants subtest asserts; the store-level DeliverSignalWithOutbox
	// would still accept it.
	//
	// When the defect is fixed, this subtest turns red on purpose: change it to
	// require that the waiter key is gone (waiterAfter == 0) and move that
	// assertion into invariants.
	t.Run("waiter_key_leak_as_it_stands", func(t *testing.T) {
		// Only the baseline round is deterministic: with a signal racing the
		// expiry, a signal that wins the waiter consumes the key itself.
		base := rounds[0]
		if base.raced || base.status != types.ExecutionStatusTimeout {
			t.Fatalf("baseline round = (raced %v, %q), want an unraced expiry that timed out", base.raced, base.status)
		}
		if base.waiterAfter != 1 {
			t.Fatalf("waiter:go exists = %d after the timeout; the leak this subtest records is gone, "+
				"so assert the key is cleaned up instead", base.waiterAfter)
		}
	})
}
