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

// TestSignalThatFinishesFirstKeepsSuccessAgainstALateExpiryOnRedis is the signal
// race with the interleaving fixed to the one the replay above almost never
// lands on: the signal side runs to the end before the expiry starts. The signal
// is delivered, the resume it queued is leased, and the woken node's commit
// concludes the execution as success, all while the deadline is already behind
// the clock. The expiry that arrives afterwards must find a finished execution
// and leave it alone.
//
// The expiry has two ways to arrive late. ExpireExecution reads the status and
// backs off. An expiry that read the status before the signal side finished is
// past that read by the time it writes, so the same timeout write it would issue
// is sent straight to the store here; the status script's terminal fence is the
// only thing between that write and a success turned into a timeout.
func TestSignalThatFinishesFirstKeepsSuccessAgainstALateExpiryOnRedis(t *testing.T) {
	ctx := context.Background()
	rig := newDeadlineRaceRig(t)
	g := raceGraph(t, "deadline-race-park", 0, types.NodeDef{Name: "park", Type: "test.race_park"})
	id := rig.submit(ctx, g)
	rig.parkOnSignal(ctx, id)

	if err := rig.eng.DeliverSignal(ctx, id, "go", map[string]any{"by": "signal"}); err != nil {
		t.Fatalf("DeliverSignal() error = %v", err)
	}
	tasks := rig.queue.take(id)
	if len(tasks) != 1 || tasks[0].Type != engine.TaskTypeNodeResume {
		t.Fatalf("the signal queued %+v, want exactly one resume task", tasks)
	}
	lease, err := rig.eng.BuildTaskLease(ctx, tasks[0])
	if err != nil {
		t.Fatalf("BuildTaskLease(resume) error = %v", err)
	}
	commit, err := rig.eng.CommitTaskResultWithOutcome(ctx, lease, engine.TaskResult{
		Output: &types.Output{Data: map[string]any{"woken": true}},
	})
	if err != nil || commit != engine.CommitOutcomeAccepted {
		t.Fatalf("CommitTaskResultWithOutcome(resume) = (%q, %v), want accepted", commit, err)
	}
	if got := rig.snapshot(ctx, id).Status; got != types.ExecutionStatusSuccess {
		t.Fatalf("execution after the signal side finished = %q, want success", got)
	}
	if n := rig.zcard(executionDeadlineIndexKey(namespace.Default)); n != 0 {
		t.Fatalf("deadline index holds %d members after the success, want it withdrawn", n)
	}

	// The expiry arrives after the end.
	outcome, err := rig.eng.ExpireExecution(ctx, id)
	if err != nil || outcome != engine.ExpireOutcomeAlreadyTerminal {
		t.Fatalf("ExpireExecution() = (%q, %v), want already_terminal", outcome, err)
	}
	// An expiry that decided before the end writes after it: the script refuses.
	if err := rig.state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusTimeout, engine.ExecutionDeadlineExceededError); err != nil {
		t.Fatalf("UpdateExecutionStatus(timeout) error = %v", err)
	}

	snap := rig.snapshot(ctx, id)
	if snap.Status != types.ExecutionStatusSuccess || snap.Error != "" {
		t.Fatalf("execution = %q %q after the late expiry, want success with no reason", snap.Status, snap.Error)
	}
	node, err := rig.state.GetNode(ctx, id, "park")
	if err != nil || node == nil || node.Status != types.NodeStatusSuccess {
		t.Fatalf("woken node = (%+v, %v), want success", node, err)
	}
	rig.requireAnnouncedOnly(0, id, types.ExecutionStatusSuccess)
	if got := rig.hooks.completionsOf(id); len(got) != 1 {
		t.Fatalf("completion announcements = %v, want exactly [success]", got)
	}
	if left := rig.queue.take(id); len(left) != 0 {
		t.Fatalf("the late expiry left %d tasks queued: %+v", len(left), left)
	}
	rig.requireDrained(0, g.Name(), id)
}

// TestSignalInTheWindowAfterTheTimeoutWriteQueuesNothingOnRedis pins the guard
// the replay above cannot reach. ExpireExecution writes the timeout status first
// and cancels the suspended nodes after, so for a moment the execution is timed
// out while its node is still listed as suspended and a waiter key is still
// there for a signal to consume. Once the cascade has run the node is no longer
// a resume target, and a late signal is harmless for that reason alone; in this
// window it is harmless only because the engine refuses to resume a timed-out
// execution. The window is made deterministic by writing the timeout status
// without the cascade.
func TestSignalInTheWindowAfterTheTimeoutWriteQueuesNothingOnRedis(t *testing.T) {
	ctx := context.Background()
	rig := newDeadlineRaceRig(t)
	g := raceGraph(t, "deadline-race-park", 0, types.NodeDef{Name: "park", Type: "test.race_park"})
	id := rig.submit(ctx, g)
	rig.parkOnSignal(ctx, id)

	if err := rig.state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusTimeout, engine.ExecutionDeadlineExceededError); err != nil {
		t.Fatalf("UpdateExecutionStatus(timeout) error = %v", err)
	}
	nodes, err := rig.state.ListSuspendedNodes(ctx, id)
	if err != nil || len(nodes) != 1 {
		t.Fatalf("ListSuspendedNodes after the timeout write = (%v, %v), want the parked node still listed", nodes, err)
	}
	if n := rig.existsWaiter(ctx, id); n != 1 {
		t.Fatalf("waiter:go exists = %d after the timeout write, want the parked node still waiting", n)
	}
	outboxBefore := rig.outboxSize(ctx, id)

	if err := rig.eng.DeliverSignal(ctx, id, "go", map[string]any{"by": "window"}); err != nil {
		t.Fatalf("DeliverSignal() error = %v, want a silent no-op", err)
	}
	if queued := rig.queue.take(id); len(queued) != 0 {
		t.Fatalf("a signal in the window queued %d tasks on a timed-out execution: %+v", len(queued), queued)
	}
	if added := rig.outboxSize(ctx, id) - outboxBefore; added != 0 {
		t.Fatalf("a signal in the window added %d outbox entries on a timed-out execution", added)
	}
	snap := rig.snapshot(ctx, id)
	if snap.Status != types.ExecutionStatusTimeout || snap.Error != engine.ExecutionDeadlineExceededError {
		t.Fatalf("execution = %q %q after the signal, want it to stay timeout for its deadline", snap.Status, snap.Error)
	}
	node, err := rig.state.GetNode(ctx, id, "park")
	if err != nil || node == nil || node.Status == types.NodeStatusSuccess || node.Status == types.NodeStatusRunning {
		t.Fatalf("parked node = (%+v, %v) after the signal, want it neither resumed nor completed", node, err)
	}

	// The cascade the writer never ran is still repaired afterwards.
	outcome, err := rig.eng.ExpireExecution(ctx, id)
	if err != nil || outcome != engine.ExpireOutcomeResumed {
		t.Fatalf("ExpireExecution() = (%q, %v), want resumed", outcome, err)
	}
	rig.requireNodesCanceled(ctx, 0, id, []string{"park"})
	rig.requireDrained(0, g.Name(), id)
}

// TestResumeQueuedBeforeTheTimeoutIsRefusedAfterItOnRedis fixes the one
// interleaving of the replay above that it reaches only in a few rounds in a
// thousand: the signal wins the waiter and its resume is already in the queue
// when the expiry writes timeout. That resume was queued while the execution was
// alive, so queueing it was right; what must hold is that a worker presented
// with it afterwards is refused, and that nothing further is queued or left in
// the outbox when the same signal arrives again.
func TestResumeQueuedBeforeTheTimeoutIsRefusedAfterItOnRedis(t *testing.T) {
	ctx := context.Background()
	rig := newDeadlineRaceRig(t)
	g := raceGraph(t, "deadline-race-park", 0, types.NodeDef{Name: "park", Type: "test.race_park"})
	id := rig.submit(ctx, g)
	rig.parkOnSignal(ctx, id)

	if err := rig.eng.DeliverSignal(ctx, id, "go", map[string]any{"by": "first"}); err != nil {
		t.Fatalf("DeliverSignal() error = %v", err)
	}
	queued := rig.queue.take(id)
	if len(queued) != 1 || queued[0].Type != engine.TaskTypeNodeResume {
		t.Fatalf("the signal queued %+v, want exactly one resume task", queued)
	}

	outcome, err := rig.eng.ExpireExecution(ctx, id)
	if err != nil || outcome != engine.ExpireOutcomeExpired {
		t.Fatalf("ExpireExecution() = (%q, %v), want expired", outcome, err)
	}
	if _, err := rig.eng.BuildTaskLease(ctx, queued[0]); !errors.Is(err, engine.ErrExecutionInactive) {
		t.Fatalf("a resume queued before the timeout was leased after it: error = %v, want ErrExecutionInactive", err)
	}
	// A lease that is minted and then released because its input cannot be built
	// answers ErrExecutionInactive too, but the release puts the task back in the
	// queue. Refusing the resume has to leave nothing behind.
	if requeued := rig.queue.take(id); len(requeued) != 0 {
		t.Fatalf("refusing a resume after the timeout requeued %d tasks: %+v", len(requeued), requeued)
	}

	outboxBefore := rig.outboxSize(ctx, id)
	if err := rig.eng.DeliverSignal(ctx, id, "go", map[string]any{"by": "again"}); err != nil {
		t.Fatalf("second DeliverSignal() error = %v, want a silent no-op", err)
	}
	if left := rig.queue.take(id); len(left) != 0 {
		t.Fatalf("a signal after the timeout queued %d tasks: %+v", len(left), left)
	}
	if added := rig.outboxSize(ctx, id) - outboxBefore; added != 0 {
		t.Fatalf("a signal after the timeout added %d outbox entries", added)
	}
	snap := rig.snapshot(ctx, id)
	if snap.Status != types.ExecutionStatusTimeout || snap.Error != engine.ExecutionDeadlineExceededError {
		t.Fatalf("execution = %q %q, want timeout for its deadline", snap.Status, snap.Error)
	}
	rig.requireAnnouncedOnly(0, id, types.ExecutionStatusTimeout)
	rig.requireDrained(0, g.Name(), id)
}

// TestResumeHeldByAWorkerIsRefusedAgainAfterTheTimeoutOnRedis fixes the
// interleaving the replay above reaches only by chance, and in which the engine's
// own execution-status fence in BuildTaskLease is the only thing that names the
// refusal. The signal wakes the node, a worker leases the resume (the node is
// running on a live lease), and then the deadline expires the execution. The same
// resume task reaching BuildTaskLease again afterwards, as a redelivery would,
// has to be refused as an inactive execution.
//
// The store refuses that second acquire on its own, because the execution is
// terminal and the node still holds its lease; with the engine's fence out, the
// refusal is classified from the node's live lease as ErrLeaseAlreadyActive, a
// worker-side retry signal rather than a final answer. The earlier tests cancel
// the node before the resume arrives, so the node-terminal fence answers there
// and the classification never reaches this path.
func TestResumeHeldByAWorkerIsRefusedAgainAfterTheTimeoutOnRedis(t *testing.T) {
	ctx := context.Background()
	rig := newDeadlineRaceRig(t)
	g := raceGraph(t, "deadline-race-park", 0, types.NodeDef{Name: "park", Type: "test.race_park"})
	id := rig.submit(ctx, g)
	rig.parkOnSignal(ctx, id)

	if err := rig.eng.DeliverSignal(ctx, id, "go", map[string]any{"by": "signal"}); err != nil {
		t.Fatalf("DeliverSignal() error = %v", err)
	}
	tasks := rig.queue.take(id)
	if len(tasks) != 1 || tasks[0].Type != engine.TaskTypeNodeResume {
		t.Fatalf("the signal queued %+v, want exactly one resume task", tasks)
	}
	resume := tasks[0]
	if _, err := rig.eng.BuildTaskLease(ctx, resume); err != nil {
		t.Fatalf("BuildTaskLease(resume) error = %v, want the worker to hold the lease", err)
	}
	node, err := rig.state.GetNode(ctx, id, "park")
	if err != nil || node == nil || node.Status != types.NodeStatusRunning || node.LeaseToken == "" {
		t.Fatalf("woken node = (%+v, %v), want running on a live lease", node, err)
	}

	outcome, err := rig.eng.ExpireExecution(ctx, id)
	if err != nil || outcome != engine.ExpireOutcomeExpired {
		t.Fatalf("ExpireExecution() = (%q, %v), want expired", outcome, err)
	}
	node, err = rig.state.GetNode(ctx, id, "park")
	if err != nil || node == nil || node.Status != types.NodeStatusRunning || node.LeaseToken == "" {
		t.Fatalf("woken node = (%+v, %v) after the expiry, want it still running on its lease "+
			"so that only the execution fence can answer", node, err)
	}

	_, err = rig.eng.BuildTaskLease(ctx, resume)
	if !errors.Is(err, engine.ErrExecutionInactive) {
		t.Fatalf("the held resume leased again after the timeout: error = %v, want ErrExecutionInactive", err)
	}
	if errors.Is(err, engine.ErrLeaseAlreadyActive) {
		t.Fatalf("the held resume was refused as a live lease, not as an inactive execution: error = %v", err)
	}
	if left := rig.queue.take(id); len(left) != 0 {
		t.Fatalf("refusing the held resume queued %d tasks: %+v", len(left), left)
	}
	snap := rig.snapshot(ctx, id)
	if snap.Status != types.ExecutionStatusTimeout || snap.Error != engine.ExecutionDeadlineExceededError {
		t.Fatalf("execution = %q %q, want timeout for its deadline", snap.Status, snap.Error)
	}
}
