package local

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine"
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
	r.state.mu.Lock()
	waiters := len(r.state.suspended)
	r.state.mu.Unlock()
	if waiters != 1 {
		r.t.Fatalf("waiters after the park = %d, want 1", waiters)
	}
}

// TestExpireRacingASignalNeverResumesATimedOutExecution replays the race
// between the deadline sweep and the signal that wakes the execution's only
// suspended node. The woken node is carried all the way: the resume task the
// signal queued is leased and committed on the same goroutine that delivered
// the signal, so the two sides really contend for the execution rather than for
// a queue entry.
//
// Success and timeout are both legitimate endings, because the deadline is
// already behind the clock. What is not legitimate is a timed-out execution
// that a woken node still advances: it must stay timeout, its node must not be
// successful, and a resume task presented after the fact must be refused.
func TestExpireRacingASignalNeverResumesATimedOutExecution(t *testing.T) {
	ctx := context.Background()
	var succeeded, timedOut int
	for round := 0; round < raceRounds(); round++ {
		rig := newDeadlineRaceRig(t)
		g := raceGraph(t, "deadline-race-park", 0, types.NodeDef{Name: "park", Type: "test.race_park"})
		id := rig.submit(ctx, g)
		rig.parkOnSignal(ctx, id)

		var signalErr, expireErr error
		var outcome engine.ExpireOutcome
		var commitOutcome engine.CommitOutcome
		raceTogether(
			func() {
				if signalErr = rig.eng.DeliverSignal(ctx, id, "go", map[string]any{"by": "race"}); signalErr != nil {
					return
				}
				// What a queue worker does with the resume the signal left behind.
				for _, task := range rig.queue.take(id) {
					lease, err := rig.eng.BuildTaskLease(ctx, task)
					if err != nil {
						if !errors.Is(err, engine.ErrExecutionInactive) {
							signalErr = err
						}
						continue
					}
					commitOutcome, signalErr = rig.eng.CommitTaskResultWithOutcome(ctx, lease, engine.TaskResult{
						Output: &types.Output{Data: map[string]any{"woken": true}},
					})
				}
			},
			func() { outcome, expireErr = rig.eng.ExpireExecution(ctx, id) },
		)
		if signalErr != nil {
			t.Fatalf("round %d: signal and resume error = %v", round, signalErr)
		}
		if expireErr != nil {
			t.Fatalf("round %d: ExpireExecution() error = %v", round, expireErr)
		}

		snap := rig.snapshot(ctx, id)
		node, err := rig.state.GetNode(ctx, id, "park")
		if err != nil || node == nil {
			t.Fatalf("round %d: GetNode(park) = (%+v, %v)", round, node, err)
		}
		switch snap.Status {
		case types.ExecutionStatusSuccess:
			succeeded++
			if node.Status != types.NodeStatusSuccess {
				t.Fatalf("round %d: execution succeeded but the woken node is %q", round, node.Status)
			}
			if commitOutcome != engine.CommitOutcomeAccepted {
				t.Fatalf("round %d: execution succeeded but the resume commit was %q", round, commitOutcome)
			}
		case types.ExecutionStatusTimeout:
			timedOut++
			if snap.Error != engine.ExecutionDeadlineExceededError {
				t.Fatalf("round %d: timeout reason = %q, want %q", round, snap.Error, engine.ExecutionDeadlineExceededError)
			}
			if outcome != engine.ExpireOutcomeExpired {
				t.Fatalf("round %d: the expiry won but ExpireExecution reported %q, want expired", round, outcome)
			}
			if node.Status == types.NodeStatusSuccess {
				t.Fatalf("round %d: execution timed out but the woken node completed", round)
			}
			if commitOutcome == engine.CommitOutcomeAccepted {
				t.Fatalf("round %d: execution timed out but a resume commit was accepted", round)
			}
			// A resume that reaches a worker after the timeout is refused outright.
			late := &engine.Task{
				ExecutionID: id, NodeName: "park", NodeIdx: 0, ActivationID: 1,
				Type:    engine.TaskTypeNodeResume,
				Payload: &types.SignalPayload{Triggered: types.SignalReceived, Name: "go"},
			}
			if _, err := rig.eng.BuildTaskLease(ctx, late); !errors.Is(err, engine.ErrExecutionInactive) {
				t.Fatalf("round %d: late resume lease error = %v, want ErrExecutionInactive", round, err)
			}
			if after := rig.snapshot(ctx, id); after.Status != types.ExecutionStatusTimeout {
				t.Fatalf("round %d: execution = %q after a late resume, want it to stay timeout", round, after.Status)
			}
		default:
			t.Fatalf("round %d: execution ended %q, want success or timeout", round, snap.Status)
		}
		rig.requireAnnouncedOnly(round, id, snap.Status)
		rig.requireDrained(round)
	}
	t.Logf("%d rounds: %d ended success, %d ended timeout", raceRounds(), succeeded, timedOut)
}
