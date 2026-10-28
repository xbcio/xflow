package local

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// TestExpireRacingTheLastNodeCommitSettlesOneTerminalState replays the race
// between an execution's last node committing its success and the deadline
// sweep expiring that same execution, on a real Engine over the real store. The
// deadline is already behind the clock, so either side may legitimately win;
// what must hold either way is that the execution ends in exactly one of the two
// terminal states, announces only that one, and leaves nothing behind.
//
// The workflow carries a concurrency limit of one and a second execution queued
// behind the first. That is what makes a leaked slot or queue place visible: the
// follower can only run if the first execution's end handed its slot on.
func TestExpireRacingTheLastNodeCommitSettlesOneTerminalState(t *testing.T) {
	ctx := context.Background()
	var succeeded, timedOut int
	for round := 0; round < raceRounds(); round++ {
		rig := newDeadlineRaceRig(t)
		g := raceWorkGraph(t, 1)
		first := rig.submit(ctx, g)
		follower := rig.submit(ctx, g)
		lease := rig.lease(ctx, first)

		var commitErr, expireErr error
		var outcome engine.ExpireOutcome
		raceTogether(
			func() {
				commitErr = rig.eng.CommitTaskResult(ctx, lease, engine.TaskResult{
					Output: &types.Output{Data: map[string]any{"ok": true}},
				})
			},
			func() { outcome, expireErr = rig.eng.ExpireExecution(ctx, first) },
		)
		if commitErr != nil {
			t.Fatalf("round %d: CommitTaskResult() error = %v", round, commitErr)
		}
		if expireErr != nil {
			t.Fatalf("round %d: ExpireExecution() error = %v", round, expireErr)
		}

		snap := rig.snapshot(ctx, first)
		switch snap.Status {
		case types.ExecutionStatusSuccess:
			succeeded++
			if snap.Error != "" {
				t.Fatalf("round %d: a successful execution carries the reason %q", round, snap.Error)
			}
			if outcome != engine.ExpireOutcomeAlreadyTerminal {
				t.Fatalf("round %d: the commit won but ExpireExecution reported %q, want already_terminal", round, outcome)
			}
		case types.ExecutionStatusTimeout:
			timedOut++
			if snap.Error != engine.ExecutionDeadlineExceededError {
				t.Fatalf("round %d: timeout reason = %q, want %q", round, snap.Error, engine.ExecutionDeadlineExceededError)
			}
			if outcome != engine.ExpireOutcomeExpired {
				t.Fatalf("round %d: the expiry won but ExpireExecution reported %q, want expired", round, outcome)
			}
		default:
			t.Fatalf("round %d: execution ended %q, want success or timeout", round, snap.Status)
		}
		rig.requireAnnouncedOnly(round, first, snap.Status)

		// The first execution's end must have handed its slot to the follower,
		// which is then running and no longer in line.
		if got := rig.snapshot(ctx, follower).Status; got != types.ExecutionStatusRunning {
			t.Fatalf("round %d: follower = %q after the first execution ended %q, want it promoted to running",
				round, got, snap.Status)
		}
		if pos, err := rig.state.ConcurrencyQueuePosition(ctx, follower); err != nil || pos != 0 {
			t.Fatalf("round %d: follower queue position = (%d, %v), want 0", round, pos, err)
		}
		rig.state.mu.Lock()
		holders := len(rig.state.slotHolders)
		_, firstHolds := rig.state.slotHolders[first]
		rig.state.mu.Unlock()
		if holders != 1 || firstHolds {
			t.Fatalf("round %d: slot holders = %d (first holds: %v), want only the follower", round, holders, firstHolds)
		}

		// Release the follower the same way, so that what remains is exactly what
		// the first execution should have left: nothing.
		if _, err := rig.eng.ExpireExecution(ctx, follower); err != nil {
			t.Fatalf("round %d: ExpireExecution(follower) error = %v", round, err)
		}
		rig.requireDrained(round)
	}
	t.Logf("%d rounds: %d ended success, %d ended timeout", raceRounds(), succeeded, timedOut)
}
