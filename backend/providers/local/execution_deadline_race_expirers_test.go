package local

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// raceExpirers is how many sweepers expire the same execution at once. Several
// engines in a cluster can list the same overdue entry in the same tick, so more
// than two is the realistic worst case.
const raceExpirers = 8

// TestConcurrentExpirersSettleOneTimeoutWithoutError starts raceExpirers
// ExpireExecution calls on one overdue execution at the same instant, on an
// execution that has something to cascade over (a suspended node). Each call
// must come back without an error; the execution must end timeout exactly once
// as a state; and the calls' answers must be the ones a loser is allowed to
// give: at least one call expired it, and every other call either expired it
// too (two callers can both see their own write land), finished the cascade the
// first left behind (resumed), or found it already concluded.
//
// Completion is announced at least once and only ever as timeout. It is not
// asserted to be exactly once: two concurrent expirers can each observe their
// own write as the winning one, the narrow window ExpireExecution documents.
func TestConcurrentExpirersSettleOneTimeoutWithoutError(t *testing.T) {
	ctx := context.Background()
	for round := 0; round < raceRounds(); round++ {
		rig := newDeadlineRaceRig(t)
		g := raceGraph(t, "deadline-race-expirers", 0, types.NodeDef{Name: "park", Type: "test.race_park"})
		id := rig.submit(ctx, g)
		rig.parkOnSignal(ctx, id)

		outcomes := make([]engine.ExpireOutcome, raceExpirers)
		errs := make([]error, raceExpirers)
		fns := make([]func(), raceExpirers)
		for i := range fns {
			fns[i] = func() { outcomes[i], errs[i] = rig.eng.ExpireExecution(ctx, id) }
		}
		raceTogether(fns...)

		expired := 0
		for i, outcome := range outcomes {
			if errs[i] != nil {
				t.Fatalf("round %d: expirer %d error = %v", round, i, errs[i])
			}
			switch outcome {
			case engine.ExpireOutcomeExpired:
				expired++
			case engine.ExpireOutcomeResumed, engine.ExpireOutcomeAlreadyTerminal,
				engine.ExpireOutcomeGone, engine.ExpireOutcomeCanceling:
			default:
				t.Fatalf("round %d: expirer %d answered %q, want expired, resumed, already_terminal, gone or canceling", round, i, outcome)
			}
		}
		if expired == 0 {
			t.Fatalf("round %d: no expirer reported expired: %v", round, outcomes)
		}

		snap := rig.snapshot(ctx, id)
		if snap.Status != types.ExecutionStatusTimeout || snap.Error != engine.ExecutionDeadlineExceededError {
			t.Fatalf("round %d: execution = %q %q, want timeout %q", round, snap.Status, snap.Error, engine.ExecutionDeadlineExceededError)
		}
		rig.requireAnnouncedOnly(round, id, types.ExecutionStatusTimeout)
		node, err := rig.state.GetNode(ctx, id, "park")
		if err != nil || node == nil || node.Status != types.NodeStatusCanceled {
			t.Fatalf("round %d: suspended node = (%+v, %v), want canceled by the cascade", round, node, err)
		}
		rig.requireDrained(round)
	}
}
