package rstate

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// raceRerunTimeout is the budget of the workflow the rerun test runs: long
// enough that an execution created by the rerun is nowhere near due, so a rerun
// that inherited its parent's overdue deadline cannot hide behind a tiny one.
const raceRerunTimeout = time.Hour

// TestExpireRacingARerunGivesTheRerunItsOwnClockOnRedis replays the race between
// the deadline sweep expiring an execution and a rerun of that same execution.
// The parent is overdue by construction (it was created two hours ago against a
// one-hour budget) while its workflow still declares a one-hour budget, so the
// execution a rerun creates is a fresh run that is not due.
//
// A rerun is refused while its parent is not yet terminal, so the rerun side
// retries until the parent's timeout write has landed. The rerun can therefore
// be accepted after the parent's terminal write but before the expiry's last
// step, the removal of the parent's deadline index member; whether a given round
// hits that window is the scheduler's choice, and the log line reports how many
// rounds were accepted inside the race. Either way the assertions are the same:
// the rerun's clock and index member must survive that removal.
func TestExpireRacingARerunGivesTheRerunItsOwnClockOnRedis(t *testing.T) {
	ctx := context.Background()
	var rerunInRace int
	for round := 0; round < raceRounds(); round++ {
		rig := newDeadlineRaceRig(t)
		g := raceGraphWithTimeout(t, "deadline-race-rerun", raceRerunTimeout, 0,
			types.NodeDef{Name: "work", Type: "test.race_work"})
		parentID := engine.NewExecutionID()
		parentCreated := time.Now().UTC().Add(-2 * time.Hour)
		parentDeadline := parentCreated.Add(raceRerunTimeout)
		if err := rig.state.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID: parentID, Graph: g, Status: types.ExecutionStatusRunning,
			CreatedAt: parentCreated, Deadline: parentDeadline,
		}); err != nil {
			t.Fatalf("round %d: CreateExecution(parent) error = %v", round, err)
		}

		var rerun engine.RerunResult
		var rerunErr, expireErr error
		var outcome engine.ExpireOutcome
		raceTogether(
			func() {
				for i := 0; i < 5000; i++ {
					rerun, rerunErr = rig.eng.Rerun(ctx, engine.RerunRequest{ExecutionID: parentID, FromNode: "work"})
					if !errors.Is(rerunErr, engine.ErrExecutionNotRerunnable) {
						return
					}
					runtime.Gosched()
				}
			},
			func() { outcome, expireErr = rig.eng.ExpireExecution(ctx, parentID) },
		)
		if expireErr != nil {
			t.Fatalf("round %d: ExpireExecution() error = %v", round, expireErr)
		}
		if outcome != engine.ExpireOutcomeExpired {
			t.Fatalf("round %d: ExpireExecution reported %q, want expired", round, outcome)
		}
		switch {
		case rerunErr == nil:
			rerunInRace++
		case errors.Is(rerunErr, engine.ErrExecutionNotRerunnable):
			// The expiry never became visible to the retry loop in time. The
			// parent is terminal now, so the rerun the race missed is made here and
			// is checked the same way.
			rerun, rerunErr = rig.eng.Rerun(ctx, engine.RerunRequest{ExecutionID: parentID, FromNode: "work"})
			if rerunErr != nil {
				t.Fatalf("round %d: Rerun() after the expiry error = %v", round, rerunErr)
			}
		default:
			t.Fatalf("round %d: Rerun() error = %v", round, rerunErr)
		}

		// The parent ended where the expiry took it, with nothing of the rerun in it.
		parent := rig.snapshot(ctx, parentID)
		if parent.Status != types.ExecutionStatusTimeout || parent.Error != engine.ExecutionDeadlineExceededError {
			t.Fatalf("round %d: parent = %q %q, want timeout %q", round, parent.Status, parent.Error, engine.ExecutionDeadlineExceededError)
		}
		rig.requireAnnouncedOnly(round, parentID, types.ExecutionStatusTimeout)

		// The rerun is a new execution with a new clock.
		if rerun.ExecutionID == "" || rerun.ExecutionID == parentID {
			t.Fatalf("round %d: rerun execution id = %q, want a new id", round, rerun.ExecutionID)
		}
		fresh := rig.snapshot(ctx, rerun.ExecutionID)
		if fresh.Status != types.ExecutionStatusRunning {
			t.Fatalf("round %d: rerun = %q %q, want running: the parent's expiry must not reach it", round, fresh.Status, fresh.Error)
		}
		if fresh.ParentID != parentID {
			t.Fatalf("round %d: rerun parent = %q, want %q", round, fresh.ParentID, parentID)
		}
		// The engine stamps the deadline when it builds the rerun; the store stamps
		// created_at when the seed script runs, a few milliseconds later. The
		// deadline is therefore the budget after created_at up to that skew.
		budget := fresh.Deadline.Sub(fresh.CreatedAt)
		if fresh.Deadline.IsZero() || budget > raceRerunTimeout || budget < raceRerunTimeout-time.Minute {
			t.Fatalf("round %d: rerun deadline = %v from created %v, want created + %v (less the stamping skew)",
				round, fresh.Deadline, fresh.CreatedAt, raceRerunTimeout)
		}
		if !fresh.CreatedAt.After(parent.CreatedAt) || !fresh.Deadline.After(parent.Deadline.Add(time.Hour)) {
			t.Fatalf("round %d: rerun clock (%v .. %v) was not restarted from the parent's (%v .. %v)",
				round, fresh.CreatedAt, fresh.Deadline, parent.CreatedAt, parent.Deadline)
		}

		// Exactly the rerun's own index member is left: the parent's was removed,
		// and removing it took nothing else with it.
		members, err := rig.rdb.ZRangeWithScores(ctx, executionDeadlineIndexKey(namespace.Default), 0, -1).Result()
		if err != nil {
			t.Fatalf("round %d: read the deadline index: %v", round, err)
		}
		if len(members) != 1 || members[0].Member != string(rerun.ExecutionID) ||
			members[0].Score != float64(fresh.Deadline.UnixMilli()) {
			t.Fatalf("round %d: deadline index = %v, want only the rerun's member %s at %d",
				round, members, rerun.ExecutionID, fresh.Deadline.UnixMilli())
		}
		if got, err := rig.eng.ExpireExecution(ctx, rerun.ExecutionID); err != nil || got != engine.ExpireOutcomeNotDue {
			t.Fatalf("round %d: ExpireExecution(rerun) = (%q, %v), want not_due", round, got, err)
		}

		// Settle the rerun the ordinary way so that what remains is nothing.
		if err := rig.eng.Cancel(ctx, rerun.ExecutionID); err != nil {
			t.Fatalf("round %d: Cancel(rerun) error = %v", round, err)
		}
		rig.requireDrained(round, g.Name(), parentID, rerun.ExecutionID)
		rig.close()
	}
	t.Logf("%d rounds: %d reruns were accepted inside the race, %d after it", raceRounds(), rerunInRace, raceRounds()-rerunInRace)
}

var _ = redis.Z{}
