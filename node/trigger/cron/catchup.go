package cron

import (
	"context"
	"log/slog"
	"time"

	cronlib "github.com/robfig/cron/v3"
	"github.com/xbcio/xflow/types"
)

// catchupWalkLimit bounds how many scheduled instants one activation examines
// while planning a catch-up.
//
// A window alone does not bound the walk, because the window is an operator's
// choice and the cadence is whatever the schedule says: the default window holds
// 3600 instants of a one-second cadence, but a window of a month holds millions
// of them, and an activation that walked all of those would hold up the trigger
// it is starting. The limit is far above anything a catch-up would actually
// fire (the cap is ten by default), so an activation that reaches it skips the
// catch-up with a warning instead of walking — firing ten instants out of three
// million is not a catch-up, it is a stampede.
const catchupWalkLimit = 100_000

// catchupPlan is what one activation does about the instants its schedule named
// while nothing was hosting it.
type catchupPlan struct {
	// policy is the misfire policy the plan was built under, carried so the
	// execution of a plan can log the reason for what it did.
	policy misfirePolicy
	// fires holds the instants to fire, oldest first. Firing in schedule order
	// keeps the order a workflow sees equal to the order the schedule named,
	// which is the only order that makes a catch-up of several daily closings
	// meaningful.
	fires []time.Time
	// missed counts the instants the plan examined and is NOT firing: the ones
	// the policy skipped and the ones the cap left out. It is what tells an
	// operator that a schedule fell behind, on a trigger that by design fires
	// none of the instants it fell behind on.
	missed int
	// watermark is the newest instant the schedule named at or before now — the
	// position the trigger's record moves to once the plan has run, whether or
	// not it fired anything. Zero when the schedule named no instant in the
	// window, in which case there is nothing to record.
	watermark time.Time
	// truncated reports that the walk hit catchupWalkLimit with instants still
	// due, so the plan is empty by design: the instants it did not examine
	// cannot be acknowledged, and the ones it did are too many to fire.
	truncated bool
}

// missedFires plans the catch-up for one activation.
//
// It is pure: the schedule, the record and the clock come in as arguments, and
// no default is read from anywhere else — which is what lets a table test pin
// DST, month boundaries, sub-minute cadences and cap truncation without moving
// the clock.
//
// last is the trigger's watermark, the last instant it acknowledged; ok says
// whether there is one. With no record the plan is empty: a trigger that has
// never acknowledged a firing has no evidence that any instant was due to
// anybody, and offering the whole window would turn every first activation into
// a burst of history.
//
// The candidates are the instants strictly after max(last, now-window) and at
// or before now. How many of them are FIRED is the policy's decision:
// skip fires none (the window is only how far the acknowledgment reaches),
// fire_once fires the newest, fire_all fires up to max of them, newest kept.
func missedFires(sched cronlib.Schedule, last time.Time, ok bool, now time.Time, cfg catchupConfig) catchupPlan {
	plan := catchupPlan{policy: cfg.policy}
	if !ok || last.IsZero() {
		return plan
	}
	from := now.Add(-cfg.window)
	if last.After(from) {
		from = last
	}
	candidates, examined, truncated := walkSchedule(sched, from, now, cfg.max)
	if truncated {
		// The walk was cut short with instants still due: nothing this plan
		// could do is a catch-up, so it does nothing and says so. The record
		// stays where it was, because the instants the walk did not reach
		// cannot be acknowledged.
		return catchupPlan{policy: cfg.policy, truncated: true}
	}
	if len(candidates) == 0 {
		return plan
	}
	// The watermark is the newest instant the schedule named, which is exactly
	// what the last candidate is — the walk cannot stop anywhere else.
	plan.watermark = candidates[len(candidates)-1]
	plan.missed = examined
	switch cfg.policy {
	case misfireFireOnce:
		plan.fires = candidates[len(candidates)-1:]
	case misfireFireAll:
		plan.fires = candidates
	}
	plan.missed -= len(plan.fires)
	return plan
}

// walkSchedule collects the scheduled instants in (from, now], keeping the
// newest max of them and counting all of them. It returns truncated=true when
// the walk hit catchupWalkLimit with instants still due.
//
// Keeping the newest and counting everything is what lets the cap be a
// catch-up decision rather than a blind spot: the operator is told how many
// instants the cap left out, and the ones kept are the ones nearest the present.
func walkSchedule(sched cronlib.Schedule, from, now time.Time, max int) (newest []time.Time, examined int, truncated bool) {
	for next := sched.Next(from); !nextAfter(next, now); next = sched.Next(next) {
		if examined == catchupWalkLimit {
			return nil, 0, true
		}
		examined++
		newest = append(newest, next)
		if len(newest) > max {
			newest = newest[1:]
		}
	}
	return newest, examined, false
}

// nextAfter reports whether next is a usable instant that is still to come at
// now. An exhausted schedule hands back the zero time instead of a next
// activation, and the zero time compares as "long past" — treating it as due
// would fire an instant from year 1 and walk forever.
func nextAfter(next, now time.Time) bool {
	return next.IsZero() || next.After(now)
}

// applyCatchup runs one plan and then moves the watermark to the position the
// plan reached.
//
// fire is the hosting path's own admission for a single firing: the two paths
// admit through different mechanisms (an embedded engine's Emit, the control
// plane's entry seed) and the plan is deliberately ignorant of which one is in
// play. The watermark moves after the firings rather than before, so a crash
// between the two leaves the instants looking unfired — which they then are —
// and the deduplicator, not the record, is what keeps the re-offer from double
// firing a moment that already ran.
func applyCatchup(ctx context.Context, in *types.TriggerActivateInput, plan catchupPlan, now time.Time, watermark types.TriggerState, fire func(cronFire)) {
	if plan.truncated {
		slog.Warn("cron trigger skipped its catch-up: the schedule names more missed instants than a catch-up examines",
			"workflow_id", in.WorkflowID,
			"node", in.NodeName,
			"limit", catchupWalkLimit)
		return
	}
	for _, scheduled := range plan.fires {
		// The observed instant of a catch-up is the activation, not the instant:
		// this firing is running now, and Time is the field that says so.
		fire(cronFire{scheduled: scheduled, observed: now, catchup: true})
	}
	if !plan.watermark.IsZero() {
		recordFiring(ctx, watermark, in, plan.watermark)
	}
	if plan.missed > 0 {
		slog.Info("cron trigger fell behind its schedule",
			"workflow_id", in.WorkflowID,
			"node", in.NodeName,
			"misfire", string(plan.policy),
			"fired", len(plan.fires),
			"missed", plan.missed,
			"last_scheduled", cronFireID(plan.watermark))
	}
}
