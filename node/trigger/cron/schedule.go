package cron

import (
	"sync"
	"time"

	cronlib "github.com/robfig/cron/v3"
)

// cronFire is one firing of a cron schedule.
//
// The two instants are not the same thing and must not be collapsed:
//
//   - scheduled is the instant the SCHEDULE named. It is the firing's identity —
//     the event ID and the entry-seed fire key are both built from it — so two
//     replicas evaluating one schedule agree on it no matter when each of them
//     got around to running the callback.
//   - observed is when the callback actually ran. It is what a workflow reads
//     when it asks when this ran, and it is later than scheduled by however long
//     the process took to get to the job.
//   - catchup marks a firing of an instant that passed while this schedule was
//     hosted by nobody: it is an admission the trigger made at activation, not
//     one its own clock made. The instant is still the schedule's, so a catch-up
//     and a punctual firing of the same moment share an identity and cannot both
//     run.
type cronFire struct {
	scheduled time.Time
	observed  time.Time
	catchup   bool
}

// scheduleCursor derives each firing's scheduled instant from the schedule
// itself.
//
// The alternative — truncating the observation instant, which is what this
// replaced — is wrong in three ways that all show up as the same failure, a
// firing that never runs:
//
//   - A callback that lands just after a boundary (a stalled process, a slow
//     scheduler) is attributed to the NEXT minute, so the minute that was
//     actually due is never admitted and the next one is admitted under a
//     duplicate identity.
//   - A sub-minute schedule (@every 1s, the fastest the standard parser
//     expresses) truncates to one identity per minute, so the deduper and the
//     control plane's admission drop 59 of every 60 firings.
//   - Two replicas that observe the same firing at different moments — one
//     delayed past the boundary — derive different identities and both run.
//
// cronlib itself does not hand the scheduled instant to the job, and it
// reschedules from the wake time (so a firing missed while the process was
// stalled is not re-run by the library). The cursor therefore walks the schedule
// forward from the instant it last accounted for, and answers with the latest
// scheduled instant at or before the observation: the firing that is due NOW,
// which is the one every replica that is awake will also be running.
type scheduleCursor struct {
	sched cronlib.Schedule
	// mu guards prev. cronlib runs each job in its own goroutine, so two
	// callbacks can overlap when a job outlives its schedule's cadence.
	mu sync.Mutex
	// prev is the instant the walk has already accounted for: the activation
	// instant before the first firing, then each scheduled instant as it fires.
	prev time.Time
}

// newScheduleCursor opens a cursor for a schedule taking effect at now: the
// first firing it will report is the schedule's first instant after now.
func newScheduleCursor(sched cronlib.Schedule, now time.Time) *scheduleCursor {
	return &scheduleCursor{sched: sched, prev: now}
}

// next returns the firing a callback observed at the given instant is running.
func (c *scheduleCursor) next(observed time.Time) cronFire {
	c.mu.Lock()
	defer c.mu.Unlock()
	scheduled := c.sched.Next(c.prev)
	for {
		after := c.sched.Next(scheduled)
		if after.After(observed) {
			break
		}
		scheduled = after
	}
	c.prev = scheduled
	return cronFire{scheduled: scheduled, observed: observed}
}

// firingWindow is the distance from one firing to the next, and it is what
// bounds that firing's admission: a retry that outlived the next firing would
// let two firings contend for one retry budget, and the later one is the one an
// operator would rather have on time.
//
// A schedule with no next instant gives no upper bound, so the schedule's own
// tightest expressible cadence stands in for one: a minute for a five-field
// expression. It is a floor, not a measurement — an exhausted schedule never
// fires again, so nothing is actually waiting on it.
func firingWindow(sched cronlib.Schedule, scheduled time.Time) time.Duration {
	if d := sched.Next(scheduled).Sub(scheduled); d > 0 {
		return d
	}
	return time.Minute
}
