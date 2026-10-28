package engine

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

const (
	// DefaultDeadlineSweepBatch is how many executions one DeadlineSweep pass
	// attempts when the config names no batch.
	DefaultDeadlineSweepBatch = 100
	// DefaultDeadlineSweepFetchFactor caps the over-fetch of one pass at this
	// multiple of its batch. Candidates still backing off are listed beyond the
	// batch so they cannot hold every slot of the page; the cap keeps that read
	// bounded when many of them are.
	//
	// The cap is also the hard limit of what a sweep can ever reach. The index is
	// read from its oldest deadline and has no cursor, so a sweep sees only the
	// head of it: Batch x FetchFactor entries (1000 by default). While fewer
	// executions than that are older than a new one, however stubbornly they fail,
	// the new one is reached once enough of them are backing off (the head then
	// holds fewer than a batch of attemptable entries). At the cap or beyond, the
	// head is made of nothing but set-aside executions and a newer one is never
	// listed until some of them clear. That is a deliberate trade-off against an
	// unbounded read per pass, and the saturation warning is how it is reported.
	//
	// Measured with a batch of 100 and the default factor, one failing-forever
	// execution queued behind N others that fail forever is first attempted on
	// pass: N=150: 5th, N=500: 34th, N=800: 60th, N=900 to 999: 68th (passes 5s
	// apart; with 1s apart: 3rd, 22nd, 40th, 47th), and for N=1000 or more never
	// (not within 400 passes).
	DefaultDeadlineSweepFetchFactor = 10
	// DefaultDeadlineSweepContinueEvery is how many candidates a sweep goes
	// through between two calls of its Continue hook.
	DefaultDeadlineSweepContinueEvery = 32
)

// ExecutionExpirer is the part of the engine a deadline sweep drives to time an
// execution out. *Engine implements it.
type ExecutionExpirer interface {
	ExpireExecution(ctx context.Context, id types.ExecutionID) (ExpireOutcome, error)
}

// DeadlineSweepStop says why a sweep ended before it had gone through its page.
type DeadlineSweepStop string

const (
	// DeadlineSweepStopNone means the sweep went through its page.
	DeadlineSweepStopNone DeadlineSweepStop = ""
	// DeadlineSweepStopLeadershipLost means the Continue hook reported false.
	DeadlineSweepStopLeadershipLost DeadlineSweepStop = "leadership_lost"
	// DeadlineSweepStopCanceled means the context ended mid-sweep.
	DeadlineSweepStopCanceled DeadlineSweepStop = "canceled"
)

// DeadlineSweepResult counts what one DeadlineSweep pass did.
type DeadlineSweepResult struct {
	// Listed is how many candidates the scan returned.
	Listed int
	// Attempted is how many of them ExpireExecution was called for. It is at most
	// the batch.
	Attempted int
	// Released is how many attempts timed an execution out or finished the
	// cascade a crashed call left behind (Expired plus Resumed).
	Released int
	// Expired is how many attempts wrote the timeout themselves. A resumed
	// cascade is the same expiry finished, not a second one.
	Expired int
	// Failed is how many attempts returned an error other than
	// ErrExecutionInactive.
	Failed int
	// SkippedBackoff is how many candidates were passed over because they were
	// inside a backoff window.
	SkippedBackoff int
	// Deferred is how many attempts found the execution canceling and set it
	// aside for a window.
	Deferred int
	// Stop is why the sweep ended early, if it did.
	Stop DeadlineSweepStop
}

// DeadlineSweepLogger is the slice of a logger a DeadlineSweep reports through.
// Logger satisfies it.
type DeadlineSweepLogger interface {
	Warn(msg string, args ...any)
}

// DeadlineSweepConfig configures a DeadlineSweep.
type DeadlineSweepConfig struct {
	// Lister finds the executions past their deadline. Required.
	Lister ExecutionDeadlineExpirer
	// Expirer times an execution out. Required.
	Expirer ExecutionExpirer
	// Backoff spaces out the retries of executions that keep failing. A sweep
	// that is rebuilt between passes must be given the same Backoff each time;
	// nil gives the sweep a private one.
	Backoff *ExpireBackoff
	// Batch bounds how many executions one Sweep attempts. Zero defaults to
	// DefaultDeadlineSweepBatch.
	Batch int
	// FetchFactor caps how many candidates one Sweep reads at this multiple of
	// Batch. Zero defaults to DefaultDeadlineSweepFetchFactor.
	FetchFactor int
	// Clock returns the current time. Nil defaults to time.Now in UTC.
	Clock func() time.Time
	// Logger receives the failure and saturation warnings. Optional.
	Logger DeadlineSweepLogger
	// Continue is asked whether the sweep may go on, every ContinueEvery
	// candidates. A false answer ends the sweep with DeadlineSweepStopLeadershipLost.
	// Optional.
	Continue func() bool
	// ContinueEvery is how many candidates pass between two Continue calls. Zero
	// defaults to DefaultDeadlineSweepContinueEvery.
	ContinueEvery int
	// OnExpired is called for each execution this sweep drove into timeout, with a
	// context that carries the execution's own namespace. A resumed cascade is not
	// reported: it is the same expiry finished. Optional.
	OnExpired func(ctx context.Context, candidate DeadlineExpiredExecution)
}

// DeadlineSweep drives the executions an ExecutionDeadlineExpirer lists into
// timeout. It is the one body shared by every loop that does so: the in-process
// backend's sweeper, the embedded distributed backend's, and the control plane's
// deadline pass.
//
// The index lists the oldest deadlines first and keeps an execution listed until
// ExpireExecution acknowledges it, so an execution that cannot be handled would
// hold a slot of every page for good and starve the ones behind it. The sweep
// therefore sets such an execution aside for a growing window (ExpireBackoff),
// reads past the ones it has set aside, and attempts at most one batch.
//
// It is safe for concurrent use.
type DeadlineSweep struct {
	cfg DeadlineSweepConfig

	mu sync.Mutex
	// saturated is set once the executions set aside fill the page the sweep can
	// read, and cleared only when they fall to half of it. Backoff windows expire
	// and renew all the time, so the count hovers around the cap; clearing at the
	// cap itself would log the warning again every few passes.
	saturated bool
}

// NewDeadlineSweep returns a DeadlineSweep for cfg.
func NewDeadlineSweep(cfg DeadlineSweepConfig) *DeadlineSweep {
	if cfg.Backoff == nil {
		cfg.Backoff = NewExpireBackoff()
	}
	if cfg.Batch <= 0 {
		cfg.Batch = DefaultDeadlineSweepBatch
	}
	if cfg.FetchFactor <= 0 {
		cfg.FetchFactor = DefaultDeadlineSweepFetchFactor
	}
	if cfg.Clock == nil {
		cfg.Clock = func() time.Time { return time.Now().UTC() }
	}
	if cfg.ContinueEvery <= 0 {
		cfg.ContinueEvery = DefaultDeadlineSweepContinueEvery
	}
	return &DeadlineSweep{cfg: cfg}
}

// errDeadlineSweepUnwired is what Sweep returns when it was built without a
// lister or an expirer.
var errDeadlineSweepUnwired = errors.New("deadline sweep needs a lister and an expirer")

// DeadlineSweepKey is the backoff key of a candidate. It carries the namespace as
// well as the ID because the same execution ID may exist under two namespaces,
// and one tenant's failures must not delay another's.
func DeadlineSweepKey(candidate DeadlineExpiredExecution) string {
	return string(candidate.Namespace) + "\x00" + string(candidate.ExecutionID)
}

// deadlineSweepAlertAt reports whether a consecutive failure count is one the
// sweep escalates. Failures are retried with growing waits, so these counts land
// minutes to hours apart: a stuck execution warns a handful of times in a day
// rather than on every retry.
func deadlineSweepAlertAt(failures int) bool {
	switch failures {
	case 5, 20, 100:
		return true
	}
	return false
}

// Sweep reads one page of executions past their deadline and drives each into
// timeout. It returns what it did; the error is the scan's own, in which case no
// candidate was attempted.
//
// Every outcome ExpireExecution reports without an error is a handled candidate.
// Expired and Resumed are what the sweep released. NotDue and AlreadyTerminal are
// benign (the execution is not due, or someone else finished it), as is
// ErrExecutionInactive. Canceling and Gone are not failures either, but neither
// withdraws the entry, so it stays listed: a cancel will withdraw its own, and a
// Gone one is an entry whose execution keys are missing (the state, or the graph,
// may be a replica lag away from reappearing, so it is not acknowledged). Both are
// set aside for a window (ExpireBackoff.Defer) and cannot hold a slot of every
// page meanwhile. Any other error is a failure: it is set aside the same way and
// counted toward the consecutive-failure warning.
//
// The scan reads the batch plus however many executions are set aside, up to
// FetchFactor times the batch, and the sweep attempts at most one batch.
// Attempted counts the attempts, so Released/Attempted stays the ratio of work
// that succeeded; candidates skipped while set aside are not counted in it.
func (s *DeadlineSweep) Sweep(ctx context.Context) (DeadlineSweepResult, error) {
	var result DeadlineSweepResult
	if s.cfg.Lister == nil || s.cfg.Expirer == nil {
		return result, errDeadlineSweepUnwired
	}
	now := s.cfg.Clock()
	limit, capped := s.fetchLimit(now)
	s.mu.Lock()
	warnSaturated := capped && !s.saturated
	switch {
	case capped:
		s.saturated = true
	case limit <= s.cfg.Batch*s.cfg.FetchFactor/2:
		s.saturated = false
	}
	s.mu.Unlock()
	if warnSaturated && s.cfg.Logger != nil {
		s.cfg.Logger.Warn("execution deadline scan is saturated by executions that keep failing to expire",
			"backing_off", s.cfg.Backoff.Waiting(now),
			"batch", s.cfg.Batch,
			"fetch_limit", limit,
		)
	}
	candidates, err := s.cfg.Lister.ListDeadlineExceededExecutions(ctx, now, limit)
	if err != nil {
		return result, err
	}
	result.Listed = len(candidates)
	for i, candidate := range candidates {
		// A demoted replica must stop expiring cluster-wide executions; the new
		// leader will pick the remainder up on its own pass.
		if i > 0 && i%s.cfg.ContinueEvery == 0 && s.cfg.Continue != nil && !s.cfg.Continue() {
			result.Stop = DeadlineSweepStopLeadershipLost
			return result, nil
		}
		if ctx.Err() != nil {
			result.Stop = DeadlineSweepStopCanceled
			return result, nil
		}
		if result.Attempted >= s.cfg.Batch {
			break
		}
		key := DeadlineSweepKey(candidate)
		if !s.cfg.Backoff.Allow(key, now) {
			result.SkippedBackoff++
			continue
		}
		result.Attempted++
		// The candidate names the namespace its keys live under; the sweep's own
		// context is namespace-less because the scan spans every namespace the
		// process serves.
		execCtx := ctx
		if candidate.Namespace != "" {
			execCtx = namespace.WithNamespace(ctx, candidate.Namespace)
		}
		outcome, err := s.cfg.Expirer.ExpireExecution(execCtx, candidate.ExecutionID)
		switch {
		case err == nil:
			s.record(execCtx, candidate, key, outcome, now, &result)
		case errors.Is(err, ErrExecutionInactive):
			// The execution concluded between the list and the call.
			s.cfg.Backoff.Success(key)
		case ctx.Err() != nil:
			// The sweep is shutting down: that is not this execution's failure.
			result.Stop = DeadlineSweepStopCanceled
			return result, nil
		default:
			result.Failed++
			failures := s.cfg.Backoff.Failure(key, now)
			if s.cfg.Logger != nil && deadlineSweepAlertAt(failures) {
				s.cfg.Logger.Warn("execution keeps failing to expire past its deadline",
					"ns", string(candidate.Namespace),
					"exec", string(candidate.ExecutionID),
					"deadline", candidate.Deadline,
					"consecutive_failures", failures,
					"err", err,
				)
			}
		}
	}
	return result, nil
}

// record settles one attempt that came back without an error.
//
// Canceling and Gone leave the entry listed (ExpireExecution acknowledges neither),
// so they are set aside rather than forgotten: Success would let the same entry
// head the next page again, every round.
func (s *DeadlineSweep) record(ctx context.Context, candidate DeadlineExpiredExecution, key string, outcome ExpireOutcome, now time.Time, result *DeadlineSweepResult) {
	if outcome == ExpireOutcomeCanceling || outcome == ExpireOutcomeGone {
		s.cfg.Backoff.Defer(key, now)
		result.Deferred++
		return
	}
	s.cfg.Backoff.Success(key)
	switch outcome {
	case ExpireOutcomeExpired:
		result.Released++
		result.Expired++
		// Only the call that wrote the timeout counts as the transition; a
		// resumed cascade is the same expiry finished, not a second one.
		if s.cfg.OnExpired != nil {
			s.cfg.OnExpired(ctx, candidate)
		}
	case ExpireOutcomeResumed:
		result.Released++
	}
}

// fetchLimit is how many candidates one sweep reads: the batch it may attempt
// plus one slot per execution still inside a backoff window at now, so those
// cannot crowd the batch out. capped reports that the bound clipped the read, in
// which case set-aside executions can still take attempts from the page.
func (s *DeadlineSweep) fetchLimit(now time.Time) (limit int, capped bool) {
	limit = s.cfg.Batch + s.cfg.Backoff.Waiting(now)
	if ceiling := s.cfg.Batch * s.cfg.FetchFactor; limit > ceiling {
		return ceiling, true
	}
	return limit, false
}
