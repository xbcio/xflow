package control

import (
	"context"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
)

// DefaultSweepPeriod is how often the sweeper scans for expired leases when
// LeaseSweeperConfig.Period is unset.
const DefaultSweepPeriod = 10 * time.Second

// LeaseReclaimer is the subset of *engine.Engine that the sweeper needs.
// Extracted as an interface so tests can fake it without spinning up a real
// engine + state store.
type LeaseReclaimer interface {
	ReclaimLease(ctx context.Context, lease engine.ExpiredLease) (bool, error)
}

// LeaseLister is the subset of engine.StateStore used by the sweeper to find
// candidates for reclamation. The full StateStore interface satisfies this
// shape implicitly.
type LeaseLister interface {
	ListExpiredLeases(ctx context.Context, before time.Time) ([]engine.ExpiredLease, error)
}

// LeaseSweeper periodically reclaims task leases whose runner crashed
// mid-execute. The engine writes IssuedAt+TTL on every lease it hands out; the
// sweeper scans for leases whose deadline has passed, revokes them atomically
// (token-fenced so a racing commit still wins), and re-enqueues the task so a
// healthy runner can pick it up.
type LeaseSweeper struct {
	state     LeaseLister
	engine    LeaseReclaimer
	period    time.Duration
	grace     time.Duration
	log       engine.Logger
	observer  SweepObserver
	elector   backend.LeaderElector
	clock     func() time.Time
	sleepFunc func(context.Context, time.Duration) error
}

// SweepObserver receives lease-sweep outcomes so observability layers can
// emit metrics. All methods must be non-blocking.
type SweepObserver interface {
	OnSweepReclaim(execID, nodeName string, ageMs int64)
	OnSweepRace(execID, nodeName string)
	OnSweepError(execID, nodeName string, err error)
}

// LeaseSweeperConfig configures a sweeper.
type LeaseSweeperConfig struct {
	// Period is the interval between sweep scans. Defaults to DefaultSweepPeriod.
	Period time.Duration
	// Grace is added to each lease deadline before it is considered expired.
	// Keeps a slow but still-alive runner from being preempted by clock skew.
	Grace time.Duration
	// Logger receives structured info/error messages. Optional.
	Logger engine.Logger
	// Observer receives sweep outcomes. Optional.
	Observer SweepObserver
	// Elector gates leader-only execution: when set and IsLeader() is false,
	// SweepOnce is a no-op. Nil means "always run" (backward-compatible
	// single-replica default).
	Elector backend.LeaderElector
}

// NewLeaseSweeper builds a sweeper bound to the given state store and engine.
// Both arguments are required; clock/sleep are wired to time.Now / context-
// aware sleeps and can be overridden in tests via the unexported helpers in
// this package.
func NewLeaseSweeper(state LeaseLister, eng LeaseReclaimer, cfg LeaseSweeperConfig) *LeaseSweeper {
	if cfg.Period <= 0 {
		cfg.Period = DefaultSweepPeriod
	}
	return &LeaseSweeper{
		state:     state,
		engine:    eng,
		period:    cfg.Period,
		grace:     cfg.Grace,
		log:       cfg.Logger,
		observer:  cfg.Observer,
		elector:   cfg.Elector,
		clock:     func() time.Time { return time.Now().UTC() },
		sleepFunc: sleepWithContext,
	}
}

// Run drives the sweep loop until ctx is canceled. Blocks the caller; spawn it
// in a goroutine.
func (s *LeaseSweeper) Run(ctx context.Context) {
	for {
		if err := s.sleepFunc(ctx, s.period); err != nil {
			return
		}
		s.SweepOnce(ctx)
	}
}

// SweepOnce executes exactly one sweep pass. Returns the number of leases the
// sweeper successfully reclaimed. Exported so tests and admin tooling can
// trigger a sweep without waiting for the next tick.
func (s *LeaseSweeper) SweepOnce(ctx context.Context) int {
	if s.elector != nil && !s.elector.IsLeader() {
		return 0
	}
	before := s.clock().Add(-s.grace)
	expired, err := s.state.ListExpiredLeases(ctx, before)
	if err != nil {
		if s.log != nil {
			s.log.Error("list expired leases", "err", err)
		}
		return 0
	}
	reclaimed := 0
	for _, lease := range expired {
		select {
		case <-ctx.Done():
			return reclaimed
		default:
		}
		ok, err := s.engine.ReclaimLease(ctx, lease)
		if err != nil {
			if s.log != nil {
				s.log.Error("reclaim lease",
					"exec", string(lease.ExecutionID),
					"node", lease.NodeName,
					"err", err,
				)
			}
			if s.observer != nil {
				s.observer.OnSweepError(string(lease.ExecutionID), lease.NodeName, err)
			}
			continue
		}
		if !ok {
			if s.observer != nil {
				s.observer.OnSweepRace(string(lease.ExecutionID), lease.NodeName)
			}
			continue
		}
		reclaimed++
		if s.observer != nil {
			ageMs := s.clock().Sub(lease.IssuedAt.Add(lease.TTL)).Milliseconds()
			s.observer.OnSweepReclaim(string(lease.ExecutionID), lease.NodeName, ageMs)
		}
	}
	return reclaimed
}

// sleepWithContext sleeps for d or returns when ctx is canceled. The error
// indicates "context canceled, stop the loop"; nil means "tick fired".
func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
