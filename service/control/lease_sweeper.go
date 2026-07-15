package control

import (
	"context"
	"sync"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
)

// DefaultSweepPeriod is how often the sweeper scans for expired leases when
// LeaseSweeperConfig.Period is unset.
const DefaultSweepPeriod = 10 * time.Second

// DefaultLeaseRepairPeriod bounds Redis lease-index reconciliation frequency.
// It is intentionally less frequent than lease expiry scans because normal
// acquire/revoke/commit paths already maintain the index atomically.
const DefaultLeaseRepairPeriod = time.Minute

// LeaseLister is the subset of engine.StateStore used by the sweeper to find
// candidates for reclamation. The full StateStore interface satisfies this
// shape implicitly.
type LeaseLister interface {
	ListExpiredLeases(ctx context.Context, before time.Time) ([]engine.ExpiredLease, error)
}

// LeaseIndexRepairer is an optional backend capability that reconciles lease
// expiry discovery indexes from authoritative running-node metadata. Backends
// without a secondary index do not implement it.
type LeaseIndexRepairer interface {
	RepairLeaseIndex(ctx context.Context, limit int) (reconciled int, err error)
}

// LeaseSweeper periodically reclaims task leases whose runner crashed
// mid-execute. The engine writes IssuedAt+TTL on every lease it hands out; the
// sweeper scans for leases whose deadline has passed, revokes them atomically
// (token-fenced so a racing commit still wins), and re-enqueues the task so a
// healthy runner can pick it up.
type LeaseSweeper struct {
	state          LeaseLister
	engine         execution.Engine
	period         time.Duration
	grace          time.Duration
	log            engine.Logger
	observer       SweepObserver
	timingObserver SweepTimingObserver
	elector        backend.LeaderElector
	clock          func() time.Time
	sleepFunc      func(context.Context, time.Duration) error

	repairPeriod time.Duration
	repairBatch  int
	repairMu     sync.Mutex
	lastRepair   time.Time
}

// SweepObserver receives lease-sweep outcomes so observability layers can
// emit metrics. All methods must be non-blocking.
type SweepObserver interface {
	OnSweepReclaim(execID, nodeName string, ageMs int64)
	OnSweepRace(execID, nodeName string)
	OnSweepError(execID, nodeName string, err error)
}

// SweepTimingObserver is an optional extension implemented by observability
// adapters that need bounded lease scan, reclaim, and repair latency metrics.
// LeaseSweeper discovers it from LeaseSweeperConfig.Observer without expanding
// the backwards-compatible SweepObserver contract.
type SweepTimingObserver interface {
	OnSweepListExpired(candidates int, elapsed time.Duration, err error)
	OnSweepReclaimResult(result string, elapsed time.Duration)
	OnSweepRepair(reconciled int, elapsed time.Duration, err error)
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
	// LeaseRepairPeriod controls optional secondary-index reconciliation.
	// Zero defaults to DefaultLeaseRepairPeriod.
	LeaseRepairPeriod time.Duration
	// LeaseRepairBatch bounds one reconciliation scan. Zero defaults to 256.
	LeaseRepairBatch int
}

// NewLeaseSweeper builds a sweeper bound to the given state store and engine.
// Both arguments are required; clock/sleep are wired to time.Now / context-
// aware sleeps and can be overridden in tests via the unexported helpers in
// this package.
func NewLeaseSweeper(state LeaseLister, eng execution.Engine, cfg LeaseSweeperConfig) *LeaseSweeper {
	if cfg.Period <= 0 {
		cfg.Period = DefaultSweepPeriod
	}
	if cfg.LeaseRepairPeriod <= 0 {
		cfg.LeaseRepairPeriod = DefaultLeaseRepairPeriod
	}
	if cfg.LeaseRepairBatch <= 0 {
		cfg.LeaseRepairBatch = 256
	}
	var timingObserver SweepTimingObserver
	if observer, ok := cfg.Observer.(SweepTimingObserver); ok {
		timingObserver = observer
	}
	return &LeaseSweeper{
		state:          state,
		engine:         eng,
		period:         cfg.Period,
		grace:          cfg.Grace,
		log:            cfg.Logger,
		observer:       cfg.Observer,
		timingObserver: timingObserver,
		elector:        cfg.Elector,
		clock:          func() time.Time { return time.Now().UTC() },
		sleepFunc:      sleepWithContext,
		repairPeriod:   cfg.LeaseRepairPeriod,
		repairBatch:    cfg.LeaseRepairBatch,
	}
}

// Run drives the sweep loop until ctx is canceled. Blocks the caller; spawn it
// in a goroutine.
func (s *LeaseSweeper) Run(ctx context.Context) {
	// Reconcile once at startup so a clean control-plane restart does not wait
	// a full repair interval before expired leases become discoverable.
	s.RepairOnce(ctx)
	for {
		if err := s.sleepFunc(ctx, s.period); err != nil {
			return
		}
		s.SweepOnce(ctx)
	}
}

// RepairOnce invokes an optional backend lease-index reconciler at its bounded
// cadence. It is separately leader-gated because a reconciliation scan is
// maintenance work rather than part of normal per-lease execution.
func (s *LeaseSweeper) RepairOnce(ctx context.Context) int {
	if s.elector != nil && !s.elector.IsLeader() {
		return 0
	}
	repairer, ok := s.state.(LeaseIndexRepairer)
	if !ok {
		return 0
	}

	now := s.clock()
	s.repairMu.Lock()
	if !s.lastRepair.IsZero() && now.Sub(s.lastRepair) < s.repairPeriod {
		s.repairMu.Unlock()
		return 0
	}
	s.lastRepair = now
	s.repairMu.Unlock()

	started := time.Now()
	reconciled, err := repairer.RepairLeaseIndex(ctx, s.repairBatch)
	s.observeTiming(func(observer SweepTimingObserver) {
		observer.OnSweepRepair(reconciled, time.Since(started), err)
	})
	if err != nil && s.log != nil {
		s.log.Error("repair lease expiry index", "err", err)
	}
	if err != nil {
		return 0
	}
	return reconciled
}

// SweepOnce executes exactly one sweep pass. Returns the number of leases the
// sweeper successfully reclaimed. Exported so tests and admin tooling can
// trigger a sweep without waiting for the next tick.
func (s *LeaseSweeper) SweepOnce(ctx context.Context) int {
	if s.elector != nil && !s.elector.IsLeader() {
		return 0
	}
	s.RepairOnce(ctx)
	before := s.clock().Add(-s.grace)
	listStarted := time.Now()
	expired, err := s.state.ListExpiredLeases(ctx, before)
	s.observeTiming(func(observer SweepTimingObserver) {
		observer.OnSweepListExpired(len(expired), time.Since(listStarted), err)
	})
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
		reclaimStarted := time.Now()
		ok, err := s.engine.ReclaimLease(ctx, lease)
		if err != nil {
			s.observeTiming(func(observer SweepTimingObserver) {
				observer.OnSweepReclaimResult("error", time.Since(reclaimStarted))
			})
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
			s.observeTiming(func(observer SweepTimingObserver) {
				observer.OnSweepReclaimResult("race", time.Since(reclaimStarted))
			})
			if s.observer != nil {
				s.observer.OnSweepRace(string(lease.ExecutionID), lease.NodeName)
			}
			continue
		}
		s.observeTiming(func(observer SweepTimingObserver) {
			observer.OnSweepReclaimResult("reclaimed", time.Since(reclaimStarted))
		})
		reclaimed++
		if s.observer != nil {
			ageMs := s.clock().Sub(lease.IssuedAt.Add(lease.TTL)).Milliseconds()
			s.observer.OnSweepReclaim(string(lease.ExecutionID), lease.NodeName, ageMs)
		}
	}
	return reclaimed
}

func (s *LeaseSweeper) observeTiming(fn func(SweepTimingObserver)) {
	if s.timingObserver == nil {
		return
	}
	defer func() { _ = recover() }()
	fn(s.timingObserver)
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
