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
	directory      ExpiredLeaseReleaser
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
	OnSweepReclaim(ctx context.Context, execID, nodeName string, ageMs int64)
	OnSweepRace(ctx context.Context, execID, nodeName string)
	OnSweepError(ctx context.Context, execID, nodeName string, err error)
}

// SweepTimingObserver is an optional extension implemented by observability
// adapters that need bounded lease scan, reclaim, and repair latency metrics.
// LeaseSweeper discovers it from LeaseSweeperConfig.Observer without expanding
// the backwards-compatible SweepObserver contract.
type SweepTimingObserver interface {
	OnSweepListExpired(ctx context.Context, candidates int, elapsed time.Duration, err error)
	OnSweepReclaimResult(ctx context.Context, result string, elapsed time.Duration)
	OnSweepRepair(ctx context.Context, reconciled int, elapsed time.Duration, err error)
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
	// RunnerDirectory is the token-fenced directory cleanup capability. When
	// non-nil, SweepOnce releases the expired lease in the directory before
	// engine reclaim; token mismatch fails closed (no reclaim).
	RunnerDirectory ExpiredLeaseReleaser
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
		directory:      cfg.RunnerDirectory,
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
		observer.OnSweepRepair(ctx, reconciled, time.Since(started), err)
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
		observer.OnSweepListExpired(ctx, len(expired), time.Since(listStarted), err)
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
		// 1. Directory cleanup first (token-fenced). This prevents a stale
		// finalized lease from occupying runner capacity or suppressing
		// redelivery after the engine has revoked the lease.
		if s.directory != nil {
			req := ExpiredDirectoryLeaseRequest{
				AssignmentID: BuildAssignmentID(taskFromExpiredLease(&lease)),
				LeaseID:      lease.LeaseID,
				LeaseToken:   lease.LeaseToken,
			}
			out, derr := s.directory.ReleaseExpiredLease(ctx, req)
			if derr != nil || out == ExpiredDirectoryLeaseTokenMismatch {
				// Fail closed: do not attempt engine reclaim. The next sweep
				// will see a fresh lease generation if one exists.
				if derr != nil && s.log != nil {
					s.log.Error("release expired lease in directory",
						"exec", string(lease.ExecutionID),
						"node", lease.NodeName,
						"err", derr,
					)
				}
				continue
			}
		}

		// 2. Engine reclaim. Judge reclaimed before err (spec §3.3.1 step 5).
		reclaimStarted := time.Now()
		ok, err := s.engine.ReclaimLease(ctx, lease)
		switch {
		case ok && err == nil:
			s.observeTiming(func(observer SweepTimingObserver) {
				observer.OnSweepReclaimResult(ctx, "reclaimed", time.Since(reclaimStarted))
			})
			reclaimed++
			if s.observer != nil {
				ageMs := s.clock().Sub(lease.IssuedAt.Add(lease.TTL)).Milliseconds()
				s.observer.OnSweepReclaim(ctx, string(lease.ExecutionID), lease.NodeName, ageMs)
			}
		case ok && err != nil:
			// Revoke/outbox applied, but immediate FlushOutbox failed. The
			// lease is already revoked (it will not be re-listed), so count
			// the reclaim as applied and let the durable OutboxDispatcher
			// retry delivery — do not wait for the next lease sweep.
			s.observeTiming(func(observer SweepTimingObserver) {
				observer.OnSweepReclaimResult(ctx, "applied_pending", time.Since(reclaimStarted))
			})
			reclaimed++
			s.recordReclaimApplied(ctx, lease, err)
		case !ok && err == nil:
			// A racing commit/report won; no new mutation was applied.
			s.observeTiming(func(observer SweepTimingObserver) {
				observer.OnSweepReclaimResult(ctx, "race", time.Since(reclaimStarted))
			})
			if s.observer != nil {
				s.observer.OnSweepRace(ctx, string(lease.ExecutionID), lease.NodeName)
			}
		case !ok && err != nil:
			// State mutation not applied; leave the expired lease in place
			// and retry on the next sweep.
			s.observeTiming(func(observer SweepTimingObserver) {
				observer.OnSweepReclaimResult(ctx, "error", time.Since(reclaimStarted))
			})
			if s.log != nil {
				s.log.Error("reclaim lease",
					"exec", string(lease.ExecutionID),
					"node", lease.NodeName,
					"err", err,
				)
			}
			if s.observer != nil {
				s.observer.OnSweepError(ctx, string(lease.ExecutionID), lease.NodeName, err)
			}
		}
	}
	return reclaimed
}

// taskFromExpiredLease reconstructs the queued task identity used to derive
// the runner-directory AssignmentID. The directory key is built from the same
// immutable fields as BuildAssignmentID.
func taskFromExpiredLease(lease *engine.ExpiredLease) *engine.Task {
	if lease == nil {
		return nil
	}
	return &engine.Task{
		ExecutionID:  lease.ExecutionID,
		NodeName:     lease.NodeName,
		NodeIdx:      lease.NodeIdx,
		ActivationID: lease.ActivationID,
		AutoDepth:    lease.AutoDepth,
		Payload:      lease.Payload,
	}
}

// reclaimAppliedObserver is an optional extension that records a reclaim whose
// state transition was applied but whose immediate delivery flush failed.
// Implementations must not block.
type reclaimAppliedObserver interface {
	OnSweepReclaimApplied(ctx context.Context, execID, nodeName string, ageMs int64)
}

// recordReclaimApplied preserves the fact that a reclaim was applied even
// though the synchronous outbox flush returned an error. It must not be
// recorded as a delivery failure and must not erase the applied reclaim.
func (s *LeaseSweeper) recordReclaimApplied(ctx context.Context, lease engine.ExpiredLease, flushErr error) {
	ageMs := s.clock().Sub(lease.IssuedAt.Add(lease.TTL)).Milliseconds()
	if obs, ok := s.observer.(reclaimAppliedObserver); ok {
		obs.OnSweepReclaimApplied(ctx, string(lease.ExecutionID), lease.NodeName, ageMs)
	} else if s.log != nil {
		s.log.Error("reclaim applied but flush failed",
			"exec", string(lease.ExecutionID),
			"node", lease.NodeName,
			"age_ms", ageMs,
			"err", flushErr,
		)
	}
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
