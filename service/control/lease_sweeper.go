package control

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// DefaultSweepPeriod is how often the sweeper scans for expired leases when
// LeaseSweeperConfig.Period is unset.
const DefaultSweepPeriod = 10 * time.Second

// DefaultLeaseRepairPeriod bounds Redis lease-index reconciliation frequency.
// It is intentionally less frequent than lease expiry scans because normal
// acquire/revoke/commit paths already maintain the index atomically.
const DefaultLeaseRepairPeriod = time.Minute

// DefaultLegacyLeaseMetaReapPeriod bounds pre-U-7 lease-metadata reaping. The
// backlog it drains is finite and only shrinks, so it is deliberately the
// slowest cadence here: each pass is bounded work against a Redis the whole
// control plane shares, and there is no correctness reason to hurry.
const DefaultLegacyLeaseMetaReapPeriod = 5 * time.Minute

// DefaultStrandedLeaseReapPeriod bounds reclamation of 'leased' assignments
// whose directory lease metadata expired. Like the legacy-metadata reaper it is
// maintenance over shared state and runs on the slowest cadence, but unlike it
// the backlog is not finite: a runner that is killed without a clean drain
// leaves one stranded assignment per in-flight task, so this drains a steady
// trickle rather than a one-time residue.
const DefaultStrandedLeaseReapPeriod = 5 * time.Minute

// DefaultDeadQueuedAssignmentReapPeriod bounds reclamation of 'queued'
// assignments whose execution is gone. It matches the stranded-lease cadence:
// both are maintenance over a directory the whole control plane shares, and both
// are bounded per pass, so there is no reason to read more often than the
// existing reaper does.
const DefaultDeadQueuedAssignmentReapPeriod = 5 * time.Minute

// DefaultOrphanedHandoffReapPeriod bounds reclamation of handoff debt whose
// owning runner is gone. It matches the other ledger-walking reapers: this one
// scans the fleet-wide handoff ledger because there is no per-runner index to
// walk once the owner has disappeared, so it is the most expensive of them and
// the least urgent — the debt it settles has already waited out a claim expiry.
const DefaultOrphanedHandoffReapPeriod = 5 * time.Minute

// DefaultCancelingExecutionReapPeriod bounds how often the leader looks for
// executions left in canceling by an interrupted Cancel. It is deliberately
// shorter than the stuck age so the worst-case recovery latency stays close to
// one threshold rather than two; the scan itself reads a small index, not the
// keyspace.
const DefaultCancelingExecutionReapPeriod = 30 * time.Second

// DefaultCancelingExecutionStuckAfter is how long an execution may sit in
// canceling before the leader treats the cancel as interrupted and re-drives
// it. Cancel's own steps are a handful of store round trips, so anything near
// this age is not a slow request; the margin exists so the reaper never races a
// Cancel that is merely slow, which would duplicate its completion notification
// (the state transitions themselves are fenced and safe either way).
const DefaultCancelingExecutionStuckAfter = time.Minute

// defaultCancelingExecutionReapBatch bounds one reaper page.
const defaultCancelingExecutionReapBatch = 64

// defaultExecutionDeadlineBatch bounds how many executions one deadline pass
// drives into timeout.
const defaultExecutionDeadlineBatch = engine.DefaultDeadlineSweepBatch

// executionDeadlineFetchFactor caps the deadline pass's over-fetch at this
// multiple of its batch (see engine.DeadlineSweep).
const executionDeadlineFetchFactor = engine.DefaultDeadlineSweepFetchFactor

// LeaseLister is the subset of engine.StateStore used by the sweeper to find
// candidates for reclamation. The full StateStore interface satisfies this
// shape implicitly.
type LeaseLister interface {
	ListExpiredLeases(ctx context.Context, before time.Time) ([]engine.ExpiredLease, error)
}

// CancelingExecutionCanceler is the engine capability the sweeper drives to
// finish an interrupted cancel. *engine.Engine implements it; the sweeper
// reaches it through an optional assertion because its engine parameter is the
// narrower execution.Engine, and a host whose engine cannot cancel simply
// leaves the pass unsupported rather than failing to build.
type CancelingExecutionCanceler interface {
	Cancel(ctx context.Context, id types.ExecutionID) error
}

// ExecutionExpirer is the engine capability the sweeper drives to time out an
// execution whose settings.timeout deadline has passed. *engine.Engine
// implements it; like CancelingExecutionCanceler it is reached through an
// optional assertion on the narrower execution.Engine.
type ExecutionExpirer = engine.ExecutionExpirer

// LeaseIndexRepairer is an optional backend capability that reconciles lease
// expiry discovery indexes from authoritative running-node metadata. Backends
// without a secondary index do not implement it.
type LeaseIndexRepairer interface {
	RepairLeaseIndex(ctx context.Context, limit int) (reconciled int, err error)
}

// ConcurrencySlotReconciler is an optional backend capability that reconciles
// workflow concurrency slot sets against the executions that hold them.
//
// It is a separate capability from LeaseIndexRepairer because the two back up
// different invariants: a repair index is derived state that can be rebuilt at
// any time, while a slot set is the limit itself. A backend that can do one
// without the other must still be able to say so.
type ConcurrencySlotReconciler interface {
	ReconcileConcurrencySlots(ctx context.Context, limit int) (swept int, err error)
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
	passObserver   SweepPassObserver
	passCandidates SweepPassCandidateObserver
	depthObserver  SweepQueueDepthObserver
	ageObserver    SweepQueueAgeObserver
	deadlineObs    ExecutionDeadlineObserver
	elector        backend.LeaderElector
	clock          func() time.Time
	sleepFunc      func(context.Context, time.Duration) error

	repairPeriod time.Duration
	repairBatch  int
	repairMu     sync.Mutex
	lastRepair   time.Time

	// The concurrency maintenance (slot reconciliation and queue-timeout
	// expiry) shares the repair period but keeps its own timestamp: it must not
	// inherit the repair pass's gates, only its cadence. The two concurrency
	// passes share ONE window, because they are one pass's worth of work.
	slotSweepMu   sync.Mutex
	lastSlotSweep time.Time

	reapPeriod time.Duration
	reapBatch  int
	reapMu     sync.Mutex
	lastReap   time.Time

	strandedPeriod time.Duration
	strandedBatch  int
	strandedMu     sync.Mutex
	lastStranded   time.Time

	deadQueuedPeriod time.Duration
	deadQueuedBatch  int
	deadQueuedMu     sync.Mutex
	lastDeadQueued   time.Time

	orphanedHandoff       OrphanedHandoffReclaimer
	orphanedHandoffPeriod time.Duration
	orphanedHandoffBatch  int
	orphanedHandoffMu     sync.Mutex
	lastOrphanedHandoff   time.Time

	cancelingPeriod time.Duration
	cancelingBatch  int
	cancelingAge    time.Duration
	cancelingMu     sync.Mutex
	lastCanceling   time.Time

	deadlinePeriod  time.Duration
	deadlineBatch   int
	deadlineMu      sync.Mutex
	lastDeadline    time.Time
	deadlineBackoff *engine.ExpireBackoff
	// deadlineSweep is built on the first pass that has both capabilities and
	// then reused, so its once-per-episode saturation warning keeps its state.
	deadlineSweep *engine.DeadlineSweep
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

// Maintenance passes reported to a SweepPassObserver. The values are metric
// label values, so they are part of the observable surface.
const (
	SweepPassRepair               = "repair"
	SweepPassLegacyLeaseMeta      = "legacy_lease_meta"
	SweepPassStrandedLease        = "stranded_lease"
	SweepPassDeadQueuedAssignment = "dead_queued_assignment"
	SweepPassOrphanedHandoff      = "orphaned_handoff"
	// SweepPassConcurrencySlot is the workflow slot-set reconciliation. It is
	// its own pass rather than part of SweepPassRepair because the two back up
	// different invariants: a lease repair index is derived state that can be
	// rebuilt at any time, while a slot set IS the limit, and an operator has to
	// be able to tell them apart.
	SweepPassConcurrencySlot = "concurrency_slot"
	// SweepPassConcurrencyQueue is the queue-timeout expiry: queued executions
	// past their queue_timeout are driven into the timeout terminal state. It
	// rides the same gate as SweepPassConcurrencySlot — one maintenance window,
	// one batch bound — but reports separately, because a queued submission
	// giving up and a leaked slot being reclaimed are different events with
	// different causes.
	SweepPassConcurrencyQueue = "concurrency_queue"
	// SweepPassCancelingExecution is the stale-canceling reaper: executions
	// left in canceling by an interrupted Cancel are found through the store's
	// canceling index and driven to completion by calling Cancel again.
	SweepPassCancelingExecution = "canceling_execution"
	// SweepPassExecutionDeadline is the deadline expiry: executions whose
	// settings.timeout deadline has passed with no task left to notice it are
	// found through the store's deadline index and driven into timeout.
	SweepPassExecutionDeadline = "execution_deadline"
)

// Outcomes a maintenance pass reports.
//
// Exactly one outcome means the pass did the work: SweepPassOutcomeRan. It is
// emitted whether or not anything was released, so "ran and released 0" is a
// positive observation rather than an absence. Every other outcome names WHY
// the pass did not run — the gate that stopped it — which is what separates it
// from a pass whose loop never reached the call at all (no series, no event).
const (
	SweepPassOutcomeRan              = "ran"
	SweepPassOutcomeError            = "error"
	SweepPassOutcomeSkippedNotLeader = "skipped_not_leader"
	SweepPassOutcomeSkippedCadence   = "skipped_cadence"
	SweepPassOutcomeUnsupported      = "unsupported"
)

// SweepPassObserver is an optional extension implemented by observability
// adapters that need every state-mutating maintenance pass accounted for.
// LeaseSweeper discovers it from LeaseSweeperConfig.Observer the same way it
// discovers SweepTimingObserver, so the backwards-compatible SweepObserver
// contract is unchanged.
//
// It exists because these passes have no other signal: each reports through an
// optional logger, so a host that injects none sees nothing at all, and the
// passes are gated twice (leadership and cadence), so a silent pass could mean
// "released nothing", "not the leader", "not due yet" or "never reached". A
// real investigation of clustered 409s could not tell those apart.
type SweepPassObserver interface {
	OnSweepPass(ctx context.Context, pass, outcome string, released int)
}

// SweepPassCandidateObserver is an optional extension of SweepPassObserver
// implemented by observability adapters that also need how many candidates a
// maintenance pass inspected, not only how much it released. LeaseSweeper
// discovers it from LeaseSweeperConfig.Observer the same way it discovers
// SweepTimingObserver, so SweepObserver and SweepPassObserver are both
// unchanged.
//
// It embeds SweepPassObserver rather than standing alone because the two counts
// are only readable together: a release count without the candidate count cannot
// say whether a pass draining nothing had nothing to drain or walked a shape far
// wider than the one it drains. An adapter that reports one therefore reports
// both, and the sweeper can call both for a single value.
type SweepPassCandidateObserver interface {
	SweepPassObserver
	// OnSweepPassCandidates reports how many candidates the pass inspected. Like
	// the released count it is reported for a pass that ran and for one that
	// failed part-way (with what it inspected before the failure), and not for a
	// pass a gate skipped — a zero there would be indistinguishable from a pass
	// that inspected nothing.
	OnSweepPassCandidates(ctx context.Context, pass, outcome string, inspected int)
}

// SweepQueueDepthObserver is an optional extension implemented by observability
// adapters that need how much work is waiting in the assignment queues, not
// only what the maintenance passes did with it. LeaseSweeper discovers it from
// LeaseSweeperConfig.Observer the same way it discovers SweepTimingObserver, so
// every existing observer contract is unchanged.
//
// It exists because queue depth is a standing condition rather than a pass
// outcome: the runner-directory LIST has no depth series at all (the broker's
// xflow_queue_depth samples asynq's queues), so a backlog the claim walk is not
// draining — the shape behind an assignment residency measured in minutes —
// is invisible to every existing signal until the reapers eventually act on it.
type SweepQueueDepthObserver interface {
	// OnAssignmentQueueDepth reports one queue's depth. lane is the node type
	// the lane serves, or QueueLaneLegacy for the shared legacy queue; the
	// empty lane name never reaches this call. A queue that is configured but
	// empty is reported as a zero, so an idle lane and a lane that was never
	// read are distinguishable.
	OnAssignmentQueueDepth(ctx context.Context, lane string, depth int64)
}

// SweepQueueAgeObserver is the optional extension that receives the age of
// the oldest sampled entry on each assignment queue, alongside its depth. It
// is discovered from LeaseSweeperConfig.Observer like SweepQueueDepthObserver.
// A queue with no stamped entry in its head sample is not reported at all.
type SweepQueueAgeObserver interface {
	OnAssignmentQueueOldestAge(ctx context.Context, lane string, age time.Duration)
}

// SweepSourceDeadlinePass is the source the sweeper reports to an
// ExecutionDeadlineObserver. It is a metric label value, so it is part of the
// observable surface.
const SweepSourceDeadlinePass = "sweeper"

// ExecutionDeadlineObserver is an optional extension implemented by
// observability adapters that count executions timed out by their deadline.
// LeaseSweeper discovers it from LeaseSweeperConfig.Observer like the other
// extensions, so every existing observer contract is unchanged.
//
// It is separate from the pass counters because they answer a different
// question: the pass counts work done (and counts a resumed cascade as a
// release), while this counts the timeout transitions themselves, per tenant.
type ExecutionDeadlineObserver interface {
	// OnExecutionDeadlineExpired reports one execution this observer drove into
	// timeout. ctx carries the execution's own namespace.
	OnExecutionDeadlineExpired(ctx context.Context, source string)
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
	// LegacyLeaseMetaReapPeriod controls the optional pre-U-7 lease-metadata
	// reaper rate. Zero defaults to DefaultLegacyLeaseMetaReapPeriod. It is
	// slower than the repair cadence because it drains a finite backlog left by
	// an upgrade, not a live index that keeps changing.
	LegacyLeaseMetaReapPeriod time.Duration
	// LegacyLeaseMetaReapBatch bounds one reaper call. Zero defaults to
	// defaultLegacyLeaseMetaReapBatch.
	LegacyLeaseMetaReapBatch int
	// StrandedLeaseReapPeriod controls the optional stranded-lease reaper rate.
	// Zero defaults to DefaultStrandedLeaseReapPeriod.
	StrandedLeaseReapPeriod time.Duration
	// StrandedLeaseReapBatch bounds one reaper call. Zero defaults to
	// defaultStrandedLeaseReapBatch.
	StrandedLeaseReapBatch int
	// DeadQueuedAssignmentReapPeriod controls the optional dead-queued-assignment
	// reaper rate. Zero defaults to DefaultDeadQueuedAssignmentReapPeriod.
	DeadQueuedAssignmentReapPeriod time.Duration
	// DeadQueuedAssignmentReapBatch bounds one reaper call. Zero defaults to
	// defaultDeadQueuedAssignmentReapBatch.
	DeadQueuedAssignmentReapBatch int
	// OrphanedHandoffReaper resolves handoff debt whose owning runner is gone.
	// Optional; nil skips the pass.
	//
	// Unlike the other optional reapers this is not a RunnerDirectory
	// capability. Settling the debt is an engine question, not a directory
	// question, and the component that holds both is Core — which is why the
	// wiring is a field the control plane fills rather than an assertion on
	// RunnerDirectory.
	OrphanedHandoffReaper OrphanedHandoffReclaimer
	// OrphanedHandoffReapPeriod controls the optional orphaned-handoff reaper
	// rate. Zero defaults to DefaultOrphanedHandoffReapPeriod.
	OrphanedHandoffReapPeriod time.Duration
	// OrphanedHandoffReapBatch bounds one reaper call. Zero defaults to
	// defaultOrphanedHandoffReapBatch.
	OrphanedHandoffReapBatch int
	// CancelingExecutionReapPeriod controls the optional stale-canceling reaper
	// rate. Zero defaults to DefaultCancelingExecutionReapPeriod.
	CancelingExecutionReapPeriod time.Duration
	// CancelingExecutionReapBatch bounds one reaper page. Zero defaults to
	// defaultCancelingExecutionReapBatch.
	CancelingExecutionReapBatch int
	// CancelingExecutionStuckAfter is the minimum age an execution must have
	// spent in canceling before its Cancel is considered interrupted and
	// re-driven. Zero defaults to DefaultCancelingExecutionStuckAfter.
	CancelingExecutionStuckAfter time.Duration
	// ExecutionDeadlinePeriod controls the execution deadline pass rate. Zero
	// defaults to DefaultSweepPeriod.
	ExecutionDeadlinePeriod time.Duration
	// ExecutionDeadlineBatch bounds how many executions one deadline pass drives
	// into timeout. Zero defaults to defaultExecutionDeadlineBatch.
	ExecutionDeadlineBatch int
}

// OrphanedHandoffReclaimer is the control-plane capability the sweeper drives to
// settle handoff debt whose owning runner is gone. Core implements it; a host
// that supplies no reaper simply skips the pass, and the ledger keeps the debt
// visible.
type OrphanedHandoffReclaimer interface {
	// ReapOrphanedHandoffs settles up to limit orphaned handoffs and reports how
	// many candidates it inspected and how many it settled. Implementation
	// requirements — token fencing, engine consultation, when debt is retained —
	// live on Core.ReapOrphanedHandoffs.
	ReapOrphanedHandoffs(ctx context.Context, limit int) (ReapResult, error)
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
	if cfg.LegacyLeaseMetaReapPeriod <= 0 {
		cfg.LegacyLeaseMetaReapPeriod = DefaultLegacyLeaseMetaReapPeriod
	}
	if cfg.LegacyLeaseMetaReapBatch <= 0 {
		cfg.LegacyLeaseMetaReapBatch = defaultLegacyLeaseMetaReapBatch
	}
	if cfg.StrandedLeaseReapPeriod <= 0 {
		cfg.StrandedLeaseReapPeriod = DefaultStrandedLeaseReapPeriod
	}
	if cfg.StrandedLeaseReapBatch <= 0 {
		cfg.StrandedLeaseReapBatch = defaultStrandedLeaseReapBatch
	}
	if cfg.DeadQueuedAssignmentReapPeriod <= 0 {
		cfg.DeadQueuedAssignmentReapPeriod = DefaultDeadQueuedAssignmentReapPeriod
	}
	if cfg.DeadQueuedAssignmentReapBatch <= 0 {
		cfg.DeadQueuedAssignmentReapBatch = defaultDeadQueuedAssignmentReapBatch
	}
	if cfg.OrphanedHandoffReapPeriod <= 0 {
		cfg.OrphanedHandoffReapPeriod = DefaultOrphanedHandoffReapPeriod
	}
	if cfg.OrphanedHandoffReapBatch <= 0 {
		cfg.OrphanedHandoffReapBatch = defaultOrphanedHandoffReapBatch
	}
	if cfg.CancelingExecutionReapPeriod <= 0 {
		cfg.CancelingExecutionReapPeriod = DefaultCancelingExecutionReapPeriod
	}
	if cfg.CancelingExecutionReapBatch <= 0 {
		cfg.CancelingExecutionReapBatch = defaultCancelingExecutionReapBatch
	}
	if cfg.CancelingExecutionStuckAfter <= 0 {
		cfg.CancelingExecutionStuckAfter = DefaultCancelingExecutionStuckAfter
	}
	if cfg.ExecutionDeadlinePeriod <= 0 {
		cfg.ExecutionDeadlinePeriod = DefaultSweepPeriod
	}
	if cfg.ExecutionDeadlineBatch <= 0 {
		cfg.ExecutionDeadlineBatch = defaultExecutionDeadlineBatch
	}
	var timingObserver SweepTimingObserver
	if observer, ok := cfg.Observer.(SweepTimingObserver); ok {
		timingObserver = observer
	}
	var passObserver SweepPassObserver
	if observer, ok := cfg.Observer.(SweepPassObserver); ok {
		passObserver = observer
	}
	var passCandidates SweepPassCandidateObserver
	if observer, ok := cfg.Observer.(SweepPassCandidateObserver); ok {
		passCandidates = observer
	}
	var depthObserver SweepQueueDepthObserver
	if observer, ok := cfg.Observer.(SweepQueueDepthObserver); ok {
		depthObserver = observer
	}
	var ageObserver SweepQueueAgeObserver
	if observer, ok := cfg.Observer.(SweepQueueAgeObserver); ok {
		ageObserver = observer
	}
	var deadlineObs ExecutionDeadlineObserver
	if observer, ok := cfg.Observer.(ExecutionDeadlineObserver); ok {
		deadlineObs = observer
	}
	return &LeaseSweeper{
		state:            state,
		engine:           eng,
		directory:        cfg.RunnerDirectory,
		period:           cfg.Period,
		grace:            cfg.Grace,
		log:              cfg.Logger,
		observer:         cfg.Observer,
		timingObserver:   timingObserver,
		passObserver:     passObserver,
		passCandidates:   passCandidates,
		depthObserver:    depthObserver,
		ageObserver:      ageObserver,
		deadlineObs:      deadlineObs,
		elector:          cfg.Elector,
		clock:            func() time.Time { return time.Now().UTC() },
		sleepFunc:        sleepWithContext,
		repairPeriod:     cfg.LeaseRepairPeriod,
		repairBatch:      cfg.LeaseRepairBatch,
		reapPeriod:       cfg.LegacyLeaseMetaReapPeriod,
		reapBatch:        cfg.LegacyLeaseMetaReapBatch,
		strandedPeriod:   cfg.StrandedLeaseReapPeriod,
		strandedBatch:    cfg.StrandedLeaseReapBatch,
		deadQueuedPeriod: cfg.DeadQueuedAssignmentReapPeriod,
		deadQueuedBatch:  cfg.DeadQueuedAssignmentReapBatch,

		orphanedHandoff:       cfg.OrphanedHandoffReaper,
		orphanedHandoffPeriod: cfg.OrphanedHandoffReapPeriod,
		orphanedHandoffBatch:  cfg.OrphanedHandoffReapBatch,

		cancelingPeriod: cfg.CancelingExecutionReapPeriod,
		cancelingBatch:  cfg.CancelingExecutionReapBatch,
		cancelingAge:    cfg.CancelingExecutionStuckAfter,

		deadlinePeriod:  cfg.ExecutionDeadlinePeriod,
		deadlineBatch:   cfg.ExecutionDeadlineBatch,
		deadlineBackoff: engine.NewExpireBackoff(),
	}
}

// Run drives the sweep loop until ctx is canceled. Blocks the caller; spawn it
// in a goroutine.
func (s *LeaseSweeper) Run(ctx context.Context) {
	// Reconcile once at startup so a clean control-plane restart does not wait
	// a full repair interval before expired leases become discoverable.
	s.RepairOnce(ctx)
	s.ReapLegacyLeaseMetaOnce(ctx)
	s.ReapStrandedLeasesOnce(ctx)
	s.ReapDeadQueuedAssignmentsOnce(ctx)
	s.ReapOrphanedHandoffsOnce(ctx)
	s.ReapStuckCancelingExecutionsOnce(ctx)
	s.ReapExpiredExecutionsOnce(ctx)
	s.ReportQueueDepthsOnce(ctx)
	for {
		if err := s.sleepFunc(ctx, s.period); err != nil {
			return
		}
		s.SweepOnce(ctx)
		s.ReapLegacyLeaseMetaOnce(ctx)
		s.ReapStrandedLeasesOnce(ctx)
		s.ReapDeadQueuedAssignmentsOnce(ctx)
		s.ReapOrphanedHandoffsOnce(ctx)
		s.ReapStuckCancelingExecutionsOnce(ctx)
		s.ReapExpiredExecutionsOnce(ctx)
		s.ReportQueueDepthsOnce(ctx)
	}
}

// ReportQueueDepthsOnce reads every assignment queue's depth from the
// directory and hands each to the observer, at the sweep cadence.
//
// It is leader-gated like the maintenance passes, for a different reason: the
// queues are cluster-wide, so a replica reporting its own read of the same LIST
// would publish a second copy of the same series and the two would fight over
// the gauge value. The gate keeps exactly one reporter per cluster, and it is
// the same gate the passes use so "who reports" needs no separate rule.
//
// It is deliberately NOT reported as a maintenance pass: the depth is a
// standing condition rather than work done, and putting it in the pass
// enumeration would add a pass label value that never releases anything.
// Failing to read the depth is therefore not a sweep error either — it is
// logged and retried on the next cadence, and the depth series simply goes
// stale rather than the gauge being zeroed, which would read as "the queue
// drained".
//
// The oldest-entry age rides the same cadence and gate, and follows the same
// failure rule independently: a failed age read leaves the depths reported and
// the age series stale, and the reverse.
func (s *LeaseSweeper) ReportQueueDepthsOnce(ctx context.Context) {
	if s.depthObserver == nil && s.ageObserver == nil {
		return
	}
	if s.elector != nil && !s.elector.IsLeader() {
		return
	}
	if reporter, ok := s.directory.(AssignmentQueueDepthReporter); ok && s.depthObserver != nil {
		depths, err := reporter.AssignmentQueueDepths(ctx)
		if err != nil {
			if s.log != nil {
				s.log.Error("read assignment queue depths", "err", err)
			}
		} else {
			for lane, depth := range depths {
				s.depthObserver.OnAssignmentQueueDepth(ctx, lane, depth)
			}
		}
	}
	if reporter, ok := s.directory.(AssignmentQueueOldestAgeReporter); ok && s.ageObserver != nil {
		ages, err := reporter.AssignmentQueueOldestAges(ctx)
		if err != nil {
			if s.log != nil {
				s.log.Error("read assignment queue oldest ages", "err", err)
			}
			return
		}
		for lane, age := range ages {
			s.ageObserver.OnAssignmentQueueOldestAge(ctx, lane, age)
		}
	}
}

// ReapStrandedLeasesOnce reclaims assignments left 'leased' after their
// directory lease metadata expired, at its own bounded cadence. It is
// separately leader-gated for the same reason as the repair and legacy-metadata
// passes: it is maintenance over shared state rather than part of any one
// lease's execution, and it is a no-op for directories that do not implement
// the capability.
//
// This closes the one shape the sweep itself cannot see. SweepOnce enumerates
// the engine's lease index, whose TTL is the same transient metadata that has
// already expired — so at the moment an assignment becomes unrecoverable it has
// also dropped out of that enumeration, taking its runner capacity with it.
// Nothing else reclaims it either: the claim-reclaim path only walks
// pre-claimation state, and the owning runner's own poll-time recovery stopped
// when the runner did. See StrandedLeaseReaper for the reachability argument.
func (s *LeaseSweeper) ReapStrandedLeasesOnce(ctx context.Context) int {
	if s.elector != nil && !s.elector.IsLeader() {
		s.observePass(ctx, SweepPassStrandedLease, SweepPassOutcomeSkippedNotLeader, ReapResult{})
		return 0
	}
	reaper, ok := s.directory.(StrandedLeaseReaper)
	if !ok {
		s.observePass(ctx, SweepPassStrandedLease, SweepPassOutcomeUnsupported, ReapResult{})
		return 0
	}

	now := s.clock()
	s.strandedMu.Lock()
	if !s.lastStranded.IsZero() && now.Sub(s.lastStranded) < s.strandedPeriod {
		s.strandedMu.Unlock()
		s.observePass(ctx, SweepPassStrandedLease, SweepPassOutcomeSkippedCadence, ReapResult{})
		return 0
	}
	s.lastStranded = now
	s.strandedMu.Unlock()

	result, err := reaper.ReapStrandedLeases(ctx, s.strandedBatch)
	if err != nil {
		// Never blocks lease execution, so it is logged and retried on the next
		// cadence rather than surfaced as a sweep error.
		if s.log != nil {
			s.log.Error("reap stranded directory leases", "err", err)
		}
		s.observePass(ctx, SweepPassStrandedLease, SweepPassOutcomeError, result)
		return 0
	}
	if result.Released > 0 && s.log != nil {
		s.log.Info("reaped stranded directory leases", "released", result.Released)
	}
	s.observePass(ctx, SweepPassStrandedLease, SweepPassOutcomeRan, result)
	return result.Released
}

// ReapDeadQueuedAssignmentsOnce reclaims queued assignments whose execution is
// gone, at its own bounded cadence. It is separately leader-gated for the same
// reason as the repair, legacy-metadata and stranded-lease passes: it is
// maintenance over shared state rather than part of any one task's execution,
// and it is a no-op for directories that do not implement the capability.
//
// This is the pass that makes the queue converge on its own. SweepOnce only
// enumerates leases, ReapStrandedLeasesOnce only releases 'leased' assignments,
// and ReapLegacyLeaseMetaOnce only drains a pre-upgrade hash. Nothing else
// removes a 'queued' assignment whose execution has expired out of state, so
// without this the only consumer of those entries is a live runner walking past
// them on a poll — which is precisely what stops happening when the queue is
// mostly dead. See DeadQueuedAssignmentReaper for the reachability and safety
// argument.
func (s *LeaseSweeper) ReapDeadQueuedAssignmentsOnce(ctx context.Context) int {
	if s.elector != nil && !s.elector.IsLeader() {
		s.observePass(ctx, SweepPassDeadQueuedAssignment, SweepPassOutcomeSkippedNotLeader, ReapResult{})
		return 0
	}
	reaper, ok := s.directory.(DeadQueuedAssignmentReaper)
	if !ok {
		s.observePass(ctx, SweepPassDeadQueuedAssignment, SweepPassOutcomeUnsupported, ReapResult{})
		return 0
	}

	now := s.clock()
	s.deadQueuedMu.Lock()
	if !s.lastDeadQueued.IsZero() && now.Sub(s.lastDeadQueued) < s.deadQueuedPeriod {
		s.deadQueuedMu.Unlock()
		s.observePass(ctx, SweepPassDeadQueuedAssignment, SweepPassOutcomeSkippedCadence, ReapResult{})
		return 0
	}
	s.lastDeadQueued = now
	s.deadQueuedMu.Unlock()

	result, err := reaper.ReapDeadQueuedAssignments(ctx, s.deadQueuedBatch)
	if err != nil {
		// Never blocks lease execution, so it is logged and retried on the next
		// cadence rather than surfaced as a sweep error.
		if s.log != nil {
			s.log.Error("reap dead queued assignments", "err", err)
		}
		s.observePass(ctx, SweepPassDeadQueuedAssignment, SweepPassOutcomeError, result)
		return 0
	}
	if result.Released > 0 && s.log != nil {
		s.log.Info("reaped dead queued assignments", "reclaimed", result.Released)
	}
	s.observePass(ctx, SweepPassDeadQueuedAssignment, SweepPassOutcomeRan, result)
	return result.Released
}

// ReapOrphanedHandoffsOnce settles handoff debt whose owning runner is gone, at
// its own bounded cadence. It is separately leader-gated for the same reason as
// the other maintenance passes: it is maintenance over shared state rather than
// part of any one task's execution.
//
// This is the pass that reaches the one shape every other one is blind to. A
// claim fenced as lease_may_exist or lease_created has exactly one resolver —
// the owning runner's own poll, which asks the engine whether a lease exists
// and settles the claim on the answer. ReclaimExpiredClaims deliberately only
// marks such a claim recoverable, and never requeues it, because an engine lease
// may exist and the directory cannot say. So when the owner dies, the debt and
// the assignment behind it are unresolved by every control-plane path there is:
// the other passes drain different shapes entirely, and Register only helps a
// runner that comes back. See OrphanedHandoffReaper for the reachability
// argument.
//
// Unlike the other three reapers this one goes through Core rather than the
// directory, because settling the debt means asking the engine. That is a field
// on the config rather than a directory assertion, so a host that wires no Core
// skips the pass and the ledger keeps the debt visible.
func (s *LeaseSweeper) ReapOrphanedHandoffsOnce(ctx context.Context) int {
	if s.elector != nil && !s.elector.IsLeader() {
		s.observePass(ctx, SweepPassOrphanedHandoff, SweepPassOutcomeSkippedNotLeader, ReapResult{})
		return 0
	}
	if s.orphanedHandoff == nil {
		s.observePass(ctx, SweepPassOrphanedHandoff, SweepPassOutcomeUnsupported, ReapResult{})
		return 0
	}

	now := s.clock()
	s.orphanedHandoffMu.Lock()
	if !s.lastOrphanedHandoff.IsZero() && now.Sub(s.lastOrphanedHandoff) < s.orphanedHandoffPeriod {
		s.orphanedHandoffMu.Unlock()
		s.observePass(ctx, SweepPassOrphanedHandoff, SweepPassOutcomeSkippedCadence, ReapResult{})
		return 0
	}
	s.lastOrphanedHandoff = now
	s.orphanedHandoffMu.Unlock()

	result, err := s.orphanedHandoff.ReapOrphanedHandoffs(ctx, s.orphanedHandoffBatch)
	if err != nil {
		// Never blocks lease execution, so it is logged and retried on the next
		// cadence rather than surfaced as a sweep error. The result travels with
		// it: a pass that settled some claims before failing has to report that,
		// or a backlog larger than one batch looks like a pass that did nothing.
		if s.log != nil {
			s.log.Error("reap orphaned runner handoffs", "err", err)
		}
		s.observePass(ctx, SweepPassOrphanedHandoff, SweepPassOutcomeError, result)
		return 0
	}
	if result.Released > 0 && s.log != nil {
		s.log.Info("reaped orphaned runner handoffs", "settled", result.Released)
	}
	s.observePass(ctx, SweepPassOrphanedHandoff, SweepPassOutcomeRan, result)
	return result.Released
}

// ReapStuckCancelingExecutionsOnce re-drives executions that an interrupted
// Cancel left in canceling past the stuck threshold. It is leader-gated and
// cadence-bounded like the other maintenance passes: the candidates are
// cluster-wide, so every replica re-driving them would multiply the work
// without changing the outcome, and the state it repairs is stranded rather
// than in flight.
//
// This closes the one gap Cancel itself cannot: Cancel is idempotent and
// resumable (see engine/cancel.go), but it only runs while the request that
// started it is alive. A crash, a canceled context, or one failed store step
// between the canceling write and the canceled write leaves the execution
// canceling forever — no other path transitions it, and while it sits there it
// keeps its workflow concurrency slot. The store's canceling index
// (engine.CancelingExecutionLister) makes those executions findable; calling
// Cancel again resumes from wherever the first attempt stopped, because every
// step is fenced and tolerates being applied twice.
//
// The threshold exists so the reaper does not race a Cancel that is merely
// slow. The state transitions are safe under that race either way — the final
// canceled write is fenced and applied once — but the completion notification
// fired by the losing call would duplicate. The default threshold is far above
// Cancel's handful of store round trips.
//
// The page is bounded and oldest-first. A cancel that fails again is logged and
// left for the next pass; ErrExecutionInactive is the benign answer (the
// execution concluded between the list and the call, or was already gone) and
// is not an error. Reported through the pass observer as inspected/released so
// "found nothing", "found candidates and fixed none" and "not the leader" stay
// distinguishable.
func (s *LeaseSweeper) ReapStuckCancelingExecutionsOnce(ctx context.Context) int {
	if s.elector != nil && !s.elector.IsLeader() {
		s.observePass(ctx, SweepPassCancelingExecution, SweepPassOutcomeSkippedNotLeader, ReapResult{})
		return 0
	}
	lister, ok := s.state.(engine.CancelingExecutionLister)
	if !ok {
		s.observePass(ctx, SweepPassCancelingExecution, SweepPassOutcomeUnsupported, ReapResult{})
		return 0
	}
	canceler, ok := s.engine.(CancelingExecutionCanceler)
	if !ok {
		s.observePass(ctx, SweepPassCancelingExecution, SweepPassOutcomeUnsupported, ReapResult{})
		return 0
	}

	now := s.clock()
	s.cancelingMu.Lock()
	if !s.lastCanceling.IsZero() && now.Sub(s.lastCanceling) < s.cancelingPeriod {
		s.cancelingMu.Unlock()
		s.observePass(ctx, SweepPassCancelingExecution, SweepPassOutcomeSkippedCadence, ReapResult{})
		return 0
	}
	s.lastCanceling = now
	s.cancelingMu.Unlock()

	candidates, err := lister.ListCancelingExecutions(ctx, now.Add(-s.cancelingAge), s.cancelingBatch)
	if err != nil {
		if s.log != nil {
			s.log.Error("list executions stuck in canceling", "err", err)
		}
		s.observePass(ctx, SweepPassCancelingExecution, SweepPassOutcomeError, ReapResult{})
		return 0
	}
	result := ReapResult{Inspected: len(candidates)}
	for i, candidate := range candidates {
		// A demoted replica must stop re-driving cluster-wide cancels; the
		// new leader will pick the remainder up on its own pass.
		if i > 0 && i%leaderRecheckInterval == 0 && leaderLost(s.elector) {
			s.observePass(ctx, SweepPassCancelingExecution, SweepPassOutcomeSkippedNotLeader, result)
			return result.Released
		}
		select {
		case <-ctx.Done():
			s.observePass(ctx, SweepPassCancelingExecution, SweepPassOutcomeRan, result)
			return result.Released
		default:
		}
		// The candidate names the namespace its keys live under; the sweeper's
		// own context is namespace-less because the scan spans every namespace
		// the process serves. A backend without a namespace dimension leaves it
		// empty, and the cancel runs on the context as-is.
		execCtx := ctx
		if candidate.Namespace != "" {
			execCtx = namespace.WithNamespace(ctx, candidate.Namespace)
		}
		if err := canceler.Cancel(execCtx, candidate.ExecutionID); err != nil {
			if !errors.Is(err, engine.ErrExecutionInactive) && s.log != nil {
				s.log.Error("re-drive interrupted cancel failed",
					"ns", string(candidate.Namespace),
					"exec", string(candidate.ExecutionID),
					"stuck_since", candidate.Since,
					"err", err,
				)
			}
			// Inactive is benign: the execution reached a terminal state between
			// the list and this call, and the lister drops the stale entry on
			// its next read. Both outcomes are retried on the next cadence.
			continue
		}
		result.Released++
	}
	if result.Released > 0 && s.log != nil {
		s.log.Info("re-drove interrupted cancels", "completed", result.Released)
	}
	s.observePass(ctx, SweepPassCancelingExecution, SweepPassOutcomeRan, result)
	return result.Released
}

// ReapExpiredExecutionsOnce drives executions whose settings.timeout deadline
// has passed into timeout. It is leader-gated and cadence-bounded like the other
// maintenance passes: the candidates are cluster-wide, and expiring an execution
// is idempotent but not free (it cascades into every node still running).
//
// It finds what no task can: an execution past its deadline with no live lease
// (every task finished and the next was never scheduled, the only runner died, or
// the execution is parked). The store's deadline index
// (engine.ExecutionDeadlineExpirer) lists them; engine.ExpireExecution does the
// work and is safe to call for any of them, whatever state it has since reached.
// The deadline is the threshold, so the scan asks for everything at or before
// now with no stuck-after margin.
//
// The candidate loop is engine.DeadlineSweep, shared with the in-process and
// embedded backends' sweepers. Expired and Resumed are what the pass released;
// NotDue, AlreadyTerminal and Gone are benign (someone else finished the
// execution), as is ErrExecutionInactive. Canceling is benign too but the entry
// stays listed until the cancel concludes, so that execution is set aside for a
// window like a failing one. Only another error is a failure.
//
// Failures are spaced out per execution (engine.ExpireBackoff). The index lists
// the oldest deadlines first, so without that an execution that keeps failing
// would hold a slot of every page forever and starve the ones behind it. The scan
// therefore reads the batch plus however many executions are backing off, up to
// executionDeadlineFetchFactor times the batch, and a pass attempts at most one
// batch. Inspected counts the attempts, so released/inspected stays the ratio of
// work that succeeded; candidates skipped while backing off are not counted.
func (s *LeaseSweeper) ReapExpiredExecutionsOnce(ctx context.Context) int {
	if s.elector != nil && !s.elector.IsLeader() {
		s.observePass(ctx, SweepPassExecutionDeadline, SweepPassOutcomeSkippedNotLeader, ReapResult{})
		return 0
	}
	lister, ok := s.state.(engine.ExecutionDeadlineExpirer)
	if !ok {
		s.observePass(ctx, SweepPassExecutionDeadline, SweepPassOutcomeUnsupported, ReapResult{})
		return 0
	}
	expirer, ok := s.engine.(ExecutionExpirer)
	if !ok {
		s.observePass(ctx, SweepPassExecutionDeadline, SweepPassOutcomeUnsupported, ReapResult{})
		return 0
	}

	now := s.clock()
	s.deadlineMu.Lock()
	if !s.lastDeadline.IsZero() && now.Sub(s.lastDeadline) < s.deadlinePeriod {
		s.deadlineMu.Unlock()
		s.observePass(ctx, SweepPassExecutionDeadline, SweepPassOutcomeSkippedCadence, ReapResult{})
		return 0
	}
	s.lastDeadline = now
	if s.deadlineSweep == nil {
		s.deadlineSweep = engine.NewDeadlineSweep(engine.DeadlineSweepConfig{
			Lister:        lister,
			Expirer:       expirer,
			Backoff:       s.deadlineBackoff,
			Batch:         s.deadlineBatch,
			FetchFactor:   executionDeadlineFetchFactor,
			Clock:         func() time.Time { return s.clock() },
			Logger:        s.log,
			Continue:      func() bool { return !leaderLost(s.elector) },
			ContinueEvery: leaderRecheckInterval,
			OnExpired:     func(execCtx context.Context, _ engine.DeadlineExpiredExecution) { s.observeDeadlineExpired(execCtx) },
		})
	}
	sweep := s.deadlineSweep
	s.deadlineMu.Unlock()

	swept, err := sweep.Sweep(ctx)
	if err != nil {
		if s.log != nil {
			s.log.Error("list executions past their deadline", "err", err)
		}
		s.observePass(ctx, SweepPassExecutionDeadline, SweepPassOutcomeError, ReapResult{})
		return 0
	}
	result := ReapResult{Inspected: swept.Attempted, Released: swept.Released}
	if swept.Stop == engine.DeadlineSweepStopLeadershipLost {
		// A demoted replica must stop expiring cluster-wide executions; the new
		// leader will pick the remainder up on its own pass.
		s.observePass(ctx, SweepPassExecutionDeadline, SweepPassOutcomeSkippedNotLeader, result)
		return result.Released
	}
	if result.Released > 0 && s.log != nil {
		s.log.Info("expired executions past their deadline",
			"expired", result.Released,
			"backing_off", swept.SkippedBackoff,
			"deferred", swept.Deferred,
		)
	}
	s.observePass(ctx, SweepPassExecutionDeadline, SweepPassOutcomeRan, result)
	return result.Released
}

// executionDeadlineKey is the backoff key of a candidate: it carries the
// namespace as well as the ID, so one tenant's failures never delay another's.
func executionDeadlineKey(candidate engine.DeadlineExpiredExecution) string {
	return engine.DeadlineSweepKey(candidate)
}

// ReapLegacyLeaseMetaOnce drains orphaned pre-U-7 assignment lease metadata at
// its own bounded cadence. Like RepairOnce it is separately leader-gated,
// because this is maintenance over shared state rather than part of any single
// lease's execution, and like RepairOnce it is a no-op for directories that do
// not implement the capability.
//
// Running it from the control plane rather than leaving the legacy hash to a
// runbook step is what makes the U-7 fix complete: the hash has no TTL and no
// reader, so nothing else will ever remove it, and an operator cannot be the
// mechanism for data that may hold task input. The reaper is safe to run
// against a live directory and alongside a previous-version instance — see
// ReapOrphanedLegacyAssignmentLeaseMeta for the reachability argument — so
// there is no window in which this must be disabled.
func (s *LeaseSweeper) ReapLegacyLeaseMetaOnce(ctx context.Context) int {
	if s.elector != nil && !s.elector.IsLeader() {
		s.observePass(ctx, SweepPassLegacyLeaseMeta, SweepPassOutcomeSkippedNotLeader, ReapResult{})
		return 0
	}
	reaper, ok := s.directory.(LegacyLeaseMetaReaper)
	if !ok {
		s.observePass(ctx, SweepPassLegacyLeaseMeta, SweepPassOutcomeUnsupported, ReapResult{})
		return 0
	}

	now := s.clock()
	s.reapMu.Lock()
	if !s.lastReap.IsZero() && now.Sub(s.lastReap) < s.reapPeriod {
		s.reapMu.Unlock()
		s.observePass(ctx, SweepPassLegacyLeaseMeta, SweepPassOutcomeSkippedCadence, ReapResult{})
		return 0
	}
	s.lastReap = now
	s.reapMu.Unlock()

	result, err := reaper.ReapOrphanedLegacyAssignmentLeaseMeta(ctx, s.reapBatch)
	if err != nil {
		// A failure here never blocks lease execution, so it is logged and
		// retried on the next cadence rather than surfaced as a sweep error.
		if s.log != nil {
			s.log.Error("reap orphaned legacy assignment lease metadata", "err", err)
		}
		s.observePass(ctx, SweepPassLegacyLeaseMeta, SweepPassOutcomeError, result)
		return 0
	}
	if result.Released > 0 && s.log != nil {
		s.log.Info("reaped orphaned legacy assignment lease metadata", "reaped", result.Released)
	}
	s.observePass(ctx, SweepPassLegacyLeaseMeta, SweepPassOutcomeRan, result)
	return result.Released
}

// RepairOnce invokes an optional backend lease-index reconciler at its bounded
// cadence. It is separately leader-gated because a reconciliation scan is
// maintenance work rather than part of normal per-lease execution.
//
// It reports through observePassReleased, not observePass: the repair capability
// is implemented by the backend state store, which counts the entries it
// reconciled but not the candidates it examined, so this pass has no honest
// inspected count to publish. A zero there would be an invented measurement.
func (s *LeaseSweeper) RepairOnce(ctx context.Context) int {
	reconciled := s.repairLeaseIndexOnce(ctx)
	// Workflow slot reconciliation rides the repair cadence rather than getting
	// one of its own: the two are the same kind of work — bounded maintenance
	// over a Redis index — and both are no-ops for backends that do not
	// implement them.
	//
	// It deliberately runs whatever the repair did, including when the repair
	// capability is missing or the repair failed: the two reconcile unrelated
	// structures, and a lease index that keeps failing must not also stop
	// leaked slots from being reclaimed. The slot sweep applies its own leader
	// gate, so a non-leader replica still reports a skip for it.
	s.SweepConcurrencySlotsOnce(ctx)
	return reconciled
}

// repairLeaseIndexOnce is the lease-index half of RepairOnce, kept separate so
// the slot sweep that rides along cannot be skipped by any of its exits.
func (s *LeaseSweeper) repairLeaseIndexOnce(ctx context.Context) int {
	if s.elector != nil && !s.elector.IsLeader() {
		s.observePassReleased(ctx, SweepPassRepair, SweepPassOutcomeSkippedNotLeader, 0)
		return 0
	}
	repairer, ok := s.state.(LeaseIndexRepairer)
	if !ok {
		s.observePassReleased(ctx, SweepPassRepair, SweepPassOutcomeUnsupported, 0)
		return 0
	}

	now := s.clock()
	s.repairMu.Lock()
	if !s.lastRepair.IsZero() && now.Sub(s.lastRepair) < s.repairPeriod {
		s.repairMu.Unlock()
		s.observePassReleased(ctx, SweepPassRepair, SweepPassOutcomeSkippedCadence, 0)
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
		s.observePassReleased(ctx, SweepPassRepair, SweepPassOutcomeError, reconciled)
		return 0
	}
	s.observePassReleased(ctx, SweepPassRepair, SweepPassOutcomeRan, reconciled)
	return reconciled
}

// SweepConcurrencySlotsOnce runs the concurrency maintenance passes at the
// lease-repair cadence: workflow slot sets are reconciled against the
// executions that hold them, and queued executions past their queue_timeout are
// driven into the timeout terminal state.
//
// Like RepairOnce it is leader-gated: the slot sets and the queues are
// cluster-wide, so every replica sweeping them would multiply the work without
// changing the outcome. It is deliberately not folded into the repair count — a
// slot set is the limit itself, not derived state, and an operator has to be
// able to tell a lease index that was repaired from a slot that was leaked, or
// a queued submission that gave up from either.
//
// It shares the repair period but not the repair pass's gates: running it from
// SweepOnce's much shorter period would re-read every workflow's slot set every
// few seconds, and routing it through the repair's cadence check would let a
// missing or failing repair stop it entirely.
func (s *LeaseSweeper) SweepConcurrencySlotsOnce(ctx context.Context) int {
	if s.elector != nil && !s.elector.IsLeader() {
		s.observePassReleased(ctx, SweepPassConcurrencySlot, SweepPassOutcomeSkippedNotLeader, 0)
		s.observePassReleased(ctx, SweepPassConcurrencyQueue, SweepPassOutcomeSkippedNotLeader, 0)
		return 0
	}

	// One window, two passes: they are the same maintenance of the same
	// subsystem and run back to back, but each reports under its own name, so a
	// queue timeout that fired is never read as a slot that leaked.
	due := s.concurrencySweepDue(s.clock())

	// The pass observer carries the timing and the count. The timing observer is
	// deliberately not used: its repair series is keyed by result alone, so
	// reporting a second reconciliation through it would double-count the lease
	// index's own repairs in the same series.
	swept := 0
	reconciler, ok := s.state.(ConcurrencySlotReconciler)
	switch {
	case !ok:
		s.observePassReleased(ctx, SweepPassConcurrencySlot, SweepPassOutcomeUnsupported, 0)
	case !due:
		s.observePassReleased(ctx, SweepPassConcurrencySlot, SweepPassOutcomeSkippedCadence, 0)
	default:
		var err error
		swept, err = reconciler.ReconcileConcurrencySlots(ctx, s.repairBatch)
		if err != nil {
			// Same rule as the lease-index repair: a leaked slot costs the
			// workflow capacity, it does not corrupt it, so a failed sweep is
			// retried on the next cadence rather than surfaced as a sweep error.
			if s.log != nil {
				s.log.Error("reconcile workflow concurrency slots", "err", err)
			}
			s.observePassReleased(ctx, SweepPassConcurrencySlot, SweepPassOutcomeError, 0)
			break
		}
		s.observePassReleased(ctx, SweepPassConcurrencySlot, SweepPassOutcomeRan, swept)
	}
	s.expireConcurrencyQueueOnce(ctx, due)
	return swept
}

// expireConcurrencyQueueOnce is the queue-timeout half of the maintenance.
//
// Its capability is checked separately from the slot reconciler's because the
// two are separate interfaces: a backend that can expire a queued submission
// but not reconcile slot sets still has to expire, and a missing slot
// reconciler must not silently stop the queue's clocks.
func (s *LeaseSweeper) expireConcurrencyQueueOnce(ctx context.Context, due bool) {
	expirer, ok := s.state.(engine.ConcurrencyQueueExpirer)
	switch {
	case !ok:
		s.observePassReleased(ctx, SweepPassConcurrencyQueue, SweepPassOutcomeUnsupported, 0)
	case !due:
		s.observePassReleased(ctx, SweepPassConcurrencyQueue, SweepPassOutcomeSkippedCadence, 0)
	default:
		expired, err := expirer.ExpireQueuedExecutions(ctx, s.repairBatch)
		if err != nil {
			if s.log != nil {
				s.log.Error("expire queued executions past their queue timeout", "err", err)
			}
			s.observePassReleased(ctx, SweepPassConcurrencyQueue, SweepPassOutcomeError, expired)
			return
		}
		s.observePassReleased(ctx, SweepPassConcurrencyQueue, SweepPassOutcomeRan, expired)
	}
}

// concurrencySweepDue reports whether the concurrency maintenance is due, and
// stamps the window when it is. Both halves read the same decision: they are one
// pass's worth of work, and a second half that re-checked the clock would run a
// cadence behind the first.
func (s *LeaseSweeper) concurrencySweepDue(now time.Time) bool {
	s.slotSweepMu.Lock()
	defer s.slotSweepMu.Unlock()
	if !s.lastSlotSweep.IsZero() && now.Sub(s.lastSlotSweep) < s.repairPeriod {
		return false
	}
	s.lastSlotSweep = now
	return true
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
	// Deliberately the sweeper's own context, not a per-lease one. This is a
	// single scan spanning every namespace, so there is no tenant it belongs to
	// and no honest way to split one duration across them. The namespace label
	// on the scan metrics therefore reads "default" and means the sweeper, not a
	// tenant; the help text for xflow_lease_sweep_candidates says so.
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
	for i, lease := range expired {
		// The entry gate only proves leadership when the pass started; a
		// bounded scan can outlive it. Re-check periodically so a demoted
		// replica stops reclaiming instead of racing the new leader.
		if i > 0 && i%leaderRecheckInterval == 0 && leaderLost(s.elector) {
			if s.log != nil {
				s.log.Info("lease sweep stopped early",
					"outcome", SweepPassOutcomeSkippedNotLeader,
					"reclaimed", reclaimed,
					"inspected", i,
					"candidates", len(expired),
				)
			}
			return reclaimed
		}
		select {
		case <-ctx.Done():
			return reclaimed
		default:
		}
		// Every observer call below uses leaseCtx, not ctx. ControlPlane.Start
		// builds the sweeper's context from context.Background() and never
		// injects a namespace, while ListExpiredLeases deliberately spans all of
		// them and stamps each ExpiredLease with its owner. The metrics adapters
		// all label through withNamespace, which falls back to "default" on a
		// namespace-less context rather than omitting the label — so every
		// reclaim, race and error the sweeper recorded was filed under
		// namespace="default" whatever tenant it actually belonged to. Not a
		// missing label an operator would notice; a wrong one attributing one
		// tenant's lease failures to another.
		//
		// engine.ReclaimLease already re-derives this same context internally
		// (see engine/lease.go), but that copy is local to the call and never
		// reaches the observer, so it cannot be relied on here.
		//
		// Scoped to the observer calls on purpose. The directory release below
		// keys off AssignmentID and its behaviour under a namespaced context is
		// a separate question from metric labelling; changing what key it looks
		// up is not a metrics fix.
		leaseCtx := ctx
		if lease.Namespace != "" {
			leaseCtx = namespace.WithNamespace(ctx, lease.Namespace)
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
						"ns", string(lease.Namespace),
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
			s.settleFinalizedHandoff(ctx, lease)
			s.observeTiming(func(observer SweepTimingObserver) {
				observer.OnSweepReclaimResult(leaseCtx, "reclaimed", time.Since(reclaimStarted))
			})
			reclaimed++
			if s.observer != nil {
				ageMs := s.clock().Sub(lease.IssuedAt.Add(lease.TTL)).Milliseconds()
				s.observer.OnSweepReclaim(leaseCtx, string(lease.ExecutionID), lease.NodeName, ageMs)
			}
		case ok && err != nil:
			s.settleFinalizedHandoff(ctx, lease)
			// Revoke/outbox applied, but immediate FlushOutbox failed. The
			// lease is already revoked (it will not be re-listed), so count
			// the reclaim as applied and let the durable OutboxDispatcher
			// retry delivery — do not wait for the next lease sweep.
			s.observeTiming(func(observer SweepTimingObserver) {
				observer.OnSweepReclaimResult(leaseCtx, "applied_pending", time.Since(reclaimStarted))
			})
			reclaimed++
			s.recordReclaimApplied(leaseCtx, lease, err)
		case !ok && err == nil:
			// A racing commit/report won; no new mutation was applied.
			s.observeTiming(func(observer SweepTimingObserver) {
				observer.OnSweepReclaimResult(leaseCtx, "race", time.Since(reclaimStarted))
			})
			if s.observer != nil {
				s.observer.OnSweepRace(leaseCtx, string(lease.ExecutionID), lease.NodeName)
			}
		case !ok && err != nil:
			// State mutation not applied; leave the expired lease in place
			// and retry on the next sweep.
			s.observeTiming(func(observer SweepTimingObserver) {
				observer.OnSweepReclaimResult(leaseCtx, "error", time.Since(reclaimStarted))
			})
			if s.log != nil {
				s.log.Error("reclaim lease",
					"ns", string(lease.Namespace),
					"exec", string(lease.ExecutionID),
					"node", lease.NodeName,
					"err", err,
				)
			}
			if s.observer != nil {
				s.observer.OnSweepError(leaseCtx, string(lease.ExecutionID), lease.NodeName, err)
			}
		}
	}
	return reclaimed
}

// settleFinalizedHandoff records the final engine disposition after a
// directory lease record was removed ahead of reclaim. It is deliberately
// best-effort only after a conclusive reclaim: a failed settlement keeps the
// durable debt visible rather than making drain look complete.
func (s *LeaseSweeper) settleFinalizedHandoff(ctx context.Context, lease engine.ExpiredLease) {
	settler, ok := s.directory.(FinalizedHandoffSettler)
	if !ok || settler == nil {
		return
	}
	if err := settler.SettleFinalizedHandoff(ctx,
		BuildAssignmentID(taskFromExpiredLease(&lease)), lease.LeaseID, lease.LeaseToken); err != nil && s.log != nil {
		s.log.Error("settle finalized runner handoff",
			"exec", string(lease.ExecutionID), "node", lease.NodeName, "err", err)
	}
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
		UnitIdx:      lease.UnitIdx,
		ActivationID: lease.ActivationID,
		AutoDepth:    lease.AutoDepth,
		Type:         lease.TaskType,
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

// observePass reports one maintenance pass to the optional pass observers. Both
// counts are whatever the pass measured before returning — including a partial
// count on an error, which is still work that happened. Each observer call is
// guarded separately so a panicking adapter cannot suppress the other one.
func (s *LeaseSweeper) observePass(ctx context.Context, pass, outcome string, result ReapResult) {
	s.observePassReleased(ctx, pass, outcome, result.Released)
	if s.passCandidates == nil {
		return
	}
	defer func() { _ = recover() }()
	s.passCandidates.OnSweepPassCandidates(ctx, pass, outcome, result.Inspected)
}

// observeDeadlineExpired reports one execution timed out by the deadline pass.
// The adapter is guarded like the pass observers: a panicking one must not stop
// the pass.
func (s *LeaseSweeper) observeDeadlineExpired(ctx context.Context) {
	if s.deadlineObs == nil {
		return
	}
	defer func() { _ = recover() }()
	s.deadlineObs.OnExecutionDeadlineExpired(ctx, SweepSourceDeadlinePass)
}

// observePassReleased reports one maintenance pass that has no candidate count
// to report. A pass that cannot count its candidates is deliberately silent on
// them rather than reporting zero, which would read as "inspected nothing".
func (s *LeaseSweeper) observePassReleased(ctx context.Context, pass, outcome string, released int) {
	if s.passObserver == nil {
		return
	}
	defer func() { _ = recover() }()
	s.passObserver.OnSweepPass(ctx, pass, outcome, released)
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

// leaderRecheckInterval is how many candidates a long leader-gated maintenance
// pass processes between leadership re-checks. IsLeader is a local atomic read
// plus a deadline comparison, so re-checking costs no network round trip; 32
// bounds how much work a replica that lost leadership can still apply without
// re-checking on every item.
const leaderRecheckInterval = 32

// leaderLost reports whether a maintenance pass should stop because this
// replica is no longer the leader. A nil gate means leadership is not
// configured (single-replica / dev) and is never lost.
func leaderLost(gate LeaderGate) bool {
	return gate != nil && !gate.IsLeader()
}
