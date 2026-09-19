package control

import (
	"context"
	"errors"
	"sync"
	"time"

	observabilitymetrics "github.com/xbcio/xflow/observability/metrics"
)

const (
	runnerControlMetricsCollectPeriod = 15 * time.Second
)

// RunnerControlMetricsCollector converts the control directory's per-runner
// projections into the fleet-only snapshot accepted by RunnerControlObserver.
// It keeps completion identities process-local and never sends them to the
// observer, so runner IDs cannot become metric labels.
type RunnerControlMetricsCollector struct {
	lister   ActivationRunnerLister
	observer observabilitymetrics.RunnerControlObserver
	now      func() time.Time

	mu        sync.Mutex
	completed map[runnerDrainCompletionKey]struct{}
}

type runnerDrainCompletionKey struct {
	runnerID   string
	generation uint64
}

// NewRunnerControlMetricsCollector constructs a collector for a directory
// capable of listing live runners. A nil observer is replaced with the shared
// noop implementation, allowing callers to keep metrics optional.
func NewRunnerControlMetricsCollector(lister ActivationRunnerLister, observer observabilitymetrics.RunnerControlObserver) *RunnerControlMetricsCollector {
	return &RunnerControlMetricsCollector{
		lister:    lister,
		observer:  runnerControlMetricsObserver(observer),
		now:       time.Now,
		completed: make(map[runnerDrainCompletionKey]struct{}),
	}
}

// Collect publishes one complete fleet projection. A completed drain duration
// is emitted at most once for each (runner ID, control generation) pair while
// that generation remains in the draining fleet. The identity is local
// bookkeeping only; it is never included in the observer payload or labels.
func (c *RunnerControlMetricsCollector) Collect(ctx context.Context) {
	if c == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	observer := runnerControlMetricsObserver(c.observer)
	fleet := observabilitymetrics.RunnerControlFleetSnapshot{}
	activeDrains := make(map[runnerDrainCompletionKey]struct{})
	completed := make(map[runnerDrainCompletionKey]time.Duration)

	if c.lister != nil {
		now := time.Now()
		if c.now != nil {
			now = c.now()
		}
		for _, runner := range c.lister.ListLiveRunners(ctx) {
			control := runner.Control
			if control == nil || control.DesiredState != RunnerDesiredStateDraining {
				continue
			}

			fleet.DrainingCount++
			key := runnerDrainCompletionKey{runnerID: runner.RunnerID, generation: control.Generation}
			if runner.RunnerID != "" {
				activeDrains[key] = struct{}{}
			}

			drain := control.Drain
			if drain == nil {
				fleet.AwaitingRunnerAcknowledgementCount++
				continue
			}
			fleet.HandoffDebtCount += nonNegativeRunnerControlMetricCount(drain.HandoffDebt)
			fleet.ReplayDebtCount += nonNegativeRunnerControlMetricCount(drain.ReplayableDebt)
			fleet.PendingActivationReceiptCount += nonNegativeRunnerControlMetricCount(drain.PendingActivationCleanup)
			if !drain.RunnerQuiescent {
				fleet.AwaitingRunnerAcknowledgementCount++
			}
			if drain.Phase == RunnerDrainPhaseTimedOut {
				fleet.OverdueDrainCount++
			}
			if drain.Phase != RunnerDrainPhaseComplete || control.RequestedAt == nil || control.RequestedAt.IsZero() || runner.RunnerID == "" {
				continue
			}
			if elapsed := now.Sub(*control.RequestedAt); elapsed >= 0 {
				completed[key] = elapsed
			}
		}
	}

	var newCompletions []time.Duration
	c.mu.Lock()
	if c.completed == nil {
		c.completed = make(map[runnerDrainCompletionKey]struct{})
	}
	for key := range c.completed {
		if _, stillDraining := activeDrains[key]; !stillDraining {
			delete(c.completed, key)
		}
	}
	for key, elapsed := range completed {
		if _, alreadyObserved := c.completed[key]; alreadyObserved {
			continue
		}
		c.completed[key] = struct{}{}
		newCompletions = append(newCompletions, elapsed)
	}
	c.mu.Unlock()

	observer.ObserveRunnerControlSnapshot(ctx, fleet)
	for _, elapsed := range newCompletions {
		observer.OnRunnerDrainCompleted(ctx, elapsed)
	}
}

// run repeatedly refreshes fleet gauges because heartbeat, handoff, and
// activation-cleanup changes can complete a drain without a new management
// mutation.
func (c *RunnerControlMetricsCollector) run(ctx context.Context, interval time.Duration) {
	if c == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if interval <= 0 {
		interval = runnerControlMetricsCollectPeriod
	}

	c.Collect(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.Collect(ctx)
		}
	}
}

func runnerControlMetricsObserver(observer observabilitymetrics.RunnerControlObserver) observabilitymetrics.RunnerControlObserver {
	if observer == nil {
		return observabilitymetrics.NoopRunnerControlObserver{}
	}
	return observer
}

func nonNegativeRunnerControlMetricCount(value int) int {
	if value < 0 {
		return 0
	}
	return value
}

// runnerControlMetricsDirectory observes the optional management-control
// extension without changing the directory passed to the dispatcher, runner
// protocol, or sweeper. It is exposed only through ControlPlane.RunnerDirectory.
type runnerControlMetricsDirectory struct {
	RunnerDirectory

	control   RunnerControlDirectory
	observer  observabilitymetrics.RunnerControlObserver
	collector *RunnerControlMetricsCollector
}

func newRunnerControlMetricsDirectory(directory RunnerDirectory, control RunnerControlDirectory, observer observabilitymetrics.RunnerControlObserver, collector *RunnerControlMetricsCollector) *runnerControlMetricsDirectory {
	return &runnerControlMetricsDirectory{
		RunnerDirectory: directory,
		control:         control,
		observer:        runnerControlMetricsObserver(observer),
		collector:       collector,
	}
}

// SetRunnerControl preserves the underlying directory's result exactly while
// recording only closed action/result values. Best-effort before-state lookup
// distinguishes an actual transition, a no-op, and an idempotent historical
// receipt replay without putting request or runner identities in metrics.
func (d *runnerControlMetricsDirectory) SetRunnerControl(ctx context.Context, req RunnerControlRequest) (RunnerControlSnapshot, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var before RunnerControlSnapshot
	beforeFound := false
	if d.control != nil {
		var err error
		before, beforeFound, err = d.control.RunnerControl(ctx, req.RunnerID)
		if err != nil {
			beforeFound = false
		}
	}

	snapshot, err := d.control.SetRunnerControl(ctx, req)
	result := runnerControlMetricResult(before, beforeFound, snapshot, req, err)
	d.observer.OnRunnerControlTransition(ctx, runnerControlMetricAction(req), result)
	if err == nil && d.collector != nil {
		d.collector.Collect(ctx)
	}
	return snapshot, err
}

// RunnerControl forwards the optional management read capability.
func (d *runnerControlMetricsDirectory) RunnerControl(ctx context.Context, runnerID string) (RunnerControlSnapshot, bool, error) {
	return d.control.RunnerControl(ctx, runnerID)
}

// RunnerControlState forwards the lightweight projection so wrapping a directory
// does not silently push recurring callers back onto the debt-aggregating read.
func (d *runnerControlMetricsDirectory) RunnerControlState(ctx context.Context, runnerID string) (RunnerControlState, bool, error) {
	states, ok := d.control.(RunnerControlStateDirectory)
	if !ok || states == nil {
		snapshot, found, err := d.control.RunnerControl(ctx, runnerID)
		if err != nil || !found {
			return RunnerControlState{}, found, err
		}
		return RunnerControlState{DesiredState: snapshot.DesiredState, Generation: snapshot.Generation}, true, nil
	}
	return states.RunnerControlState(ctx, runnerID)
}

func runnerControlMetricAction(req RunnerControlRequest) string {
	switch req.DesiredState {
	case RunnerDesiredStateDraining:
		return observabilitymetrics.RunnerControlActionDrain
	case RunnerDesiredStateActive:
		return observabilitymetrics.RunnerControlActionResume
	default:
		return observabilitymetrics.RunnerControlActionOther
	}
}

func runnerControlMetricResult(before RunnerControlSnapshot, beforeFound bool, after RunnerControlSnapshot, req RunnerControlRequest, err error) string {
	switch {
	case err == nil:
		if !beforeFound || after.Generation > before.Generation {
			return observabilitymetrics.RunnerControlResultTransitioned
		}
		// A receipt from an earlier generation is a replay even when the
		// current desired state has moved on. The same condition covers custom
		// directories that return an older desired-state projection.
		if after.Generation < before.Generation || after.DesiredState != before.DesiredState || after.DesiredState != req.DesiredState {
			return observabilitymetrics.RunnerControlResultReplayed
		}
		return observabilitymetrics.RunnerControlResultUnchanged
	case errors.Is(err, ErrRunnerControlRequestConflict):
		return observabilitymetrics.RunnerControlResultConflict
	case errors.Is(err, ErrRunnerNotFound):
		return observabilitymetrics.RunnerControlResultNotFound
	case errors.Is(err, ErrRunnerControlInvalidState), errors.Is(err, ErrRunnerControlRequestIDRequired), errors.Is(err, ErrRunnerIDRequired):
		return observabilitymetrics.RunnerControlResultInvalid
	default:
		return observabilitymetrics.RunnerControlResultError
	}
}

// runnerControlMetricsRunnerLister mirrors the management module's optional
// list capability. It is separate from ActivationRunnerLister: returning this
// method only when the underlying directory supports it avoids making an
// unsupported management list endpoint appear to be available.
type runnerControlMetricsRunnerLister interface {
	ListRunners(context.Context) ([]string, error)
}

type runnerControlMetricsListDirectory struct {
	*runnerControlMetricsDirectory
	lister runnerControlMetricsRunnerLister
}

func (d *runnerControlMetricsListDirectory) ListRunners(ctx context.Context) ([]string, error) {
	return d.lister.ListRunners(ctx)
}

// newRunnerControlMetricsManagementDirectory decorates only the directory
// returned by ControlPlane.RunnerDirectory. The raw directory remains available
// to process-internal components, preserving their structural capabilities.
func newRunnerControlMetricsManagementDirectory(directory RunnerDirectory, observer observabilitymetrics.RunnerControlObserver) (RunnerDirectory, *RunnerControlMetricsCollector) {
	control, supported := directory.(RunnerControlDirectory)
	if !supported || control == nil {
		return directory, nil
	}

	var collector *RunnerControlMetricsCollector
	if lister, ok := directory.(ActivationRunnerLister); ok && lister != nil {
		collector = NewRunnerControlMetricsCollector(lister, observer)
	}
	adapter := newRunnerControlMetricsDirectory(directory, control, observer, collector)
	if lister, ok := directory.(runnerControlMetricsRunnerLister); ok && lister != nil {
		return &runnerControlMetricsListDirectory{runnerControlMetricsDirectory: adapter, lister: lister}, collector
	}
	return adapter, collector
}
