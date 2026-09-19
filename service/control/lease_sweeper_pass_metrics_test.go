package control

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	backendlocal "github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/observability/metrics"

	dto "github.com/prometheus/client_model/go"
)

// SweepMetrics is the adapter controlplane.go wires as the sweeper's Observer.
// Asserting it here makes a drift between service/control's SweepPassObserver
// and the metrics package's local mirror of it a compile failure, instead of a
// type assertion that quietly stops matching and a pass family that quietly
// stops being emitted.
var _ SweepPassObserver = metrics.SweepMetrics{}

// passMetricsDirectory implements every reaping capability the sweeper
// discovers by type assertion, so one swapper can drive all four passes.
type passMetricsDirectory struct {
	mu                  sync.Mutex
	strandedReleased    int
	strandedInspected   int
	strandedErr         error
	legacyReaped        int
	legacyInspected     int
	legacyErr           error
	deadQueuedReclaimed int
	deadQueuedInspected int
	deadQueuedErr       error
}

func (d *passMetricsDirectory) ReleaseExpiredLease(context.Context, ExpiredDirectoryLeaseRequest) (ExpiredDirectoryLeaseOutcome, error) {
	return ExpiredDirectoryLeaseAlreadyReleased, nil
}

func (d *passMetricsDirectory) ReapStrandedLeases(context.Context, int) (ReapResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return ReapResult{Inspected: d.strandedInspected, Released: d.strandedReleased}, d.strandedErr
}

func (d *passMetricsDirectory) ReapOrphanedLegacyAssignmentLeaseMeta(context.Context, int) (ReapResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return ReapResult{Inspected: d.legacyInspected, Released: d.legacyReaped}, d.legacyErr
}

func (d *passMetricsDirectory) ReapDeadQueuedAssignments(context.Context, int) (ReapResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return ReapResult{Inspected: d.deadQueuedInspected, Released: d.deadQueuedReclaimed}, d.deadQueuedErr
}

// passMetricsState is a LeaseLister that also reconciles a lease index, which
// is the capability the repair pass is gated on.
type passMetricsState struct {
	*fakeLeaseLister
	reconciled   int
	reconcileErr error
}

func (s *passMetricsState) RepairLeaseIndex(context.Context, int) (int, error) {
	return s.reconciled, s.reconcileErr
}

type passEvent struct {
	pass     string
	outcome  string
	released int
}

type recordingPassObserver struct {
	*fakeObserver
	mu     sync.Mutex
	events []passEvent
}

func (o *recordingPassObserver) OnSweepPass(_ context.Context, pass, outcome string, released int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, passEvent{pass: pass, outcome: outcome, released: released})
}

func (o *recordingPassObserver) snapshot() []passEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]passEvent(nil), o.events...)
}

// passTestSweeper bundles a sweeper with the fakes it was built from and the
// clock the caller advances to move a pass across its cadence boundary.
type passTestSweeper struct {
	*LeaseSweeper
	now       *time.Time
	directory *passMetricsDirectory
	state     *passMetricsState
	elector   *fakeElector
}

func newPassTestSweeper(observer SweepObserver) *passTestSweeper {
	directory := &passMetricsDirectory{}
	state := &passMetricsState{fakeLeaseLister: &fakeLeaseLister{}}
	elector := &fakeElector{}
	elector.leader.Store(true)
	sw := NewLeaseSweeper(state, &fakeReclaimer{}, LeaseSweeperConfig{
		Elector:                        elector,
		RunnerDirectory:                directory,
		Observer:                       observer,
		LeaseRepairPeriod:              time.Minute,
		LegacyLeaseMetaReapPeriod:      time.Minute,
		StrandedLeaseReapPeriod:        time.Minute,
		DeadQueuedAssignmentReapPeriod: time.Minute,
	})
	now := time.Date(2026, 9, 19, 21, 52, 0, 0, time.UTC)
	sw.clock = func() time.Time { return now }
	return &passTestSweeper{LeaseSweeper: sw, now: &now, directory: directory, state: state, elector: elector}
}

// maintenancePasses describes each pass the way the tests need to drive it:
// how to run it, how to make it release n (or fail), and how to make its
// capability unavailable.
var maintenancePasses = []struct {
	name        string
	run         func(*LeaseSweeper)
	releases    func(*passMetricsDirectory, *passMetricsState, int, error)
	unsupported func(*passTestSweeper)
}{
	{
		name: SweepPassRepair,
		run:  func(s *LeaseSweeper) { s.RepairOnce(context.Background()) },
		releases: func(_ *passMetricsDirectory, st *passMetricsState, n int, err error) {
			st.reconciled, st.reconcileErr = n, err
		},
		unsupported: func(h *passTestSweeper) { h.LeaseSweeper.state = &fakeLeaseLister{} },
	},
	{
		name: SweepPassLegacyLeaseMeta,
		run:  func(s *LeaseSweeper) { s.ReapLegacyLeaseMetaOnce(context.Background()) },
		releases: func(d *passMetricsDirectory, _ *passMetricsState, n int, err error) {
			d.legacyReaped, d.legacyErr = n, err
		},
		unsupported: func(h *passTestSweeper) { h.LeaseSweeper.directory = nil },
	},
	{
		name: SweepPassStrandedLease,
		run:  func(s *LeaseSweeper) { s.ReapStrandedLeasesOnce(context.Background()) },
		releases: func(d *passMetricsDirectory, _ *passMetricsState, n int, err error) {
			d.strandedReleased, d.strandedErr = n, err
		},
		unsupported: func(h *passTestSweeper) { h.LeaseSweeper.directory = nil },
	},
	{
		name: SweepPassDeadQueuedAssignment,
		run:  func(s *LeaseSweeper) { s.ReapDeadQueuedAssignmentsOnce(context.Background()) },
		releases: func(d *passMetricsDirectory, _ *passMetricsState, n int, err error) {
			d.deadQueuedReclaimed, d.deadQueuedErr = n, err
		},
		unsupported: func(h *passTestSweeper) { h.LeaseSweeper.directory = nil },
	},
}

func TestLeaseSweeperReportsEveryMaintenancePassOutcome(t *testing.T) {
	for _, pass := range maintenancePasses {
		t.Run(pass.name, func(t *testing.T) {
			t.Run("ran counts what it released", func(t *testing.T) {
				observer := &recordingPassObserver{fakeObserver: &fakeObserver{}}
				h := newPassTestSweeper(observer)
				pass.releases(h.directory, h.state, 4, nil)

				pass.run(h.LeaseSweeper)

				want := []passEvent{{pass: pass.name, outcome: SweepPassOutcomeRan, released: 4}}
				if got := observer.snapshot(); !equalPassEvents(got, want) {
					t.Fatalf("pass events = %+v, want %+v", got, want)
				}
			})

			t.Run("ran is reported even when it released nothing", func(t *testing.T) {
				observer := &recordingPassObserver{fakeObserver: &fakeObserver{}}
				h := newPassTestSweeper(observer)

				pass.run(h.LeaseSweeper)

				want := []passEvent{{pass: pass.name, outcome: SweepPassOutcomeRan, released: 0}}
				if got := observer.snapshot(); !equalPassEvents(got, want) {
					t.Fatalf("pass events = %+v, want %+v: a pass that ran and released "+
						"nothing must still report a run", got, want)
				}
			})

			t.Run("cadence gate is reported as a skip", func(t *testing.T) {
				observer := &recordingPassObserver{fakeObserver: &fakeObserver{}}
				h := newPassTestSweeper(observer)
				pass.releases(h.directory, h.state, 4, nil)

				pass.run(h.LeaseSweeper)
				*h.now = h.now.Add(30 * time.Second)
				pass.run(h.LeaseSweeper)

				want := []passEvent{
					{pass: pass.name, outcome: SweepPassOutcomeRan, released: 4},
					{pass: pass.name, outcome: SweepPassOutcomeSkippedCadence},
				}
				if got := observer.snapshot(); !equalPassEvents(got, want) {
					t.Fatalf("pass events = %+v, want %+v", got, want)
				}
			})

			t.Run("leader gate is reported as a skip", func(t *testing.T) {
				observer := &recordingPassObserver{fakeObserver: &fakeObserver{}}
				h := newPassTestSweeper(observer)
				h.elector.leader.Store(false)

				pass.run(h.LeaseSweeper)

				want := []passEvent{{pass: pass.name, outcome: SweepPassOutcomeSkippedNotLeader}}
				if got := observer.snapshot(); !equalPassEvents(got, want) {
					t.Fatalf("pass events = %+v, want %+v", got, want)
				}
			})

			t.Run("a missing capability is reported as unsupported", func(t *testing.T) {
				observer := &recordingPassObserver{fakeObserver: &fakeObserver{}}
				h := newPassTestSweeper(observer)
				pass.unsupported(h)

				pass.run(h.LeaseSweeper)

				want := []passEvent{{pass: pass.name, outcome: SweepPassOutcomeUnsupported}}
				if got := observer.snapshot(); !equalPassEvents(got, want) {
					t.Fatalf("pass events = %+v, want %+v", got, want)
				}
			})

			t.Run("a failure is reported with the work it still did", func(t *testing.T) {
				observer := &recordingPassObserver{fakeObserver: &fakeObserver{}}
				h := newPassTestSweeper(observer)
				pass.releases(h.directory, h.state, 2, errors.New("redis unavailable"))

				pass.run(h.LeaseSweeper)

				want := []passEvent{{pass: pass.name, outcome: SweepPassOutcomeError, released: 2}}
				if got := observer.snapshot(); !equalPassEvents(got, want) {
					t.Fatalf("pass events = %+v, want %+v", got, want)
				}
			})
		})
	}
}

func equalPassEvents(got, want []passEvent) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestLeaseSweeperPassMetricsAnswerWhetherAPassReleasedAnything drives the real
// adapter through a real registry, because that is the only form of the answer
// an operator without a logger gets. It is the R7 acceptance test: the pass the
// 21:57 409s coincided with ran twice, released nothing, and — crucially — is
// reported as having run, not as silence.
func TestLeaseSweeperPassMetricsAnswerWhetherAPassReleasedAnything(t *testing.T) {
	m := metrics.New()
	h := newPassTestSweeper(metrics.NewSweepMetrics(m))

	h.ReapStrandedLeasesOnce(context.Background())
	*h.now = h.now.Add(30 * time.Second)
	h.ReapStrandedLeasesOnce(context.Background())

	if got := mustPassCounter(t, m, "xflow_lease_maintenance_pass_total",
		map[string]string{"pass": SweepPassStrandedLease, "outcome": SweepPassOutcomeRan}); got != 1 {
		t.Fatalf("outcome=ran for the stranded reaper = %v, want 1: the 21:57 pass "+
			"released nothing, and without this series that is indistinguishable "+
			"from a pass that never ran", got)
	}
	if got := mustPassCounter(t, m, "xflow_lease_maintenance_pass_total",
		map[string]string{"pass": SweepPassStrandedLease, "outcome": SweepPassOutcomeSkippedCadence}); got != 1 {
		t.Fatalf("outcome=skipped_cadence = %v, want 1", got)
	}
	if got := mustPassCounter(t, m, "xflow_lease_maintenance_released_total",
		map[string]string{"pass": SweepPassStrandedLease}); got != 0 {
		t.Fatalf("released_total{pass=stranded_lease} = %v, want a visible 0", got)
	}

	// The other three passes were never called on this sweeper, and that absence
	// is exactly what "never ran" looks like — a different reading from the zero
	// above.
	for _, pass := range []string{SweepPassRepair, SweepPassLegacyLeaseMeta, SweepPassDeadQueuedAssignment} {
		if _, ok := findPassCounter(t, m, "xflow_lease_maintenance_pass_total",
			map[string]string{"pass": pass}); ok {
			t.Errorf("pass=%s has a series although nothing ran it", pass)
		}
	}
}

func TestLeaseSweeperPassMetricsCountWhatEachPassReleased(t *testing.T) {
	m := metrics.New()
	h := newPassTestSweeper(metrics.NewSweepMetrics(m))
	h.directory.legacyReaped = 3
	h.directory.strandedReleased = 5
	h.directory.deadQueuedReclaimed = 7
	h.state.reconciled = 11

	h.ReapLegacyLeaseMetaOnce(context.Background())
	h.ReapStrandedLeasesOnce(context.Background())
	h.ReapDeadQueuedAssignmentsOnce(context.Background())
	h.RepairOnce(context.Background())

	for pass, want := range map[string]float64{
		SweepPassLegacyLeaseMeta:      3,
		SweepPassStrandedLease:        5,
		SweepPassDeadQueuedAssignment: 7,
		SweepPassRepair:               11,
	} {
		if got := mustPassCounter(t, m, "xflow_lease_maintenance_released_total",
			map[string]string{"pass": pass}); got != want {
			t.Errorf("released_total{pass=%s} = %v, want %v", pass, got, want)
		}
		if got := mustPassCounter(t, m, "xflow_lease_maintenance_pass_total",
			map[string]string{"pass": pass, "outcome": SweepPassOutcomeRan}); got != 1 {
			t.Errorf("pass_total{pass=%s,outcome=ran} = %v, want 1", pass, got)
		}
	}
}

// TestLeaseSweeperPassMetricsReportALeaderGatedSkipNotARelease pins the skip
// side of the contract: a replica that is not the leader reports the gate for
// every pass and contributes no release sample, so a skip can never be read as
// a pass that ran and released nothing.
func TestLeaseSweeperPassMetricsReportALeaderGatedSkipNotARelease(t *testing.T) {
	m := metrics.New()
	h := newPassTestSweeper(metrics.NewSweepMetrics(m))
	h.elector.leader.Store(false)
	// Non-zero on the fakes on purpose: nothing may reach them.
	h.directory.strandedReleased = 5
	h.directory.legacyReaped = 3
	h.directory.deadQueuedReclaimed = 7
	h.state.reconciled = 11

	h.RepairOnce(context.Background())
	h.ReapLegacyLeaseMetaOnce(context.Background())
	h.ReapStrandedLeasesOnce(context.Background())
	h.ReapDeadQueuedAssignmentsOnce(context.Background())

	for _, pass := range []string{SweepPassRepair, SweepPassLegacyLeaseMeta, SweepPassStrandedLease, SweepPassDeadQueuedAssignment} {
		if got := mustPassCounter(t, m, "xflow_lease_maintenance_pass_total",
			map[string]string{"pass": pass, "outcome": SweepPassOutcomeSkippedNotLeader}); got != 1 {
			t.Errorf("pass_total{pass=%s,outcome=skipped_not_leader} = %v, want 1", pass, got)
		}
		if got, ok := findPassCounter(t, m, "xflow_lease_maintenance_pass_total",
			map[string]string{"pass": pass, "outcome": SweepPassOutcomeRan}); ok {
			t.Errorf("pass_total{pass=%s,outcome=ran} = %v although the replica is not "+
				"the leader", pass, got)
		}
		if got, ok := findPassCounter(t, m, "xflow_lease_maintenance_released_total",
			map[string]string{"pass": pass}); ok {
			t.Errorf("released_total{pass=%s} = %v for a skipped pass, want no series", pass, got)
		}
	}
}

// TestNewControlPlaneWiresThePassObserver is the wiring proof for the whole
// family, on the production assembly path rather than on a hand-built sweeper:
// controlplane.go assigns metrics.NewSweepMetrics to sweeperCfg.Observer, and
// that one value now also carries the pass extension, so no second wiring line
// was needed. If the adapter ever stops satisfying SweepPassObserver — the
// failure mode a silently drifting mirror interface produces — the sweeper
// reports to nothing and this test catches it.
func TestNewControlPlaneWiresThePassObserver(t *testing.T) {
	m := metrics.New()
	cp, err := NewControlPlane(Config{Backend: backendlocal.New(), Metrics: m})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	if cp.sweeper.passObserver == nil {
		t.Fatal("NewControlPlane() did not wire a SweepPassObserver into the LeaseSweeper; " +
			"a host that injects no logger would again have no way to read whether a " +
			"maintenance pass ran")
	}

	cp.sweeper.ReapStrandedLeasesOnce(context.Background())

	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, family := range families {
		if family.GetName() != "xflow_lease_maintenance_pass_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			if passMetricLabel(metric, "pass") == SweepPassStrandedLease {
				return
			}
		}
	}
	t.Fatal("xflow_lease_maintenance_pass_total has no stranded_lease series after the " +
		"pass ran, so the assembled control plane does not reach the pass metrics")
}

// findPassCounter returns the value of one series of a counter in the registry,
// or ok=false when that series was never created.
func findPassCounter(t *testing.T, m *metrics.Metrics, name string, want map[string]string) (float64, bool) {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			matches := true
			for key, expected := range want {
				if passMetricLabel(metric, key) != expected {
					matches = false
					break
				}
			}
			if matches {
				return metric.GetCounter().GetValue(), true
			}
		}
	}
	return 0, false
}

func mustPassCounter(t *testing.T, m *metrics.Metrics, name string, want map[string]string) float64 {
	t.Helper()
	value, ok := findPassCounter(t, m, name, want)
	if !ok {
		t.Fatalf("%s%v has no series, so nothing reported it", name, want)
	}
	return value
}

func passMetricLabel(metric *dto.Metric, name string) string {
	for _, label := range metric.GetLabel() {
		if label.GetName() == name {
			return label.GetValue()
		}
	}
	return ""
}
