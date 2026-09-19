package control

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	backendlocal "github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/observability/metrics"
)

// The metrics adapter is what controlplane.go wires, and the candidate observer
// is discovered from it by type assertion. Asserting the mirror here makes a
// drift between service/control's SweepPassCandidateObserver and the metrics
// package's local mirror of it a compile failure, instead of an assertion that
// quietly stops matching and a candidate family that quietly stops existing.
var (
	_ SweepPassCandidateObserver = metrics.SweepMetrics{}
	_ SweepPassCandidateObserver = (*recordingCandidateObserver)(nil)
)

type candidateEvent struct {
	pass      string
	outcome   string
	inspected int
}

// recordingCandidateObserver records the candidate half of the pass contract. It
// embeds the pass recorder, so it satisfies SweepPassCandidateObserver — which
// embeds SweepPassObserver — and one value captures both events of a pass.
type recordingCandidateObserver struct {
	*recordingPassObserver
	mu         sync.Mutex
	candidates []candidateEvent
}

func newRecordingCandidateObserver() *recordingCandidateObserver {
	return &recordingCandidateObserver{
		recordingPassObserver: &recordingPassObserver{fakeObserver: &fakeObserver{}},
	}
}

func (o *recordingCandidateObserver) OnSweepPassCandidates(_ context.Context, pass, outcome string, inspected int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.candidates = append(o.candidates, candidateEvent{pass: pass, outcome: outcome, inspected: inspected})
}

func (o *recordingCandidateObserver) candidateSnapshot() []candidateEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]candidateEvent(nil), o.candidates...)
}

// candidatePasses drives each reaping pass the same way. The repair pass is
// deliberately absent: its capability lives in the backend state store, which
// reports no candidate count —
// TestLeaseSweeperRepairPassDoesNotReportCandidates pins that separately.
var candidatePasses = []struct {
	name   string
	run    func(*LeaseSweeper)
	counts func(*passMetricsDirectory, *passMetricsState, int, int)
}{
	{
		name: SweepPassLegacyLeaseMeta,
		run:  func(s *LeaseSweeper) { s.ReapLegacyLeaseMetaOnce(context.Background()) },
		counts: func(d *passMetricsDirectory, _ *passMetricsState, inspected, released int) {
			d.legacyInspected, d.legacyReaped = inspected, released
		},
	},
	{
		name: SweepPassStrandedLease,
		run:  func(s *LeaseSweeper) { s.ReapStrandedLeasesOnce(context.Background()) },
		counts: func(d *passMetricsDirectory, _ *passMetricsState, inspected, released int) {
			d.strandedInspected, d.strandedReleased = inspected, released
		},
	},
	{
		name: SweepPassDeadQueuedAssignment,
		run:  func(s *LeaseSweeper) { s.ReapDeadQueuedAssignmentsOnce(context.Background()) },
		counts: func(d *passMetricsDirectory, _ *passMetricsState, inspected, released int) {
			d.deadQueuedInspected, d.deadQueuedReclaimed = inspected, released
		},
	},
}

// TestLeaseSweeperReportsInspectedCandidatesForEveryPass is the requirement
// itself: every reaping pass reports how many candidates it inspected, beside
// how many it released, through the observation path that exists for a host with
// no logger.
func TestLeaseSweeperReportsInspectedCandidatesForEveryPass(t *testing.T) {
	for _, pass := range candidatePasses {
		t.Run(pass.name, func(t *testing.T) {
			observer := newRecordingCandidateObserver()
			h := newPassTestSweeper(observer)
			pass.counts(h.directory, h.state, 7, 2)

			pass.run(h.LeaseSweeper)

			want := []candidateEvent{{pass: pass.name, outcome: SweepPassOutcomeRan, inspected: 7}}
			if got := observer.candidateSnapshot(); !equalCandidateEvents(got, want) {
				t.Fatalf("candidate events = %+v, want %+v", got, want)
			}
			// The released half is unchanged and still reported beside it.
			wantPass := []passEvent{{pass: pass.name, outcome: SweepPassOutcomeRan, released: 2}}
			if got := observer.snapshot(); !equalPassEvents(got, wantPass) {
				t.Fatalf("pass events = %+v, want %+v", got, wantPass)
			}
		})
	}
}

// TestLeaseSweeperPassCandidateMetricsShowScopeDivergence drives the real adapter
// through a real registry for the two readings the pair exists to separate: a
// pass whose scope is far wider than the work it finds, and a pass that releases
// everything it inspected.
func TestLeaseSweeperPassCandidateMetricsShowScopeDivergence(t *testing.T) {
	m := metrics.New()
	h := newPassTestSweeper(metrics.NewSweepMetrics(m))
	h.directory.strandedInspected, h.directory.strandedReleased = 100, 3
	h.directory.legacyInspected, h.directory.legacyReaped = 4, 4

	h.ReapStrandedLeasesOnce(context.Background())
	h.ReapLegacyLeaseMetaOnce(context.Background())

	// Inspected far more than released: the pass walks a shape far wider than
	// the one it drains, which is the defect this count makes visible.
	if got := mustPassCounter(t, m, "xflow_lease_maintenance_candidates_total",
		map[string]string{"pass": SweepPassStrandedLease}); got != 100 {
		t.Fatalf("candidates_total{pass=stranded_lease} = %v, want 100", got)
	}
	if got := mustPassCounter(t, m, "xflow_lease_maintenance_released_total",
		map[string]string{"pass": SweepPassStrandedLease}); got != 3 {
		t.Fatalf("released_total{pass=stranded_lease} = %v, want 3", got)
	}

	// Inspected exactly what it released: nothing is being held back.
	if got := mustPassCounter(t, m, "xflow_lease_maintenance_candidates_total",
		map[string]string{"pass": SweepPassLegacyLeaseMeta}); got != 4 {
		t.Fatalf("candidates_total{pass=legacy_lease_meta} = %v, want 4", got)
	}
	if got := mustPassCounter(t, m, "xflow_lease_maintenance_released_total",
		map[string]string{"pass": SweepPassLegacyLeaseMeta}); got != 4 {
		t.Fatalf("released_total{pass=legacy_lease_meta} = %v, want 4", got)
	}
}

// TestLeaseSweeperPassCandidateMetricsReportAPartiallyFailedPass pins the error
// semantics: a pass that failed part-way reported what it inspected before the
// failure, exactly as it reports the releases it already made.
func TestLeaseSweeperPassCandidateMetricsReportAPartiallyFailedPass(t *testing.T) {
	// The observer contract first: one value records both halves of the event.
	observer := newRecordingCandidateObserver()
	h := newPassTestSweeper(observer)
	h.directory.strandedInspected, h.directory.strandedReleased = 9, 2
	h.directory.strandedErr = errors.New("redis unavailable")

	h.ReapStrandedLeasesOnce(context.Background())

	wantEvents := []candidateEvent{{pass: SweepPassStrandedLease, outcome: SweepPassOutcomeError, inspected: 9}}
	if got := observer.candidateSnapshot(); !equalCandidateEvents(got, wantEvents) {
		t.Fatalf("candidate events = %+v, want %+v: a failed pass still reports what "+
			"it inspected before the failure", got, wantEvents)
	}
	wantPass := []passEvent{{pass: SweepPassStrandedLease, outcome: SweepPassOutcomeError, released: 2}}
	if got := observer.snapshot(); !equalPassEvents(got, wantPass) {
		t.Fatalf("pass events = %+v, want %+v", got, wantPass)
	}

	// Then the exported series, through the adapter the control plane wires.
	m := metrics.New()
	h = newPassTestSweeper(metrics.NewSweepMetrics(m))
	h.directory.strandedInspected, h.directory.strandedReleased = 9, 2
	h.directory.strandedErr = errors.New("redis unavailable")

	h.ReapStrandedLeasesOnce(context.Background())

	if got := mustPassCounter(t, m, "xflow_lease_maintenance_candidates_total",
		map[string]string{"pass": SweepPassStrandedLease}); got != 9 {
		t.Fatalf("candidates_total{pass=stranded_lease} = %v, want 9", got)
	}
	if got := mustPassCounter(t, m, "xflow_lease_maintenance_released_total",
		map[string]string{"pass": SweepPassStrandedLease}); got != 2 {
		t.Fatalf("released_total{pass=stranded_lease} = %v, want 2", got)
	}
}

// TestLeaseSweeperPassCandidateMetricsPublishAVisibleZero pins the zero case at
// the sweeper: a pass that ran and inspected nothing carries a visible 0 rather
// than no sample, so "found nothing to inspect" and "never reached the call" stay
// distinguishable — the same property the released counter already has.
func TestLeaseSweeperPassCandidateMetricsPublishAVisibleZero(t *testing.T) {
	m := metrics.New()
	h := newPassTestSweeper(metrics.NewSweepMetrics(m))

	h.ReapStrandedLeasesOnce(context.Background())

	if got := mustPassCounter(t, m, "xflow_lease_maintenance_candidates_total",
		map[string]string{"pass": SweepPassStrandedLease}); got != 0 {
		t.Fatalf("candidates_total{pass=stranded_lease} = %v, want a visible 0", got)
	}
	if got := mustPassCounter(t, m, "xflow_lease_maintenance_pass_total",
		map[string]string{"pass": SweepPassStrandedLease, "outcome": SweepPassOutcomeRan}); got != 1 {
		t.Fatalf("pass_total{outcome=ran} = %v, want 1", got)
	}
}

// TestLeaseSweeperPassCandidateMetricsSkipGatedPasses keeps a gated pass out of
// the candidate family entirely: a zero would be indistinguishable from a pass
// that inspected nothing, which is the same confusion the released counter
// already avoids.
func TestLeaseSweeperPassCandidateMetricsSkipGatedPasses(t *testing.T) {
	m := metrics.New()
	h := newPassTestSweeper(metrics.NewSweepMetrics(m))
	h.directory.strandedInspected = 5
	h.directory.strandedReleased = 1

	h.ReapStrandedLeasesOnce(context.Background())
	*h.now = h.now.Add(30 * time.Second)
	h.ReapStrandedLeasesOnce(context.Background()) // cadence gate
	h.elector.leader.Store(false)
	*h.now = h.now.Add(time.Minute)
	h.ReapStrandedLeasesOnce(context.Background()) // leader gate

	if got, ok := findPassCounter(t, m, "xflow_lease_maintenance_candidates_total",
		map[string]string{"pass": SweepPassStrandedLease}); !ok || got != 5 {
		t.Fatalf("candidates_total{pass=stranded_lease} = %v (present=%v), want exactly "+
			"the one run's 5", got, ok)
	}
	if got := mustPassCounter(t, m, "xflow_lease_maintenance_pass_total",
		map[string]string{"pass": SweepPassStrandedLease, "outcome": SweepPassOutcomeSkippedCadence}); got != 1 {
		t.Fatalf("skipped_cadence = %v, want 1", got)
	}
	if got := mustPassCounter(t, m, "xflow_lease_maintenance_pass_total",
		map[string]string{"pass": SweepPassStrandedLease, "outcome": SweepPassOutcomeSkippedNotLeader}); got != 1 {
		t.Fatalf("skipped_not_leader = %v, want 1", got)
	}
}

// TestLeaseSweeperRepairPassDoesNotReportCandidates pins the deliberate gap. The
// repair capability is implemented by the backend state store, which reports a
// reconciled count but no candidate count, so this pass reports its releases and
// stays silent on candidates rather than publishing an invented zero.
func TestLeaseSweeperRepairPassDoesNotReportCandidates(t *testing.T) {
	observer := newRecordingCandidateObserver()
	h := newPassTestSweeper(observer)
	h.state.reconciled = 11

	h.RepairOnce(context.Background())

	wantPass := []passEvent{{pass: SweepPassRepair, outcome: SweepPassOutcomeRan, released: 11}}
	if got := observer.snapshot(); !equalPassEvents(got, wantPass) {
		t.Fatalf("pass events = %+v, want %+v", got, wantPass)
	}
	if got := observer.candidateSnapshot(); len(got) != 0 {
		t.Fatalf("candidate events = %+v, want none for the repair pass", got)
	}

	m := metrics.New()
	h = newPassTestSweeper(metrics.NewSweepMetrics(m))
	h.state.reconciled = 11

	h.RepairOnce(context.Background())

	if got := mustPassCounter(t, m, "xflow_lease_maintenance_released_total",
		map[string]string{"pass": SweepPassRepair}); got != 11 {
		t.Fatalf("released_total{pass=repair} = %v, want 11", got)
	}
	if got, ok := findPassCounter(t, m, "xflow_lease_maintenance_candidates_total",
		map[string]string{"pass": SweepPassRepair}); ok {
		t.Fatalf("candidates_total{pass=repair} = %v, want no series: the repair "+
			"capability cannot count candidates, so zero would be invented", got)
	}
}

// TestNewControlPlaneWiresThePassCandidateObserver is the wiring proof on the
// production assembly path: controlplane.go assigns metrics.NewSweepMetrics to
// sweeperCfg.Observer, and that one value carries the candidate extension too, so
// no second wiring line was needed. If the adapter ever stops satisfying
// SweepPassCandidateObserver, the count silently disappears and this catches it.
func TestNewControlPlaneWiresThePassCandidateObserver(t *testing.T) {
	m := metrics.New()
	cp, err := NewControlPlane(Config{Backend: backendlocal.New(), Metrics: m})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	if cp.sweeper.passCandidates == nil {
		t.Fatal("NewControlPlane() did not wire a SweepPassCandidateObserver into the " +
			"LeaseSweeper; a pass would again report what it released with no way to " +
			"read how much it inspected to release it")
	}

	// The in-memory directory does not implement the reaper capability, so this
	// pass reports unsupported and publishes no candidate sample. A zero here
	// would claim the deployment inspected nothing, when it never inspected.
	cp.sweeper.ReapStrandedLeasesOnce(context.Background())

	if got := mustPassCounter(t, m, "xflow_lease_maintenance_pass_total",
		map[string]string{"pass": SweepPassStrandedLease, "outcome": SweepPassOutcomeUnsupported}); got != 1 {
		t.Fatalf("pass_total{pass=stranded_lease,outcome=unsupported} = %v, want 1", got)
	}
	if got, ok := findPassCounter(t, m, "xflow_lease_maintenance_candidates_total",
		map[string]string{"pass": SweepPassStrandedLease}); ok {
		t.Fatalf("candidates_total{pass=stranded_lease} = %v, want no series for a pass "+
			"whose capability is missing", got)
	}
}

func equalCandidateEvents(got, want []candidateEvent) bool {
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
