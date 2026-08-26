package metrics

import (
	"context"
	"testing"
	"time"
)

// Three genuinely different sweep outcomes share ONE counter and are told apart
// only by a string literal written here:
//
//	OnSweepReclaim        -> result="reclaimed"        work reclaimed and delivered
//	OnSweepRace           -> result="race"             another sweeper got there first
//	OnSweepReclaimApplied -> result="applied_pending"  state applied, delivery NOT confirmed
//
// Only the first literal was ever asserted (metrics_test.go's adapter sweep
// calls OnSweepReclaim). The other two could be typed as "reclaimed" and the
// suite would not notice, which is not a cosmetic mislabel: applied_pending
// means the synchronous outbox flush failed and the durable dispatcher still
// has to retry, so reporting it as reclaimed claims work was delivered when it
// was not, and race reports a reclaim this replica did not perform.
//
// The same shape appears in ReconcileMetrics.OnReconcileSettled, where a bool
// picks between appended and duplicate. service/control's tests assert on the
// observer, not on the registry, so the bool-to-label mapping lives here alone.

func TestSweepOutcomesReachTheRegistryAsDistinctResults(t *testing.T) {
	m := New()
	s := NewSweepMetrics(m)
	ctx := context.Background()

	s.OnSweepReclaim(ctx, "exec-1", "node-1", 1500)
	s.OnSweepRace(ctx, "exec-2", "node-2")
	s.OnSweepReclaimApplied(ctx, "exec-3", "node-3", 900)

	byResult := map[string]float64{}
	for _, metric := range gatherMetricFamily(t, m, "xflow_lease_sweep_reclaimed_total").GetMetric() {
		byResult[labelValue(metric, "result")] += metric.GetCounter().GetValue()
	}

	if len(byResult) != 3 {
		t.Fatalf("three sweep outcomes produced %d series (%v); at least two of "+
			"them are being reported as the same event", len(byResult), byResult)
	}
	for _, want := range []string{"reclaimed", "race", "applied_pending"} {
		if byResult[want] != 1 {
			t.Errorf("result=%q counted %v, want 1 (all results: %v)", want, byResult[want], byResult)
		}
	}
}

func TestSweepLeaseAgeIsRecordedOnlyForOutcomesThatHaveOne(t *testing.T) {
	// The age histogram is shared the same way, and a race carries no age at all
	// — the lease belonged to somebody else. Observing a zero for it would drag
	// the p50 of a real reclaim age toward zero and make an aging-lease alert
	// unusable, so the absence of that series is the assertion.
	m := New()
	s := NewSweepMetrics(m)
	ctx := context.Background()

	s.OnSweepReclaim(ctx, "exec-1", "node-1", 2000)
	s.OnSweepRace(ctx, "exec-2", "node-2")

	seen := map[string]uint64{}
	for _, metric := range gatherMetricFamily(t, m, "xflow_lease_age_seconds").GetMetric() {
		seen[labelValue(metric, "result")] = metric.GetHistogram().GetSampleCount()
	}
	if _, ok := seen["race"]; ok {
		t.Fatalf("a lost race contributed a lease-age observation (%v); it has no "+
			"age to report and its zero pulls the reclaim-age distribution down", seen)
	}
	if seen["reclaimed"] != 1 {
		t.Fatalf("reclaim lease-age observations = %d, want 1 (all: %v)", seen["reclaimed"], seen)
	}
	if got := gatherMetricFamily(t, m, "xflow_lease_age_seconds").GetMetric(); len(got) > 0 {
		var sum float64
		for _, metric := range got {
			if labelValue(metric, "result") == "reclaimed" {
				sum = metric.GetHistogram().GetSampleSum()
			}
		}
		// 2000ms in, 2 seconds out. The argument is milliseconds and the
		// histogram is seconds, so a missing conversion is a 1000x error that
		// no count-only assertion can see.
		if sum != 2 {
			t.Fatalf("lease age sum = %v seconds for a 2000ms lease, want 2", sum)
		}
	}
}

func TestSweepReclaimResultRecordsCountAndDurationUnderTheSameResult(t *testing.T) {
	// OnSweepReclaimResult writes a counter and a duration histogram from one
	// label map. metrics_test.go asserts the counter only, so the histogram
	// could be dropped, or given a different result label, and the two series
	// would stop being joinable without anything failing.
	m := New()
	NewSweepMetrics(m).OnSweepReclaimResult(context.Background(), "conflict", 250*time.Millisecond)

	count := gatherMetricFamily(t, m, "xflow_lease_reclaim_total").GetMetric()
	dur := gatherMetricFamily(t, m, "xflow_lease_reclaim_duration_seconds").GetMetric()
	if len(count) != 1 || len(dur) != 1 {
		t.Fatalf("want one series each, got count=%d duration=%d", len(count), len(dur))
	}
	if got := labelValue(dur[0], "result"); got != "conflict" {
		t.Fatalf("duration result label = %q, want conflict: the duration cannot "+
			"be broken down by the outcome it belongs to", got)
	}
	if got := dur[0].GetHistogram().GetSampleSum(); got != 0.25 {
		t.Fatalf("duration sum = %v seconds for a 250ms reclaim, want 0.25", got)
	}
}

func TestSweepErrorsAreCountedSeparatelyFromOutcomes(t *testing.T) {
	// An error is not a fourth value of result — it lands on its own metric. If
	// it were folded into the outcome counter, the reclaimed/race/applied ratio
	// an operator reads would silently include failures.
	m := New()
	s := NewSweepMetrics(m)
	s.OnSweepError(context.Background(), "exec-1", "node-1", assertErr{})

	for _, metric := range gatherMetricFamily(t, m, "xflow_lease_sweep_errors_total").GetMetric() {
		if got := labelValue(metric, "reason"); got == "" {
			t.Fatal("sweep errors carry no reason label; a second error source added " +
				"later would be indistinguishable from a reclaim failure")
		}
		if got := metric.GetCounter().GetValue(); got != 1 {
			t.Fatalf("sweep errors = %v, want 1", got)
		}
	}
	if metricFamilyExists(t, m, "xflow_lease_sweep_reclaimed_total") {
		t.Fatal("a sweep error also incremented the outcome counter; the " +
			"reclaimed/race ratio now includes failures")
	}
}

func TestReconcileSettledDistinguishesAppendedFromDuplicate(t *testing.T) {
	// Delivery is at-least-once by design, so duplicates are expected and their
	// MAGNITUDE is the evidence for whether the duplicate window is worth
	// closing. Inverting the bool makes that number report its own complement,
	// which is worse than not having it: a healthy pipeline would read as
	// duplicating everything, and a pipeline that had started duplicating would
	// read as clean.
	m := New()
	r := NewReconcileMetrics(m)
	ctx := context.Background()

	r.OnReconcileSettled(ctx, "success", true, 10)
	r.OnReconcileSettled(ctx, "success", false, 10)
	r.OnReconcileSettled(ctx, "success", false, 10)

	byResult := map[string]float64{}
	for _, metric := range gatherMetricFamily(t, m, "xflow_audit_reconcile_settled_total").GetMetric() {
		if got := labelValue(metric, "outcome"); got != "success" {
			t.Fatalf("outcome label = %q, want success: the outcome and result "+
				"arguments have been crossed", got)
		}
		byResult[labelValue(metric, "result")] += metric.GetCounter().GetValue()
	}
	if byResult["appended"] != 1 || byResult["duplicate"] != 2 {
		t.Fatalf("settled by result = %v, want appended=1 duplicate=2", byResult)
	}
}

func metricFamilyExists(t *testing.T, m *Metrics, name string) bool {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, family := range families {
		if family.GetName() == name {
			return true
		}
	}
	return false
}
