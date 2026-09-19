package metrics

import (
	"context"
	"testing"
)

// The pass family exists for one question the R6 409 investigation could not
// answer: did the stranded-lease reaper's pass at 21:57 release anything?
// The reaper reported through an optional logger that the host never injected,
// so a pass that ran and released nothing looked exactly like a pass that never
// ran — and the two call for opposite follow-ups.
//
// These tests pin the two properties that answer it: outcome="ran" is recorded
// whether or not anything was released, and the released counter carries a
// visible zero for such a pass instead of being absent. Anything that collapses
// those two states back into one fails here.

// findCounterSeries returns the value of the counter series of name whose
// labels include every entry of want. ok is false when no such series exists,
// which is the assertion for "this pass never reported".
func findCounterSeries(t *testing.T, m *Metrics, name string, want map[string]string) (value float64, ok bool) {
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
				if labelValue(metric, key) != expected {
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

func mustCounterSeries(t *testing.T, m *Metrics, name string, want map[string]string) float64 {
	t.Helper()
	value, ok := findCounterSeries(t, m, name, want)
	if !ok {
		t.Fatalf("%s%v has no series, so nothing reported it", name, want)
	}
	return value
}

func TestSweepPassMetricsSeparateARunFromTheCadenceGate(t *testing.T) {
	m := New()
	s := NewSweepMetrics(m)
	ctx := context.Background()

	// One pass that ran and released nothing, then one that the cadence gate
	// skipped. The stranded reaper is the pass the 21:57 409s coincided with.
	s.OnSweepPass(ctx, "stranded_lease", "ran", 0)
	s.OnSweepPass(ctx, "stranded_lease", "skipped_cadence", 0)

	if got := mustCounterSeries(t, m, "xflow_lease_maintenance_pass_total",
		map[string]string{"pass": "stranded_lease", "outcome": "ran"}); got != 1 {
		t.Errorf("outcome=ran = %v, want 1: a pass that ran and released nothing "+
			"must still report that it ran, or it is indistinguishable from a pass "+
			"that never ran at all", got)
	}
	if got := mustCounterSeries(t, m, "xflow_lease_maintenance_pass_total",
		map[string]string{"pass": "stranded_lease", "outcome": "skipped_cadence"}); got != 1 {
		t.Errorf("outcome=skipped_cadence = %v, want 1: the gate that stopped the "+
			"pass has to be visible, otherwise the skip reads as a release of zero", got)
	}

	// The released counter carries a zero for the run. Without this series the
	// only evidence of the run is the pass counter, and an operator reading
	// released_total alone cannot tell "ran, released 0" from "did not run".
	if got := mustCounterSeries(t, m, "xflow_lease_maintenance_released_total",
		map[string]string{"pass": "stranded_lease"}); got != 0 {
		t.Errorf("released_total{pass=stranded_lease} = %v, want a visible 0", got)
	}

	// A pass nobody called has no series at all — that absence IS "never ran",
	// and it is the other half of the distinction.
	for _, pass := range []string{"repair", "legacy_lease_meta", "dead_queued_assignment"} {
		if _, ok := findCounterSeries(t, m, "xflow_lease_maintenance_pass_total",
			map[string]string{"pass": pass, "outcome": "ran"}); ok {
			t.Errorf("pass=%s has an outcome=ran series although nothing ran it", pass)
		}
	}
}

func TestSweepPassMetricsCountReleasesOnlyForPassesThatRan(t *testing.T) {
	m := New()
	s := NewSweepMetrics(m)
	ctx := context.Background()

	s.OnSweepPass(ctx, "repair", "ran", 5)
	s.OnSweepPass(ctx, "legacy_lease_meta", "ran", 0)
	s.OnSweepPass(ctx, "stranded_lease", "ran", 3)
	s.OnSweepPass(ctx, "stranded_lease", "ran", 4)
	s.OnSweepPass(ctx, "dead_queued_assignment", "error", 2)
	// A gate that skipped the pass never contributes releases: a sample here
	// would be indistinguishable from the run above that released nothing.
	s.OnSweepPass(ctx, "dead_queued_assignment", "skipped_not_leader", 0)
	s.OnSweepPass(ctx, "never_ran_only_skipped", "skipped_not_leader", 0)

	for pass, want := range map[string]float64{
		"repair":                 5,
		"legacy_lease_meta":      0,
		"stranded_lease":         7,
		"dead_queued_assignment": 2,
	} {
		if got := mustCounterSeries(t, m, "xflow_lease_maintenance_released_total",
			map[string]string{"pass": pass}); got != want {
			t.Errorf("released_total{pass=%s} = %v, want %v", pass, got, want)
		}
	}

	// Every pass that ran is counted once per call, by outcome.
	if got := mustCounterSeries(t, m, "xflow_lease_maintenance_pass_total",
		map[string]string{"pass": "stranded_lease", "outcome": "ran"}); got != 2 {
		t.Errorf("pass_total{pass=stranded_lease,outcome=ran} = %v, want 2", got)
	}
	if got := mustCounterSeries(t, m, "xflow_lease_maintenance_pass_total",
		map[string]string{"pass": "dead_queued_assignment", "outcome": "error"}); got != 1 {
		t.Errorf("pass_total{pass=dead_queued_assignment,outcome=error} = %v, want 1: "+
			"a pass that failed is a pass that ran, and its partial release still counts", got)
	}

	// The skipped-only pass is reported as a skip and contributes no release.
	if got := mustCounterSeries(t, m, "xflow_lease_maintenance_pass_total",
		map[string]string{"pass": "never_ran_only_skipped", "outcome": "skipped_not_leader"}); got != 1 {
		t.Errorf("pass_total{pass=never_ran_only_skipped,outcome=skipped_not_leader} = %v, want 1", got)
	}
	if got, ok := findCounterSeries(t, m, "xflow_lease_maintenance_released_total",
		map[string]string{"pass": "never_ran_only_skipped"}); ok {
		t.Errorf("released_total{pass=never_ran_only_skipped} = %v, want no series: a "+
			"skipped pass released nothing, and a zero sample here would read as a "+
			"run that released nothing", got)
	}
}
