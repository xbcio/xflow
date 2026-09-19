package metrics

import (
	"context"
	"testing"
)

// The candidate family answers the question the released count alone cannot:
// is a pass releasing everything it inspects, or inspecting far more than it
// releases — a scope wider than the shape it exists to drain, or a batch smaller
// than the backlog? These tests pin the properties that make the pair readable:
// the candidate count is reported for a pass that ran (including as a visible
// zero), a failed pass still reports what it inspected, and a skipped pass
// reports neither.

func TestSweepPassCandidateMetricsReportWhatAPassInspected(t *testing.T) {
	m := New()
	s := NewSweepMetrics(m)
	ctx := context.Background()

	s.OnSweepPass(ctx, "stranded_lease", "ran", 3)
	s.OnSweepPassCandidates(ctx, "stranded_lease", "ran", 100)
	// A pass that ran and found nothing to inspect: a visible zero, not an
	// absence, for the same reason the released counter carries one.
	s.OnSweepPass(ctx, "legacy_lease_meta", "ran", 0)
	s.OnSweepPassCandidates(ctx, "legacy_lease_meta", "ran", 0)
	// A gate that skipped the pass contributes no candidate sample, and neither
	// does a pass whose capability cannot count candidates (repair).
	s.OnSweepPass(ctx, "dead_queued_assignment", "skipped_cadence", 0)
	s.OnSweepPassCandidates(ctx, "dead_queued_assignment", "skipped_cadence", 0)
	s.OnSweepPass(ctx, "repair", "ran", 5)

	if got := mustCounterSeries(t, m, "xflow_lease_maintenance_candidates_total",
		map[string]string{"pass": "stranded_lease"}); got != 100 {
		t.Errorf("candidates_total{pass=stranded_lease} = %v, want 100: the pass walked "+
			"100 candidates to release 3, which is the divergence the series exists "+
			"to show", got)
	}
	if got := mustCounterSeries(t, m, "xflow_lease_maintenance_released_total",
		map[string]string{"pass": "stranded_lease"}); got != 3 {
		t.Errorf("released_total{pass=stranded_lease} = %v, want 3", got)
	}
	if got := mustCounterSeries(t, m, "xflow_lease_maintenance_candidates_total",
		map[string]string{"pass": "legacy_lease_meta"}); got != 0 {
		t.Errorf("candidates_total{pass=legacy_lease_meta} = %v, want a visible 0", got)
	}
	if got := mustCounterSeries(t, m, "xflow_lease_maintenance_candidates_total",
		map[string]string{"pass": "stranded_lease"}); got ==
		mustCounterSeries(t, m, "xflow_lease_maintenance_released_total",
			map[string]string{"pass": "stranded_lease"}) {
		t.Error("inspected and released are equal for a pass driven with a 100-vs-3 " +
			"split, so the pair cannot answer whether a pass releases what it inspects")
	}
	for _, pass := range []string{"dead_queued_assignment", "repair"} {
		if got, ok := findCounterSeries(t, m, "xflow_lease_maintenance_candidates_total",
			map[string]string{"pass": pass}); ok {
			t.Errorf("candidates_total{pass=%s} = %v, want no series", pass, got)
		}
	}
}

func TestSweepPassCandidateMetricsReportAPartiallyFailedPass(t *testing.T) {
	m := New()
	s := NewSweepMetrics(m)
	ctx := context.Background()

	s.OnSweepPass(ctx, "stranded_lease", "error", 2)
	s.OnSweepPassCandidates(ctx, "stranded_lease", "error", 9)
	s.OnSweepPass(ctx, "stranded_lease", "ran", 4)
	s.OnSweepPassCandidates(ctx, "stranded_lease", "ran", 4)

	// The failed pass still carries what it inspected before it failed: the
	// count is a measurement, not a success flag, and dropping it would hide the
	// candidates a repeatedly failing pass keeps walking.
	if got := mustCounterSeries(t, m, "xflow_lease_maintenance_candidates_total",
		map[string]string{"pass": "stranded_lease"}); got != 13 {
		t.Errorf("candidates_total{pass=stranded_lease} = %v, want 13 (9 from the failed "+
			"pass and 4 from the one that ran)", got)
	}
	if got := mustCounterSeries(t, m, "xflow_lease_maintenance_released_total",
		map[string]string{"pass": "stranded_lease"}); got != 6 {
		t.Errorf("released_total{pass=stranded_lease} = %v, want 6", got)
	}
}
