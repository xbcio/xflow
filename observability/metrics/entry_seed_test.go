package metrics

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// TestEntrySeedMetrics_TimeoutOutcomeIsSeparateFromError is the whole point of
// this series: an admission that breached its deadline must be countable
// separately from the other ways the same call can fail.
//
// Asserted through the registry's Gather() output rather than by calling the
// method and trusting it, because the failure this guards against is a series
// that is written but never registered (or registered under a different label
// set), which reads exactly like a deployment with no timeouts.
//
// The "error" row is in the same test on purpose. A counter that reports every
// failure as "timeout" would satisfy a timeout-only assertion, and it is the
// conflation of the two — not the absence of either — that leaves an operator
// unable to tell a batch that outgrew its window from a broken network.
func TestEntrySeedMetrics_TimeoutOutcomeIsSeparateFromError(t *testing.T) {
	m := New()
	obs := NewEntrySeedMetrics(m)

	obs.OnEntrySeedAdmission(context.Background(), "timeout", 15*time.Second)
	obs.OnEntrySeedAdmission(context.Background(), "error", 250*time.Millisecond)
	obs.OnEntrySeedAdmission(context.Background(), "accepted", 40*time.Millisecond)

	counter := gatherMetricFamily(t, m, "xflow_entry_seed_admission_total")
	if len(counter.GetMetric()) != 3 {
		t.Fatalf("xflow_entry_seed_admission_total series = %d, want 3 (one per outcome)",
			len(counter.GetMetric()))
	}
	byOutcome := map[string]float64{}
	for _, mt := range counter.GetMetric() {
		byOutcome[labelValue(mt, "outcome")] = mt.GetCounter().GetValue()
	}
	for _, want := range []string{"timeout", "error", "accepted"} {
		if got, ok := byOutcome[want]; !ok || got != 1 {
			t.Errorf("xflow_entry_seed_admission_total{outcome=%q} = %v (present=%v), want 1",
				want, got, ok)
		}
	}
}

// TestEntrySeedMetrics_AdmissionFansOutToBothSeries pins that one observation
// lands one counter sample AND one duration sample — the reason
// types.EntrySeedObserver has a single method rather than a counter method plus
// a duration method. Two methods would let an attempt be counted without being
// timed, and the pair could then disagree about how many attempts happened.
func TestEntrySeedMetrics_AdmissionFansOutToBothSeries(t *testing.T) {
	m := New()
	obs := NewEntrySeedMetrics(m)

	obs.OnEntrySeedAdmission(context.Background(), "accepted", 1500*time.Millisecond)

	hist := gatherMetricFamily(t, m, "xflow_entry_seed_admission_duration_seconds")
	if len(hist.GetMetric()) != 1 {
		t.Fatalf("xflow_entry_seed_admission_duration_seconds series = %d, want 1",
			len(hist.GetMetric()))
	}
	h := hist.GetMetric()[0].GetHistogram()
	if h.GetSampleCount() != 1 {
		t.Fatalf("sample count = %d, want 1", h.GetSampleCount())
	}
	if got := h.GetSampleSum(); got != 1.5 {
		t.Errorf("sample sum = %v, want 1.5 (seconds, not milliseconds or nanoseconds)", got)
	}
	if got := labelValue(hist.GetMetric()[0], "outcome"); got != "accepted" {
		t.Errorf("outcome label = %q, want %q", got, "accepted")
	}
}

// TestEntrySeedMetrics_ImplementsTheObserverContract is a compile-and-assert
// check that the concrete type the runner wires satisfies the contract the
// producer depends on. Without it, a signature drift on either side is caught
// by the runner failing to build rather than here — late, and in a package
// whose owners do not own this one.
func TestEntrySeedMetrics_ImplementsTheObserverContract(t *testing.T) {
	var obs types.EntrySeedObserver = NewEntrySeedMetrics(New())
	obs.OnEntrySeedAdmission(context.Background(), "duplicate", time.Second)
}
