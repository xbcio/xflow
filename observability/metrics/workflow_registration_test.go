package metrics

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/namespace"
)

// TestWorkflowRegistrationMetricsPartitionByOperationOutcome pins the R4
// observability requirement at the metrics boundary: a registration that failed
// and one that succeeded must be different series, because "did my workflow
// register" is the question the counter exists to answer and a single
// undifferentiated count answers nothing.
func TestWorkflowRegistrationMetricsPartitionByOperationOutcome(t *testing.T) {
	m := New()
	observer := NewWorkflowRegistrationMetrics(m)
	ctx := namespace.WithNamespace(context.Background(), "tenant-a")

	observer.ObserveRegistration(ctx, "replace", "registered")
	observer.ObserveRegistration(ctx, "add", "error")
	observer.ObserveRegistration(ctx, "add", "error")

	got := map[string]float64{}
	for _, metric := range gatherMetricFamily(t, m, metricWorkflowRegistration).GetMetric() {
		key := labelValue(metric, "operation") + "/" + labelValue(metric, "outcome")
		got[key] = metric.GetCounter().GetValue()
		if ns := labelValue(metric, "namespace"); ns != "tenant-a" {
			t.Errorf("namespace label = %q, want tenant-a", ns)
		}
	}
	for key, want := range map[string]float64{
		"replace/registered": 1,
		"add/error":          2,
	} {
		if got[key] != want {
			t.Errorf("%s = %v, want %v (all: %v)", key, got[key], want, got)
		}
	}
	if len(got) != 2 {
		t.Fatalf("series = %v, want exactly the two attempted outcomes", got)
	}
}

// TestWorkflowRegistrationMetricsAbsentOutcomeStaysAbsent is the shape the alert
// depends on: a process whose registration failed publishes no
// outcome="registered" sample at all, rather than a zero. A zero would make
// "registration never ran" and "registration is not wired" indistinguishable
// from a successful one on a dashboard that averages or sums.
func TestWorkflowRegistrationMetricsAbsentOutcomeStaysAbsent(t *testing.T) {
	m := New()
	observer := NewWorkflowRegistrationMetrics(m)

	observer.ObserveRegistration(context.Background(), "add", "error")

	for _, metric := range gatherMetricFamily(t, m, metricWorkflowRegistration).GetMetric() {
		if outcome := labelValue(metric, "outcome"); outcome == "registered" {
			t.Fatal("a failed attempt published an outcome=\"registered\" series; " +
				"the alert on a missing registration cannot tell them apart")
		}
	}
}

// TestWorkflowRegistrationMetricsNilRegistryIsNoOp pins that a Server built
// without WithServerMetrics — the dev and test default — counts into nothing
// rather than panicking.
func TestWorkflowRegistrationMetricsNilRegistryIsNoOp(t *testing.T) {
	observer := NewWorkflowRegistrationMetrics(nil)
	observer.ObserveRegistration(context.Background(), "add", "registered")
}
