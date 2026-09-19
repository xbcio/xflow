package metrics

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/namespace"
)

// TestReportRejectionMetricsAttributeByReason pins the R6 acceptance criterion
// at the metrics boundary: each rejection reason is its own series, so a 409
// rate can be read per fence instead of as one undifferentiated number.
func TestReportRejectionMetricsAttributeByReason(t *testing.T) {
	m := New()
	observer := NewReportRejectionMetrics(m)
	ctx := context.Background()

	observer.OnReportRejected(ctx, "engine_stale_token")
	observer.OnReportRejected(ctx, "engine_stale_token")
	observer.OnReportRejected(ctx, "directory_lease_not_found")
	observer.OnReportRejectionDivergence(ctx)

	counts := map[string]float64{}
	for _, metric := range gatherMetricFamily(t, m, metricReportRejections).GetMetric() {
		counts[labelValue(metric, "reason")] = metric.GetCounter().GetValue()
	}
	for reason, want := range map[string]float64{
		"engine_stale_token":        2,
		"directory_lease_not_found": 1,
	} {
		if got := counts[reason]; got != want {
			t.Errorf("rejections[%q] = %v, want %v (all: %v)", reason, got, want, counts)
		}
	}
	if len(counts) != 2 {
		t.Fatalf("rejection series = %v, want exactly the two reasons emitted", counts)
	}

	divergence := 0.0
	for _, metric := range gatherMetricFamily(t, m, metricLeaseDivergence).GetMetric() {
		divergence += metric.GetCounter().GetValue()
	}
	if divergence != 1 {
		t.Fatalf("divergence = %v, want 1", divergence)
	}
}

// TestReportRejectionMetricsCarryNoHighCardinalityLabels keeps runner, lease,
// execution, and node identity out of the label set. The reason set is closed,
// but the values come from a caller, so this is the guard that a future caller
// cannot turn an identifier into a label.
func TestReportRejectionMetricsCarryNoHighCardinalityLabels(t *testing.T) {
	m := New()
	observer := NewReportRejectionMetrics(m)
	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("tenant-a"))

	observer.OnReportRejected(ctx, "engine_stale_token")
	observer.OnReportRejectionDivergence(ctx)

	for _, name := range []string{metricReportRejections, metricLeaseDivergence} {
		for _, metric := range gatherMetricFamily(t, m, name).GetMetric() {
			for _, label := range metric.GetLabel() {
				switch label.GetName() {
				case "namespace", "reason":
				default:
					t.Fatalf("%s label %q is not one of the bounded dimensions", name, label.GetName())
				}
			}
		}
	}
}
