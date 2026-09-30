package metrics

import (
	"context"
	"testing"
)

func TestRunnerDescriptorMetricsCountByReason(t *testing.T) {
	m := New()
	observer := NewRunnerDescriptorMetrics(m)
	ctx := context.Background()

	observer.OnRunnerDescriptorRejected(ctx, "undeclared_type")
	observer.OnRunnerDescriptorRejected(ctx, "undeclared_type")
	observer.OnRunnerDescriptorRejected(ctx, "envelope_too_large")

	counts := map[string]float64{}
	for _, metric := range gatherMetricFamily(t, m, metricRunnerDescriptorRejected).GetMetric() {
		for _, label := range metric.GetLabel() {
			switch label.GetName() {
			case "namespace", "reason":
			default:
				t.Fatalf("label %q is not one of the bounded dimensions", label.GetName())
			}
		}
		counts[labelValue(metric, "reason")] = metric.GetCounter().GetValue()
	}
	if counts["undeclared_type"] != 2 || counts["envelope_too_large"] != 1 || len(counts) != 2 {
		t.Fatalf("rejections = %v, want undeclared_type=2 envelope_too_large=1", counts)
	}
}
