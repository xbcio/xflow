package metrics

import "testing"

// TestAPIAdmissionMetricsPartitionRejectionsByReason pins that a rate refusal
// and an inflight refusal are different series: one is the caller's quota and
// the other is this server's state, and an operator who cannot tell them apart
// cannot tell "raise the tenant's rate" from "add a replica".
func TestAPIAdmissionMetricsPartitionRejectionsByReason(t *testing.T) {
	m := New()
	a := NewAPIAdmissionMetrics(m)

	a.ObserveRejected("execute", "rate")
	a.ObserveRejected("execute", "rate")
	a.ObserveRejected("webhook", "inflight")

	got := map[string]float64{}
	for _, metric := range gatherMetricFamily(t, m, metricAPIRateLimited).GetMetric() {
		got[labelValue(metric, "route")+"/"+labelValue(metric, "reason")] = metric.GetCounter().GetValue()
	}
	want := map[string]float64{"execute/rate": 2, "webhook/inflight": 1}
	if len(got) != len(want) {
		t.Fatalf("series = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v, want %v", k, got[k], v)
		}
	}
}

func TestAPIAdmissionMetricsGauges(t *testing.T) {
	m := New()
	a := NewAPIAdmissionMetrics(m)

	a.SetInflight("execute", 3)
	a.SetInflight("execute", 2)
	a.SetLimiterBuckets(7)
	a.ObserveRateLimitFallback()

	inflight := gatherMetricFamily(t, m, metricAPIInflight).GetMetric()
	if len(inflight) != 1 || inflight[0].GetGauge().GetValue() != 2 || labelValue(inflight[0], "route_class") != "execute" {
		t.Fatalf("inflight = %v, want one execute series at 2", inflight)
	}
	if v := gatherMetricFamily(t, m, metricAPILimiterBuckets).GetMetric()[0].GetGauge().GetValue(); v != 7 {
		t.Fatalf("buckets = %v, want 7", v)
	}
	if v := gatherMetricFamily(t, m, metricAPIRateLimitFallback).GetMetric()[0].GetCounter().GetValue(); v != 1 {
		t.Fatalf("fallback = %v, want 1", v)
	}
}

// A server built without metrics must count into nothing rather than panic.
func TestAPIAdmissionMetricsNilRegistryIsNoOp(t *testing.T) {
	a := NewAPIAdmissionMetrics(nil)
	a.ObserveRejected("execute", "rate")
	a.SetInflight("execute", 1)
	a.SetLimiterBuckets(1)
	a.ObserveRateLimitFallback()
}
