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
	a.ObserveRateLimitFallback("redis_error")
	a.ObserveRateLimitFallback("redis_error")
	a.ObserveRateLimitFallback("breaker_trip")

	inflight := gatherMetricFamily(t, m, metricAPIInflight).GetMetric()
	if len(inflight) != 1 || inflight[0].GetGauge().GetValue() != 2 || labelValue(inflight[0], "route_class") != "execute" {
		t.Fatalf("inflight = %v, want one execute series at 2", inflight)
	}
	if v := gatherMetricFamily(t, m, metricAPILimiterBuckets).GetMetric()[0].GetGauge().GetValue(); v != 7 {
		t.Fatalf("buckets = %v, want 7", v)
	}
	// The reasons are separate series: a trip is an event, and the decisions
	// that skip Redis while the breaker is open are not the same count.
	fallbacks := gatherMetricFamily(t, m, metricAPIRateLimitFallback).GetMetric()
	if len(fallbacks) != 2 {
		t.Fatalf("fallback series = %v, want two (redis_error, breaker_trip)", fallbacks)
	}
	for _, metric := range fallbacks {
		want, ok := map[string]float64{"redis_error": 2, "breaker_trip": 1}[labelValue(metric, "reason")]
		if !ok || metric.GetCounter().GetValue() != want {
			t.Fatalf("fallback series %q = %v, want %v", labelValue(metric, "reason"), metric.GetCounter().GetValue(), want)
		}
	}
}

// A server built without metrics must count into nothing rather than panic.
func TestAPIAdmissionMetricsNilRegistryIsNoOp(t *testing.T) {
	a := NewAPIAdmissionMetrics(nil)
	a.ObserveRejected("execute", "rate")
	a.SetInflight("execute", 1)
	a.SetLimiterBuckets(1)
	a.ObserveRateLimitFallback("redis_error")
	a.ObserveRateLimitFallback("breaker_open")
}
