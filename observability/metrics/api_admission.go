package metrics

// API admission metric names.
const (
	metricAPIRateLimited       = "xflow_api_rate_limited_total"
	metricAPIInflight          = "xflow_api_inflight"
	metricAPILimiterBuckets    = "xflow_api_limiter_buckets"
	metricAPIRateLimitFallback = "xflow_api_rate_limit_fallback_total"
)

// APIAdmissionMetrics reports the API-layer admission gates in front of the
// mutating /v1 routes and the webhook entry route: the per-replica inflight
// bound and the per-namespace token bucket.
//
// Labels are closed enums chosen by the caller:
//
//   - route is the route class the gate protected: "execute" (the two execute
//     routes), "execution" (signal, revoke, cancel, pause, resume, rerun) or
//     "webhook" (the server-hosted webhook entry).
//   - reason is "rate" (the namespace's token bucket was empty, answered 429) or
//     "inflight" (the replica was at its inflight bound, answered 503).
//
// Deliberately absent: the namespace and the workflow. The bucket key carries
// them, but a counter labelled by them would let one noisy tenant's rejections
// mint series on every replica, and the operator question — "is this server
// shedding load, and why" — is answered without them.
type APIAdmissionMetrics struct {
	Metrics *Metrics
}

// NewAPIAdmissionMetrics creates an admission observer backed by m. A nil m
// records nothing.
func NewAPIAdmissionMetrics(m *Metrics) APIAdmissionMetrics {
	return APIAdmissionMetrics{Metrics: m}
}

// ObserveRejected counts one refused request.
func (a APIAdmissionMetrics) ObserveRejected(route, reason string) {
	a.Metrics.Inc(metricAPIRateLimited, map[string]string{"route": route, "reason": reason})
}

// SetInflight reports how many requests of one route class this replica is
// serving right now.
func (a APIAdmissionMetrics) SetInflight(routeClass string, n int) {
	a.Metrics.Set(metricAPIInflight, map[string]string{"route_class": routeClass}, float64(n))
}

// SetLimiterBuckets reports how many per-key token buckets this replica holds
// in memory, which is what the idle-bucket sweep bounds.
func (a APIAdmissionMetrics) SetLimiterBuckets(n int) {
	a.Metrics.Set(metricAPILimiterBuckets, map[string]string{}, float64(n))
}

// ObserveRateLimitFallback counts one shared-mode rate decision that did not
// consult Redis, by reason: "redis_error" (the Redis call failed and this
// replica's local bucket decided instead), "breaker_trip" (the run of
// consecutive failures opened the breaker — one increment per trip, not per
// failed call), or "breaker_open" (the breaker skipped the Redis call during its
// cooldown).
func (a APIAdmissionMetrics) ObserveRateLimitFallback(reason string) {
	a.Metrics.Inc(metricAPIRateLimitFallback, map[string]string{"reason": reason})
}
