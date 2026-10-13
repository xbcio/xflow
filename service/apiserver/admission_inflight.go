package apiserver

import (
	"net/http"
	"strconv"
	"sync"

	"github.com/xbcio/xflow/observability/metrics"
)

// DefaultAPIMaxInflight is the per-replica inflight bound when
// APILimits.MaxInflight is zero.
const DefaultAPIMaxInflight = 1024

// apiAdmissionRetryAfterOverload is the Retry-After a 503 server_overloaded
// carries: no release time is knowable, so it is a floor.
const apiAdmissionRetryAfterOverload = 1

func (l APILimits) maxInflight() int {
	if l.MaxInflight == 0 {
		return DefaultAPIMaxInflight
	}
	return l.MaxInflight
}

// inflightGuard bounds how many covered requests this replica serves at once.
// It is the overload backstop: unlike the rate limit it is on by default,
// because it answers for this process's own capacity, which no caller quota
// can.
type inflightGuard struct {
	max      int
	mu       sync.Mutex
	total    int
	perClass map[string]int
	metrics  metrics.APIAdmissionMetrics
}

// newInflightGuard returns a guard admitting at most limit requests; a
// non-positive limit returns nil, which admits everything.
func newInflightGuard(limit int, m metrics.APIAdmissionMetrics) *inflightGuard {
	if limit <= 0 {
		return nil
	}
	return &inflightGuard{max: limit, perClass: make(map[string]int), metrics: m}
}

func (g *inflightGuard) acquire(class string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.total >= g.max {
		return false
	}
	g.total++
	g.perClass[class]++
	g.metrics.SetInflight(class, g.perClass[class])
	return true
}

func (g *inflightGuard) release(class string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.total--
	g.perClass[class]--
	g.metrics.SetInflight(class, g.perClass[class])
}

func (g *inflightGuard) inUse() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.total
}

// guard wraps next in the inflight bound. The acquire never blocks: a full
// replica answers at once through reject rather than queueing work it cannot
// start. A nil admission, or one with the bound disabled, returns next as is.
func (a *apiAdmission) guard(class string, reject func(http.ResponseWriter, *http.Request), next http.HandlerFunc) http.HandlerFunc {
	if a == nil || a.inflight == nil {
		return next
	}
	g := a.inflight
	return func(w http.ResponseWriter, r *http.Request) {
		if !g.acquire(class) {
			a.metrics.ObserveRejected(class, "inflight")
			reject(w, r)
			return
		}
		defer g.release(class)
		next(w, r)
	}
}

func writeServerOverloaded(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Retry-After", strconv.Itoa(apiAdmissionRetryAfterOverload))
	writeFail(w, r, http.StatusServiceUnavailable, "server_overloaded", "server is overloaded")
}

// writeWebhookServerOverloaded is writeServerOverloaded in the webhook route's
// own {"error": code} shape.
func writeWebhookServerOverloaded(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Retry-After", strconv.Itoa(apiAdmissionRetryAfterOverload))
	writeWebhookError(w, http.StatusServiceUnavailable, "server_overloaded")
}
