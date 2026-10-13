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
	max int
	// webhookMax bounds the webhook class below max. The webhook route is
	// unauthenticated until its body has been read and its HMAC verified, so
	// without a sub-limit an unsigned flood would hold every slot an
	// authenticated execute needs. Derived, not configured: max/4, at least 1.
	webhookMax int
	mu         sync.Mutex
	total      int
	perClass   map[string]int
	metrics    metrics.APIAdmissionMetrics
}

// newInflightGuard returns a guard admitting at most limit requests; a
// non-positive limit returns nil, which admits everything.
func newInflightGuard(limit int, m metrics.APIAdmissionMetrics) *inflightGuard {
	if limit <= 0 {
		return nil
	}
	return &inflightGuard{
		max:        limit,
		webhookMax: max(1, limit/4),
		perClass:   make(map[string]int),
		metrics:    m,
	}
}

func (g *inflightGuard) acquire(class string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.total >= g.max {
		return false
	}
	if class == routeClassWebhook && g.perClass[class] >= g.webhookMax {
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

func noRelease() {}

// acquire takes one inflight slot for class without blocking. A full replica
// answers at once rather than queueing work it cannot start. A nil admission,
// or one with the bound disabled, always admits.
func (a *apiAdmission) acquire(class string) (release func(), ok bool) {
	if a == nil || a.inflight == nil {
		return noRelease, true
	}
	g := a.inflight
	if !g.acquire(class) {
		a.metrics.ObserveRejected(class, "inflight")
		return nil, false
	}
	return func() { g.release(class) }, true
}

// guard wraps next in the inflight bound, answering a full replica through
// reject. The release is deferred, so a panicking handler still frees its slot.
func (a *apiAdmission) guard(class string, reject func(http.ResponseWriter, *http.Request), next http.HandlerFunc) http.HandlerFunc {
	if a == nil || a.inflight == nil {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		release, ok := a.acquire(class)
		if !ok {
			reject(w, r)
			return
		}
		defer release()
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
