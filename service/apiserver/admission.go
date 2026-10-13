package apiserver

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"

	"github.com/xbcio/xflow/backend/providers/distributed/ratelimit"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/service/control"
)

// API rate limit modes: where the rate is counted.
const (
	// APIRateLimitModeLocal counts each replica's rate in its own memory: the
	// configured rate is one replica's share, and the fleet admits rate×replicas.
	APIRateLimitModeLocal = "local"
	// APIRateLimitModeRedis counts the rate in one Redis bucket per key shared by
	// every replica, at the cost of a Redis round trip per rate-limited request.
	// When Redis does not answer, the decision falls back to this replica's local
	// bucket (fail open) and xflow_api_rate_limit_fallback_total counts it.
	APIRateLimitModeRedis = "redis"
)

// APILimits configures the API admission layer in front of the mutating
// workflow/execution routes and the webhook entry route.
//
// The zero value is the default posture: no rate limit, and at most
// DefaultAPIMaxInflight requests in flight per replica.
type APILimits struct {
	// Rate is the sustained requests per second one namespace may make. Zero
	// disables rate limiting.
	Rate float64
	// Burst is how many requests a namespace may make at once on top of an idle
	// bucket. Zero derives it from Rate (at least 1).
	Burst int
	// MaxInflight bounds the covered requests this replica serves concurrently.
	// Zero means DefaultAPIMaxInflight; negative disables the bound.
	MaxInflight int
	// Mode is APIRateLimitModeLocal (the default when empty) or
	// APIRateLimitModeRedis.
	Mode string
}

// validate reports a configuration this server cannot honour.
func (l APILimits) validate() error {
	if l.Rate < 0 || math.IsNaN(l.Rate) || math.IsInf(l.Rate, 0) {
		return fmt.Errorf("apiserver: APILimits.Rate must be a finite non-negative number, got %v", l.Rate)
	}
	if l.Burst < 0 {
		return fmt.Errorf("apiserver: APILimits.Burst must not be negative, got %d", l.Burst)
	}
	switch l.Mode {
	case "", APIRateLimitModeLocal, APIRateLimitModeRedis:
	default:
		return fmt.Errorf("apiserver: APILimits.Mode must be %q or %q, got %q", APIRateLimitModeLocal, APIRateLimitModeRedis, l.Mode)
	}
	return nil
}

func (l APILimits) burst() int {
	if l.Burst > 0 {
		return l.Burst
	}
	return max(1, int(math.Ceil(l.Rate)))
}

// Route classes: the label values the admission metrics carry. Closed set.
const (
	routeClassExecute   = "execute"
	routeClassExecution = "execution"
	routeClassWebhook   = "webhook"
)

const (
	// sharedRateLimitTimeout caps the Redis round trip of one shared-mode
	// decision, so a slow Redis costs a bounded delay before falling back.
	sharedRateLimitTimeout = 250 * time.Millisecond
	// sharedRateLimitWarnEvery rate-limits the fallback warning: a Redis outage
	// would otherwise log once per request.
	sharedRateLimitWarnEvery = 30 * time.Second
)

// apiAdmission holds the admission gates. A nil *apiAdmission admits
// everything, so a module built without New (unit tests) is unaffected.
type apiAdmission struct {
	inflight *inflightGuard
	limiter  rateDecider
	metrics  metrics.APIAdmissionMetrics
}

// rateDecider decides one request against key's bucket and, when it refuses,
// says how long until a token will be available.
type rateDecider interface {
	allow(ctx context.Context, key string) (bool, time.Duration)
}

// newAPIAdmission builds the admission layer. shared is the Redis bucket for
// APIRateLimitModeRedis and is ignored otherwise.
func newAPIAdmission(l APILimits, shared *ratelimit.TokenBucket, m *metrics.Metrics, log engine.Logger) *apiAdmission {
	a := &apiAdmission{metrics: metrics.NewAPIAdmissionMetrics(m)}
	a.inflight = newInflightGuard(l.maxInflight(), a.metrics)
	if l.Rate > 0 {
		local := newLocalRateLimiter(rate.Limit(l.Rate), l.burst(), a.metrics)
		a.limiter = local
		if l.Mode == APIRateLimitModeRedis && shared != nil {
			a.limiter = &sharedRateLimiter{
				bucket: shared, rate: l.Rate, burst: l.burst(),
				local: local, metrics: a.metrics, log: log, now: time.Now,
			}
		}
	}
	return a
}

// allowRate decides one request of class against key's bucket. It reports the
// Retry-After seconds a refusal carries (at least 1).
func (a *apiAdmission) allowRate(ctx context.Context, class, key string) (bool, int) {
	if a == nil || a.limiter == nil {
		return true, 0
	}
	ok, wait := a.limiter.allow(ctx, key)
	if ok {
		return true, 0
	}
	a.metrics.ObserveRejected(class, "rate")
	return false, retryAfterSeconds(wait)
}

// admissionRouteClass maps an operation to the route class the admission
// layer covers, or reports that the operation is not covered. Both auth paths
// key on it: the authz wrapper with its Op constants, the bare wrapper with
// its op labels (two of which differ from the Op constants). Entry seed, wait,
// reads, registration and schedules are deliberately absent: see the
// API-SPECIFICATION admission section for why each is excluded.
func admissionRouteClass(op string) (string, bool) {
	switch op {
	case OpWorkflowCreate, OpWorkflowInvoke, "execute_workflow", "execute_workflow_by_id":
		return routeClassExecute, true
	case OpExecutionSignal, OpExecutionRevoke, OpExecutionCancel,
		OpExecutionPause, OpExecutionResume, OpExecutionRerun:
		return routeClassExecution, true
	}
	return "", false
}

// enter runs both user-face gates for one request whose namespace is already in
// its context: the inflight bound first (it needs no Redis, so an overloaded
// replica sheds load without a round trip), then the namespace rate. On a
// refusal it has written the 503 or 429 and returns ok=false; otherwise the
// caller must call release once the handler returns. Called after
// authentication and before the audit admission, so a refused request writes
// no audit row and touches no store.
func (a *apiAdmission) enter(w http.ResponseWriter, r *http.Request, class string) (release func(), ok bool) {
	release, ok = a.acquire(class)
	if !ok {
		writeServerOverloaded(w, r)
		return nil, false
	}
	if allowed, retry := a.allowRate(r.Context(), class, namespaceRateKey(r.Context())); !allowed {
		release()
		writeRateLimited(w, r, retry)
		return nil, false
	}
	return release, true
}

func namespaceRateKey(ctx context.Context) string {
	return "api:" + string(namespace.FromContext(ctx))
}

func webhookRateKey(ns, workflowID string) string {
	return "webhook:" + ns + "/" + workflowID
}

// retryAfterSeconds rounds a refill wait up to whole seconds, at least 1: the
// header is integral, and rounding down would invite a retry that is refused.
func retryAfterSeconds(wait time.Duration) int {
	s := int(math.Ceil(wait.Seconds()))
	return max(1, s)
}

func writeRateLimited(w http.ResponseWriter, r *http.Request, retryAfter int) {
	w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
	writeFail(w, r, http.StatusTooManyRequests, "rate_limited", "rate limit exceeded")
}

// The webhook route answers in its own {"error": code} shape, not the user-face
// envelope; its rate refusal follows it.
func writeWebhookRateLimited(w http.ResponseWriter, retryAfter int) {
	w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
	writeWebhookError(w, http.StatusTooManyRequests, "rate_limited")
}

const (
	// localRateLimiterSweepThreshold is the bucket count past which a new key
	// triggers an opportunistic sweep of idle buckets. Same shape as the enroll
	// limiter's: a key that is never seen again never evicts itself.
	localRateLimiterSweepThreshold = 1024
	// localRateLimiterSweepCooldown keeps a map that stays oversized from
	// walking itself on every new key.
	localRateLimiterSweepCooldown = time.Minute
)

// localRateLimiter is one in-memory token bucket per key.
type localRateLimiter struct {
	mu        sync.Mutex
	buckets   map[string]*localBucket
	limit     rate.Limit
	burst     int
	refill    time.Duration // time for an empty bucket to refill to burst
	now       func() time.Time
	lastSweep time.Time
	metrics   metrics.APIAdmissionMetrics
}

type localBucket struct {
	lim      *rate.Limiter
	lastSeen time.Time
}

func newLocalRateLimiter(limit rate.Limit, burst int, m metrics.APIAdmissionMetrics) *localRateLimiter {
	return &localRateLimiter{
		buckets: make(map[string]*localBucket),
		limit:   limit,
		burst:   burst,
		refill:  time.Duration(float64(burst) / float64(limit) * float64(time.Second)),
		now:     time.Now,
		metrics: m,
	}
}

func (l *localRateLimiter) allow(_ context.Context, key string) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	b := l.buckets[key]
	if b == nil {
		if len(l.buckets) >= localRateLimiterSweepThreshold && now.Sub(l.lastSweep) >= localRateLimiterSweepCooldown {
			l.sweep(now)
			l.lastSweep = now
		}
		b = &localBucket{lim: rate.NewLimiter(l.limit, l.burst)}
		l.buckets[key] = b
		l.metrics.SetLimiterBuckets(len(l.buckets))
	}
	b.lastSeen = now
	if b.lim.AllowN(now, 1) {
		return true, 0
	}
	// AllowN consumed nothing, so a refusal costs the caller no token. The wait
	// is the time to refill the missing fraction of one token.
	missing := 1 - b.lim.TokensAt(now)
	return false, time.Duration(missing / float64(l.limit) * float64(time.Second))
}

// sweep drops every bucket idle for a full refill. Such a bucket is back at
// burst, which is exactly what a fresh bucket for the same key would hold, so
// dropping it changes no decision.
func (l *localRateLimiter) sweep(now time.Time) {
	for key, b := range l.buckets {
		if now.Sub(b.lastSeen) >= l.refill {
			delete(l.buckets, key)
		}
	}
	l.metrics.SetLimiterBuckets(len(l.buckets))
}

func (l *localRateLimiter) size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}

// sharedRateLimiter decides in a Redis bucket shared by every replica and falls
// back to the local bucket when Redis does not answer. Failing open is the
// R1 decision: an unreachable limiter must not take the API down with it; the
// cost is that the limit is per replica until Redis is back, which the
// fallback counter and warning say.
type sharedRateLimiter struct {
	bucket  *ratelimit.TokenBucket
	rate    float64
	burst   int
	local   *localRateLimiter
	metrics metrics.APIAdmissionMetrics
	log     engine.Logger
	now     func() time.Time

	warnMu   sync.Mutex
	lastWarn time.Time
}

func (s *sharedRateLimiter) allow(ctx context.Context, key string) (bool, time.Duration) {
	rctx, cancel := context.WithTimeout(ctx, sharedRateLimitTimeout)
	defer cancel()
	ok, wait, err := s.bucket.Take(rctx, key, s.rate, s.burst)
	if err == nil {
		return ok, wait
	}
	s.metrics.ObserveRateLimitFallback()
	s.warn(err)
	return s.local.allow(ctx, key)
}

func (s *sharedRateLimiter) warn(err error) {
	if s.log == nil {
		return
	}
	s.warnMu.Lock()
	now := s.now()
	due := s.lastWarn.IsZero() || now.Sub(s.lastWarn) >= sharedRateLimitWarnEvery
	if due {
		s.lastWarn = now
	}
	s.warnMu.Unlock()
	if due {
		s.log.Warn("api_rate_limit_redis_unavailable",
			"fallback", "local", "err", err)
	}
}

// apiAdmissionFor builds the admission layer New installs. The shared mode
// counts through the control plane's own Redis (the distributed backend's
// client), so it shares one connection pool and one failure domain with the
// state it protects.
func apiAdmissionFor(cfg Config, cp *control.ControlPlane) (*apiAdmission, error) {
	var shared *ratelimit.TokenBucket
	if cfg.APILimits.Mode == APIRateLimitModeRedis {
		var client redis.Cmdable
		if cp != nil {
			if provider, ok := cp.Backend().(redisReadinessProvider); ok {
				client = provider.RedisClient()
			}
		}
		if client == nil {
			return nil, errAPIRateLimitNoRedis
		}
		shared = ratelimit.New(client, "")
	}
	return newAPIAdmission(cfg.APILimits, shared, cfg.Metrics, cfg.Logger), nil
}

// errAPIRateLimitNoRedis is returned by New when the shared mode was asked for
// on a server with no Redis to share through. That is a configuration error,
// not an outage, so it fails startup rather than silently counting locally.
var errAPIRateLimitNoRedis = errors.New("apiserver: APILimits.Mode \"redis\" requires a Redis-backed control plane")
