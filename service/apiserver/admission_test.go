package apiserver

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"

	"github.com/xbcio/xflow/backend/providers/distributed/ratelimit"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/observability/metrics"
)

// fakeClock is a hand-advanced clock for the limiter: refill is driven by
// advance, never by sleeping, so the tests hold on a loaded machine.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newTestLocalLimiter(r float64, burst int, clk *fakeClock) *localRateLimiter {
	l := newLocalRateLimiter(rate.Limit(r), burst, metrics.NewAPIAdmissionMetrics(nil))
	l.now = clk.now
	return l
}

func TestLocalRateLimiterSpendsBurstThenRefills(t *testing.T) {
	clk := newFakeClock()
	l := newTestLocalLimiter(2, 3, clk)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if ok, _ := l.allow(ctx, "k"); !ok {
			t.Fatalf("request %d refused inside burst", i)
		}
	}
	ok, wait := l.allow(ctx, "k")
	if ok {
		t.Fatal("request past burst admitted")
	}
	if wait != 500*time.Millisecond {
		t.Fatalf("wait = %v, want 500ms (one token at 2/s)", wait)
	}
	// A refusal must not consume: after exactly the reported wait one token
	// exists, not a fraction of one.
	if ok, _ := l.allow(ctx, "k"); ok {
		t.Fatal("second refusal admitted without time passing")
	}
	clk.advance(500 * time.Millisecond)
	if ok, _ := l.allow(ctx, "k"); !ok {
		t.Fatal("refused after the reported wait elapsed")
	}
	// A long idle period refills to burst and no further.
	clk.advance(time.Hour)
	for i := 0; i < 3; i++ {
		if ok, _ := l.allow(ctx, "k"); !ok {
			t.Fatalf("request %d after idle refused", i)
		}
	}
	if ok, _ := l.allow(ctx, "k"); ok {
		t.Fatal("idle refill exceeded burst")
	}
}

func TestLocalRateLimiterKeysAreIndependent(t *testing.T) {
	clk := newFakeClock()
	l := newTestLocalLimiter(1, 1, clk)
	ctx := context.Background()
	if ok, _ := l.allow(ctx, "a"); !ok {
		t.Fatal("a refused")
	}
	if ok, _ := l.allow(ctx, "a"); ok {
		t.Fatal("a admitted twice")
	}
	if ok, _ := l.allow(ctx, "b"); !ok {
		t.Fatal("b charged for a's traffic")
	}
}

func TestRetryAfterSecondsRoundsUpToAtLeastOne(t *testing.T) {
	for _, c := range []struct {
		wait time.Duration
		want int
	}{
		{0, 1},
		{time.Millisecond, 1},
		{999 * time.Millisecond, 1},
		{time.Second, 1},
		{1001 * time.Millisecond, 2},
		{2500 * time.Millisecond, 3},
	} {
		if got := retryAfterSeconds(c.wait); got != c.want {
			t.Errorf("retryAfterSeconds(%v) = %d, want %d", c.wait, got, c.want)
		}
	}
}

// TestLocalRateLimiterSweepBoundsBuckets pins that keys seen once and never
// again cannot grow the map without bound: once past the threshold, a new key
// sweeps every bucket idle for a full refill.
func TestLocalRateLimiterSweepBoundsBuckets(t *testing.T) {
	clk := newFakeClock()
	l := newTestLocalLimiter(10, 10, clk) // refill = 1s
	ctx := context.Background()
	for i := 0; i < localRateLimiterSweepThreshold; i++ {
		l.allow(ctx, "old-"+strconv.Itoa(i))
	}
	if got := l.size(); got != localRateLimiterSweepThreshold {
		t.Fatalf("size = %d, want %d", got, localRateLimiterSweepThreshold)
	}
	// One key still active inside the refill window survives the sweep.
	clk.advance(2 * time.Second)
	l.allow(ctx, "old-0")
	clk.advance(500 * time.Millisecond)
	l.allow(ctx, "new")
	if got := l.size(); got != 2 {
		t.Fatalf("size after sweep = %d, want 2 (the still-active key and the new one)", got)
	}
	// The cooldown keeps an oversized map from sweeping on every new key.
	for i := 0; i < localRateLimiterSweepThreshold; i++ {
		l.allow(ctx, "burst-"+strconv.Itoa(i))
	}
	clk.advance(2 * time.Second)
	l.allow(ctx, "after")
	if got := l.size(); got != localRateLimiterSweepThreshold+3 {
		t.Fatalf("size inside cooldown = %d, want no sweep (%d)", got, localRateLimiterSweepThreshold+3)
	}
	clk.advance(localRateLimiterSweepCooldown)
	l.allow(ctx, "later")
	if got := l.size(); got != 1 {
		t.Fatalf("size after cooldown sweep = %d, want 1", got)
	}
}

func TestAPIAdmissionRateKeyIsTheContextNamespace(t *testing.T) {
	a := newAPIAdmission(APILimits{Rate: 1, Burst: 1}, nil, nil, nil)
	nsA := namespace.WithNamespace(context.Background(), "tenant-a")
	nsB := namespace.WithNamespace(context.Background(), "tenant-b")
	if ok, _ := a.allowRate(nsA, routeClassExecute, namespaceRateKey(nsA)); !ok {
		t.Fatal("tenant-a first refused")
	}
	ok, retry := a.allowRate(nsA, routeClassExecute, namespaceRateKey(nsA))
	if ok || retry != 1 {
		t.Fatalf("tenant-a second = (%v, %d), want refused with Retry-After 1", ok, retry)
	}
	if ok, _ := a.allowRate(nsB, routeClassExecute, namespaceRateKey(nsB)); !ok {
		t.Fatal("tenant-b charged for tenant-a")
	}
}

func TestAPIAdmissionWithoutRateAdmitsEverything(t *testing.T) {
	a := newAPIAdmission(APILimits{}, nil, nil, nil)
	for i := 0; i < 10_000; i++ {
		if ok, _ := a.allowRate(context.Background(), routeClassExecute, "k"); !ok {
			t.Fatalf("request %d refused with no rate configured", i)
		}
	}
	var nilAdmission *apiAdmission
	if ok, _ := nilAdmission.allowRate(context.Background(), routeClassExecute, "k"); !ok {
		t.Fatal("nil admission refused")
	}
}

func TestAPILimitsValidate(t *testing.T) {
	for _, bad := range []APILimits{{Rate: -1}, {Burst: -1}, {Mode: "cluster"}} {
		if err := bad.validate(); err == nil {
			t.Errorf("%+v validated", bad)
		}
	}
	for _, good := range []APILimits{{}, {Rate: 5, Burst: 10, Mode: APIRateLimitModeRedis}, {Mode: APIRateLimitModeLocal}} {
		if err := good.validate(); err != nil {
			t.Errorf("%+v: %v", good, err)
		}
	}
	if got := (APILimits{Rate: 2.5}).burst(); got != 3 {
		t.Errorf("derived burst = %d, want 3", got)
	}
	if got := (APILimits{Rate: 0.1}).burst(); got != 1 {
		t.Errorf("derived burst = %d, want 1", got)
	}
}

// warnCountingLogger counts Warn calls; every other level is discarded.
type warnCountingLogger struct {
	mu    sync.Mutex
	warns int
}

func (l *warnCountingLogger) Debug(string, ...any)  {}
func (l *warnCountingLogger) Debugf(string, ...any) {}
func (l *warnCountingLogger) Info(string, ...any)   {}
func (l *warnCountingLogger) Infof(string, ...any)  {}
func (l *warnCountingLogger) Warn(string, ...any) {
	l.mu.Lock()
	l.warns++
	l.mu.Unlock()
}
func (l *warnCountingLogger) Warnf(string, ...any)  {}
func (l *warnCountingLogger) Error(string, ...any)  {}
func (l *warnCountingLogger) Errorf(string, ...any) {}
func (l *warnCountingLogger) Panic(string, ...any)  {}
func (l *warnCountingLogger) Panicf(string, ...any) {}

func (l *warnCountingLogger) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.warns
}

// TestSharedRateLimiterSharesOneBudget stands in two replicas with two
// admission layers over one Redis: together they admit one burst, not two.
func TestSharedRateLimiterSharesOneBudget(t *testing.T) {
	mr := miniredis.RunT(t)
	mr.SetTime(time.Unix(1_700_000_000, 0))
	limits := APILimits{Rate: 1, Burst: 4, Mode: APIRateLimitModeRedis}
	replicas := make([]*apiAdmission, 2)
	for i := range replicas {
		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = client.Close() })
		replicas[i] = newAPIAdmission(limits, ratelimit.New(client, ""), nil, nil)
	}
	admitted := 0
	for i := 0; i < 8; i++ {
		if ok, _ := replicas[i%2].allowRate(context.Background(), routeClassExecute, "api:ns"); ok {
			admitted++
		}
	}
	if admitted != 4 {
		t.Fatalf("admitted %d across two replicas, want the shared burst 4", admitted)
	}
}

// TestSharedRateLimiterFailsOpenToLocal is the R1 fallback: with Redis gone the
// decision is the local bucket's (so the limit still applies, per replica), each
// fallback is counted by reason, and the warning is rate-limited rather than per
// request.
func TestSharedRateLimiterFailsOpenToLocal(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	mr.Close()

	m := metrics.New()
	log := &warnCountingLogger{}
	clk := newFakeClock()
	a := newAPIAdmission(APILimits{Rate: 1, Burst: 2, Mode: APIRateLimitModeRedis}, ratelimit.New(client, ""), m, log)
	shared := a.limiter.(*sharedRateLimiter)
	shared.now = clk.now
	shared.local.now = clk.now

	ctx := context.Background()
	results := []bool{}
	for i := 0; i < 3; i++ {
		ok, _ := a.allowRate(ctx, routeClassExecute, "api:ns")
		results = append(results, ok)
	}
	if !results[0] || !results[1] || results[2] {
		t.Fatalf("fallback decisions = %v, want the local burst of 2 then a refusal", results)
	}
	// Two warnings: the rate-limited unavailable message and the trip that the
	// third failure opened (see the breaker test for the trip's own behavior).
	if got := log.count(); got != 2 {
		t.Fatalf("warnings = %d, want 2 (rate-limited unavailable + breaker trip)", got)
	}
	clk.advance(sharedRateLimitWarnEvery)
	a.allowRate(ctx, routeClassExecute, "api:ns")
	if got := log.count(); got != 3 {
		t.Fatalf("warnings after the window = %d, want 3", got)
	}
	got := fallbackCounts(t, m)
	want := map[string]float64{fallbackReasonRedisError: 4, fallbackReasonBreakerTrip: 1}
	if len(got) != len(want) {
		t.Fatalf("fallback series = %v, want %v", got, want)
	}
	for reason, v := range want {
		if got[reason] != v {
			t.Errorf("fallback %s = %v, want %v", reason, got[reason], v)
		}
	}
}

// fallbackCounts gathers xflow_api_rate_limit_fallback_total by reason.
func fallbackCounts(t *testing.T, m *metrics.Metrics) map[string]float64 {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]float64{}
	for _, f := range families {
		if f.GetName() != "xflow_api_rate_limit_fallback_total" {
			continue
		}
		for _, metric := range f.GetMetric() {
			for _, l := range metric.GetLabel() {
				if l.GetName() == "reason" {
					out[l.GetValue()] = metric.GetCounter().GetValue()
				}
			}
		}
	}
	return out
}

// redisCountingHook counts the commands that reach the Redis client's process
// path, so "the breaker skipped the call" is observed rather than inferred from
// elapsed time.
type redisCountingHook struct {
	mu sync.Mutex
	n  int
}

func (h *redisCountingHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *redisCountingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.mu.Lock()
		h.n++
		h.mu.Unlock()
		return next(ctx, cmd)
	}
}

func (h *redisCountingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *redisCountingHook) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.n
}

// TestSharedRateLimiterBreakerSkipsRedisWhileOpen pins the breaker: after
// sharedRateLimitBreakerFailures consecutive failures the limiter stops calling
// Redis for the cooldown (each skipped call would otherwise wait out the full
// timeout while holding an inflight slot), the trips and skips are counted by
// reason, and the first request after the cooldown probes Redis again. The
// clock is the injectable one, so no test sleeps.
func TestSharedRateLimiterBreakerSkipsRedisWhileOpen(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	calls := &redisCountingHook{}
	client.AddHook(calls)
	mr.Close()

	m := metrics.New()
	clk := newFakeClock()
	a := newAPIAdmission(APILimits{Rate: 1, Burst: 100, Mode: APIRateLimitModeRedis}, ratelimit.New(client, ""), m, &warnCountingLogger{})
	shared := a.limiter.(*sharedRateLimiter)
	shared.now = clk.now
	shared.local.now = clk.now
	ctx := context.Background()

	// The failures the breaker tolerates each try Redis.
	for i := 0; i < sharedRateLimitBreakerFailures; i++ {
		if ok, _ := shared.allow(ctx, "api:ns"); !ok {
			t.Fatalf("fallback decision %d refused inside the local burst", i)
		}
	}
	if got := calls.count(); got != sharedRateLimitBreakerFailures {
		t.Fatalf("Redis calls = %d, want %d", got, sharedRateLimitBreakerFailures)
	}

	// The breaker is open: decisions keep being made, but without Redis.
	for i := 0; i < 5; i++ {
		if ok, _ := shared.allow(ctx, "api:ns"); !ok {
			t.Fatalf("breaker-open decision %d refused inside the local burst", i)
		}
	}
	if got := calls.count(); got != sharedRateLimitBreakerFailures {
		t.Fatalf("Redis calls while the breaker is open = %d, want still %d", got, sharedRateLimitBreakerFailures)
	}

	// The cooldown is a timer, not a permanent latch: the next request probes.
	clk.advance(sharedRateLimitBreakerCooldown)
	if ok, _ := shared.allow(ctx, "api:ns"); !ok {
		t.Fatal("probe after the cooldown refused inside the local burst")
	}
	if got := calls.count(); got != sharedRateLimitBreakerFailures+1 {
		t.Fatalf("Redis calls after the cooldown = %d, want one probe (%d)", got, sharedRateLimitBreakerFailures+1)
	}

	got := fallbackCounts(t, m)
	want := map[string]float64{
		fallbackReasonRedisError:  sharedRateLimitBreakerFailures + 1, // failed calls plus the probe
		fallbackReasonBreakerTrip: 1,
		fallbackReasonBreakerOpen: 5,
	}
	if len(got) != len(want) {
		t.Fatalf("fallback series = %v, want %v", got, want)
	}
	for reason, v := range want {
		if got[reason] != v {
			t.Errorf("fallback %s = %v, want %v", reason, got[reason], v)
		}
	}
}

// TestLocalRateLimiterConcurrentAdmitsExactlyBurst is the -race case: many
// goroutines on one key with no time passing admit exactly burst.
func TestLocalRateLimiterConcurrentAdmitsExactlyBurst(t *testing.T) {
	clk := newFakeClock()
	l := newTestLocalLimiter(1, 50, clk)
	var wg sync.WaitGroup
	var mu sync.Mutex
	admitted := 0
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, _ := l.allow(context.Background(), "k"); ok {
				mu.Lock()
				admitted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if admitted != 50 {
		t.Fatalf("admitted %d, want burst 50", admitted)
	}
}
