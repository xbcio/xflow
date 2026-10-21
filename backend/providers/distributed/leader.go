package distributed

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend"
)

var _ backend.LeaderElector = (*RedisLeaderElector)(nil)

// leaderReleaseScript deletes key only if it still holds this instance's
// token, so a stale/late Resign never deletes a lease another instance now
// owns.
var leaderReleaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`)

// leaderRenewScript extends key's TTL only if it still holds this instance's
// token, so renewal never revives a lease that already expired and was
// claimed by someone else. TTL is passed in milliseconds (PEXPIRE) so
// sub-second lease durations don't truncate to zero.
var leaderRenewScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`)

// defaultLeaderRenewFailureTolerance is how many consecutive transient renewal
// errors (Redis unreachable, timeout) are tolerated before the elector steps
// down. A token mismatch is never tolerated, because it means the lease already
// belongs to another instance.
const defaultLeaderRenewFailureTolerance = 3

// RedisLeaderElector coordinates leadership across replicas sharing the same
// Redis instance using a SETNX-based lease with periodic renewal.
type RedisLeaderElector struct {
	rdb redis.UniversalClient
	key string
	ttl time.Duration

	// renewFailureTolerance bounds consecutive transient renewal errors before
	// leadership is dropped. Zero or negative restores the historical
	// first-error downgrade. Set before renewal starts.
	renewFailureTolerance int

	isLeader              atomic.Bool
	leaseDeadlineUnixNano atomic.Int64

	mu        sync.Mutex
	token     string
	subs      map[chan bool]struct{}
	stopRenew context.CancelFunc
	renewGen  uint64
}

// NewRedisLeaderElector builds an elector bound to key with the given lease
// TTL. Renewal runs at ttl/3 while leadership is held.
func NewRedisLeaderElector(rdb redis.UniversalClient, key string, ttl time.Duration) *RedisLeaderElector {
	return &RedisLeaderElector{
		rdb:                   rdb,
		key:                   key,
		ttl:                   normalizeLeaderTTL(ttl),
		renewFailureTolerance: defaultLeaderRenewFailureTolerance,
		subs:                  make(map[chan bool]struct{}),
	}
}

// WithLeaderRenewFailureTolerance sets how many consecutive transient renewal
// errors are tolerated before leadership is dropped. Zero or negative restores
// the historical behavior of dropping leadership on the first error.
//
// The tolerance converts a Redis blip into a local retry storm instead of an
// immediate leader change: renewal is retried at ttl/9 (clamped to [10ms, 1s])
// but never past the local lease deadline, so a failure that outlives the lease
// still steps down before another replica can claim the key. A zero renewal
// result is a lost lease rather than a transient error and is never retried.
func (e *RedisLeaderElector) WithLeaderRenewFailureTolerance(n int) *RedisLeaderElector {
	e.renewFailureTolerance = n
	return e
}

func normalizeLeaderTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return defaultLeaderLeaseTTL
	}
	if ttl < time.Millisecond {
		return time.Millisecond
	}
	return ttl
}

func randomToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Campaign blocks until this instance acquires the lease or ctx is canceled.
// Once acquired it returns nil and a background goroutine renews the lease
// until Resign is called or renewal fails repeatedly (at which point
// IsLeader flips to false and Notify emits false; the caller is expected to
// observe that and call Campaign again to re-enter the competition).
func (e *RedisLeaderElector) Campaign(ctx context.Context) error {
	ticker := time.NewTicker(acquireRetryInterval(e.ttl))
	defer ticker.Stop()
	for {
		ok, err := e.tryAcquire(ctx)
		if err != nil {
			return err
		}
		if ok {
			e.startRenewal()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// acquireRetryInterval scales the SETNX retry cadence with the lease TTL (as
// specced: ttl/3) with a floor so a very small TTL doesn't cause a busy loop.
func acquireRetryInterval(ttl time.Duration) time.Duration {
	const minInterval = 10 * time.Millisecond
	interval := ttl / 3
	if interval < minInterval {
		return minInterval
	}
	return interval
}

func (e *RedisLeaderElector) tryAcquire(ctx context.Context) (bool, error) {
	token := randomToken()
	ok, err := e.rdb.SetNX(ctx, e.key, token, e.ttl).Result()
	if err != nil {
		return false, err
	}
	if ok {
		e.mu.Lock()
		e.token = token
		e.mu.Unlock()
		e.extendLocalDeadline()
		e.setLeader(true)
	}
	return ok, nil
}

// startRenewal launches the background goroutine that keeps the lease alive
// while this instance holds leadership. Any previous renewal goroutine is
// stopped first so repeated Campaign calls never leak goroutines.
//
// Each call is tagged with a monotonically increasing generation. The
// goroutine's own failure-cleanup path only clears e.stopRenew if the
// generation is still its own; otherwise a newer Campaign/startRenewal call
// has already installed a fresh cancel func (and possibly a fresh
// goroutine), and clobbering it here would leak that newer goroutine forever
// since nothing else would ever be able to cancel it.
func (e *RedisLeaderElector) startRenewal() {
	renewCtx, cancel := context.WithCancel(context.Background())

	e.mu.Lock()
	e.renewGen++
	myGen := e.renewGen
	myToken := e.token
	prevStop := e.stopRenew
	e.stopRenew = cancel
	e.mu.Unlock()
	if prevStop != nil {
		prevStop()
	}

	ttlMillis := e.ttl.Milliseconds()
	interval := e.ttl / 3
	if interval <= 0 {
		interval = time.Millisecond
	}
	retryInterval := leaderRenewRetryInterval(e.ttl)

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				if !e.renewLease(renewCtx, myToken, ttlMillis, retryInterval) {
					// A stopped renewal (Resign or a newer Campaign) must not
					// be reported as a leadership loss; the caller that stopped
					// it already owns the resulting state.
					if renewCtx.Err() != nil {
						return
					}
					e.finishRenewal(myGen)
					return
				}
				e.extendLocalDeadline()
			}
		}
	}()
}

// renewLease keeps trying to extend the Redis lease until one attempt
// succeeds, the local deadline passes, the failure tolerance is exhausted, or
// ctx is canceled. It reports true only on a successful renewal.
//
// A zero renewal result is not a retryable error: the script only returns zero
// when the key no longer holds this instance's token, so another replica
// already owns the lease and retrying would only delay the downgrade.
func (e *RedisLeaderElector) renewLease(ctx context.Context, token string, ttlMillis int64, retryInterval time.Duration) bool {
	failures := 0
	for {
		renewed, err := leaderRenewScript.Run(ctx, e.rdb, []string{e.key}, token, ttlMillis).Result()
		if ctx.Err() != nil {
			return false
		}
		if err == nil {
			n, _ := renewed.(int64)
			return n > 0
		}
		failures++
		if e.renewFailuresExhausted(failures) {
			return false
		}
		if !e.waitRenewRetry(ctx, retryInterval) {
			return false
		}
	}
}

// renewFailuresExhausted reports whether consecutive transient renewal errors
// have used up the configured tolerance. A non-positive tolerance keeps the
// historical immediate downgrade.
func (e *RedisLeaderElector) renewFailuresExhausted(failures int) bool {
	tolerance := e.renewFailureTolerance
	if tolerance <= 0 {
		tolerance = 1
	}
	return failures >= tolerance
}

// waitRenewRetry waits one retry interval without crossing the local leadership
// deadline, which only a successful renewal may extend. It returns false when
// the wait was interrupted (ctx canceled) or the deadline was reached: either
// way the caller must stop renewing.
func (e *RedisLeaderElector) waitRenewRetry(ctx context.Context, interval time.Duration) bool {
	deadline := e.leaseDeadline()
	if deadline.IsZero() {
		return false
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return false
	}
	if interval >= remaining {
		// Wake when the local lease runs out: another attempt cannot land in
		// time to keep it.
		interval = remaining
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return time.Now().Before(deadline)
	}
}

// finishRenewal clears this generation's renewal state after leadership loss
// and emits the false notification. The generation guard keeps a superseded
// goroutine from clearing the cancel func installed by a newer Campaign.
func (e *RedisLeaderElector) finishRenewal(myGen uint64) {
	lost := false
	e.mu.Lock()
	if e.renewGen == myGen {
		e.stopRenew = nil
		lost = true
	}
	e.mu.Unlock()
	if lost {
		e.clearLocalDeadline()
		e.setLeader(false)
	}
}

// IsLeader reports current leadership without a network round-trip. On local
// deadline expiry it clears local leadership but does NOT release the Redis
// lease key (leaderReleaseScript runs only in Resign) — the Redis key lingers
// until its TTL (default 15s) expires. This is acceptable because leadership
// here only gates background maintenance (LeaseSweeper.SweepOnce / RepairOnce),
// not state mutations: at most one TTL window of skipped sweeps occurs before
// the next replica takes over. A true control-plane HA path would fence on
// Redis directly; that is planned, not implemented.
//
// NOTE: this "query" has side-effects (stopRenewal + setLeader(false)) when
// the local deadline has expired. This is an intentional lazy-expiration
// pattern so callers do not need a separate expiry timer. The method is
// concurrency-safe: stopRenewal and setLeader both acquire e.mu internally,
// and the atomic loads provide a fast-path that does not contend.
func (e *RedisLeaderElector) IsLeader() bool {
	if !e.isLeader.Load() {
		return false
	}
	deadline := e.leaseDeadlineUnixNano.Load()
	if deadline <= 0 || time.Now().After(time.Unix(0, deadline)) {
		e.stopRenewal()
		e.clearLocalDeadline()
		e.setLeader(false)
		return false
	}
	return true
}

// Resign voluntarily releases leadership so another instance can take over
// without waiting for the lease TTL to expire. Notify subscriber references are
// cleared (#9: subs map only-grows mitigation).
func (e *RedisLeaderElector) Resign(ctx context.Context) error {
	e.stopRenewal()

	if !e.isLeader.Load() {
		e.clearLocalDeadline()
		return nil
	}

	token := e.currentToken()
	_, err := leaderReleaseScript.Run(ctx, e.rdb, []string{e.key}, token).Result()
	e.clearLocalDeadline()
	e.setLeader(false)

	// Clean up subscriber channels to prevent unbounded growth of the subs map
	// across repeated Campaign/Resign cycles (#9). Channels are not closed (the
	// interface contract says "Never closed"); consumers detecting leadership
	// loss via the false emission above should discard their reference. Clearing
	// the map allows the channel (and its consumer goroutine) to be GC'd once
	// the consumer drops its reference.
	e.mu.Lock()
	e.subs = make(map[chan bool]struct{})
	e.mu.Unlock()

	return err
}

func (e *RedisLeaderElector) stopRenewal() {
	e.mu.Lock()
	stop := e.stopRenew
	e.stopRenew = nil
	e.mu.Unlock()
	if stop != nil {
		stop()
	}
}

// Notify returns a buffered-size-1 subscription channel that emits the current
// leadership state and then every subsequent leadership change. Sends are
// non-blocking; a slow consumer only ever sees the latest state rather than
// stalling the producer.
func (e *RedisLeaderElector) Notify() <-chan bool {
	ch := make(chan bool, 1)
	e.mu.Lock()
	e.subs[ch] = struct{}{}
	sendLatestLeaderState(ch, e.isLeader.Load())
	e.mu.Unlock()
	return ch
}

func (e *RedisLeaderElector) setLeader(v bool) {
	if old := e.isLeader.Swap(v); old == v {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for ch := range e.subs {
		sendLatestLeaderState(ch, v)
	}
}

func (e *RedisLeaderElector) currentToken() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.token
}

// leaderRenewRetryInterval is the pause between renewal attempts after a
// transient error: ttl/9, clamped so a tiny lease does not busy-loop and a long
// one still retries within its local deadline.
func leaderRenewRetryInterval(ttl time.Duration) time.Duration {
	const (
		minInterval = 10 * time.Millisecond
		maxInterval = time.Second
	)
	interval := ttl / 9
	if interval < minInterval {
		return minInterval
	}
	if interval > maxInterval {
		return maxInterval
	}
	return interval
}

// extendLocalDeadline refreshes the local leadership deadline. A safety margin
// of ttl/3 is subtracted so the local view expires before the Redis key's real
// TTL, reducing the split-brain window where this instance believes it is still
// leader while Redis has already let the key expire (#9).
func (e *RedisLeaderElector) extendLocalDeadline() {
	safetyMargin := e.ttl / 3
	e.leaseDeadlineUnixNano.Store(time.Now().Add(e.ttl - safetyMargin).UnixNano())
}

// leaseDeadline returns the current local leadership deadline, or the zero
// time when no deadline is set.
func (e *RedisLeaderElector) leaseDeadline() time.Time {
	ns := e.leaseDeadlineUnixNano.Load()
	if ns <= 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

func (e *RedisLeaderElector) clearLocalDeadline() {
	e.leaseDeadlineUnixNano.Store(0)
}

func sendLatestLeaderState(ch chan bool, v bool) {
	select {
	case ch <- v:
		return
	default:
	}
	select {
	case <-ch:
	default:
	}
	select {
	case ch <- v:
	default:
	}
}
