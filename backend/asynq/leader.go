package asynq

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

// RedisLeaderElector coordinates leadership across replicas sharing the same
// Redis instance using a SETNX-based lease with periodic renewal.
type RedisLeaderElector struct {
	rdb *redis.Client
	key string
	ttl time.Duration

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
func NewRedisLeaderElector(rdb *redis.Client, key string, ttl time.Duration) *RedisLeaderElector {
	return &RedisLeaderElector{
		rdb:  rdb,
		key:  key,
		ttl:  normalizeLeaderTTL(ttl),
		subs: make(map[chan bool]struct{}),
	}
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

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				renewed, err := leaderRenewScript.Run(renewCtx, e.rdb, []string{e.key}, myToken, ttlMillis).Result()
				if renewCtx.Err() != nil {
					return
				}
				if err != nil || renewed == int64(0) {
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
					return
				}
				e.extendLocalDeadline()
			}
		}
	}()
}

// IsLeader reports current leadership without a network round-trip. On local
// deadline expiry it clears local leadership but does NOT release the Redis
// lease key (leaderReleaseScript runs only in Resign) — the Redis key lingers
// until its TTL (default 15s) expires. This is acceptable because leadership
// here only gates background maintenance (LeaseSweeper.SweepOnce / RepairOnce),
// not state mutations: at most one TTL window of skipped sweeps occurs before
// the next replica takes over. A true control-plane HA path would fence on
// Redis directly; that is planned, not implemented.
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
// without waiting for the lease TTL to expire.
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

func (e *RedisLeaderElector) extendLocalDeadline() {
	e.leaseDeadlineUnixNano.Store(time.Now().Add(e.ttl).UnixNano())
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
