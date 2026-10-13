// Package ratelimit is a Redis-shared token bucket: one bucket per key, refilled
// at a fixed rate up to a burst, consulted and debited by every replica in one
// atomic script so the limit holds fleet-wide rather than per process.
//
// It is the optional shared counting mode of the API admission layer
// (service/apiserver). The default mode counts per replica in memory and never
// touches Redis; this package exists for deployments that ask for one budget
// across replicas and accept a Redis round trip per admitted request for it.
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultKeyPrefix namespaces every bucket key this package writes.
const DefaultKeyPrefix = "xflow:ratelimit:"

// takeLua refills and debits one bucket atomically.
//
// The clock is Redis TIME, not the caller's: replicas whose clocks disagree
// would otherwise each refill the shared bucket by their own notion of elapsed
// time, and the fastest clock would set everyone's limit. A stored timestamp
// in the future (a Redis failover onto a host whose clock is behind) refills
// nothing rather than going negative.
//
// Times are milliseconds. Microseconds would exceed the 14 significant digits
// Lua 5.1's number-to-string conversion keeps, and a timestamp written back
// with lost digits would refill the bucket by the rounding error on every call.
//
// The key expires once a full refill has elapsed: an absent bucket and a full
// one answer identically, so expiry loses nothing and bounds the keyspace to the
// keys that were recently active.
//
// KEYS[1] bucket hash. ARGV[1] rate in tokens per second, ARGV[2] burst,
// ARGV[3] expiry in milliseconds. Returns {allowed (0|1), retry_after_ms}.
var takeLua = redis.NewScript(`
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil or ts == nil then
	tokens = burst
	ts = now
end
if now > ts then
	tokens = math.min(burst, tokens + (now - ts) * rate / 1000)
	ts = now
end
local allowed = 0
local wait = 0
if tokens >= 1 then
	tokens = tokens - 1
	allowed = 1
else
	wait = math.ceil((1 - tokens) * 1000 / rate)
end
redis.call('HSET', KEYS[1], 'tokens', string.format('%.6f', tokens), 'ts', string.format('%.0f', ts))
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return {allowed, wait}
`)

// TokenBucket is a set of Redis-held token buckets addressed by key.
type TokenBucket struct {
	client redis.Cmdable
	prefix string
}

// New returns a TokenBucket over client. An empty prefix uses
// DefaultKeyPrefix.
func New(client redis.Cmdable, prefix string) *TokenBucket {
	if prefix == "" {
		prefix = DefaultKeyPrefix
	}
	return &TokenBucket{client: client, prefix: prefix}
}

// Take debits one token from key's bucket. It reports whether the token was
// granted and, when it was not, how long until one will be. An error means
// Redis gave no answer; the caller decides what an unanswered request gets.
func (b *TokenBucket) Take(ctx context.Context, key string, ratePerSecond float64, burst int) (bool, time.Duration, error) {
	if b == nil || b.client == nil {
		return false, 0, errors.New("ratelimit: no redis client")
	}
	if ratePerSecond <= 0 || burst < 1 {
		return false, 0, fmt.Errorf("ratelimit: invalid bucket rate=%v burst=%d", ratePerSecond, burst)
	}
	// One full refill plus a second of slack, so a key never expires while its
	// bucket is still below burst.
	ttl := int64(math.Ceil(float64(burst)/ratePerSecond*1000)) + 1000
	res, err := takeLua.Run(ctx, b.client, []string{b.prefix + key},
		strconv.FormatFloat(ratePerSecond, 'f', -1, 64), burst, ttl).Int64Slice()
	if err != nil {
		return false, 0, err
	}
	if len(res) != 2 {
		return false, 0, fmt.Errorf("ratelimit: unexpected script reply of %d values", len(res))
	}
	return res[0] == 1, time.Duration(res[1]) * time.Millisecond, nil
}
