package redisx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Safety-net walk coordination. Every namespace index has a low-frequency
// keyspace walk that recovers registrations lost to a crash; run by every
// replica it multiplies SCAN load by the replica count for no extra coverage.
// These helpers let exactly one replica per (namespace, kind) walk at a time.
//
//	xflow:ns:<t>:walk-lock:<kind>  holder's random owner token, PX WalkLockTTL
//	xflow:ns:<t>:walk-done:<kind>  unix ms of the last completed full round,
//	                               PX 2 x WalkRound
//
// Both keys are plain single-key strings with no `exec:{` segment and no hash
// tag, so they match no execution SCAN glob and are slot-safe on Redis Cluster.
const (
	// WalkRound is the target duration of one full safety-net round.
	WalkRound = time.Hour
	// WalkLockTTL is how long a holder keeps the lock without renewing it.
	WalkLockTTL = 2 * time.Minute
	// WalkRenewEveryPages is how often a long page loop renews the lock.
	WalkRenewEveryPages = 64
	// WalkScanCount is the SCAN COUNT hint of one walk page.
	WalkScanCount = 1000
)

// acquireWalkLockLua takes the lock when free, or extends it when the caller
// already owns it. KEYS[1]=lock, ARGV[1]=owner, ARGV[2]=ttl ms.
var acquireWalkLockLua = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if not cur then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return 1
end
if cur == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
`)

// renewWalkLockLua extends the lock only while the caller still owns it.
var renewWalkLockLua = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
`)

// WalkLockKey returns the holder lock key for one namespace and walk kind.
func WalkLockKey(namespace, kind string) string {
	return "xflow:ns:" + namespace + ":walk-lock:" + kind
}

// WalkDoneKey returns the round-done marker key for one namespace and kind.
func WalkDoneKey(namespace, kind string) string {
	return "xflow:ns:" + namespace + ":walk-done:" + kind
}

// NewWalkOwner returns a random owner token for one process or store.
func NewWalkOwner() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("redisx: random source unavailable: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// AcquireWalkLock takes the lock if it is free or already owned by owner (which
// extends it). It reports whether owner holds the lock afterwards.
func AcquireWalkLock(ctx context.Context, rdb redis.Cmdable, key, owner string, ttl time.Duration) (bool, error) {
	n, err := acquireWalkLockLua.Run(ctx, rdb, []string{key}, owner, ttl.Milliseconds()).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// RenewWalkLock extends the lock only while owner still holds it. A false
// result means another process holds it (or it expired); the caller must stop
// walking at once.
func RenewWalkLock(ctx context.Context, rdb redis.Cmdable, key, owner string, ttl time.Duration) (bool, error) {
	n, err := renewWalkLockLua.Run(ctx, rdb, []string{key}, owner, ttl.Milliseconds()).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// MarkWalkRoundDone records that a full round finished at now. The marker
// expires after two rounds, so a long-dead walk is treated as never run.
func MarkWalkRoundDone(ctx context.Context, rdb redis.Cmdable, key string, now time.Time, round time.Duration) error {
	return rdb.Set(ctx, key, strconv.FormatInt(now.UnixMilli(), 10), 2*round).Err()
}

// WalkRoundRecent reports whether a completed-round marker exists. Its TTL
// bounds its age, so existence is the freshness test.
func WalkRoundRecent(ctx context.Context, rdb redis.Cmdable, key string) (bool, error) {
	n, err := rdb.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// WalkPagesPerStep sizes one steady step so a full round of size/count pages
// spans about round when a step runs every step: ceil((size/count+1) /
// (round/step)), at least 1.
func WalkPagesPerStep(size, count int64, round, step time.Duration) int64 {
	if size <= 0 || count <= 0 || step <= 0 {
		return 1
	}
	steps := max(int64(round/step), 1)
	return max((size/count+steps)/steps, 1)
}

// BeginWalk is the gate every safety-net walk passes before any SCAN or DBSIZE.
// It acquires or renews the holder lock for (namespace, kind) as owner. holding
// reports whether owner may walk now. When owner has just become the holder
// (wasHolding false), roundRecent tells whether a prior holder completed a round
// within the marker's lifetime, in which case the new holder paces itself
// instead of backfilling at full speed. On error holding is false.
func BeginWalk(ctx context.Context, rdb redis.Cmdable, namespace, kind, owner string, wasHolding bool) (holding, roundRecent bool, err error) {
	ok, err := AcquireWalkLock(ctx, rdb, WalkLockKey(namespace, kind), owner, WalkLockTTL)
	if err != nil || !ok {
		return false, false, err
	}
	if wasHolding {
		return true, false, nil
	}
	roundRecent, err = WalkRoundRecent(ctx, rdb, WalkDoneKey(namespace, kind))
	if err != nil {
		return false, false, err
	}
	return true, roundRecent, nil
}
