package distributed

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

// Two things about the leader lease were configured and never read back.
//
// 1. extendLocalDeadline subtracts ttl/3 so the local view of leadership
//    expires strictly before the Redis key it is derived from. That margin is
//    the whole split-brain mitigation (#9): without it, this instance keeps
//    answering IsLeader() == true through the window where Redis may already
//    have let the key go and another replica claimed it. Every existing test
//    that watches the local deadline expire waits well past ttl itself — the
//    recampaign test sleeps 2*ttl — so none of them can tell "expires at
//    2/3·ttl" from "expires at ttl".
//
// 2. normalizeLeaderTTL's fallback for a non-positive configured TTL was
//    pinned only by TestRedisLeaderElectorInvalidTTLStillSetsExpiringLease,
//    which asserts the Redis key TTL is positive. Two milliseconds is
//    positive. A fallback that degraded by four orders of magnitude would
//    leave that test green while turning leadership into a re-election storm,
//    because renewal runs at ttl/3 and cannot beat a lease that short.

func TestExtendLocalDeadlineLeavesASafetyMarginBeforeTheRedisTTL(t *testing.T) {
	const ttl = 900 * time.Millisecond
	e := &RedisLeaderElector{ttl: ttl}

	before := time.Now()
	e.extendLocalDeadline()
	remaining := time.Unix(0, e.leaseDeadlineUnixNano.Load()).Sub(before)

	// Upper bound is the assertion that matters: the local deadline must land
	// strictly inside the Redis key's lifetime, not on top of it.
	if remaining >= ttl {
		t.Fatalf("local deadline is %v out of a %v lease: it expires no earlier "+
			"than the Redis key, so for that whole window this instance answers "+
			"IsLeader() == true while the key may already belong to another replica",
			remaining, ttl)
	}
	// Lower bound keeps the margin from swallowing the lease. Renewal runs at
	// ttl/3, so a local deadline below that never survives to its first renewal
	// and leadership flaps on every cycle.
	if remaining <= ttl/3 {
		t.Fatalf("local deadline is %v out of a %v lease, at or below the ttl/3 "+
			"renewal interval: leadership expires locally before the first renewal "+
			"can extend it", remaining, ttl)
	}
}

func TestIsLeaderGoesFalseWhileTheRedisLeaseIsStillHeld(t *testing.T) {
	// The behavioural half of the margin. miniredis does not advance its own
	// clock, so the Redis key provably never expires during this test: any
	// loss of leadership observed here is the local deadline firing first,
	// which is exactly the property the margin exists to provide.
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	const ttl = 3 * time.Second
	key := "test:leader:local-margin"
	l := newTestRedisLeaderElector(t, mr.Addr(), key, ttl)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := l.Campaign(ctx); err != nil {
		t.Fatalf("Campaign() error = %v", err)
	}
	// Renewal would keep pushing the deadline out; stopping it is what makes
	// this a test of the deadline rather than a test of the renewer.
	l.stopRenewal()

	// Sampled at 1s, well inside the ~2s local deadline. Without this the test
	// is satisfied by a margin that consumes the entire lease, or by a deadline
	// that is never set at all.
	time.Sleep(time.Second)
	if !l.IsLeader() {
		t.Fatal("IsLeader() = false one second into a 3s lease: the local " +
			"deadline is shorter than the renewal interval, so leadership is lost " +
			"before it can be renewed")
	}

	// Sampled at 2.5s: past the ~2s local deadline, still inside the 3s Redis
	// key. Dropping the margin puts the local deadline at 3s and leaves this
	// true.
	time.Sleep(1500 * time.Millisecond)
	if l.IsLeader() {
		t.Fatal("IsLeader() = true 2.5s into a 3s lease with renewal stopped: " +
			"the local view expires no earlier than the Redis key, so this replica " +
			"would still act as leader in the window where the key can be claimed " +
			"by another one")
	}
	if got := mr.TTL(key); got <= 0 {
		t.Fatalf("Redis key TTL = %v: the key expired too, so this test cannot "+
			"distinguish the local deadline firing early from the lease simply "+
			"running out", got)
	}
}

func TestNormalizeLeaderTTL(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		// A non-positive configured TTL is a misconfiguration, and the fallback
		// is what keeps it from becoming an outage. Pinned to the constant so
		// New() and this branch cannot drift apart.
		{"zero falls back", 0, defaultLeaderLeaseTTL},
		{"negative falls back", -time.Second, defaultLeaderLeaseTTL},
		// Sub-millisecond is clamped rather than rejected: the renew script
		// passes milliseconds to PEXPIRE, and anything below 1ms truncates to
		// zero there, which deletes the key instead of extending it.
		{"sub-millisecond is clamped", 100 * time.Microsecond, time.Millisecond},
		{"exactly one millisecond passes through", time.Millisecond, time.Millisecond},
		{"a configured value passes through", 42 * time.Second, 42 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeLeaderTTL(tc.in); got != tc.want {
				t.Fatalf("normalizeLeaderTTL(%v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestDefaultLeaderLeaseTTLIsLongEnoughToRenew(t *testing.T) {
	// The table above pins the fallback to the constant; this pins the constant
	// to something a renewal can actually beat. Renewal runs at ttl/3, and each
	// renewal is a Redis round trip, so a default in the milliseconds would
	// re-elect continuously — and every assertion above would still pass,
	// because they all compare against whatever this constant happens to be.
	if defaultLeaderLeaseTTL < time.Second {
		t.Fatalf("defaultLeaderLeaseTTL = %v: renewal at %v cannot survive a "+
			"round trip to Redis, so leadership would flap continuously",
			defaultLeaderLeaseTTL, defaultLeaderLeaseTTL/3)
	}
	if defaultLeaderLeaseTTL > time.Minute {
		t.Fatalf("defaultLeaderLeaseTTL = %v: a crashed leader's work stays "+
			"stranded for that long before another replica can take over",
			defaultLeaderLeaseTTL)
	}
}
