package distributed

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

var errInjectedRenewFailure = errors.New("injected transient renewal failure")

// flakyRenewClient delegates every command to a real client but fails EVALSHA
// while armed. The elector's only script is the renewal script, so this faults
// renewal without disturbing Campaign (SETNX) or Resign (EVAL... which also
// needs the prefix check below).
type flakyRenewClient struct {
	redis.UniversalClient
	failRemaining atomic.Int64
	attempts      atomic.Int64
}

func (c *flakyRenewClient) EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	c.attempts.Add(1)
	for {
		remaining := c.failRemaining.Load()
		if remaining <= 0 {
			break
		}
		if c.failRemaining.CompareAndSwap(remaining, remaining-1) {
			return redis.NewCmdResult(nil, errInjectedRenewFailure)
		}
	}
	return c.UniversalClient.EvalSha(ctx, sha1, keys, args...)
}

// Eval also serves the renewal script when EVALSHA returns NOSCRIPT, so an
// armed fault must cover it too.
func (c *flakyRenewClient) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	for {
		remaining := c.failRemaining.Load()
		if remaining <= 0 {
			break
		}
		if c.failRemaining.CompareAndSwap(remaining, remaining-1) {
			return redis.NewCmdResult(nil, errInjectedRenewFailure)
		}
	}
	return c.UniversalClient.Eval(ctx, script, keys, args...)
}

func newFlakyRenewElector(t *testing.T, ttl time.Duration) (*flakyRenewClient, *RedisLeaderElector, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	base := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = base.Close() })
	c := &flakyRenewClient{UniversalClient: base}
	return c, NewRedisLeaderElector(c, "test:leader:renewal-retry", ttl), mr
}

func drainLeaderNotify(t *testing.T, ch <-chan bool) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("Notify() did not emit after Campaign")
	}
}

// TestRedisLeaderElectorRetriesTransientRenewalErrors pins that a failed
// renewal does not immediately drop leadership: the retry lands inside the
// local deadline, extends it, and emits no false notification.
func TestRedisLeaderElectorRetriesTransientRenewalErrors(t *testing.T) {
	const ttl = 1200 * time.Millisecond
	client, l, _ := newFlakyRenewElector(t, ttl)
	// One transient failure: the first renewal tick fails, the retry succeeds.
	client.failRemaining.Store(1)

	notify := l.Notify()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := l.Campaign(ctx); err != nil {
		t.Fatalf("Campaign() error = %v", err)
	}
	drainLeaderNotify(t, notify)
	before := l.leaseDeadline()

	// Only a successful renewal moves the local deadline; polling avoids
	// pinning the exact retry cadence.
	deadline := time.Now().Add(3 * time.Second)
	for !l.leaseDeadline().After(before) {
		if time.Now().After(deadline) {
			t.Fatal("local deadline never advanced: the transient renewal error was not retried to success")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !l.IsLeader() {
		t.Fatal("IsLeader() = false after a transient renewal error recovered inside the deadline")
	}
	select {
	case v := <-notify:
		t.Fatalf("Notify() emitted %v after a transient renewal error, want no leadership change", v)
	default:
	}
	if got := client.attempts.Load(); got < 2 {
		t.Fatalf("renewal attempts = %d, want at least 2 (one failed, one retried)", got)
	}

	if err := l.Resign(context.Background()); err != nil {
		t.Fatalf("Resign() error = %v", err)
	}
}

// TestRedisLeaderElectorStepsDownWhenRenewalErrorsOutlastTheDeadline drives
// unbroken renewal errors with the tolerance raised so only the local deadline
// can stop the pass: leadership must drop and Notify must emit false.
func TestRedisLeaderElectorStepsDownWhenRenewalErrorsOutlastTheDeadline(t *testing.T) {
	const ttl = 900 * time.Millisecond
	client, l, _ := newFlakyRenewElector(t, ttl)
	l.WithLeaderRenewFailureTolerance(1000)
	client.failRemaining.Store(1 << 20)

	notify := l.Notify()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	started := time.Now()
	if err := l.Campaign(ctx); err != nil {
		t.Fatalf("Campaign() error = %v", err)
	}
	drainLeaderNotify(t, notify)

	select {
	case v := <-notify:
		if v {
			t.Fatal("Notify() emitted true, want false after renewal errors outlasted the deadline")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Notify() never emitted false after renewal errors outlasted the deadline")
	}
	if l.IsLeader() {
		t.Fatal("IsLeader() = true after renewal errors outlasted the deadline")
	}
	// One failed attempt plus at least one retry: the downgrade must not be the
	// first-error behavior the tolerance exists to replace.
	if got := client.attempts.Load(); got < 2 {
		t.Fatalf("renewal attempts = %d, want at least 2 retries before the deadline", got)
	}
	if elapsed := time.Since(started); elapsed < ttl/3 {
		t.Fatalf("stepped down after %v, before the first renewal tick at %v", elapsed, ttl/3)
	}
}

// TestRedisLeaderElectorDoesNotRetryLostLease pins the other half of the
// split: renewed == 0 means another instance owns the key and must step down
// immediately even when the transient-error tolerance is large.
func TestRedisLeaderElectorDoesNotRetryLostLease(t *testing.T) {
	const ttl = 900 * time.Millisecond
	client, l, mr := newFlakyRenewElector(t, ttl)
	l.WithLeaderRenewFailureTolerance(1000)

	notify := l.Notify()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := l.Campaign(ctx); err != nil {
		t.Fatalf("Campaign() error = %v", err)
	}
	drainLeaderNotify(t, notify)
	mr.Del("test:leader:renewal-retry")
	before := client.attempts.Load()

	select {
	case v := <-notify:
		if v {
			t.Fatal("Notify() emitted true, want false after the lease was lost")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Notify() never emitted false after the lease was lost")
	}
	if l.IsLeader() {
		t.Fatal("IsLeader() = true after the lease was lost")
	}
	// A retrying implementation would keep attempting until the ~600ms local
	// deadline; the immediate path makes exactly one attempt after the loss.
	if got := client.attempts.Load() - before; got > 1 {
		t.Fatalf("renewal attempts after lease loss = %d, want at most 1 (renewed == 0 must not be retried)", got)
	}
}

// TestRenewalFailureDoesNotExtendTheLocalDeadline pins the lease-margin
// invariant on the failure path: only a successful renewal may move the local
// deadline. Extending it after an error would keep this replica answering
// IsLeader() == true past the Redis key's lifetime — the split-brain window the
// ttl/3 margin exists to close.
func TestRenewalFailureDoesNotExtendTheLocalDeadline(t *testing.T) {
	client, l, _ := newFlakyRenewElector(t, 900*time.Millisecond)
	// Retry rather than bail on the first error, so the failure path is
	// exercised repeatedly instead of once.
	l.WithLeaderRenewFailureTolerance(1000)
	client.failRemaining.Store(1 << 20)

	l.extendLocalDeadline()
	before := l.leaseDeadline()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() { done <- l.renewLease(ctx, "token", 900, leaderRenewRetryInterval(l.ttl)) }()

	// More than one retry interval, so at least one failed attempt happened.
	time.Sleep(250 * time.Millisecond)
	if got := l.leaseDeadline(); !got.Equal(before) {
		cancel()
		<-done
		t.Fatalf("local deadline moved from %v to %v after a failed renewal: a failure must never extend the lease view", before, got)
	}
	cancel()
	<-done
}

func TestRenewFailuresExhausted(t *testing.T) {
	for _, tc := range []struct {
		name      string
		tolerance int
		failures  int
		want      bool
	}{
		{"zero tolerance keeps the immediate downgrade", 0, 1, true},
		{"negative tolerance keeps the immediate downgrade", -1, 1, true},
		{"tolerance allows earlier failures", 3, 1, false},
		{"tolerance allows earlier failures 2", 3, 2, false},
		{"tolerance is exhausted at the threshold", 3, 3, true},
		{"beyond the threshold stays exhausted", 3, 4, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &RedisLeaderElector{renewFailureTolerance: tc.tolerance}
			if got := e.renewFailuresExhausted(tc.failures); got != tc.want {
				t.Fatalf("renewFailuresExhausted(%d) with tolerance %d = %v, want %v",
					tc.failures, tc.tolerance, got, tc.want)
			}
		})
	}
}

func TestLeaderRenewRetryInterval(t *testing.T) {
	for _, tc := range []struct {
		ttl  time.Duration
		want time.Duration
	}{
		{time.Millisecond, 10 * time.Millisecond},
		{9 * time.Millisecond, 10 * time.Millisecond},
		{900 * time.Millisecond, 100 * time.Millisecond},
		{90 * time.Second, time.Second},
	} {
		if got := leaderRenewRetryInterval(tc.ttl); got != tc.want {
			t.Fatalf("leaderRenewRetryInterval(%v) = %v, want %v", tc.ttl, got, tc.want)
		}
	}
}
