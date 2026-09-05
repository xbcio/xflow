package control

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// stateLen reads the tracked-source count under the same mutex the limiter
// itself uses, so it is safe to call from a concurrency test alongside other
// goroutines still touching the limiter.
func stateLen(l *enrollLimiter) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.state)
}

// fakeClock lets the lockout-expiry test run in microseconds. A test that
// actually sleeps for the 15-minute window is both unrunnable and flaky.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestLimiter(max int, lockFor time.Duration) (*enrollLimiter, *fakeClock) {
	clk := &fakeClock{t: time.Unix(1700000000, 0).UTC()}
	l := newEnrollLimiter(max, lockFor)
	l.now = clk.now
	return l, clk
}

func TestLimiterLocksOutAfterConfiguredFailures(t *testing.T) {
	l, _ := newTestLimiter(10, 15*time.Minute)
	const ip = "10.0.0.1"
	for i := 0; i < 10; i++ {
		if !l.Allow(ip) {
			t.Fatalf("attempt %d was blocked before the limit was reached", i+1)
		}
		l.RecordFailure(ip)
	}
	if l.Allow(ip) {
		t.Fatal("11th attempt must be blocked after 10 consecutive failures")
	}
}

func TestLimiterIsPerSourceIP(t *testing.T) {
	l, _ := newTestLimiter(10, 15*time.Minute)
	for i := 0; i < 10; i++ {
		l.RecordFailure("10.0.0.1")
	}
	if l.Allow("10.0.0.1") {
		t.Fatal("the failing source must be locked out")
	}
	if !l.Allow("10.0.0.2") {
		t.Fatal("an unrelated source must not be collateral damage")
	}
}

func TestLimiterUnlocksAfterTheWindow(t *testing.T) {
	l, clk := newTestLimiter(10, 15*time.Minute)
	const ip = "10.0.0.1"
	for i := 0; i < 10; i++ {
		l.RecordFailure(ip)
	}
	clk.advance(14 * time.Minute)
	if l.Allow(ip) {
		t.Fatal("still inside the lockout window; must stay blocked")
	}
	clk.advance(2 * time.Minute)
	if !l.Allow(ip) {
		t.Fatal("lockout window elapsed; must be allowed again")
	}
	// The budget must be reset, not left at the limit — otherwise a single
	// further failure re-locks immediately and the window never really expires.
	l.RecordFailure(ip)
	if !l.Allow(ip) {
		t.Fatal("failure budget was not reset when the lockout expired")
	}
}

func TestLimiterSuccessResetsTheCounter(t *testing.T) {
	l, _ := newTestLimiter(10, 15*time.Minute)
	const ip = "10.0.0.1"
	for i := 0; i < 9; i++ {
		l.RecordFailure(ip)
	}
	l.RecordSuccess(ip)
	for i := 0; i < 9; i++ {
		if !l.Allow(ip) {
			t.Fatalf("attempt %d blocked; a success must clear the failure count", i+1)
		}
		l.RecordFailure(ip)
	}
}

func TestLimiterIgnoresUnknownSource(t *testing.T) {
	l, _ := newTestLimiter(10, 15*time.Minute)
	// An empty SourceIP means the transport could not attribute the request.
	// Throttling all of them into one bucket would be a self-inflicted DoS.
	for i := 0; i < 50; i++ {
		l.RecordFailure("")
	}
	if !l.Allow("") {
		t.Fatal("an unattributable source must not be throttled as one shared bucket")
	}
}

func TestNilLimiterAllows(t *testing.T) {
	var l *enrollLimiter
	if !l.Allow("10.0.0.1") {
		t.Fatal("a nil limiter must be a no-op, not a deny-all")
	}
	l.RecordFailure("10.0.0.1")
	l.RecordSuccess("10.0.0.1")
}

func TestLimiterEvictsIdleEntryBelowLimit(t *testing.T) {
	l, clk := newTestLimiter(10, 15*time.Minute)
	const ip = "10.0.0.1"
	for i := 0; i < 5; i++ {
		l.RecordFailure(ip)
	}
	if got := stateLen(l); got != 1 {
		t.Fatalf("expected 1 tracked source after failures below the limit, got %d", got)
	}

	clk.advance(14 * time.Minute)
	l.Allow(ip) // still inside the idle window; must not be evicted yet
	if got := stateLen(l); got != 1 {
		t.Fatalf("entry evicted before the idle window elapsed, got %d entries", got)
	}

	clk.advance(1 * time.Minute) // now exactly at the lockFor boundary
	if !l.Allow(ip) {
		t.Fatal("a source that never reached the failure limit must not be blocked")
	}
	if got := stateLen(l); got != 0 {
		t.Fatalf("idle entry below the limit must be evicted once the window elapses, got %d entries remaining", got)
	}
}

func TestLimiterSweepBoundsMapGrowth(t *testing.T) {
	l, clk := newTestLimiter(10, 15*time.Minute)

	// Fill the map past the sweep threshold with sources that fail once each
	// and never return — exactly the vector that never self-evicts via Allow.
	for i := 0; i <= enrollLimiterSweepThreshold; i++ {
		l.RecordFailure(fmt.Sprintf("10.%d.%d.%d", i/65536, (i/256)%256, i%256))
	}
	if got := stateLen(l); got <= enrollLimiterSweepThreshold {
		t.Fatalf("test setup did not actually cross the sweep threshold: got %d entries", got)
	}

	clk.advance(15 * time.Minute) // every existing entry is now idle-stale

	// One more failure from a fresh source pushes len(l.state) back over the
	// threshold and must trigger the opportunistic sweep.
	l.RecordFailure("10.0.0.99")
	if got := stateLen(l); got != 1 {
		t.Fatalf("map was not swept after crossing the threshold: got %d entries, want 1 (only the fresh source)", got)
	}
}

func TestLimiterUnlocksExactlyAtBoundary(t *testing.T) {
	l, clk := newTestLimiter(10, 15*time.Minute)
	const ip = "10.0.0.1"
	for i := 0; i < 10; i++ {
		l.RecordFailure(ip)
	}
	clk.advance(15 * time.Minute) // now == lockedTill exactly
	if !l.Allow(ip) {
		t.Fatal("at the exact boundary (now == lockedTill) the source must already be unlocked; time.Time.Before is strict")
	}
}

func TestLimiterConcurrentAccess(t *testing.T) {
	l := newEnrollLimiter(defaultEnrollFailureLimit, defaultEnrollLockout)

	const dedicatedSources = 8
	var wg sync.WaitGroup

	// Each of these sources is driven exclusively by one goroutine, straight
	// past the failure limit. No other goroutine ever touches the same
	// source, so the accepted burst-before-lockout race (concurrent Allow
	// calls interleaving with RecordFailure on the *same* source — accepted
	// by the Task 2 review ruling as depth-in-defense against a 2^256 search
	// space, not the sole defense) never comes into play here. This only
	// exercises the mutex, not the race the design accepts.
	for i := 0; i < dedicatedSources; i++ {
		ip := fmt.Sprintf("10.1.0.%d", i)
		wg.Add(1)
		go func(ip string) {
			defer wg.Done()
			for j := 0; j < defaultEnrollFailureLimit; j++ {
				l.Allow(ip)
				l.RecordFailure(ip)
			}
		}(ip)
	}

	// A handful of goroutines hammer a small set of shared sources with a mix
	// of every operation. We deliberately assert nothing about their exact
	// failure counts — the concurrent-burst window is accepted design (see
	// review ruling), so any such assertion would be flaky by construction.
	// This block exists purely to give -race something to find if the
	// locking is wrong, and to prove nothing panics under contention.
	sharedIPs := []string{"10.0.0.21", "10.0.0.22", "10.0.0.23"}
	for i := 0; i < 20; i++ {
		ip := sharedIPs[i%len(sharedIPs)]
		wg.Add(1)
		go func(ip string) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				switch j % 3 {
				case 0:
					l.Allow(ip)
				case 1:
					l.RecordFailure(ip)
				case 2:
					l.RecordSuccess(ip)
				}
			}
		}(ip)
	}

	wg.Wait()

	for i := 0; i < dedicatedSources; i++ {
		ip := fmt.Sprintf("10.1.0.%d", i)
		if l.Allow(ip) {
			t.Fatalf("source %s was driven past the failure limit by a single goroutine and must be locked out", ip)
		}
	}
}
