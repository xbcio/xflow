package control

import (
	"testing"
	"time"
)

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
