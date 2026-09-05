package control

import (
	"sync"
	"time"
)

// Spec §2.3.4: ten consecutive failures from one source lock it out for fifteen
// minutes.
const (
	defaultEnrollFailureLimit = 10
	defaultEnrollLockout      = 15 * time.Minute

	// enrollLimiterSweepThreshold caps how large l.state may grow before
	// RecordFailure performs an opportunistic sweep. Lazy per-key eviction in
	// Allow cannot bound the map on its own: a source that fails below the
	// limit and never comes back is exactly the source that never calls
	// Allow again, so it never triggers its own eviction. On an
	// unauthenticated endpoint reachable from arbitrary (including IPv6)
	// source addresses, that is a standing memory-exhaustion vector.
	enrollLimiterSweepThreshold = 1024

	// enrollLimiterSweepCooldown gates how often RecordFailure is willing to
	// pay for a full O(n) walk of l.state. Without it, sustained load that
	// keeps the map oversized (more arrivals than reclaimable entries) would
	// make every single RecordFailure call walk the whole map while holding
	// the one mutex every other request needs — a fix for one resource
	// exhaustion vector must not become a CPU/latency amplifier itself.
	enrollLimiterSweepCooldown = time.Minute
)

// enrollLimiter throttles failed enroll attempts per source IP. The enroll
// endpoint is unauthenticated by construction — a runner has no credential yet
// — so without this a reusable code is a standing brute-force target with no
// server-side cost to the attacker.
//
// now is injectable because a lockout test that sleeps for the real window is
// both unrunnable in CI and flaky.
type enrollLimiter struct {
	mu        sync.Mutex
	state     map[string]*enrollFailState
	max       int
	lockFor   time.Duration
	now       func() time.Time
	lastSweep time.Time
}

type enrollFailState struct {
	fails      int
	lockedTill time.Time
	lastFail   time.Time
}

// reclaimable reports whether an entry can be dropped without changing what
// Allow would decide for that source. Two cases qualify: a counter that has
// gone idle past the lockout window without ever reaching the limit, and a
// lockout that has already expired (the very next Allow for that source would
// delete it and admit anyway, so reclaiming it here changes nothing). An
// entry still inside its lockout window is never reclaimable — dropping it
// would hand the source a fresh budget mid-lockout.
func reclaimable(st *enrollFailState, now time.Time, lockFor time.Duration) bool {
	if !st.lockedTill.IsZero() {
		return !now.Before(st.lockedTill)
	}
	return now.Sub(st.lastFail) >= lockFor
}

func newEnrollLimiter(max int, lockFor time.Duration) *enrollLimiter {
	return &enrollLimiter{
		state:   make(map[string]*enrollFailState),
		max:     max,
		lockFor: lockFor,
		now:     time.Now,
	}
}

// Allow reports whether sourceIP may attempt an enroll right now. A locked-out
// source is rejected before the code store is ever touched.
func (l *enrollLimiter) Allow(sourceIP string) bool {
	if l == nil || sourceIP == "" {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	st := l.state[sourceIP]
	if st == nil {
		return true
	}
	now := l.now()
	if now.Before(st.lockedTill) {
		return false
	}
	if reclaimable(st, now, l.lockFor) {
		// The lockout elapsed, or the source went idle past the window
		// without ever reaching the limit. Drop the entry entirely so the
		// source gets a fresh budget; leaving fails at max would let one
		// further failure re-lock immediately and the window would never
		// really expire — and leaving a stale sub-limit entry around forever
		// would grow the map without bound.
		delete(l.state, sourceIP)
	}
	return true
}

func (l *enrollLimiter) RecordFailure(sourceIP string) {
	if l == nil || sourceIP == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	st := l.state[sourceIP]
	if st == nil {
		st = &enrollFailState{}
		l.state[sourceIP] = st
	}
	st.fails++
	st.lastFail = now
	if st.fails >= l.max {
		st.lockedTill = now.Add(l.lockFor)
	}
	if len(l.state) > enrollLimiterSweepThreshold && now.Sub(l.lastSweep) >= enrollLimiterSweepCooldown {
		l.sweep(now)
		l.lastSweep = now
	}
}

// sweep walks the map once and evicts every reclaimable entry — one that
// never reached the failure limit and has gone idle past the lockout window,
// or one whose lockout has already expired. It runs synchronously inside
// RecordFailure's own critical section — there is no goroutine, no timer, no
// Close(). Lazy per-key eviction in Allow cannot bound the map by itself,
// because the source that never calls Allow again is precisely the source
// that never triggers it. An entry still inside an active lockout is never
// touched: reclaimable returns false for it, by construction.
func (l *enrollLimiter) sweep(now time.Time) {
	for ip, st := range l.state {
		if reclaimable(st, now, l.lockFor) {
			delete(l.state, ip)
		}
	}
}

func (l *enrollLimiter) RecordSuccess(sourceIP string) {
	if l == nil || sourceIP == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.state, sourceIP)
}
