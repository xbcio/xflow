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
)

// enrollLimiter throttles failed enroll attempts per source IP. The enroll
// endpoint is unauthenticated by construction — a runner has no credential yet
// — so without this a reusable code is a standing brute-force target with no
// server-side cost to the attacker.
//
// now is injectable because a lockout test that sleeps for the real window is
// both unrunnable in CI and flaky.
type enrollLimiter struct {
	mu      sync.Mutex
	state   map[string]*enrollFailState
	max     int
	lockFor time.Duration
	now     func() time.Time
}

type enrollFailState struct {
	fails      int
	lockedTill time.Time
	lastFail   time.Time
}

// idleStale reports whether st has never triggered a lockout and has gone
// quiet for at least the lockout window. Such an entry is safe to reclaim
// unconditionally — it carries no active (or ever-active) lockout, so
// deleting it cannot prematurely lift a block.
func idleStale(st *enrollFailState, now time.Time, lockFor time.Duration) bool {
	return st.lockedTill.IsZero() && now.Sub(st.lastFail) >= lockFor
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
	if !st.lockedTill.IsZero() || idleStale(st, now, l.lockFor) {
		// Either the lockout elapsed, or the source went idle past the window
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
	if len(l.state) > enrollLimiterSweepThreshold {
		l.sweep(now)
	}
}

// sweep walks the map once and evicts every entry that is both un-locked-out
// (it never reached the failure limit) and idle-stale (quiet for at least the
// lockout window). It runs synchronously inside RecordFailure's own critical
// section — there is no goroutine, no timer, no Close(). Lazy per-key
// eviction in Allow cannot bound the map by itself, because the source that
// never calls Allow again is precisely the source that never triggers it.
func (l *enrollLimiter) sweep(now time.Time) {
	for ip, st := range l.state {
		if idleStale(st, now, l.lockFor) {
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
