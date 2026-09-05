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
	if l.now().Before(st.lockedTill) {
		return false
	}
	if !st.lockedTill.IsZero() {
		// The lockout elapsed. Drop the entry entirely so the source gets a
		// fresh budget; leaving fails at max would let one further failure
		// re-lock immediately and the window would never really expire.
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
	st := l.state[sourceIP]
	if st == nil {
		st = &enrollFailState{}
		l.state[sourceIP] = st
	}
	st.fails++
	if st.fails >= l.max {
		st.lockedTill = l.now().Add(l.lockFor)
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
