package engine

import (
	"sync"
	"time"
)

const (
	// expireBackoffBase is the wait after an execution's first failed expiry.
	expireBackoffBase = time.Second
	// expireBackoffMax caps the wait, so a persistently failing execution is
	// still retried a few times an hour rather than being abandoned.
	expireBackoffMax = 5 * time.Minute
	// expireBackoffMaxEntries bounds how many failing keys are remembered.
	expireBackoffMaxEntries = 4096
)

type expireBackoffEntry struct {
	failures int
	retryAt  time.Time
}

// ExpireBackoff spaces out the retries of ExpireExecution calls that keep
// failing, key by key, so one broken execution cannot take the sweep's whole
// page and starve the ones behind it.
//
// A key is opaque: callers with a namespace dimension fold it into the key. The
// wait after the n-th consecutive failure doubles from one second and stops at
// five minutes, with no jitter, so a schedule is reproducible. The clock is
// the caller's: every method takes the current time, which keeps the schedule
// testable without sleeping.
//
// Memory is bounded. Once expireBackoffMaxEntries keys are remembered, recording
// a failure for a new key evicts the record that is due soonest. That record is
// either stale (its execution stopped being listed, so Success never cleared
// it) or the next one to be retried anyway, so dropping it costs at most one
// early retry and never blocks the new key from being tracked.
//
// The zero value is ready to use. It is safe for concurrent use.
type ExpireBackoff struct {
	mu         sync.Mutex
	entries    map[string]expireBackoffEntry
	maxEntries int // zero means expireBackoffMaxEntries
}

// NewExpireBackoff returns an empty ExpireBackoff.
func NewExpireBackoff() *ExpireBackoff {
	return &ExpireBackoff{}
}

// Allow reports whether key may be attempted at now: it has no recorded
// failure, or its backoff window has elapsed.
func (b *ExpireBackoff) Allow(key string, now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	e, ok := b.entries[key]
	return !ok || !now.Before(e.retryAt)
}

// Failure records one more consecutive failure for key at now, schedules its
// next attempt, and returns the new consecutive failure count.
func (b *ExpireBackoff) Failure(key string, now time.Time) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.entries == nil {
		b.entries = make(map[string]expireBackoffEntry)
	}
	e, ok := b.entries[key]
	if !ok && len(b.entries) >= b.capacity() {
		b.evictEarliestLocked()
	}
	e.failures++
	e.retryAt = now.Add(expireBackoffDelay(e.failures))
	b.entries[key] = e
	return e.failures
}

// Success forgets key. It is a no-op for a key with no record.
func (b *ExpireBackoff) Success(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.entries, key)
}

// Len returns how many keys currently have a failure on record, whether or not
// their backoff window has elapsed yet.
func (b *ExpireBackoff) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.entries)
}

// Waiting returns how many keys are still inside their backoff window at now,
// that is how many Allow would refuse. Unlike Len it does not count a record
// whose window has elapsed, such as one for an execution that was settled
// elsewhere and so will never see Success.
func (b *ExpireBackoff) Waiting(now time.Time) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	waiting := 0
	for _, e := range b.entries {
		if now.Before(e.retryAt) {
			waiting++
		}
	}
	return waiting
}

// Failures returns key's consecutive failure count, or zero for a key with no
// record.
func (b *ExpireBackoff) Failures(key string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.entries[key].failures
}

func (b *ExpireBackoff) capacity() int {
	if b.maxEntries > 0 {
		return b.maxEntries
	}
	return expireBackoffMaxEntries
}

// evictEarliestLocked drops the record that is due soonest, smallest key first
// on a tie so the choice does not depend on map order.
func (b *ExpireBackoff) evictEarliestLocked() {
	var victim string
	var earliest time.Time
	found := false
	for key, e := range b.entries {
		if !found || e.retryAt.Before(earliest) || (e.retryAt.Equal(earliest) && key < victim) {
			victim, earliest, found = key, e.retryAt, true
		}
	}
	if found {
		delete(b.entries, victim)
	}
}

// expireBackoffDelay is the wait after the failures-th consecutive failure.
func expireBackoffDelay(failures int) time.Duration {
	d := expireBackoffBase
	for i := 1; i < failures && d < expireBackoffMax; i++ {
		d *= 2
	}
	if d > expireBackoffMax {
		d = expireBackoffMax
	}
	return d
}
