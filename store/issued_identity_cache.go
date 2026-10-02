package store

import (
	"context"
	"sync"
	"time"
)

const (
	// DefaultIssuedIdentityCacheTTL is the entry lifetime an embedding host is
	// expected to hand the constructor when it wants the cache and has no reason
	// to pick its own number (SAS does exactly that). It is a deliberate middle
	// ground: long enough that the ~10-13 runner requests/sec a host sees collapse
	// to about one store read per runner per half-minute, short enough that a
	// revocation performed by a *different* process — another replica, or an
	// operator at a SQL prompt — reaches this process's credential checks within
	// half a minute. Nothing in this package applies it implicitly: a caller that
	// passes 0 gets no cache, because the shared cmd/server binary must be inert
	// until an operator asks for otherwise.
	DefaultIssuedIdentityCacheTTL = 30 * time.Second

	// MaxIssuedIdentityCacheTTL caps what a host may configure. For any process that
	// did not itself perform the mutation the TTL is the ONLY bound on how long a
	// revoked or rotated credential keeps authenticating, so widening the cap is a
	// deliberate code change with a security argument attached, not a config knob.
	MaxIssuedIdentityCacheTTL = 5 * time.Minute

	// DefaultIssuedIdentityCacheMaxEntries bounds the map by construction. Positive
	// entries are normally bounded by the fleet's runner count, which is why
	// absence is not cached — but the store's own equality is not Go's: the SQL
	// table's runner_id is utf8mb4_unicode_ci, so `WHERE runner_id = ?` matches
	// case- and trailing-space-insensitively while this map does not. A caller
	// presenting a case variant of a real runner id therefore gets a genuine row
	// back and would mint a fresh entry for a key that is not the canonical one.
	// A cap turns that from an unbounded allocation into a bounded one; a healthy
	// fleet never approaches it.
	DefaultIssuedIdentityCacheMaxEntries = 16384

	// Lazy expiry bounds the map only while lookups keep arriving; a fleet that goes
	// quiet would keep every entry forever. These follow enroll_limiter.go's sweep
	// bounds: sweep at most once per cooldown, and only once the map is big enough
	// that the walk is worth it.
	issuedIdentityCacheSweepThreshold = 4096
	issuedIdentityCacheSweepCooldown  = time.Minute
)

// IssuedIdentityCacheObserver receives cache outcomes. It is declared here rather
// than importing observability/metrics so the store layer keeps no dependency on
// the metrics registry — the same local-mirror shape as
// execution/subgraph.PackageCacheObserver. A nil observer means silent.
//
// result is a closed enum: "hit" or "miss". A miss covers absence, storage errors
// and elapsed TTL alike; the caller already distinguishes those and this series is
// for sizing the cache, not for alerting on the store.
//
// Both callbacks run with no cache lock held, and may run concurrently from many
// goroutines (one per in-flight Lookup). They must not block: they sit on the
// runner-authentication hot path, and a callback that waits will show up as
// runner request latency. An implementation must also not call back into the
// store it observes — it is only ever called from inside a store method, so a
// re-entrant call would be free to deadlock on this store's own mutex were the
// callback invoked under it; it is invoked outside, and the contract keeps it that
// way.
type IssuedIdentityCacheObserver interface {
	OnIssuedIdentityCache(result string)
	OnIssuedIdentityCacheEntries(count int)
}

// NewCachedIssuedIdentityStore wraps inner with a process-local, TTL-bounded,
// write-invalidated read-through cache for Lookup.
//
// inner == nil, or ttl <= 0, returns inner unchanged — caching is off, and a host
// that sets no TTL pays exactly the pre-cache behaviour. ttl above
// MaxIssuedIdentityCacheTTL is clamped to the cap rather than rejected: a host that
// asks for an hour has made a tuning mistake, not a request to widen the staleness
// bound this package is willing to guarantee.
//
// The returned store is safe for concurrent use.
func NewCachedIssuedIdentityStore(inner IssuedIdentityStore, ttl time.Duration, observer IssuedIdentityCacheObserver) IssuedIdentityStore {
	if inner == nil || ttl <= 0 {
		return inner
	}
	if ttl > MaxIssuedIdentityCacheTTL {
		ttl = MaxIssuedIdentityCacheTTL
	}
	return &cachedIssuedIdentityStore{
		inner:      inner,
		ttl:        ttl,
		observer:   observer,
		maxEntries: DefaultIssuedIdentityCacheMaxEntries,
		entries:    make(map[string]cachedIssuedIdentityEntry),
	}
}

type cachedIssuedIdentityEntry struct {
	// identity is always a private copy. The value handed to a caller and the value
	// retained here never share a backing array, so a caller mutating a Scope slice
	// cannot corrupt what the next request authenticates against.
	identity IssuedIdentity
	// deadline is an absolute instant, anchored at the read that produced the row.
	// Hits do NOT extend it: a sliding deadline would let a steady request stream
	// keep one row alive indefinitely, which is precisely the stale-revocation
	// window this cache is supposed to bound.
	deadline time.Time
}

type cachedIssuedIdentityStore struct {
	inner      IssuedIdentityStore
	ttl        time.Duration
	maxEntries int
	observer   IssuedIdentityCacheObserver
	now        func() time.Time

	mu        sync.Mutex
	entries   map[string]cachedIssuedIdentityEntry
	gen       uint64
	lastSweep time.Time
}

var _ IssuedIdentityStore = (*cachedIssuedIdentityStore)(nil)

func (c *cachedIssuedIdentityStore) Issue(ctx context.Context, id IssuedIdentity) error {
	defer c.invalidate(id.RunnerID)
	return c.inner.Issue(ctx, id)
}

func (c *cachedIssuedIdentityStore) Revoke(ctx context.Context, runnerID string, scope OwnerScope) error {
	defer c.invalidate(runnerID)
	return c.inner.Revoke(ctx, runnerID, scope)
}

func (c *cachedIssuedIdentityStore) Renew(ctx context.Context, runnerID string, expiresAt time.Time) error {
	defer c.invalidate(runnerID)
	return c.inner.Renew(ctx, runnerID, expiresAt)
}

// RotateCredential advances a live identity to its next token generation. The
// cached row carries the PREVIOUS hash, so without this eviction a rotation would
// deny the runner that just rotated while continuing to accept the token the store
// had already retired.
//
// In the rotating process that is exact. On a replica that did not perform the
// rotation it is not: that replica's cached row keeps matching the retired token as
// its CURRENT hash until the entry deadline, so the retired token survives there
// for up to the TTL rather than for the grace window the rotation intended. That is
// the same cross-process staleness the operator accepted for revocation, and the
// TTL is what bounds it — but it means the grace window is not a fleet-wide
// guarantee, only a local one.
func (c *cachedIssuedIdentityStore) RotateCredential(ctx context.Context, runnerID string, expectGeneration int64, newHash [32]byte, previousValidUntil time.Time) (int64, error) {
	defer c.invalidate(runnerID)
	return c.inner.RotateCredential(ctx, runnerID, expectGeneration, newHash, previousValidUntil)
}

// List is a straight passthrough and is deliberately never cached: it is the
// authoritative roster the management surface renders, and a cached roster would
// report a runner as present after it had been revoked — the one direction an
// operator must never be lied to in.
func (c *cachedIssuedIdentityStore) List(ctx context.Context) ([]IssuedIdentity, error) {
	return c.inner.List(ctx)
}

func (c *cachedIssuedIdentityStore) Lookup(ctx context.Context, runnerID string) (IssuedIdentity, bool, error) {
	now := c.clock()

	c.mu.Lock()
	if entry, ok := c.entries[runnerID]; ok && now.Before(entry.deadline) {
		id := entry.identity.Clone()
		c.mu.Unlock()
		c.observe("hit")
		return id, true, nil
	}
	// Snapshot the generation under the same lock that observed the entry map, and
	// before the store read. A mutation that lands while the read is in flight must
	// not be overwritten by the row this read is about to return: without this
	// check, a Revoke racing a Lookup could have its eviction undone by the
	// lookup's own insert, and the process would go on serving a credential it had
	// just revoked. The bound on that race is one store round trip, not the TTL.
	//
	// An entry that is present but past its deadline is left in the map here rather
	// than deleted: it is a dead slot, never a served row (the check above rejects
	// it), and reclaimLocked/sweepLocked are the two places that remove it. Paying
	// for its removal on this path would mean a second locked section on every
	// expiry.
	gen := c.gen
	c.mu.Unlock()

	id, found, err := c.inner.Lookup(ctx, runnerID)
	if err != nil {
		// An outage is never remembered. Caching a failure as "absent" would turn a
		// blip into a lockout for the duration of the TTL, and the authenticator's
		// logs depend on the absent/error distinction being truthful.
		c.observe("miss")
		return IssuedIdentity{}, false, err
	}
	if !found {
		// Absence is deliberately NOT cached, for two independent reasons. The
		// principal authenticator probes Lookup for every runner-id header before it
		// has seen a credential, so a forged or unknown id would otherwise be able to
		// fill this map without bound. And a negative entry is the one kind that can
		// refuse a *legitimately issued* runner: it has to be invalidated by the
		// Issue that mints that runner, in whichever process performs it. Positive
		// entries can only ever serve a row that really existed, one TTL late. (The
		// bounded-growth argument is only as strong as the max-entries cap, because
		// the store's collation — not Go's equality — decides what "the same runner
		// id" means; see the constant.)
		c.observe("miss")
		return IssuedIdentity{}, false, nil
	}

	c.mu.Lock()
	publish := c.gen == gen
	var count int
	if publish {
		c.reclaimLocked(now)
		c.entries[runnerID] = cachedIssuedIdentityEntry{
			identity: id.Clone(),
			deadline: c.entryDeadline(now, id),
		}
		c.sweepLocked(now)
		count = len(c.entries)
	}
	c.mu.Unlock()
	if publish {
		c.observeEntries(count)
	}

	c.observe("miss")
	return id, true, nil
}

// entryDeadline is min(read-start + ttl, the row's own expiry). Clamping to
// ExpiresAt costs nothing and means an expired row is re-read from the store at its
// expiry instead of being served until the cache TTL lapses — the credential check
// would reject it either way, so this is purely about not answering from a row the
// store would already call dead. A row whose ExpiresAt is already behind the read
// gets a deadline in the past, so it is re-read on the very next lookup.
func (c *cachedIssuedIdentityStore) entryDeadline(now time.Time, id IssuedIdentity) time.Time {
	deadline := now.Add(c.ttl)
	if !id.ExpiresAt.IsZero() && id.ExpiresAt.Before(deadline) {
		deadline = id.ExpiresAt
	}
	return deadline
}

// invalidate drops runnerID's entry and always advances the generation. The
// generation advance is unconditional and is the whole point: it is what makes an
// in-flight Lookup decline to publish a row that predates this mutation, even when
// there was no entry to delete.
//
// It runs on the error path too (see the callers' deferred form). A Revoke that
// returns an error may still have committed — a timeout is not a rollback — and
// this is the security-relevant path, so the cost of one extra store read is
// preferred to serving a row the store may no longer hold.
//
// It bounds the window for *newly arriving* lookups. It does not retract an entry
// handed out by a concurrent hit that already passed its deadline check before the
// mutation committed; that request saw the pre-mutation row, which is what a
// read-before-write ordering permits in any cache. The residual window for a
// same-process mutation is therefore one concurrent hit, not the TTL.
func (c *cachedIssuedIdentityStore) invalidate(runnerID string) {
	c.mu.Lock()
	_, existed := c.entries[runnerID]
	if existed {
		delete(c.entries, runnerID)
	}
	c.gen++
	count := len(c.entries)
	c.mu.Unlock()
	if existed {
		c.observeEntries(count)
	}
}

// reclaimLocked enforces the entry cap. Expired entries go first because they are
// free to identify; if the map is still at the cap, the single earliest-expiring
// entry is dropped. The O(n) walk only runs at the cap, which a healthy fleet —
// bounded by its runner count — never reaches.
func (c *cachedIssuedIdentityStore) reclaimLocked(now time.Time) {
	if len(c.entries) < c.maxEntries {
		return
	}
	for key, entry := range c.entries {
		if !now.Before(entry.deadline) {
			delete(c.entries, key)
		}
	}
	if len(c.entries) < c.maxEntries {
		return
	}
	var (
		oldestKey string
		oldest    time.Time
	)
	for key, entry := range c.entries {
		if oldestKey == "" || entry.deadline.Before(oldest) {
			oldestKey, oldest = key, entry.deadline
		}
	}
	if oldestKey != "" {
		delete(c.entries, oldestKey)
	}
}

func (c *cachedIssuedIdentityStore) sweepLocked(now time.Time) {
	if len(c.entries) < issuedIdentityCacheSweepThreshold || now.Sub(c.lastSweep) < issuedIdentityCacheSweepCooldown {
		return
	}
	c.lastSweep = now
	for key, entry := range c.entries {
		if !now.Before(entry.deadline) {
			delete(c.entries, key)
		}
	}
}

func (c *cachedIssuedIdentityStore) clock() time.Time {
	if c.now != nil {
		return c.now().UTC()
	}
	return time.Now().UTC()
}

func (c *cachedIssuedIdentityStore) observe(result string) {
	if c.observer != nil {
		c.observer.OnIssuedIdentityCache(result)
	}
}

func (c *cachedIssuedIdentityStore) observeEntries(count int) {
	if c.observer != nil {
		c.observer.OnIssuedIdentityCacheEntries(count)
	}
}
