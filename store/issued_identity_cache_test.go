package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// stubIssuedIdentityStore is a controllable inner store: it counts every call and
// can hold Lookup open so a test can land a mutation while a read is in flight.
type stubIssuedIdentityStore struct {
	mu         sync.Mutex
	identities map[string]IssuedIdentity
	lookups    int
	issues     int
	revokes    int
	renews     int
	rotations  int
	lists      int
	lookupErr  error

	lookupGate    chan struct{} // non-nil: Lookup blocks until it is closed
	lookupEntered chan struct{} // non-nil: signalled once Lookup is inside
}

var _ IssuedIdentityStore = (*stubIssuedIdentityStore)(nil)

func newStubIssuedIdentityStore(ids ...IssuedIdentity) *stubIssuedIdentityStore {
	s := &stubIssuedIdentityStore{identities: make(map[string]IssuedIdentity)}
	for _, id := range ids {
		s.identities[id.RunnerID] = id
	}
	return s
}

func (s *stubIssuedIdentityStore) Issue(_ context.Context, id IssuedIdentity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.issues++
	s.identities[id.RunnerID] = id.Clone()
	return nil
}

func (s *stubIssuedIdentityStore) Lookup(_ context.Context, runnerID string) (IssuedIdentity, bool, error) {
	s.mu.Lock()
	s.lookups++
	id, ok := s.identities[runnerID]
	err := s.lookupErr
	gate, entered := s.lookupGate, s.lookupEntered
	s.mu.Unlock()

	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	if gate != nil {
		<-gate
	}
	if err != nil {
		return IssuedIdentity{}, false, err
	}
	if !ok {
		return IssuedIdentity{}, false, nil
	}
	return id.Clone(), true, nil
}

func (s *stubIssuedIdentityStore) List(_ context.Context) ([]IssuedIdentity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lists++
	out := make([]IssuedIdentity, 0, len(s.identities))
	for _, id := range s.identities {
		out = append(out, id.Clone())
	}
	return out, nil
}

func (s *stubIssuedIdentityStore) Revoke(_ context.Context, runnerID string, scope OwnerScope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.revokes++
	if id, ok := s.identities[runnerID]; ok {
		id.RevokedAt = time.Now().UTC()
		s.identities[runnerID] = id
	}
	return nil
}

func (s *stubIssuedIdentityStore) Renew(_ context.Context, runnerID string, expiresAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.renews++
	if id, ok := s.identities[runnerID]; ok {
		id.ExpiresAt = expiresAt
		s.identities[runnerID] = id
	}
	return nil
}

func (s *stubIssuedIdentityStore) RotateCredential(_ context.Context, runnerID string, expectGeneration int64, newHash [32]byte, previousValidUntil time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rotations++
	id, ok := s.identities[runnerID]
	if !ok {
		return 0, nil
	}
	id.PreviousTokenHash = id.TokenHash
	id.PreviousTokenValidUntil = previousValidUntil
	id.TokenHash = newHash
	id.CredentialGeneration = expectGeneration + 1
	s.identities[runnerID] = id
	return id.CredentialGeneration, nil
}

func (s *stubIssuedIdentityStore) counters() (lookups, issues, revokes, renews int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lookups, s.issues, s.revokes, s.renews
}

func cacheTestIdentity(runnerID, token string, namespaces ...string) IssuedIdentity {
	return IssuedIdentity{
		RunnerID:  runnerID,
		TokenHash: HashSecret(token),
		Scope:     RunnerPolicy{Name: runnerID, IDPrefix: runnerID[:1], AllowedNamespaces: namespaces},
		IssuedAt:  time.Unix(1700000000, 0).UTC(),
	}
}

// cacheTestAt returns a store whose clock the test drives explicitly, plus the
// clock itself, so TTL behaviour is asserted without sleeping.
func newTestCachedStore(t *testing.T, inner IssuedIdentityStore, ttl time.Duration) (*cachedIssuedIdentityStore, func(time.Time)) {
	t.Helper()
	wrapped := NewCachedIssuedIdentityStore(inner, ttl, nil)
	store, ok := wrapped.(*cachedIssuedIdentityStore)
	if !ok {
		t.Fatalf("NewCachedIssuedIdentityStore returned %T, want *cachedIssuedIdentityStore", wrapped)
	}
	now := time.Unix(1700000000, 0).UTC()
	store.now = func() time.Time { return now }
	return store, func(next time.Time) { now = next }
}

func TestCachedIssuedIdentityStoreServesRepeatedLookupsFromOneRead(t *testing.T) {
	ctx := context.Background()
	inner := newStubIssuedIdentityStore(cacheTestIdentity("runner-1", "tok", "team-a"))
	cached, _ := newTestCachedStore(t, inner, time.Minute)

	for i := 0; i < 5; i++ {
		id, found, err := cached.Lookup(ctx, "runner-1")
		if err != nil || !found {
			t.Fatalf("Lookup #%d = (%+v, %v, %v), want the issued row", i, id, found, err)
		}
		if id.RunnerID != "runner-1" || len(id.Scope.AllowedNamespaces) != 1 {
			t.Fatalf("Lookup #%d returned %+v, want the stored row intact", i, id)
		}
	}
	if lookups, _, _, _ := inner.counters(); lookups != 1 {
		t.Fatalf("inner Lookups = %d, want 1: the whole point of the cache is that a "+
			"repeated runner id costs one store read per TTL", lookups)
	}
}

func TestCachedIssuedIdentityStoreRevokeIsVisibleToTheNextLookup(t *testing.T) {
	ctx := context.Background()
	inner := newStubIssuedIdentityStore(cacheTestIdentity("runner-1", "tok", "team-a"))
	cached, _ := newTestCachedStore(t, inner, time.Minute)

	if _, found, err := cached.Lookup(ctx, "runner-1"); err != nil || !found {
		t.Fatalf("warm-up Lookup = (_, %v, %v), want found", found, err)
	}
	if err := cached.Revoke(ctx, "runner-1", OwnerScope{}); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if _, found, err := cached.Lookup(ctx, "runner-1"); err != nil || !found {
		t.Fatalf("post-revoke Lookup = (_, %v, %v), want found (the stub does not "+
			"enforce revocation; this asserts the row is re-read, not that it vanishes)", found, err)
	}
	if lookups, _, revokes, _ := inner.counters(); lookups != 2 || revokes != 1 {
		t.Fatalf("inner (lookups, revokes) = (%d, %d), want (2, 1): a revoke must evict, "+
			"or this process keeps serving the credential it just revoked", lookups, revokes)
	}
}

func TestCachedIssuedIdentityStoreRenewIsVisibleToTheNextLookup(t *testing.T) {
	ctx := context.Background()
	inner := newStubIssuedIdentityStore(cacheTestIdentity("runner-1", "tok", "team-a"))
	cached, _ := newTestCachedStore(t, inner, time.Minute)

	if _, _, err := cached.Lookup(ctx, "runner-1"); err != nil {
		t.Fatalf("warm-up Lookup: %v", err)
	}
	next := time.Unix(1800000000, 0).UTC()
	if err := cached.Renew(ctx, "runner-1", next); err != nil {
		t.Fatalf("Renew: %v", err)
	}
	if _, _, err := cached.Lookup(ctx, "runner-1"); err != nil {
		t.Fatalf("post-renew Lookup: %v", err)
	}
	if lookups, _, _, renews := inner.counters(); lookups != 2 || renews != 1 {
		t.Fatalf("inner (lookups, renews) = (%d, %d), want (2, 1): a cached row carries "+
			"the pre-renew ExpiresAt, so a successful renewal would be denied at the old "+
			"deadline without this eviction", lookups, renews)
	}
}

func TestCachedIssuedIdentityStoreIssueIsVisibleToTheNextLookup(t *testing.T) {
	ctx := context.Background()
	inner := newStubIssuedIdentityStore(cacheTestIdentity("runner-1", "tok", "team-a"))
	cached, _ := newTestCachedStore(t, inner, time.Minute)

	if _, _, err := cached.Lookup(ctx, "runner-1"); err != nil {
		t.Fatalf("warm-up Lookup: %v", err)
	}
	if err := cached.Issue(ctx, cacheTestIdentity("runner-1", "tok2", "team-b")); err != nil {
		t.Fatalf("Issue: %v", err)
	}
	// The stub's Issue overwrites in place, which is what a re-issued runner id
	// looks like; the cached row must not outlive it.
	if _, _, err := cached.Lookup(ctx, "runner-1"); err != nil {
		t.Fatalf("post-issue Lookup: %v", err)
	}
	if lookups, issues, _, _ := inner.counters(); lookups != 2 || issues != 1 {
		t.Fatalf("inner (lookups, issues) = (%d, %d), want (2, 1)", lookups, issues)
	}
}

func TestCachedIssuedIdentityStoreRotationIsVisibleToTheNextLookup(t *testing.T) {
	ctx := context.Background()
	inner := newStubIssuedIdentityStore(cacheTestIdentity("runner-1", "tok", "team-a"))
	cached, _ := newTestCachedStore(t, inner, time.Minute)

	if _, _, err := cached.Lookup(ctx, "runner-1"); err != nil {
		t.Fatalf("warm-up Lookup: %v", err)
	}
	if _, err := cached.RotateCredential(ctx, "runner-1", 1, HashSecret("tok2"), time.Unix(1800000000, 0).UTC()); err != nil {
		t.Fatalf("RotateCredential: %v", err)
	}
	id, found, err := cached.Lookup(ctx, "runner-1")
	if err != nil || !found {
		t.Fatalf("post-rotation Lookup = (_, %v, %v), want the rotated row", found, err)
	}
	if id.CredentialGeneration != 2 {
		t.Fatalf("generation = %d, want 2: without eviction the pre-rotation row keeps "+
			"being served, which both denies the runner that just rotated and keeps "+
			"accepting the token the store retired", id.CredentialGeneration)
	}
	if lookups, _, _, _ := inner.counters(); lookups != 2 {
		t.Fatalf("inner Lookups = %d, want 2", lookups)
	}
}

// TestCachedIssuedIdentityStoreDoesNotPublishARowAcrossAConcurrentMutation is the
// regression test for the race that makes naive read-through caches wrong: the
// lookup's store read completes AFTER a revocation has already evicted the key, so
// an unguarded insert would put the pre-revocation row back and the process would
// keep authenticating a credential it had just revoked.
func TestCachedIssuedIdentityStoreDoesNotPublishARowAcrossAConcurrentMutation(t *testing.T) {
	ctx := context.Background()
	inner := newStubIssuedIdentityStore(cacheTestIdentity("runner-1", "tok", "team-a"))

	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	inner.mu.Lock()
	inner.lookupGate = gate
	inner.lookupEntered = entered
	inner.mu.Unlock()

	cached, _ := newTestCachedStore(t, inner, time.Minute)

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, _, err := cached.Lookup(ctx, "runner-1"); err != nil {
			t.Errorf("concurrent Lookup: %v", err)
		}
	}()

	<-entered // the lookup is now inside the store read
	if err := cached.Revoke(ctx, "runner-1", OwnerScope{}); err != nil {
		t.Fatalf("Revoke during in-flight lookup: %v", err)
	}
	close(gate) // let the read complete, carrying the pre-revoke row
	<-done

	cached.mu.Lock()
	entries := len(cached.entries)
	cached.mu.Unlock()
	if entries != 0 {
		t.Fatalf("cache holds %d entr(ies) after a lookup raced a revoke, want 0: the "+
			"in-flight read published a row that predates the revocation", entries)
	}

	if _, _, err := cached.Lookup(ctx, "runner-1"); err != nil {
		t.Fatalf("post-race Lookup: %v", err)
	}
	if lookups, _, _, _ := inner.counters(); lookups != 2 {
		t.Fatalf("inner Lookups = %d, want 2: the second lookup had to reach the store "+
			"because the raced entry must not have been cached", lookups)
	}
}

func TestCachedIssuedIdentityStoreDoesNotCacheAbsence(t *testing.T) {
	ctx := context.Background()
	inner := newStubIssuedIdentityStore()
	cached, _ := newTestCachedStore(t, inner, time.Minute)

	for i := 0; i < 3; i++ {
		if _, found, err := cached.Lookup(ctx, "runner-unknown"); err != nil || found {
			t.Fatalf("Lookup #%d = (_, %v, %v), want (false, nil)", i, found, err)
		}
	}
	if lookups, _, _, _ := inner.counters(); lookups != 3 {
		t.Fatalf("inner Lookups = %d, want 3: absence must never be cached — a negative "+
			"entry keyed by a caller-supplied id is both unbounded and able to refuse a "+
			"freshly issued runner", lookups)
	}
	cached.mu.Lock()
	entries := len(cached.entries)
	cached.mu.Unlock()
	if entries != 0 {
		t.Fatalf("cache holds %d entr(ies) after three absent lookups, want 0", entries)
	}
}

func TestCachedIssuedIdentityStoreDoesNotCacheStoreErrors(t *testing.T) {
	ctx := context.Background()
	inner := newStubIssuedIdentityStore(cacheTestIdentity("runner-1", "tok"))
	inner.lookupErr = errors.New("store unavailable")
	cached, _ := newTestCachedStore(t, inner, time.Minute)

	for i := 0; i < 3; i++ {
		if _, found, err := cached.Lookup(ctx, "runner-1"); !errors.Is(err, inner.lookupErr) || found {
			t.Fatalf("Lookup #%d = (_, %v, %v), want the store error", i, found, err)
		}
	}
	if lookups, _, _, _ := inner.counters(); lookups != 3 {
		t.Fatalf("inner Lookups = %d, want 3: an outage must not be remembered as a "+
			"verdict — caching it would turn a blip into a lockout for the TTL", lookups)
	}

	inner.mu.Lock()
	inner.lookupErr = nil
	inner.mu.Unlock()
	if _, found, err := cached.Lookup(ctx, "runner-1"); err != nil || !found {
		t.Fatalf("post-recovery Lookup = (_, %v, %v), want the row", found, err)
	}
}

func TestCachedIssuedIdentityStoreExpiresEntries(t *testing.T) {
	ctx := context.Background()
	inner := newStubIssuedIdentityStore(cacheTestIdentity("runner-1", "tok"))
	cached, setNow := newTestCachedStore(t, inner, 30*time.Second)
	base := time.Unix(1700000000, 0).UTC()

	if _, _, err := cached.Lookup(ctx, "runner-1"); err != nil {
		t.Fatalf("warm-up Lookup: %v", err)
	}
	setNow(base.Add(29 * time.Second))
	if _, _, err := cached.Lookup(ctx, "runner-1"); err != nil {
		t.Fatalf("in-TTL Lookup: %v", err)
	}
	if lookups, _, _, _ := inner.counters(); lookups != 1 {
		t.Fatalf("inner Lookups = %d at TTL-1s, want 1", lookups)
	}

	setNow(base.Add(31 * time.Second))
	if _, _, err := cached.Lookup(ctx, "runner-1"); err != nil {
		t.Fatalf("post-TTL Lookup: %v", err)
	}
	if lookups, _, _, _ := inner.counters(); lookups != 2 {
		t.Fatalf("inner Lookups = %d after the TTL elapsed, want 2", lookups)
	}
}

// TestCachedIssuedIdentityStoreDoesNotSlideItsDeadline pins that hits do not extend
// the entry. A sliding deadline would let a steady request stream keep one row alive
// forever, which is exactly the unbounded staleness the TTL exists to prevent.
func TestCachedIssuedIdentityStoreDoesNotSlideItsDeadline(t *testing.T) {
	ctx := context.Background()
	inner := newStubIssuedIdentityStore(cacheTestIdentity("runner-1", "tok"))
	cached, setNow := newTestCachedStore(t, inner, 30*time.Second)
	base := time.Unix(1700000000, 0).UTC()

	if _, _, err := cached.Lookup(ctx, "runner-1"); err != nil {
		t.Fatalf("warm-up Lookup: %v", err)
	}
	// Hit every 20s: with a sliding deadline this would never expire.
	for i := 1; i <= 3; i++ {
		setNow(base.Add(time.Duration(i) * 20 * time.Second))
		if _, _, err := cached.Lookup(ctx, "runner-1"); err != nil {
			t.Fatalf("Lookup at +%ds: %v", i*20, err)
		}
	}
	if lookups, _, _, _ := inner.counters(); lookups != 2 {
		t.Fatalf("inner Lookups = %d after 60s of traffic on a 30s TTL, want 2: the "+
			"deadline is anchored at the read, not extended by hits", lookups)
	}
}

func TestCachedIssuedIdentityStoreClampsEntryDeadlineToTheRowsExpiry(t *testing.T) {
	ctx := context.Background()
	expiry := time.Unix(1700000010, 0).UTC() // 10s after the test clock's base
	id := cacheTestIdentity("runner-1", "tok")
	id.ExpiresAt = expiry
	inner := newStubIssuedIdentityStore(id)
	cached, setNow := newTestCachedStore(t, inner, time.Hour)

	if _, _, err := cached.Lookup(ctx, "runner-1"); err != nil {
		t.Fatalf("warm-up Lookup: %v", err)
	}
	setNow(expiry.Add(time.Second))
	if _, _, err := cached.Lookup(ctx, "runner-1"); err != nil {
		t.Fatalf("post-expiry Lookup: %v", err)
	}
	if lookups, _, _, _ := inner.counters(); lookups != 2 {
		t.Fatalf("inner Lookups = %d past the row's own expiry, want 2: a row must not be "+
			"served from cache after the store would call it dead", lookups)
	}
}

func TestCachedIssuedIdentityStoreTTLZeroDisablesCaching(t *testing.T) {
	inner := newStubIssuedIdentityStore(cacheTestIdentity("runner-1", "tok"))
	for _, ttl := range []time.Duration{0, -time.Second} {
		got := NewCachedIssuedIdentityStore(inner, ttl, nil)
		if got != IssuedIdentityStore(inner) {
			t.Fatalf("ttl=%v returned %T, want the inner store unchanged: disabling must "+
				"be a real config-only path (turn it off and restart)", ttl, got)
		}
	}
	if got := NewCachedIssuedIdentityStore(nil, time.Minute, nil); got != nil {
		t.Fatalf("nil inner returned %T, want nil", got)
	}
}

func TestCachedIssuedIdentityStoreClampsConfiguredTTL(t *testing.T) {
	inner := newStubIssuedIdentityStore()
	got := NewCachedIssuedIdentityStore(inner, time.Hour, nil)
	cached, ok := got.(*cachedIssuedIdentityStore)
	if !ok {
		t.Fatalf("returned %T, want *cachedIssuedIdentityStore", got)
	}
	if cached.ttl != MaxIssuedIdentityCacheTTL {
		t.Fatalf("ttl = %v, want the %v cap: the TTL is the only staleness bound for "+
			"processes that did not perform the mutation", cached.ttl, MaxIssuedIdentityCacheTTL)
	}
}

func TestCachedIssuedIdentityStoreHandsOutPrivateCopies(t *testing.T) {
	ctx := context.Background()
	inner := newStubIssuedIdentityStore(cacheTestIdentity("runner-1", "tok", "team-a"))
	cached, _ := newTestCachedStore(t, inner, time.Minute)

	first, _, err := cached.Lookup(ctx, "runner-1")
	if err != nil {
		t.Fatalf("warm-up Lookup: %v", err)
	}
	first.Scope.AllowedNamespaces[0] = "mutated"
	first.RunnerID = "mutated"

	second, _, err := cached.Lookup(ctx, "runner-1")
	if err != nil {
		t.Fatalf("second Lookup: %v", err)
	}
	if second.RunnerID != "runner-1" || second.Scope.AllowedNamespaces[0] != "team-a" {
		t.Fatalf("second Lookup = %+v, want the unmutated row: a caller must not be able "+
			"to corrupt what the next request authenticates against", second)
	}
}

func TestCachedIssuedIdentityStoreListIsNeverCached(t *testing.T) {
	ctx := context.Background()
	inner := newStubIssuedIdentityStore(cacheTestIdentity("runner-1", "tok"))
	cached, _ := newTestCachedStore(t, inner, time.Minute)

	for i := 0; i < 3; i++ {
		if _, err := cached.List(ctx); err != nil {
			t.Fatalf("List #%d: %v", i, err)
		}
	}
	inner.mu.Lock()
	lists := inner.lists
	inner.mu.Unlock()
	if lists != 3 {
		t.Fatalf("inner List calls = %d, want 3: the roster must never be served from "+
			"a cache that could report a revoked runner as present", lists)
	}
}

func TestCachedIssuedIdentityStoreSweepsExpiredEntries(t *testing.T) {
	ctx := context.Background()
	inner := newStubIssuedIdentityStore()
	for i := 0; i < issuedIdentityCacheSweepThreshold+10; i++ {
		id := cacheTestIdentity(runnerIDForIndex(i), "tok")
		inner.identities[id.RunnerID] = id
	}
	cached, setNow := newTestCachedStore(t, inner, 10*time.Second)
	base := time.Unix(1700000000, 0).UTC()

	for i := 0; i < issuedIdentityCacheSweepThreshold+10; i++ {
		if _, _, err := cached.Lookup(ctx, runnerIDForIndex(i)); err != nil {
			t.Fatalf("Lookup %d: %v", i, err)
		}
	}
	cached.mu.Lock()
	filled := len(cached.entries)
	cached.mu.Unlock()
	if filled < issuedIdentityCacheSweepThreshold {
		t.Fatalf("entries = %d, want at least %d before the sweep can trigger", filled, issuedIdentityCacheSweepThreshold)
	}

	// Move past both the entry TTL and the sweep cooldown, then touch one key so a
	// lookup runs the sweep.
	setNow(base.Add(2 * time.Minute))
	if _, _, err := cached.Lookup(ctx, runnerIDForIndex(0)); err != nil {
		t.Fatalf("sweeping Lookup: %v", err)
	}
	cached.mu.Lock()
	after := len(cached.entries)
	lastSweep := cached.lastSweep
	cached.mu.Unlock()
	if !lastSweep.Equal(base.Add(2 * time.Minute)) {
		t.Fatalf("lastSweep = %v, want the sweep to have run at the lookup's clock", lastSweep)
	}
	if after >= filled {
		t.Fatalf("entries = %d after sweeping past the TTL (was %d), want the expired "+
			"entries reclaimed: lazy expiry alone cannot bound the map", after, filled)
	}
}

func runnerIDForIndex(i int) string {
	return "runner-" + string(rune('a'+i%26)) + string(rune('a'+i/26%26)) + string(rune('a'+i/676))
}

func TestCachedIssuedIdentityStoreNotifiesObserver(t *testing.T) {
	ctx := context.Background()
	inner := newStubIssuedIdentityStore(cacheTestIdentity("runner-1", "tok"))
	obs := &recordingCacheObserver{}
	wrapped := NewCachedIssuedIdentityStore(inner, time.Minute, obs)
	cached := wrapped.(*cachedIssuedIdentityStore)
	now := time.Unix(1700000000, 0).UTC()
	cached.now = func() time.Time { return now }

	if _, _, err := cached.Lookup(ctx, "runner-1"); err != nil {
		t.Fatalf("first Lookup: %v", err)
	}
	if _, _, err := cached.Lookup(ctx, "runner-1"); err != nil {
		t.Fatalf("second Lookup: %v", err)
	}
	if got := obs.results; len(got) != 2 || got[0] != "miss" || got[1] != "hit" {
		t.Fatalf("observed results = %v, want [miss hit]", got)
	}
	if last := obs.counts[len(obs.counts)-1]; last != 1 {
		t.Fatalf("last observed entry count = %d, want 1", last)
	}

	if err := cached.Revoke(ctx, "runner-1", OwnerScope{}); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if last := obs.counts[len(obs.counts)-1]; last != 0 {
		t.Fatalf("entry count after revoke = %d, want 0: the gauge must be re-set on "+
			"deletes too, or it reads healthy while the cache is empty", last)
	}
}

type recordingCacheObserver struct {
	results []string
	counts  []int
}

func (o *recordingCacheObserver) OnIssuedIdentityCache(result string) { o.results = append(o.results, result) }
func (o *recordingCacheObserver) OnIssuedIdentityCacheEntries(count int) {
	o.counts = append(o.counts, count)
}
