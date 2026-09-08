package control

import (
	"context"
	"crypto/subtle"
	"fmt"
	"sync"
	"time"
)

// MemoryIssuedIdentityStore is the in-process implementation.
type MemoryIssuedIdentityStore struct {
	mu   sync.RWMutex
	byID map[string]IssuedIdentity
}

var _ IssuedIdentityStore = (*MemoryIssuedIdentityStore)(nil)

func NewMemoryIssuedIdentityStore() *MemoryIssuedIdentityStore {
	return &MemoryIssuedIdentityStore{byID: make(map[string]IssuedIdentity)}
}

func (s *MemoryIssuedIdentityStore) Issue(_ context.Context, id IssuedIdentity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Clone on the way in too: otherwise the caller retains a reference to the
	// same backing arrays now held by the store and could mutate stored state
	// without the lock.
	s.byID[id.RunnerID] = id.Clone()
	return nil
}

func (s *MemoryIssuedIdentityStore) Lookup(_ context.Context, runnerID string) (IssuedIdentity, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.byID[runnerID]
	if !ok {
		return IssuedIdentity{}, false, nil
	}
	return id.Clone(), true, nil
}

func (s *MemoryIssuedIdentityStore) List(_ context.Context) ([]IssuedIdentity, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]IssuedIdentity, 0, len(s.byID))
	for _, id := range s.byID {
		out = append(out, id.Clone())
	}
	return out, nil
}

// Revoke stamps RevokedAt. Revoking an already-revoked identity is a no-op
// that returns nil: revocation is a state, not an event, and an operator
// retrying after a timeout must not see a spurious failure.
func (s *MemoryIssuedIdentityStore) Revoke(_ context.Context, runnerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.byID[runnerID]
	if !ok {
		return ErrIssuedIdentityNotFound
	}
	if !id.RevokedAt.IsZero() {
		return nil
	}
	id.RevokedAt = time.Now().UTC()
	s.byID[runnerID] = id
	return nil
}

// Renew extends ExpiresAt. It does NOT touch the token, the runner id, or the
// scope. Renewing an identity that is already expired or revoked returns
// ErrIssuedIdentityNotFound: an expired identity that can renew itself makes
// expiry theater.
func (s *MemoryIssuedIdentityStore) Renew(_ context.Context, runnerID string, expiresAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.byID[runnerID]
	if !ok {
		return ErrIssuedIdentityNotFound
	}
	if !id.RevokedAt.IsZero() {
		return ErrIssuedIdentityNotFound
	}
	if !id.ExpiresAt.IsZero() && !id.ExpiresAt.After(time.Now().UTC()) {
		return ErrIssuedIdentityNotFound
	}
	id.ExpiresAt = expiresAt
	s.byID[runnerID] = id
	return nil
}

// IssuedIdentityAuthenticator authenticates runners holding an enroll-issued
// token.
//
// The Authenticator interface carries no context.Context (auth.go:95-98), so the
// store is queried with context.Background(). That is acceptable because every
// implementation is either an in-memory map read or a single primary-key row
// read; widening the interface would touch every existing authenticator and
// every call site for no behavioral gain.
type IssuedIdentityAuthenticator struct{ store IssuedIdentityStore }

var _ Authenticator = (*IssuedIdentityAuthenticator)(nil)

func NewIssuedIdentityAuthenticator(store IssuedIdentityStore) *IssuedIdentityAuthenticator {
	return &IssuedIdentityAuthenticator{store: store}
}

func (a *IssuedIdentityAuthenticator) AuthenticateRegister(runnerID, token string, _ TransportInfo) (RunnerPolicy, error) {
	return a.authenticate(runnerID, token)
}

func (a *IssuedIdentityAuthenticator) AuthenticateOngoing(runnerID, token string, _ TransportInfo) (RunnerPolicy, error) {
	return a.authenticate(runnerID, token)
}

func (a *IssuedIdentityAuthenticator) authenticate(runnerID, token string) (RunnerPolicy, error) {
	if token == "" {
		return RunnerPolicy{}, ErrAuthMissingToken
	}
	if a == nil || a.store == nil {
		return RunnerPolicy{}, ErrAuthUnknownToken
	}
	id, ok, err := a.store.Lookup(context.Background(), runnerID)
	if err != nil {
		// A store error and an absent identity collapse to the same EXTERNAL
		// verdict — distinguishing them would tell a caller whether a runner id
		// exists. They must not collapse in our OWN logs: Core.authDeny logs
		// this error verbatim, so without the wrap an outage of the identity
		// store makes every runner's heartbeat log "unknown auth token", which
		// points an operator at credentials when the fault is the database.
		//
		// The wrap keeps ErrAuthUnknownToken as the only errors.Is-matchable
		// identity (MultiAuthenticator.dispatch and every caller still see it),
		// and authDeny returns the constant ErrUnauthenticated regardless of
		// which error it logged, so nothing about the response changes. The
		// store error is interpolated with %v, not %w: nothing consumes it
		// programmatically, and widening the error tree would invite callers to
		// couple to store internals through an auth error.
		return RunnerPolicy{}, fmt.Errorf("%w: issued-identity lookup failed: %v", ErrAuthUnknownToken, err)
	}
	if !ok {
		return RunnerPolicy{}, ErrAuthUnknownToken
	}
	want := HashSecret(token)
	if subtle.ConstantTimeCompare(want[:], id.TokenHash[:]) != 1 {
		return RunnerPolicy{}, ErrAuthUnknownToken
	}
	return id.Scope, nil
}

// MultiAuthenticator tries each member in order and returns the first success.
// It is how the static runners.yaml path (FilePolicyStore) and the enroll-issued
// path coexist without either knowing the other exists.
type MultiAuthenticator struct{ auths []Authenticator }

var _ Authenticator = (*MultiAuthenticator)(nil)

func NewMultiAuthenticator(auths ...Authenticator) *MultiAuthenticator {
	kept := make([]Authenticator, 0, len(auths))
	for _, a := range auths {
		if a != nil {
			kept = append(kept, a)
		}
	}
	return &MultiAuthenticator{auths: kept}
}

// Members exposes the composed authenticators for callers outside this
// package (e.g. future wiring or introspection). It returns a copy, not
// m.auths itself: m.auths is read by dispatch on every in-flight runner
// request, and handing out the live slice would let a caller mutate
// (`members[0] = x`) this MultiAuthenticator's own dispatch order/membership
// out from under those readers — the same shared-backing-array defect this
// task closed for IssuedIdentity.Scope, one level up.
//
// IsConfigured (auth.go), being in the same package, ranges over m.auths
// directly instead of calling this — it runs on the artifact module's
// per-request path (module_artifact.go), and that path should not pay for an
// allocation just to work around an exported-API hazard it doesn't have.
func (m *MultiAuthenticator) Members() []Authenticator {
	if m == nil {
		return nil
	}
	out := make([]Authenticator, len(m.auths))
	copy(out, m.auths)
	return out
}

func (m *MultiAuthenticator) AuthenticateRegister(runnerID, token string, info TransportInfo) (RunnerPolicy, error) {
	return m.dispatch(func(a Authenticator) (RunnerPolicy, error) {
		return a.AuthenticateRegister(runnerID, token, info)
	})
}

func (m *MultiAuthenticator) AuthenticateOngoing(runnerID, token string, info TransportInfo) (RunnerPolicy, error) {
	return m.dispatch(func(a Authenticator) (RunnerPolicy, error) {
		return a.AuthenticateOngoing(runnerID, token, info)
	})
}

func (m *MultiAuthenticator) dispatch(call func(Authenticator) (RunnerPolicy, error)) (RunnerPolicy, error) {
	if m == nil || len(m.auths) == 0 {
		return RunnerPolicy{}, ErrAuthUnknownToken
	}
	var (
		lastPolicy RunnerPolicy
		lastErr    = ErrAuthUnknownToken

		dryRunPolicy RunnerPolicy
		dryRunErr    error
	)
	for _, a := range m.auths {
		policy, err := call(a)
		if err == nil {
			return policy, nil
		}
		// A dry-run denial is not really a denial: FilePolicyStore.deny returns
		// (permissivePolicy, dryRunError) so the caller allows the request and
		// only logs. Remember the first one and prefer it over a hard denial
		// from a later member, otherwise composing a second authenticator behind
		// a dry-run file store would silently flip dry-run into enforcing.
		if dryRunErr == nil && IsDryRunDenial(err) {
			dryRunPolicy, dryRunErr = policy, err
		}
		lastPolicy, lastErr = policy, err
	}
	if dryRunErr != nil {
		return dryRunPolicy, dryRunErr
	}
	_ = lastPolicy
	return RunnerPolicy{}, lastErr
}
