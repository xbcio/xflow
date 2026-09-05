package control

import (
	"context"
	"crypto/subtle"
	"sync"
	"time"
)

// IssuedIdentity is a server-issued runner credential. It is the dynamic
// equivalent of a PolicyEntry (auth.go:137): same shape — id + token + scope —
// but minted by enroll instead of hand-written into runners.yaml.
//
// The static file path is untouched. The two coexist behind MultiAuthenticator.
type IssuedIdentity struct {
	RunnerID  string
	TokenHash [32]byte
	// Scope is the policy this identity grants, copied from the registration
	// code at issue time. Revoking the code later does NOT narrow an already
	// issued identity — the two are independently revocable by design, so an
	// operator rotating a leaked code does not knock every runner offline.
	Scope    RunnerPolicy
	CodeID   string
	IssuedAt time.Time
}

// clone returns a copy of id whose Scope's AllowedNodeTypes / AllowedNamespaces
// slices do not alias id's. IssuedIdentity crosses the store boundary by
// value, but the struct copy alone leaves RunnerPolicy's two slice fields
// pointing at the original backing arrays; without this, a caller mutating a
// returned identity's scope would mutate the store's internal state without
// holding its lock. Mirrors RegistrationCode.clone() (registration_code.go) —
// append([]string(nil), nil...) yields nil, so the nil-vs-empty distinction
// survives the clone.
func (id IssuedIdentity) clone() IssuedIdentity {
	id.Scope.AllowedNodeTypes = append([]string(nil), id.Scope.AllowedNodeTypes...)
	id.Scope.AllowedNamespaces = append([]string(nil), id.Scope.AllowedNamespaces...)
	return id
}

// IssuedIdentityStore persists identities minted by enroll.
type IssuedIdentityStore interface {
	Issue(ctx context.Context, id IssuedIdentity) error
	// Lookup returns the identity for runnerID. Absent → (zero, false, nil).
	Lookup(ctx context.Context, runnerID string) (IssuedIdentity, bool, error)
	List(ctx context.Context) ([]IssuedIdentity, error)
}

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
	s.byID[id.RunnerID] = id.clone()
	return nil
}

func (s *MemoryIssuedIdentityStore) Lookup(_ context.Context, runnerID string) (IssuedIdentity, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.byID[runnerID]
	if !ok {
		return IssuedIdentity{}, false, nil
	}
	return id.clone(), true, nil
}

func (s *MemoryIssuedIdentityStore) List(_ context.Context) ([]IssuedIdentity, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]IssuedIdentity, 0, len(s.byID))
	for _, id := range s.byID {
		out = append(out, id.clone())
	}
	return out, nil
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
	if err != nil || !ok {
		// A store error and an absent identity collapse to the same external
		// verdict. Distinguishing them would tell a caller whether a runner id
		// exists.
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
