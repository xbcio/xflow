package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"time"

	"github.com/xbcio/xflow/namespace"
)

// This file's contents used to live in service/control. They moved here (Task
// 7) because store/sqlstore must implement RegistrationCodeStore and
// IssuedIdentityStore, and test/architecture/production_dependencies_test.go
// forbids anything under store/ from importing service/* (or cmd/*) — no
// exceptions are allowlisted. service/control keeps its old exported names as
// type aliases (see store_types.go) so every existing call site — including
// Task 1/3/5/6's — compiles unchanged: an alias is the same type, so the
// compiler (not a human re-reading a diff) proves the migration dropped
// nothing.
//
// RunnerPolicy moved too, even though the plan that predates this move did
// not call for it: RegistrationCode.Policy() returns a RunnerPolicy and
// IssuedIdentity.Scope is one, so leaving RunnerPolicy in service/control
// would force store to import service/control right back — the exact
// direction this move exists to prevent.

// registrationCodeBytes is the entropy of a registration code. Spec §2.3.4
// makes this the primary defense: at 32 bytes brute force is infeasible, so
// the rate limiter is depth behind it, not the only wall.
const registrationCodeBytes = 32

var (
	// These three are server-side distinctions ONLY. They are written to the
	// audit trail, and Core.Enroll collapses all of them into the single
	// external control.ErrEnrollRejected. Spec §2.3.4 item 4: a prober must not
	// be able to learn that a code exists but is revoked, that it exists but has
	// expired, or that a lookup simply failed to find one.
	ErrRegistrationCodeUnknown = errors.New("store: registration code not recognized")
	ErrRegistrationCodeRevoked = errors.New("store: registration code revoked")
	ErrRegistrationCodeExpired = errors.New("store: registration code expired")

	// ErrRegistrationCodeNotFound is a management-face error (revoking an id
	// that does not exist). It never reaches the enroll path.
	ErrRegistrationCodeNotFound = errors.New("store: registration code not found")

	// ErrEnrollScopeCorrupted means a persisted scope column — a registration
	// code's AllowedNamespaces/AllowedNodeTypes, or an issued identity's
	// Scope.AllowedNamespaces/AllowedNodeTypes — failed to decode as JSON. It
	// must never be conflated with ErrRegistrationCodeUnknown or
	// ErrRegistrationCodeNotFound: those two mean "no such code", but this one
	// means the code (or identity) DOES exist and its data is damaged. On the
	// namespace axis those are not equivalent outcomes —
	// RunnerPolicy.AllowsNamespace treats an empty AllowedNamespaces as
	// "default namespace only", which is a WIDER grant than most non-empty
	// scopes ever set. A store that decoded a corrupted column to nil (the
	// way "not found" would suggest) would silently turn storage corruption
	// into a privilege escalation on the very next enroll. Implementations
	// must return this error rather than substitute a zero-value scope, and
	// callers must propagate it rather than swallow it.
	ErrEnrollScopeCorrupted = errors.New("store: enroll scope data corrupted")

	// ErrIssuedIdentityNotFound is returned when a lifecycle operation names a
	// runner with no issued identity. It is an operator-facing error on the
	// management path; it must never be surfaced on the runner-facing
	// authentication path, where every failure looks like ErrAuthUnknownToken.
	ErrIssuedIdentityNotFound = errors.New("store: issued identity not found")
)

// HashSecret is the one-way transform applied to every credential this
// package's stores persist — registration codes and issued runner tokens
// alike. Storing only the hash is what makes "no database read can recover a
// credential" true.
func HashSecret(s string) [32]byte { return sha256.Sum256([]byte(s)) }

// GenerateRegistrationCode returns a new code id and its plaintext. The
// returned plaintext is the only copy that will ever exist outside the
// caller.
func GenerateRegistrationCode() (id string, plaintext string, err error) {
	idRaw := make([]byte, 16)
	if _, err := rand.Read(idRaw); err != nil {
		return "", "", err
	}
	codeRaw := make([]byte, registrationCodeBytes)
	if _, err := rand.Read(codeRaw); err != nil {
		return "", "", err
	}
	return base64.RawURLEncoding.EncodeToString(idRaw),
		base64.RawURLEncoding.EncodeToString(codeRaw),
		nil
}

// ErrOwnerScopeUnset is returned when a store method receives a zero-value
// OwnerScope. The zero value is not "everything" and is not "the default
// namespace" — it is a caller bug, and it must fail closed. A bare string
// parameter with "" meaning "all rows" is the same fail-open shape as the
// privilege escalation this type exists to close.
var ErrOwnerScopeUnset = errors.New("store: owner scope not set")

// OwnerScope narrows a registration-code read or write to the rows one caller
// may see. Exactly one of All or Namespace must be set.
//
// All is reserved for platform operators holding a *_global scope. Namespace
// is the tenant case: it matches rows whose OwnerNamespace equals it, and
// nothing else — in particular it does NOT match the legacy rows whose
// OwnerNamespace is "", written before that field existed. See the
// OwnerNamespace field doc below: "" is not "platform-owned", it is "unknown",
// and only All: true can read it.
type OwnerScope struct {
	// All grants visibility over every row regardless of OwnerNamespace.
	All bool
	// Namespace is the single namespace whose rows are visible.
	Namespace string
}

// Validate reports whether exactly one of the two selectors is set.
func (s OwnerScope) Validate() error {
	if s.All == (s.Namespace != "") {
		// Both set, or neither set. Both are caller bugs.
		return ErrOwnerScopeUnset
	}
	return nil
}

// Matches reports whether a row owned by owner is visible under s. Call
// Validate first; Matches on an invalid scope reports false, which is the
// fail-closed direction but is not a substitute for the error.
func (s OwnerScope) Matches(owner string) bool {
	if s.All {
		return true
	}
	return s.Namespace != "" && s.Namespace == owner
}

// RegistrationCode is a reusable enrollment credential. The plaintext is
// returned exactly once, from the management create endpoint, and is never
// persisted anywhere.
type RegistrationCode struct {
	ID       string
	CodeHash [32]byte
	// AllowedNamespaces / AllowedNodeTypes are the ceiling on what a runner
	// enrolled with this code may ever claim. "*" means unrestricted, matching
	// RunnerPolicy's own convention.
	AllowedNamespaces []string
	AllowedNodeTypes  []string
	// OwnerNamespace is the namespace whose principal minted this code, and is
	// the only namespace that may list, revoke, or audit it. "" marks a row
	// written before this field existed — NOT "platform-owned": no code path
	// writes "" today (the authenticator normalizes an empty principal
	// namespace to namespace.Default before it ever reaches here), so such a
	// row is visible only under OwnerScope{All: true}, i.e. only to a
	// principal holding the corresponding *_global scope. That is
	// deliberately fail-closed — a legacy row silently becoming visible to
	// whichever tenant asked first is the bug this field exists to prevent.
	OwnerNamespace string
	Revoked        bool
	CreatedAt      time.Time
	// ExpiresAt is when this code stops being accepted by enroll. The zero
	// value means "never" and is what every code minted before the server grew
	// a registration-code TTL carries, so turning the feature on does not
	// retroactively invalidate codes already handed out.
	//
	// Expiry and Revoked are independent: revocation is an operator killing a
	// specific code, expiry is a deadline the code was born with. Neither
	// implies the other, and ResolveByPlaintext reports them as distinct
	// server-side reasons (both collapse to one external verdict).
	//
	// Domain types carry a value time.Time and read the zero value as "none";
	// the DB rows use *time.Time and NULL for the same state, converted at the
	// repo boundary — the same split IssuedIdentity.ExpiresAt documents, and
	// for the same reason (MySQL 8 strict mode rejects '0000-00-00').
	ExpiresAt time.Time
}

// IsExpired reports whether the code's deadline has passed as of now. A zero
// ExpiresAt is never expired.
//
// The predicate lives here, on the domain type, rather than in each store:
// two implementations of "is this code still usable" would drift, and the
// drift would be a privilege escalation — the same reasoning Policy()
// documents one type over.
//
// The boundary is "not After", so a code is expired at the exact instant it
// comes due rather than one tick later. That matches
// IssuedIdentityAuthenticator's identical check verbatim; the two lifetimes
// must not disagree about what "now" means at the boundary.
func (c RegistrationCode) IsExpired(now time.Time) bool {
	return !c.ExpiresAt.IsZero() && !c.ExpiresAt.After(now)
}

// Clone returns a copy of c whose AllowedNamespaces / AllowedNodeTypes slices
// do not alias c's. RegistrationCode is handed across the store boundary by
// value, but the struct copy alone leaves the two slice fields pointing at
// the original backing arrays; without this, a caller mutating a returned
// entry's slice would mutate the store's internal state without holding its
// lock. append([]string(nil), nil...) yields nil, so the nil-vs-empty
// distinction survives the clone.
//
// Exported (unlike its service/control predecessor's unexported clone())
// because service/control's in-memory store implementations, which stayed
// behind in service/control when this type moved here, still need to call it
// from outside this package.
func (c RegistrationCode) Clone() RegistrationCode {
	c.AllowedNamespaces = append([]string(nil), c.AllowedNamespaces...)
	c.AllowedNodeTypes = append([]string(nil), c.AllowedNodeTypes...)
	return c
}

// Policy projects the code's scope onto RunnerPolicy so the enroll path
// reuses RunnerPolicy.Allows / AllowsNamespace instead of reimplementing scope
// matching. Two implementations of "is this allowed" would drift, and the
// drift would be a privilege escalation.
func (c RegistrationCode) Policy() RunnerPolicy {
	return RunnerPolicy{
		Name:              "registration-code:" + c.ID,
		AllowedNodeTypes:  c.AllowedNodeTypes,
		AllowedNamespaces: c.AllowedNamespaces,
	}
}

// EnrollAuditRecord is one enrollment attempt. Failures are recorded too: an
// unaudited failure makes brute force invisible from the server side, which
// is exactly what §2.3.4 forbids.
type EnrollAuditRecord struct {
	CodeID  string
	Success bool
	// Reason is the server-side rejection detail. It is written here and never
	// returned to the caller.
	Reason   string
	RunnerID string
	SourceIP string
	At       time.Time
}

// RegistrationCodeStore persists registration codes and their enroll
// attempts. Implementations: control.MemoryRegistrationCodeStore (dev/tests)
// and store/sqlstore's registrationCodeRepo (Task 7). Both are held to the
// same contract test (store/storecontract.RunRegistrationCodeStoreContract).
type RegistrationCodeStore interface {
	Create(ctx context.Context, code RegistrationCode) error
	// ResolveByPlaintext returns the non-revoked, unexpired code whose hash
	// matches plaintext. Comparison must be constant-time against every stored
	// code.
	//
	// Expiry is enforced HERE rather than in Core.Enroll so that an expired
	// code never escapes the storage boundary at all: a future caller that
	// forgets to re-check would otherwise fail open. That is the same placement
	// Revoked already has, and the two must stay together — a reader who finds
	// one check here will not go looking for the other elsewhere.
	ResolveByPlaintext(ctx context.Context, plaintext string) (RegistrationCode, error)
	// List returns the codes visible under scope. An invalid scope returns
	// ErrOwnerScopeUnset and no rows.
	List(ctx context.Context, scope OwnerScope) ([]RegistrationCode, error)
	// Revoke marks the code revoked. A code that exists but is outside scope
	// returns ErrRegistrationCodeNotFound — byte-identical to a code that does
	// not exist, so the endpoint cannot be used to probe for other tenants'
	// code ids.
	Revoke(ctx context.Context, id string, scope OwnerScope) error
	AppendEnrollAudit(ctx context.Context, rec EnrollAuditRecord) error
	// EnrollAudit returns the attempts against codeID, or
	// ErrRegistrationCodeNotFound when codeID is outside scope.
	EnrollAudit(ctx context.Context, codeID string, scope OwnerScope) ([]EnrollAuditRecord, error)
}

// IssuedIdentity is a server-issued runner credential. It is the dynamic
// equivalent of control.PolicyEntry: same shape — id + token + scope — but
// minted by enroll instead of hand-written into runners.yaml.
//
// The static file path is untouched. The two coexist behind
// control.MultiAuthenticator.
type IssuedIdentity struct {
	RunnerID  string
	TokenHash [32]byte
	// Scope is the policy this identity grants, copied from the registration
	// code at issue time. Revoking the code later does NOT narrow an already
	// issued identity — the two are independently revocable by design, so an
	// operator rotating a leaked code does not knock every runner offline.
	Scope  RunnerPolicy
	CodeID string
	// OwnerNamespace is the namespace that owned the registration code this
	// identity was minted from, snapshotted at issue time. It is the only
	// namespace that may revoke this identity.
	//
	// It is a snapshot, not a join through CodeID, for the same reason Scope
	// above is: the identity is independent of the code once issued, so
	// deleting the code must not erase who owns the runner.
	//
	// "" carries the same meaning as RegistrationCode.OwnerNamespace's "" —
	// "unknown", NOT "platform-owned". It marks a row issued before this field
	// existed, and only OwnerScope{All: true} matches it. A legacy runner
	// silently becoming revocable by whichever tenant asked first is the exact
	// cross-tenant hole this field closes.
	OwnerNamespace string
	IssuedAt       time.Time
	// ExpiresAt is when this identity stops authenticating. The zero value
	// means "never" and is what every identity issued before the server grew a
	// TTL carries, so turning the feature on does not retroactively lock out a
	// running fleet.
	//
	// Domain types carry a value time.Time and read the zero value as "none";
	// the DB rows use *time.Time and NULL for the same state. The two are
	// converted at the repo boundary, because MySQL 8 strict mode rejects
	// '0000-00-00' outright (the same reason dbIssuedIdentity.IssuedAt is
	// already a pointer).
	ExpiresAt time.Time
	// RevokedAt is when an operator killed this identity. Zero means live.
	// Revocation is separate from the registration code's own Revoked flag:
	// revoking a code does not narrow identities already issued from it, by
	// design, so revoking one runner needs its own switch.
	RevokedAt time.Time
}

// Clone returns a copy of id whose Scope's AllowedNodeTypes / AllowedNamespaces
// slices do not alias id's. IssuedIdentity crosses the store boundary by
// value, but the struct copy alone leaves RunnerPolicy's two slice fields
// pointing at the original backing arrays; without this, a caller mutating a
// returned identity's scope would mutate the store's internal state without
// holding its lock. Mirrors RegistrationCode.Clone() — append([]string(nil),
// nil...) yields nil, so the nil-vs-empty distinction survives the clone.
//
// Exported for the same reason as RegistrationCode.Clone(): service/control's
// MemoryIssuedIdentityStore calls it from outside this package.
func (id IssuedIdentity) Clone() IssuedIdentity {
	id.Scope.AllowedNodeTypes = append([]string(nil), id.Scope.AllowedNodeTypes...)
	id.Scope.AllowedNamespaces = append([]string(nil), id.Scope.AllowedNamespaces...)
	return id
}

// IssuedIdentityStore persists identities minted by enroll.
type IssuedIdentityStore interface {
	Issue(ctx context.Context, id IssuedIdentity) error
	// Lookup returns the identity for runnerID. Absent → (zero, false, nil).
	// A storage or decode failure → (zero, false, err); callers must not read
	// that as "absent" — control.IssuedIdentityAuthenticator relies on the
	// distinction to tell an outage apart from a wrong token in its logs.
	Lookup(ctx context.Context, runnerID string) (IssuedIdentity, bool, error)
	List(ctx context.Context) ([]IssuedIdentity, error)
	// Revoke stamps RevokedAt on the identity owned by scope. Revoking an
	// already-revoked identity is a no-op that returns nil: revocation is a
	// state, not an event, and an operator retrying after a timeout must not
	// see a spurious failure.
	//
	// An identity outside scope is reported as ErrIssuedIdentityNotFound, the
	// same verdict as one that does not exist. Distinguishing the two would
	// turn this endpoint into an existence oracle for other tenants' runner
	// ids, which mirrors registrationCodeRepo.Revoke's own choice.
	Revoke(ctx context.Context, runnerID string, scope OwnerScope) error
	// Renew extends ExpiresAt. It does NOT touch the token, the runner id, or
	// the scope — see the design's R9. Renewing an identity that is already
	// expired or revoked returns ErrIssuedIdentityNotFound: an expired identity
	// that can renew itself makes expiry theater.
	Renew(ctx context.Context, runnerID string, expiresAt time.Time) error
}

// RunnerPolicy is the effective set of permissions bound to an authenticated
// runner. Cached on the runner state so the dispatcher can filter node types
// without touching the policy store on every Assign.
type RunnerPolicy struct {
	// Name identifies the matched policy entry for logging. Not
	// security-relevant — the token match is what proves identity.
	Name string
	// IDPrefix is the required prefix of the runner's self-declared ID.
	IDPrefix string
	// AllowedNodeTypes is the set of node types this runner may execute.
	// A single "*" entry means all node types.
	AllowedNodeTypes []string
	// AllowedNamespaces is the set of namespaces this runner may join. A single
	// "*" entry means all namespaces. An empty set means the default namespace
	// only — the same meaning canServeNamespace already gives an empty set, so
	// one "empty" cannot mean "everything" in the policy layer and "default
	// only" in the filter layer.
	AllowedNamespaces []string
}

// Allows reports whether the policy permits the given node type. Called from
// the dispatcher's Assign hot path — kept O(N) with N ~= handful of types.
func (p RunnerPolicy) Allows(nodeType string) bool {
	for _, t := range p.AllowedNodeTypes {
		if t == "*" || t == nodeType {
			return true
		}
	}
	return false
}

// AllowsNamespace reports whether the policy permits joining ns. An empty
// AllowedNamespaces means the default namespace only, matching
// canServeNamespace's treatment of an empty namespace set.
func (p RunnerPolicy) AllowsNamespace(ns namespace.Namespace) bool {
	if ns == "" {
		ns = namespace.Default
	}
	if len(p.AllowedNamespaces) == 0 {
		return ns == namespace.Default
	}
	for _, a := range p.AllowedNamespaces {
		if a == "*" || namespace.Namespace(a) == ns {
			return true
		}
	}
	return false
}
