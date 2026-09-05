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
	// These two are server-side distinctions ONLY. They are written to the
	// audit trail, and Core.Enroll collapses both of them into the single
	// external control.ErrEnrollRejected. Spec §2.3.4 item 4: a prober must not
	// be able to learn that a code exists but is revoked, or that a lookup
	// simply failed to find one.
	ErrRegistrationCodeUnknown = errors.New("store: registration code not recognized")
	ErrRegistrationCodeRevoked = errors.New("store: registration code revoked")

	// ErrRegistrationCodeNotFound is a management-face error (revoking an id
	// that does not exist). It never reaches the enroll path.
	ErrRegistrationCodeNotFound = errors.New("store: registration code not found")
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
	Revoked           bool
	CreatedAt         time.Time
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
	// ResolveByPlaintext returns the non-revoked code whose hash matches
	// plaintext. Comparison must be constant-time against every stored code.
	ResolveByPlaintext(ctx context.Context, plaintext string) (RegistrationCode, error)
	List(ctx context.Context) ([]RegistrationCode, error)
	Revoke(ctx context.Context, id string) error
	AppendEnrollAudit(ctx context.Context, rec EnrollAuditRecord) error
	EnrollAudit(ctx context.Context, codeID string) ([]EnrollAuditRecord, error)
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
	Scope    RunnerPolicy
	CodeID   string
	IssuedAt time.Time
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
	Lookup(ctx context.Context, runnerID string) (IssuedIdentity, bool, error)
	List(ctx context.Context) ([]IssuedIdentity, error)
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
