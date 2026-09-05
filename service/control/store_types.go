package control

import "github.com/xbcio/xflow/store"

// The enroll data model lives in store so store/sqlstore can implement it
// without importing service/* — the architecture guard
// (test/architecture/production_dependencies_test.go) forbids that direction.
// These are aliases, not adapters: same type, so the compiler proves the
// migration is complete and a field added to store.RegistrationCode (or any
// of the other five) cannot be silently dropped on the way through. Every
// existing call site in this package, and every external package that already
// says control.RegistrationCode / control.RunnerPolicy / etc., compiles
// unchanged.
type (
	RegistrationCode      = store.RegistrationCode
	EnrollAuditRecord     = store.EnrollAuditRecord
	IssuedIdentity        = store.IssuedIdentity
	RegistrationCodeStore = store.RegistrationCodeStore
	IssuedIdentityStore   = store.IssuedIdentityStore
	RunnerPolicy          = store.RunnerPolicy
)

var (
	// These two are server-side distinctions ONLY. They are written to the
	// audit trail, and Core.Enroll collapses both of them into the single
	// external ErrEnrollRejected. Spec §2.3.4 item 4: a prober must not be
	// able to learn that a code exists but is revoked, or that a lookup
	// simply failed to find one.
	ErrRegistrationCodeUnknown = store.ErrRegistrationCodeUnknown
	ErrRegistrationCodeRevoked = store.ErrRegistrationCodeRevoked

	// ErrRegistrationCodeNotFound is a management-face error (revoking an id
	// that does not exist). It never reaches the enroll path.
	ErrRegistrationCodeNotFound = store.ErrRegistrationCodeNotFound

	// HashSecret is the one-way transform applied to every credential this
	// package's stores persist — registration codes and issued runner tokens
	// alike.
	HashSecret = store.HashSecret

	// GenerateRegistrationCode returns a new code id and its plaintext.
	GenerateRegistrationCode = store.GenerateRegistrationCode
)
