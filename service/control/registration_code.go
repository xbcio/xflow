package control

import (
	"context"
	"crypto/subtle"
	"errors"
	"sync"
	"time"
)

// ErrRegistrationScopeDenied would be returned when a registration code does
// not cover a requested scope. It is currently unused: enrollScopeReason
// (enroll.go) returns a string reason rather than this error, and no other
// caller constructs it. Left here rather than moved to store/enroll.go
// because the addendum's move list does not name it, and store/enroll.go's
// sentinels are only the ones actually returned by store.RegistrationCodeStore
// implementations.
var ErrRegistrationScopeDenied = errors.New("control: registration code does not cover requested scope")

// MemoryRegistrationCodeStore is the in-process implementation used by dev
// servers and tests.
type MemoryRegistrationCodeStore struct {
	mu    sync.RWMutex
	codes []RegistrationCode
	audit []EnrollAuditRecord
	// now is injected so code expiry can be tested at the exact boundary
	// without sleeping. nil means time.Now, which is the shape
	// IssuedIdentityAuthenticator already uses; there is no repo-wide Clock
	// interface and inventing one for these two call sites would be a bigger
	// change than the feature.
	now func() time.Time
}

var _ RegistrationCodeStore = (*MemoryRegistrationCodeStore)(nil)

func NewMemoryRegistrationCodeStore() *MemoryRegistrationCodeStore {
	return &MemoryRegistrationCodeStore{}
}

func (s *MemoryRegistrationCodeStore) clock() time.Time {
	if s == nil || s.now == nil {
		return time.Now().UTC()
	}
	return s.now().UTC()
}

func (s *MemoryRegistrationCodeStore) Create(_ context.Context, code RegistrationCode) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Clone on the way in too: otherwise the caller retains a reference to the
	// same backing arrays now held by the store and could mutate stored state
	// without the lock.
	s.codes = append(s.codes, code.Clone())
	return nil
}

func (s *MemoryRegistrationCodeStore) ResolveByPlaintext(_ context.Context, plaintext string) (RegistrationCode, error) {
	want := HashSecret(plaintext)
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Scan every entry without breaking early. An early return on match leaks
	// the matching entry's position through timing; ConstantTimeCompare makes
	// each individual comparison timing-independent, and the full scan makes the
	// loop as a whole timing-independent.
	var found RegistrationCode
	matched := false
	for _, c := range s.codes {
		if subtle.ConstantTimeCompare(want[:], c.CodeHash[:]) == 1 {
			found, matched = c, true
		}
	}
	if !matched {
		return RegistrationCode{}, ErrRegistrationCodeUnknown
	}
	// Both lifecycle checks run AFTER the full constant-time scan, never
	// inside it: short-circuiting on a revoked or expired entry would restore
	// exactly the position-dependent timing the scan exists to erase.
	if found.Revoked {
		return RegistrationCode{}, ErrRegistrationCodeRevoked
	}
	if found.IsExpired(s.clock()) {
		return RegistrationCode{}, ErrRegistrationCodeExpired
	}
	return found.Clone(), nil
}

// Consume claims one use of the code. It takes the write lock, not the read
// lock ResolveByPlaintext uses: this is the one place on the enroll path that
// mutates stored state, and holding the write lock across the check and the
// increment is what makes two concurrent enrolls racing for the last use
// unable to both win. The SQL implementation gets the same guarantee from a
// single conditional UPDATE.
//
// Lookup is by ID, so there is no constant-time concern here — the caller
// already proved possession of the plaintext through ResolveByPlaintext, and
// the id it returned is not attacker-supplied.
func (s *MemoryRegistrationCodeStore) Consume(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.codes {
		if s.codes[i].ID != id {
			continue
		}
		// Re-checked here rather than trusted from the earlier resolve: the
		// scope check runs in between, and a code revoked in that window must
		// not still enroll a runner.
		if s.codes[i].Revoked {
			return ErrRegistrationCodeRevoked
		}
		if s.codes[i].IsExhausted() {
			return ErrRegistrationCodeExhausted
		}
		s.codes[i].UseCount++
		return nil
	}
	return ErrRegistrationCodeUnknown
}

func (s *MemoryRegistrationCodeStore) List(_ context.Context, scope OwnerScope) ([]RegistrationCode, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]RegistrationCode, 0, len(s.codes))
	for _, c := range s.codes {
		if !scope.Matches(c.OwnerNamespace) {
			continue
		}
		out = append(out, c.Clone())
	}
	return out, nil
}

func (s *MemoryRegistrationCodeStore) Revoke(_ context.Context, id string, scope OwnerScope) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.codes {
		if s.codes[i].ID != id {
			continue
		}
		// Out of scope reports not-found rather than forbidden: a distinct
		// error would turn this endpoint into an existence oracle for other
		// tenants' code ids.
		if !scope.Matches(s.codes[i].OwnerNamespace) {
			return ErrRegistrationCodeNotFound
		}
		s.codes[i].Revoked = true
		return nil
	}
	return ErrRegistrationCodeNotFound
}

func (s *MemoryRegistrationCodeStore) AppendEnrollAudit(_ context.Context, rec EnrollAuditRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.audit = append(s.audit, rec)
	return nil
}

func (s *MemoryRegistrationCodeStore) EnrollAudit(_ context.Context, codeID string, scope OwnerScope) ([]EnrollAuditRecord, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	// The audit rows carry no namespace of their own; the code they belong to
	// is the authority. A codeID outside scope — or one with no code row at
	// all — is not-found, not an empty list: an empty list would tell the
	// caller "this id exists and has no attempts".
	owned := false
	for _, c := range s.codes {
		if c.ID == codeID {
			owned = scope.Matches(c.OwnerNamespace)
			break
		}
	}
	if !owned {
		return nil, ErrRegistrationCodeNotFound
	}
	out := make([]EnrollAuditRecord, 0, len(s.audit))
	for _, rec := range s.audit {
		if rec.CodeID == codeID {
			out = append(out, rec)
		}
	}
	return out, nil
}
