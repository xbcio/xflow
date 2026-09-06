package control

import (
	"context"
	"crypto/subtle"
	"errors"
	"sync"
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
}

var _ RegistrationCodeStore = (*MemoryRegistrationCodeStore)(nil)

func NewMemoryRegistrationCodeStore() *MemoryRegistrationCodeStore {
	return &MemoryRegistrationCodeStore{}
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
	if found.Revoked {
		return RegistrationCode{}, ErrRegistrationCodeRevoked
	}
	return found.Clone(), nil
}

func (s *MemoryRegistrationCodeStore) List(_ context.Context) ([]RegistrationCode, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]RegistrationCode, len(s.codes))
	for i, c := range s.codes {
		out[i] = c.Clone()
	}
	return out, nil
}

func (s *MemoryRegistrationCodeStore) Revoke(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.codes {
		if s.codes[i].ID == id {
			s.codes[i].Revoked = true
			return nil
		}
	}
	return ErrRegistrationCodeNotFound
}

func (s *MemoryRegistrationCodeStore) AppendEnrollAudit(_ context.Context, rec EnrollAuditRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.audit = append(s.audit, rec)
	return nil
}

func (s *MemoryRegistrationCodeStore) EnrollAudit(_ context.Context, codeID string) ([]EnrollAuditRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]EnrollAuditRecord, 0, len(s.audit))
	for _, rec := range s.audit {
		if rec.CodeID == codeID {
			out = append(out, rec)
		}
	}
	return out, nil
}
