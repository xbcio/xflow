package control

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"sync"
	"time"
)

// registrationCodeBytes is the entropy of a registration code. Spec §2.3.4
// makes this the primary defense: at 32 bytes brute force is infeasible, so the
// rate limiter is depth behind it, not the only wall.
const registrationCodeBytes = 32

var (
	// These three are server-side distinctions ONLY. They are written to the
	// audit trail, and Core.Enroll collapses all of them into the single
	// external ErrEnrollRejected. Spec §2.3.4 item 4: a prober must not be able
	// to learn that a code exists but is revoked, or that it exists but does not
	// cover the requested namespace.
	ErrRegistrationCodeUnknown = errors.New("control: registration code not recognized")
	ErrRegistrationCodeRevoked = errors.New("control: registration code revoked")
	ErrRegistrationScopeDenied = errors.New("control: registration code does not cover requested scope")

	// ErrRegistrationCodeNotFound is a management-face error (revoking an id
	// that does not exist). It never reaches the enroll path.
	ErrRegistrationCodeNotFound = errors.New("control: registration code not found")
)

// HashSecret is the one-way transform applied to every credential this package
// persists — registration codes and issued runner tokens alike. Storing only the
// hash is what makes "no database read can recover a credential" true.
func HashSecret(s string) [32]byte { return sha256.Sum256([]byte(s)) }

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

// Policy projects the code's scope onto RunnerPolicy so the enroll path reuses
// RunnerPolicy.Allows / AllowsNamespace (auth.go:65,77) instead of
// reimplementing scope matching. Two implementations of "is this allowed" would
// drift, and the drift would be a privilege escalation.
func (c RegistrationCode) Policy() RunnerPolicy {
	return RunnerPolicy{
		Name:              "registration-code:" + c.ID,
		AllowedNodeTypes:  c.AllowedNodeTypes,
		AllowedNamespaces: c.AllowedNamespaces,
	}
}

// EnrollAuditRecord is one enrollment attempt. Failures are recorded too: an
// unaudited failure makes brute force invisible from the server side, which is
// exactly what §2.3.4 forbids.
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

// RegistrationCodeStore persists registration codes and their enroll attempts.
// Implementations: MemoryRegistrationCodeStore (dev/tests) and the sqlstore
// repo (Task 7). Both are held to the same contract test.
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

// GenerateRegistrationCode returns a new code id and its plaintext. The
// returned plaintext is the only copy that will ever exist outside the caller.
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
	s.codes = append(s.codes, code)
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
	return found, nil
}

func (s *MemoryRegistrationCodeStore) List(_ context.Context) ([]RegistrationCode, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]RegistrationCode, len(s.codes))
	copy(out, s.codes)
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
