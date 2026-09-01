package engine

import (
	"bytes"
	"strings"
	"testing"

	"github.com/google/uuid"
)

type countingReader struct {
	data  []byte
	reads int
}

func (r *countingReader) Read(p []byte) (int, error) {
	r.reads++
	return copy(p, r.data), nil
}

func TestNewLeaseCredentialsUsesOneReadAndProducesIndependentUUIDv4s(t *testing.T) {
	r := &countingReader{data: append(bytes.Repeat([]byte{0x11}, 16), bytes.Repeat([]byte{0x22}, 16)...)}
	leaseID, leaseToken := newLeaseCredentialsFromReader(r)
	if r.reads != 1 {
		t.Fatalf("CSPRNG reads = %d, want 1", r.reads)
	}

	id := parseCredentialUUID(t, string(leaseID), "lease-")
	token := parseCredentialUUID(t, string(leaseToken), "token-")
	for name, got := range map[string]uuid.UUID{"lease ID": id, "lease token": token} {
		if got.Version() != uuid.Version(4) {
			t.Errorf("%s version = %v, want 4", name, got.Version())
		}
		if got.Variant() != uuid.RFC4122 {
			t.Errorf("%s variant = %v, want RFC4122", name, got.Variant())
		}
	}
	if id == token {
		t.Fatal("lease ID and fencing token must be distinct")
	}
}

func TestNewLeaseCredentialsAreUnique(t *testing.T) {
	const n = 1000
	seen := make(map[string]struct{}, 2*n)
	for i := 0; i < n; i++ {
		id, token := newLeaseCredentials()
		for _, credential := range []string{string(id), string(token)} {
			if _, exists := seen[credential]; exists {
				t.Fatalf("duplicate credential after %d pairs: %s", i+1, credential)
			}
			seen[credential] = struct{}{}
		}
	}
}

func parseCredentialUUID(t *testing.T, credential, prefix string) uuid.UUID {
	t.Helper()
	if !strings.HasPrefix(credential, prefix) {
		t.Fatalf("credential %q has no %q prefix", credential, prefix)
	}
	id, err := uuid.Parse(strings.TrimPrefix(credential, prefix))
	if err != nil {
		t.Fatalf("parse credential %q: %v", credential, err)
	}
	return id
}

func BenchmarkLeaseCredentialGeneration(b *testing.B) {
	b.Run("independent-random-reads", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = LeaseID("lease-" + uuid.New().String())
			_ = LeaseToken("token-" + uuid.New().String())
		}
	})
	b.Run("paired-random-read", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = newLeaseCredentials()
		}
	})
}
