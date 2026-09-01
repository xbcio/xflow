package engine

import (
	"crypto/rand"
	"io"

	"github.com/google/uuid"
)

// newLeaseCredentials mints the independent identifiers used to address and
// fence one lease. Reading both UUIDs in one CSPRNG call halves the syscall
// overhead without pooling secret material on the Go heap or reducing either
// credential's 122 bits of UUIDv4 entropy.
func newLeaseCredentials() (LeaseID, LeaseToken) {
	return newLeaseCredentialsFromReader(rand.Reader)
}

func newLeaseCredentialsFromReader(r io.Reader) (LeaseID, LeaseToken) {
	var random [2 * len(uuid.UUID{})]byte
	if _, err := io.ReadFull(r, random[:]); err != nil {
		// Match uuid.New's fail-closed behavior: failure of the operating-system
		// CSPRNG is unrecoverable and must never produce a predictable token.
		panic(err)
	}

	var id, token uuid.UUID
	copy(id[:], random[:len(id)])
	copy(token[:], random[len(id):])
	setUUIDv4(&id)
	setUUIDv4(&token)
	return LeaseID("lease-" + id.String()), LeaseToken("token-" + token.String())
}

func setUUIDv4(id *uuid.UUID) {
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
}
