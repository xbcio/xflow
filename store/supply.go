package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"
)

// ErrRevisionConflict is returned by PutSupply when the caller's expected
// revision does not match the stored one (optimistic concurrency / If-Match).
var ErrRevisionConflict = errors.New("store: revision conflict")

// SupplyResource is a namespace-scoped, named, versioned byte snapshot produced
// by a supply node. The platform treats Content as opaque bytes: it is NOT a
// secret (credentials keep going through the credential system) and it is NOT
// schema-validated here — validation belongs to the consumer (for wasm, a
// negative return from the guest's configure).
//
// Two version numbers with distinct jobs, mirroring Kubernetes resourceVersion
// vs observedGeneration:
//   - Revision is monotonic and bumped on EVERY write, even when the content is
//     byte-identical. It is the write-side CAS token (If-Match).
//   - ContentHash is the sha256 of Content. It is the read-side comparison a
//     consumer uses to decide whether to rebuild anything: identical content
//     means no rebuild, even across a Revision bump.
type SupplyResource struct {
	Namespace string
	Name      string

	Content     []byte
	ContentType string
	Revision    uint64
	ContentHash string
	UpdatedAt   time.Time
	UpdatedBy   string
	// LastFetchAt and LastError describe the most recent collection attempt in
	// pull mode. They are observability fields only; nothing schedules off them.
	LastFetchAt time.Time
	LastError   string
}

// Supplies persists SupplyResource snapshots.
type Supplies interface {
	// GetSupply reads one snapshot. Returns ErrNotFound when absent.
	GetSupply(ctx context.Context, namespace, name string) (*SupplyResource, error)
	// PutSupply writes a snapshot and returns the stored result.
	//
	// ifMatch is the optimistic-concurrency token:
	//   - nil          → unconditional write
	//   - *ifMatch == 0 → create only if absent
	//   - *ifMatch == N → write only if the current Revision is exactly N
	//
	// A mismatch returns ErrRevisionConflict and leaves the stored row
	// untouched. On success Revision is bumped by one and ContentHash is
	// recomputed from rec.Content; the caller's rec is not mutated.
	PutSupply(ctx context.Context, rec *SupplyResource, ifMatch *uint64) (*SupplyResource, error)
}

// ContentHash returns the read-side comparison token for supply content.
func ContentHash(content []byte) string {
	sum := sha256.Sum256(content)
	return "sha256:" + hex.EncodeToString(sum[:])
}
