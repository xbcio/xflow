package blob

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/xbcio/xflow/namespace"
)

// memKey identifies one blob inside a Memory store.
type memKey struct {
	ns     namespace.Namespace
	digest string
}

// Memory is an in-process Store for tests and local use. It keeps every blob in
// RAM, never compresses, and is safe for concurrent use.
type Memory struct {
	mu       sync.Mutex
	maxBytes int64
	blobs    map[memKey][]byte
}

var _ Store = (*Memory)(nil)

// NewMemory returns an empty Memory store with no size limit.
func NewMemory() *Memory {
	return &Memory{blobs: make(map[memKey][]byte)}
}

// WithMaxBytes limits the size of a single blob and returns m. Put rejects
// larger data with ErrTooLarge. A value of zero or less removes the limit.
// Call it before the store is shared between goroutines.
func (m *Memory) WithMaxBytes(n int64) *Memory {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.maxBytes = n
	return m
}

// Len returns the number of blobs currently held.
func (m *Memory) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.blobs)
}

// Put implements Store. It stores a private copy of data, so later changes to
// the caller's slice do not affect the stored blob.
func (m *Memory) Put(ctx context.Context, ns namespace.Namespace, data []byte) (Ref, error) {
	if err := ctx.Err(); err != nil {
		return Ref{}, err
	}
	if err := checkNamespace(ns); err != nil {
		return Ref{}, err
	}
	size := int64(len(data))
	m.mu.Lock()
	limit := m.maxBytes
	m.mu.Unlock()
	if limit > 0 && size > limit {
		return Ref{}, fmt.Errorf("%w: %d bytes exceeds the %d byte limit", ErrTooLarge, size, limit)
	}
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	stored := append([]byte(nil), data...)

	m.mu.Lock()
	m.blobs[memKey{ns: ns, digest: digest}] = stored
	m.mu.Unlock()
	return Ref{Namespace: string(ns), Digest: digest, Size: size, Stored: size}, nil
}

// Get implements Store. It re-checks the digest and size of the stored bytes
// on every read, as a real backend must, and returns a copy the caller owns.
//
// The lock only covers fetching the stored slice. Stored slices are never
// modified after Put installs them (Put replaces the map entry, Delete removes
// it), so hashing and copying them outside the lock is race free.
func (m *Memory) Get(ctx context.Context, ns namespace.Namespace, ref Ref) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key, err := m.keyFor(ns, ref)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	stored, ok := m.blobs[key]
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", ErrNotFound, key.ns, key.digest)
	}
	sum := sha256.Sum256(stored)
	if hex.EncodeToString(sum[:]) != ref.Digest || int64(len(stored)) != ref.Size {
		return nil, fmt.Errorf("%w: %s/%s", ErrCorrupt, key.ns, key.digest)
	}
	return append([]byte(nil), stored...), nil
}

// Delete implements Store. Deleting a missing blob returns nil.
func (m *Memory) Delete(ctx context.Context, ns namespace.Namespace, ref Ref) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key, err := m.keyFor(ns, ref)
	if err != nil {
		return err
	}
	m.mu.Lock()
	delete(m.blobs, key)
	m.mu.Unlock()
	return nil
}

// keyFor validates ref and checks it belongs to ns, then returns its map key.
func (m *Memory) keyFor(ns namespace.Namespace, ref Ref) (memKey, error) {
	if err := checkRef(ns, ref); err != nil {
		return memKey{}, err
	}
	return memKey{ns: ns, digest: ref.Digest}, nil
}
