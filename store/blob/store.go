package blob

import (
	"context"

	"github.com/xbcio/xflow/namespace"
)

// Store moves blob bytes by content. It knows nothing about retention, links to
// executions, or garbage collection: those live in the layers above.
//
// Implementations must be safe for concurrent use. A Store never accepts a
// digest from the caller; it always computes the SHA-256 of the bytes itself.
type Store interface {
	// Put stores data under ns and returns its Ref. Put is idempotent per
	// (ns, content): storing the same bytes again returns an equivalent Ref and
	// no error. The implementation takes its own copy, so the caller may reuse
	// data after Put returns. Data over the Store's size limit is rejected with
	// ErrTooLarge.
	Put(ctx context.Context, ns namespace.Namespace, data []byte) (Ref, error)

	// Get returns the uncompressed bytes ref points at. ref.Namespace must equal
	// ns, otherwise Get returns ErrNamespaceMismatch. A malformed ref returns an
	// error wrapping ErrInvalidRef, a missing blob returns ErrNotFound, and bytes
	// that do not match ref.Digest or ref.Size return ErrCorrupt.
	//
	// The returned slice belongs to the caller, who may modify it freely.
	Get(ctx context.Context, ns namespace.Namespace, ref Ref) ([]byte, error)

	// Delete removes the bytes ref points at. It is idempotent: deleting a blob
	// that does not exist returns nil. It applies the same ref and namespace
	// checks as Get. A Store whose underlying storage cannot delete returns
	// ErrDeleteUnsupported.
	//
	// Delete gives no ordering guarantee against a concurrent Put of the same
	// content: the Put may land before or after the delete. The caller that
	// must not lose a freshly written blob (a garbage collector, for instance)
	// fences that itself.
	Delete(ctx context.Context, ns namespace.Namespace, ref Ref) error
}
