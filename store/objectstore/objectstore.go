// Package objectstore defines a generic, S3-shaped object storage contract.
//
// The contract makes no assumption about what is stored: a key is an opaque
// string, a body is opaque bytes. It knows nothing about scripts, languages,
// runtimes, or workflows. Consumer-specific semantics travel in Object.Metadata,
// which every backend stores and returns without interpreting.
//
// The content-addressing semantics xflow actually needs (immutable bytes keyed
// by their own sha256) live one layer up, in store.ArtifactStore, precisely so
// that layer can be reused across backends.
package objectstore

import (
	"context"
	"errors"
	"io"
	"time"
)

// ErrNotFound is returned when the requested key does not exist. Every Store
// implementation must report a missing key as this sentinel (wrapped is fine) so
// a composition like ReadThrough can tell "not here" apart from "backend broken"
// without knowing which backend it holds.
//
// It lives here rather than in the parent store package because store imports
// objectstore, so the reverse would be an import cycle. Implementations that also
// live under store (the MySQL blob repo) still return THIS sentinel rather than
// store.ErrNotFound: what they implement is this interface.
var ErrNotFound = errors.New("objectstore: not found")

// ErrPreconditionFailed is returned by PutObject when PutOptions.IfNoneMatch was
// set and the key already exists. Callers that are content-addressing treat this
// as success-by-deduplication, not as a failure.
var ErrPreconditionFailed = errors.New("objectstore: precondition failed")

// Object is the metadata a backend reports for a stored object.
type Object struct {
	Key  string
	Size int64

	// ETag identifies the content. Every implementation in this repository
	// fills it with "sha256:<hex>".
	//
	// WARNING: real S3 returns MD5 for a single-part upload, and for a
	// multipart upload an ETag that is not any single hash of the content. If
	// an S3 backend is ever added here, its ETag must NOT be treated as a
	// sha256: put the sha256 in Metadata, or recover it from the key. Getting
	// this wrong yields an integrity check that silently verifies nothing.
	ETag string

	ContentType  string
	LastModified time.Time

	// Metadata holds small key/value pairs the backend stores verbatim and
	// never interprets. Consumer semantics (a filename, for instance) travel
	// here so backends need not know about them.
	Metadata map[string]string
}

// PutOptions are the write-side options.
type PutOptions struct {
	ContentType string
	Metadata    map[string]string

	// IfNoneMatch writes only when the key is absent, returning
	// ErrPreconditionFailed when it already exists. It mirrors S3's
	// "If-None-Match: *". Content-addressed callers set it to avoid rewriting
	// a multi-megabyte body that is byte-identical to what is already stored.
	IfNoneMatch bool
}

// Store is the generic object storage contract.
//
// Deliberate omissions, each for a reason:
//
//   - No ListObjects: every backend pays a real implementation cost for it (an
//     extra index in SQL, pagination cursors in S3) and there is no consumer.
//   - No multipart upload: excluded by the 16 MiB per-object ceiling.
//   - No DeleteObject on Store itself: deletion is offered as the optional
//     Deleter interface instead. Its only consumer is blob lifecycle management
//     (store/blob), and most Store implementations never delete, so widening
//     Store would force every implementation and test fake to grow a method
//     only one caller uses. HTTPStore is read-only and does not implement it.
//
// Get and Head are kept separate on purpose — that part of S3's shape is worth
// copying. Head is a SELECT without the content column in SQL, and a real HEAD
// request over HTTP. Merging them into one Get would block both optimizations
// that separation enables: a proxy 404ing without pulling 16 MiB, and a local
// cache validating by size without going back to the origin.
type Store interface {
	// PutObject stores body under key. size is the exact byte count of body.
	PutObject(ctx context.Context, key string, body io.Reader, size int64, opts PutOptions) (*Object, error)

	// GetObject returns the content and metadata for key, or an error wrapping
	// ErrNotFound if absent. The caller closes the reader.
	GetObject(ctx context.Context, key string) (io.ReadCloser, *Object, error)

	// HeadObject returns metadata only, never the content. A missing key is
	// reported as an error wrapping ErrNotFound.
	HeadObject(ctx context.Context, key string) (*Object, error)
}

// Deleter is an optional capability of a Store: removing an object's bytes.
// Callers discover it with a type assertion on the Store they hold:
//
//	if d, ok := s.(objectstore.Deleter); ok { ... }
//
// A Store that does not implement it (HTTPStore, which is read-only) simply
// cannot have its objects deleted through this package.
type Deleter interface {
	// DeleteObject removes the object stored under key. Deleting a key that does
	// not exist returns nil, so a retry after a partial failure is safe.
	//
	// An implementation backed by directories (FSStore) does not remove the
	// empty directories a delete leaves behind: doing so races a concurrent
	// PutObject creating the same directory chain, and an empty directory is
	// harmless in a content-addressed layout.
	//
	// DeleteObject gives no ordering guarantee against a concurrent PutObject of
	// the same key: the put may land before or after the delete. A caller that
	// must not lose a freshly written object (a blob garbage collector, for
	// instance) fences that itself rather than relying on this method.
	DeleteObject(ctx context.Context, key string) error
}
