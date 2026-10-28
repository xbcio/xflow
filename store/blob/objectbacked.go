package blob

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store/objectstore"
)

const (
	// DefaultMaxBytes is the per-blob uncompressed size limit ObjectBacked uses
	// when ObjectBackedOptions.MaxBytes is zero.
	DefaultMaxBytes int64 = 64 << 20

	// DefaultMinCompress is the size below which ObjectBacked does not try to
	// compress when ObjectBackedOptions.MinCompress is zero. A zstd frame has
	// overhead of its own, so very small blobs rarely shrink.
	DefaultMinCompress = 1024
)

// ObjectBackedOptions configures an ObjectBacked store. The zero value stores
// every blob raw with the default size limit.
type ObjectBackedOptions struct {
	// Compress turns on zstd for blobs that are large enough and actually get
	// smaller. A blob that does not shrink is stored raw.
	Compress bool
	// MinCompress is the smallest blob Compress will try. Zero means
	// DefaultMinCompress.
	MinCompress int
	// MaxBytes is the largest uncompressed blob the store accepts. Zero means
	// DefaultMaxBytes. Lowering it makes blobs written under a higher limit
	// unreadable: Get returns ErrTooLarge for them. Only raise it or leave it.
	MaxBytes int64
	// Index, when set, makes Put register every blob with the lifecycle index
	// before it touches the object store, which is what lets a Collector delete
	// bytes without ever stranding a Ref (see Index). Put then returns the Ref the
	// index records, so two writers that encode one digest differently agree on
	// its Codec and Stored. Nil leaves ObjectBacked without garbage-collection
	// safety: use it only where nothing deletes.
	Index Index
}

// ObjectBacked is a Store that keeps blobs in an objectstore.Store.
//
// Each object is a one-byte frame header followed by the payload (see
// frame.go), so the encoding travels inside the bytes. Get trusts only that
// header and never Ref.Codec or Ref.Stored, which are the writer's records:
// two writers sharing one object store can pick different codecs for the same
// digest, and the object store does not have to persist any metadata.
//
// Without an Index, ObjectBacked provides no garbage-collection safety. Delete
// removes the bytes at once, so do not call it for a digest a writer may still
// reference unless the caller fences against concurrent Puts of that digest. With
// an Index, deletion belongs to a Collector, which deletes through the Index's
// fence; calling Delete directly bypasses that fence and is unsafe in the same
// way.
type ObjectBacked struct {
	objects     objectstore.Store
	index       Index
	compress    bool
	minCompress int
	maxBytes    int64
}

var _ Store = (*ObjectBacked)(nil)

// NewObjectBacked returns a Store that keeps its blobs in objects.
func NewObjectBacked(objects objectstore.Store, opts ObjectBackedOptions) *ObjectBacked {
	b := &ObjectBacked{
		objects: objects, index: opts.Index,
		compress: opts.Compress, minCompress: opts.MinCompress, maxBytes: opts.MaxBytes,
	}
	if b.minCompress <= 0 {
		b.minCompress = DefaultMinCompress
	}
	if b.maxBytes <= 0 {
		b.maxBytes = DefaultMaxBytes
	}
	return b
}

// Put implements Store. Storing content that is already present is a success:
// the existing object has the same digest and therefore the same bytes.
//
// The duplicate branch is only a cheap check: it does not read the existing
// object back or verify its content. A damaged object is reported as ErrCorrupt
// when someone Gets it, and repairing it is left to the garbage-collection
// layer, which Put never overwrites on its own. The returned Ref.Codec and
// Ref.Stored describe what THIS call would have written. They are the writer's
// record for statistics only. Get reads the frame header of whichever object is
// actually stored, so a Ref from a call that deduplicated against an object with
// a different codec still reads back correctly.
//
// With an Index the order is fixed: Register first, then the object store. The
// returned Ref then carries the Codec and Stored the index records, which a
// writer that lost the race to write the bytes would otherwise get wrong. When
// Register reports that it took the object back from a collector, the bytes are
// rewritten unconditionally: the collector may already have deleted them (a
// crash between the delete and its commit leaves the claim standing with no
// bytes), and a Put that deduplicated against nothing would return a Ref to
// nothing. Callers that need the blob to survive until an execution is done then
// call Index.Link with the returned Ref; a Link that returns ErrFenced or
// ErrNotFound means a collector overtook this Put, and the caller Puts again.
func (b *ObjectBacked) Put(ctx context.Context, ns namespace.Namespace, data []byte) (Ref, error) {
	if err := ctx.Err(); err != nil {
		return Ref{}, err
	}
	if err := checkNamespace(ns); err != nil {
		return Ref{}, err
	}
	size := int64(len(data))
	if size > b.maxBytes {
		return Ref{}, fmt.Errorf("%w: %d bytes exceeds the %d byte limit", ErrTooLarge, size, b.maxBytes)
	}
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	key, err := objectKey(ns, digest)
	if err != nil {
		return Ref{}, err
	}

	ref := Ref{Namespace: string(ns), Digest: digest, Size: size, Stored: size}
	// open returns a fresh reader over the frame header byte followed by the
	// payload, and the total length. A raw payload is streamed behind the header
	// instead of copied, so a large blob is not held in memory twice. Put may
	// need to write twice (see store), hence a function rather than one reader.
	open := func() (io.Reader, int64) {
		return io.MultiReader(bytes.NewReader([]byte{frameRaw}), bytes.NewReader(data)), size + 1
	}
	if frame, ok := b.encode(data); ok {
		ref.Codec, ref.Stored = CodecZstd, int64(len(frame)-1)
		open = func() (io.Reader, int64) { return bytes.NewReader(frame), int64(len(frame)) }
	}
	// Register before any byte is written or deduplicated against: this is the
	// writer's half of the fence (see Index). It also replaces ref with the
	// metadata the index records.
	revived := false
	if b.index != nil {
		reg, err := b.index.Register(ctx, ref)
		if err != nil {
			return Ref{}, fmt.Errorf("blob: register %s: %w", key, err)
		}
		ref, revived = reg.Ref, reg.Revived
	}
	if err := b.store(ctx, key, open, size, revived); err != nil {
		return Ref{}, err
	}
	return ref, nil
}

// encode returns the zstd frame for data when compression is on, the blob is at
// least minCompress long, and the frame is smaller than the data.
func (b *ObjectBacked) encode(data []byte) ([]byte, bool) {
	if !b.compress || len(data) < b.minCompress {
		return nil, false
	}
	return encodeZstd(data)
}
