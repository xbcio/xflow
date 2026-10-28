package blob_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store/blob"
	"github.com/xbcio/xflow/store/objectstore"
)

// TestObjectBacked_MixedWriters covers two writers with different Compress
// settings sharing one object store, in both orders. The second Put of the
// same digest takes the duplicate branch and leaves the first writer's object
// in place, so the second Ref's Codec describes a write that never happened.
func TestObjectBacked_MixedWriters(t *testing.T) {
	ctx := context.Background()
	const ns namespace.Namespace = "mixed"
	data := bytes.Repeat([]byte("shared content "), 4<<10)

	// flip returns a Ref that lies about its codec and stored size.
	flip := func(r blob.Ref) blob.Ref {
		if r.Codec == blob.CodecZstd {
			r.Codec, r.Stored = blob.CodecNone, r.Size
		} else {
			r.Codec, r.Stored = blob.CodecZstd, 1
		}
		return r
	}

	for _, tt := range []struct {
		name          string
		firstCompress bool
	}{
		{"CompressedThenRaw", true},
		{"RawThenCompressed", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			shared := objectstore.NewFSStore(t.TempDir())
			first := blob.NewObjectBacked(shared, blob.ObjectBackedOptions{Compress: tt.firstCompress})
			second := blob.NewObjectBacked(shared, blob.ObjectBackedOptions{Compress: !tt.firstCompress})

			refFirst, err := first.Put(ctx, ns, data)
			if err != nil {
				t.Fatalf("first Put error = %v", err)
			}
			refSecond, err := second.Put(ctx, ns, data) // The object already exists.
			if err != nil {
				t.Fatalf("second Put of an existing digest error = %v", err)
			}
			if (refFirst.Codec == blob.CodecZstd) != tt.firstCompress || (refSecond.Codec == blob.CodecZstd) == tt.firstCompress || refFirst.Digest != refSecond.Digest {
				t.Fatalf("refs = %+v and %+v, want one digest and opposite codecs (first compressed: %v)", refFirst, refSecond, tt.firstCompress)
			}

			// Get must trust the frame header only, so even a Ref that lies
			// about its codec and stored size reads back correctly through
			// either store.
			for _, ref := range []blob.Ref{refFirst, refSecond, flip(refFirst), flip(refSecond)} {
				for name, s := range map[string]blob.Store{"first": first, "second": second} {
					got, err := s.Get(ctx, ns, ref)
					if err != nil || !bytes.Equal(got, data) {
						t.Errorf("%s writer Get(%+v) = (%d bytes, %v), want the original %d bytes", name, ref, len(got), err, len(data))
					}
				}
			}
		})
	}
}
