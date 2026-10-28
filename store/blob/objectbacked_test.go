package blob_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store/blob"
	"github.com/xbcio/xflow/store/blob/blobtest"
	"github.com/xbcio/xflow/store/objectstore"
)

func TestObjectBacked_Contract(t *testing.T) {
	for _, compress := range []bool{false, true} {
		name := "Raw"
		if compress {
			name = "Compress"
		}
		t.Run(name, func(t *testing.T) {
			blobtest.RunStoreContract(t, func(t *testing.T) blob.Store {
				t.Helper()
				return blob.NewObjectBacked(objectstore.NewFSStore(t.TempDir()), blob.ObjectBackedOptions{Compress: compress})
			})
		})
	}
}

func TestObjectBacked_CompressionChoice(t *testing.T) {
	ctx := context.Background()
	const ns namespace.Namespace = "codec"
	newStore := func(t *testing.T, opts blob.ObjectBackedOptions) *blob.ObjectBacked {
		t.Helper()
		return blob.NewObjectBacked(objectstore.NewFSStore(t.TempDir()), opts)
	}
	random := make([]byte, 8<<10)
	if _, err := rand.Read(random); err != nil {
		t.Fatalf("rand.Read error = %v", err)
	}

	t.Run("CompressibleShrinks", func(t *testing.T) {
		data := bytes.Repeat([]byte("xflow blob "), 10<<10)
		ref, err := newStore(t, blob.ObjectBackedOptions{Compress: true}).Put(ctx, ns, data)
		if err != nil {
			t.Fatalf("Put error = %v", err)
		}
		if ref.Codec != blob.CodecZstd || ref.Stored >= ref.Size {
			t.Fatalf("Put(compressible %d bytes) ref = %+v, want Codec zstd and Stored < Size", len(data), ref)
		}
	})

	t.Run("IncompressibleStaysRaw", func(t *testing.T) {
		ref, err := newStore(t, blob.ObjectBackedOptions{Compress: true}).Put(ctx, ns, random)
		if err != nil {
			t.Fatalf("Put error = %v", err)
		}
		if ref.Codec != blob.CodecNone || ref.Stored != ref.Size {
			t.Fatalf("Put(random %d bytes) ref = %+v, want Codec empty and Stored == Size", len(random), ref)
		}
	})

	t.Run("BelowMinCompressStaysRaw", func(t *testing.T) {
		data := bytes.Repeat([]byte("a"), 512)
		ref, err := newStore(t, blob.ObjectBackedOptions{Compress: true}).Put(ctx, ns, data)
		if err != nil {
			t.Fatalf("Put error = %v", err)
		}
		if ref.Codec != blob.CodecNone {
			t.Fatalf("Put(512 bytes) with default MinCompress ref = %+v, want Codec empty", ref)
		}
		ref, err = newStore(t, blob.ObjectBackedOptions{Compress: true, MinCompress: 100}).Put(ctx, ns, data)
		if err != nil || ref.Codec != blob.CodecZstd {
			t.Fatalf("Put(512 bytes) with MinCompress 100 = (%+v, %v), want Codec zstd", ref, err)
		}
	})

	t.Run("CompressOffNeverCompresses", func(t *testing.T) {
		data := bytes.Repeat([]byte("a"), 64<<10)
		ref, err := newStore(t, blob.ObjectBackedOptions{}).Put(ctx, ns, data)
		if err != nil || ref.Codec != blob.CodecNone || ref.Stored != ref.Size {
			t.Fatalf("Put with Compress off = (%+v, %v), want a raw ref", ref, err)
		}
	})
}
