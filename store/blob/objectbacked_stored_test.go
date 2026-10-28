package blob

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"github.com/xbcio/xflow/store/objectstore"
)

// TestObjectBacked_StoredMatchesObject checks the Ref.Stored a Put reports
// against the size of the object that actually landed in the object store.
// Stored counts the payload without the one-byte frame header, so it must be
// one less than the object, and equal to Size for a raw blob.
func TestObjectBacked_StoredMatchesObject(t *testing.T) {
	ctx := context.Background()
	const ns = "stored"
	incompressible := make([]byte, 4<<10)
	if _, err := rand.Read(incompressible); err != nil {
		t.Fatalf("rand.Read error = %v", err)
	}

	for _, tt := range []struct {
		name      string
		compress  bool
		data      []byte
		wantCodec string
	}{
		{"RawWithoutCompression", false, bytes.Repeat([]byte("stored "), 2<<10), CodecNone},
		{"CompressedWhenItShrinks", true, bytes.Repeat([]byte("stored "), 2<<10), CodecZstd},
		{"RawWhenItDoesNotShrink", true, incompressible, CodecNone},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fs := objectstore.NewFSStore(t.TempDir())
			s := NewObjectBacked(fs, ObjectBackedOptions{Compress: tt.compress})
			ref, err := s.Put(ctx, ns, tt.data)
			if err != nil {
				t.Fatalf("Put error = %v", err)
			}
			key, err := objectKey(ns, ref.Digest)
			if err != nil {
				t.Fatalf("objectKey error = %v", err)
			}
			info, err := fs.HeadObject(ctx, key)
			if err != nil {
				t.Fatalf("HeadObject(%q) error = %v", key, err)
			}
			if ref.Codec != tt.wantCodec {
				t.Errorf("Ref.Codec = %q, want %q", ref.Codec, tt.wantCodec)
			}
			if ref.Stored != info.Size-1 {
				t.Errorf("Ref.Stored = %d for an object of %d bytes, want %d (object minus the header byte)", ref.Stored, info.Size, info.Size-1)
			}
			if ref.Codec == CodecNone && (ref.Stored != ref.Size || info.Size != ref.Size+1) {
				t.Errorf("raw blob: Stored = %d, object = %d bytes, want Stored = Size = %d and object = Size+1", ref.Stored, info.Size, ref.Size)
			}
		})
	}
}
