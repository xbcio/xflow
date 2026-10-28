package blobtest

import (
	"bytes"
	"context"
	"testing"

	"github.com/xbcio/xflow/store/blob"
)

func caseRoundTrip(t *testing.T, s blob.Store) {
	ctx := context.Background()
	payloads := []struct {
		name string
		data []byte
	}{
		{"Empty", []byte{}},
		{"OneByte", []byte{0x7f}},
		{"OneMiB", randomBytes(t, 1<<20)},
		{"Compressible", bytes.Repeat([]byte("xflow blob "), 100<<10)},
	}
	for _, p := range payloads {
		t.Run(p.name, func(t *testing.T) {
			ref, err := s.Put(ctx, nsA, p.data)
			if err != nil {
				t.Fatalf("Put(%q, %d bytes) error = %v", nsA, len(p.data), err)
			}
			// The digest is over the uncompressed bytes whatever the codec, and a
			// Store that does not compress must report Stored == Size.
			if want := sha256Hex(p.data); ref.Digest != want {
				t.Errorf("Put(%d bytes) digest = %q, want %q", len(p.data), ref.Digest, want)
			}
			if ref.Codec == blob.CodecNone && ref.Stored != ref.Size {
				t.Errorf("Put(%d bytes) ref = %+v, want Stored == Size when Codec is empty", len(p.data), ref)
			}
			got, err := s.Get(ctx, nsA, ref)
			if err != nil {
				t.Fatalf("Get(%q, %+v) error = %v", nsA, ref, err)
			}
			if !bytes.Equal(got, p.data) {
				t.Fatalf("Get(%q, %+v) returned %d bytes that differ from the %d bytes stored", nsA, ref, len(got), len(p.data))
			}
		})
	}
}

func caseIdempotent(t *testing.T, s blob.Store) {
	ctx := context.Background()
	data := []byte("same content twice")
	first, err := s.Put(ctx, nsA, data)
	if err != nil {
		t.Fatalf("first Put error = %v", err)
	}
	second, err := s.Put(ctx, nsA, data)
	if err != nil {
		t.Fatalf("second Put of identical content error = %v", err)
	}
	if first.Digest != second.Digest || first.Size != second.Size || first.Namespace != second.Namespace {
		t.Fatalf("second Put ref = %+v, want it equivalent to the first %+v", second, first)
	}
	for i, ref := range []blob.Ref{first, second} {
		got, err := s.Get(ctx, nsA, ref)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("Get(ref #%d %+v) = (%q, %v), want (%q, nil)", i+1, ref, got, err, data)
		}
	}
}

func caseDifferentDigest(t *testing.T, s blob.Store) {
	ctx := context.Background()
	a, err := s.Put(ctx, nsA, []byte("content one"))
	if err != nil {
		t.Fatalf("Put(content one) error = %v", err)
	}
	b, err := s.Put(ctx, nsA, []byte("content two"))
	if err != nil {
		t.Fatalf("Put(content two) error = %v", err)
	}
	if a.Digest == b.Digest {
		t.Fatalf("different content produced the same digest %q", a.Digest)
	}
}

func caseRefFields(t *testing.T, s blob.Store) {
	ctx := context.Background()
	data := randomBytes(t, 4096)
	ref, err := s.Put(ctx, nsA, data)
	if err != nil {
		t.Fatalf("Put error = %v", err)
	}
	if ref.Namespace != string(nsA) {
		t.Errorf("ref.Namespace = %q, want %q", ref.Namespace, nsA)
	}
	if ref.Size != int64(len(data)) {
		t.Errorf("ref.Size = %d, want %d", ref.Size, len(data))
	}
	if want := sha256Hex(data); ref.Digest != want {
		t.Errorf("ref.Digest = %q, want %q", ref.Digest, want)
	}
	if err := ref.Validate(); err != nil {
		t.Errorf("ref.Validate() for %+v error = %v, want nil", ref, err)
	}
}
