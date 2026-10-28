package blobtest

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/store/blob"
)

// caseGetDetectsSizeMismatch requires every Store to compare ref.Size with the
// size of the bytes it holds, not only the digest. A Ref that names the right
// digest with a wrong size is a damaged or forged Ref, and the Store must report
// ErrCorrupt rather than return bytes the Ref does not describe.
func caseGetDetectsSizeMismatch(t *testing.T, s blob.Store) {
	ctx := context.Background()
	ref, err := s.Put(ctx, nsA, []byte("size is part of the ref"))
	if err != nil {
		t.Fatalf("Put error = %v", err)
	}
	bad := ref
	bad.Size++
	if bad.Codec == blob.CodecNone {
		// Keep the Ref well formed (Stored == Size without a codec) so the
		// failure under test is the size/bytes mismatch, not ErrInvalidRef.
		bad.Stored++
	}
	if _, err := s.Get(ctx, nsA, bad); !errors.Is(err, blob.ErrCorrupt) {
		t.Fatalf("Get(%q, %+v) with Size one too large error = %v, want ErrCorrupt", nsA, bad, err)
	}
}

func caseGetMissing(t *testing.T, s blob.Store) {
	ref := blob.Ref{
		Namespace: string(nsA),
		Digest:    sha256Hex([]byte("never written")),
		Size:      13,
		Stored:    13,
	}
	if _, err := s.Get(context.Background(), nsA, ref); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("Get(%q, %+v) error = %v, want ErrNotFound", nsA, ref, err)
	}
}

func caseGetRejectsInvalidRef(t *testing.T, s blob.Store) {
	ctx := context.Background()
	good, err := s.Put(ctx, nsA, []byte("valid ref source"))
	if err != nil {
		t.Fatalf("Put error = %v", err)
	}
	mutations := []struct {
		name   string
		mutate func(r *blob.Ref)
	}{
		{"DigestNotHex", func(r *blob.Ref) { r.Digest = "zz" + r.Digest[2:] }},
		{"DigestTooShort", func(r *blob.Ref) { r.Digest = r.Digest[:63] }},
		{"DigestTooLong", func(r *blob.Ref) { r.Digest += "0" }},
		{"DigestUppercase", func(r *blob.Ref) { r.Digest = "ABCDEF" + r.Digest[6:] }},
		{"DigestEmpty", func(r *blob.Ref) { r.Digest = "" }},
		{"CodecUnknown", func(r *blob.Ref) { r.Codec = "gzip" }},
		{"SizeNegative", func(r *blob.Ref) { r.Size = -1 }},
	}
	for _, m := range mutations {
		t.Run(m.name, func(t *testing.T) {
			bad := good
			m.mutate(&bad)
			if _, err := s.Get(ctx, nsA, bad); !errors.Is(err, blob.ErrInvalidRef) {
				t.Fatalf("Get(%q, %+v) error = %v, want ErrInvalidRef", nsA, bad, err)
			}
		})
	}
}
