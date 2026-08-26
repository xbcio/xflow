package store_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/objectstore"
)

// tamperedStore is an objectstore.Store that serves bytes other than the ones
// its key names. It stands in for every way the read path can hand back the
// wrong content: a torn or bit-rotted file in the runner's on-disk cache, a
// cache directory a second process wrote into, a proxy or CDN in front of
// GET /v1/artifacts/{digest}, or a backend row whose content and content_hash
// have drifted apart.
type tamperedStore struct {
	body []byte
	// size is what the backend claims. Defaults to len(body) so the served
	// object is internally consistent — the point of these tests is content
	// that lies while every count agrees.
	size int64
}

func (t *tamperedStore) PutObject(context.Context, string, io.Reader, int64, objectstore.PutOptions) (*objectstore.Object, error) {
	return nil, errors.New("not supported")
}

func (t *tamperedStore) GetObject(_ context.Context, key string) (io.ReadCloser, *objectstore.Object, error) {
	size := t.size
	if size == 0 {
		size = int64(len(t.body))
	}
	return io.NopCloser(bytes.NewReader(t.body)), &objectstore.Object{
		Key: key,
		// ETag is the digest the key encodes, not a hash of body: this is
		// exactly what objectstore's fsstore and httpstore both do
		// (digestFromKey parses the key, it never recomputes anything), so a
		// caller trusting ETag learns nothing about the bytes.
		Size: size,
		ETag: strings.TrimPrefix(key[strings.LastIndex(key, "/")+1:], ""),
	}, nil
}

func (t *tamperedStore) HeadObject(_ context.Context, key string) (*objectstore.Object, error) {
	size := t.size
	if size == 0 {
		size = int64(len(t.body))
	}
	return &objectstore.Object{Key: key, Size: size}, nil
}

// TestArtifactStoreOpenRejectsContentThatDoesNotMatchItsDigest pins that Open
// hashes what it read.
//
// The digest is the only thing binding a workflow definition to the code a
// runner executes. A script node carries artifact_digest; the runner resolves
// it through ArtifactStore.Open and feeds the result straight to the wasm
// compiler (sdk/xflow/runner.go:893, node/internal/code/script/script.go:188).
// Nothing between those two points recomputes anything: ValidateDigest checks
// the *shape* of the requested string, objectstore's digestFromKey parses the
// digest back out of the key, and Object.ETag is set from that parse. The
// digest is a name the whole way down.
//
// The runner-side path is the one that matters. objectstore.ReadThrough serves
// a cache hit from local disk and returns it without so much as a size check
// (readthrough.go:48) — the size and write-through validations only run on the
// origin miss path. So whatever ends up at
// ~/Library/Caches/xflow/artifacts/artifacts/sha256/ab/cd/<hex> is compiled and
// executed as-is, forever, by every subsequent execution: script.go memoises
// the resolved bytes per (namespace, digest, language), so one bad read is
// pinned in process memory until restart.
//
// Verifying costs one sha256 over at most 16 MiB, once per digest per process —
// not once per message. It is not on the hot path.
func TestArtifactStoreOpenRejectsContentThatDoesNotMatchItsDigest(t *testing.T) {
	ctx := context.Background()
	genuine := []byte("\x00asm\x01\x00\x00\x00 the module the workflow actually asked for")
	digest := store.ContentHash(genuine)

	t.Run("genuine content is returned unchanged", func(t *testing.T) {
		s := store.NewArtifactStore(&tamperedStore{body: genuine}, nil)

		rc, ref, err := s.Open(ctx, digest)
		if err != nil {
			t.Fatalf("Open on matching content: %v", err)
		}
		defer func() { _ = rc.Close() }()

		got, err := io.ReadAll(rc)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if !bytes.Equal(got, genuine) {
			t.Fatalf("Open returned %d bytes, want the %d stored ones", len(got), len(genuine))
		}
		if ref.Digest != digest {
			t.Fatalf("ref.Digest = %q, want %q", ref.Digest, digest)
		}
		if ref.Size != int64(len(genuine)) {
			t.Fatalf("ref.Size = %d, want %d", ref.Size, len(genuine))
		}
	})

	// Each substitution keeps the byte count identical, so the size checks that
	// already exist in ReadThrough and FSStore cannot catch any of them. Only a
	// hash can.
	sameLength := func(b []byte, edit func([]byte)) []byte {
		out := append([]byte(nil), b...)
		edit(out)
		return out
	}
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{
			name: "a single flipped bit",
			body: sameLength(genuine, func(b []byte) { b[len(b)/2] ^= 0x01 }),
		},
		{
			name: "an entirely different module of the same length",
			body: sameLength(genuine, func(b []byte) {
				copy(b, "\x00asm\x01\x00\x00\x00 a module nobody in this workflow named")
			}),
		},
		{
			name: "a truncated read padded back to length",
			body: sameLength(genuine, func(b []byte) {
				for i := len(b) / 2; i < len(b); i++ {
					b[i] = 0
				}
			}),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if len(tc.body) != len(genuine) {
				t.Fatalf("setup: substituted body is %d bytes, want %d — the point of "+
					"this case is that every existing size check still passes",
					len(tc.body), len(genuine))
			}
			if bytes.Equal(tc.body, genuine) {
				t.Fatal("setup: substituted body is identical to the genuine one")
			}

			s := store.NewArtifactStore(&tamperedStore{body: tc.body}, nil)

			rc, _, err := s.Open(ctx, digest)
			if err == nil {
				_ = rc.Close()
				t.Fatalf("Open returned content whose sha256 is not %s; the digest in the "+
					"workflow definition is the only thing binding it to the code that "+
					"runs, so accepting these bytes means the runner compiles and "+
					"executes a module nobody authorized, and memoises it until restart",
					digest)
			}
			if !errors.Is(err, store.ErrDigestMismatch) {
				t.Fatalf("err = %v, want ErrDigestMismatch: callers classify this as "+
					"permanent (retrying a content-addressed read cannot help), so the "+
					"sentinel has to survive", err)
			}
			// The error names both digests so an operator can tell "the backend
			// lost my bytes" from "the backend has someone else's bytes".
			if !strings.Contains(err.Error(), digest) {
				t.Fatalf("err = %v, want it to name the requested digest %s", err, digest)
			}
			if !strings.Contains(err.Error(), store.ContentHash(tc.body)) {
				t.Fatalf("err = %v, want it to name the digest of what was actually read (%s)",
					err, store.ContentHash(tc.body))
			}
			// Never echo the bytes themselves: an artifact is source or a
			// compiled module, and this error is logged.
			if bytes.Contains([]byte(err.Error()), tc.body[:16]) {
				t.Fatalf("the mismatch error quotes the content back")
			}
		})
	}
}
