package objectstore_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xbcio/xflow/store/objectstore"
)

// ceilingBytes mirrors the unexported maxArtifactResponseBytes (httpstore.go:15).
// It is duplicated rather than exported because the constant is deliberately
// package-private; if the two ever diverge the at-ceiling sub-test below fails
// loudly, which is the outcome we want from a drifting duplicate.
const ceilingBytes = 16 << 20

// TestFSStore_ResolveRejectsSiblingDirectoryWithRootAsPrefix pins the trailing
// separator in resolve's boundary check (fsstore.go:160):
//
//	strings.HasPrefix(cleaned, s.root+string(filepath.Separator))
//
// The separator is the entire difference between a path-segment boundary and a
// plain substring match. Drop it and any sibling directory whose name merely
// begins with root's own name — root "/tmp/x/001" versus "/tmp/x/001-evil" —
// satisfies the check and the store happily reads and writes outside its root.
//
// TestFSStore_PathTraversal, the existing traversal test, cannot catch this. It
// tries "../../etc/passwd" and "../../../etc/shadow", which resolve nowhere
// near the randomly named t.TempDir() root and so share no prefix with it at
// all. Those strings are rejected by any prefix check, correct or broken; they
// establish that a check exists, not that it draws the boundary in the right
// place.
//
// All three methods are exercised because resolve is called from each of them
// and a boundary that leaks on write is as bad as one that leaks on read.
func TestFSStore_ResolveRejectsSiblingDirectoryWithRootAsPrefix(t *testing.T) {
	root := t.TempDir()
	// Deliberately a sibling of root, not a child, and named so that its path
	// has root's full path as a string prefix. t.TempDir only cleans up root
	// itself, so this one is removed explicitly.
	sibling := root + "-evil"
	if err := os.MkdirAll(sibling, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", sibling, err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sibling) })

	const secret = "top secret"
	secretPath := filepath.Join(sibling, "secret.txt")
	if err := os.WriteFile(secretPath, []byte(secret), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Cleans to <parent>/<base(root)>-evil/secret.txt — outside root, but with
	// root's own path as a character-level prefix.
	key := "../" + filepath.Base(root) + "-evil/secret.txt"

	fs := objectstore.NewFSStore(root)
	ctx := context.Background()

	if rc, _, err := fs.GetObject(ctx, key); err == nil {
		_ = rc.Close()
		t.Errorf("GetObject(%q) succeeded: the root boundary is a bare substring "+
			"match, so a sibling directory sharing root's name prefix is readable", key)
	}
	if _, err := fs.HeadObject(ctx, key); err == nil {
		t.Errorf("HeadObject(%q) succeeded: the root boundary leaks metadata about "+
			"files outside root", key)
	}
	body := "overwritten"
	if _, err := fs.PutObject(ctx, key, strings.NewReader(body), int64(len(body)), objectstore.PutOptions{}); err == nil {
		t.Errorf("PutObject(%q) succeeded: the root boundary permits writes outside root", key)
	}

	// The write attempt above must not have landed even if PutObject somehow
	// reported an error afterwards.
	got, err := os.ReadFile(secretPath)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", secretPath, err)
	}
	if string(got) != secret {
		t.Errorf("file outside root now contains %q, want %q: a rejected key still "+
			"reached the filesystem", got, secret)
	}

	// Control: a well-formed key must still work, so the test cannot be
	// satisfied by a resolve that rejects everything.
	if _, err := fs.PutObject(ctx, testKey, strings.NewReader("ok"), 2, objectstore.PutOptions{}); err != nil {
		t.Fatalf("PutObject(%q) on a valid key failed: %v", testKey, err)
	}
}

// unboundedOrigin is an objectstore.Store whose GetObject returns more bytes
// than the ceiling and reports Size == -1 (unknown length, as a chunked
// response with no Content-Length would). Size == -1 matters: the size-mismatch
// check further down in ReadThrough.GetObject is gated on Size >= 0, so with an
// unknown size the byte ceiling is the only thing left standing.
type unboundedOrigin struct {
	size int64
	n    int
}

func (o *unboundedOrigin) GetObject(_ context.Context, key string) (io.ReadCloser, *objectstore.Object, error) {
	return io.NopCloser(bytes.NewReader(bytes.Repeat([]byte("A"), o.n))),
		&objectstore.Object{Key: key, Size: o.size}, nil
}

func (o *unboundedOrigin) HeadObject(_ context.Context, key string) (*objectstore.Object, error) {
	return &objectstore.Object{Key: key, Size: o.size}, nil
}

func (o *unboundedOrigin) PutObject(_ context.Context, key string, body io.Reader, size int64, _ objectstore.PutOptions) (*objectstore.Object, error) {
	_, _ = io.Copy(io.Discard, body)
	return &objectstore.Object{Key: key, Size: size}, nil
}

// TestReadThrough_OriginByteCeiling pins readthrough.go:65, the second of the
// two byte ceilings on the read path. Nothing in the package ever built an
// origin near the ceiling — grepping the test files for the constant or for
// "exceeds" matched nothing — so the check could be disabled outright and the
// suite stayed green.
//
// The comment above that line says exactly why it exists: HTTPStore enforces
// its own cap via limitedReadCloser, and this one is "a second layer here in
// case origin is a different impl". ReadThrough is the runner-side cache, so
// "a different impl" is not hypothetical — anything satisfying objectstore.Store
// can be handed in. Without this check the whole origin body is read into
// memory before anything examines it, and since io.ReadAll is bounded only by
// LimitReader(…, ceiling+1) the failure without it is not an OOM but something
// quieter: an oversize artifact gets truncated to ceiling+1 bytes and cached
// under the digest-shaped key of the artifact it is not.
//
// Both halves are asserted. Rejecting the oversize body proves the check runs;
// accepting a body of exactly the ceiling proves it is `>` and not `>=`, which
// would make the effective cap one byte smaller than the constant everything
// else in the package is sized against.
func TestReadThrough_OriginByteCeiling(t *testing.T) {
	ctx := context.Background()

	t.Run("one byte over the ceiling is rejected and not cached", func(t *testing.T) {
		cache := objectstore.NewFSStore(t.TempDir())
		rt := objectstore.NewReadThrough(cache, &unboundedOrigin{size: -1, n: ceilingBytes + 1})

		rc, _, err := rt.GetObject(ctx, testKey)
		if err == nil {
			_ = rc.Close()
			t.Fatalf("GetObject returned nil error for an origin body of %d bytes: "+
				"the ceiling is not enforced, so the body is silently truncated to "+
				"the LimitReader bound and cached under a digest it does not hash to",
				ceilingBytes+1)
		}
		if !strings.Contains(err.Error(), "exceeds") {
			t.Fatalf("GetObject error = %v, want the size-ceiling error", err)
		}
		if rc, _, cerr := cache.GetObject(ctx, testKey); cerr == nil {
			_ = rc.Close()
			t.Fatal("the rejected body was written to the cache anyway")
		}
	})

	t.Run("exactly at the ceiling is accepted", func(t *testing.T) {
		cache := objectstore.NewFSStore(t.TempDir())
		rt := objectstore.NewReadThrough(cache, &unboundedOrigin{size: -1, n: ceilingBytes})

		rc, _, err := rt.GetObject(ctx, testKey)
		if err != nil {
			t.Fatalf("GetObject of exactly %d bytes failed: %v; the ceiling is off by "+
				"one and rejects the largest artifact the rest of the package allows",
				ceilingBytes, err)
		}
		n, err := io.Copy(io.Discard, rc)
		_ = rc.Close()
		if err != nil {
			t.Fatalf("draining body: %v", err)
		}
		if n != ceilingBytes {
			t.Fatalf("read %d bytes, want %d", n, ceilingBytes)
		}
	})
}
