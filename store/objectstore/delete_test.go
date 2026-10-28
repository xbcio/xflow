package objectstore_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store/objectstore"
)

var _ objectstore.Deleter = (*objectstore.FSStore)(nil)

// putContent stores content under key and fails the test on error.
func putContent(ctx context.Context, t *testing.T, s objectstore.Store, key, content string) {
	t.Helper()
	if _, err := s.PutObject(ctx, key, strings.NewReader(content), int64(len(content)), objectstore.PutOptions{}); err != nil {
		t.Fatalf("PutObject(%q): %v", key, err)
	}
}

// requireContent fails the test unless key currently holds exactly want.
func requireContent(ctx context.Context, t *testing.T, s objectstore.Store, key, want string) {
	t.Helper()
	rc, _, err := s.GetObject(ctx, key)
	if err != nil {
		t.Fatalf("GetObject(%q): %v", key, err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read %q: %v", key, err)
	}
	if !bytes.Equal(got, []byte(want)) {
		t.Fatalf("GetObject(%q) content = %q, want %q", key, got, want)
	}
}

func TestFSStore_DeleteObject(t *testing.T) {
	t.Run("RemovesExistingObject", func(t *testing.T) {
		dir := t.TempDir()
		fs := objectstore.NewFSStore(dir)
		ctx := context.Background()
		putContent(ctx, t, fs, testKey, "hello world")

		if err := fs.DeleteObject(ctx, testKey); err != nil {
			t.Fatalf("DeleteObject(%q): %v", testKey, err)
		}
		if _, _, err := fs.GetObject(ctx, testKey); !errors.Is(err, objectstore.ErrNotFound) {
			t.Errorf("GetObject(%q) after delete = %v, want ErrNotFound", testKey, err)
		}
		if _, err := fs.HeadObject(ctx, testKey); !errors.Is(err, objectstore.ErrNotFound) {
			t.Errorf("HeadObject(%q) after delete = %v, want ErrNotFound", testKey, err)
		}

		// DeleteObject does not prune: the shard directories stay (empty), which
		// is harmless and avoids racing a concurrent PutObject's MkdirAll.
		shard := filepath.Join(dir, "artifacts")
		if info, err := os.Stat(shard); err != nil || !info.IsDir() {
			t.Errorf("Stat(%q) after delete = (%v, %v), want the shard directory kept", shard, info, err)
		}
		if _, err := os.Stat(dir); err != nil {
			t.Errorf("Stat(root %q) after delete: %v, want root kept", dir, err)
		}
	})

	t.Run("MissingKeyIsNil", func(t *testing.T) {
		fs := objectstore.NewFSStore(t.TempDir())
		ctx := context.Background()

		if err := fs.DeleteObject(ctx, testKey); err != nil {
			t.Errorf("DeleteObject(%q) on empty store = %v, want nil", testKey, err)
		}

		// Idempotent: a second delete of a key that was just deleted is also nil.
		putContent(ctx, t, fs, testKey, "x")
		for i := range 2 {
			if err := fs.DeleteObject(ctx, testKey); err != nil {
				t.Errorf("DeleteObject(%q) call %d = %v, want nil", testKey, i+1, err)
			}
		}
	})

	t.Run("KeepsSiblingObjects", func(t *testing.T) {
		fs := objectstore.NewFSStore(t.TempDir())
		ctx := context.Background()
		// Same ab/cd shard directory as testKey, different file.
		other := testKey[:len(testKey)-1] + "0"
		putContent(ctx, t, fs, testKey, "target")
		putContent(ctx, t, fs, other, "bystander")

		if err := fs.DeleteObject(ctx, testKey); err != nil {
			t.Fatalf("DeleteObject(%q): %v", testKey, err)
		}
		requireContent(ctx, t, fs, other, "bystander")
	})

	t.Run("RejectsEscapingKeys", func(t *testing.T) {
		root := t.TempDir()
		ctx := context.Background()
		fs := objectstore.NewFSStore(root)

		// t.TempDir returns <per-test dir>/001, so the parent is private to this test.
		outside := filepath.Join(filepath.Dir(root), "outside.txt")
		// A sibling whose path has root's full path as a character-level prefix,
		// the case the trailing separator in resolve's boundary check exists for.
		sibling := root + "-evil"
		if err := os.MkdirAll(sibling, 0o755); err != nil {
			t.Fatalf("MkdirAll(%s): %v", sibling, err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(sibling) })
		siblingFile := filepath.Join(sibling, "secret.txt")

		const secret = "keep me"
		for _, p := range []string{outside, siblingFile} {
			if err := os.WriteFile(p, []byte(secret), 0o644); err != nil {
				t.Fatalf("WriteFile(%s): %v", p, err)
			}
		}
		putContent(ctx, t, fs, testKey, "inside")

		keys := []string{
			"",
			"../outside.txt",
			"../../etc/passwd",
			"../" + filepath.Base(root) + "-evil/secret.txt",
		}
		for _, key := range keys {
			if err := fs.DeleteObject(ctx, key); err == nil {
				t.Errorf("DeleteObject(%q) = nil, want an error (key escapes root)", key)
			}
		}

		for _, p := range []string{outside, siblingFile} {
			got, err := os.ReadFile(p)
			if err != nil {
				t.Fatalf("ReadFile(%s) after rejected deletes: %v", p, err)
			}
			if string(got) != secret {
				t.Errorf("file %s = %q, want %q", p, got, secret)
			}
		}
		requireContent(ctx, t, fs, testKey, "inside")
	})

	t.Run("PartitionByNamespace", func(t *testing.T) {
		root := t.TempDir()
		fs := objectstore.NewFSStore(root)
		fs.PartitionByNamespace = true
		ctxA := namespace.WithNamespace(context.Background(), "tenant-a")
		ctxB := namespace.WithNamespace(context.Background(), "tenant-b")
		putContent(ctxA, t, fs, testKey, "from a")
		putContent(ctxB, t, fs, testKey, "from b")

		if err := fs.DeleteObject(ctxA, testKey); err != nil {
			t.Fatalf("DeleteObject(%q) in tenant-a: %v", testKey, err)
		}
		if _, _, err := fs.GetObject(ctxA, testKey); !errors.Is(err, objectstore.ErrNotFound) {
			t.Errorf("GetObject(%q) in tenant-a after delete = %v, want ErrNotFound", testKey, err)
		}
		// Deleting in tenant-a leaves tenant-b's object and directory alone.
		requireContent(ctxB, t, fs, testKey, "from b")
		if _, err := os.Stat(filepath.Join(root, "ns", "tenant-b")); err != nil {
			t.Errorf("Stat(ns/tenant-b) after tenant-a delete: %v, want it kept", err)
		}

		// A namespace with no stored object deletes as a no-op.
		ctxC := namespace.WithNamespace(context.Background(), "tenant-c")
		if err := fs.DeleteObject(ctxC, testKey); err != nil {
			t.Errorf("DeleteObject(%q) in empty tenant-c = %v, want nil", testKey, err)
		}

		// An invalid namespace is rejected by resolve before anything is touched.
		bad := namespace.WithNamespace(context.Background(), "a:b")
		if err := fs.DeleteObject(bad, testKey); err == nil {
			t.Errorf("DeleteObject(%q) with namespace %q = nil, want an error", testKey, "a:b")
		}
		requireContent(ctxB, t, fs, testKey, "from b")
	})

	t.Run("RefusesToRemoveDirectory", func(t *testing.T) {
		root := t.TempDir()
		fs := objectstore.NewFSStore(root)
		ctx := context.Background()

		// A key that names an (empty) directory must not be removed: os.Remove
		// alone would delete it, so DeleteObject has to look at the entry first.
		dirPath := filepath.Join(root, filepath.FromSlash(testKey))
		if err := os.MkdirAll(dirPath, 0o755); err != nil {
			t.Fatalf("MkdirAll(%s): %v", dirPath, err)
		}
		if err := fs.DeleteObject(ctx, testKey); err == nil {
			t.Errorf("DeleteObject(%q) on a directory = nil, want an error", testKey)
		}
		if info, err := os.Stat(dirPath); err != nil || !info.IsDir() {
			t.Errorf("Stat(%s) after DeleteObject = (%v, %v), want the directory kept", dirPath, info, err)
		}
	})

	t.Run("ConcurrentPutAndDeleteSameKey", func(t *testing.T) {
		const (
			workers    = 8
			iterations = 200
		)
		fs := objectstore.NewFSStore(t.TempDir())
		ctx := context.Background()
		const content = "raced content"

		// All Puts and Deletes hit one shard directory, so a Delete's pruning
		// of empty directories keeps racing the Put's directory creation.
		start := make(chan struct{})
		var wg sync.WaitGroup
		for range workers {
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				for range iterations {
					_, err := fs.PutObject(ctx, testKey, strings.NewReader(content), int64(len(content)), objectstore.PutOptions{})
					if err != nil {
						t.Errorf("PutObject(%q) during concurrent deletes: %v", testKey, err)
						return
					}
				}
			}()
			go func() {
				defer wg.Done()
				<-start
				for range iterations {
					// Both a successful delete and a missing key return nil.
					if err := fs.DeleteObject(ctx, testKey); err != nil {
						t.Errorf("DeleteObject(%q) during concurrent puts: %v", testKey, err)
						return
					}
				}
			}()
		}
		close(start)
		wg.Wait()
	})
}
