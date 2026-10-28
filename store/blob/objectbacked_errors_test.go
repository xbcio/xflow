package blob_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store/blob"
	"github.com/xbcio/xflow/store/objectstore"
)

// onlyStore hides every optional method of the Store it wraps, in particular
// DeleteObject.
type onlyStore struct{ objectstore.Store }

func TestObjectBacked_Errors(t *testing.T) {
	ctx := context.Background()
	const ns namespace.Namespace = "errors"

	t.Run("NotFoundIsMapped", func(t *testing.T) {
		s := blob.NewObjectBacked(objectstore.NewFSStore(t.TempDir()), blob.ObjectBackedOptions{})
		ref, _ := blob.NewMemory().Put(ctx, ns, []byte("never stored here"))
		if _, err := s.Get(ctx, ns, ref); !errors.Is(err, blob.ErrNotFound) {
			t.Fatalf("Get of a missing blob error = %v, want blob.ErrNotFound", err)
		}
	})

	t.Run("TooLarge", func(t *testing.T) {
		s := blob.NewObjectBacked(objectstore.NewFSStore(t.TempDir()), blob.ObjectBackedOptions{MaxBytes: 16})
		if _, err := s.Put(ctx, ns, make([]byte, 17)); !errors.Is(err, blob.ErrTooLarge) {
			t.Errorf("Put(17 bytes) with MaxBytes 16 error = %v, want ErrTooLarge", err)
		}
		if _, err := s.Put(ctx, ns, make([]byte, 16)); err != nil {
			t.Errorf("Put(16 bytes) with MaxBytes 16 error = %v, want nil", err)
		}
		ref := blob.Ref{Namespace: string(ns), Digest: string(bytes.Repeat([]byte("a"), 64)), Size: 17, Stored: 17}
		if _, err := s.Get(ctx, ns, ref); !errors.Is(err, blob.ErrTooLarge) {
			t.Errorf("Get with ref.Size 17 and MaxBytes 16 error = %v, want ErrTooLarge", err)
		}
	})

	t.Run("DeleteLifecycle", func(t *testing.T) {
		s := blob.NewObjectBacked(objectstore.NewFSStore(t.TempDir()), blob.ObjectBackedOptions{})
		ref, err := s.Put(ctx, ns, []byte("to delete"))
		if err != nil {
			t.Fatalf("Put error = %v", err)
		}
		for i := 1; i <= 2; i++ { // The second Delete is a repeat and must also succeed.
			if err := s.Delete(ctx, ns, ref); err != nil {
				t.Fatalf("Delete #%d error = %v, want nil", i, err)
			}
		}
		if _, err := s.Get(ctx, ns, ref); !errors.Is(err, blob.ErrNotFound) {
			t.Fatalf("Get after Delete error = %v, want ErrNotFound", err)
		}
	})

	t.Run("DeleteUnsupported", func(t *testing.T) {
		s := blob.NewObjectBacked(onlyStore{objectstore.NewFSStore(t.TempDir())}, blob.ObjectBackedOptions{})
		ref, err := s.Put(ctx, ns, []byte("cannot delete"))
		if err != nil {
			t.Fatalf("Put error = %v", err)
		}
		if err := s.Delete(ctx, ns, ref); !errors.Is(err, blob.ErrDeleteUnsupported) {
			t.Fatalf("Delete on a non-Deleter error = %v, want ErrDeleteUnsupported", err)
		}
	})
}

func TestObjectBacked_NamespaceFilesAreDistinct(t *testing.T) {
	ctx := context.Background()
	t.Run("SameContentTwoNamespaces", func(t *testing.T) {
		dir := t.TempDir()
		s := blob.NewObjectBacked(objectstore.NewFSStore(dir), blob.ObjectBackedOptions{})
		data := []byte("same content, two namespaces")
		for _, ns := range []namespace.Namespace{"Tenant", "tenant"} {
			if _, err := s.Put(ctx, ns, data); err != nil {
				t.Fatalf("Put(%q) error = %v", ns, err)
			}
		}
		var files []string
		err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() {
				files = append(files, path)
			}
			return err
		})
		if err != nil {
			t.Fatalf("WalkDir error = %v", err)
		}
		if len(files) != 2 {
			t.Fatalf("two namespaces storing the same content left %d files %v, want 2", len(files), files)
		}
	})
}
