package blob

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/xbcio/xflow/store/objectstore"
)

// racyStore answers its first failPuts PutObject calls with
// ErrPreconditionFailed, as if the object existed, while HeadObject (not
// overridden) reports it missing: the object was deleted in between.
type racyStore struct {
	*objectstore.FSStore
	failPuts int
	puts     int
}

func (r *racyStore) PutObject(ctx context.Context, key string, body io.Reader, size int64, opts objectstore.PutOptions) (*objectstore.Object, error) {
	r.puts++
	if r.puts <= r.failPuts {
		return nil, objectstore.ErrPreconditionFailed
	}
	return r.FSStore.PutObject(ctx, key, body, size, opts)
}

func TestObjectBacked_DuplicatePut(t *testing.T) {
	ctx := context.Background()
	data := bytes.Repeat([]byte("duplicate "), 512)

	// headSize returns the size of the object stored for data.
	headSize := func(t *testing.T, f *corruptionFixture) int64 {
		t.Helper()
		key, err := objectKey(f.ns, f.refFor(data).Digest)
		if err != nil {
			t.Fatalf("objectKey error = %v", err)
		}
		info, err := f.fs.HeadObject(ctx, key)
		if err != nil {
			t.Fatalf("HeadObject(%q) error = %v", key, err)
		}
		return info.Size
	}

	t.Run("PlausibleObjectIsNotReadBack", func(t *testing.T) {
		counter := &countingStore{FSStore: objectstore.NewFSStore(t.TempDir())}
		s := NewObjectBacked(counter, ObjectBackedOptions{})
		first, err := s.Put(ctx, "dup", data)
		if err != nil {
			t.Fatalf("first Put error = %v", err)
		}
		second, err := s.Put(ctx, "dup", data)
		if err != nil || second != first {
			t.Fatalf("repeat Put = (%+v, %v), want (%+v, nil)", second, err, first)
		}
		if n := counter.read.Load(); n != 0 {
			t.Errorf("repeat Put read %d bytes of the existing object, want 0", n)
		}
	})

	for name, existing := range map[string][]byte{
		"EmptyObject":     nil,
		"OversizedObject": make([]byte, frameReadLimit(int64(len(data)))+1),
	} {
		t.Run(name, func(t *testing.T) {
			f := newCorruptionFixture(t)
			f.plant(f.refFor(data), existing)
			if _, err := f.store.Put(ctx, f.ns, data); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("Put over an existing %d byte object error = %v, want ErrCorrupt", len(existing), err)
			}
			if got := headSize(t, f); got != int64(len(existing)) {
				t.Errorf("existing object is now %d bytes, want it left at %d", got, len(existing))
			}
		})
	}

	t.Run("ObjectDeletedBeforeCheck", func(t *testing.T) {
		racy := &racyStore{FSStore: objectstore.NewFSStore(t.TempDir()), failPuts: 1}
		s := NewObjectBacked(racy, ObjectBackedOptions{})
		ref, err := s.Put(ctx, "dup", data)
		if err != nil {
			t.Fatalf("Put error = %v, want the retry to succeed", err)
		}
		if racy.puts != 2 {
			t.Errorf("PutObject calls = %d, want 2 (failed once, retried once)", racy.puts)
		}
		if got, err := s.Get(ctx, "dup", ref); err != nil || !bytes.Equal(got, data) {
			t.Errorf("Get after the retried Put = (%d bytes, %v), want the original %d bytes", len(got), err, len(data))
		}
	})

	t.Run("ObjectKeepsDisappearing", func(t *testing.T) {
		racy := &racyStore{FSStore: objectstore.NewFSStore(t.TempDir()), failPuts: 5}
		s := NewObjectBacked(racy, ObjectBackedOptions{})
		if _, err := s.Put(ctx, "dup", data); !errors.Is(err, objectstore.ErrNotFound) {
			t.Fatalf("Put error = %v, want it to wrap objectstore.ErrNotFound", err)
		}
		if racy.puts != 2 {
			t.Errorf("PutObject calls = %d, want it to stop after 2", racy.puts)
		}
	})
}
