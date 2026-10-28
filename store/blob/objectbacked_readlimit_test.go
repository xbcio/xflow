package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"github.com/xbcio/xflow/store/objectstore"
)

// countingStore counts the bytes its GetObject readers hand out.
type countingStore struct {
	*objectstore.FSStore
	read atomic.Int64
}

func (c *countingStore) GetObject(ctx context.Context, key string) (io.ReadCloser, *objectstore.Object, error) {
	rc, obj, err := c.FSStore.GetObject(ctx, key)
	if err != nil {
		return nil, nil, err
	}
	return &countingReader{ReadCloser: rc, n: &c.read}, obj, nil
}

type countingReader struct {
	io.ReadCloser
	n *atomic.Int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.n.Add(int64(n))
	return n, err
}

// TestObjectBacked_ReadIsBounded plants a 48 MiB object under a Ref that
// promises only a few bytes. Get must refuse it after reading no more than the
// frame limit plus one byte, instead of pulling the whole object into memory.
func TestObjectBacked_ReadIsBounded(t *testing.T) {
	const junkSize = 48 << 20
	junk := make([]byte, junkSize) // The zero header byte is a valid raw frame header.

	for _, refSize := range []int{10, 64 << 10} {
		t.Run(fmt.Sprintf("RefSize%d", refSize), func(t *testing.T) {
			fs := objectstore.NewFSStore(t.TempDir())
			counter := &countingStore{FSStore: fs}
			f := &corruptionFixture{t: t, fs: fs, store: NewObjectBacked(counter, ObjectBackedOptions{}), ns: "bounded"}
			ref := f.refFor(make([]byte, refSize))
			f.plant(ref, junk)

			got, err := f.store.Get(context.Background(), f.ns, ref)
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("Get of a %d byte object under ref size %d = (%d bytes, %v), want ErrCorrupt", junkSize, refSize, len(got), err)
			}
			if limit, read := frameReadLimit(ref.Size)+1, counter.read.Load(); read > limit {
				t.Errorf("Get read %d bytes of a %d byte object under ref size %d, want at most %d", read, junkSize, refSize, limit)
			}
		})
	}
}
