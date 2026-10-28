package blobtest

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store/blob"
	"github.com/xbcio/xflow/store/objectstore"
)

// recordingFS is a FSStore that remembers the key of the last object written.
// The scenarios need that key to damage or remove the bytes behind a Ref, and
// blobtest must not duplicate the package's key layout to compute it.
type recordingFS struct {
	*objectstore.FSStore
	mu      sync.Mutex
	lastKey string
}

func (r *recordingFS) PutObject(ctx context.Context, key string, body io.Reader, size int64, opts objectstore.PutOptions) (*objectstore.Object, error) {
	r.mu.Lock()
	r.lastKey = key
	r.mu.Unlock()
	return r.FSStore.PutObject(ctx, key, body, size, opts)
}

// keyOfLastPut returns the object key of the most recent PutObject call.
func (r *recordingFS) keyOfLastPut() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastKey
}

// gcRig is the whole stack a collector works on: a real ObjectBacked over a real
// FSStore, registering with the Index under test, and a Collector deleting
// through the same Index. Only the Index varies between runs, so the same
// scenarios exercise the memory, SQLite, and MySQL implementations.
type gcRig struct {
	t     *testing.T
	h     IndexHarness
	hook  *HookIndex
	fs    *recordingFS
	store *blob.ObjectBacked
	gc    *blob.Collector
}

// newGCRig builds a rig whose collector examines at most batch candidates per
// Run. Compression is on so both frame types are covered by every scenario.
func newGCRig(t *testing.T, h IndexHarness, batch int) *gcRig {
	t.Helper()
	hook := &HookIndex{Index: h.Index}
	fs := &recordingFS{FSStore: objectstore.NewFSStore(t.TempDir())}
	store := blob.NewObjectBacked(fs, blob.ObjectBackedOptions{Compress: true, Index: hook})
	gc, err := blob.NewCollector(hook, store, blob.CollectorOptions{Grace: grace, Batch: batch})
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}
	return &gcRig{t: t, h: h, hook: hook, fs: fs, store: store, gc: gc}
}

// compressible returns content long enough, and repetitive enough, that the
// store writes it as a zstd frame.
func compressible(label string) []byte {
	return []byte(strings.Repeat("compressible "+label+" ", 200))
}

// put stores data and fails the test on error.
func (r *gcRig) put(data []byte) blob.Ref {
	r.t.Helper()
	ref, err := r.store.Put(context.Background(), idxNS, data)
	if err != nil {
		r.t.Fatalf("Put(%d bytes) error = %v", len(data), err)
	}
	return ref
}

// wantReadable fails unless ref reads back as exactly data.
func (r *gcRig) wantReadable(why string, ref blob.Ref, data []byte) {
	r.t.Helper()
	got, err := r.store.Get(context.Background(), namespace.Namespace(ref.Namespace), ref)
	if err != nil {
		r.t.Fatalf("%s: Get(%s) error = %v, want the bytes", why, ref.Digest[:8], err)
	}
	if string(got) != string(data) {
		r.t.Fatalf("%s: Get(%s) returned %d different bytes, want %d", why, ref.Digest[:8], len(got), len(data))
	}
}

// wantGone fails unless the bytes ref names have been deleted.
func (r *gcRig) wantGone(why string, ref blob.Ref) {
	r.t.Helper()
	if _, err := r.store.Get(context.Background(), namespace.Namespace(ref.Namespace), ref); !errors.Is(err, blob.ErrNotFound) {
		r.t.Fatalf("%s: Get(%s) error = %v, want ErrNotFound", why, ref.Digest[:8], err)
	}
}

// run performs one collection pass and fails the test on error.
func (r *gcRig) run() blob.CollectResult {
	r.t.Helper()
	res, err := r.gc.Run(context.Background())
	if err != nil {
		r.t.Fatalf("Collector.Run error = %v (result %+v)", err, res)
	}
	return res
}

// age moves the clock far enough that anything untouched so far is past grace.
func (r *gcRig) age() {
	r.h.Clock.Advance(grace + time.Millisecond)
}
