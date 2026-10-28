//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"

	"github.com/xbcio/xflow/store/blob"
)

// blobE2ECollector is a Collector over the run's own index and store with the
// grace the backend was configured with: the pass the backend's GC loop makes,
// run by hand because that loop's clock and sleep cannot be reached from here.
func blobE2ECollector(t *testing.T, r *blobE2ERun) *blob.Collector {
	t.Helper()
	c, err := blob.NewCollector(r.index, r.store, blob.CollectorOptions{Grace: blobE2EGrace, Batch: 100})
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}
	return c
}

// blobE2EOnlyRef returns the one blob the run stored.
func blobE2EOnlyRef(t *testing.T, r *blobE2ERun) blob.Ref {
	t.Helper()
	refs := r.store.written()
	if len(refs) != 1 {
		t.Fatalf("blobs stored = %d, want exactly 1 (the items array)", len(refs))
	}
	return refs[0]
}

// blobE2EAssertReadable fails unless the blob can still be read from the real
// object store (the embedded store, so the read is not counted).
func blobE2EAssertReadable(t *testing.T, r *blobE2ERun, ref blob.Ref, when string) {
	t.Helper()
	data, err := r.store.Store.Get(context.Background(), r.ns, ref)
	if err != nil || len(data) == 0 {
		t.Fatalf("blob %s: Get = %d bytes, %v; want it readable", when, len(data), err)
	}
	if n := len(blobE2EObjects(r.dir)); n != 1 {
		t.Fatalf("blob %s: %d object file(s) on disk, want 1", when, n)
	}
}

// blobE2EGCPass records the collector pass made from inside a blob read.
type blobE2EGCPass struct {
	mu   sync.Mutex
	runs int
	res  blob.CollectResult
	err  error
}

func (p *blobE2EGCPass) record(res blob.CollectResult, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.runs++
	p.res, p.err = res, err
}

func (p *blobE2EGCPass) get() (int, blob.CollectResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.runs, p.res, p.err
}

// TestBlobE2EGCKeepsBlobDuringMapExecution: a collector pass runs while the map
// node's batches execute, with the clock two graces past the blob's creation, so
// the grace margin of a fresh blob no longer protects it and only its link to the
// execution does. The pass must find nothing to collect, and the run must finish.
func TestBlobE2EGCKeepsBlobDuringMapExecution(t *testing.T) {
	addr := requireRedis(t)
	r := newBlobE2ERun(t, addr, true)
	r.serve(t)
	col := blobE2ECollector(t, r)
	var pass blobE2EGCPass
	r.store.onFirstRead(func() {
		r.clock.advance(2 * blobE2EGrace)
		res, err := col.Run(context.Background())
		pass.record(res, err)
	})
	out := r.run(t, 7)

	blobE2EAssertSuccess(t, out)
	runs, res, err := pass.get()
	if runs != 1 || err != nil {
		t.Fatalf("collector passes during the map = %d, err %v; want exactly 1 without error", runs, err)
	}
	if res.Candidates != 0 || res.Deleted != 0 || res.Failed != 0 {
		t.Errorf("collector pass during the map = %+v, want nothing collected", res)
	}
	blobE2EAssertBlobOn(t, r, 5)
	blobE2EAssertReadable(t, r, blobE2EOnlyRef(t, r), "after the run")
}

// TestBlobE2EGCWaitsForLinkExpiry: after the execution finished through its batch
// tasks, the blob outlives the execution by its link, not by luck. Two hours on
// (the link runs 2*ttl + grace from creation) a pass collects nothing; once the
// link has expired the same pass deletes the blob. The second half is the
// control: it shows the collector in this test can delete, so the first half is
// a real "kept" and not a collector that never ran.
func TestBlobE2EGCWaitsForLinkExpiry(t *testing.T) {
	addr := requireRedis(t)
	r := newBlobE2ERun(t, addr, true)
	r.serve(t)
	out := r.run(t, 7)
	blobE2EAssertSuccess(t, out)
	ref := blobE2EOnlyRef(t, r)
	col := blobE2ECollector(t, r)
	ctx := context.Background()

	// Past the grace and past the execution's own TTL, but inside the link.
	r.clock.advance(blobE2EExecTTL + 2*blobE2EGrace)
	res, err := col.Run(ctx)
	if err != nil || res.Candidates != 0 || res.Deleted != 0 || res.PrunedLinks != 0 {
		t.Fatalf("pass before the link expires = %+v, %v; want nothing pruned, listed or deleted", res, err)
	}
	blobE2EAssertReadable(t, r, ref, "before the link expires")

	// Past the link: 2*ttl + grace from creation.
	r.clock.advance(blobE2EExecTTL + 2*blobE2EGrace)
	res, err = col.Run(ctx)
	if err != nil || res.PrunedLinks != 1 || res.Deleted != 1 || res.Failed != 0 {
		t.Fatalf("pass after the link expires = %+v, %v; want the link pruned and the blob deleted", res, err)
	}
	if n := len(blobE2EObjects(r.dir)); n != 0 {
		t.Fatalf("object files after the collection = %d, want 0", n)
	}
}
