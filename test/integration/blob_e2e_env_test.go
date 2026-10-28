//go:build integration

package integration

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store/blob"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/store/objectstore"
)

const (
	// blobE2EGrace is the collector grace the backend is configured with.
	blobE2EGrace = 30 * time.Second
	// blobE2EExecTTL is the execution TTL, so a blob's link outlives its
	// execution by 2*ttl + grace and a test can step the index clock across it.
	blobE2EExecTTL = time.Hour
	// blobE2EInlineMax is far below the encoded size of the 30 test items, so
	// every run with writing on externalizes them.
	blobE2EInlineMax = 256
	// blobE2EWait bounds every wait on an execution: a run that does not reach a
	// terminal state in this time is a defect, never a slow machine.
	blobE2EWait = 30 * time.Second
)

// blobE2EClock is the clock of the blob index: real time plus an offset a test
// moves forward to cross a link's expiry without sleeping.
type blobE2EClock struct {
	mu     sync.Mutex
	offset time.Duration
}

func (c *blobE2EClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Now().Add(c.offset)
}

func (c *blobE2EClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.offset += d
}

// blobE2ERun is one assembled process: a distributed backend on real Redis that
// only produces, a test-side asynq consumer, and the engine between them. The
// blob layer sits on an in-memory index and an object store rooted in a temp
// directory, and the database is the in-memory store, so no MySQL is involved.
type blobE2ERun struct {
	ns    namespace.Namespace
	addr  string
	rdb   *redis.Client
	b     *distributed.Backend
	eng   *engine.Engine
	cap   *dpCapture
	clock *blobE2EClock
	index *blob.MemoryIndex
	store *blobE2EStore // the store the backend writes blobs to
	dir   string        // directory the object store is rooted at

	mu       sync.Mutex
	holders  []string       // Redis keys that held the items marker at a probe
	probes   int            // probes taken
	attempts map[string]int // asynq task id -> times the handler was called
	failures []error        // errors the handler returned
}

// newBlobE2EBase builds what every blob scenario shares: a namespace of its own,
// the blob store over a temp-dir object store, a blob index on a controllable
// clock, and a distributed backend wired to them. consumer says whether the
// backend consumes its queues itself (the control plane's backend does) or only
// produces (the in-process runs, which consume through blobE2ERun.serve).
func newBlobE2EBase(t *testing.T, addr string, blobOn, consumer bool) *blobE2ERun {
	t.Helper()
	// Every run gets a namespace of its own. The engine keeps resolved items in a
	// process-wide cache keyed by namespace and reference, so two scenarios that
	// shared a namespace could serve each other's cached array and hide a read
	// failure the second one is meant to see.
	ns := namespace.Namespace("ns-" + uuid.NewString())
	clock := &blobE2EClock{}
	index := blob.NewMemoryIndex(clock.Now)
	dir := t.TempDir()
	inner := blob.NewObjectBacked(objectstore.NewFSStore(dir), blob.ObjectBackedOptions{Index: index})
	r := &blobE2ERun{
		ns: ns, addr: addr, clock: clock, index: index, dir: dir,
		store:    &blobE2EStore{Store: inner},
		rdb:      redis.NewClient(&redis.Options{Addr: addr}),
		attempts: map[string]int{},
	}
	r.cap = newDPCapture(r.rdb, ns)
	// dpRun.cleanup removes only the keys that carry this run's namespace.
	t.Cleanup(func() { (&dpRun{ns: ns}).cleanup(t, r.rdb) })
	b, err := distributed.New(addr, memstore.New(),
		distributed.WithConcurrency(1),
		distributed.WithConsumer(consumer),
		distributed.WithOwnedNamespaces([]namespace.Namespace{ns}),
		distributed.WithExecTTL(blobE2EExecTTL),
		distributed.WithBlobStore(distributed.BlobConfig{
			Store: r.store, Index: index, Enabled: blobOn,
			InlineMax: blobE2EInlineMax, Grace: blobE2EGrace, Batch: 100,
		}),
	)
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	r.b = b
	return r
}
