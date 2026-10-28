package distributed

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/blob"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/store/objectstore"
)

// countingIndex counts the calls that reach the lifecycle index, which is how a
// test proves the blob layer never touched it.
type countingIndex struct {
	blob.Index
	registers, links atomic.Int64
}

func (i *countingIndex) Register(ctx context.Context, ref blob.Ref) (blob.Registration, error) {
	i.registers.Add(1)
	return i.Index.Register(ctx, ref)
}

func (i *countingIndex) Link(ctx context.Context, execID string, ref blob.Ref, expireAt time.Time) error {
	i.links.Add(1)
	return i.Index.Link(ctx, execID, ref, expireAt)
}

// blobTestObserver records what the blob layer reports.
type blobTestObserver struct {
	mu                    sync.Mutex
	puts, gets            int
	gcCalls, gcDel, gcFen int
}

func (o *blobTestObserver) OnPut(string) { o.mu.Lock(); o.puts++; o.mu.Unlock() }
func (o *blobTestObserver) OnGet(string) { o.mu.Lock(); o.gets++; o.mu.Unlock() }
func (o *blobTestObserver) OnGC(deleted, fenced int) {
	o.mu.Lock()
	o.gcCalls, o.gcDel, o.gcFen = o.gcCalls+1, o.gcDel+deleted, o.gcFen+fenced
	o.mu.Unlock()
}

// blobWaitBound only turns a hang into a failure; a passing test never waits
// for it.
const blobWaitBound = 5 * time.Second

// fakeGC replaces buildBlobGC so a test can drive the collection loop without a
// real Collector. runs counts passes, builds counts constructions, running is
// the number of passes in flight, and lastCtx is the context of the latest pass.
type fakeGC struct {
	builds, runs, running atomic.Int64
	lastCtx               atomic.Pointer[context.Context]
	gotCfg                atomic.Pointer[BlobConfig]
	buildErr              error
	result                blob.CollectResult
	// blockAt, when positive, makes that pass wait for its context to end and
	// then call onCancel, so a test can look at the world at the moment the loop
	// is told to stop.
	blockAt  int64
	onCancel func()
}

func installFakeGC(t *testing.T, g *fakeGC) *fakeGC {
	t.Helper()
	old := buildBlobGC
	t.Cleanup(func() { buildBlobGC = old })
	buildBlobGC = func(cfg BlobConfig) (func(context.Context) (blob.CollectResult, error), error) {
		g.builds.Add(1)
		g.gotCfg.Store(&cfg)
		if g.buildErr != nil {
			return nil, g.buildErr
		}
		return func(ctx context.Context) (blob.CollectResult, error) {
			n := g.runs.Add(1)
			g.running.Add(1)
			defer g.running.Add(-1)
			g.lastCtx.Store(&ctx)
			if g.blockAt > 0 && n == g.blockAt {
				<-ctx.Done()
				if g.onCancel != nil {
					g.onCancel()
				}
			}
			return g.result, nil
		}, nil
	}
	return g
}

// blobTestSetup returns a valid config over a file-backed blob store whose Index
// is a counting wrapper, so a test can see whether the blob layer was touched.
// Writing is off, as it is by default.
func blobTestSetup(t *testing.T) (BlobConfig, *countingIndex) {
	t.Helper()
	idx := &countingIndex{Index: blob.NewMemoryIndex(nil)}
	backing := blob.NewObjectBacked(objectstore.NewFSStore(t.TempDir()), blob.ObjectBackedOptions{Index: idx})
	return BlobConfig{Store: backing, Index: idx, Grace: time.Minute, Batch: 10}, idx
}

// newBlobBackend builds a backend over miniredis, a memstore and stub with cfg.
func newBlobBackend(t *testing.T, stub *stubTransport, cfg BlobConfig, opts ...Option) *Backend {
	t.Helper()
	rdb := newRedisStateTestClient(t)
	all := append([]Option{WithTransport(stub), WithBlobStore(cfg)}, opts...)
	b, err := New(rdb.Options().Addr, memstore.New(), all...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return b
}

// bigItems is an items array whose encoding is well above the default InlineMax.
func bigItems() []any {
	items := make([]any, 3000)
	for i := range items {
		items[i] = map[string]any{"id": float64(i), "name": "item-padding-padding-padding"}
	}
	return items
}

// Mutation note: dropping the db == nil check from validateBlobSelection must
// turn this red. The check runs before any connection, so the error cannot come
// from the Redis stage and miniredis must see no connection at all.
func TestNewRejectsBlobWithoutDB(t *testing.T) {
	mr := newMiniredis(t)
	cfg, _ := blobTestSetup(t)
	b, err := New(mr.Addr(), nil, WithTransport(&stubTransport{}), WithBlobStore(cfg))
	if err == nil {
		_ = b.rdb.Close()
		t.Fatal("New() = nil error, want WithBlobStore without a store.Store rejected")
	}
	if strings.Contains(err.Error(), "redis") {
		t.Fatalf("error = %q, want a rejection before Redis is contacted", err)
	}
	if got := mr.TotalConnectionCount(); got != 0 {
		t.Fatalf("redis saw %d connection(s), want 0", got)
	}
}

// Mutation note: deleting any one case from validateBlobSelection must turn its
// row red, and merging two cases into one message must trip the distinct-text
// check. The negative numbers are distinctive so a message that echoed a
// configured value would be caught.
func TestValidateBlobSelectionRejectsEachBadField(t *testing.T) {
	good, _ := blobTestSetup(t)
	cases := []struct {
		name   string
		field  string // rows of one field may share a message; fields may not
		db     bool
		mutate func(*BlobConfig)
		want   string
		secret string
	}{
		{"nil db", "db", false, func(*BlobConfig) {}, "store.Store", ""},
		{"nil store", "Store", true, func(c *BlobConfig) { c.Store = nil }, "Store is required", ""},
		{"nil index", "Index", true, func(c *BlobConfig) { c.Index = nil }, "Index is required", ""},
		{"negative inline max", "InlineMax", true, func(c *BlobConfig) { c.InlineMax = -4242 }, "InlineMax", "4242"},
		{"zero grace", "Grace", true, func(c *BlobConfig) { c.Grace = 0 }, "Grace", ""},
		{"negative grace", "Grace", true, func(c *BlobConfig) { c.Grace = -7777 * time.Second }, "Grace", "7777"},
		{"zero batch", "Batch", true, func(c *BlobConfig) { c.Batch = 0 }, "Batch", ""},
		{"negative batch", "Batch", true, func(c *BlobConfig) { c.Batch = -3131 }, "Batch", "3131"},
		{"negative gc interval", "GCInterval", true, func(c *BlobConfig) { c.GCInterval = -5151 * time.Second }, "GCInterval", "5151"},
	}
	byField := map[string]string{} // field -> message
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := good
			tc.mutate(&cfg)
			var db store.Store
			if tc.db {
				db = memstore.New()
			}
			err := validateBlobSelection(&config{blob: &cfg}, db)
			if err == nil {
				t.Fatal("validateBlobSelection = nil, want an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to name %q", err, tc.want)
			}
			if tc.secret != "" && strings.Contains(err.Error(), tc.secret) {
				t.Fatalf("error = %q, must not repeat the configured value", err)
			}
			if prev, ok := byField[tc.field]; ok && prev != err.Error() {
				t.Fatalf("field %s gave %q and %q, want one message per field", tc.field, prev, err)
			}
			byField[tc.field] = err.Error()
		})
	}
	distinct := map[string]string{}
	for field, msg := range byField {
		if other, dup := distinct[msg]; dup {
			t.Fatalf("fields %s and %s share the message %q, want distinct messages", other, field, msg)
		}
		distinct[msg] = field
	}
	if err := validateBlobSelection(&config{}, nil); err != nil {
		t.Fatalf("validateBlobSelection with no blob = %v, want nil", err)
	}
	if err := validateBlobSelection(&config{blob: &good}, memstore.New()); err != nil {
		t.Fatalf("validateBlobSelection with a valid config = %v, want nil", err)
	}
}

// externalizeVia asks the backend's state store, through the interface the
// engine asserts on, to externalize a large items array.
func externalizeVia(t *testing.T, b *Backend, execID string) (bool, error) {
	t.Helper()
	items, ok := b.State().(engine.BatchItemsStore)
	if !ok {
		t.Fatal("backend state store does not implement engine.BatchItemsStore")
	}
	_, externalized, err := items.ExternalizeItems(context.Background(), execID, bigItems())
	return externalized, err
}

// Mutation note: making New configure the blob layer when no option is given
// must turn this red, and so must starting the collector without one.
func TestNewWithoutBlobConfiguresNothing(t *testing.T) {
	fake := installFakeGC(t, &fakeGC{})
	stub := &stubTransport{}
	b := newTestBackend(t, stub)
	if externalized, err := externalizeVia(t, b, "exec-none"); externalized || err != nil {
		t.Fatalf("ExternalizeItems = (%v, %v), want (false, nil) without WithBlobStore", externalized, err)
	}
	stop, err := b.StartBinding(engine.New(b.State(), b.Queue()))
	if err != nil {
		t.Fatalf("StartBinding: %v", err)
	}
	stop()
	if got := fake.builds.Load(); got != 0 {
		t.Fatalf("buildBlobGC was called %d time(s), want 0 without WithBlobStore", got)
	}
}

// Mutation note: forcing Enabled to true anywhere between WithBlobStore and
// ConfigureBlob must turn the first half red; ignoring Enabled in the second
// half must turn that red.
func TestRefWritesDefaultOff(t *testing.T) {
	cfg, idx := blobTestSetup(t)
	if cfg.Enabled {
		t.Fatal("test setup must leave Enabled unset")
	}
	off := newBlobBackend(t, &stubTransport{}, cfg)
	if externalized, err := externalizeVia(t, off, "exec-off"); externalized || err != nil {
		t.Fatalf("ExternalizeItems = (%v, %v), want (false, nil) while Enabled is false", externalized, err)
	}
	if n := idx.registers.Load() + idx.links.Load(); n != 0 {
		t.Fatalf("the index saw %d call(s) while writing was off, want 0", n)
	}

	cfg.Enabled = true
	on := newBlobBackend(t, &stubTransport{}, cfg)
	externalized, err := externalizeVia(t, on, "exec-on")
	if err != nil || !externalized {
		t.Fatalf("ExternalizeItems = (%v, %v), want (true, nil) once Enabled is true", externalized, err)
	}
	if idx.registers.Load() == 0 || idx.links.Load() == 0 {
		t.Fatalf("registers=%d links=%d, want the blob registered and linked", idx.registers.Load(), idx.links.Load())
	}
}

// Mutation note: starting the collector before the consumer or never stopping it
// must turn this red. The pass that is in flight when stop is called records the
// world at the moment its context ends: the consumer must already be stopped (the
// collector is released second) and the transport still open (resources are
// released last).
func TestBlobGCStartsAfterOutboxAndStopsBeforeIt(t *testing.T) {
	stub := &stubTransport{}
	var consumerStopped, transportClosed atomic.Bool
	fake := installFakeGC(t, &fakeGC{blockAt: 2, result: blob.CollectResult{Deleted: 2, Fenced: 1}})
	fake.onCancel = func() {
		consumerStopped.Store(stub.stopped.Load())
		transportClosed.Store(stub.closed.Load())
	}
	obs := &blobTestObserver{}
	cfg, _ := blobTestSetup(t)
	cfg.GCInterval, cfg.Observer = time.Millisecond, obs
	b := newBlobBackend(t, stub, cfg)
	before := runtime.NumGoroutine()

	stop, err := b.StartBinding(engine.New(b.State(), b.Queue()))
	if err != nil {
		t.Fatalf("StartBinding: %v", err)
	}
	waitUntil(t, blobWaitBound, "the second pass to be in flight", func() bool {
		return fake.runs.Load() >= 2 && fake.running.Load() == 1
	})
	if stub.stopped.Load() || stub.closed.Load() {
		t.Fatal("the consumer or transport was stopped while the collector was still running")
	}
	stop()

	if got := fake.running.Load(); got != 0 {
		t.Fatalf("%d pass(es) still running after stop returned, want 0", got)
	}
	if !consumerStopped.Load() || transportClosed.Load() {
		t.Fatalf("at cancel: consumer stopped=%v, transport closed=%v, want true and false",
			consumerStopped.Load(), transportClosed.Load())
	}
	runsAtStop := fake.runs.Load()
	waitUntil(t, blobWaitBound, "the collector goroutine to exit", func() bool { return runtime.NumGoroutine() <= before })
	if got := fake.runs.Load(); got != runsAtStop {
		t.Fatalf("runs went from %d to %d after stop, want no more passes", runsAtStop, got)
	}
	obs.mu.Lock()
	defer obs.mu.Unlock()
	if obs.gcCalls == 0 || obs.gcDel != 2*obs.gcCalls || obs.gcFen != obs.gcCalls {
		t.Fatalf("observer saw %d pass(es) deleting %d and fencing %d, want 2 deleted and 1 fenced each",
			obs.gcCalls, obs.gcDel, obs.gcFen)
	}
}

// Mutation note: dropping stopGC from the afterOutboxStart rollback must turn
// this red. The hook waits until a pass is blocked in flight, so the loop is
// provably alive when the rollback runs, and the pass only ends when its context
// is canceled.
func TestBindRollbackStopsBlobGC(t *testing.T) {
	stub := &stubTransport{}
	fake := installFakeGC(t, &fakeGC{blockAt: 1})
	cfg, _ := blobTestSetup(t)
	cfg.GCInterval = time.Millisecond
	b := newBlobBackend(t, stub, cfg)
	b.testHooks.afterOutboxStart = func() error {
		waitUntil(t, blobWaitBound, "a pass to be in flight", func() bool { return fake.running.Load() == 1 })
		return errors.New("monitor boom")
	}
	before := runtime.NumGoroutine()

	stop, err := b.StartBinding(engine.New(b.State(), b.Queue()))
	if err == nil || stop != nil {
		t.Fatalf("StartBinding = (%v, %v), want a nil stop and the injected error", stop != nil, err)
	}
	if got := fake.running.Load(); got != 0 {
		t.Fatalf("%d collection pass(es) still running after the failed bind, want 0", got)
	}
	if !stub.stopped.Load() || !stub.closed.Load() {
		t.Fatalf("consumer stopped=%v, transport closed=%v, want both after rollback", stub.stopped.Load(), stub.closed.Load())
	}
	assertRDBClosed(t, b)
	waitUntil(t, blobWaitBound, "the collector goroutine to exit", func() bool { return runtime.NumGoroutine() <= before })
}

// Mutation note: starting the collector on the non-consumer path must turn each
// row red. API-only instances still configure the blob layer, because they
// produce the items, so writing works there with no collector running.
func TestNonConsumerStartsNoBlobGC(t *testing.T) {
	entries := map[string]func(*Backend, *engine.Engine) (func(), error){
		"Bind":         func(b *Backend, eng *engine.Engine) (func(), error) { return b.Bind(eng), nil },
		"StartBinding": func(b *Backend, eng *engine.Engine) (func(), error) { return b.StartBinding(eng) },
		"bindHandler": func(b *Backend, eng *engine.Engine) (func(), error) {
			return b.bindHandler(eng, func(context.Context, *engine.Task) error { return nil }, true)
		},
	}
	for name, enter := range entries {
		t.Run(name, func(t *testing.T) {
			fake := installFakeGC(t, &fakeGC{})
			cfg, _ := blobTestSetup(t)
			cfg.Enabled, cfg.GCInterval = true, time.Millisecond
			b := newBlobBackend(t, &stubTransport{}, cfg, WithConsumer(false))
			if externalized, err := externalizeVia(t, b, "exec-api"); err != nil || !externalized {
				t.Fatalf("ExternalizeItems = (%v, %v), want (true, nil) on an API-only backend", externalized, err)
			}
			stop, err := enter(b, engine.New(b.State(), b.Queue()))
			if err != nil {
				t.Fatalf("binding: %v", err)
			}
			stop()
			if got := fake.builds.Load(); got != 0 {
				t.Fatalf("buildBlobGC was called %d time(s) on a non-consumer backend, want 0", got)
			}
			if got := fake.runs.Load(); got != 0 {
				t.Fatalf("%d collection pass(es) ran on a non-consumer backend, want 0", got)
			}
		})
	}
}

// Mutation note: leaving the error unchecked in bindHandler, or skipping the
// outbox/consumer release on this path, must turn this red. The outbox
// dispatcher has no stop signal of its own, so the goroutine count is what shows
// it was awaited.
func TestBuildBlobGCFailureRollsBack(t *testing.T) {
	stub := &stubTransport{}
	fake := installFakeGC(t, &fakeGC{buildErr: errors.New("collector boom")})
	cfg, _ := blobTestSetup(t)
	b := newBlobBackend(t, stub, cfg)
	before := runtime.NumGoroutine()

	stop, err := b.StartBinding(engine.New(b.State(), b.Queue()))
	if err == nil || stop != nil {
		t.Fatalf("StartBinding = (%v, %v), want a nil stop and an error", stop != nil, err)
	}
	if !strings.Contains(err.Error(), "distributed: start blob gc:") || !strings.Contains(err.Error(), "collector boom") {
		t.Fatalf("error = %q, want the %q prefix wrapping the cause", err, "distributed: start blob gc:")
	}
	if fake.builds.Load() != 1 || fake.runs.Load() != 0 {
		t.Fatalf("builds=%d runs=%d, want 1 and 0", fake.builds.Load(), fake.runs.Load())
	}
	if !stub.stopped.Load() || !stub.closed.Load() {
		t.Fatalf("consumer stopped=%v, transport closed=%v, want both after rollback", stub.stopped.Load(), stub.closed.Load())
	}
	assertRDBClosed(t, b)
	waitUntil(t, blobWaitBound, "the outbox dispatcher goroutine to exit", func() bool { return runtime.NumGoroutine() <= before })
}

// Mutation note: filling Grace or Batch with a default, or any default for
// Enabled, must turn this red; so must dropping the InlineMax default.
func TestBlobDefaultsAreFilledOnce(t *testing.T) {
	fake := installFakeGC(t, &fakeGC{})
	cfg, _ := blobTestSetup(t)
	cfg.Grace, cfg.Batch = 91*time.Second, 17
	b := newBlobBackend(t, &stubTransport{}, cfg)
	stop, err := b.StartBinding(engine.New(b.State(), b.Queue()))
	if err != nil {
		t.Fatalf("StartBinding: %v", err)
	}
	stop()
	got := fake.gotCfg.Load()
	if got == nil {
		t.Fatal("buildBlobGC was never called")
	}
	if got.InlineMax != 64<<10 || got.Enabled || got.Grace != 91*time.Second || got.Batch != 17 {
		t.Fatalf("collector config = InlineMax %d, Enabled %v, Grace %s, Batch %d; want 65536, false, 1m31s, 17",
			got.InlineMax, got.Enabled, got.Grace, got.Batch)
	}
}
