package rstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store/blob"
	"github.com/xbcio/xflow/store/objectstore"
	"github.com/xbcio/xflow/types"
)

const (
	blobTestTTL   = time.Minute
	blobTestGrace = 10 * time.Second
)

// blobClock is the one clock the Store's link expiry and the MemoryIndex share,
// so a test can cross a retention boundary without sleeping.
type blobClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *blobClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *blobClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// blobLinkRecord is one Index.Link call the Store made.
type blobLinkRecord struct {
	ExecID   string
	Ref      blob.Ref
	ExpireAt time.Time
}

// blobRecordingIndex is the Index handed to the Store. It remembers every Link
// that reached it and can be told to fail them, while the MemoryIndex underneath
// keeps the real lifecycle state.
type blobRecordingIndex struct {
	blob.Index
	mu       sync.Mutex
	links    []blobLinkRecord
	attempts int
	linkErr  error
}

func (i *blobRecordingIndex) Link(ctx context.Context, execID string, ref blob.Ref, expireAt time.Time) error {
	i.mu.Lock()
	i.attempts++
	if i.linkErr != nil {
		err := i.linkErr
		i.mu.Unlock()
		return err
	}
	i.links = append(i.links, blobLinkRecord{ExecID: execID, Ref: ref, ExpireAt: expireAt})
	i.mu.Unlock()
	return i.Index.Link(ctx, execID, ref, expireAt)
}

func (i *blobRecordingIndex) recorded() []blobLinkRecord {
	i.mu.Lock()
	defer i.mu.Unlock()
	return append([]blobLinkRecord(nil), i.links...)
}

func (i *blobRecordingIndex) linkAttempts() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.attempts
}

// blobCountingStore counts the calls that reach the blob Store, which is how a
// test proves the disabled path never touches it.
type blobCountingStore struct {
	blob.Store
	puts, gets atomic.Int64
	putErr     error
	getErr     error
}

func (s *blobCountingStore) Put(ctx context.Context, ns namespace.Namespace, data []byte) (blob.Ref, error) {
	s.puts.Add(1)
	if s.putErr != nil {
		return blob.Ref{}, s.putErr
	}
	return s.Store.Put(ctx, ns, data)
}

func (s *blobCountingStore) Get(ctx context.Context, ns namespace.Namespace, ref blob.Ref) ([]byte, error) {
	s.gets.Add(1)
	if s.getErr != nil {
		return nil, s.getErr
	}
	return s.Store.Get(ctx, ns, ref)
}

// blobResultObserver counts the results Policy reports, which is how a test
// proves a refusal was recorded exactly once.
type blobResultObserver struct {
	mu   sync.Mutex
	puts map[string]int
}

func (o *blobResultObserver) OnPut(result string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.puts == nil {
		o.puts = map[string]int{}
	}
	o.puts[result]++
}

func (o *blobResultObserver) OnGet(string) {}

func (o *blobResultObserver) putCounts() map[string]int {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make(map[string]int, len(o.puts))
	maps.Copy(out, o.puts)
	return out
}

// blobFixtureOptions varies the Store a blobFixture configures. The zero value
// is a writing Store that externalizes every non-empty array.
type blobFixtureOptions struct {
	Disabled  bool
	InlineMax int
	Compress  bool
	Observer  blob.PolicyObserver
}

// blobFixture is a Store wired to a counting blob Store over a temp-dir
// FSStore, with a recording Index in front of a MemoryIndex and a shared
// controllable clock.
type blobFixture struct {
	t     *testing.T
	state *Store
	rdb   redis.UniversalClient
	clock *blobClock
	index *blobRecordingIndex
	store *blobCountingStore
	fs    *objectstore.FSStore
	dir   string
	mr    *miniredis.Miniredis // nil on a real Redis
}

func newBlobFixture(t *testing.T, rdb redis.UniversalClient, opts blobFixtureOptions) *blobFixture {
	t.Helper()
	clock := &blobClock{t: time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)}
	memIndex := blob.NewMemoryIndex(clock.Now)
	dir := t.TempDir()
	fs := objectstore.NewFSStore(dir)
	backing := blob.NewObjectBacked(fs, blob.ObjectBackedOptions{Index: memIndex, Compress: opts.Compress})
	f := &blobFixture{
		t:     t,
		rdb:   rdb,
		clock: clock,
		index: &blobRecordingIndex{Index: memIndex},
		store: &blobCountingStore{Store: backing},
		fs:    fs,
		dir:   dir,
	}
	f.state = New(rdb, nil, blobTestTTL)
	err := f.state.ConfigureBlob(BlobConfig{
		Store:          f.store,
		Index:          f.index,
		Enabled:        !opts.Disabled,
		InlineMax:      opts.InlineMax,
		Grace:          blobTestGrace,
		PolicyObserver: opts.Observer,
		now:            clock.Now,
	})
	if err != nil {
		t.Fatalf("ConfigureBlob() error = %v", err)
	}
	return f
}

// replica returns a Store over the same Redis and the same blob configuration
// as f.state but with none of its process-local state: no per-execution TTL
// map, and execTTL as its only default. It stands for another replica of the
// service, or this one after a restart.
func (f *blobFixture) replica(execTTL time.Duration) *Store {
	f.t.Helper()
	s := New(f.rdb, nil, execTTL)
	err := s.ConfigureBlob(BlobConfig{
		Store:   f.store,
		Index:   f.index,
		Enabled: true,
		Grace:   blobTestGrace,
		now:     f.clock.Now,
	})
	if err != nil {
		f.t.Fatalf("ConfigureBlob() on a replica error = %v", err)
	}
	return s
}

// createExecutionOn creates a running execution on state with retention as the
// per-invocation TTL hint, so the retention record Redis keeps for it is the one
// a creating replica would have written.
func (f *blobFixture) createExecutionOn(ctx context.Context, state *Store, id types.ExecutionID, retention time.Duration) {
	f.t.Helper()
	snap := &engine.ExecutionSnapshot{ID: id, Graph: testGraphOneNode(), Status: types.ExecutionStatusRunning}
	if err := state.CreateExecution(engine.WithExecutionTTL(ctx, retention), snap); err != nil {
		f.t.Fatalf("CreateExecution(%q) error = %v", id, err)
	}
}

func newBlobMiniredisFixture(t *testing.T, opts blobFixtureOptions) *blobFixture {
	t.Helper()
	_, mr, rdb := newTestRedisState(t)
	f := newBlobFixture(t, rdb, opts)
	f.mr = mr
	return f
}

// blobTestItems returns n decoded-JSON-shaped items, so a round trip through
// the blob layer compares equal with reflect.DeepEqual.
func blobTestItems(n int) []any {
	items := make([]any, n)
	for i := range items {
		items[i] = map[string]any{
			"id":   float64(i),
			"name": fmt.Sprintf("item-%04d", i),
			"tags": []any{"a", "b"},
		}
	}
	return items
}

// externalize calls ExternalizeItems and requires that the array was stored.
func (f *blobFixture) externalize(ctx context.Context, execID string, items []any) json.RawMessage {
	f.t.Helper()
	ref, ok, err := f.state.ExternalizeItems(ctx, execID, items)
	if err != nil {
		f.t.Fatalf("ExternalizeItems(%q) error = %v", execID, err)
	}
	if !ok || len(ref) == 0 {
		f.t.Fatalf("ExternalizeItems(%q) = (%s, %v), want a stored reference", execID, ref, ok)
	}
	return ref
}

// blobObjects counts the regular files under dir, which is how a test proves
// nothing was written to the object store.
func blobObjects(t *testing.T, dir string) int {
	t.Helper()
	n := 0
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err == nil && d.Type().IsRegular() {
			n++
		}
		return err
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	return n
}

// TestExpireAtCoversDeadLetterRetention pins the link expiry to now + 2*ttl +
// grace. A dead-lettered outbox entry is given a fresh ttl when it arrives, after
// the retries that led there, so a link of one ttl would lapse while the
// entry's Ref is still readable.
func TestExpireAtCoversDeadLetterRetention(t *testing.T) {
	f := newBlobMiniredisFixture(t, blobFixtureOptions{})
	start := f.clock.Now()
	f.externalize(context.Background(), "exec-expiry", blobTestItems(20))

	links := f.index.recorded()
	if len(links) != 1 {
		t.Fatalf("recorded %d links, want 1", len(links))
	}
	want := start.Add(2*blobTestTTL + blobTestGrace)
	if got := links[0].ExpireAt; got.Before(want) {
		t.Fatalf("link expire_at = %s, want at least now + 2*ttl + grace = %s", got, want)
	}
	if got := links[0].ExecID; got != "exec-expiry" {
		t.Fatalf("link exec id = %q, want %q", got, "exec-expiry")
	}
}

// TestExpireAtUsesExecutionTTLOverride checks that ttl is the execution's own
// retention, not the adapter default, when the execution has an override.
func TestExpireAtUsesExecutionTTLOverride(t *testing.T) {
	f := newBlobMiniredisFixture(t, blobFixtureOptions{})
	const override = 7 * time.Minute
	f.state.ttlMu.Lock()
	f.state.execTTLs["exec-override"] = override
	f.state.ttlMu.Unlock()

	start := f.clock.Now()
	f.externalize(context.Background(), "exec-override", blobTestItems(20))

	want := start.Add(2*override + blobTestGrace)
	links := f.index.recorded()
	if len(links) != 1 {
		t.Fatalf("recorded %d links, want 1", len(links))
	}
	if got := links[0].ExpireAt; got.Before(want) {
		t.Fatalf("link expire_at = %s, want at least %s", got, want)
	}
}

// TestBlobLinkExpiry pins the arithmetic on its own, so a change to the rule
// fails here with the numbers in view.
func TestBlobLinkExpiry(t *testing.T) {
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	got := blobLinkExpiry(now, 10*time.Minute, 3*time.Minute)
	if want := now.Add(23 * time.Minute); !got.Equal(want) {
		t.Fatalf("blobLinkExpiry = %s, want %s", got, want)
	}
}

// TestExternalizeLinksBeforeReturningRef: a reference handed to the caller
// must already be linked, otherwise a collector can reclaim the bytes before the
// caller has written the reference anywhere.
func TestExternalizeLinksBeforeReturningRef(t *testing.T) {
	f := newBlobMiniredisFixture(t, blobFixtureOptions{})
	ref := f.externalize(context.Background(), "exec-link", blobTestItems(20))

	links := f.index.recorded()
	if len(links) != 1 {
		t.Fatalf("recorded %d links by the time ExternalizeItems returned, want 1", len(links))
	}
	var returned blob.Ref
	if err := json.Unmarshal(ref, &returned); err != nil {
		t.Fatalf("decode returned reference: %v", err)
	}
	if links[0].Ref.Digest != returned.Digest || links[0].Ref.Namespace != returned.Namespace {
		t.Fatalf("linked %+v, returned %+v: the link must name the returned blob", links[0].Ref, returned)
	}

	// The link is real: with the clock past the retention minus a margin, the
	// blob is still not a collection candidate.
	f.clock.Advance(2 * blobTestTTL)
	cands, err := f.index.Candidates(context.Background(), blobTestGrace, 10)
	if err != nil {
		t.Fatalf("Candidates() error = %v", err)
	}
	if len(cands) != 0 {
		t.Fatalf("Candidates() = %+v within 2*ttl of the write, want none", cands)
	}
}

// TestDisabledNeverWritesBlob: with writing off ExternalizeItems must not encode
// the array, touch the blob Store or the Index, or leave an object behind. The
// channel in the items cannot be JSON encoded, so an implementation that encodes
// before checking the switch returns an error instead of "not externalized".
func TestDisabledNeverWritesBlob(t *testing.T) {
	f := newBlobMiniredisFixture(t, blobFixtureOptions{Disabled: true})
	items := append(blobTestItems(50), make(chan int))

	ref, ok, err := f.state.ExternalizeItems(context.Background(), "exec-off", items)
	if err != nil || ok || ref != nil {
		t.Fatalf("ExternalizeItems = (%s, %v, %v), want (nil, false, nil)", ref, ok, err)
	}
	if puts, gets := f.store.puts.Load(), f.store.gets.Load(); puts != 0 || gets != 0 {
		t.Fatalf("blob Store saw %d puts and %d gets, want none", puts, gets)
	}
	if n := f.index.linkAttempts(); n != 0 {
		t.Fatalf("Index saw %d Link calls, want none", n)
	}
	if n := blobObjects(t, f.dir); n != 0 {
		t.Fatalf("%d objects on disk, want none", n)
	}
}

// TestDisabledWritesNothingForPlainItems is the same promise as above with items
// that encode fine, so it fails on an actual blob write rather than on an
// encoding error: nothing reaches the Store, the Index or the disk.
func TestDisabledWritesNothingForPlainItems(t *testing.T) {
	f := newBlobMiniredisFixture(t, blobFixtureOptions{Disabled: true})

	ref, ok, err := f.state.ExternalizeItems(context.Background(), "exec-off-plain", blobTestItems(50))
	if err != nil || ok || ref != nil {
		t.Fatalf("ExternalizeItems = (%s, %v, %v), want (nil, false, nil)", ref, ok, err)
	}
	if puts := f.store.puts.Load(); puts != 0 {
		t.Fatalf("blob Store saw %d puts, want none", puts)
	}
	if n := f.index.linkAttempts(); n != 0 {
		t.Fatalf("Index saw %d Link calls, want none", n)
	}
	if n := blobObjects(t, f.dir); n != 0 {
		t.Fatalf("%d objects on disk, want none", n)
	}
}

// TestDisabledStillResolvesExistingRefs: switching writing off must not strand
// references that are already in flight.
func TestDisabledStillResolvesExistingRefs(t *testing.T) {
	f := newBlobMiniredisFixture(t, blobFixtureOptions{})
	items := blobTestItems(20)
	ref := f.externalize(context.Background(), "exec-flip", items)

	if err := f.state.ConfigureBlob(BlobConfig{Store: f.store, Index: f.index, Enabled: false}); err != nil {
		t.Fatalf("ConfigureBlob() error = %v", err)
	}
	got, err := f.state.ResolveItems(context.Background(), ref)
	if err != nil || !reflect.DeepEqual(got, items) {
		t.Fatalf("ResolveItems after disabling = (%d items, %v), want the original %d", len(got), err, len(items))
	}
}

// TestUnconfiguredStoreBehavesAsBefore: without ConfigureBlob the Store neither
// stores nor reads anything and sends no command to Redis for it.
func TestUnconfiguredStoreBehavesAsBefore(t *testing.T) {
	state, mr, _ := newTestRedisState(t)
	before := mr.CommandCount()

	ref, ok, err := state.ExternalizeItems(context.Background(), "exec-none", append(blobTestItems(5), make(chan int)))
	if err != nil || ok || ref != nil {
		t.Fatalf("ExternalizeItems = (%s, %v, %v), want (nil, false, nil)", ref, ok, err)
	}
	if _, err := state.ResolveItems(context.Background(), json.RawMessage(`{}`)); !errors.Is(err, ErrBlobNotConfigured) {
		t.Fatalf("ResolveItems error = %v, want ErrBlobNotConfigured", err)
	}
	if after := mr.CommandCount(); after != before {
		t.Fatalf("Redis saw %d new commands, want none", after-before)
	}
	if got := state.blob.Load(); got != nil {
		t.Fatalf("blob runtime = %+v on an unconfigured Store, want nil", got)
	}
}

// TestExternalizeResolveRoundTrip: the array that comes back is the array that
// went in, element for element, for plain and compressed objects and for
// several sizes.
func TestExternalizeResolveRoundTrip(t *testing.T) {
	for _, compress := range []bool{false, true} {
		for _, n := range []int{1, 7, 500} {
			t.Run(fmt.Sprintf("compress=%v/n=%d", compress, n), func(t *testing.T) {
				f := newBlobMiniredisFixture(t, blobFixtureOptions{Compress: compress})
				items := blobTestItems(n)
				ref := f.externalize(context.Background(), "exec-rt", items)

				got, err := f.state.ResolveItems(context.Background(), ref)
				if err != nil {
					t.Fatalf("ResolveItems() error = %v", err)
				}
				if len(got) != len(items) {
					t.Fatalf("ResolveItems() returned %d items, want %d", len(got), len(items))
				}
				for i := range items {
					if !reflect.DeepEqual(got[i], items[i]) {
						t.Fatalf("item %d = %#v, want %#v", i, got[i], items[i])
					}
				}
			})
		}
	}
}

// TestExternalizeUnderInlineMaxStaysInline: an array whose encoding is no larger
// than InlineMax is not stored, and the boundary is inclusive.
func TestExternalizeUnderInlineMaxStaysInline(t *testing.T) {
	items := blobTestItems(3)
	encoded, err := json.Marshal(items)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	for _, tc := range []struct {
		name      string
		inlineMax int
		want      bool
	}{
		{"well under", len(encoded) + 1000, false},
		{"exactly at the limit", len(encoded), false},
		{"one byte over", len(encoded) - 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newBlobMiniredisFixture(t, blobFixtureOptions{InlineMax: tc.inlineMax})
			ref, ok, err := f.state.ExternalizeItems(context.Background(), "exec-small", items)
			if err != nil || ok != tc.want || (ok != (ref != nil)) {
				t.Fatalf("ExternalizeItems = (%s, %v, %v), want externalized=%v", ref, ok, err, tc.want)
			}
			if !tc.want && (f.store.puts.Load() != 0 || f.index.linkAttempts() != 0) {
				t.Fatalf("a payload kept inline reached the blob layer: %d puts, %d links", f.store.puts.Load(), f.index.linkAttempts())
			}
		})
	}
}

// decodeBlobRef parses the reference ExternalizeItems returned.
func decodeBlobRef(t *testing.T, raw json.RawMessage) blob.Ref {
	t.Helper()
	var ref blob.Ref
	if err := json.Unmarshal(raw, &ref); err != nil {
		t.Fatalf("decode reference %s: %v", raw, err)
	}
	return ref
}

// TestResolveItemsErrorsStayDistinguishable: the three permanent conditions come
// back as themselves, so the layer above can tell "gone" from "damaged" from
// "someone else's" and none of them is flattened into a generic failure.
func TestResolveItemsErrorsStayDistinguishable(t *testing.T) {
	ctx := context.Background()
	kinds := []error{blob.ErrNotFound, blob.ErrCorrupt, blob.ErrNamespaceMismatch, blob.ErrInvalidRef}

	cases := []struct {
		name string
		want error
		run  func(f *blobFixture, ref json.RawMessage) error
	}{
		{"missing blob", blob.ErrNotFound, func(f *blobFixture, ref json.RawMessage) error {
			if err := f.store.Delete(ctx, namespace.Default, decodeBlobRef(t, ref)); err != nil {
				t.Fatalf("Delete() error = %v", err)
			}
			_, err := f.state.ResolveItems(ctx, ref)
			return err
		}},
		{"damaged blob", blob.ErrCorrupt, func(f *blobFixture, ref json.RawMessage) error {
			r := decodeBlobRef(t, ref)
			key := fmt.Sprintf("blobs/%x/sha256/%s/%s/%s", namespace.Default, r.Digest[0:2], r.Digest[2:4], r.Digest)
			damaged := append([]byte{0x00}, make([]byte, r.Size)...)
			if _, err := f.fs.PutObject(ctx, key, bytes.NewReader(damaged), int64(len(damaged)), objectstore.PutOptions{}); err != nil {
				t.Fatalf("PutObject() error = %v", err)
			}
			_, err := f.state.ResolveItems(ctx, ref)
			return err
		}},
		{"another namespace", blob.ErrNamespaceMismatch, func(f *blobFixture, ref json.RawMessage) error {
			_, err := f.state.ResolveItems(namespace.WithNamespace(ctx, "tenant-b"), ref)
			return err
		}},
		{"not a reference", blob.ErrInvalidRef, func(f *blobFixture, _ json.RawMessage) error {
			_, err := f.state.ResolveItems(ctx, json.RawMessage(`"nope"`))
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newBlobMiniredisFixture(t, blobFixtureOptions{})
			err := tc.run(f, f.externalize(ctx, "exec-err", blobTestItems(20)))
			for _, kind := range kinds {
				if got := errors.Is(err, kind); got != (kind == tc.want) {
					t.Fatalf("errors.Is(%v, %v) = %v, want %v", err, kind, got, kind == tc.want)
				}
			}
			if !types.IsPermanent(err) {
				t.Fatalf("error %v is not marked permanent; the engine relies on that to stop retrying", err)
			}
		})
	}
}

// TestResolveMissingBlobIsNotAnEmptyArray: a blob that is gone is an error. An
// empty array would silently drop every item of the map node.
func TestResolveMissingBlobIsNotAnEmptyArray(t *testing.T) {
	ctx := context.Background()
	f := newBlobMiniredisFixture(t, blobFixtureOptions{})
	ref := f.externalize(ctx, "exec-gone", blobTestItems(20))
	if err := f.store.Delete(ctx, namespace.Default, decodeBlobRef(t, ref)); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	items, err := f.state.ResolveItems(ctx, ref)
	if !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("ResolveItems = (%d items, %v), want an error matching blob.ErrNotFound", len(items), err)
	}
	if items != nil {
		t.Fatalf("ResolveItems returned %d items next to an error, want nil", len(items))
	}
}

// TestStoreFailuresAreReturnedNotSwallowed: a failing Put or Link is an error
// for the caller, never a quiet "keep it inline".
func TestStoreFailuresAreReturnedNotSwallowed(t *testing.T) {
	ctx := context.Background()
	t.Run("put fails", func(t *testing.T) {
		f := newBlobMiniredisFixture(t, blobFixtureOptions{})
		f.store.putErr = errors.New("disk on fire")
		ref, ok, err := f.state.ExternalizeItems(ctx, "exec-put", blobTestItems(20))
		if err == nil || ok || ref != nil {
			t.Fatalf("ExternalizeItems = (%s, %v, %v), want an error", ref, ok, err)
		}
		if !errors.Is(err, blob.ErrTransient) {
			t.Fatalf("error %v does not match blob.ErrTransient", err)
		}
	})
	t.Run("link fails", func(t *testing.T) {
		f := newBlobMiniredisFixture(t, blobFixtureOptions{})
		f.index.linkErr = errors.New("index down")
		ref, ok, err := f.state.ExternalizeItems(ctx, "exec-link-fail", blobTestItems(20))
		if err == nil || ok || ref != nil {
			t.Fatalf("ExternalizeItems = (%s, %v, %v), want an error", ref, ok, err)
		}
	})
}

// TestExternalizeKeepsInlineWhenStoreRefusesTooLarge: an array the blob Store
// will not take is not a failure of the map node. Without blob support the same
// items stay inline and the map runs, so turning the capability on must not make
// that map fail. The refusal is counted once, by Policy, and nothing is linked.
func TestExternalizeKeepsInlineWhenStoreRefusesTooLarge(t *testing.T) {
	ctx := context.Background()
	t.Run("too large stays inline", func(t *testing.T) {
		obs := &blobResultObserver{}
		f := newBlobMiniredisFixture(t, blobFixtureOptions{Observer: obs})
		f.store.putErr = blob.ErrTooLarge

		ref, ok, err := f.state.ExternalizeItems(ctx, "exec-too-large", blobTestItems(20))
		if err != nil || ok || ref != nil {
			t.Fatalf("ExternalizeItems = (%s, %v, %v), want (nil, false, nil)", ref, ok, err)
		}
		if n := f.store.puts.Load(); n != 1 {
			t.Fatalf("blob Store saw %d puts, want the one that was refused", n)
		}
		if n := f.index.linkAttempts(); n != 0 || len(f.index.recorded()) != 0 {
			t.Fatalf("Index saw %d Link calls and recorded %d links, want none", n, len(f.index.recorded()))
		}
		if got, want := obs.putCounts(), map[string]int{blob.PutResultTooLarge: 1}; !reflect.DeepEqual(got, want) {
			t.Fatalf("observer saw %v, want %v: the refusal is counted once, by Policy", got, want)
		}
	})
	t.Run("transient error is still an error", func(t *testing.T) {
		obs := &blobResultObserver{}
		f := newBlobMiniredisFixture(t, blobFixtureOptions{Observer: obs})
		f.store.putErr = errors.New("disk on fire")

		ref, ok, err := f.state.ExternalizeItems(ctx, "exec-transient", blobTestItems(20))
		if err == nil || ok || ref != nil {
			t.Fatalf("ExternalizeItems = (%s, %v, %v), want an error", ref, ok, err)
		}
		if !errors.Is(err, blob.ErrTransient) || errors.Is(err, blob.ErrTooLarge) {
			t.Fatalf("error %v, want one that matches blob.ErrTransient and not blob.ErrTooLarge", err)
		}
		if got, want := obs.putCounts(), map[string]int{blob.PutResultError: 1}; !reflect.DeepEqual(got, want) {
			t.Fatalf("observer saw %v, want %v", got, want)
		}
	})
}

// TestExternalizeFailsBeforeWritingWhenRetentionIsUnreadable: the retention is
// read before any blob is stored, so a record Redis cannot give back (here one
// that is not a number) fails the call with nothing left behind: no blob put, no
// link, no object on disk.
func TestExternalizeFailsBeforeWritingWhenRetentionIsUnreadable(t *testing.T) {
	f := newBlobMiniredisFixture(t, blobFixtureOptions{})
	ctx := context.Background()
	const id = types.ExecutionID("exec-bad-retention")
	if err := f.rdb.Set(ctx, retentionRecordKey(namespace.FromContext(ctx), id), "not-a-number", 0).Err(); err != nil {
		t.Fatalf("seed retention record: %v", err)
	}

	ref, ok, err := f.state.ExternalizeItems(ctx, string(id), blobTestItems(20))
	if err == nil || ok || ref != nil {
		t.Fatalf("ExternalizeItems = (%s, %v, %v), want an error", ref, ok, err)
	}
	if puts := f.store.puts.Load(); puts != 0 {
		t.Fatalf("blob Store saw %d puts before the retention failure, want none", puts)
	}
	if n := f.index.linkAttempts(); n != 0 {
		t.Fatalf("Index saw %d Link calls, want none", n)
	}
	if n := blobObjects(t, f.dir); n != 0 {
		t.Fatalf("%d objects on disk, want none", n)
	}
}

// TestExternalizeSkipsRetentionReadForPayloadsThatStayInline: an array that fits
// under InlineMax needs no link, so ExternalizeItems must decide that before it
// asks Redis for the retention. The record here is unreadable, which fails a
// payload that would be stored (see the test above) but must not touch one that
// stays inline: such a call has no reason to fail and no reason to cost a read.
// The one-byte-over case is the contrast: same record, same items, and the call
// does read it and fails.
func TestExternalizeSkipsRetentionReadForPayloadsThatStayInline(t *testing.T) {
	items := blobTestItems(3)
	encoded, err := json.Marshal(items)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	for _, tc := range []struct {
		name      string
		inlineMax int
		wantErr   bool
	}{
		{"well under", len(encoded) + 1000, false},
		{"exactly at the limit", len(encoded), false},
		{"one byte over", len(encoded) - 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newBlobMiniredisFixture(t, blobFixtureOptions{InlineMax: tc.inlineMax})
			ctx := context.Background()
			const id = types.ExecutionID("exec-bad-retention-small")
			key := retentionRecordKey(namespace.FromContext(ctx), id)
			if err := f.rdb.Set(ctx, key, "not-a-number", 0).Err(); err != nil {
				t.Fatalf("seed retention record: %v", err)
			}
			before := f.mr.CommandCount()

			ref, ok, err := f.state.ExternalizeItems(ctx, string(id), items)
			reads := f.mr.CommandCount() - before
			if tc.wantErr {
				if err == nil || ok || ref != nil {
					t.Fatalf("ExternalizeItems = (%s, %v, %v), want an error from the unreadable record", ref, ok, err)
				}
				if reads == 0 {
					t.Fatal("ExternalizeItems sent no Redis command, want the retention read the contrast relies on")
				}
				return
			}
			if err != nil || ok || ref != nil {
				t.Fatalf("ExternalizeItems = (%s, %v, %v), want (nil, false, nil)", ref, ok, err)
			}
			if reads != 0 {
				t.Fatalf("a payload kept inline sent %d Redis commands, want none", reads)
			}
			if puts := f.store.puts.Load(); puts != 0 {
				t.Fatalf("blob Store saw %d puts, want none", puts)
			}
			if n := f.index.linkAttempts(); n != 0 {
				t.Fatalf("Index saw %d Link calls, want none", n)
			}
		})
	}
}

// TestExternalizeFallsBackInlineWhenEveryLinkIsFenced: when the blob layer gives
// up after repeated fences it chooses inline itself, which is externalized=false
// with a nil error, not a failure.
func TestExternalizeFallsBackInlineWhenEveryLinkIsFenced(t *testing.T) {
	f := newBlobMiniredisFixture(t, blobFixtureOptions{})
	f.index.linkErr = blob.ErrFenced

	ref, ok, err := f.state.ExternalizeItems(context.Background(), "exec-fenced", blobTestItems(20))
	if err != nil || ok || ref != nil {
		t.Fatalf("ExternalizeItems = (%s, %v, %v), want (nil, false, nil)", ref, ok, err)
	}
	if n := f.index.linkAttempts(); n < 2 {
		t.Fatalf("Link attempted %d times, want the blob layer to have retried", n)
	}
}

// TestExpireAtUsesPersistedRetentionOnAnotherReplica: one Store creates an
// execution with a 90 minute retention, and a different Store, with no
// per-execution TTL of its own and a much shorter default, externalizes the
// items of one of its map nodes. The link has to be sized from the retention
// Redis holds, not from that default, or it lapses while the execution is still
// running and the collector reclaims a blob the map node still needs.
func TestExpireAtUsesPersistedRetentionOnAnotherReplica(t *testing.T) {
	const retention = 90 * time.Minute
	f := newBlobMiniredisFixture(t, blobFixtureOptions{})
	ctx := context.Background()
	f.createExecutionOn(ctx, f.replica(2*time.Hour), "exec-replica", retention)

	other := f.replica(blobTestTTL)
	start := f.clock.Now()
	if _, ok, err := other.ExternalizeItems(ctx, "exec-replica", blobTestItems(20)); err != nil || !ok {
		t.Fatalf("ExternalizeItems() = (_, %v, %v), want a stored reference", ok, err)
	}

	links := f.index.recorded()
	if len(links) != 1 {
		t.Fatalf("recorded %d links, want 1", len(links))
	}
	want := start.Add(2*retention + blobTestGrace)
	if got := links[0].ExpireAt; got.Before(want) {
		t.Fatalf("link expire_at = %s, want at least now + 2*%s + grace = %s", got, retention, want)
	}
}

// TestExpireAtKeepsTheLargerOfLocalAndPersistedTTL: the persisted retention only
// ever lengthens the link. A Store whose own default is longer than the record
// keeps using its default, so the correction cannot shorten a link that was
// already long enough.
func TestExpireAtKeepsTheLargerOfLocalAndPersistedTTL(t *testing.T) {
	const localDefault = 3 * time.Hour
	f := newBlobMiniredisFixture(t, blobFixtureOptions{})
	ctx := context.Background()
	f.createExecutionOn(ctx, f.replica(2*time.Hour), "exec-longer", 90*time.Minute)

	start := f.clock.Now()
	if _, ok, err := f.replica(localDefault).ExternalizeItems(ctx, "exec-longer", blobTestItems(20)); err != nil || !ok {
		t.Fatalf("ExternalizeItems() = (_, %v, %v), want a stored reference", ok, err)
	}
	links := f.index.recorded()
	if len(links) != 1 {
		t.Fatalf("recorded %d links, want 1", len(links))
	}
	if want := start.Add(2*localDefault + blobTestGrace); !links[0].ExpireAt.Equal(want) {
		t.Fatalf("link expire_at = %s, want now + 2*%s + grace = %s", links[0].ExpireAt, localDefault, want)
	}
}

// TestBlobNamespaceComesFromContext: the blob is issued in the context's
// namespace, and only that namespace can read it back.
func TestBlobNamespaceComesFromContext(t *testing.T) {
	f := newBlobMiniredisFixture(t, blobFixtureOptions{})
	tenant := namespace.WithNamespace(context.Background(), "tenant-a")
	items := blobTestItems(20)
	ref := f.externalize(tenant, "exec-ns", items)

	if got := decodeBlobRef(t, ref).Namespace; got != "tenant-a" {
		t.Fatalf("reference namespace = %q, want %q", got, "tenant-a")
	}
	got, err := f.state.ResolveItems(tenant, ref)
	if err != nil || !reflect.DeepEqual(got, items) {
		t.Fatalf("ResolveItems in the issuing namespace = (%d items, %v)", len(got), err)
	}
	if _, err := f.state.ResolveItems(context.Background(), ref); !errors.Is(err, blob.ErrNamespaceMismatch) {
		t.Fatalf("ResolveItems in the default namespace error = %v, want blob.ErrNamespaceMismatch", err)
	}
}

// TestConfigureBlobRejectsBadConfig: misconfiguration fails at startup, not on
// the first map node.
func TestConfigureBlobRejectsBadConfig(t *testing.T) {
	mem := blob.NewMemory()
	idx := blob.NewMemoryIndex(nil)
	for _, tc := range []struct {
		name string
		cfg  BlobConfig
	}{
		{"no store", BlobConfig{Index: idx, Enabled: true, Grace: time.Hour}},
		{"no index", BlobConfig{Store: mem, Enabled: true, Grace: time.Hour}},
		{"negative inline max", BlobConfig{Store: mem, Index: idx, InlineMax: -1, Grace: time.Hour}},
		{"enabled without grace", BlobConfig{Store: mem, Index: idx, Enabled: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state, _, _ := newTestRedisState(t)
			if err := state.ConfigureBlob(tc.cfg); err == nil {
				t.Fatal("ConfigureBlob() error = nil, want an error")
			}
			if state.blob.Load() != nil {
				t.Fatal("a rejected config left a blob runtime installed")
			}
		})
	}
}

// TestExternalizeAddsNoRedisKeys: the capability lives entirely outside Redis.
// Externalizing and resolving must not create a key, so there is nothing for
// the execution's key inventory or TTL sweep to miss.
func TestExternalizeAddsNoRedisKeys(t *testing.T) {
	f := newBlobMiniredisFixture(t, blobFixtureOptions{})
	ctx := context.Background()
	ref := f.externalize(ctx, "exec-keys", blobTestItems(20))
	if _, err := f.state.ResolveItems(ctx, ref); err != nil {
		t.Fatalf("ResolveItems() error = %v", err)
	}
	if keys := f.mr.Keys(); len(keys) != 0 {
		t.Fatalf("Redis holds %v after a round trip, want no keys", keys)
	}
}

// TestResolveItemsMarksPermanentConditions: every condition a re-read cannot fix
// comes back marked types.ErrPermanent, which is all the engine (it may not
// import the blob layer) can see, and still matches its blob sentinel.
func TestResolveItemsMarksPermanentConditions(t *testing.T) {
	ctx := context.Background()
	// overwrite replaces the stored bytes of a blob under the key that blob's digest
	// maps to, so the reference still resolves, to bytes that no longer match it.
	overwrite := func(f *blobFixture, ref json.RawMessage, data []byte) {
		r := decodeBlobRef(t, ref)
		key := fmt.Sprintf("blobs/%x/sha256/%s/%s/%s", namespace.Default, r.Digest[0:2], r.Digest[2:4], r.Digest)
		if _, err := f.fs.PutObject(ctx, key, bytes.NewReader(data), int64(len(data)), objectstore.PutOptions{}); err != nil {
			t.Fatalf("PutObject() error = %v", err)
		}
	}

	cases := []struct {
		name string
		want error
		run  func(f *blobFixture, ref json.RawMessage) error
	}{
		{"not found", blob.ErrNotFound, func(f *blobFixture, ref json.RawMessage) error {
			if err := f.store.Delete(ctx, namespace.Default, decodeBlobRef(t, ref)); err != nil {
				t.Fatalf("Delete() error = %v", err)
			}
			_, err := f.state.ResolveItems(ctx, ref)
			return err
		}},
		{"never written", blob.ErrNotFound, func(f *blobFixture, _ json.RawMessage) error {
			never := blob.Ref{Namespace: string(namespace.Default), Digest: fmt.Sprintf("%064x", 7), Size: 12, Stored: 12}
			raw, err := json.Marshal(never)
			if err != nil {
				t.Fatalf("marshal ref: %v", err)
			}
			_, err = f.state.ResolveItems(ctx, raw)
			return err
		}},
		{"corrupt", blob.ErrCorrupt, func(f *blobFixture, ref json.RawMessage) error {
			overwrite(f, ref, append([]byte{0x00}, make([]byte, decodeBlobRef(t, ref).Size)...))
			_, err := f.state.ResolveItems(ctx, ref)
			return err
		}},
		{"namespace mismatch", blob.ErrNamespaceMismatch, func(f *blobFixture, ref json.RawMessage) error {
			_, err := f.state.ResolveItems(namespace.WithNamespace(ctx, "tenant-b"), ref)
			return err
		}},
		{"invalid ref json", blob.ErrInvalidRef, func(f *blobFixture, _ json.RawMessage) error {
			_, err := f.state.ResolveItems(ctx, json.RawMessage(`"nope"`))
			return err
		}},
		{"invalid ref digest", blob.ErrInvalidRef, func(f *blobFixture, _ json.RawMessage) error {
			_, err := f.state.ResolveItems(ctx, json.RawMessage(`{"sha256":"../../etc/passwd","size":3}`))
			return err
		}},
		{"too large", blob.ErrTooLarge, func(f *blobFixture, ref json.RawMessage) error {
			f.store.getErr = blob.ErrTooLarge
			_, err := f.state.ResolveItems(ctx, ref)
			return err
		}},
		{"stored bytes are not an array", blob.ErrCorrupt, func(f *blobFixture, _ json.RawMessage) error {
			object, err := f.store.Put(ctx, namespace.Default, []byte(`{"not":"an array"}`))
			if err != nil {
				t.Fatalf("Put() error = %v", err)
			}
			raw, err := json.Marshal(object)
			if err != nil {
				t.Fatalf("marshal ref: %v", err)
			}
			_, err = f.state.ResolveItems(ctx, raw)
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newBlobMiniredisFixture(t, blobFixtureOptions{})
			err := tc.run(f, f.externalize(ctx, "exec-permanent", blobTestItems(20)))
			if err == nil {
				t.Fatal("ResolveItems returned no error")
			}
			if !types.IsPermanent(err) {
				t.Errorf("types.IsPermanent(%v) = false, want true", err)
			}
			if !errors.Is(err, tc.want) {
				t.Errorf("errors.Is(%v, %v) = false, want the blob sentinel to survive the marking", err, tc.want)
			}
		})
	}
}

// TestResolveItemsLeavesRetryableConditionsUnmarked: a store that may recover and
// a process that has no blob store at all are not permanent. The first is worth
// retrying here; the second may resolve on a replica that is configured.
func TestResolveItemsLeavesRetryableConditionsUnmarked(t *testing.T) {
	ctx := context.Background()
	t.Run("transient store failure", func(t *testing.T) {
		f := newBlobMiniredisFixture(t, blobFixtureOptions{})
		ref := f.externalize(ctx, "exec-transient", blobTestItems(20))
		f.store.getErr = errors.New("disk on fire")
		_, err := f.state.ResolveItems(ctx, ref)
		if err == nil || types.IsPermanent(err) {
			t.Fatalf("ResolveItems error = %v, want a retryable error", err)
		}
		if !errors.Is(err, blob.ErrTransient) {
			t.Errorf("error %v lost blob.ErrTransient", err)
		}
	})
	t.Run("blob store not configured", func(t *testing.T) {
		_, _, rdb := newTestRedisState(t)
		bare := New(rdb, nil, blobTestTTL)
		_, err := bare.ResolveItems(ctx, json.RawMessage(`{}`))
		if !errors.Is(err, ErrBlobNotConfigured) {
			t.Fatalf("ResolveItems error = %v, want ErrBlobNotConfigured", err)
		}
		if types.IsPermanent(err) {
			t.Errorf("ErrBlobNotConfigured is marked permanent; it describes this process's wiring, not the data")
		}
	})
}
