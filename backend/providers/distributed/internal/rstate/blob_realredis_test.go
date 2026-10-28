package rstate

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store/blob"
	"github.com/xbcio/xflow/types"
)

// blobRealRedisRun is one test's slice of a real Redis. The server is shared, so
// nothing here flushes it: every key the test can create carries the run's unique
// namespace, and cleanup deletes exactly those keys.
type blobRealRedisRun struct {
	*blobFixture
	ns  namespace.Namespace
	ctx context.Context
}

func newBlobRealRedisRun(t *testing.T, opts blobFixtureOptions) *blobRealRedisRun {
	t.Helper()
	addr := realRedisAddr(t)
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	ns := namespace.Namespace(fmt.Sprintf("itemsreal%d", time.Now().UnixNano()))
	t.Cleanup(func() {
		ctx := context.Background()
		var cursor uint64
		for {
			keys, next, err := rdb.Scan(ctx, cursor, "xflow:ns:"+string(ns)+":*", 200).Result()
			if err != nil {
				t.Errorf("cleanup scan: %v", err)
				break
			}
			if len(keys) > 0 {
				_ = rdb.Del(ctx, keys...).Err()
			}
			if cursor = next; cursor == 0 {
				break
			}
		}
		_ = rdb.SRem(ctx, namespaceSetKey, string(ns)).Err()
		_ = rdb.Close()
	})
	return &blobRealRedisRun{
		blobFixture: newBlobFixture(t, rdb, opts),
		ns:          ns,
		ctx:         namespace.WithNamespace(context.Background(), ns),
	}
}

// createExecution registers a real execution so its retention is the one Redis
// holds, then returns its id.
func (r *blobRealRedisRun) createExecution(id types.ExecutionID, ttl time.Duration) {
	r.t.Helper()
	ctx := engine.WithExecutionTTL(r.ctx, ttl)
	g := testGraphOneNode()
	if err := r.state.CreateExecution(ctx, &engine.ExecutionSnapshot{ID: id, Graph: g, Status: types.ExecutionStatusRunning}); err != nil {
		r.t.Fatalf("CreateExecution(%q) error = %v", id, err)
	}
}

// namespaceKeys lists every key of the run's namespace, sorted.
func (r *blobRealRedisRun) namespaceKeys() []string {
	r.t.Helper()
	var out []string
	var cursor uint64
	for {
		keys, next, err := r.rdb.Scan(context.Background(), cursor, "xflow:ns:"+string(r.ns)+":*", 200).Result()
		if err != nil {
			r.t.Fatalf("scan namespace keys: %v", err)
		}
		out = append(out, keys...)
		if cursor = next; cursor == 0 {
			break
		}
	}
	sort.Strings(out)
	return out
}

// blobNamedKeys lists keys anywhere in the database whose name mentions blob.
// Nothing in Redis is meant to be named that way, so a hit is a stray write.
func (r *blobRealRedisRun) blobNamedKeys() []string {
	r.t.Helper()
	var out []string
	var cursor uint64
	for {
		keys, next, err := r.rdb.Scan(context.Background(), cursor, "*blob*", 500).Result()
		if err != nil {
			r.t.Fatalf("scan blob-named keys: %v", err)
		}
		out = append(out, keys...)
		if cursor = next; cursor == 0 {
			break
		}
	}
	return out
}

// blobCommandCounter is a go-redis hook that counts the commands a client sends.
type blobCommandCounter struct{ n atomic.Int64 }

func (h *blobCommandCounter) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *blobCommandCounter) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.n.Add(1)
		return next(ctx, cmd)
	}
}

func (h *blobCommandCounter) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.n.Add(int64(len(cmds)))
		return next(ctx, cmds)
	}
}

// TestRedisBlobRoundTrip is the miniredis round trip again on a real server:
// the array comes back element for element.
func TestRedisBlobRoundTrip(t *testing.T) {
	r := newBlobRealRedisRun(t, blobFixtureOptions{})
	items := blobTestItems(200)
	ref := r.externalize(r.ctx, "exec-real-rt", items)

	got, err := r.state.ResolveItems(r.ctx, ref)
	if err != nil {
		t.Fatalf("ResolveItems() error = %v", err)
	}
	if !reflect.DeepEqual(got, items) {
		t.Fatalf("ResolveItems() returned %d items that differ from the %d stored", len(got), len(items))
	}
	if _, err := r.state.ResolveItems(namespace.WithNamespace(context.Background(), "someone-else"), ref); !errors.Is(err, blob.ErrNamespaceMismatch) {
		t.Fatalf("ResolveItems from another namespace error = %v, want blob.ErrNamespaceMismatch", err)
	}
}

// TestRedisBlobKeepsItemsOutOfRedis: after externalizing a large array, no key of
// the run's namespace holds any of its bytes, and nothing in the database is
// named after blobs. The marker string appears in every item, so a copy of the
// array in any value would contain it.
func TestRedisBlobKeepsItemsOutOfRedis(t *testing.T) {
	r := newBlobRealRedisRun(t, blobFixtureOptions{})
	const execID = types.ExecutionID("exec-real-bytes")
	r.createExecution(execID, time.Hour)
	before := r.namespaceKeys()

	marker := "marker-" + strings.Repeat("Z", 40)
	items := make([]any, 100)
	for i := range items {
		items[i] = map[string]any{"i": float64(i), "payload": marker}
	}
	ref := r.externalize(r.ctx, string(execID), items)
	if _, err := r.state.ResolveItems(r.ctx, ref); err != nil {
		t.Fatalf("ResolveItems() error = %v", err)
	}

	after := r.namespaceKeys()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("namespace keys changed: before %v, after %v", before, after)
	}
	for _, key := range after {
		raw, err := r.rdb.Dump(context.Background(), key).Result()
		if err != nil {
			t.Fatalf("DUMP %s: %v", key, err)
		}
		if strings.Contains(raw, marker) {
			t.Fatalf("key %s holds a copy of the items", key)
		}
	}
	if hits := r.blobNamedKeys(); len(hits) != 0 {
		t.Fatalf("keys named after blobs: %v", hits)
	}
}

// TestRedisBlobLinkExpiryMatchesExecutionKeyTTL reads the TTL the real server
// holds on the execution's own keys and checks the link outlives twice that, plus
// grace. Reading it from Redis, not from the test's constant, is what ties the
// rule to the retention the execution actually has. The assertion on the key's
// TTL itself keeps the check from passing on a ttl of zero.
func TestRedisBlobLinkExpiryMatchesExecutionKeyTTL(t *testing.T) {
	r := newBlobRealRedisRun(t, blobFixtureOptions{})
	const execID = types.ExecutionID("exec-real-ttl")
	const ttl = 90 * time.Minute
	r.createExecution(execID, ttl)

	keyTTL, err := r.rdb.PTTL(context.Background(), execKey(r.ns, execID, "graph")).Result()
	if err != nil {
		t.Fatalf("PTTL error = %v", err)
	}
	if keyTTL < ttl-time.Minute || keyTTL > ttl {
		t.Fatalf("execution key TTL on the server = %s, want about %s", keyTTL, ttl)
	}

	start := r.clock.Now()
	r.externalize(r.ctx, string(execID), blobTestItems(50))
	links := r.index.recorded()
	if len(links) != 1 {
		t.Fatalf("recorded %d links, want 1", len(links))
	}
	if want := start.Add(2*keyTTL + blobTestGrace); links[0].ExpireAt.Before(want) {
		t.Fatalf("link expire_at = %s, want at least now + 2*%s + grace = %s", links[0].ExpireAt, keyTTL, want)
	}
}

// TestRedisBlobLinkExpiryUsesPersistedRetentionOnAnotherReplica: replica A
// creates an execution with a 90 minute retention, which the real server records.
// Replica B shares the Redis and the blob configuration but has no per-execution
// TTL and a one minute default. Its link must still outlive twice the retention
// the server holds, not twice its own default.
func TestRedisBlobLinkExpiryUsesPersistedRetentionOnAnotherReplica(t *testing.T) {
	r := newBlobRealRedisRun(t, blobFixtureOptions{})
	const execID = types.ExecutionID("exec-real-replica")
	const retention = 90 * time.Minute
	r.createExecutionOn(r.ctx, r.replica(2*time.Hour), execID, retention)

	recorded, err := r.rdb.Get(context.Background(), retentionRecordKey(r.ns, execID)).Result()
	if err != nil {
		t.Fatalf("read retention record: %v", err)
	}
	if want := fmt.Sprint(retention.Milliseconds()); recorded != want {
		t.Fatalf("retention record = %q, want %q", recorded, want)
	}

	other := r.replica(blobTestTTL)
	start := r.clock.Now()
	if _, ok, err := other.ExternalizeItems(r.ctx, string(execID), blobTestItems(50)); err != nil || !ok {
		t.Fatalf("ExternalizeItems() = (_, %v, %v), want a stored reference", ok, err)
	}
	links := r.index.recorded()
	if len(links) != 1 {
		t.Fatalf("recorded %d links, want 1", len(links))
	}
	if want := start.Add(2*retention + blobTestGrace); links[0].ExpireAt.Before(want) {
		t.Fatalf("link expire_at = %s, want at least now + 2*%s + grace = %s", links[0].ExpireAt, retention, want)
	}
}

// TestRedisBlobUnknownRetentionFallsBackToLocalTTL: an execution with no
// retention record on the server reads as engine.ExecutionRetentionUnknown, and
// the link is then sized from the Store's own value alone.
func TestRedisBlobUnknownRetentionFallsBackToLocalTTL(t *testing.T) {
	r := newBlobRealRedisRun(t, blobFixtureOptions{})
	const execID = types.ExecutionID("exec-real-unknown")
	if got, err := r.state.GetExecutionRetention(r.ctx, execID); err != nil || got != engine.ExecutionRetentionUnknown {
		t.Fatalf("GetExecutionRetention() = (%v, %v), want ExecutionRetentionUnknown", got, err)
	}

	start := r.clock.Now()
	r.externalize(r.ctx, string(execID), blobTestItems(50))
	links := r.index.recorded()
	if len(links) != 1 {
		t.Fatalf("recorded %d links, want 1", len(links))
	}
	if want := start.Add(2*blobTestTTL + blobTestGrace); !links[0].ExpireAt.Equal(want) {
		t.Fatalf("link expire_at = %s, want now + 2*%s + grace = %s", links[0].ExpireAt, blobTestTTL, want)
	}
}

// TestRedisBlobDisabledWritesNothing: a Store whose blob writing is off, and one
// that was never configured, send no command to Redis for ExternalizeItems and
// leave no key behind.
func TestRedisBlobDisabledWritesNothing(t *testing.T) {
	r := newBlobRealRedisRun(t, blobFixtureOptions{Disabled: true})
	counter := &blobCommandCounter{}
	r.rdb.AddHook(counter)
	before := r.namespaceKeys()
	items := append(blobTestItems(20), make(chan int))

	for name, state := range map[string]*Store{
		"disabled":     r.state,
		"unconfigured": New(r.rdb, nil, time.Minute),
	} {
		start := counter.n.Load()
		ref, ok, err := state.ExternalizeItems(r.ctx, "exec-real-off", items)
		if err != nil || ok || ref != nil {
			t.Fatalf("%s: ExternalizeItems = (%s, %v, %v), want (nil, false, nil)", name, ref, ok, err)
		}
		if n := counter.n.Load() - start; n != 0 {
			t.Fatalf("%s: ExternalizeItems sent %d Redis commands, want none", name, n)
		}
	}
	if puts := r.store.puts.Load(); puts != 0 {
		t.Fatalf("blob Store saw %d puts, want none", puts)
	}
	if after := r.namespaceKeys(); !reflect.DeepEqual(before, after) {
		t.Fatalf("namespace keys changed: before %v, after %v", before, after)
	}
	if hits := r.blobNamedKeys(); len(hits) != 0 {
		t.Fatalf("keys named after blobs: %v", hits)
	}
}

// TestRedisBlobResolveMarksPermanentConditions: the permanent marking does not
// depend on the Redis behind the Store. A blob that was deleted, and a reference
// read in another namespace, both come back as types.ErrPermanent with their
// blob sentinel intact, while a store that merely failed does not.
func TestRedisBlobResolveMarksPermanentConditions(t *testing.T) {
	r := newBlobRealRedisRun(t, blobFixtureOptions{})
	ref := r.externalize(r.ctx, "exec-real-permanent", blobTestItems(20))

	_, err := r.state.ResolveItems(namespace.WithNamespace(r.ctx, r.ns+"-other"), ref)
	if !types.IsPermanent(err) || !errors.Is(err, blob.ErrNamespaceMismatch) {
		t.Errorf("another namespace: ResolveItems error = %v, want permanent and blob.ErrNamespaceMismatch", err)
	}

	r.store.getErr = errors.New("disk on fire")
	if _, err := r.state.ResolveItems(r.ctx, ref); err == nil || types.IsPermanent(err) {
		t.Errorf("failing store: ResolveItems error = %v, want a retryable error", err)
	}
	r.store.getErr = nil

	if err := r.store.Delete(r.ctx, r.ns, decodeBlobRef(t, ref)); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := r.state.ResolveItems(r.ctx, ref); !types.IsPermanent(err) || !errors.Is(err, blob.ErrNotFound) {
		t.Errorf("deleted blob: ResolveItems error = %v, want permanent and blob.ErrNotFound", err)
	}
}
