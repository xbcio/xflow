package rstate

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// newOutboxMetricsTestStore seeds one ready outbox entry per execution into a
// miniredis-backed store and returns it alongside the raw client, so a test can
// add noise keys or rewrite a score directly.
func newOutboxMetricsTestStore(t *testing.T, availableAt map[types.ExecutionID]time.Time) (*Store, *redis.Client) {
	t.Helper()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	state := New(rdb, nil, time.Hour)
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	ids := make([]types.ExecutionID, 0, len(availableAt))
	for id := range availableAt {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if err := state.CreateExecutionWithOutbox(ctx, &engine.ExecutionSnapshot{
			ID:     id,
			Status: types.ExecutionStatusRunning,
			Graph:  testGraphTwoNode(),
		}, []engine.OutboxEntry{{
			ID:          string(id) + "/start/0",
			Task:        engine.Task{ExecutionID: id, NodeName: "start", Type: engine.TaskTypeNodeExec},
			AvailableAt: availableAt[id],
		}}); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
	return state, rdb
}

// TestOutboxMetricsReportsDueWorkSeparatelyFromPending pins the distinction the
// two gauges exist to draw. Pending counts every entry in a ready index,
// including entries a live deliverer has leased and entries waiting out a retry
// backoff — both scored in the future, both undeliverable this instant, and
// both invisible in a bare ZCARD. An operator watching only pending cannot tell
// "dispatch is behind" from "the backlog is all future work", and the
// throughput defect this accompanies was exactly the first of those.
func TestOutboxMetricsReportsDueWorkSeparatelyFromPending(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	state, _ := newOutboxMetricsTestStore(t, map[types.ExecutionID]time.Time{
		"exec-due":      {},
		"exec-deferred": time.Now().Add(time.Hour),
		"exec-leased":   {},
	})

	// Take the lease on the third execution's only entry. Leasing pushes the
	// member's score to now+visibility without removing it, which is precisely
	// the state pending cannot distinguish from due.
	entries, err := state.LeaseOutbox(ctx, "exec-leased", time.Now().UTC(), 8)
	if err != nil {
		t.Fatalf("LeaseOutbox() error = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("LeaseOutbox() handed out %d entries, want 1", len(entries))
	}

	snapshot, err := state.OutboxMetrics(ctx)
	if err != nil {
		t.Fatalf("OutboxMetrics() error = %v", err)
	}
	if snapshot.Pending != 3 {
		t.Fatalf("Pending = %d, want 3 -- a leased entry stays in the ready index "+
			"until it is acked", snapshot.Pending)
	}
	if snapshot.Ready != 1 {
		t.Fatalf("Ready = %d, want 1 -- only the undelayed, unleased entry is "+
			"deliverable in this tick", snapshot.Ready)
	}
}

// TestOutboxDiscoveryCostTracksTheKeyspaceNotTheBacklog is the honest guard on
// this store's FALLBACK discovery path, and it pins a KNOWN LIMITATION rather
// than a property anyone should be pleased with.
//
// Discovery walks the keyspace with SCAN and filters by pattern, so the keys it
// examines per call is the page, not the matches. A backlog of B ready
// executions therefore takes about (keyspace / page) calls to come all the way
// around, no matter how small B is: growing the number of UNRELATED keys slows
// dispatch down without touching the backlog. That is the defect behind the
// measured dispatch ceiling — the integration reached only a fifth of the work
// it created, and the outbox backlog grew even though delivery was healthy.
//
// This path is now the fallback rather than the only mechanism: the readiness
// index answers discovery in one ZRANGEBYSCORE, and the sweep exists to bound
// how long a registration the index missed can stay invisible. The index is
// therefore switched OFF here, which is both how the fallback is exercised
// deliberately and a faithful model of the deployments that have no index at
// all — see TestOutboxReadyIndexDiscoveryDoesNotTrackTheKeyspace for the same
// measurement with the accelerator in place.
//
// What the test asserts is therefore two things. First, the defect is real and
// measurable here: adding noise keys multiplies the discovery calls needed at a
// fixed page. Second, the page is an effective lever: sized to the keyspace,
// the same backlog is discovered in ONE call with no noise sensitivity at all,
// which is why the dispatcher's page is host-configurable.
//
// A correct index cannot be written atomically with the entry it indexes: every
// outbox mutation is a Lua script over keys that share the execution's {id}
// hash tag, and Redis Cluster refuses a script that touches a key outside that
// slot. The index is therefore a best-effort accelerator, and this sweep is
// what bounds its staleness.
func TestOutboxDiscoveryCostTracksTheKeyspaceNotTheBacklog(t *testing.T) {
	const backlog = 24
	const noise = 4000
	const page = 512

	available := make(map[types.ExecutionID]time.Time, backlog)
	for i := 0; i < backlog; i++ {
		available[types.ExecutionID(fmt.Sprintf("exec-discovery-%03d", i))] = time.Time{}
	}
	state, rdb := newOutboxMetricsTestStore(t, available)
	state.ConfigureOutboxReadyIndex(false)
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)

	hook := &keyspaceScanHook{}
	rdb.AddHook(hook)

	cleanCalls := discoveryCalls(t, ctx, state, page, backlog)

	// Noise of a kind the discovery pattern never matches, named so it sorts
	// BEFORE the execution keys: SCAN walks the keyspace in sorted order, so
	// this is the case where the ready indexes sit behind everything else --
	// which is what a real keyspace looks like, since the ready indexes are a
	// small family among many.
	//
	// Real deployments carry mostly keys of other kinds, which is why the
	// keyspace size and the backlog size have nothing to do with each other.
	for i := 0; i < noise; i++ {
		if err := rdb.Set(ctx, fmt.Sprintf("%s%05d", noiseKeyPrefix, i), "x", time.Hour).Err(); err != nil {
			t.Fatal(err)
		}
	}
	noisyCalls := discoveryCalls(t, ctx, state, page, backlog)

	if noisyCalls <= cleanCalls {
		t.Fatalf("discovery calls at page %d: %d with a clean keyspace, %d with %d unrelated "+
			"keys -- adding keys the discovery pattern cannot match must cost calls, or this "+
			"store's discovery is no longer keyspace-bound and the page knob (and this test) "+
			"need revisiting", page, cleanCalls, noisyCalls, noise)
	}

	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	state, rdb = newOutboxMetricsTestStore(t, available)
	state.ConfigureOutboxReadyIndex(false)
	rdb.AddHook(hook)
	for i := 0; i < noise; i++ {
		if err := rdb.Set(ctx, fmt.Sprintf("%s%05d", noiseKeyPrefix, i), "x", time.Hour).Err(); err != nil {
			t.Fatal(err)
		}
	}
	keyspace, err := rdb.DBSize(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	sized := discoveryCalls(t, ctx, state, int(keyspace), backlog)
	if sized != 1 {
		t.Fatalf("discovery calls with the page sized to the %d-key keyspace = %d, want 1 -- "+
			"a page that covers the keyspace must find the whole backlog in one call, which "+
			"is what makes raising it the supported answer to a stalled backlog",
			keyspace, sized)
	}

	t.Logf("discovery calls to cover %d ready executions at page %d: %d clean, %d with %d "+
		"unrelated keys, %d when the page covers the %d-key keyspace",
		backlog, page, cleanCalls, noisyCalls, noise, sized, keyspace)
}

// discoveryCalls runs discovery until every seeded execution has been seen, and
// reports how many calls that took. A call limit turns "never discovered" into a
// failure rather than a hang.
func discoveryCalls(t *testing.T, ctx context.Context, state *Store, page, backlog int) int {
	t.Helper()
	seen := make(map[types.ExecutionID]struct{}, backlog)
	const callLimit = 4096
	for call := 1; call <= callLimit; call++ {
		ids, err := state.ListOutboxExecutions(ctx, page)
		if err != nil {
			t.Fatalf("ListOutboxExecutions() call %d error = %v", call, err)
		}
		for _, id := range ids {
			seen[id] = struct{}{}
		}
		if len(seen) >= backlog {
			return call
		}
	}
	t.Fatalf("after %d calls only %d of %d executions were discovered", callLimit, len(seen), backlog)
	return 0
}

// noiseKeyPrefix names the unrelated keys. It sorts before the execution key
// family ("xflow:ns:<ns>:exec:..."), which puts the whole ready backlog at the
// far end of the SCAN order.
const noiseKeyPrefix = "xflow:ns:default:aaa-noise:"

// keyspaceScanHook answers SCAN the way a real server does: it walks the WHOLE
// keyspace COUNT keys at a time and filters each page by MATCH, returning the
// cursor to resume from. miniredis's own SCAN ignores COUNT and returns every
// matching key with cursor 0, so it cannot express the property this file is
// about — that the cost of reaching a match is driven by the keys the scan
// examines, not by the keys it returns. Every other command falls through.
type keyspaceScanHook struct {
	mu   sync.Mutex
	scan int
}

func (h *keyspaceScanHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *keyspaceScanHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		scan, ok := cmd.(*redis.ScanCmd)
		if !ok {
			return next(ctx, cmd)
		}
		cursor, pattern, count, err := scanArgs(scan.Args())
		if err != nil {
			scan.SetErr(err)
			return nil
		}
		keys, err := allKeys(ctx, next)
		if err != nil {
			scan.SetErr(err)
			return nil
		}
		h.mu.Lock()
		h.scan++
		h.mu.Unlock()

		start := int(cursor)
		if start > len(keys) {
			start = len(keys)
		}
		end := start + count
		if end > len(keys) {
			end = len(keys)
		}
		nextCursor := uint64(end)
		if end >= len(keys) {
			nextCursor = 0
		}
		var matched []string
		for _, key := range keys[start:end] {
			ok, err := path.Match(pattern, key)
			if err != nil {
				scan.SetErr(fmt.Errorf("match %q: %w", pattern, err))
				return nil
			}
			if ok {
				matched = append(matched, key)
			}
		}
		scan.SetVal(matched, nextCursor)
		return nil
	}
}

func (h *keyspaceScanHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// scanCount reports how many SCAN commands have been issued through this hook,
// which is how a test asserts that a discovery path ran NO keyspace scan at all
// rather than a cheaper one.
func (h *keyspaceScanHook) scanCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.scan
}

// allKeys lists the store's whole keyspace, sorted, which is the order SCAN
// walks in and the order a cursor can meaningfully index.
func allKeys(ctx context.Context, next redis.ProcessHook) ([]string, error) {
	cmd := redis.NewStringSliceCmd(ctx, "keys", "*")
	if err := next(ctx, cmd); err != nil {
		return nil, err
	}
	keys, err := cmd.Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(keys)
	return keys, nil
}

func scanArgs(args []any) (uint64, string, int, error) {
	if len(args) < 2 {
		return 0, "", 0, fmt.Errorf("SCAN missing cursor")
	}
	cursor, err := parseUint(args[1])
	if err != nil {
		return 0, "", 0, fmt.Errorf("parse SCAN cursor %v: %w", args[1], err)
	}
	var pattern string
	count := 10
	for i := 2; i+1 < len(args); i += 2 {
		switch strings.ToLower(fmt.Sprint(args[i])) {
		case "match":
			pattern = fmt.Sprint(args[i+1])
		case "count":
			n, err := parseUint(args[i+1])
			if err != nil {
				return 0, "", 0, fmt.Errorf("parse SCAN count: %w", err)
			}
			count = int(n)
		}
	}
	if pattern == "" {
		return 0, "", 0, fmt.Errorf("SCAN missing MATCH pattern")
	}
	if count <= 0 {
		count = 10
	}
	return cursor, pattern, count, nil
}

func parseUint(v any) (uint64, error) {
	var out uint64
	_, err := fmt.Sscanf(strings.TrimSpace(fmt.Sprint(v)), "%d", &out)
	return out, err
}
