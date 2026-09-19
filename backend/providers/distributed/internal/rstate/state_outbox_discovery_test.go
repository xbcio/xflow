package rstate

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
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

// newOutboxDiscoveryTestStore seeds one ready outbox entry per id (under the
// given namespace) into a miniredis-backed store.
func newOutboxDiscoveryTestStore(t *testing.T, seeds map[namespace.Namespace][]types.ExecutionID) (*Store, *pagedScanHook) {
	t.Helper()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	state := New(rdb, nil, time.Minute)
	hook := &pagedScanHook{}
	for ns, ids := range seeds {
		ctx := namespace.WithNamespace(context.Background(), ns)
		for _, id := range ids {
			if err := state.CreateExecutionWithOutbox(ctx, &engine.ExecutionSnapshot{
				ID:     id,
				Status: types.ExecutionStatusRunning,
				Graph:  testGraphTwoNode(),
			}, []engine.OutboxEntry{{
				ID:          string(id) + "/start/0",
				Task:        engine.Task{ExecutionID: id, NodeName: "start", Type: engine.TaskTypeNodeExec},
				AvailableAt: time.Now().Add(-time.Second),
			}}); err != nil {
				t.Fatalf("seed %s/%s: %v", ns, id, err)
			}
		}
	}
	rdb.AddHook(hook)
	return state, hook
}

// TestListOutboxExecutionsResumesDiscoveryFromStoredCursor pins the discovery
// contract the dispatcher relies on when the readiness index is NOT carrying the
// load: a store with the index disabled, a deployment whose registrations are
// failing, or an index whose read errored. The scan used to materialize every
// matching key and truncate the sorted result to the limit, so with more
// executions than the limit the same lowest IDs came back on every tick and the
// rest were never discovered. A page that resumes from the cursor saved by the
// previous tick is what makes them reachable.
//
// The index is switched off deliberately: with it on, discovery is answered by
// one ZRANGEBYSCORE and the cursor below never advances, because the sweep is
// the path under test here. See state_outbox_index_test.go for the accelerator.
//
// miniredis cannot express this: its SCAN ignores COUNT and always returns every
// matching key with cursor 0. The pagedScanHook supplies faithful SCAN paging
// while the keys themselves are real.
func TestListOutboxExecutionsResumesDiscoveryFromStoredCursor(t *testing.T) {
	const execs = 6
	const limit = 2
	ids := make([]types.ExecutionID, execs)
	for i := range ids {
		ids[i] = types.ExecutionID(fmt.Sprintf("exec-discover-%02d", i))
	}
	state, hook := newOutboxDiscoveryTestStore(t, map[namespace.Namespace][]types.ExecutionID{
		namespace.Default: ids,
	})
	state.ConfigureOutboxReadyIndex(false)

	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	var seen []types.ExecutionID
	// Six keys at two per page: three calls cover the namespace exactly once.
	for call := 0; call < 3; call++ {
		page, err := state.ListOutboxExecutions(ctx, limit)
		if err != nil {
			t.Fatalf("ListOutboxExecutions() call %d error = %v", call+1, err)
		}
		if len(page) > limit {
			t.Fatalf("ListOutboxExecutions() call %d returned %d ids, want at most %d -- a "+
				"single namespace must not exceed its budget", call+1, len(page), limit)
		}
		seen = append(seen, page...)
	}

	want := []types.ExecutionID{ids[0], ids[1], ids[2], ids[3], ids[4], ids[5]}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("discovered %v, want %v in order -- each tick must resume from the cursor "+
			"the previous tick stored, or the same lowest IDs are rediscovered forever", seen, want)
	}
	if got, want := hook.cursorsFor(namespace.Default, "outbox:ready"), []uint64{0, 2, 4}; !reflect.DeepEqual(got, want) {
		t.Fatalf("SCAN cursors = %v, want %v -- the cursor returned by one call must be the "+
			"one fed to the next", got, want)
	}
}

// TestListOutboxExecutionsDoesNotStarveLaterNamespaces pins the sharding rule.
// Namespaces are scanned in sorted order, so with one shared page counter the
// first namespace's backlog filled it and the loop stopped: every later
// namespace was never scanned, and because its cursor therefore never advanced
// it could not be discovered on any later tick either. A namespace's entries
// have to surface even while an earlier-sorting namespace is saturated.
//
// Like the cursor test, this is the sweep path with the readiness index off.
func TestListOutboxExecutionsDoesNotStarveLaterNamespaces(t *testing.T) {
	const limit = 4
	acme := make([]types.ExecutionID, 12)
	for i := range acme {
		acme[i] = types.ExecutionID(fmt.Sprintf("exec-acme-%02d", i))
	}
	// "acme" sorts before "default", so it is scanned first on every call.
	state, hook := newOutboxDiscoveryTestStore(t, map[namespace.Namespace][]types.ExecutionID{
		"acme":            acme,
		namespace.Default: {"exec-default-0"},
	})
	state.ConfigureOutboxReadyIndex(false)

	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	seenDefault := false
	for call := 0; call < 8; call++ {
		page, err := state.ListOutboxExecutions(ctx, limit)
		if err != nil {
			t.Fatalf("ListOutboxExecutions() call %d error = %v", call+1, err)
		}
		// With two namespaces each gets half the page; the aggregate stays at the
		// limit because there are fewer namespaces than slots.
		if len(page) > limit {
			t.Fatalf("ListOutboxExecutions() call %d returned %d ids, want at most %d",
				call+1, len(page), limit)
		}
		for _, id := range page {
			if id == "exec-default-0" {
				seenDefault = true
			}
		}
	}

	if !seenDefault {
		t.Fatalf("the default namespace was never discovered across 8 calls while acme had a "+
			"backlog; acme cursors = %v, default cursors = %v (empty means default's cursor "+
			"never advanced and its ready entries stayed invisible)",
			hook.cursorsFor("acme", "outbox:ready"), hook.cursorsFor(namespace.Default, "outbox:ready"))
	}
	if got := hook.cursorsFor(namespace.Default, "outbox:ready"); len(got) == 0 {
		t.Fatal("the default namespace was scanned zero times")
	}
}

func TestSplitNamespaceScanBudget(t *testing.T) {
	tests := []struct {
		name       string
		limit      int
		namespaces int
		want       []int
	}{
		{name: "no namespaces", limit: 10, namespaces: 0, want: nil},
		{name: "even split", limit: 6, namespaces: 3, want: []int{2, 2, 2}},
		{name: "remainder goes to earliest", limit: 7, namespaces: 3, want: []int{3, 2, 2}},
		{name: "single namespace takes the page", limit: 5, namespaces: 1, want: []int{5}},
		{name: "more namespaces than limit still each get one", limit: 2, namespaces: 5, want: []int{1, 1, 1, 1, 1}},
		{name: "equal count and limit", limit: 3, namespaces: 3, want: []int{1, 1, 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitNamespaceScanBudget(tt.limit, tt.namespaces)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("splitNamespaceScanBudget(%d, %d) = %v, want %v", tt.limit, tt.namespaces, got, tt.want)
			}
			for i, b := range got {
				if b < 1 {
					t.Fatalf("slot %d = %d, want at least 1 -- a zero slot freezes that "+
						"namespace's cursor and it is never discovered", i, b)
				}
			}
		})
	}
}

// pagedScanHook answers SCAN the way a real server does: it keeps a key space,
// filters by the MATCH pattern, and walks it COUNT keys at a time, returning the
// next cursor. Every other command falls through to miniredis.
//
// miniredis's own SCAN ignores COUNT and returns everything with cursor 0, which
// cannot exercise cursor resumption or per-namespace budgets.
type pagedScanHook struct {
	mu sync.Mutex
	// calls records the (pattern, cursor) pairs SCAN was asked for, in order.
	calls []scanCall
}

type scanCall struct {
	pattern string
	cursor  uint64
	count   int64
}

func (h *pagedScanHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *pagedScanHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		scan, ok := cmd.(*redis.ScanCmd)
		if !ok {
			return next(ctx, cmd)
		}
		call, err := scanCallFromArgs(scan.Args())
		if err != nil {
			scan.SetErr(err)
			return nil
		}
		// The keys live in the wrapped miniredis; read them without disturbing
		// the command's own cursor argument.
		keys, err := nextKeys(ctx, next, call.pattern)
		if err != nil {
			scan.SetErr(err)
			return nil
		}
		h.mu.Lock()
		h.calls = append(h.calls, call)
		h.mu.Unlock()

		start := int(call.cursor)
		if start > len(keys) {
			scan.SetErr(fmt.Errorf("cursor %d past end of %d keys for %q", start, len(keys), call.pattern))
			return nil
		}
		end := start + int(call.count)
		if end > len(keys) {
			end = len(keys)
		}
		next := uint64(end)
		if end >= len(keys) {
			next = 0
		}
		scan.SetVal(keys[start:end], next)
		return nil
	}
}

func (h *pagedScanHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *pagedScanHook) cursorsFor(ns namespace.Namespace, suffix string) []uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	pattern := execScanPattern(ns, suffix)
	var out []uint64
	for _, call := range h.calls {
		if call.pattern == pattern {
			out = append(out, call.cursor)
		}
	}
	return out
}

// nextKeys lists the store's keys matching pattern. It issues KEYS against the
// wrapped client so the result reflects whatever miniredis actually holds.
func nextKeys(ctx context.Context, next redis.ProcessHook, pattern string) ([]string, error) {
	cmd := redis.NewStringSliceCmd(ctx, "keys", pattern)
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

func scanCallFromArgs(args []any) (scanCall, error) {
	if len(args) < 2 {
		return scanCall{}, errors.New("SCAN command missing cursor")
	}
	cursor, err := strconv.ParseUint(strings.TrimSpace(fmt.Sprint(args[1])), 10, 64)
	if err != nil {
		return scanCall{}, fmt.Errorf("parse SCAN cursor %v: %w", args[1], err)
	}
	call := scanCall{cursor: cursor}
	for i := 2; i+1 < len(args); i += 2 {
		switch strings.ToLower(fmt.Sprint(args[i])) {
		case "match":
			call.pattern = fmt.Sprint(args[i+1])
		case "count":
			call.count, err = strconv.ParseInt(fmt.Sprint(args[i+1]), 10, 64)
			if err != nil {
				return scanCall{}, fmt.Errorf("parse SCAN count: %w", err)
			}
		}
	}
	if call.pattern == "" {
		return scanCall{}, errors.New("SCAN command missing MATCH pattern")
	}
	if call.count <= 0 {
		return scanCall{}, fmt.Errorf("SCAN count = %d, want positive", call.count)
	}
	return call, nil
}
