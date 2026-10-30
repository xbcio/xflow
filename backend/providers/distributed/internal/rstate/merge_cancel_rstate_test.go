package rstate

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// The tests in this file drive the Redis store directly. The engine has no
// consumer for TaskTypeMergeCancel intents yet, so none of them may let an
// engine flush the outbox. Where a test builds an engine it only asks it to
// build one lease and never calls FlushOutbox.

const mcrReason = "canceled by merge"

// mcrExec creates a running execution over the single-node error-details graph
// and returns its id, node index and unit index.
func mcrExec(t *testing.T, state *Store, id types.ExecutionID, node string) (int, int) {
	t.Helper()
	g := errorDetailsRedisGraph(t, node)
	if err := state.CreateExecution(context.Background(), &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution(%s) error = %v", id, err)
	}
	idx, ok := g.NodeIndex(node)
	if !ok {
		t.Fatalf("node %q is not in the graph", node)
	}
	return idx, g.UnitIndexForNode(idx)
}

// mcrSystemCommit is a lease-less System commit of the given status, the shape
// every fenced System commit shares.
func mcrSystemCommit(id types.ExecutionID, node string, nodeIdx, unitIdx int, status types.NodeStatus) engine.CommitNodeRequest {
	req := engine.CommitNodeRequest{
		ExecutionID: id, NodeName: node, NodeIdx: nodeIdx, UnitIdx: unitIdx,
		ActivationID: 1, Status: status, System: true,
	}
	switch status {
	case types.NodeStatusCanceled:
		req.Error = mcrReason
	case types.NodeStatusPinned, types.NodeStatusSkipped:
		req.Port = "main"
	}
	return req
}

// mcrDump captures every key of one execution as comparable text, so a test can
// prove a refused commit changed nothing. TTLs are left out on purpose: they
// move with the clock and say nothing about the data.
func mcrDump(t *testing.T, rdb redis.UniversalClient, id types.ExecutionID) map[string]string {
	t.Helper()
	ctx := context.Background()
	keys, err := rdb.Keys(ctx, execScanPattern(namespace.Default, "*")).Result()
	if err != nil {
		t.Fatalf("KEYS error = %v", err)
	}
	own := "{" + string(id) + "}"
	out := map[string]string{}
	for _, key := range keys {
		if !strings.Contains(key, own) {
			continue
		}
		kind, err := rdb.Type(ctx, key).Result()
		if err != nil {
			t.Fatalf("TYPE %s error = %v", key, err)
		}
		var body string
		switch kind {
		case "string":
			body = rdb.Get(ctx, key).Val()
		case "hash":
			all := rdb.HGetAll(ctx, key).Val()
			parts := make([]string, 0, len(all))
			for k, v := range all {
				parts = append(parts, k+"="+v)
			}
			sort.Strings(parts)
			body = strings.Join(parts, "|")
		case "zset":
			parts := []string{}
			for _, z := range rdb.ZRangeWithScores(ctx, key, 0, -1).Val() {
				parts = append(parts, fmt.Sprintf("%v@%v", z.Member, z.Score))
			}
			body = strings.Join(parts, "|")
		case "set":
			members := rdb.SMembers(ctx, key).Val()
			sort.Strings(members)
			body = strings.Join(members, "|")
		default:
			body = "<" + kind + ">"
		}
		out[key] = kind + ":" + body
	}
	return out
}

func mcrRemaining(t *testing.T, rdb redis.UniversalClient, id types.ExecutionID) int {
	t.Helper()
	raw, err := rdb.Get(context.Background(), remainingNodesKey(namespace.Default, id)).Result()
	if err != nil {
		t.Fatalf("GET remaining error = %v", err)
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		t.Fatalf("remaining %q is not a number: %v", raw, err)
	}
	return n
}

// TestRedisSystemFenceShapes pins the legal System commit shapes of a unit
// against its scheduling marker. The first three rows are the shapes that
// existed before the canceled one (pinned, skipped, skipped with the skip
// marker) and must keep behaving exactly as they did; the canceled rows are the
// fourth shape and its refusals. A refused row must leave every key of the
// execution untouched, which is what makes a refusal safe to retry.
func TestRedisSystemFenceShapes(t *testing.T) {
	cases := []struct {
		name   string
		status types.NodeStatus
		marker string // value of the unit's schedule "action" field; "" = none
		want   bool
	}{
		{"pinned without a marker is accepted", types.NodeStatusPinned, "", true},
		{"pinned with an execute marker is accepted", types.NodeStatusPinned, "execute", true},
		{"pinned on a skip-marked unit is refused", types.NodeStatusPinned, "skip", false},
		{"skipped without a marker is accepted", types.NodeStatusSkipped, "", true},
		{"skipped with an execute marker is accepted", types.NodeStatusSkipped, "execute", true},
		{"skipped with the skip marker is accepted", types.NodeStatusSkipped, "skip", true},
		{"success without the skip marker is refused", types.NodeStatusSuccess, "", false},
		{"success with an execute marker is refused", types.NodeStatusSuccess, "execute", false},
		{"success with the skip marker is accepted", types.NodeStatusSuccess, "skip", true},
		{"canceled with an execute marker is accepted", types.NodeStatusCanceled, "execute", true},
		{"canceled without a marker is refused", types.NodeStatusCanceled, "", false},
		{"canceled with the skip marker is refused", types.NodeStatusCanceled, "skip", false},
		{"canceled with a seed marker is refused", types.NodeStatusCanceled, "seed", false},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			state, _, rdb := newTestRedisState(t)
			id := types.ExecutionID(fmt.Sprintf("mcr-shape-%d", i))
			nodeIdx, unitIdx := mcrExec(t, state, id, "alpha")
			if tc.marker != "" {
				key := scheduleKey(namespace.Default, id, unitIdx)
				if err := rdb.HSet(ctx, key, "action", tc.marker).Err(); err != nil {
					t.Fatalf("HSET schedule error = %v", err)
				}
			}
			before := mcrDump(t, rdb, id)
			remainingBefore := mcrRemaining(t, rdb, id)

			res, err := state.CommitNode(ctx, mcrSystemCommit(id, "alpha", nodeIdx, unitIdx, tc.status))
			if err != nil {
				t.Fatalf("CommitNode(%s) error = %v", tc.status, err)
			}
			if tc.want {
				if res.Outcome != engine.CommitOutcomeAccepted || !res.Applied {
					t.Fatalf("CommitNode(%s, marker %q) = %+v, want accepted", tc.status, tc.marker, res)
				}
				node, err := state.GetNode(ctx, id, "alpha")
				if err != nil || node == nil || node.Status != tc.status {
					t.Fatalf("node after the commit = %+v, %v; want %s", node, err, tc.status)
				}
				if got := mcrRemaining(t, rdb, id); got != remainingBefore-1 && !res.ExecutionDone {
					t.Fatalf("remaining = %d, want %d", got, remainingBefore-1)
				}
				return
			}
			if res.Outcome != engine.CommitOutcomeStaleToken || res.Applied {
				t.Fatalf("CommitNode(%s, marker %q) = %+v, want a refusal", tc.status, tc.marker, res)
			}
			after := mcrDump(t, rdb, id)
			if len(after) != len(before) {
				t.Fatalf("a refused commit changed the key set: before %d keys, after %d", len(before), len(after))
			}
			for key, was := range before {
				if after[key] != was {
					t.Fatalf("a refused commit changed %s:\n before %s\n after  %s", key, was, after[key])
				}
			}
		})
	}
}

// mcrLogger records the Error calls a store makes. Every other level is
// dropped: the tests only need to know that a failure was reported.
type mcrLogger struct {
	mu     sync.Mutex
	errors []string
}

func (l *mcrLogger) Error(msg string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errors = append(l.errors, msg)
}
func (l *mcrLogger) recorded() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.errors...)
}
func (l *mcrLogger) Debug(string, ...any)  {}
func (l *mcrLogger) Debugf(string, ...any) {}
func (l *mcrLogger) Info(string, ...any)   {}
func (l *mcrLogger) Infof(string, ...any)  {}
func (l *mcrLogger) Warn(string, ...any)   {}
func (l *mcrLogger) Warnf(string, ...any)  {}
func (l *mcrLogger) Errorf(string, ...any) {}
func (l *mcrLogger) Panic(string, ...any)  {}
func (l *mcrLogger) Panicf(string, ...any) {}

var _ engine.Logger = (*mcrLogger)(nil)
var _ = errors.New
var _ = time.Second

// mcrPark parks the single node of a fresh execution on one signal through the
// durable suspend transition, the only suspend path the engine drives.
func mcrPark(t *testing.T, state *Store, id types.ExecutionID, mode types.SuspendMode, names ...string) *engine.TaskLease {
	t.Helper()
	ctx := context.Background()
	nodeIdx, unitIdx := mcrExec(t, state, id, "alpha")
	lease := &engine.TaskLease{
		LeaseID:    engine.LeaseID(fmt.Sprintf("lease-%s", id)),
		LeaseToken: engine.LeaseToken(fmt.Sprintf("token-%s", id)),
		IssuedAt:   time.Now().UTC().Truncate(time.Millisecond),
		TTL:        time.Minute,
		Task: engine.Task{
			ExecutionID: id, NodeName: "alpha", NodeIdx: nodeIdx,
			UnitIdx:      unitIdx,
			Type:         engine.TaskTypeNodeExec,
			ActivationID: 1,
		},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease acquired=%v err=%v", acquired, err)
	}
	node, err := state.GetNode(ctx, id, "alpha")
	if err != nil || node == nil {
		t.Fatalf("GetNode = %+v, %v", node, err)
	}
	lease.Attempt = node.Attempt
	if _, claimed, err := state.ClaimTaskLease(ctx, lease); err != nil || !claimed {
		t.Fatalf("ClaimTaskLease claimed=%v err=%v", claimed, err)
	}
	spec := &types.SuspendSpec{Mode: mode, Signals: names, Quorum: 1}
	committed, err := state.SuspendTaskLeaseWithOutbox(ctx, lease, nil, false, false, spec, "")
	if err != nil || !committed {
		t.Fatalf("SuspendTaskLeaseWithOutbox committed=%v err=%v", committed, err)
	}
	return lease
}

// mcrCanceledCommit is the canceled System commit for the parked node.
func mcrCanceledCommit(state *Store, lease *engine.TaskLease) engine.CommitNodeRequest {
	task := lease.Task
	return engine.CommitNodeRequest{
		ExecutionID: task.ExecutionID, NodeName: task.NodeName,
		NodeIdx: task.NodeIdx, UnitIdx: task.UnitIdx,
		ActivationID: 1, Status: types.NodeStatusCanceled, System: true, Error: mcrReason,
	}
}

// TestRedisCanceledSuspendedNodeRetiresItsWaiter walks the waiter lifecycle of
// a canceled single-signal node: the park stores a waiter spec (which is how
// any cleanup can find the signal-named keys), the cancel retires the waiter
// and the spec, and a late signal is stored with no resume written.
func TestRedisCanceledSuspendedNodeRetiresItsWaiter(t *testing.T) {
	ctx := context.Background()
	state, _, rdb := newTestRedisState(t)
	id := types.ExecutionID("mcr-waiter-retire")
	lease := mcrPark(t, state, id, types.ModeSignal, "approve")

	res, err := state.CommitNode(ctx, mcrCanceledCommit(state, lease))
	if err != nil || res.Outcome != engine.CommitOutcomeAccepted {
		t.Fatalf("CommitNode(canceled) = %+v, %v; want accepted", res, err)
	}
	if n, _ := rdb.Exists(ctx,
		waiterKey(namespace.Default, id, "approve"),
		waiterSpecKey(namespace.Default, id, "alpha"),
		signalBatchKey(namespace.Default, id, "alpha"),
		resumeLockKey(namespace.Default, id, "alpha"),
	).Result(); n != 0 {
		t.Fatalf("%d waiter-side keys survived the cancel, want none", n)
	}
	if members, _ := rdb.SMembers(ctx, suspendedNodesKey(namespace.Default, id)).Result(); len(members) != 0 {
		t.Fatalf("suspended_nodes = %v after the cancel, want empty", members)
	}
	if n, _ := rdb.ZCard(ctx, timeoutZSetKey(namespace.Default, id)).Result(); n != 0 {
		t.Fatalf("timeout ZSET holds %d members after the cancel, want none", n)
	}

	// A late signal finds no waiter: it is stored for a wait that will never
	// come, and no resume intent is written.
	if node, _, err := state.DeliverSignal(ctx, id, "approve", map[string]any{"by": "late"}); err != nil || node != "" {
		t.Fatalf("DeliverSignal after the cancel = %q, %v; want no resumed node", node, err)
	}
	if n, _ := rdb.HLen(ctx, outboxBodyKey(namespace.Default, id)).Result(); n != 0 {
		t.Fatalf("outbox holds %d entries after the late signal, want none", n)
	}
	node, err := state.GetNode(ctx, id, "alpha")
	if err != nil || node == nil || node.Status != types.NodeStatusCanceled {
		t.Fatalf("node after the late signal = %+v, %v; want canceled", node, err)
	}
}

// TestRedisCanceledMultiSignalNodeRetiresItsWaiters covers the same retirement
// for a multi-signal park, whose spec the suspend script always stored.
func TestRedisCanceledMultiSignalNodeRetiresItsWaiters(t *testing.T) {
	ctx := context.Background()
	state, _, rdb := newTestRedisState(t)
	id := types.ExecutionID("mcr-waiter-multi")
	lease := mcrPark(t, state, id, types.ModeMultiSignal, "a", "b")

	res, err := state.CommitNode(ctx, mcrCanceledCommit(state, lease))
	if err != nil || res.Outcome != engine.CommitOutcomeAccepted {
		t.Fatalf("CommitNode(canceled) = %+v, %v; want accepted", res, err)
	}
	if n, _ := rdb.Exists(ctx,
		waiterKey(namespace.Default, id, "a"),
		waiterKey(namespace.Default, id, "b"),
		waiterSpecKey(namespace.Default, id, "alpha"),
		signalBatchKey(namespace.Default, id, "alpha"),
	).Result(); n != 0 {
		t.Fatalf("%d waiter-side keys survived the cancel, want none", n)
	}
}

// TestRedisMergeCancelWriteFailureIsLogged proves the pipeline failure is
// swallowed but reported: the cancel stays accepted (it is durable either way)
// and the store logs the cleanup failure so an operator can see it.
func TestRedisMergeCancelWriteFailureIsLogged(t *testing.T) {
	ctx := context.Background()
	state, mr, _ := newTestRedisState(t)
	logger := &mcrLogger{}
	state.SetLogger(logger)
	id := types.ExecutionID("mcr-log-failure")
	mcrPark(t, state, id, types.ModeSignal, "approve")

	// Close Redis under the store after the park, then drive the retire path
	// directly: with the server gone the commit's own Lua would fail before the
	// cleanup ever runs, so this aims at the pipeline contract itself — the
	// failure is swallowed (nil return) and reported to the logger.
	mr.Close()
	state.retireCanceledWaiter(ctx, id, "alpha")
	if len(logger.recorded()) == 0 {
		t.Fatalf("no error was logged; a swallowed cleanup failure must be reported")
	}
}
