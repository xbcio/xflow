package rstate

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// This file is the durable-backend half of U-9 (see sdk/xflow/
// execution_error_details_test.go for the consumer-surface half).
//
// The in-memory backend stores engine.NodeSnapshot by value, so a new field on
// that struct needs no work there. Redis does not: the node's error text lives
// in a meta hash written by commitNodeLua, and a field that is only added to
// the Go struct is persisted by nothing and read back as nil. These tests pin
// the Redis write/read pair for both writers of that hash — the fenced commit
// (normal node failure) and UpsertNode (cancel/recovery snapshots).
//
// No SQL assertion is included, deliberately. store.NodeRecord has no error
// column at all: the SQL projection is an audit trail for the EXECUTION-level
// reason (xflow_executions.error_msg), not a node-level store, and GetNode
// never reads it. Adding a column would have been the disproportionate answer
// to "make the detail durable" — the Redis meta hash already IS the durable
// node snapshot that the inspect read path loads.

func errorDetailsRedisGraph(t *testing.T, nodeName string) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "redis-error-details",
		Nodes: []types.NodeDef{{Name: nodeName, Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

// TestRedisCommitNodePersistsErrorDetails proves a failed node's structured
// detail survives the fenced commit and comes back off GetNode.
func TestRedisCommitNodePersistsErrorDetails(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("redis-error-details")
	nodeName := "denied"
	g := errorDetailsRedisGraph(t, nodeName)

	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	idx, ok := g.NodeIndex(nodeName)
	if !ok {
		t.Fatalf("node %q missing from graph", nodeName)
	}

	lease := privateRedisLease(id, nodeName, idx, "lease-details", "token-details")
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	lease.Attempt = 1

	details := map[string]any{
		"source":       "endpoint_allowlist",
		"rejected_url": "https://denied.test/path",
	}
	if _, err := state.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID:  id,
		NodeName:     nodeName,
		NodeIdx:      idx,
		ActivationID: lease.Task.ActivationID,
		LeaseID:      lease.LeaseID,
		LeaseToken:   lease.LeaseToken,
		Attempt:      lease.Attempt,
		Status:       types.NodeStatusFailed,
		Error:        "browser.host_denied: browser destination is denied by policy",
		ErrorDetails: details,
		AllowCycles:  false,
		Fatal:        true,
		AdvanceTask:  nil,
	}); err != nil {
		t.Fatalf("CommitNode() error = %v", err)
	}

	snap, err := state.GetNode(ctx, id, nodeName)
	if err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}
	if snap == nil {
		t.Fatal("GetNode() = nil, want the committed node snapshot")
	}
	if got, want := snap.ErrorDetails["source"], "endpoint_allowlist"; got != want {
		t.Fatalf("GetNode() ErrorDetails[source] = %#v, want %#v (full details %#v); "+
			"the meta hash round trip dropped the field", got, want, snap.ErrorDetails)
	}
	if got, want := snap.ErrorDetails["rejected_url"], "https://denied.test/path"; got != want {
		t.Errorf("GetNode() ErrorDetails[rejected_url] = %#v, want %#v", got, want)
	}

	// The raw hash is asserted too: it is what a second replica reads, and it
	// is where a renamed or mis-indexed Lua argument would show up as a value
	// landing under the wrong field.
	raw, err := state.rdb.HGet(ctx, nodeMetaKey(namespace.FromContext(ctx), id, nodeName), "error_details").Result()
	if err != nil {
		t.Fatalf("HGET error_details: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatalf("stored error_details is not JSON (%q): %v", raw, err)
	}
	if got, want := decoded["source"], "endpoint_allowlist"; got != want {
		t.Fatalf("stored error_details[source] = %#v, want %#v (raw %s)", got, want, raw)
	}
}

// TestRedisCommitNodeAlwaysWritesErrorDetailsSlot pins the one property that
// makes a stale detail impossible: commitNodeLua HSETs error_details on EVERY
// accepted commit, including a detail-free success, rather than only when it
// has something to say.
//
// HExists is asserted rather than HGet because the two states it distinguishes
// are the whole point. HGET cannot tell "the field was written as empty" from
// "the field was never written" - both return "" - so a conditional write
// would satisfy a value-based assertion while leaving a later re-attempt to
// read whatever a previous attempt had stored.
func TestRedisCommitNodeAlwaysWritesErrorDetailsSlot(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	id := types.ExecutionID("redis-detail-slot")
	nodeName := "clean"
	g := errorDetailsRedisGraph(t, nodeName)

	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	idx, _ := g.NodeIndex(nodeName)

	lease := privateRedisLease(id, nodeName, idx, "lease-clean", "token-clean")
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	lease.Attempt = 1
	if _, err := state.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID: id, NodeName: nodeName, NodeIdx: idx,
		LeaseID: lease.LeaseID, LeaseToken: lease.LeaseToken, Attempt: lease.Attempt,
		Status: types.NodeStatusSuccess,
		Output: map[string]any{"ok": true}, StoreOutput: true,
	}); err != nil {
		t.Fatalf("CommitNode() error = %v", err)
	}

	metaKey := nodeMetaKey(namespace.FromContext(ctx), id, nodeName)
	exists, err := rdb.HExists(ctx, metaKey, "error_details").Result()
	if err != nil {
		t.Fatalf("HEXISTS error_details: %v", err)
	}
	if !exists {
		t.Fatal("error_details is absent from the node meta hash after a detail-free commit; " +
			"the slot is written unconditionally so a later attempt cannot read a stale value")
	}
	value, err := rdb.HGet(ctx, metaKey, "error_details").Result()
	if err != nil {
		t.Fatalf("HGET error_details: %v", err)
	}
	if value != "" {
		t.Fatalf("error_details = %q after a detail-free commit, want empty", value)
	}

	snap, err := state.GetNode(ctx, id, nodeName)
	if err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}
	if len(snap.ErrorDetails) != 0 {
		t.Fatalf("GetNode() ErrorDetails = %#v, want empty", snap.ErrorDetails)
	}
}

// TestRedisUpsertNodePersistsErrorDetails covers the second writer of the meta
// hash. UpsertNode's meta block is entered only when some OTHER field is
// non-zero, so a snapshot carrying nothing but a detail is the case that would
// silently drop it if the guard were not extended.
func TestRedisUpsertNodePersistsErrorDetails(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	id := types.ExecutionID("redis-upsert-error-details")

	err := state.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID:  id,
		Name:         "canceled-node",
		Status:       types.NodeStatusCanceled,
		ErrorDetails: map[string]any{"source": "navigation_policy"},
	})
	if err != nil {
		t.Fatalf("UpsertNode() error = %v", err)
	}

	snap, err := state.GetNode(ctx, id, "canceled-node")
	if err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}
	if snap == nil {
		t.Fatal("GetNode() = nil, want the upserted snapshot")
	}
	if got, want := snap.ErrorDetails["source"], "navigation_policy"; got != want {
		t.Fatalf("GetNode() ErrorDetails[source] = %#v, want %#v", got, want)
	}
}

// TestRedisGetNodeRejectsMalformedErrorDetails pins the decode fault path.
// A corrupt hash must surface as an error rather than silently degrading to a
// nil map, which is indistinguishable from "this failure had no detail" — the
// exact ambiguity this field was added to remove.
func TestRedisGetNodeRejectsMalformedErrorDetails(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	id := types.ExecutionID("redis-bad-error-details")
	name := "broken"
	t_ns := namespace.FromContext(ctx)

	if err := rdb.Set(ctx, nodeStatusKey(t_ns, id, name), string(types.NodeStatusFailed), time.Minute).Err(); err != nil {
		t.Fatalf("seed node status: %v", err)
	}
	if err := rdb.HSet(ctx, nodeMetaKey(t_ns, id, name), "error_details", "{not json").Err(); err != nil {
		t.Fatalf("seed node meta: %v", err)
	}

	if _, err := state.GetNode(ctx, id, name); err == nil {
		t.Fatal("GetNode() error = nil, want a decode failure for malformed error_details")
	}
}
