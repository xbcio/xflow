package rstate

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

const (
	inventoryActiveTTL     = time.Minute
	inventoryCompletionTTL = 30 * time.Second
)

// inventoryGraphWithConnection builds the fixture graph used by the inventory
// test: two units with one data edge between them. The edge matters — creation
// seeds an in-degree counter only for units with a non-zero in-degree, so a
// connection-free graph would leave that half of the inventory unexercised.
func inventoryGraphWithConnection() *graph.Graph {
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "inventory-two-node-chain",
		Nodes: []types.NodeDef{{Name: "start", Type: "test.echo"}, {Name: "end", Type: "test.echo"}},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "end", Input: "main"}}}},
		},
	})
	if err != nil {
		panic(err)
	}
	return g
}

// TestTransientExecutionKeysIncludesCountersWithoutGraph pins the completion-TTL
// inventory to the completion counters even when the cached graph is missing.
// They are execution-scoped and safe to expire; omitting them when LoadGraph
// cannot supply a graph would leave a finished execution's counters on the
// active TTL, which is the regression the shared creation inventory introduced.
func TestTransientExecutionKeysIncludesCountersWithoutGraph(t *testing.T) {
	id := types.ExecutionID("exec-inventory-no-graph")
	keys := transientExecutionKeys(namespace.Default, id, nil)
	want := map[string]bool{
		remainingNodesKey(namespace.Default, id): false,
		failedNodesKey(namespace.Default, id):    false,
	}
	for _, key := range keys {
		if _, ok := want[key]; ok {
			want[key] = true
		}
	}
	for key, found := range want {
		if !found {
			t.Errorf("transientExecutionKeys(nil graph) omits %q; completion shortening must cover the counters even without a graph", key)
		}
	}
}

// TestCreatedExecutionKeyInventoryCoverage pins the single creation-key
// inventory that cleanupCreatedExecution and completion-time TTL shortening
// share. It drives the real createExecution path (miniredis, no SQL) for
// snapshot shapes whose optional keys used to be written but never rolled back
// or shortened, then asserts three properties per shape:
//
//  1. every execution-scoped key createExecution wrote is enumerated by
//     createdExecutionKeys (the inventory cannot under-cover creation). The
//     fixture graph carries one data edge, so creation seeds an in-degree
//     counter and the unit-indexed half of the inventory is exercised too;
//  2. terminal TTL shortening brings every inventory key down to the
//     completion TTL -- except the transient marker and retention record,
//     which deliberately keep their own lifetimes;
//  3. rollback deletes every execution-scoped key that creation wrote, leaving
//     no residue. No transition runs here, so this does not exercise the extra
//     transition-raced keys cleanupCreatedExecution enumerates.
func TestCreatedExecutionKeyInventoryCoverage(t *testing.T) {
	cases := []struct {
		name          string
		mutate        func(*engine.ExecutionSnapshot)
		withSkipEntry bool
		wantSuffixes  []string
	}{
		{
			name: "minimal",
			wantSuffixes: []string{
				"status", "graph", "remaining_nodes", "failed_nodes", "indegree:1",
			},
		},
		{
			name: "rerun-derived",
			mutate: func(e *engine.ExecutionSnapshot) {
				e.ParentID = types.ExecutionID("rerun-parent")
				e.RerunFrom = "start"
			},
			wantSuffixes: []string{"parent", "rerun_from"},
		},
		{
			name: "subworkflow-child",
			mutate: func(e *engine.ExecutionSnapshot) {
				e.ParentID = types.ExecutionID("subworkflow-parent")
				e.ParentNode = "start"
				e.Depth = 2
			},
			wantSuffixes: []string{"parent", "parent_node", "depth"},
		},
		{
			name: "all-optional",
			mutate: func(e *engine.ExecutionSnapshot) {
				e.Namespace = "default"
				e.Params = map[string]any{"k": "v"}
				e.Runtime = &types.Runtime{Vars: map[string]any{"r": 1}}
				e.Scope = map[string]any{"$item": "x"}
				e.TestRun = true
				e.TraceID = "trace-1"
				e.SpanID = "span-1"
				e.TraceCarrier = map[string]string{"traceparent": "00-abc-def-01"}
				e.ParentID = types.ExecutionID("all-parent")
				e.ParentNode = "start"
				e.Depth = 1
				e.RerunFrom = "end"
				e.Deadline = time.UnixMilli(1_900_000_000_123).UTC()
			},
			withSkipEntry: true,
			wantSuffixes: []string{
				"namespace", "params", "runtime", "scope", "test_run",
				"trace_id", "span_id", "trace_carrier", "parent",
				"parent_node", "depth", "rerun_from", "deadline_ms", "schedule:0",
				"outbox:ready", "outbox:body",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, err := miniredis.Run()
			if err != nil {
				t.Fatalf("miniredis.Run() error = %v", err)
			}
			t.Cleanup(srv.Close)
			rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
			t.Cleanup(func() { _ = rdb.Close() })
			state := New(rdb, nil, time.Hour)

			ctx := engine.WithExecutionTransient(context.Background(), engine.TransientHint{
				TTL:           inventoryActiveTTL,
				CompletionTTL: inventoryCompletionTTL,
			})
			id := types.ExecutionID("exec-inventory-" + tc.name)
			e := engine.ExecutionSnapshot{
				ID:     id,
				Status: types.ExecutionStatusRunning,
				Graph:  inventoryGraphWithConnection(),
			}
			if tc.mutate != nil {
				tc.mutate(&e)
			}
			var entries []engine.OutboxEntry
			if tc.withSkipEntry {
				entries = []engine.OutboxEntry{{
					ID: "skip/" + string(id) + "/start/0",
					Task: engine.Task{
						ExecutionID: id,
						NodeName:    "start",
						UnitIdx:     0,
						Type:        engine.TaskTypeNodeSkip,
					},
				}}
			}
			if err := state.createExecution(ctx, &e, entries, time.Time{}, 0); err != nil {
				t.Fatalf("createExecution() error = %v", err)
			}

			inventory := createdExecutionKeys(namespace.Default, id, e.Graph)
			inInventory := make(map[string]struct{}, len(inventory))
			for _, key := range inventory {
				inInventory[key] = struct{}{}
			}

			// 1. Creation cannot write an execution-scoped key the inventory omits.
			prefix := execKey(namespace.Default, id, "")
			for _, key := range srv.Keys() {
				if !strings.HasPrefix(key, prefix) {
					continue
				}
				if _, ok := inInventory[key]; !ok {
					t.Errorf("createExecution wrote %q, which createdExecutionKeys does not enumerate", key)
				}
			}
			for _, suffix := range tc.wantSuffixes {
				key := execKey(namespace.Default, id, suffix)
				if !srv.Exists(key) {
					t.Errorf("creation did not write %q; the fixture no longer exercises it", key)
				}
				if _, ok := inInventory[key]; !ok {
					t.Errorf("createdExecutionKeys omits creation key %q", key)
				}
			}

			// 2. Completion-time TTL shortening covers every creation key except
			// the two policy keys, whose lifetimes are set by their own rules.
			if err := state.shortenTransientCompletionTTL(ctx, id); err != nil {
				t.Fatalf("shortenTransientCompletionTTL() error = %v", err)
			}
			transientKey := transientMarkKey(namespace.Default, id)
			retentionKey := retentionRecordKey(namespace.Default, id)
			for _, key := range inventory {
				if !srv.Exists(key) {
					continue
				}
				ttl := srv.TTL(key)
				if key == transientKey || key == retentionKey {
					if ttl <= inventoryCompletionTTL {
						t.Errorf("policy key %q was shortened to %v; it must keep its own lifetime", key, ttl)
					}
					continue
				}
				if ttl <= 0 || ttl > inventoryCompletionTTL+time.Second {
					t.Errorf("completion TTL shortening left %q at %v, want about %v", key, ttl, inventoryCompletionTTL)
				}
			}

			// 3. Rollback removes every creation key, leaving no residue. (No
			// transition runs in this test, so the transition-raced keys
			// cleanupCreatedExecution also covers are not exercised here.)
			state.cleanupCreatedExecution(ctx, &e)
			for _, key := range srv.Keys() {
				if strings.HasPrefix(key, prefix) {
					t.Errorf("cleanupCreatedExecution left execution-scoped key %q", key)
				}
			}
		})
	}
}
