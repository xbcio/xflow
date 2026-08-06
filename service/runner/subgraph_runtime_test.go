package runner

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// A batch lease has no Input and no registered handler for the synthetic
// "m/_batch/0" node: its work lives in SubgraphPayload. Falling through to the
// normal handler path either dereferences the nil Input or fails handler
// lookup, and either way the map node's items never run. The runner must
// recognize the payload and route it to the subgraph path.
//
// What this asserts is the ROUTING — that the runner takes the batch branch at
// all and reports a result the control plane can commit through the expansion
// barrier. What the batch path then does with the body is asserted in
// subgraph_body_test.go.
// batchLeaseForTest builds the lease a control plane hands a runner for one
// batch of a map expansion: no Input, all the work in SubgraphPayload.
func batchLeaseForTest(items []any) *engine.TaskLease {
	pkg := bodyPackageForTest()
	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		panic("compute package hash: " + err.Error())
	}
	return &engine.TaskLease{
		LeaseID:    engine.LeaseID("lease-parent"),
		LeaseToken: engine.LeaseToken("token-parent"),
		Attempt:    1,
		Task: engine.Task{
			ExecutionID: types.ExecutionID("exec-1"),
			NodeName:    "m/_batch/0",
			NodeIdx:     0,
			Type:        engine.TaskTypeNodeBatch,
		},
		NodeType: "xflow.map",
		SubgraphPayload: &engine.SubgraphLeasePayload{
			ProtocolVersion: 1,
			ParentNode:      "m",
			ParentNodeIdx:   0,
			BatchIndex:      0,
			ChildExecID:     types.ExecutionID("exec-1/sub/m/lease-parent/0"),
			Items:           items,
			AllItems:        items,
			BatchSize:       len(items),
			Package:         pkg,
			PackageHash:     hash,
		},
	}
}

func TestRunnerExecutesABatchLeaseThroughTheSubgraphPath(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	items := []any{map[string]any{"id": 1}, map[string]any{"id": 2}}
	lease := batchLeaseForTest(items)
	client := &fakeProtocolClient{lease: lease, cancel: cancel}
	// Only the BODY's member type is registered — nothing answers to the batch's
	// own synthetic node name or to "xflow.map", so a runner that fell through to
	// the handler path would fail lookup instead of reaching the body.
	registry := execution.NewRegistry()
	registry.RegisterGlobal("test.body_item", &bodyItemHandler{})

	r := New(client, registry, Config{
		RunnerID:          "runner-1",
		Concurrency:       1,
		Capabilities:      []protocol.Capability{{NodeType: "xflow.map"}},
		HeartbeatInterval: time.Hour,
		PollWait:          time.Millisecond,
		SubgraphRuntime:   NewSubgraphRuntime(registry, NewPackageCache(PackageCacheConfig{MaxEntries: 4})),
	})

	if err := r.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if client.reported.Lease == nil {
		t.Fatal("runner never reported the batch lease")
	}
	if client.reported.Result.Error != nil {
		t.Fatalf("batch execution failed: %v", client.reported.Result.Error)
	}
	out := client.reported.Result.Output
	if out == nil || out.Data == nil {
		t.Fatalf("batch reported no output: %+v", client.reported.Result)
	}
	if got, _ := out.Data["count"].(int); got != len(items) {
		t.Errorf("batch result count = %v, want %d (one entry per item)", out.Data["count"], len(items))
	}
}

// A runner that advertises xflow.map but was assembled without a
// SubgraphRuntime can still be handed a batch lease — capability advertisement
// and runtime wiring are separate config fields, so nothing stops the two from
// disagreeing. It must report a named failure the control plane can act on. The
// alternative is falling through to the handler path, where the batch's
// synthetic node name produces an opaque lookup error that says nothing about
// the real misconfiguration.
func TestRunnerWithoutASubgraphRuntimeReportsAFailureInsteadOfFallingThrough(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lease := batchLeaseForTest([]any{map[string]any{"id": 1}})
	client := &fakeProtocolClient{lease: lease, cancel: cancel}
	registry := execution.NewRegistry()

	r := New(client, registry, Config{
		RunnerID:          "runner-1",
		Concurrency:       1,
		Capabilities:      []protocol.Capability{{NodeType: "xflow.map"}},
		HeartbeatInterval: time.Hour,
		PollWait:          time.Millisecond,
		// SubgraphRuntime deliberately absent.
	})

	if err := r.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if client.reported.Lease == nil {
		t.Fatal("runner never reported the batch lease, so the control plane waits on a lease that will never commit")
	}
	err := client.reported.Result.Error
	if err == nil {
		t.Fatalf("batch reported success without a runtime to execute it: %+v", client.reported.Result)
	}
	if !strings.Contains(err.Error(), "SubgraphRuntime") {
		t.Errorf("reported error = %q, want it to name the missing SubgraphRuntime "+
			"(an opaque handler-lookup error would mean the batch fell through)", err)
	}
}
