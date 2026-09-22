package rstate

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// boundaryPayload is over outputCompressMinBytes and redundant the way captured
// traffic is, so any path that consults the codec must store a zstd frame.
func boundaryPayload() map[string]any {
	items := make([]any, 200)
	for i := range items {
		items[i] = map[string]any{
			"authorization": "Bearer 0f8a1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7081",
			"uri":           "/api/v1/orders/detail",
			"body":          `{"order_id":"SO1234567890","status":"PAID","items":[]}`,
		}
	}
	return map[string]any{"results": items, "count": len(items)}
}

// TestOutputCompressionCoversTheGroupExitPath pins the gap that shipped in
// v0.0.24: compression was wired into CommitNode, UpsertNode, PutOutput and the
// suspend path, but NOT into the boundary exits of CommitGroup. In the collection
// pipeline the map node's `collect` exit IS the output key that dominates the
// keyspace, and it is committed as a group unit — so for the one key class the
// feature exists to shrink, the switch was a no-op.
//
// It is asserted from the stored bytes rather than from a flag, because "the
// option is set" and "this path consults the codec" are different claims.
func TestOutputCompressionCoversTheGroupExitPath(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	state := New(rdb, nil, time.Minute)
	state.ConfigureOutputCompression(true)
	ctx := context.Background()

	g, err := graph.Compile(&types.WorkflowDef{
		Name: "boundary-compression",
		Nodes: []types.NodeDef{
			{Name: "ingest", Kind: types.NodeKindTrigger},
			{Name: "analyze", Kind: types.NodeKindAction},
			{Name: "store", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"ingest":  {"main": {Targets: []types.Connection{{Node: "analyze", Input: "main"}}}},
			"analyze": {"main": {Targets: []types.Connection{{Node: "store", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "edge", Members: []string{"ingest", "analyze"}}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	id := types.ExecutionID("boundary-compression")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	gu := g.Groups()[0].UnitIdx
	ok, err := state.AcquireGroupLease(ctx, &engine.GroupLease{
		LeaseID: "L-b", LeaseToken: "T-b", Attempt: 1,
		ExecutionID: id, GroupUnitIdx: gu, GroupID: "edge",
		IssuedAt: time.Now(), TTL: time.Minute,
	})
	if err != nil || !ok {
		t.Fatalf("AcquireGroupLease() ok=%v err=%v", ok, err)
	}

	payload := boundaryPayload()
	res, err := state.CommitGroup(ctx, engine.GroupCommitRequest{
		ExecutionID: id, GroupUnitIdx: gu, GroupID: "edge",
		LeaseID: "L-b", LeaseToken: "T-b", Attempt: 1,
		Outcome: engine.GroupOutcomeSuccess,
		Exits:   []engine.GroupExitResult{{NodeName: "analyze", Port: "main", Data: payload}},
	})
	if err != nil {
		t.Fatalf("CommitGroup() error = %v", err)
	}
	if !res.Applied {
		t.Fatalf("CommitGroup() applied=false, want an applied boundary commit")
	}

	ns := namespace.FromContext(ctx)
	raw, err := rdb.Get(ctx, outputKey(ns, id, "analyze")).Bytes()
	if err != nil {
		t.Fatalf("boundary output key absent after an applied commit: %v", err)
	}
	if !bytes.HasPrefix(raw, zstdFrameMagic) {
		t.Fatalf("boundary exit stored %d bytes that do not start with the zstd frame "+
			"magic (%x); the group path is bypassing encodeOutputValue. A value this size "+
			"is exactly the one the compression switch exists for.", len(raw), zstdFrameMagic)
	}
	plain, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("re-encode payload for comparison: %v", err)
	}
	if len(raw) >= len(plain) {
		t.Fatalf("compressed boundary exit is %d bytes against %d plain; the frame did not shrink it",
			len(raw), len(plain))
	}

	// The point of compressing is that every reader still gets the map back, so
	// the round trip is asserted through the public API rather than the codec.
	got, err := state.GetOutput(ctx, id, "analyze")
	if err != nil {
		t.Fatalf("GetOutput() error = %v", err)
	}
	if got == nil {
		t.Fatal("GetOutput() = nil for a compressed boundary exit")
	}
	// JSON round-trips a number to float64, so the count is compared as one.
	wantCount := float64(len(payload["results"].([]any)))
	if c, ok := got["count"].(float64); !ok || c != wantCount {
		t.Errorf("GetOutput()[count] = %v (%T), want %v", got["count"], got["count"], wantCount)
	}

	// Compression must stay confined to storage: the shared contract has to pass
	// against a backend that has just written a compressed boundary value.
	statestoretest.RunStateStoreContract(t, state)
}
