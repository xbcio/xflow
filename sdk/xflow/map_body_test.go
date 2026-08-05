package xflow

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// mapBodyRecorder is the map body's only node. It records the $item it received
// so the test can prove the body EXECUTED, not merely that the map node
// terminalized. Before the body path landed, a batch was a pass-through: the
// execution reached success with this recorder never called.
type mapBodyRecorder struct {
	mu   sync.Mutex
	seen []int
}

func (h *mapBodyRecorder) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.map_body_recorder"}
}

func (h *mapBodyRecorder) Execute(_ context.Context, in *types.Input) (*types.Output, error) {
	id, _ := in.Data["$item"].(int)
	h.mu.Lock()
	h.seen = append(h.seen, id)
	h.mu.Unlock()
	return &types.Output{Data: map[string]any{"doubled": id * 2}}, nil
}

func (h *mapBodyRecorder) items() []int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]int(nil), h.seen...)
}

// sdk.NewLocal is the embedded deployment: no runner, no control plane. Its
// batches run in this process, so the engine it assembles needs a body
// executor. Without one, ExecuteBatch refuses (ErrNoBatchBodyExecutor) and a
// map node hangs forever instead of iterating.
func TestLocalEngineRunsAMapNodesBody(t *testing.T) {
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	defer eng.Stop()

	recorder := &mapBodyRecorder{}
	body := Workflow("double-body")
	body.LocalNode("double", recorder)

	wf := Workflow("map-body-local")
	start := wf.Node("start", node.Start())
	mapNode := wf.Node("m", node.Map("$input.ids", 2))
	mapNode.Body(body)
	wf.Connect(start, mapNode)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wfID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	execID, err := eng.Invoke(ctx, wfID, Start(), map[string]any{"ids": []any{1, 2, 3}})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	res, err := eng.Wait(ctx, execID)
	if err != nil {
		t.Fatalf("Wait: %v — the map node never finished, so its batches ran nothing", err)
	}
	if res.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution status = %v, want success", res.Status)
	}

	ran := recorder.items()
	if len(ran) != 3 {
		t.Fatalf("body ran %d time(s) with items %v, want 3 — one per item regardless of batch_size", len(ran), ran)
	}
	got := map[int]bool{}
	for _, id := range ran {
		got[id] = true
	}
	for _, want := range []int{1, 2, 3} {
		if !got[want] {
			t.Errorf("body never saw item %d; it saw %v", want, ran)
		}
	}

	// batch_size=2 over 3 items means 2 batches, but the map node must report
	// per-ITEM results: batch_size is a durability policy, not a semantic one.
	out, _ := res.Output["m"].(map[string]any)
	if count, _ := out["count"].(int); count != 3 {
		t.Errorf("map node count = %v, want 3 (items, not batches)", out["count"])
	}
}
