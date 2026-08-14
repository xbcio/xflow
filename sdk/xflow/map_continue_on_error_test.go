package xflow

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// itemFailer fails exactly one item and records every item it saw. Which items
// it saw AFTER the failing one is the whole question: MapBodyExecutor stops a
// batch at its first failed item unless continue_on_error is set.
type itemFailer struct {
	failOn int
	mu     sync.Mutex
	seen   []int
}

func (h *itemFailer) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.item_failer"}
}

func (h *itemFailer) Execute(_ context.Context, in *types.Input) (*types.Output, error) {
	id, _ := in.Data["$item"].(int)
	h.mu.Lock()
	h.seen = append(h.seen, id)
	h.mu.Unlock()
	if id == h.failOn {
		return nil, fmt.Errorf("item %d is poison", id)
	}
	return &types.Output{Data: map[string]any{"ok": id}}, nil
}

func (h *itemFailer) items() []int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]int(nil), h.seen...)
}

// TestMapContinueOnErrorReachesTheBodyExecutor pins the whole path for the
// setting: builder → RawParams → workflow definition → compiled NodeMeta →
// mapContinueOnError → BatchBodyRequest → MapBodyExecutor's break.
//
// MapNode.RawParams emitted only items and batch_size, so a Go-SDK author had
// no way to turn continue_on_error on at all — the descriptor advertised it and
// the engine read it, but nothing in between could set it. The setting silently
// stayed false and one malformed item cost every later item in its batch. A
// test on RawParams alone would not have caught that: the gap was that no test
// asserted the setting SURVIVES to the executor.
//
// batch_size covers all four items in ONE batch, which is what makes the
// difference observable: the break is per batch, so a batch-of-one would pass
// either way.
func TestMapContinueOnErrorReachesTheBodyExecutor(t *testing.T) {
	for _, tc := range []struct {
		name         string
		keepGoing    bool
		wantSeen     int
		wantStatus   types.ExecutionStatus
		explainCount string
	}{
		{
			name:         "on: the failed item does not abandon the rest",
			keepGoing:    true,
			wantSeen:     4,
			wantStatus:   types.ExecutionStatusSuccess,
			explainCount: "every item ran; the failed one holds its slot as {_error,_index}",
		},
		{
			name:         "off: the batch stops at the failed item",
			keepGoing:    false,
			wantSeen:     2,
			wantStatus:   types.ExecutionStatusFailed,
			explainCount: "items 1 and 2 ran, then the batch broke before 3 and 4",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eng, err := NewLocal()
			if err != nil {
				t.Fatalf("NewLocal: %v", err)
			}
			defer eng.Stop()

			failer := &itemFailer{failOn: 2}
			body := Workflow("failing-body")
			body.LocalNode("step", failer)

			mapBuilder := node.Map("$input.ids", 4)
			if tc.keepGoing {
				mapBuilder = mapBuilder.ContinueOnError()
			}

			wf := Workflow("map-continue-on-error")
			start := wf.Node("start", node.Start())
			mapNode := wf.Node("m", mapBuilder)
			mapNode.Body(body)
			wf.Connect(start, mapNode)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			wfID, err := eng.AddWorkflow(ctx, wf)
			if err != nil {
				t.Fatalf("AddWorkflow: %v", err)
			}

			execID, err := eng.Invoke(ctx, wfID, Start(), map[string]any{"ids": []any{1, 2, 3, 4}})
			if err != nil {
				t.Fatalf("Invoke: %v", err)
			}
			res, err := eng.Wait(ctx, execID)
			if err != nil {
				t.Fatalf("Wait: %v", err)
			}
			if res.Status != tc.wantStatus {
				t.Errorf("execution status = %v, want %v", res.Status, tc.wantStatus)
			}

			ran := failer.items()
			if len(ran) != tc.wantSeen {
				t.Fatalf("body ran %d time(s) with items %v, want %d — %s",
					len(ran), ran, tc.wantSeen, tc.explainCount)
			}
		})
	}
}

// TestMapRawParamsCarriesContinueOnError is the unit-level half: the builder
// writes the parameter the descriptor declares, under the name the engine reads
// (engine.mapContinueOnError looks up "continue_on_error").
func TestMapRawParamsCarriesContinueOnError(t *testing.T) {
	off, _ := node.Map("$input.ids", 2).RawParams().(map[string]any)
	if got, ok := off["continue_on_error"].(bool); !ok || got {
		t.Errorf("default continue_on_error = %v (present=%v), want false", off["continue_on_error"], ok)
	}

	on, _ := node.Map("$input.ids", 2).ContinueOnError().RawParams().(map[string]any)
	if got, _ := on["continue_on_error"].(bool); !got {
		t.Errorf("ContinueOnError() left continue_on_error = %v, want true", on["continue_on_error"])
	}
}
