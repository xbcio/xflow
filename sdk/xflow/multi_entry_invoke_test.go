package xflow

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/trigger"
	"github.com/xbcio/xflow/types"
)

// multiEntryWorkflow declares three entries — start and two webhook triggers —
// each with a private lane, and a wait_all merge every entry feeds:
//
//	start ─→ start_lane ─┐
//	hook_a → a_lane ─────┼→ join(merge wait_all) → done
//	hook_b → b_lane ─────┘
//
// An execution begins from exactly one entry. The other two must resolve as
// skipped, or the completion counter never reaches zero and the wait_all join
// never becomes ready (DSL-SPECIFICATION.md: an unselected trigger root must
// not block the selected entry's downstream).
func multiEntryWorkflow(name string) *WorkflowBuilder {
	wf := Workflow(name)
	start := wf.Node("start", node.Start())
	hookA := wf.Node("hook_a", trigger.Webhook().Method("POST").Path("/"+name+"/a"))
	hookB := wf.Node("hook_b", trigger.Webhook().Method("POST").Path("/"+name+"/b"))
	startLane := wf.Node("start_lane", node.Expr(`"start"`))
	aLane := wf.Node("a_lane", node.Expr(`"a"`))
	bLane := wf.Node("b_lane", node.Expr(`"b"`))
	join := wf.Node("join", node.Merge(node.MergeWaitAll))
	done := wf.Node("done", node.End())
	wf.Connect(start, startLane).Connect(startLane, join)
	wf.Connect(hookA, aLane).Connect(aLane, join)
	wf.Connect(hookB, bLane).Connect(bLane, join)
	wf.Connect(join, done)
	return wf
}

func TestInvokeResolvesUnselectedEntriesAsSkipped(t *testing.T) {
	backends := map[string]func(t *testing.T) *Engine{
		"local": func(t *testing.T) *Engine {
			eng, err := NewLocal()
			if err != nil {
				t.Fatal(err)
			}
			return eng
		},
		"cluster": func(t *testing.T) *Engine {
			mr, err := miniredis.Run()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(mr.Close)
			eng, err := NewCluster(ClusterConfig{RedisAddr: mr.Addr()}, WithConcurrency(4))
			if err != nil {
				t.Fatal(err)
			}
			return eng
		},
	}
	entries := []struct {
		entry    Entry
		ran      string
		skipped  []string
		laneName string
	}{
		{Start(), "start_lane", []string{"hook_a", "a_lane", "hook_b", "b_lane"}, "start"},
		{Trigger("hook_a"), "a_lane", []string{"start", "start_lane", "hook_b", "b_lane"}, "hook_a"},
	}
	for backend, newEngine := range backends {
		for _, tc := range entries {
			t.Run(backend+"/"+tc.laneName, func(t *testing.T) {
				eng := newEngine(t)
				t.Cleanup(eng.Stop)
				ctx := context.Background()
				wfID, err := eng.AddWorkflow(ctx, multiEntryWorkflow("multi-entry-"+backend+"-"+tc.laneName))
				if err != nil {
					t.Fatalf("AddWorkflow: %v", err)
				}
				id, err := eng.Invoke(ctx, wfID, tc.entry, nil)
				if err != nil {
					t.Fatalf("Invoke: %v", err)
				}
				waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				defer cancel()
				res, err := eng.Wait(waitCtx, id)
				if err != nil {
					t.Fatalf("Wait: %v (the unselected entries held the execution open)", err)
				}
				if res.Status != types.ExecutionStatusSuccess {
					t.Fatalf("status = %s, want success", res.Status)
				}
				want := map[string]types.NodeStatus{tc.ran: types.NodeStatusSuccess, "join": types.NodeStatusSuccess, "done": types.NodeStatusSuccess}
				for _, name := range tc.skipped {
					want[name] = types.NodeStatusSkipped
				}
				for name, status := range want {
					d, err := eng.Inspect(ctx, id, name)
					if err != nil || len(d.Nodes) != 1 {
						t.Fatalf("Inspect(%s): %v", name, err)
					}
					if d.Nodes[0].Status != status {
						t.Errorf("node %s = %s, want %s", name, d.Nodes[0].Status, status)
					}
				}
			})
		}
	}
}
