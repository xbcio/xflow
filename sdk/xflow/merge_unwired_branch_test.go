package xflow

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// TestWaitAllJoinFiresDespiteUnwiredBranch pins how a wait_all join treats an
// upstream branch that will never deliver on the edge that reaches the join.
//
// A branch whose gate routed to a port with no connection to the join (an IF
// that took "false" while only "true" is wired, which is what a rejected
// approval whose rejected port is unwired looks like) still counts as an
// arrival with ActiveCount 0. The join waits for every in-edge to arrive but
// needs only one active arrival, so it runs once the other branch is in. It does
// not hang and it does not skip.
//
// The data it runs with is a separate matter. The join reads the stored output
// of each in-edge source by node name, whichever port that source left through,
// so a gate that ran still contributes its output even though its edge to the
// join was not the active one. Unlabelled edges all land on main and the last
// non-nil source in graph edge order wins. That order follows the source node
// names, not the order the branches arrived in. A source that was skipped has no
// output, so it never overwrites. Labelled edges keep one key per port.
func TestWaitAllJoinFiresDespiteUnwiredBranch(t *testing.T) {
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

	cases := []struct {
		name string
		// gate is the name of the dead branch's gate node. Edges are ordered by
		// source name, so "a_gate" sorts before the live node and "z_gate" after.
		gate string
		// skipped inserts a node behind the gate's wired port, so the dead
		// branch reaches the join through a node that is skipped, not one that ran.
		skipped  bool
		labelled bool
		// wantMain is the "from" value the join sees on main, or "" when the
		// join output is keyed by port instead.
		wantMain string
	}{
		{name: "gate sorts before live: live wins main", gate: "a_gate", wantMain: "live"},
		{name: "gate sorts after live: gate output wins main", gate: "z_gate", wantMain: "dead"},
		{name: "skipped node behind the gate never overwrites", gate: "z_gate", skipped: true, wantMain: "live"},
		{name: "labelled edges keep one key per port", gate: "z_gate", labelled: true},
	}
	for backend, newEngine := range backends {
		for _, tc := range cases {
			t.Run(backend+"/"+tc.name, func(t *testing.T) {
				eng := newEngine(t)
				t.Cleanup(eng.Stop)
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()

				wf := Workflow("wait-all-unwired-" + backend)
				start := wf.Node("start", node.Start())
				live := wf.Node("m_live", node.Set(map[string]any{"from": "live"}))
				deadSrc := wf.Node("dead_src", node.Set(map[string]any{"from": "dead"}))
				gate := wf.Node(tc.gate, node.IF("false"))
				join := wf.Node("join", node.Merge(node.MergeWaitAll))
				wf.Connect(start, live).Connect(start, deadSrc).Connect(deadSrc, gate)

				// The dead branch's wired port is "true"; the IF takes "false".
				var deadEnd types.EdgeEndpoint = gate.Output("true")
				if tc.skipped {
					behind := wf.Node("zz_behind", node.Set(map[string]any{"from": "behind"}))
					wf.Connect(gate.Output("true"), behind)
					deadEnd = behind
				}
				if tc.labelled {
					join.Inputs("live", "dead")
					wf.Connect(live, join.Input("live")).Connect(deadEnd, join.Input("dead"))
				} else {
					wf.Connect(live, join).Connect(deadEnd, join)
				}

				wfID, err := eng.AddWorkflow(ctx, wf)
				if err != nil {
					t.Fatalf("AddWorkflow: %v", err)
				}
				id, err := eng.Invoke(ctx, wfID, EntryNode("start"), nil)
				if err != nil {
					t.Fatalf("Invoke: %v", err)
				}
				res, err := eng.Wait(ctx, id)
				if err != nil {
					t.Fatalf("Wait: %v (the unwired branch held the join open)", err)
				}
				if res.Status != types.ExecutionStatusSuccess {
					t.Fatalf("status = %s, want success", res.Status)
				}

				if tc.skipped {
					behind, err := eng.Inspect(ctx, id, "zz_behind")
					if err != nil || len(behind.Nodes) != 1 {
						t.Fatalf("Inspect(zz_behind): %v", err)
					}
					if got := behind.Nodes[0].Status; got != types.NodeStatusSkipped {
						t.Fatalf("zz_behind status = %s, want skipped: the gate took its other port", got)
					}
				}
				joinDetail, err := eng.Inspect(ctx, id, "join")
				if err != nil || len(joinDetail.Nodes) != 1 {
					t.Fatalf("Inspect(join): %v", err)
				}
				if got := joinDetail.Nodes[0].Status; got != types.NodeStatusSuccess {
					t.Fatalf("join status = %s, want success: one active arrival is enough for wait_all", got)
				}
				out := joinDetail.Nodes[0].Output
				if tc.labelled {
					liveOut, _ := out["live"].(map[string]any)
					deadOut, _ := out["dead"].(map[string]any)
					if liveOut["from"] != "live" || deadOut["from"] != "dead" {
						t.Fatalf("join output = %#v, want live and dead under their own ports", out)
					}
					return
				}
				if got := out["from"]; got != tc.wantMain {
					t.Fatalf("join output from = %v, want %q (output %#v)", got, tc.wantMain, out)
				}
			})
		}
	}
}

// TestWaitAllJoinSkipsWhenNoBranchIsActive is the other side of the rule above:
// when every in-edge arrives inactive, the join is skipped rather than run, and
// the skip carries on to whatever follows it.
func TestWaitAllJoinSkipsWhenNoBranchIsActive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	eng, err := NewLocal()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(eng.Stop)

	wf := Workflow("wait-all-no-active-branch")
	start := wf.Node("start", node.Start())
	g1 := wf.Node("g1", node.IF("false"))
	g2 := wf.Node("g2", node.IF("false"))
	join := wf.Node("join", node.Merge(node.MergeWaitAll))
	after := wf.Node("after", node.Set(map[string]any{"x": 1}))
	wf.Connect(start, g1).Connect(start, g2)
	wf.Connect(g1.Output("true"), join).Connect(g2.Output("true"), join)
	wf.Connect(join, after)

	wfID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}
	id, err := eng.Invoke(ctx, wfID, EntryNode("start"), nil)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	res, err := eng.Wait(ctx, id)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if res.Status != types.ExecutionStatusSuccess {
		t.Fatalf("status = %s, want success: a skipped branch is not a failure", res.Status)
	}
	for _, name := range []string{"join", "after"} {
		d, err := eng.Inspect(ctx, id, name)
		if err != nil || len(d.Nodes) != 1 {
			t.Fatalf("Inspect(%s): %v", name, err)
		}
		if got := d.Nodes[0].Status; got != types.NodeStatusSkipped {
			t.Errorf("%s status = %s, want skipped: no in-edge of the join was active", name, got)
		}
	}
}
