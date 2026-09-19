package rstate

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// compileSkipDiamondGraph builds start -> split, with split having two ports
// whose branches reconverge on join (split.main -> join.left,
// split.other -> join.right).
//
// The reconvergence is what makes a skip reachable: two independent
// destinations of a fan-out are each scheduled by whichever port the source
// commits on, whereas a skip needs an in-edge that will never carry data.
func compileSkipDiamondGraph(tb testing.TB) *graph.Graph {
	tb.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "rstate-skip-diamond",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "split", Type: "test.echo"},
			{Name: "on_main", Type: "test.echo"},
			{Name: "on_other", Type: "test.echo"},
			{Name: "join", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "split", Input: "main"}}}},
			"split": {
				"main":  {Targets: []types.Connection{{Node: "on_main", Input: "main"}}},
				"other": {Targets: []types.Connection{{Node: "on_other", Input: "main"}}},
			},
			"on_main":  {"main": {Targets: []types.Connection{{Node: "join", Input: "left"}}}},
			"on_other": {"main": {Targets: []types.Connection{{Node: "join", Input: "right"}}}},
		},
	})
	if err != nil {
		tb.Fatalf("Compile() error = %v", err)
	}
	return g
}

// commitNodeAs leases node and commits it on port, the minimum needed to reach
// an advance of that node.
func commitNodeAs(t *testing.T, ctx context.Context, state *Store, id types.ExecutionID, node string, nodeIdx int, port string) {
	t.Helper()
	lease := &engine.TaskLease{
		LeaseID:    engine.LeaseID("lease-" + node),
		LeaseToken: engine.LeaseToken("token-" + node),
		Task:       engine.Task{ExecutionID: id, NodeName: node, NodeIdx: nodeIdx, Type: engine.TaskTypeNodeExec},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease(%s) acquired=%v err=%v", node, acquired, err)
	}
	if _, err := state.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID: id,
		NodeName:    node,
		NodeIdx:     nodeIdx,
		LeaseID:     lease.LeaseID,
		LeaseToken:  lease.LeaseToken,
		Attempt:     1,
		Status:      types.NodeStatusSuccess,
		Output:      map[string]any{"out": node},
		StoreOutput: true,
		Port:        port,
		AdvanceTask: &engine.Task{
			ExecutionID: id,
			NodeName:    node,
			NodeIdx:     nodeIdx,
			Type:        engine.TaskTypeNodeAdvance,
			Port:        &port,
		},
	}); err != nil {
		t.Fatalf("CommitNode(%s) error = %v", node, err)
	}
}

// TestAdvanceNodeReportsSkippedDestination is the end-to-end proof that the Go
// side learns a skip happened at all.
//
// The decision is made inside advanceNodeLua, so the caller has no other way to
// know: the script's reply is the only channel. This drives a real commit, runs
// the real script against miniredis, and asserts the result names the
// destination the dead branch stranded — the whole point of widening the reply.
func TestAdvanceNodeReportsSkippedDestination(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("advance-skip-report")
	g := compileSkipDiamondGraph(t)

	if err := state.CreateExecutionWithOutbox(ctx, &engine.ExecutionSnapshot{
		ID:     id,
		Graph:  g,
		Status: types.ExecutionStatusRunning,
	}, []engine.OutboxEntry{{
		ID:   "exec/" + string(id) + "/start/0",
		Task: engine.Task{ExecutionID: id, NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeExec},
	}}); err != nil {
		t.Fatalf("CreateExecutionWithOutbox: %v", err)
	}

	startIdx, _ := g.NodeIndex("start")
	commitNodeAs(t, ctx, state, id, "start", startIdx, "main")

	splitIdx, _ := g.NodeIndex("split")
	commitNodeAs(t, ctx, state, id, "split", splitIdx, "main")

	// The advance of split is what resolves its stranded "other" destination.
	res, err := state.AdvanceNode(ctx, engine.AdvanceNodeRequest{
		ExecutionID: id,
		NodeName:    "split",
		NodeIdx:     splitIdx,
		Arrivals: []engine.DownstreamArrival{
			{NodeName: "on_main", NodeIdx: mustNodeIdx(t, g, "on_main"), UnitIdx: mustNodeIdx(t, g, "on_main"), ArrivalCount: 1, ActiveCount: 0, MergeMode: "wait_all"},
			{NodeName: "on_other", NodeIdx: mustNodeIdx(t, g, "on_other"), UnitIdx: mustNodeIdx(t, g, "on_other"), ArrivalCount: 1, ActiveCount: 1, MergeMode: "wait_all"},
		},
	})
	if err != nil {
		t.Fatalf("AdvanceNode: %v", err)
	}
	if !res.Applied {
		t.Fatalf("AdvanceNode.Applied = false, want true (advanced=%+v)", res)
	}
	if len(res.Skipped) != 1 {
		t.Fatalf("Skipped = %+v, want exactly one entry for the arrival with no active input", res.Skipped)
	}
	if res.Skipped[0].NodeName != "on_main" {
		t.Fatalf("skipped node = %q, want %q (the arrival whose ActiveCount was 0)",
			res.Skipped[0].NodeName, "on_main")
	}
	if res.Skipped[0].Count != 1 {
		t.Fatalf("skipped count = %d, want 1", res.Skipped[0].Count)
	}
}

// TestAdvanceNodeReportsNoSkipOnAllExecute is the distributed backend's
// negative control: when every arrival carries an active input, the script must
// report an empty skip list. Without this, a script that unconditionally
// returned something would still satisfy the test above.
func TestAdvanceNodeReportsNoSkipOnAllExecute(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("advance-no-skip")
	g := twoNodeLinearGraph(t)

	if err := state.CreateExecutionWithOutbox(ctx, &engine.ExecutionSnapshot{
		ID:     id,
		Graph:  g,
		Status: types.ExecutionStatusRunning,
	}, []engine.OutboxEntry{{
		ID:   "exec/" + string(id) + "/start/0",
		Task: engine.Task{ExecutionID: id, NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeExec},
	}}); err != nil {
		t.Fatalf("CreateExecutionWithOutbox: %v", err)
	}

	startIdx, _ := g.NodeIndex("start")
	commitNodeAs(t, ctx, state, id, "start", startIdx, "main")

	nextIdx, _ := g.NodeIndex("next")
	res, err := state.AdvanceNode(ctx, engine.AdvanceNodeRequest{
		ExecutionID: id,
		NodeName:    "start",
		NodeIdx:     startIdx,
		Arrivals: []engine.DownstreamArrival{
			{NodeName: "next", NodeIdx: nextIdx, UnitIdx: nextIdx, ArrivalCount: 1, ActiveCount: 1, MergeMode: "wait_all"},
		},
	})
	if err != nil {
		t.Fatalf("AdvanceNode: %v", err)
	}
	if !res.Applied {
		t.Fatalf("AdvanceNode.Applied = false, want true (advanced=%+v)", res)
	}
	if len(res.Skipped) != 0 {
		t.Fatalf("Skipped = %+v, want none: the only arrival carried an active input", res.Skipped)
	}
}

// mustNodeIdx resolves a node name to its index, failing the test rather than
// returning a zero that would silently address the wrong node.
func mustNodeIdx(t *testing.T, g *graph.Graph, name string) int {
	t.Helper()
	idx, ok := g.NodeIndex(name)
	if !ok {
		t.Fatalf("node %q absent from the fixture graph", name)
	}
	return idx
}

// TestSkippedUnitsFromLuaHandlesOldAndNewReplyShapes covers the protocol change
// the skip report required.
//
// The script's reply grew an element, and a rolling deploy means the new Go
// code can meet a script revision that returns the older, shorter shape. That
// case has to decode as an empty skip list rather than an error: the transition
// has already been applied by Redis, and failing the caller would turn a
// missing metric into a failed advance.
//
// The malformed cases matter for the same reason. A reply the decoder cannot
// parse must not invent a skip — an operator alerting on this counter cannot
// tolerate a phantom.
func TestSkippedUnitsFromLuaHandlesOldAndNewReplyShapes(t *testing.T) {
	cases := []struct {
		name  string
		value any
		want  []engine.SkippedUnit
	}{
		{name: "nil (older script or missing element)", value: nil, want: nil},
		{name: "scalar (the pre-change bare `return 1`)", value: int64(1), want: nil},
		{name: "empty table", value: []any{}, want: nil},
		{name: "odd element count is dropped", value: []any{"only-a-name"}, want: nil},
		{
			name:  "one skip",
			value: []any{"join", int64(1)},
			want:  []engine.SkippedUnit{{NodeName: "join", Count: 1}},
		},
		{
			name:  "several skips",
			value: []any{"a", int64(2), "b", int64(1)},
			want:  []engine.SkippedUnit{{NodeName: "a", Count: 2}, {NodeName: "b", Count: 1}},
		},
		{
			name:  "a zero count is not reported as a skip",
			value: []any{"a", int64(0), "b", int64(1)},
			want:  []engine.SkippedUnit{{NodeName: "b", Count: 1}},
		},
		{
			name:  "a trailing half entry is dropped",
			value: []any{"a", int64(1), "b"},
			want:  []engine.SkippedUnit{{NodeName: "a", Count: 1}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := skippedUnitsFromLua(tc.value)
			if len(got) != len(tc.want) {
				t.Fatalf("skippedUnitsFromLua(%#v) = %+v, want %+v", tc.value, got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("skippedUnitsFromLua(%#v)[%d] = %+v, want %+v",
						tc.value, i, got[i], tc.want[i])
				}
			}
		})
	}
}

// TestRunAdvanceNodeLuaNormalizesBothReplyShapes pins the other half of the same
// protocol change.
//
// Redis renders a Lua table as a scalar when it holds one element and as an
// array otherwise, so the applied flag arrives as either `1` or `{1, ...}`.
// Reading only the array shape turns a fenced or duplicate advance — a normal
// outcome, not a fault — into an error the caller reports as a delivery
// failure. The scalar branch is therefore load-bearing, not defensive.
func TestRunAdvanceNodeLuaNormalizesBothReplyShapes(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()
	ctx := context.Background()

	// A bare scalar: the shape every early `return 0` guard produces, and the
	// shape a pre-change script produced on success.
	scalar := redis.NewScript(`return 7`)
	res, err := runAdvanceNodeLua(ctx, scalar, rdb, nil, nil)
	if err != nil {
		t.Fatalf("runAdvanceNodeLua(scalar) error = %v", err)
	}
	if len(res) != 1 || redisResultInt(res[0]) != 7 {
		t.Fatalf("runAdvanceNodeLua(scalar) = %#v, want a one-element [7]", res)
	}

	// The current two-element shape.
	two := redis.NewScript(`return {1, {'join', 1}}`)
	res, err = runAdvanceNodeLua(ctx, two, rdb, nil, nil)
	if err != nil {
		t.Fatalf("runAdvanceNodeLua(array) error = %v", err)
	}
	if len(res) < 2 || redisResultInt(res[0]) != 1 {
		t.Fatalf("runAdvanceNodeLua(array) = %#v, want [1, ...]", res)
	}
	skips := skippedUnitsFromLua(res[1])
	if len(skips) != 1 || skips[0].NodeName != "join" {
		t.Fatalf("skips decoded from the array shape = %+v, want one entry for join", skips)
	}
}
