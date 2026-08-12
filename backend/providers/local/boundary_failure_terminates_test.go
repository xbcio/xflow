package local

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"
)

// boundaryCounter counts its invocations so a test can prove the parameter
// boundary failed BEFORE the handler, not inside it.
type boundaryCounter struct{ n atomic.Int32 }

func (h *boundaryCounter) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.boundary_counter"}
}

func (h *boundaryCounter) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	h.n.Add(1)
	return &types.Output{Data: map[string]any{"ok": true}}, nil
}

// TestBoundaryEvaluationFailureReachesATerminalState pins that a parameter
// template the boundary cannot evaluate FAILS the execution instead of hanging
// it.
//
// The failure this guards against is not a slow one, it is a permanent stall.
// Runner.Execute's second return value is an executor error, and the dispatcher
// classifies an UNCLASSIFIED one as ExecutorFailureUnknown -- which
// deliberately leaves the lease fenced rather than releasing it, because an
// unknown failure might have started the handler and re-running it could repeat
// a side effect. That reasoning does not apply to the parameter boundary, which
// runs before the handler; but the dispatcher cannot tell from a bare error.
//
// Measured before the fix, with exactly this fixture: handler invocations 0,
// execution status stuck at "running" past the test deadline, no terminal
// state. The LeaseSweeper that would eventually reclaim such a lease is wired
// only in the control plane, so on a local backend nothing recovers it at all.
//
// A syntactically invalid expression is used because it is the one input that
// can never succeed -- it isolates "the boundary failed" from "the data was not
// ready yet", which retry legitimately fixes.
func TestBoundaryEvaluationFailureReachesATerminalState(t *testing.T) {
	reg := execution.NewRegistry()
	h := &boundaryCounter{}
	reg.RegisterGlobal("test.boundary_counter", h)

	b := New(WithConcurrency(1), WithRegistry(reg))
	eng := engine.New(b.State(), b.Queue())
	stop := b.Bind(eng)
	defer stop()

	def := &types.WorkflowDef{
		Name: "boundary-failure", Version: "v1",
		Nodes: []types.NodeDef{
			// Parses as a template, does not parse as an expression. The
			// compile-time form check only rejects text AROUND ${{ }}, so this
			// reaches the runtime boundary intact.
			{Name: "n", Type: "test.boundary_counter", Parameters: map[string]any{
				"bad": "${{ $params.x + }}",
			}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile rejected the fixture, so it no longer exercises the "+
			"runtime boundary; pick an expression compile still accepts: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	res, err := b.WaitDone(ctx, id)
	if err != nil {
		t.Fatalf("the execution never reached a terminal state (%v). A boundary "+
			"failure must be committed through the engine so retry / on_error "+
			"policy can terminate it -- an unclassified executor error leaves "+
			"the lease fenced forever on a backend with no lease sweeper.", err)
	}
	if res.Status != types.ExecutionStatusFailed {
		t.Fatalf("execution status = %v, want failed", res.Status)
	}
	if got := h.n.Load(); got != 0 {
		t.Fatalf("handler ran %d times; the boundary must fail BEFORE the "+
			"handler, so a template it cannot evaluate never reaches user code", got)
	}

	// The recorded error names the node and the parameter. It must not carry
	// the upstream values the expression was reading -- a node's output is
	// routinely an HTTP response body holding credentials.
	ns, err := b.State().GetNode(context.Background(), id, "n")
	if err != nil {
		t.Fatalf("get node: %v", err)
	}
	if ns == nil {
		t.Fatal("no node snapshot for the failed node")
	}
	if ns.Error == "" {
		t.Fatal("the failed node recorded no error; the boundary failure must " +
			"be committed as the node's outcome, not swallowed")
	}
	if !strings.Contains(ns.Error, `"bad"`) {
		t.Errorf("node error should name the failing parameter: %q", ns.Error)
	}
}
