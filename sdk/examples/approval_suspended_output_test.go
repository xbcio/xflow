package examples_test

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/types"
)

// upstreamDecisionKeys are the keys an approval gate publishes as its own
// record or outcome. A gate that is still parked must not carry an upstream
// node's values under them: a reader of the parked gate would take them for
// decisions this gate made.
var upstreamDecisionKeys = []string{"approved", "approver", "comment", "decisions"}

// parkedApprovalOutput runs start -> up -> gate where up publishes decision-like
// keys beside a business field, waits for the gate to park, optionally
// delivers one approval, and returns the gate's inspected output.
func parkedApprovalOutput(t *testing.T, decideFirst bool) map[string]any {
	t.Helper()
	ctx := context.Background()
	hooks := newSuspensionHooks()
	eng, err := xflow.NewLocal(xflow.WithHooks(hooks))
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	wf := xflow.Workflow("approval_parked_output")
	start := wf.Node("start", node.Start())
	up := wf.Node("up", node.Set(map[string]any{
		"approved":  true,
		"approver":  "vp",
		"comment":   "from upstream",
		"decisions": []any{map[string]any{"approver": "vp", "action": "approve"}},
		"amount":    42,
	}))
	gate := wf.Node("gate", node.Approval([]string{"alice", "bob"}, node.ApprovalAll))
	wf.Connect(start, up).Connect(up, gate)

	wfID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}
	id, err := eng.Invoke(ctx, wfID, xflow.EntryNode("start"), nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	hooks.waitForSuspensions(t, waitCtx, "gate", 1)
	if decideFirst {
		err := eng.Signal(ctx, id, "gate/approval/alice", map[string]any{
			types.VerifiedActorKey: "alice", "action": "approve",
		})
		if err != nil {
			t.Fatalf("Signal(alice) error = %v", err)
		}
		hooks.waitForSuspensions(t, waitCtx, "gate", 2)
	}

	detail, err := eng.Inspect(ctx, id, "gate")
	if err != nil {
		t.Fatalf("Inspect(gate) error = %v", err)
	}
	if len(detail.Nodes) != 1 || detail.Nodes[0].Status != types.NodeStatusSuspended {
		t.Fatalf("Inspect(gate) nodes = %+v, want one suspended node", detail.Nodes)
	}
	return detail.Nodes[0].Output
}

// TestApprovalParkedOutputKeepsUpstreamDecisionsOut pins the visible output of
// an approval gate that is waiting. The business field an upstream node
// published has to reach the gate's output, because the nodes after the gate
// read it. The decision-class keys have to come from the gate's own ledger only.
func TestApprovalParkedOutputKeepsUpstreamDecisionsOut(t *testing.T) {
	t.Run("first suspension", func(t *testing.T) {
		out := parkedApprovalOutput(t, false)
		if out["amount"] != 42 && out["amount"] != float64(42) {
			t.Errorf("amount = %v, want 42 passed through", out["amount"])
		}
		for _, key := range upstreamDecisionKeys {
			if v, leaked := out[key]; leaked {
				t.Errorf("parked gate output carries upstream %q = %v before anybody decided", key, v)
			}
		}
	})

	t.Run("after one of two approvals", func(t *testing.T) {
		out := parkedApprovalOutput(t, true)
		if out["amount"] != 42 && out["amount"] != float64(42) {
			t.Errorf("amount = %v, want 42 passed through", out["amount"])
		}
		for _, key := range []string{"approved", "approver", "comment"} {
			if v, leaked := out[key]; leaked {
				t.Errorf("parked gate output carries upstream %q = %v after one approval", key, v)
			}
		}
		decisions, ok := out["decisions"].([]map[string]any)
		if !ok || len(decisions) != 1 || decisions[0]["approver"] != "alice" {
			t.Errorf("decisions = %#v, want only alice's own approval", out["decisions"])
		}
	})
}
