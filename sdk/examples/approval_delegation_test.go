package examples_test

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/types"
)

const delegatedApprovalNode = "DelegatedApproval"

// TestApprovalDelegateAndAddSignerRoundTrip drives a delegation and an added
// signer through a real engine rather than the node alone.
//
// What this covers that the node's own tests cannot is the round trip: the
// amended chain has to survive being carried across suspensions, and each
// amendment re-arms the wait. A chain that was accepted by the node but lost on
// the way out would leave the gate armed for approvers who had already handed
// their slot on, and the execution would park until its timeout.
func TestApprovalDelegateAndAddSignerRoundTrip(t *testing.T) {
	ctx := context.Background()
	hooks := &approvalSuspensionHooks{}
	eng, err := xflow.NewLocal(xflow.WithHooks(hooks))
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	wf := xflow.Workflow("approval_delegation")
	start := wf.Node("start", node.Start())
	gate := wf.Node(delegatedApprovalNode, node.Approval([]string{"alice", "bob"}, node.ApprovalAll))
	wf.Connect(start, gate)

	workflowID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}
	id, err := eng.Invoke(ctx, workflowID, xflow.EntryNode("start"), nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}

	// Each signal is sent only once the gate has re-armed, because a signal
	// delivered while the node is not suspended is not queued for a later arming:
	// it names an approver the gate is not currently waiting on, and is dropped.
	// That is also why the delegator cannot resume the gate at all once their slot
	// is gone -- the check is not only the identity guard inside the node, it is
	// that the name is never armed again. The refused signal below therefore needs
	// no arming of its own and resumes nothing, which the final suspension count
	// pins.
	steps := []struct {
		signal     string
		body       map[string]any
		armedAfter int
	}{
		{delegatedApprovalNode + "/approval/alice", map[string]any{
			types.VerifiedActorKey: "alice", "action": "delegate", "assignee": "dave",
		}, 1},
		{delegatedApprovalNode + "/approval/dave", map[string]any{
			types.VerifiedActorKey: "dave", "action": "add_signer", "assignee": "erin", "position": "after",
		}, 2},
		{delegatedApprovalNode + "/approval/bob", map[string]any{
			types.VerifiedActorKey: "bob", "action": "approve",
		}, 3},
		{delegatedApprovalNode + "/approval/dave", map[string]any{
			types.VerifiedActorKey: "dave", "action": "approve",
		}, 4},
		// The delegator's own credentials are no longer a slot: this is refused,
		// and must not open the gate or add a decision.
		{delegatedApprovalNode + "/approval/alice", map[string]any{
			types.VerifiedActorKey: "alice", "action": "approve",
		}, 5},
		{delegatedApprovalNode + "/approval/erin", map[string]any{
			types.VerifiedActorKey: "erin", "action": "approve",
		}, 5},
	}
	for _, step := range steps {
		hooks.waitForSuspensions(t, delegatedApprovalNode, step.armedAfter)
		if err := eng.Signal(ctx, id, step.signal, step.body); err != nil {
			t.Fatalf("Signal(%s) error = %v", step.signal, err)
		}
	}

	result, err := eng.Wait(ctx, id)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("status = %q, want success; workflow error: %s", result.Status, result.Error)
	}

	gateOut := mustNodeOutput(t, result, delegatedApprovalNode)
	if gateOut["approved"] != true {
		t.Fatalf("approved = %v, want true", gateOut["approved"])
	}
	decisions, ok := gateOut["decisions"].([]map[string]any)
	if !ok {
		t.Fatalf("decisions = %#v, want a decision list", gateOut["decisions"])
	}

	// The trail is the record of how the gate reached its decision, so its shape
	// is asserted: a delegation, an added signer, and the three slots that then
	// had to decide. The delegator's refused approval is absent, and its absence
	// is the point -- alice's slot was handed on, so nothing she sends afterwards
	// decides it.
	want := []struct {
		approver string
		action   string
	}{
		{"alice", "delegate"},
		{"dave", "add_signer"},
		{"bob", "approve"},
		{"dave", "approve"},
		{"erin", "approve"},
	}
	if len(decisions) != len(want) {
		t.Fatalf("decisions = %v, want %d entries", decisions, len(want))
	}
	for i, expect := range want {
		if decisions[i]["approver"] != expect.approver || decisions[i]["action"] != expect.action {
			t.Fatalf("decision[%d] = %v, want %s %s", i, decisions[i], expect.approver, expect.action)
		}
	}

	// Five suspensions: the initial wait plus one re-arm per accepted signal
	// before the last one. The delegator's refused approval is not among them --
	// it resumed the gate no more than it decided anything, because the name it
	// was delivered on was no longer one the gate waits for.
	if got := hooks.suspensions(delegatedApprovalNode); got != 5 {
		t.Fatalf("%s suspended %d times, want 5", delegatedApprovalNode, got)
	}
}

// approvalSuspensionHooks records when a node suspends, so the test can send each
// signal only once the gate is waiting for it.
type approvalSuspensionHooks struct {
	engine.BaseHooks

	suspended map[string]int
}

func (h *approvalSuspensionHooks) OnNodeSuspended(_ context.Context, _ types.ExecutionID, name string) {
	if h.suspended == nil {
		h.suspended = make(map[string]int)
	}
	h.suspended[name]++
}

func (h *approvalSuspensionHooks) waitForSuspensions(t *testing.T, nodeName string, want int) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if h.suspended[nodeName] >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s suspended %d times, want >= %d", nodeName, h.suspended[nodeName], want)
}

// suspensions is read after the run has finished, so the count is final and has
// a single right answer.
func (h *approvalSuspensionHooks) suspensions(nodeName string) int {
	return h.suspended[nodeName]
}
