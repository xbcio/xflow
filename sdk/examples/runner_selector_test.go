package examples_test

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/types"
)

var collectRiskSignalNode = node.Define("demo.runner_selector.collect_risk_signal",
	func(_ context.Context, input *types.Input) (*types.Output, error) {
		return &types.Output{Data: map[string]any{
			"ticket":   input.Data["ticket"],
			"severity": input.Data["severity"],
			"source":   "remote-sensor",
		}}, nil
	},
).DisplayName("Collect Risk Signal")

var recordRunnerSelectorDecisionNode = node.Define("demo.runner_selector.record_decision",
	func(_ context.Context, input *types.Input) (*types.Output, error) {
		return &types.Output{Data: map[string]any{
			"ticket":   input.Data["ticket"],
			"severity": input.Data["severity"],
			"source":   input.Data["source"],
			"status":   "approved",
		}}, nil
	},
).DisplayName("Record Decision")

func buildRunnerSelectorWorkflow() *xflow.WorkflowBuilder {
	wf := xflow.Workflow("runner-selector-risk-review").
		RunnerSelector(xflow.DefaultRunnerSelector(map[string]string{
			"mode":   "remote",
			"env":    "prod",
			"tenant": "tenant-a",
		}))

	start := wf.Node("start", node.Start())
	collect := wf.Node("CollectRiskSignal", collectRiskSignalNode.New(nil))

	// In default mode, this node-level selector replaces the workflow selector.
	// NewLocal preserves and validates this metadata, but it does not place
	// tasks on runners. Server + runner protocol is the enforcement path.
	approval := wf.Node("SecurityApproval",
		node.Approval([]string{"sec-owner"}, node.ApprovalAny),
	).RunnerSelector(xflow.RunnerSelector(map[string]string{
		"mode": "local",
		"env":  "prod",
	}))

	record := wf.Node("RecordDecision", recordRunnerSelectorDecisionNode.New(nil))

	wf.Connect(start, collect).
		Connect(collect, approval).
		Connect(approval.Output("approved"), record)

	return wf
}

func TestRunnerSelectorWorkflowLocalExample(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	eng, err := xflow.NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error: %v", err)
	}
	defer eng.Stop()

	workflowID, err := eng.AddWorkflow(ctx, buildRunnerSelectorWorkflow())
	if err != nil {
		t.Fatalf("AddWorkflow() error: %v", err)
	}

	id, err := eng.Invoke(ctx, workflowID, xflow.Start(), map[string]any{
		"ticket":   "RISK-2026-001",
		"severity": "high",
	})
	if err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}

	waitForExampleNodeStatus(t, ctx, eng, id, "SecurityApproval", types.NodeStatusSuspended)
	approve(t, ctx, eng, id, "SecurityApproval/approval", "sec-owner", "local approval accepted")

	result, err := eng.Wait(ctx, id)
	if err != nil {
		t.Fatalf("Wait() error: %v", err)
	}
	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("status = %q, want success; workflow error: %s", result.Status, result.Error)
	}

	out := mustNodeOutput(t, result, "RecordDecision")
	if out["status"] != "approved" {
		t.Fatalf("status = %q, want approved", out["status"])
	}
	if out["source"] != "remote-sensor" {
		t.Fatalf("source = %q, want remote-sensor", out["source"])
	}
}
