// Package examples demonstrates how to build and run workflows with the xflow
// embedded engine in local mode (no external dependencies).
//
// Scenario: Employee expense reimbursement approval.
//
// DAG topology:
//
//	ParseClaim ──→ EnrichClaim ──→ PolicyCheck ──main──→ AutoApprove
//	                                    ╚──error──→ RequestReview
//
// Key patterns demonstrated:
//
//  1. Sequential function node calls  (ParseClaim → EnrichClaim)
//  2. Error-port branching            (PolicyCheck OnError=error_output)
//  3. Fatal system error              (ParseClaim fails → workflow status=failed)
//
// This file intentionally uses LocalNode for a concise local-mode example.
// For distributed services, use typed nodes declared with node.Define as shown
// in vulnerability_approval_test.go.
//
// Note on branching: the engine is a DAG engine with port-aware routing.
// When PolicyCheck completes on the "main" port, only downstream nodes
// connected to "main" (AutoApprove) are scheduled. Nodes connected to
// inactive ports (RequestReview via "error") are automatically skipped
// by the engine — no manual branch guard is needed in handlers.
package examples_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/xbcio/xflow/nodes/node"
	"github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/types"
)

// ── policy limits ─────────────────────────────────────────────────────────────

// policyLimits maps expense category to the CNY auto-approval ceiling.
// Claims above this limit require finance manager sign-off.
var policyLimits = map[string]float64{
	"travel":    5000,
	"meals":     500,
	"equipment": 20000,
	"training":  3000,
}

// ── handlers ──────────────────────────────────────────────────────────────────

// parseClaimHandler validates and normalises the expense claim.
//
// Reads from input.Data — which is the workflow-level params map injected by
// the engine for source (zero-in-degree) nodes.
//
// OnError strategy: default (stop). A missing or negative amount is a fatal
// input error; the workflow cannot continue without valid claim data.
type parseClaimHandler struct{}

func (h *parseClaimHandler) Descriptor() node.Descriptor {
	return node.Descriptor{Type: "demo.expense.parse", DisplayName: "Parse Claim"}
}

func (h *parseClaimHandler) Execute(_ context.Context, input *node.Input) (*node.Output, error) {
	amount, ok := input.Data["amount"].(float64)
	if !ok || amount <= 0 {
		return nil, fmt.Errorf("parse claim: amount must be a positive number, got %v", input.Data["amount"])
	}
	category, _ := input.Data["category"].(string)
	if category == "" {
		return nil, fmt.Errorf("parse claim: category is required")
	}
	submitter, _ := input.Data["submitter"].(string)
	if submitter == "" {
		submitter = "anonymous"
	}
	return &node.Output{Data: map[string]any{
		"amount":    amount,
		"category":  category,
		"submitter": submitter,
		"currency":  "CNY",
	}}, nil
}

// enrichClaimHandler attaches policy metadata to the claim.
// This represents a typical lookup/enrichment step (e.g. calling an internal
// policy service or database).
type enrichClaimHandler struct{}

func (h *enrichClaimHandler) Descriptor() node.Descriptor {
	return node.Descriptor{Type: "demo.expense.enrich", DisplayName: "Enrich Claim"}
}

func (h *enrichClaimHandler) Execute(_ context.Context, input *node.Input) (*node.Output, error) {
	category, _ := input.Data["category"].(string)
	limit, found := policyLimits[category]
	if !found {
		limit = 1000 // default ceiling for unlisted categories
	}

	out := make(map[string]any, len(input.Data)+2)
	for k, v := range input.Data {
		out[k] = v
	}
	out["policy_limit"] = limit
	out["department"] = "engineering"
	return &node.Output{Data: out}, nil
}

// policyCheckNode is a reusable typed node definition (demo.expense.policy).
// It decides whether a claim can be auto-approved or requires manual review.
//
//   - Within limit  → success {approved:true, ...}  exits via "main" port.
//   - Over limit    → business error {approved:false, ...} exits via "error"
//     port (when the node is configured with OnError(node.OnErrorOutput)).
//
// Both cases populate Output.Data with the routing flag "approved" so that
// downstream handlers can determine which branch is active without relying on
// port topology alone.
var policyCheckNode = node.Define("demo.expense.policy", func(_ context.Context, input *node.Input) (*node.Output, error) {
	amount, _ := input.Data["amount"].(float64)
	limit, _ := input.Data["policy_limit"].(float64)

	if amount > limit {
		// Business error: amount exceeds policy limit → route to "error" port.
		return &node.Output{
			Data: map[string]any{
				"approved": false,
				"amount":   amount,
				"limit":    limit,
				"reason":   fmt.Sprintf("%.2f CNY exceeds policy limit of %.2f", amount, limit),
			},
			Error: &node.Error{
				Message:    fmt.Sprintf("claim exceeds policy limit: %.2f > %.2f", amount, limit),
				StatusCode: 422,
			},
		}, nil
	}

	// Within limit → route to "main" port.
	return &node.Output{Data: map[string]any{
		"approved": true,
		"amount":   amount,
		"limit":    limit,
	}}, nil
}).DisplayName("Policy Check").
	Output("main").
	Output("error")

// autoApproveHandler finalises a claim that passed the policy check.
// Connected to PolicyCheck's "main" output port.
// Only executed when PolicyCheck routes to "main" (within-limit claims).
type autoApproveHandler struct{}

func (h *autoApproveHandler) Descriptor() node.Descriptor {
	return node.Descriptor{Type: "demo.expense.auto_approve", DisplayName: "Auto Approve"}
}

func (h *autoApproveHandler) Execute(_ context.Context, input *node.Input) (*node.Output, error) {
	amount, _ := input.Data["amount"].(float64)
	return &node.Output{Data: map[string]any{
		"status":  "auto_approved",
		"amount":  amount,
		"message": fmt.Sprintf("%.2f CNY approved automatically — within policy", amount),
	}}, nil
}

// requestReviewHandler queues an over-limit claim for manager approval.
// Connected to PolicyCheck's "error" output port.
// Only executed when PolicyCheck routes to "error" (over-limit claims).
type requestReviewHandler struct{}

func (h *requestReviewHandler) Descriptor() node.Descriptor {
	return node.Descriptor{Type: "demo.expense.request_review", DisplayName: "Request Review"}
}

func (h *requestReviewHandler) Execute(_ context.Context, input *node.Input) (*node.Output, error) {
	amount, _ := input.Data["amount"].(float64)
	reason, _ := input.Data["reason"].(string)
	return &node.Output{Data: map[string]any{
		"status":  "review_requested",
		"amount":  amount,
		"reason":  reason,
		"message": "claim forwarded to finance manager for manual approval",
	}}, nil
}

// ── workflow factory ──────────────────────────────────────────────────────────

// buildExpenseWorkflow constructs the reimbursement approval workflow.
//
// Topology:
//
//	ParseClaim ──→ EnrichClaim ──→ PolicyCheck ──main──→ AutoApprove
//	                                    ╚──error──→ RequestReview
//
// The workflow-level params passed to Run/Submit are injected by the engine
// into ParseClaim (the sole source node) as input.Data.
func buildExpenseWorkflow() *xflow.WorkflowBuilder {
	wf := xflow.Workflow("expense-claim")

	// ParseClaim: direct handler — validates the raw claim params.
	// Reads from input.Data (injected from workflow-level submission params).
	parse := wf.LocalNode("ParseClaim", &parseClaimHandler{})

	// EnrichClaim: direct handler — attaches policy limit and department.
	enrich := wf.LocalNode("EnrichClaim", &enrichClaimHandler{})

	// PolicyCheck: registered handler with OnErrorOutput routing.
	// Within-limit claims exit via "main"; over-limit claims exit via "error".
	check := wf.Node("PolicyCheck",
		policyCheckNode.New(nil).OnError(node.OnErrorOutput),
	)

	approve := wf.LocalNode("AutoApprove", &autoApproveHandler{})
	review := wf.LocalNode("RequestReview", &requestReviewHandler{})

	wf.Connect(parse.Output("main"), enrich).
		Connect(enrich.Output("main"), check).
		Connect(check.Output("main"), approve). // within-policy branch
		Connect(check.Output("error"), review)  // over-policy branch

	return wf
}

// ── tests ─────────────────────────────────────────────────────────────────────

// TestExpenseClaim runs three reimbursement scenarios.
func TestExpenseClaim(t *testing.T) {
	ctx := context.Background()

	t.Run("small amount — auto approved", func(t *testing.T) {
		// 200 CNY meals claim is under the 500 CNY meals limit.
		params := map[string]any{
			"amount":    float64(200),
			"category":  "meals",
			"submitter": "alice",
		}
		result := runExpenseWorkflow(t, ctx, params)
		if result.Status != types.ExecutionStatusSuccess {
			t.Fatalf("status = %q, want success; workflow error: %s", result.Status, result.Error)
		}

		approveOut := mustNodeOutput(t, result, "AutoApprove")
		if approveOut["status"] != "auto_approved" {
			t.Errorf("AutoApprove.status = %q, want \"auto_approved\"", approveOut["status"])
		}
		if _, ok := result.Output["RequestReview"]; ok {
			t.Errorf("RequestReview should be skipped (not in output), but found: %v", result.Output["RequestReview"])
		}
		t.Logf("outcome: %v", approveOut["message"])
	})

	t.Run("large amount — manual review", func(t *testing.T) {
		// 800 CNY meals claim exceeds the 500 CNY meals limit.
		// PolicyCheck returns a business error; OnErrorOutput routes it to the
		// "error" port. The workflow itself still completes with status=success.
		params := map[string]any{
			"amount":    float64(800),
			"category":  "meals",
			"submitter": "bob",
		}
		result := runExpenseWorkflow(t, ctx, params)
		if result.Status != types.ExecutionStatusSuccess {
			t.Fatalf("status = %q, want success; workflow error: %s", result.Status, result.Error)
		}

		reviewOut := mustNodeOutput(t, result, "RequestReview")
		if reviewOut["status"] != "review_requested" {
			t.Errorf("RequestReview.status = %q, want \"review_requested\"", reviewOut["status"])
		}
		if _, ok := result.Output["AutoApprove"]; ok {
			t.Errorf("AutoApprove should be skipped (not in output), but found: %v", result.Output["AutoApprove"])
		}
		t.Logf("outcome: %v", reviewOut["message"])
	})

	t.Run("invalid input — workflow fails", func(t *testing.T) {
		// amount is missing; ParseClaim returns a system error.
		// Default OnErrorStop causes the workflow to fail immediately —
		// neither EnrichClaim, PolicyCheck, nor the branch nodes run.
		params := map[string]any{
			"category":  "travel",
			"submitter": "charlie",
		}
		result := runExpenseWorkflow(t, ctx, params)
		if result.Status != types.ExecutionStatusFailed {
			t.Fatalf("status = %q, want failed", result.Status)
		}
		t.Logf("workflow failed as expected: %s", result.Error)
	})
}

// ── helpers ───────────────────────────────────────────────────────────────────

func runExpenseWorkflow(t *testing.T, ctx context.Context, params map[string]any) types.Result {
	t.Helper()
	eng, err := xflow.NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error: %v", err)
	}
	defer eng.Stop()

	id, err := eng.Submit(ctx, buildExpenseWorkflow(), params)
	if err != nil {
		t.Fatalf("Submit() error: %v", err)
	}
	result, err := eng.Wait(ctx, id)
	if err != nil {
		t.Fatalf("Wait() error: %v", err)
	}
	return result
}

// mustNodeOutput asserts that the named node produced a map output and returns it.
func mustNodeOutput(t *testing.T, result types.Result, nodeName string) map[string]any {
	t.Helper()
	raw, ok := result.Output[nodeName]
	if !ok {
		t.Fatalf("node %q: output missing from result", nodeName)
	}
	out, ok := raw.(map[string]any)
	if !ok {
		t.Fatalf("node %q: output is %T, want map[string]any", nodeName, raw)
	}
	return out
}
