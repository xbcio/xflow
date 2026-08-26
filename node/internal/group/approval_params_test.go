package group_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// parseApprovalParams (approval.go) has several validation branches that no
// existing test reaches: every test in approval_test.go / approval_bounds_test.go
// / approval_decision_test.go builds Params via approvalInput(), which always
// supplies a well-formed []any of strings for "approvers" and, at most, a
// valid duration string for "timeout". The branches below are only exercised
// through this file.

func TestApproval_PrepareSuspendRejectsMissingApprovers(t *testing.T) {
	sh := node.Approval([]string{"alice"}, node.ApprovalAny)
	_, err := sh.PrepareSuspend(context.Background(), &types.Input{
		NodeName: "g",
		Params:   map[string]any{"mode": "any"},
	})
	if err == nil {
		t.Fatal("expected error when Params has no \"approvers\" key at all")
	}
	if !strings.Contains(err.Error(), "must not be empty") {
		t.Fatalf("error = %q, want substring %q", err.Error(), "must not be empty")
	}
}

func TestApproval_PrepareSuspendRejectsNonListApprovers(t *testing.T) {
	sh := node.Approval([]string{"alice"}, node.ApprovalAny)
	_, err := sh.PrepareSuspend(context.Background(), &types.Input{
		NodeName: "g",
		Params:   map[string]any{"approvers": "alice", "mode": "any"},
	})
	if err == nil {
		t.Fatal("expected error when approvers is a bare string instead of a list")
	}
	if !strings.Contains(err.Error(), "list of strings") {
		t.Fatalf("error = %q, want substring %q", err.Error(), "list of strings")
	}
}

func TestApproval_PrepareSuspendRejectsNonStringApproverElement(t *testing.T) {
	sh := node.Approval([]string{"alice"}, node.ApprovalAny)
	_, err := sh.PrepareSuspend(context.Background(), &types.Input{
		NodeName: "g",
		Params:   map[string]any{"approvers": []any{"alice", 42}, "mode": "any"},
	})
	if err == nil {
		t.Fatal("expected error when an approvers element is not a string")
	}
	if !strings.Contains(err.Error(), "list of strings") {
		t.Fatalf("error = %q, want substring %q", err.Error(), "list of strings")
	}
}

func TestApproval_PrepareSuspendRejectsInvalidTimeoutDuration(t *testing.T) {
	sh := node.Approval([]string{"alice"}, node.ApprovalAny)
	_, err := sh.PrepareSuspend(context.Background(), &types.Input{
		NodeName: "g",
		Params:   map[string]any{"approvers": []any{"alice"}, "mode": "any", "timeout": "not-a-duration"},
	})
	if err == nil {
		t.Fatal("expected error for an unparsable timeout duration string")
	}
	if !strings.Contains(err.Error(), "invalid timeout duration") {
		t.Fatalf("error = %q, want substring %q", err.Error(), "invalid timeout duration")
	}
}

func TestApproval_PrepareSuspendTimeoutFloat64IsNanoseconds(t *testing.T) {
	sh := node.Approval([]string{"alice"}, node.ApprovalAny)
	spec, err := sh.PrepareSuspend(context.Background(), &types.Input{
		NodeName: "g",
		Params: map[string]any{
			"approvers": []any{"alice"},
			"mode":      "any",
			// A JSON round trip decodes a numeric timeout as float64. The
			// code takes this value directly as nanoseconds (time.Duration's
			// own unit), not seconds.
			"timeout": float64(5 * time.Second),
		},
	})
	if err != nil {
		t.Fatalf("PrepareSuspend() error = %v", err)
	}
	if spec.Timeout != 5*time.Second {
		t.Fatalf("Timeout = %v, want %v: a float64 timeout must be read as nanoseconds", spec.Timeout, 5*time.Second)
	}
}

func TestApproval_PrepareSuspendTimeoutNativeDurationPassthrough(t *testing.T) {
	sh := node.Approval([]string{"alice"}, node.ApprovalAny)
	spec, err := sh.PrepareSuspend(context.Background(), &types.Input{
		NodeName: "g",
		Params: map[string]any{
			"approvers": []any{"alice"},
			"mode":      "any",
			// A caller building Params programmatically (not through JSON)
			// may hand over a native time.Duration value directly.
			"timeout": 3 * time.Minute,
		},
	})
	if err != nil {
		t.Fatalf("PrepareSuspend() error = %v", err)
	}
	if spec.Timeout != 3*time.Minute {
		t.Fatalf("Timeout = %v, want %v: a native time.Duration value must pass through unchanged", spec.Timeout, 3*time.Minute)
	}
}
