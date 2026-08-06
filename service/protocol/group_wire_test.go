package protocol

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

func TestGroupLeaseWire_RoundTrip(t *testing.T) {
	deadline := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	payload := &engine.GroupLeasePayload{
		ProtocolVersion: 1,
		GroupExecID:     "exec-123",
		GroupID:         "exec-1/grp1/0",
		GroupUnitIdx:    3,
		WorkflowVersion: "v1",
		GraphHash:       "sha256:abc",
		PackageHash:     "pkg-sha256:v1:def",
		Package: &graph.SubgraphPackage{
			Version:   1,
			GroupName: "grp1",
			EntryNode: "A",
			Def: &types.WorkflowDef{
				Name: "grp1",
				Nodes: []types.NodeDef{
					{Name: "A", Type: "http.request", Version: 1},
				},
			},
			Requirements: []graph.Requirement{
				{NodeType: "http.request", NodeVersion: 1},
			},
		},
		Input: &types.Input{
			Data: map[string]any{"key": "value"},
		},
		IdempotencyKey: "normal/exec-1/grp1/0",
		Deadline:       deadline,
	}

	data, err := MarshalGroupLease(payload)
	if err != nil {
		t.Fatalf("MarshalGroupLease: %v", err)
	}

	decoded, err := UnmarshalGroupLease(data)
	if err != nil {
		t.Fatalf("UnmarshalGroupLease: %v", err)
	}

	if decoded.GroupUnitIdx != 3 {
		t.Errorf("GroupUnitIdx = %d, want 3", decoded.GroupUnitIdx)
	}
	if decoded.GroupID != "exec-1/grp1/0" {
		t.Errorf("GroupID = %q, want exec-1/grp1/0", decoded.GroupID)
	}
	if decoded.PackageHash != "pkg-sha256:v1:def" {
		t.Errorf("PackageHash = %q, want pkg-sha256:v1:def", decoded.PackageHash)
	}
	if decoded.Package == nil {
		t.Error("Package nil after round-trip")
	}
	if decoded.Input == nil {
		t.Error("Input nil after round-trip")
	}
	if decoded.IdempotencyKey != "normal/exec-1/grp1/0" {
		t.Errorf("IdempotencyKey = %q", decoded.IdempotencyKey)
	}
	if !decoded.Deadline.Equal(deadline) {
		t.Errorf("Deadline = %v, want %v", decoded.Deadline, deadline)
	}
}

func TestGroupLeaseWire_MissingUnitIdxFailsClosed(t *testing.T) {
	// Simulate old-format payload without group_unit_idx.
	oldPayload := `{"protocol_version":1,"group_exec_id":"x","group_id":"y","package_hash":"h","idempotency_key":"k"}`
	_, err := UnmarshalGroupLease([]byte(oldPayload))
	if err == nil {
		t.Fatal("expected error for payload missing group_unit_idx")
	}
	if !strings.Contains(err.Error(), "group_unit_idx") {
		t.Errorf("error = %v, want mention of group_unit_idx", err)
	}
}

func TestGroupResultWire_RoundTrip(t *testing.T) {
	result := engine.GroupResult{
		ProtocolVersion: 1,
		GroupExecID:     "exec-123",
		Attempt:         2,
		Outcome:         engine.GroupOutcomeSuccess,
		Exits: []engine.GroupExitResult{
			{NodeName: "C", Port: "result", Data: map[string]any{"n": 42}},
		},
	}

	data, err := MarshalGroupResult(result)
	if err != nil {
		t.Fatalf("MarshalGroupResult: %v", err)
	}

	decoded, err := UnmarshalGroupResult(data)
	if err != nil {
		t.Fatalf("UnmarshalGroupResult: %v", err)
	}

	if decoded.Outcome != engine.GroupOutcomeSuccess {
		t.Errorf("Outcome = %q, want success", decoded.Outcome)
	}
	if len(decoded.Exits) != 1 {
		t.Fatalf("exits = %d, want 1", len(decoded.Exits))
	}
	if decoded.Exits[0].NodeName != "C" || decoded.Exits[0].Port != "result" {
		t.Errorf("exit = %+v, want C:result", decoded.Exits[0])
	}
	if decoded.Attempt != 2 {
		t.Errorf("Attempt = %d, want 2", decoded.Attempt)
	}
}

func TestGroupResultWire_FailedWithError(t *testing.T) {
	result := engine.GroupResult{
		ProtocolVersion: 1,
		GroupExecID:     "exec-456",
		Attempt:         1,
		Outcome:         engine.GroupOutcomeFailed,
		Error:           "timeout exceeded",
	}

	data, err := MarshalGroupResult(result)
	if err != nil {
		t.Fatalf("MarshalGroupResult: %v", err)
	}

	decoded, err := UnmarshalGroupResult(data)
	if err != nil {
		t.Fatalf("UnmarshalGroupResult: %v", err)
	}

	if decoded.Outcome != engine.GroupOutcomeFailed {
		t.Errorf("Outcome = %q, want failed", decoded.Outcome)
	}
	if decoded.Error != "timeout exceeded" {
		t.Errorf("Error = %q, want 'timeout exceeded'", decoded.Error)
	}
}

func TestCapability_NewFieldsBackwardsCompat(t *testing.T) {
	// Old-format capability (only node_type + node_version).
	oldJSON := `{"node_type":"http.request","node_version":1}`
	var cap Capability
	if err := json.Unmarshal([]byte(oldJSON), &cap); err != nil {
		t.Fatalf("Unmarshal old capability: %v", err)
	}
	if cap.NodeType != "http.request" {
		t.Errorf("NodeType = %q", cap.NodeType)
	}
	if cap.Runtimes != nil || cap.Features != nil || cap.Resources != nil || cap.Credentials != nil {
		t.Error("new fields should be nil for old-format capability")
	}

	// New-format capability with all fields.
	newJSON := `{"node_type":"code.python","node_version":2,"runtimes":["python3.11"],"features":["group.exec.v1"],"resources":["resource_pool.sql"],"credentials":["db_cred"]}`
	var cap2 Capability
	if err := json.Unmarshal([]byte(newJSON), &cap2); err != nil {
		t.Fatalf("Unmarshal new capability: %v", err)
	}
	if len(cap2.Runtimes) != 1 || cap2.Runtimes[0] != "python3.11" {
		t.Errorf("Runtimes = %v", cap2.Runtimes)
	}
	if len(cap2.Features) != 1 || cap2.Features[0] != "group.exec.v1" {
		t.Errorf("Features = %v", cap2.Features)
	}
	if len(cap2.Resources) != 1 || cap2.Resources[0] != "resource_pool.sql" {
		t.Errorf("Resources = %v", cap2.Resources)
	}
	if len(cap2.Credentials) != 1 || cap2.Credentials[0] != "db_cred" {
		t.Errorf("Credentials = %v", cap2.Credentials)
	}
}

func TestCapability_NoSecretValues(t *testing.T) {
	// Credentials field must only contain reference names, never secrets.
	cap := Capability{
		NodeType:    "http.request",
		NodeVersion: 1,
		Credentials: []string{"my_api_key", "oauth_token"},
	}
	data, _ := json.Marshal(cap)
	s := string(data)
	// Sanity: the serialized form should NOT contain anything that looks like
	// a real secret (these are just reference names).
	if strings.Contains(s, "sk_live_") || strings.Contains(s, "AKIA") {
		t.Error("serialized capability contains suspected secret value")
	}
}

func TestRenewLeaseRequest_RoundTrip(t *testing.T) {
	req := RenewLeaseRequest{
		RunnerID:   "runner-1",
		SessionID:  "sess-1",
		LeaseID:    "lease-abc",
		LeaseToken: "token-xyz",
		Extend:     30000, // 30s in ms
	}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var decoded RenewLeaseRequest
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.LeaseID != "lease-abc" || decoded.LeaseToken != "token-xyz" {
		t.Errorf("decoded = %+v", decoded)
	}
	if decoded.Extend != 30000 {
		t.Errorf("Extend = %d, want 30000", decoded.Extend)
	}
}
