package protocol

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

const (
	ProtocolVersion = 1
	RenewLeasePath  = "/v1/runners/lease/renew"

	// FeatureGroupProtocolV1 is the capability feature string a runner
	// advertises (in Capability.Features) to indicate it supports the v1
	// group co-location protocol — receiving GroupLeaseWire payloads and
	// reporting GroupResultWire outcomes. Servers use this to gate group-aware
	// task dispatch: only runners that report this feature receive group
	// leases.
	FeatureGroupProtocolV1 = "group_protocol_v1"
)

// GroupLeaseWire is the on-wire representation of a group lease payload sent
// to runners. GroupUnitIdx is the live-wire unit authority — after decode the
// runner MUST validate it against the task's UnitIdx; mismatch = fail closed.
type GroupLeaseWire struct {
	ProtocolVersion int                  `json:"protocol_version"`
	GroupExecID     string               `json:"group_exec_id"`
	GroupID         string               `json:"group_id"`
	GroupUnitIdx    int                   `json:"group_unit_idx"`
	WorkflowVersion string               `json:"workflow_version,omitempty"`
	GraphHash       string               `json:"graph_hash,omitempty"`
	PackageHash     string               `json:"package_hash"`
	Package         *graph.GroupPackage   `json:"package,omitempty"`
	Input           *types.Input          `json:"input,omitempty"`
	IdempotencyKey  string               `json:"idempotency_key"`
	Deadline        *time.Time           `json:"deadline,omitempty"`
}

// GroupResultWire is the on-wire representation of a group execution result
// reported by a runner.
type GroupResultWire struct {
	ProtocolVersion int                     `json:"protocol_version"`
	GroupExecID     string                  `json:"group_exec_id"`
	Attempt         int                     `json:"attempt"`
	Outcome         engine.GroupOutcome     `json:"outcome"`
	Exits           []GroupExitResultWire   `json:"exits,omitempty"`
	Error           string                  `json:"error,omitempty"`
}

// GroupExitResultWire is a single boundary exit port output on the wire.
type GroupExitResultWire struct {
	NodeName string         `json:"node_name"`
	Port     string         `json:"port"`
	Data     map[string]any `json:"data,omitempty"`
}

// RenewLeaseRequest is the wire request for extending an active lease.
type RenewLeaseRequest struct {
	RunnerID  string `json:"runner_id"`
	SessionID string `json:"session_id"`
	LeaseID   string `json:"lease_id"`
	LeaseToken string `json:"lease_token"`
	Extend    int64  `json:"extend_ms"`
	AuthToken string `json:"auth_token,omitempty"`
}

// RenewLeaseResponse is the wire response for a lease renewal.
type RenewLeaseResponse struct {
	Renewed  bool      `json:"renewed"`
	Deadline time.Time `json:"deadline,omitempty"`
	Error    string    `json:"error,omitempty"`
}

// MarshalGroupLease encodes an engine GroupLeasePayload to wire JSON.
func MarshalGroupLease(p *engine.GroupLeasePayload) ([]byte, error) {
	if p == nil {
		return nil, fmt.Errorf("nil group lease payload")
	}
	var deadline *time.Time
	if !p.Deadline.IsZero() {
		deadline = &p.Deadline
	}
	wire := GroupLeaseWire{
		ProtocolVersion: p.ProtocolVersion,
		GroupExecID:     p.GroupExecID,
		GroupID:         p.GroupID,
		GroupUnitIdx:    p.GroupUnitIdx,
		WorkflowVersion: p.WorkflowVersion,
		GraphHash:       p.GraphHash,
		PackageHash:     p.PackageHash,
		Package:         p.Package,
		Input:           p.Input,
		IdempotencyKey:  p.IdempotencyKey,
		Deadline:        deadline,
	}
	return json.Marshal(wire)
}

// UnmarshalGroupLease decodes a wire JSON group lease into the engine payload.
// Returns an error if the payload is missing the GroupUnitIdx field (old format
// without unit identity must fail closed for group payloads).
func UnmarshalGroupLease(data []byte) (*engine.GroupLeasePayload, error) {
	// Use raw message to detect presence of group_unit_idx.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	if _, ok := raw["group_unit_idx"]; !ok {
		return nil, fmt.Errorf("group lease payload missing group_unit_idx (old format without unit identity not supported for group payloads)")
	}

	var wire GroupLeaseWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return nil, err
	}

	var deadline time.Time
	if wire.Deadline != nil {
		deadline = *wire.Deadline
	}

	return &engine.GroupLeasePayload{
		ProtocolVersion: wire.ProtocolVersion,
		GroupExecID:     wire.GroupExecID,
		GroupID:         wire.GroupID,
		GroupUnitIdx:    wire.GroupUnitIdx,
		WorkflowVersion: wire.WorkflowVersion,
		GraphHash:       wire.GraphHash,
		PackageHash:     wire.PackageHash,
		Package:         wire.Package,
		Input:           wire.Input,
		IdempotencyKey:  wire.IdempotencyKey,
		Deadline:        deadline,
	}, nil
}

// MarshalGroupResult encodes a group result to wire JSON.
func MarshalGroupResult(res engine.GroupResult) ([]byte, error) {
	exits := make([]GroupExitResultWire, 0, len(res.Exits))
	for _, e := range res.Exits {
		exits = append(exits, GroupExitResultWire{
			NodeName: e.NodeName,
			Port:     e.Port,
			Data:     e.Data,
		})
	}
	wire := GroupResultWire{
		ProtocolVersion: res.ProtocolVersion,
		GroupExecID:     res.GroupExecID,
		Attempt:         res.Attempt,
		Outcome:         res.Outcome,
		Exits:           exits,
		Error:           res.Error,
	}
	return json.Marshal(wire)
}

// UnmarshalGroupResult decodes a wire JSON group result.
func UnmarshalGroupResult(data []byte) (engine.GroupResult, error) {
	var wire GroupResultWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return engine.GroupResult{}, err
	}
	exits := make([]engine.GroupExitResult, 0, len(wire.Exits))
	for _, e := range wire.Exits {
		exits = append(exits, engine.GroupExitResult{
			NodeName: e.NodeName,
			Port:     e.Port,
			Data:     e.Data,
		})
	}
	return engine.GroupResult{
		ProtocolVersion: wire.ProtocolVersion,
		GroupExecID:     wire.GroupExecID,
		Attempt:         wire.Attempt,
		Outcome:         wire.Outcome,
		Exits:           exits,
		Error:           wire.Error,
	}, nil
}
