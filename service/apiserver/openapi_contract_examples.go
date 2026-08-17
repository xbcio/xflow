package apiserver

import (
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// This file exists only to let the api/openapi contract test marshal the REAL
// handler values. The handler response/request structs are unexported on
// purpose (they are internal wire shapes), but the contract test must marshal
// exactly what the handler writes so a renamed JSON tag or an omitempty
// mistake surfaces as a schema mismatch rather than a vacuous {}. Each Example*
// constructor builds the real typed value with every required field non-zero
// and returns it as any; the caller (api/openapi) json-marshals it and
// validates the result against the OpenAPI schema. The values are deliberately
// non-zero: a zero value plus omitempty serializes to {} and would pass any
// schema.

// ExampleEnvelope builds a real success envelope (the apiserver envelope struct
// that writeData/writeFail serialize) so the contract's Envelope schema is
// validated against the actual wire shape, not a hand-written stand-in.
func ExampleEnvelope() any {
	return envelope{
		Success: true,
		Code:    "200",
		Message: "",
		Data:    map[string]any{"workflow_id": "wf-01H8XG"},
		TraceID: "4bf92f3577b34da6a3ce929d0e0e4736",
	}
}

// ExampleRegisterWorkflowResponse builds a registerWorkflowResponse (POST/PUT
// /v1/workflows, /v1/workflows/{id}).
func ExampleRegisterWorkflowResponse(id string, warnings []string) any {
	return registerWorkflowResponse{WorkflowID: types.WorkflowID(id), Warnings: warnings}
}

// ExampleExecuteWorkflowResponse builds an executeWorkflowResponse (POST
// /v1/workflows/execute, /v1/workflows/{id}/execute).
func ExampleExecuteWorkflowResponse(id types.ExecutionID) any {
	return executeWorkflowResponse{ExecutionID: id}
}

// ExampleSignalRequest builds a signalRequest (POST /v1/executions/{id}/signals).
func ExampleSignalRequest(name string, data map[string]any) any {
	return signalRequest{Name: name, Data: data}
}

// ExampleExecuteWorkflowRequest builds an executeWorkflowRequest (POST
// /v1/workflows/execute) with a minimal inline definition so every required
// field of the request body is non-zero.
func ExampleExecuteWorkflowRequest() any {
	return executeWorkflowRequest{
		Workflow: &types.WorkflowDef{
			Namespace: "default",
			Name:      "health-check",
			Version:   "v1",
			Nodes:     []types.NodeDef{{ID: "n1", Name: "Start", Type: "http.request", Kind: types.NodeKindAction, Version: 1}},
		},
		Entry:  "Start",
		Input:  map[string]any{"ok": true},
		Params: map[string]any{"k": "v"},
	}
}

// ExampleLeaderResponse builds a leaderResponse (GET /v1/management/leader).
func ExampleLeaderResponse(isLeader bool) any {
	return leaderResponse{IsLeader: isLeader}
}

// ExampleReadyResponse builds a readyResponse (GET /readyz).
func ExampleReadyResponse(ready, leader bool) any {
	return readyResponse{Ready: ready, Leader: leader}
}

// ExampleSupplyPutResponse builds the supply PUT success data (PUT
// /v1/supplies/{name}).
func ExampleSupplyPutResponse(revision uint64, contentHash string) any {
	return map[string]any{"revision": revision, "content_hash": contentHash}
}

// ExampleWaitTimeoutResponse builds a waitTimeoutResponse (GET
// /v1/executions/{id}/wait, 202 branch).
func ExampleWaitTimeoutResponse(id types.ExecutionID, status types.ExecutionStatus) any {
	return waitTimeoutResponse{ExecutionID: id, Status: status, TimedOut: true}
}

// ExampleDeadLetterListResponse builds a deadLetterListResponse (GET
// /v1/management/dead-letters/{execID}).
func ExampleDeadLetterListResponse(entries []engine.OutboxEntry, nextCursor string) any {
	return deadLetterListResponse{Entries: entries, NextCursor: nextCursor}
}

// ExampleDeadLetterReplayRequest builds a deadLetterReplayRequest (POST
// /v1/management/dead-letters/{execID}/replay).
func ExampleDeadLetterReplayRequest(entryID, requestID, reason string) any {
	return deadLetterReplayRequest{EntryID: entryID, RequestID: requestID, Reason: reason}
}

// ExampleDeadLetterReplayResponse builds a deadLetterReplayResponse (POST
// /v1/management/dead-letters/{execID}/replay, success branch).
func ExampleDeadLetterReplayResponse(outcome, auditID, executionID, nodeID, activationID string) any {
	return deadLetterReplayResponse{
		Outcome:      outcome,
		AuditID:      auditID,
		ExecutionID:  executionID,
		NodeID:       nodeID,
		ActivationID: activationID,
	}
}
