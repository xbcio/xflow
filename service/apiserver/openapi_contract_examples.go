package apiserver

import (
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/control"
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

// ExampleErrorEnvelope builds a real failure envelope (writeFail's shape) so
// the contract's ErrorEnvelope schema is validated against the actual failure
// wire shape. Data is left as the zero `any`; with `omitempty` on envelope.Data
// the marshaled body omits the `data` key entirely. ErrorEnvelope sets
// `additionalProperties: false`, so a future loss of `omitempty` (which would
// re-emit `data: null`) turns the contract test red — the success-only
// ExampleEnvelope could not catch that regression.
func ExampleErrorEnvelope() any {
	return envelope{
		Success: false,
		Code:    "workflow_not_found",
		Message: "workflow not found",
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

// ExampleRegistrationCodeCreateRequest builds a registrationCodeCreateRequest
// (POST /v1/management/registration-codes). expiresInSeconds is a pointer in
// the real type — absent and 0 are different requests — so the contract test
// can exercise both the "asked for a lifetime" and the "took the deployment
// default" shapes against the same schema.
func ExampleRegistrationCodeCreateRequest(namespaces, nodeTypes []string, expiresInSeconds *int64) any {
	return registrationCodeCreateRequest{
		AllowedNamespaces: namespaces,
		AllowedNodeTypes:  nodeTypes,
		ExpiresInSeconds:  expiresInSeconds,
	}
}

// ExampleRegistrationCodeView builds a registrationCodeView (GET
// /v1/management/registration-codes). Carries neither the plaintext nor the
// hash, which is the property the schema exists to pin down.
//
// It takes a domain RegistrationCode and runs it through the handler's own
// newRegistrationCodeView rather than filling the view's fields directly.
// That is the whole point: the projection is where nil slices become `[]` and
// where a zero ExpiresAt becomes an absent key, so an example that bypassed it
// would validate a shape no handler ever writes — and the nil-slice case,
// which is the one that broke the contract, would be untestable from here.
func ExampleRegistrationCodeView(id string, createdAt, expiresAt time.Time, namespaces, nodeTypes []string, revoked bool) any {
	return newRegistrationCodeView(control.RegistrationCode{
		ID:                id,
		AllowedNamespaces: namespaces,
		AllowedNodeTypes:  nodeTypes,
		Revoked:           revoked,
		CreatedAt:         createdAt,
		ExpiresAt:         expiresAt,
	})
}
