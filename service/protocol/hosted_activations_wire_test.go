package protocol

import (
	"encoding/json"
	"strings"
	"testing"
)

// The hosted-activation report's presence semantics are the whole point of the
// nested message: "does not report" (nil) and "reports hosting nothing"
// (present, empty) drive opposite reconciliation decisions, so every transport
// must preserve the distinction.

func TestHeartbeatRequestHostedActivationsGRPCRoundTrips(t *testing.T) {
	item := ActivationInventoryItem{
		WorkflowID:      "wf-a",
		WorkflowVersion: "v1",
		EntryUnitID:     "tg",
		ReplicaIndex:    1,
		Generation:      1<<53 + 7,
	}
	request := HeartbeatRequest{RunnerID: "runner-a", SessionID: "session-a", HostedActivations: &HostedActivationsReport{Activations: []ActivationInventoryItem{item}}}

	got := HeartbeatRequestFromProto(HeartbeatRequestToProto(request))
	if got.HostedActivations == nil || len(got.HostedActivations.Activations) != 1 {
		t.Fatalf("hosted activations round trip = %+v, want the reported item", got.HostedActivations)
	}
	if got.HostedActivations.Activations[0] != item {
		t.Fatalf("hosted activations item = %+v, want %+v (exact generation)", got.HostedActivations.Activations[0], item)
	}

	// nil stays nil: an old runner must not be read as "hosts nothing".
	if got := HeartbeatRequestFromProto(HeartbeatRequestToProto(HeartbeatRequest{RunnerID: "runner-a"})); got.HostedActivations != nil {
		t.Fatalf("nil heartbeat hosted activations round trip = %+v, want nil", got.HostedActivations)
	}

	// Present-but-empty stays present: "hosts nothing" is a real report.
	empty := HeartbeatRequestFromProto(HeartbeatRequestToProto(HeartbeatRequest{RunnerID: "runner-a", HostedActivations: &HostedActivationsReport{}}))
	if empty.HostedActivations == nil {
		t.Fatal("empty heartbeat hosted activations lost its presence in the round trip")
	}
	if len(empty.HostedActivations.Activations) != 0 {
		t.Fatalf("empty heartbeat hosted activations = %+v, want no items", empty.HostedActivations.Activations)
	}
}

func TestHeartbeatRequestHostedActivationsJSONPresence(t *testing.T) {
	// nil is omitted entirely, keeping the body byte-identical for runners
	// that never report.
	req := HeartbeatRequest{RunnerID: "runner-1", SessionID: "sess-1", Capacity: 1}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(data), "hosted_activations") {
		t.Fatalf("heartbeat request JSON = %s, must not contain hosted_activations when nil", data)
	}

	// A present-but-empty report survives JSON as present.
	emptyData, err := json.Marshal(HeartbeatRequest{RunnerID: "runner-1", HostedActivations: &HostedActivationsReport{}})
	if err != nil {
		t.Fatalf("marshal empty report: %v", err)
	}
	var empty HeartbeatRequest
	if err := json.Unmarshal(emptyData, &empty); err != nil {
		t.Fatalf("unmarshal empty report: %v", err)
	}
	if empty.HostedActivations == nil {
		t.Fatalf("empty report JSON %s decoded to nil, losing the hosts-nothing signal", emptyData)
	}

	// A populated report round-trips intact.
	item := ActivationInventoryItem{WorkflowID: "wf-a", WorkflowVersion: "v1", EntryUnitID: "tg", Generation: 9}
	populatedData, err := json.Marshal(HeartbeatRequest{RunnerID: "runner-1", HostedActivations: &HostedActivationsReport{Activations: []ActivationInventoryItem{item}}})
	if err != nil {
		t.Fatalf("marshal populated report: %v", err)
	}
	var populated HeartbeatRequest
	if err := json.Unmarshal(populatedData, &populated); err != nil {
		t.Fatalf("unmarshal populated report: %v", err)
	}
	if populated.HostedActivations == nil || len(populated.HostedActivations.Activations) != 1 || populated.HostedActivations.Activations[0].Generation != 9 {
		t.Fatalf("populated report = %+v, want the reported item back", populated.HostedActivations)
	}
}

// An old runner's heartbeat body (no hosted_activations key at all) must
// decode on a new server as nil, never as an empty report.
func TestOldRunnerHeartbeatRequestHasNoHostedActivations(t *testing.T) {
	var req HeartbeatRequest
	oldPeerBody := `{"runner_id":"runner-1","session_id":"sess-1","capacity":2,"in_flight":1,"timestamp":100,"drain_observation":{"generation":3,"recovery_only":true,"active_activations":2}}`
	if err := json.Unmarshal([]byte(oldPeerBody), &req); err != nil {
		t.Fatalf("unmarshal old-peer heartbeat request: %v", err)
	}
	if req.HostedActivations != nil {
		t.Fatalf("HostedActivations = %+v, want nil when the peer never sent the field", req.HostedActivations)
	}
	if req.DrainObservation == nil || req.DrainObservation.Generation != 3 {
		t.Fatalf("drain observation = %+v, want the old peer's value preserved", req.DrainObservation)
	}
}
