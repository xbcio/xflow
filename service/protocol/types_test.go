package protocol

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

func TestRunnerProtocolSessionIDRoundTripsJSON(t *testing.T) {
	register := RegisterRunnerResponse{RunnerID: "runner-1", SessionID: "session-1"}
	registerJSON, err := json.Marshal(register)
	if err != nil {
		t.Fatalf("marshal register response: %v", err)
	}
	if !strings.Contains(string(registerJSON), `"session_id":"session-1"`) {
		t.Fatalf("register response JSON = %s, want session_id", registerJSON)
	}

	var heartbeat HeartbeatRequest
	if err := json.Unmarshal([]byte(`{"runner_id":"runner-1","session_id":"session-1","capacity":2,"in_flight":1}`), &heartbeat); err != nil {
		t.Fatalf("unmarshal heartbeat: %v", err)
	}
	if heartbeat.SessionID != "session-1" {
		t.Fatalf("heartbeat SessionID = %q, want session-1", heartbeat.SessionID)
	}

	var poll PollTaskRequest
	if err := json.Unmarshal([]byte(`{"runner_id":"runner-1","session_id":"session-1","capacity":2}`), &poll); err != nil {
		t.Fatalf("unmarshal poll: %v", err)
	}
	if poll.SessionID != "session-1" {
		t.Fatalf("poll SessionID = %q, want session-1", poll.SessionID)
	}

	var report ReportResultRequest
	if err := json.Unmarshal([]byte(`{"runner_id":"runner-1","session_id":"session-1","lease":{"task":{"execution_id":"e1","node_name":"n1","node_idx":0,"type":0},"node_type":"xflow.function","issued_at":"2026-07-02T00:00:00Z"},"result":{}}`), &report); err != nil {
		t.Fatalf("unmarshal report: %v", err)
	}
	if report.SessionID != "session-1" {
		t.Fatalf("report SessionID = %q, want session-1", report.SessionID)
	}
}

func TestPollTaskResponseRoundTripsLeaseJSON(t *testing.T) {
	want := PollTaskResponse{
		Lease: &engine.TaskLease{
			LeaseID:     engine.LeaseID("lease-1"),
			LeaseToken:  engine.LeaseToken("token-1"),
			Attempt:     2,
			Task:        engine.Task{ExecutionID: types.ExecutionID("exec-1"), NodeName: "start", NodeIdx: 3, Type: engine.TaskTypeNodeExec},
			Input:       &types.Input{Data: map[string]any{"claim_id": "c-1"}},
			NodeType:    "xflow.function",
			NodeVersion: 1,
		},
		Wait: 250 * time.Millisecond,
	}

	data, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}

	var got PollTaskResponse
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}

	if got.Wait != want.Wait {
		t.Fatalf("wait = %s, want %s", got.Wait, want.Wait)
	}
	if got.Lease == nil {
		t.Fatal("lease is nil")
	}
	if got.Lease.LeaseID != want.Lease.LeaseID || got.Lease.LeaseToken != want.Lease.LeaseToken {
		t.Fatalf("lease fencing = (%q, %q), want (%q, %q)", got.Lease.LeaseID, got.Lease.LeaseToken, want.Lease.LeaseID, want.Lease.LeaseToken)
	}
	if got.Lease.Task.ExecutionID != want.Lease.Task.ExecutionID || got.Lease.Task.NodeName != want.Lease.Task.NodeName {
		t.Fatalf("task = %+v, want %+v", got.Lease.Task, want.Lease.Task)
	}
	if got.Lease.NodeType != want.Lease.NodeType || got.Lease.NodeVersion != want.Lease.NodeVersion {
		t.Fatalf("routing = (%q, %d), want (%q, %d)", got.Lease.NodeType, got.Lease.NodeVersion, want.Lease.NodeType, want.Lease.NodeVersion)
	}
	if got.Lease.Input.Data["claim_id"] != "c-1" {
		t.Fatalf("input data = %v, want claim_id c-1", got.Lease.Input.Data)
	}
}

func TestReportResultRequestRoundTripsSuccessfulTaskResultJSON(t *testing.T) {
	want := ReportResultRequest{
		RunnerID: "runner-1",
		Lease: &engine.TaskLease{
			LeaseID:     engine.LeaseID("lease-1"),
			LeaseToken:  engine.LeaseToken("token-1"),
			Task:        engine.Task{ExecutionID: types.ExecutionID("exec-1"), NodeName: "start"},
			NodeType:    "xflow.function",
			NodeVersion: 1,
		},
		Result: engine.TaskResult{
			Output: &types.Output{Data: map[string]any{"ok": true}, Port: "main"},
		},
	}

	data, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}

	var got ReportResultRequest
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}

	if got.RunnerID != want.RunnerID {
		t.Fatalf("runner id = %q, want %q", got.RunnerID, want.RunnerID)
	}
	if got.Lease == nil || got.Lease.LeaseID != want.Lease.LeaseID {
		t.Fatalf("lease = %+v, want %+v", got.Lease, want.Lease)
	}
	if got.Result.Output == nil {
		t.Fatal("result output is nil")
	}
	if got.Result.Output.Data["ok"] != true || got.Result.Output.Port != "main" {
		t.Fatalf("result output = %+v, want ok=true on main", got.Result.Output)
	}
}

func TestReportResultRequestRoundTripsTaskResultErrorJSON(t *testing.T) {
	want := ReportResultRequest{
		RunnerID: "runner-1",
		Lease: &engine.TaskLease{
			LeaseID:  engine.LeaseID("lease-1"),
			Task:     engine.Task{ExecutionID: types.ExecutionID("exec-1"), NodeName: "start"},
			NodeType: "xflow.function",
		},
		Result: engine.TaskResult{Error: errors.New("handler failed")},
	}

	data, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}

	var got ReportResultRequest
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}

	if got.Result.Error == nil {
		t.Fatalf("result error was not preserved in %s", data)
	}
	if got.Result.Error.Error() != "handler failed" {
		t.Fatalf("result error = %q, want handler failed", got.Result.Error.Error())
	}
}

func TestRunnerLabelRequestsRoundTripGRPCConversion(t *testing.T) {
	register := RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Concurrency:  2,
		Labels:       map[string]string{"mode": "remote", "env": "prod"},
		Capabilities: []Capability{{NodeType: "xflow.function"}},
	}
	gotRegister := RegisterRequestFromProto(RegisterRequestToProto(register))
	if gotRegister.Labels["mode"] != "remote" || gotRegister.Labels["env"] != "prod" {
		t.Fatalf("register labels = %+v, want mode/env", gotRegister.Labels)
	}

	poll := PollTaskRequest{
		RunnerID:     "runner-1",
		Capacity:     1,
		Labels:       map[string]string{"mode": "local"},
		Capabilities: []Capability{{NodeType: "xflow.function"}},
	}
	gotPoll := PollTaskRequestFromProto(PollTaskRequestToProto(poll))
	if got := gotPoll.Labels["mode"]; got != "local" {
		t.Fatalf("poll label mode = %q, want local", got)
	}
}

// --- Task 19: rolling-upgrade compatibility for the supply hint/observed fields ---

// A HeartbeatRequest with no SupplyObserved set must marshal to EXACTLY the
// same bytes as before this task's fields existed — this is what
// "byte-identical for runners with no supplies" means concretely, and it is
// what makes a rolling upgrade safe: a NEW server parsing an OLD runner's
// heartbeat body sees a request indistinguishable from one sent by another
// new runner that simply has no supplies.
func TestHeartbeatRequestOmitsSupplyObservedWhenNil(t *testing.T) {
	req := HeartbeatRequest{RunnerID: "runner-1", SessionID: "sess-1", Capacity: 2, InFlight: 1, Timestamp: 100}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(data), "supply_observed") {
		t.Fatalf("heartbeat request JSON = %s, must not contain supply_observed when nil", data)
	}
}

// Symmetric assertion for the response side: HeartbeatResponse with no
// SupplyHints must not carry the field either.
func TestHeartbeatResponseOmitsSupplyHintsWhenNil(t *testing.T) {
	resp := HeartbeatResponse{ServerTime: 100}
	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(data), "supply_hints") {
		t.Fatalf("heartbeat response JSON = %s, must not contain supply_hints when nil", data)
	}
}

// An OLD runner's heartbeat body (no supply_observed key at all — literally
// what a pre-Task-19 binary sends) must unmarshal cleanly on a NEW server:
// SupplyObserved decodes to nil, not an error and not a zero-length
// non-nil map that would behave differently downstream.
func TestOldRunnerHeartbeatRequestDecodesOnNewServer(t *testing.T) {
	var req HeartbeatRequest
	oldPeerBody := `{"runner_id":"runner-1","session_id":"sess-1","capacity":2,"in_flight":1,"timestamp":100}`
	if err := json.Unmarshal([]byte(oldPeerBody), &req); err != nil {
		t.Fatalf("unmarshal old-peer heartbeat request: %v", err)
	}
	if req.SupplyObserved != nil {
		t.Fatalf("SupplyObserved = %#v, want nil when the peer never sent the field", req.SupplyObserved)
	}
	if req.RunnerID != "runner-1" || req.SessionID != "sess-1" {
		t.Fatalf("other fields lost in decode: %+v", req)
	}
}

// Symmetric: an OLD server's heartbeat response body (no supply_hints,
// literally what a pre-Task-19 binary sends) must unmarshal cleanly on a NEW
// runner: SupplyHints decodes to nil.
func TestOldServerHeartbeatResponseDecodesOnNewRunner(t *testing.T) {
	var resp HeartbeatResponse
	oldPeerBody := `{"server_time":100}`
	if err := json.Unmarshal([]byte(oldPeerBody), &resp); err != nil {
		t.Fatalf("unmarshal old-peer heartbeat response: %v", err)
	}
	if resp.SupplyHints != nil {
		t.Fatalf("SupplyHints = %#v, want nil when the peer never sent the field", resp.SupplyHints)
	}
	if resp.ServerTime != 100 {
		t.Fatalf("ServerTime = %d, want 100", resp.ServerTime)
	}
}

// A NEW runner's heartbeat body, when it DOES have supplies, must round-trip
// SupplyObserved through JSON so a new server can read it back intact.
func TestHeartbeatRequestRoundTripsSupplyObserved(t *testing.T) {
	req := HeartbeatRequest{
		RunnerID: "runner-1", SessionID: "sess-1", Capacity: 1,
		SupplyObserved: map[string]string{"rules": "sha256:abc"},
	}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got HeartbeatRequest
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.SupplyObserved["rules"] != "sha256:abc" {
		t.Fatalf("SupplyObserved = %#v, want {rules: sha256:abc}", got.SupplyObserved)
	}
}

// Symmetric round-trip for SupplyHints on the response side.
func TestHeartbeatResponseRoundTripsSupplyHints(t *testing.T) {
	resp := HeartbeatResponse{ServerTime: 100, SupplyHints: map[string]string{"rules": "sha256:def"}}
	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got HeartbeatResponse
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.SupplyHints["rules"] != "sha256:def" {
		t.Fatalf("SupplyHints = %#v, want {rules: sha256:def}", got.SupplyHints)
	}
}
