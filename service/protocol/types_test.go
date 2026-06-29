package protocol

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

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
