package protocol

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

func TestClientRegisterHeartbeatPollAndReportResult(t *testing.T) {
	seen := make(map[string]string)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen[r.URL.Path] = r.Method
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case RegisterRunnerPath:
			var req RegisterRunnerRequest
			decodeClientTestJSON(t, r, &req)
			if req.RunnerID != "runner-1" || req.Concurrency != 2 {
				t.Fatalf("register request = %+v", req)
			}
			_ = json.NewEncoder(w).Encode(RegisterRunnerResponse{RunnerID: req.RunnerID})
		case HeartbeatPath:
			var req HeartbeatRequest
			decodeClientTestJSON(t, r, &req)
			if req.RunnerID != "runner-1" || req.Capacity != 2 || req.InFlight != 1 {
				t.Fatalf("heartbeat request = %+v", req)
			}
			_ = json.NewEncoder(w).Encode(HeartbeatResponse{ServerTime: 123})
		case PollTaskPath:
			var req PollTaskRequest
			decodeClientTestJSON(t, r, &req)
			if req.RunnerID != "runner-1" || req.Capacity != 1 {
				t.Fatalf("poll request = %+v", req)
			}
			_ = json.NewEncoder(w).Encode(PollTaskResponse{Lease: &engine.TaskLease{
				LeaseID:  engine.LeaseID("lease-1"),
				Task:     engine.Task{ExecutionID: types.ExecutionID("exec-1"), NodeName: "start"},
				NodeType: "xflow.function",
			}})
		case ReportResultPath:
			var req ReportResultRequest
			decodeClientTestJSON(t, r, &req)
			if req.RunnerID != "runner-1" || req.Lease == nil || req.Lease.LeaseID != "lease-1" {
				t.Fatalf("result request = %+v", req)
			}
			_ = json.NewEncoder(w).Encode(ReportResultResponse{Accepted: true})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, server.Client())
	ctx := context.Background()

	if resp, err := client.Register(ctx, RegisterRunnerRequest{
		RunnerID:    "runner-1",
		Concurrency: 2,
	}); err != nil || resp.RunnerID != "runner-1" {
		t.Fatalf("Register() = %+v, %v", resp, err)
	}
	if _, err := client.Heartbeat(ctx, HeartbeatRequest{RunnerID: "runner-1", Capacity: 2, InFlight: 1}); err != nil {
		t.Fatalf("Heartbeat() error = %v", err)
	}
	pollResp, err := client.Poll(ctx, PollTaskRequest{RunnerID: "runner-1", Capacity: 1})
	if err != nil {
		t.Fatalf("Poll() error = %v", err)
	}
	if pollResp.Lease == nil || pollResp.Lease.LeaseID != "lease-1" {
		t.Fatalf("poll response = %+v", pollResp)
	}
	resultResp, err := client.ReportResult(ctx, ReportResultRequest{
		RunnerID: "runner-1",
		Lease:    pollResp.Lease,
		Result:   engine.TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}},
	})
	if err != nil || !resultResp.Accepted {
		t.Fatalf("ReportResult() = %+v, %v", resultResp, err)
	}

	for _, path := range []string{RegisterRunnerPath, HeartbeatPath, PollTaskPath, ReportResultPath} {
		if seen[path] != http.MethodPost {
			t.Fatalf("%s method = %q, want POST", path, seen[path])
		}
	}
}

func TestClientReturnsResponseBodyForNon2xx(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "stale lease", http.StatusConflict)
	}))
	defer server.Close()

	client := NewClient(server.URL, server.Client())
	_, err := client.ReportResult(context.Background(), ReportResultRequest{
		RunnerID: "runner-1",
		Lease:    &engine.TaskLease{LeaseID: engine.LeaseID("lease-1")},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "409") || !strings.Contains(err.Error(), "stale lease") {
		t.Fatalf("error = %v, want status and response body", err)
	}
}

func decodeClientTestJSON(t *testing.T, r *http.Request, dst any) {
	t.Helper()
	if r.Method != http.MethodPost {
		t.Fatalf("method = %s, want POST", r.Method)
	}
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		t.Fatal(err)
	}
}
