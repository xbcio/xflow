package protocol

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
)

func TestHTTPStreamSimulatesConnect(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/runners/register", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"runner_id": "r1"})
	})
	lease := engine.TaskLease{LeaseID: "L1", NodeType: "xflow.function"}
	delivered := false
	mux.HandleFunc("/v1/runners/poll", func(w http.ResponseWriter, r *http.Request) {
		if delivered {
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"wait_nanos": int64(time.Second)})
			return
		}
		delivered = true
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(PollTaskResponse{Lease: &lease})
	})
	mux.HandleFunc("/v1/runners/result", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ReportResultResponse{Accepted: true})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewClient(srv.URL, srv.Client())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer stream.Close()

	if err := stream.Send(RunnerFrame{Hello: &HelloFrame{RunnerID: "r1", Concurrency: 1, Capabilities: []Capability{{NodeType: "xflow.function"}}}}); err != nil {
		t.Fatalf("send hello: %v", err)
	}
	if fr, err := stream.Recv(); err != nil || fr.Welcome == nil || fr.Welcome.RunnerID != "r1" {
		t.Fatalf("expected WELCOME, got fr=%+v err=%v", fr, err)
	}
	if fr, err := stream.Recv(); err != nil || fr.Task == nil || fr.Task.Lease == nil || fr.Task.Lease.LeaseID != "L1" {
		t.Fatalf("expected TASK L1, got fr=%+v err=%v", fr, err)
	}
	if err := stream.Send(RunnerFrame{Result: &ResultFrame{LeaseID: "L1", Lease: &lease, Result: engine.TaskResult{}}}); err != nil {
		t.Fatalf("send result: %v", err)
	}
	if fr, err := stream.Recv(); err != nil || fr.Ack == nil || !fr.Ack.Accepted {
		t.Fatalf("expected ACK accepted, got fr=%+v err=%v", fr, err)
	}
}
