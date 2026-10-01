package runner

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// countingEchoHandler echoes its input as output and counts executions, so a
// test can tell one run from a replay loop.
type countingEchoHandler struct{ runs *atomic.Int32 }

func (countingEchoHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.runner.echo_count"}
}

func (h countingEchoHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	h.runs.Add(1)
	return &types.Output{Data: input.Data}, nil
}

// replayingReportServer is an HTTP control plane that, like the Redis runner
// directory, hands an unreported lease out again on every poll until a report
// for it is accepted. Report bodies are capped at reportCap with a 413, the
// way control.decodeJSON caps them.
type replayingReportServer struct {
	pollBody  []byte
	reportCap int64
	cancel    context.CancelFunc

	mu       sync.Mutex
	accepted *protocol.ReportResultRequest
	reports  int
}

func (s *replayingReportServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case protocol.RegisterRunnerPath:
		_, _ = w.Write([]byte(`{"runner_id":"runner-1","session_id":"session-1"}`))
	case protocol.PollTaskPath:
		s.mu.Lock()
		done := s.accepted != nil
		s.mu.Unlock()
		if done {
			_, _ = w.Write([]byte(`{"wait":1000000}`))
			return
		}
		_, _ = w.Write(s.pollBody)
	case protocol.ReportResultPath:
		var req protocol.ReportResultRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, s.reportCap)).Decode(&req); err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				_, _ = w.Write([]byte(`{"error":"runner request too large"}`))
				return
			}
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.reports++
		s.accepted = &req
		s.mu.Unlock()
		_, _ = w.Write([]byte(`{"accepted":true}`))
		s.cancel()
	default:
		_, _ = w.Write([]byte(`{}`))
	}
}

// pollBodyWithLeaseOfSize encodes a poll response carrying lease, padded
// through its input so the body is exactly size bytes.
func pollBodyWithLeaseOfSize(t *testing.T, lease engine.TaskLease, size int) []byte {
	t.Helper()
	encode := func(pad int) []byte {
		l := lease
		l.Input = &types.Input{Data: map[string]any{"blob": strings.Repeat("x", pad)}}
		b, err := json.Marshal(protocol.PollTaskResponse{Lease: &l})
		if err != nil {
			t.Fatalf("encode poll response: %v", err)
		}
		return b
	}
	pad := size - len(encode(0))
	if pad < 0 {
		t.Fatalf("size %d too small for the lease", size)
	}
	body := encode(pad)
	if len(body) != size {
		t.Fatalf("poll fixture encodes to %d bytes, want %d", len(body), size)
	}
	return body
}

// A lease the poll path may legally deliver can produce a report the server
// cannot accept. That report must land as one permanent failure, not leave
// the lease to be replayed and re-executed on every poll.
func TestRunnerReportsOversizeResultAsPermanentFailure(t *testing.T) {
	lease := engine.TaskLease{
		LeaseID:  "lease-big",
		Attempt:  1,
		Task:     engine.Task{ExecutionID: "exec-big", NodeName: "big", Payload: &types.SignalPayload{Name: "go", Data: map[string]any{"k": "v"}}},
		NodeType: "test.runner.echo_count",
	}
	tests := []struct {
		name      string
		pollSize  int
		reportCap int64
	}{
		// The echoed lease plus its echoed output is about twice the cap, so
		// the client refuses the body before sending it.
		{"lease just under the protocol cap", protocol.MaxRunnerResponseBodyBytes - 1024, protocol.MaxRunnerRequestBodyBytes},
		// A server with a smaller cap answers 413 instead.
		{"server answers 413", 128 << 10, 64 << 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			srv := &replayingReportServer{
				pollBody:  pollBodyWithLeaseOfSize(t, lease, tt.pollSize),
				reportCap: tt.reportCap,
				cancel:    cancel,
			}
			ts := httptest.NewServer(srv)
			t.Cleanup(ts.Close)

			var runs atomic.Int32
			registry := execution.NewRegistry()
			registry.RegisterGlobal("test.runner.echo_count", countingEchoHandler{runs: &runs})
			r := New(protocol.NewClient(ts.URL, ts.Client()), registry, Config{
				RunnerID:          "runner-1",
				Concurrency:       1,
				Capabilities:      []protocol.Capability{{NodeType: "test.runner.echo_count"}},
				HeartbeatInterval: time.Hour,
				PollWait:          time.Millisecond,
			})
			if err := r.Run(ctx); err != nil {
				t.Fatalf("Run() error = %v", err)
			}

			srv.mu.Lock()
			defer srv.mu.Unlock()
			if srv.accepted == nil {
				t.Fatalf("no report accepted after %d handler runs, want one permanent failure", runs.Load())
			}
			if got := runs.Load(); got != 1 {
				t.Fatalf("handler ran %d times, want 1", got)
			}
			got := srv.accepted
			if !types.IsPermanent(got.Result.Error) {
				t.Fatalf("reported error = %v, want permanent", got.Result.Error)
			}
			if got.Result.Output != nil {
				t.Fatalf("reported output = %d keys, want none", len(got.Result.Output.Data))
			}
			echo := got.Lease
			if echo == nil || echo.LeaseID != lease.LeaseID || echo.Attempt != lease.Attempt ||
				echo.Task.ExecutionID != lease.Task.ExecutionID || echo.Task.NodeName != lease.Task.NodeName {
				t.Fatalf("echoed lease = %+v, want identity of %+v", echo, lease)
			}
			if echo.Input != nil {
				t.Fatal("echoed lease carries input, want it dropped")
			}
			if p := echo.Task.Payload; p == nil || p.Name != "go" || p.Data != nil {
				t.Fatalf("echoed task payload = %+v, want name kept and data dropped", p)
			}
		})
	}
}
