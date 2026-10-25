package runner

import (
	"context"
	"encoding/json"
	"math"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// infHandler returns output encoding/json cannot carry, the way an
// xflow.function `1 / 0` evaluates to +Inf rather than an error.
type infHandler struct{ runs *atomic.Int32 }

func (infHandler) Descriptor() types.Descriptor { return types.Descriptor{Type: "test.runner.inf"} }

func (h infHandler) Execute(context.Context, *types.Input) (*types.Output, error) {
	h.runs.Add(1)
	return &types.Output{Data: map[string]any{"result": math.Inf(1)}}, nil
}

// A result the client cannot encode used to fail ReportResult locally on every
// attempt: the lease stayed held server-side, the capacity-1 runner claimed
// nothing else, and the lease replayed until its attempts ran out. It must land
// once as a permanent failure instead.
func TestRunnerReportsUnencodableResultAsPermanentFailure(t *testing.T) {
	lease := engine.TaskLease{
		LeaseID:  "lease-inf",
		Attempt:  1,
		Task:     engine.Task{ExecutionID: "exec-inf", NodeName: "boom"},
		NodeType: "test.runner.inf",
	}
	pollBody, err := json.Marshal(protocol.PollTaskResponse{Lease: &lease})
	if err != nil {
		t.Fatalf("encode poll response: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	srv := &replayingReportServer{pollBody: pollBody, reportCap: protocol.MaxRunnerRequestBodyBytes, cancel: cancel}
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	var runs atomic.Int32
	registry := execution.NewRegistry()
	registry.RegisterGlobal("test.runner.inf", infHandler{runs: &runs})
	r := New(protocol.NewClient(ts.URL, ts.Client()), registry, Config{
		RunnerID:          "runner-1",
		Concurrency:       1,
		Capabilities:      []protocol.Capability{{NodeType: "test.runner.inf"}},
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
	if !types.IsPermanent(got.Result.Error) || !strings.Contains(got.Result.Error.Error(), "Inf") {
		t.Fatalf("reported error = %v, want permanent failure naming the unencodable value", got.Result.Error)
	}
	if got.Result.Output != nil {
		t.Fatal("reported output must be dropped")
	}
	if r.inFlight.Load() != 0 || len(r.active.snapshot()) != 0 {
		t.Fatalf("slot not released: in_flight=%d active=%v", r.inFlight.Load(), r.active.snapshot())
	}
}
