package control

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

func TestHTTPPollFailsLeaseAboveRunnerBodyLimit(t *testing.T) {
	ctx := context.Background()
	eng := &fakeControlEngine{}
	runners := NewMemoryRunnerDirectory()
	logger := &recordingLogger{}
	ts := httptest.NewServer(NewServer(eng, runners, WithControlLogger(logger)).Handler())
	t.Cleanup(ts.Close)
	client := protocol.NewClient(ts.URL, ts.Client())

	reg, err := client.Register(ctx, protocol.RegisterRunnerRequest{
		InstanceUID:  "test-instance",
		RunnerID:     "runner-1",
		Concurrency:  1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	lease := engine.TaskLease{
		LeaseID:    "lease-big",
		LeaseToken: "token-big",
		Attempt:    1,
		Task:       engine.Task{ExecutionID: "exec-big", NodeName: "big", NodeIdx: 0},
		Input:      &types.Input{Data: map[string]any{"blob": strings.Repeat("x", MaxRegisterRunnerBodyBytes+1)}},
		NodeType:   "xflow.function",
	}
	eng.buildLease = &lease
	if _, err := runners.EnqueueAssignment(ctx, Assignment{
		AssignmentID: BuildAssignmentID(&lease.Task),
		Task:         lease.Task,
		Routing:      engine.TaskRouting{NodeType: lease.NodeType},
	}); err != nil {
		t.Fatalf("EnqueueAssignment() error = %v", err)
	}

	got, err := client.Poll(ctx, protocol.PollTaskRequest{RunnerID: "runner-1", SessionID: reg.SessionID, Capacity: 1})
	if err != nil {
		t.Fatalf("Poll() error = %v, want a no-task answer that keeps the session alive", err)
	}
	if got.Lease != nil {
		t.Fatalf("Poll() delivered lease %q above the body limit", got.Lease.LeaseID)
	}
	if got.Wait <= 0 {
		t.Fatalf("Poll() wait = %v, want a positive wait hint", got.Wait)
	}

	if eng.committedLease == nil || eng.committedLease.LeaseID != "lease-big" {
		t.Fatalf("committed lease = %+v, want lease-big failed", eng.committedLease)
	}
	var classified *types.ClassifiedError
	if !errors.As(eng.committedResult.Error, &classified) || !types.IsPermanent(classified) || classified.Code != LeaseTooLargeErrorCode {
		t.Fatalf("committed error = %v, want permanent %s", eng.committedResult.Error, LeaseTooLargeErrorCode)
	}

	entries := logger.withMsg("task lease exceeds runner HTTP response limit; failing task")
	if len(entries) != 1 {
		t.Fatalf("oversize log entries = %d, want 1 (all: %+v)", len(entries), logger.entries)
	}
	for key, want := range map[string]any{"exec": "exec-big", "node": "big", "lease": "lease-big", "attempt": 1, "limit": MaxRegisterRunnerBodyBytes} {
		if got := entries[0].field(key); got != want {
			t.Errorf("log field %q = %v, want %v", key, got, want)
		}
	}

	// A released lease must not be replayed to a recovery-only poll.
	replay, err := client.Poll(ctx, protocol.PollTaskRequest{RunnerID: "runner-1", SessionID: reg.SessionID, Capacity: 1, RecoveryOnly: true})
	if err != nil {
		t.Fatalf("recovery Poll() error = %v", err)
	}
	if replay.Lease != nil {
		t.Fatalf("recovery Poll() replayed failed lease %q", replay.Lease.LeaseID)
	}
}
