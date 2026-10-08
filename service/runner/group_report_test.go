package runner

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
)

// A group lease whose GroupRuntime.Execute fails outright (the package never
// ran at all — see group_runtime.go's Execute/ExecuteRequest) must still be
// reported with a non-nil GroupResult{Outcome: Failed}. Before the runner.go
// fix, this branch reported GroupResult == nil, which the control plane
// cannot commit through CommitGroupResult (see core.go's isGroupTask fork) —
// the group's entry node never leased under its own per-node identity, so the
// report was rejected instead of finalizing the group through its own
// OnError policy.
func TestRunnerReportsGroupExecuteErrorAsFailedGroupResult(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	registry := execution.NewRegistry()
	cache := NewPackageCache(PackageCacheConfig{MaxEntries: 4})

	lease := &engine.TaskLease{
		LeaseID:    "lease-1",
		LeaseToken: "token-1",
		Attempt:    1,
		Task:       engine.Task{ExecutionID: "exec-1", NodeName: "g", Type: engine.TaskTypeGroupExec},
		NodeType:   engine.GroupNodeType,
		GroupPayload: &engine.GroupLeasePayload{
			ProtocolVersion: 1,
			GroupExecID:     "gexec-1",
			PackageHash:     "hash-never-cached",
			// Package left nil: PackageCache.Resolve returns ErrPackageMissing
			// before any package ever runs — GroupRuntime.Execute surfaces this
			// as a non-nil error, not a (GroupResult{Outcome: Failed}, nil)
			// return, since the executor never produced a result at all.
			Package:  nil,
			Deadline: time.Now().Add(time.Minute),
		},
	}

	client := &fakeProtocolClient{lease: lease, cancel: cancel}
	r := New(client, registry, Config{
		RunnerID:          "runner-1",
		Concurrency:       1,
		Capabilities:      []protocol.Capability{{NodeType: engine.GroupNodeType}},
		HeartbeatInterval: time.Hour,
		PollWait:          time.Millisecond,
		GroupRuntime:      NewGroupRuntime(registry, cache, WithSuspendDisabled()),
	})

	runDone := make(chan error, 1)
	go func() { runDone <- r.Run(ctx) }()

	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not report and stop")
	}

	got := client.reported.GroupResult
	if got == nil {
		t.Fatal("reported GroupResult is nil, want a synthesized failure")
	}
	if got.Outcome != engine.GroupOutcomeFailed {
		t.Fatalf("Outcome = %q, want %q", got.Outcome, engine.GroupOutcomeFailed)
	}
	if got.Error == "" {
		t.Fatal("Error is empty, want the executor's failure message")
	}
	if got.ProtocolVersion != lease.GroupPayload.ProtocolVersion {
		t.Fatalf("ProtocolVersion = %d, want %d", got.ProtocolVersion, lease.GroupPayload.ProtocolVersion)
	}
	if got.GroupExecID != lease.GroupPayload.GroupExecID {
		t.Fatalf("GroupExecID = %q, want %q", got.GroupExecID, lease.GroupPayload.GroupExecID)
	}
	if got.Attempt != lease.Attempt {
		t.Fatalf("Attempt = %d, want %d", got.Attempt, lease.Attempt)
	}
}

// A group task's oversize report must still carry a GroupResult, not fall
// back to req.Result: the control plane only commits through
// CommitGroupResult when GroupResult is non-nil for a group task (core.go's
// isGroupTask fork), so nil-ing it here would reproduce the same misrouted
// commit the sibling fix above closes, once per oversize group report.
func TestOversizeReportSynthesizesFailedGroupResult(t *testing.T) {
	lease := engine.TaskLease{
		LeaseID: "lease-1",
		Attempt: 2,
		Task:    engine.Task{ExecutionID: "exec-1", NodeName: "g", Type: engine.TaskTypeGroupExec},
	}
	req := protocol.ReportResultRequest{
		Lease: &lease,
		GroupResult: &engine.GroupResult{
			ProtocolVersion: 1,
			GroupExecID:     "gexec-1",
			Outcome:         engine.GroupOutcomeSuccess,
		},
	}

	got := oversizeReport(req, errors.New("too large"))

	if got.GroupResult == nil {
		t.Fatal("GroupResult is nil, want a synthesized failure")
	}
	if got.GroupResult.Outcome != engine.GroupOutcomeFailed {
		t.Fatalf("Outcome = %q, want %q", got.GroupResult.Outcome, engine.GroupOutcomeFailed)
	}
	if got.GroupResult.Error == "" {
		t.Fatal("Error is empty, want the oversize cause")
	}
	if got.GroupResult.ProtocolVersion != 1 || got.GroupResult.GroupExecID != "gexec-1" {
		t.Fatalf("identity = %+v, want ProtocolVersion=1 GroupExecID=gexec-1", got.GroupResult)
	}
	if got.GroupResult.Attempt != lease.Attempt {
		t.Fatalf("Attempt = %d, want %d", got.GroupResult.Attempt, lease.Attempt)
	}
}
