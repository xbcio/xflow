package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

// A report for a group-exec lease with no GroupResult must be rejected
// explicitly, releasing the runner's leased capacity immediately (aligned
// with the pre-022bcfc behavior of releasing on an accidental stale-token
// classification) rather than falling through to the node commit path, which
// CommitTaskResultWithOutcome now refuses outright (ErrGroupLeaseNotSupported)
// without releasing capacity — leaving it to the sweeper instead.
func TestReportResultRejectsGroupLeaseWithNilGroupResult(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(time.Minute))
	// registerRedisDirectoryRunner only registers the xflow.function capability,
	// which a group-exec assignment's routing (engine.GroupNodeType) would never
	// claim -- register directly with the matching capability/policy instead.
	session, err := directory.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-group-nil-result",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: engine.GroupNodeType}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{engine.GroupNodeType}},
		Now:          time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	assignment := Assignment{
		AssignmentID: "exec-group-nil-result/group/activation-1",
		Task: engine.Task{
			ExecutionID: "exec-group-nil-result",
			NodeName:    "group",
			Type:        engine.TaskTypeGroupExec,
		},
		Routing:   engine.TaskRouting{NodeType: engine.GroupNodeType},
		Namespace: "default",
	}
	lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-group-nil-result", time.Minute)
	finalizeRedisRunnerDirectoryLeaseMetaTestAssignment(t, ctx, directory, session, assignment, lease)

	before := server.HGet(directory.keys.runnerLeaseCount, session.RunnerID)
	if before != "1" {
		t.Fatalf("runnerLeaseCount before report = %q, want %q", before, "1")
	}

	fake := &fakeControlEngine{}
	core := &Core{
		engine:   fake,
		runners:  directory,
		pollWait: time.Second,
	}

	_, reportErr := core.reportResult(ctx, protocol.ReportResultRequest{
		RunnerID:  session.RunnerID,
		SessionID: session.SessionID,
		Lease:     lease,
		// GroupResult deliberately left nil: this is the exact gap being closed.
	}, TransportInfo{})

	if !errors.Is(reportErr, ErrGroupResultMissing) {
		t.Fatalf("reportResult() error = %v, want ErrGroupResultMissing", reportErr)
	}
	if fake.committedLease != nil {
		t.Fatal("engine was invoked, want the report rejected before reaching it")
	}

	after := server.HGet(directory.keys.runnerLeaseCount, session.RunnerID)
	if after != "0" {
		t.Fatalf("runnerLeaseCount after report = %q, want %q (capacity released immediately)", after, "0")
	}
}
