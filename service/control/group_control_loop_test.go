package control

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// The renewal branches are selected by type assertion on c.engine, so a fake
// satisfying them proves nothing about production: if *engine.Engine ever stops
// matching, every renewal silently falls through to "engine does not support",
// the runner stops renewing, and the sweeper hands live work to a second runner.
// Pin both interfaces against the real type.
var (
	_ groupLeaseEngine      = (*engine.Engine)(nil)
	_ nodeLeaseEngine       = (*engine.Engine)(nil)
	_ nodeTimeoutCommitter  = (*engine.Engine)(nil)
)

// groupFakeEngine extends fakeControlEngine with group lease capabilities.
type groupFakeEngine struct {
	fakeControlEngine

	groupBuildLease   *engine.TaskLease
	groupBuildPayload *engine.GroupLeasePayload
	groupBuildErr     error

	groupRecoverLease   *engine.TaskLease
	groupRecoverPayload *engine.GroupLeasePayload
	groupRecoverErr     error

	groupCommitOutcome engine.CommitOutcome
	groupCommitErr     error
	groupCommittedRes  engine.GroupResult

	groupRenewResult bool
	groupRenewErr    error
	groupRenewedLease *engine.TaskLease

	nodeRenewResult  bool
	nodeRenewErr     error
	nodeRenewedLease *engine.TaskLease
	nodeRenewExtend  time.Duration
}

func (g *groupFakeEngine) BuildGroupLease(_ context.Context, t *engine.Task) (*engine.TaskLease, *engine.GroupLeasePayload, error) {
	if g.groupBuildErr != nil {
		return nil, nil, g.groupBuildErr
	}
	if g.groupBuildLease != nil {
		lease := *g.groupBuildLease
		return &lease, g.groupBuildPayload, nil
	}
	return nil, nil, errors.New("no group build lease configured")
}

func (g *groupFakeEngine) RecoverGroupLease(_ context.Context, _ types.ExecutionID, _ int) (*engine.TaskLease, *engine.GroupLeasePayload, error) {
	if g.groupRecoverErr != nil {
		return nil, nil, g.groupRecoverErr
	}
	if g.groupRecoverLease != nil {
		lease := *g.groupRecoverLease
		return &lease, g.groupRecoverPayload, nil
	}
	return nil, nil, errors.New("no group recover lease configured")
}

func (g *groupFakeEngine) CommitGroupResult(_ context.Context, lease *engine.TaskLease, res engine.GroupResult) (engine.CommitOutcome, error) {
	g.groupCommittedRes = res
	if g.groupCommitErr != nil {
		return g.groupCommitOutcome, g.groupCommitErr
	}
	if g.groupCommitOutcome != "" {
		return g.groupCommitOutcome, nil
	}
	return engine.CommitOutcomeAccepted, nil
}

func (g *groupFakeEngine) RenewGroupLease(_ context.Context, lease *engine.TaskLease, extend time.Duration) (bool, error) {
	g.groupRenewedLease = lease
	if g.groupRenewErr != nil {
		return false, g.groupRenewErr
	}
	return g.groupRenewResult, nil
}

func (g *groupFakeEngine) RenewTaskLease(_ context.Context, lease *engine.TaskLease, extend time.Duration) (bool, error) {
	g.nodeRenewedLease = lease
	g.nodeRenewExtend = extend
	if g.nodeRenewErr != nil {
		return false, g.nodeRenewErr
	}
	return g.nodeRenewResult, nil
}

func groupTestAssignment() Assignment {
	task := engine.Task{
		ExecutionID: "exec-grp-1",
		NodeName:    "grp1",
		NodeIdx:     0,
		UnitIdx:     1,
		Type:        engine.TaskTypeGroupExec,
	}
	return Assignment{
		AssignmentID: BuildAssignmentID(&task),
		Task:         task,
		Routing: engine.TaskRouting{
			NodeType: "xflow.group",
			Requirements: []engine.CapabilityRequirement{
				{NodeType: "xflow.group", Feature: engine.FeatureGroupExecV1},
			},
		},
	}
}

// TestGroupPollDispatchesBuildGroupLease verifies that when a group task is
// claimed, pollTask dispatches through BuildGroupLease (not BuildTaskLease)
// and serializes the GroupLeasePayload into TaskLease.GroupPayload.
func TestGroupPollDispatchesBuildGroupLease(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-grp",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.group", Features: []string{"group.exec.v1"}}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	assignment := groupTestAssignment()
	mustEnqueueAssignment(t, ctx, dir, assignment)

	groupLease := &engine.TaskLease{
		LeaseID:    "lease-grp-1",
		LeaseToken: "token-grp-1",
		Task:       assignment.Task,
		Attempt:    1,
		NodeType:   "xflow.group",
	}
	groupPayload := &engine.GroupLeasePayload{
		ProtocolVersion: 1,
		GroupExecID:     "gexec-1",
		GroupID:         "exec-grp-1/grp1/0",
		GroupUnitIdx:    1,
		PackageHash:     "hash-abc",
		IdempotencyKey:  "normal/exec-grp-1/grp1/0",
	}
	fake := &groupFakeEngine{
		groupBuildLease:   groupLease,
		groupBuildPayload: groupPayload,
	}

	core := &Core{engine: fake, runners: dir, pollWait: time.Second}
	resp, err := core.pollTask(ctx, protocol.PollTaskRequest{
		RunnerID:     session.RunnerID,
		SessionID:    session.SessionID,
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.group", Features: []string{"group.exec.v1"}}},
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("pollTask() error = %v", err)
	}
	if resp.Lease == nil {
		t.Fatal("pollTask() returned nil lease")
	}
	if resp.Lease.LeaseToken != groupLease.LeaseToken {
		t.Errorf("lease token = %q, want %q", resp.Lease.LeaseToken, groupLease.LeaseToken)
	}
	if resp.Lease.GroupPayload == nil {
		t.Fatal("lease.GroupPayload is nil, want serialized GroupLeasePayload")
	}
	if resp.Lease.GroupPayload.GroupUnitIdx != 1 {
		t.Errorf("GroupPayload.GroupUnitIdx = %d, want 1", resp.Lease.GroupPayload.GroupUnitIdx)
	}
}

// TestGroupPollRecoversOnAlreadyActive verifies the pre-finalize crash recovery
// path: BuildGroupLease returns ErrGroupLeaseAlreadyActive, Core falls through
// to RecoverGroupLease and returns the recovered lease.
func TestGroupPollRecoversOnAlreadyActive(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-grp",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.group", Features: []string{"group.exec.v1"}}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	assignment := groupTestAssignment()
	mustEnqueueAssignment(t, ctx, dir, assignment)

	recoveredLease := &engine.TaskLease{
		LeaseID:    "lease-grp-recover",
		LeaseToken: "token-grp-recover",
		Task:       assignment.Task,
		Attempt:    2,
		NodeType:   "xflow.group",
	}
	recoveredPayload := &engine.GroupLeasePayload{
		ProtocolVersion: 1,
		GroupExecID:     "gexec-recover",
		GroupID:         "exec-grp-1/grp1/0",
		GroupUnitIdx:    1,
		PackageHash:     "hash-recover",
		IdempotencyKey:  "normal/exec-grp-1/grp1/0",
	}
	fake := &groupFakeEngine{
		groupBuildErr:       engine.ErrGroupLeaseAlreadyActive,
		groupRecoverLease:   recoveredLease,
		groupRecoverPayload: recoveredPayload,
	}

	core := &Core{engine: fake, runners: dir, pollWait: time.Second}
	resp, err := core.pollTask(ctx, protocol.PollTaskRequest{
		RunnerID:     session.RunnerID,
		SessionID:    session.SessionID,
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.group", Features: []string{"group.exec.v1"}}},
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("pollTask() error = %v", err)
	}
	if resp.Lease == nil {
		t.Fatal("pollTask() returned nil lease after recovery")
	}
	if resp.Lease.LeaseToken != recoveredLease.LeaseToken {
		t.Errorf("recovered lease token = %q, want %q", resp.Lease.LeaseToken, recoveredLease.LeaseToken)
	}
	if resp.Lease.GroupPayload == nil {
		t.Fatal("recovered lease.GroupPayload is nil")
	}
}

// TestGroupReportResultCommitsViaGroupPath verifies that reportResult detects a
// group task and commits through CommitGroupResult (not CommitTaskResultWithOutcome).
func TestGroupReportResultCommitsViaGroupPath(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-grp",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.group", Features: []string{"group.exec.v1"}}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	assignment := groupTestAssignment()
	mustEnqueueAssignment(t, ctx, dir, assignment)
	claim := mustClaimGroupAssignment(t, ctx, dir, session)

	lease := &engine.TaskLease{
		LeaseID:    "lease-grp-report",
		LeaseToken: "token-grp-report",
		Task:       assignment.Task,
		Attempt:    1,
		NodeType:   "xflow.group",
	}
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	fake := &groupFakeEngine{
		groupCommitOutcome: engine.CommitOutcomeAccepted,
	}
	core := &Core{engine: fake, runners: dir, pollWait: time.Second}

	groupResult := engine.GroupResult{
		ProtocolVersion: 1,
		GroupExecID:     "gexec-1",
		Attempt:         1,
		Outcome:         engine.GroupOutcomeSuccess,
		Exits: []engine.GroupExitResult{{
			NodeName: "C",
			Port:     "result",
			Data:     map[string]any{"output": "done"},
		}},
	}
	resp, err := core.reportResult(ctx, protocol.ReportResultRequest{
		RunnerID:    session.RunnerID,
		SessionID:   session.SessionID,
		Lease:       lease,
		GroupResult: &groupResult,
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("reportResult() error = %v", err)
	}
	if !resp.Accepted {
		t.Fatal("reportResult() accepted=false for group result")
	}
	if fake.groupCommittedRes.GroupExecID != "gexec-1" {
		t.Errorf("committed GroupExecID = %q, want gexec-1", fake.groupCommittedRes.GroupExecID)
	}
}

// TestGroupRenewLeaseSuccess verifies the renew lease endpoint extends a group
// lease through the engine's RenewGroupLease method.
func TestGroupRenewLeaseSuccess(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-grp",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.group", Features: []string{"group.exec.v1"}}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	assignment := groupTestAssignment()
	mustEnqueueAssignment(t, ctx, dir, assignment)
	claim := mustClaimGroupAssignment(t, ctx, dir, session)

	lease := &engine.TaskLease{
		LeaseID:    "lease-grp-renew",
		LeaseToken: "token-grp-renew",
		Task:       assignment.Task,
		Attempt:    1,
		NodeType:   "xflow.group",
	}
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	fake := &groupFakeEngine{groupRenewResult: true}
	core := &Core{engine: fake, runners: dir, pollWait: time.Second}

	resp, err := core.renewLease(ctx, protocol.RenewLeaseRequest{
		RunnerID:   session.RunnerID,
		SessionID:  session.SessionID,
		LeaseID:    string(lease.LeaseID),
		LeaseToken: string(lease.LeaseToken),
		Extend:     30000,
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("renewLease() error = %v", err)
	}
	if !resp.Renewed {
		t.Fatal("renewLease() renewed=false, want true")
	}
	if fake.groupRenewedLease == nil || fake.groupRenewedLease.LeaseToken != lease.LeaseToken {
		t.Errorf("renewed lease = %+v, want token %q", fake.groupRenewedLease, lease.LeaseToken)
	}
}

// TestGroupRenewLeaseStaleTokenReturnsNotRenewed verifies that a stale token
// renew attempt returns renewed=false without error.
func TestGroupRenewLeaseStaleTokenReturnsNotRenewed(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-grp",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.group", Features: []string{"group.exec.v1"}}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	assignment := groupTestAssignment()
	mustEnqueueAssignment(t, ctx, dir, assignment)
	claim := mustClaimGroupAssignment(t, ctx, dir, session)

	lease := &engine.TaskLease{
		LeaseID:    "lease-grp-renew",
		LeaseToken: "token-grp-renew",
		Task:       assignment.Task,
		Attempt:    1,
		NodeType:   "xflow.group",
	}
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	fake := &groupFakeEngine{groupRenewResult: false}
	core := &Core{engine: fake, runners: dir, pollWait: time.Second}

	resp, err := core.renewLease(ctx, protocol.RenewLeaseRequest{
		RunnerID:   session.RunnerID,
		SessionID:  session.SessionID,
		LeaseID:    string(lease.LeaseID),
		LeaseToken: string(lease.LeaseToken),
		Extend:     30000,
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("renewLease() error = %v", err)
	}
	if resp.Renewed {
		t.Fatal("renewLease() renewed=true, want false for stale token")
	}
}

// TestGroupPollFinalizeResponseLossReplay verifies that after a successful
// BuildGroupLease + FinalizeClaim followed by response loss, the re-poll
// replays the finalized lease with its GroupPayload intact via the
// claim.Lease != nil path.
func TestGroupPollFinalizeResponseLossReplay(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-grp",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.group", Features: []string{"group.exec.v1"}}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	assignment := groupTestAssignment()
	mustEnqueueAssignment(t, ctx, dir, assignment)

	groupLease := &engine.TaskLease{
		LeaseID:    "lease-grp-replay",
		LeaseToken: "token-grp-replay",
		Task:       assignment.Task,
		Attempt:    1,
		NodeType:   "xflow.group",
	}
	groupPayload := &engine.GroupLeasePayload{
		ProtocolVersion: 1,
		GroupExecID:     "gexec-replay",
		GroupID:         "exec-grp-1/grp1/0",
		GroupUnitIdx:    1,
		PackageHash:     "hash-replay",
		IdempotencyKey:  "normal/exec-grp-1/grp1/0",
	}
	fake := &groupFakeEngine{
		groupBuildLease:     groupLease,
		groupBuildPayload:   groupPayload,
		groupRecoverLease:   groupLease,
		groupRecoverPayload: groupPayload,
	}

	// Use a directory wrapper that fails FinalizeClaim on first attempt,
	// simulating a response loss. The lease is already durable in the engine.
	failDir := &failingFinalizeRunnerDirectory{RunnerDirectory: dir, failures: 1}
	core := &Core{engine: fake, runners: failDir, pollWait: time.Second}
	req := protocol.PollTaskRequest{
		RunnerID:     session.RunnerID,
		SessionID:    session.SessionID,
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.group", Features: []string{"group.exec.v1"}}},
	}

	// First poll: BuildGroupLease succeeds but FinalizeClaim fails → error.
	if _, err := core.pollTask(ctx, req, TransportInfo{}); err == nil {
		t.Fatal("first poll error = nil, want simulated finalize response loss")
	}

	// Second poll: BuildGroupLease returns ErrGroupLeaseAlreadyActive because
	// the lease was already durably acquired. RecoverGroupLease rebuilds it.
	fake.groupBuildErr = engine.ErrGroupLeaseAlreadyActive
	resp, err := core.pollTask(ctx, req, TransportInfo{})
	if err != nil {
		t.Fatalf("replay pollTask() error = %v", err)
	}
	if resp.Lease == nil {
		t.Fatal("replay poll: missing lease")
	}
	if resp.Lease.LeaseToken != groupLease.LeaseToken {
		t.Errorf("replay lease token = %q, want %q", resp.Lease.LeaseToken, groupLease.LeaseToken)
	}
	// On replay, the recovered lease must have the GroupPayload so the
	// runner can resume group execution.
	if resp.Lease.GroupPayload == nil {
		t.Fatal("replay poll: GroupPayload is nil, want recovered payload")
	}
}

// TestHTTPRenewLeaseEndpoint verifies the HTTP route is registered and works.
func TestHTTPRenewLeaseEndpoint(t *testing.T) {
	dir := NewMemoryRunnerDirectory()
	ctx := context.Background()
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-grp",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.group", Features: []string{"group.exec.v1"}}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	assignment := groupTestAssignment()
	mustEnqueueAssignment(t, ctx, dir, assignment)
	claim := mustClaimGroupAssignment(t, ctx, dir, session)
	lease := &engine.TaskLease{
		LeaseID:    "lease-grp-http",
		LeaseToken: "token-grp-http",
		Task:       assignment.Task,
		Attempt:    1,
		NodeType:   "xflow.group",
	}
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	fake := &groupFakeEngine{groupRenewResult: true}
	server := httptest.NewServer(NewServer(fake, dir).Handler())
	defer server.Close()

	var resp protocol.RenewLeaseResponse
	postJSON(t, server.URL+protocol.RenewLeasePath, protocol.RenewLeaseRequest{
		RunnerID:   session.RunnerID,
		SessionID:  session.SessionID,
		LeaseID:    string(lease.LeaseID),
		LeaseToken: string(lease.LeaseToken),
		Extend:     30000,
	}, http.StatusOK, &resp)
	if !resp.Renewed {
		t.Fatal("HTTP renew lease: renewed=false, want true")
	}
}

// TestGroupReportResultTimeoutRace verifies that when CommitGroupResult
// returns a stale-token outcome (lease was already reclaimed by the sweeper),
// reportResult returns the fencing error.
func TestGroupReportResultTimeoutRace(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-grp",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.group", Features: []string{"group.exec.v1"}}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	assignment := groupTestAssignment()
	mustEnqueueAssignment(t, ctx, dir, assignment)
	claim := mustClaimGroupAssignment(t, ctx, dir, session)

	lease := &engine.TaskLease{
		LeaseID:    "lease-grp-timeout",
		LeaseToken: "token-grp-timeout",
		Task:       assignment.Task,
		Attempt:    1,
		NodeType:   "xflow.group",
	}
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	fake := &groupFakeEngine{
		groupCommitOutcome: engine.CommitOutcomeStaleToken,
		groupCommitErr:     engine.ErrInvalidLeaseToken,
	}
	core := &Core{engine: fake, runners: dir, pollWait: time.Second}

	_, err = core.reportResult(ctx, protocol.ReportResultRequest{
		RunnerID:  session.RunnerID,
		SessionID: session.SessionID,
		Lease:     lease,
		GroupResult: &engine.GroupResult{
			Outcome:     engine.GroupOutcomeSuccess,
			GroupExecID: "gexec-timeout",
		},
	}, TransportInfo{})
	if !errors.Is(err, engine.ErrInvalidLeaseToken) {
		t.Fatalf("reportResult() error = %v, want ErrInvalidLeaseToken", err)
	}
}

// TestNodeRenewLeaseDelegatesToEngine verifies that renew for a node task uses
// the existing node-lease renewal path (not the group path).
func TestNodeRenewLeaseDelegatesToEngine(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-node",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	assignment := stableTestAssignment("node-renew")
	mustEnqueueAssignment(t, ctx, dir, assignment)
	claim := mustClaimAssignment(t, ctx, dir, session)

	lease := &engine.TaskLease{
		LeaseID:    "lease-node-renew",
		LeaseToken: "token-node-renew",
		Task:       assignment.Task,
		Attempt:    1,
		NodeType:   "xflow.function",
	}
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	fake := &groupFakeEngine{groupRenewResult: true, nodeRenewResult: true}
	core := &Core{engine: fake, runners: dir, pollWait: time.Second}

	resp, err := core.renewLease(ctx, protocol.RenewLeaseRequest{
		RunnerID:   session.RunnerID,
		SessionID:  session.SessionID,
		LeaseID:    string(lease.LeaseID),
		LeaseToken: string(lease.LeaseToken),
		Extend:     30000,
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("renewLease() error = %v", err)
	}
	if !resp.Renewed {
		t.Fatalf("renewLease() renewed=false err=%q — a node lease that the engine renewed "+
			"must be reported as renewed, or the runner stops renewing and the sweeper "+
			"hands its work to a second runner", resp.Error)
	}
	if resp.Deadline.IsZero() {
		t.Error("renewLease() returned no deadline for a renewed node lease")
	}
	if fake.groupRenewedLease != nil {
		t.Error("a node task went down the group renewal path")
	}
	if fake.nodeRenewedLease == nil {
		t.Fatal("renewLease() never called the engine's node renewal")
	}
	if fake.nodeRenewedLease.LeaseToken != lease.LeaseToken {
		t.Errorf("engine renewed token %q, want %q — the fence must be the caller's own lease",
			fake.nodeRenewedLease.LeaseToken, lease.LeaseToken)
	}
	if fake.nodeRenewExtend != 30*time.Second {
		t.Errorf("engine renewed with extend %v, want 30s (the request's Extend in ms)", fake.nodeRenewExtend)
	}
}

// TestNodeRenewLeaseReportsEngineRefusal verifies a refused renewal (stale
// token, node no longer running) surfaces as Renewed=false rather than as an
// error, so the runner can cancel its handler instead of retrying forever.
func TestNodeRenewLeaseReportsEngineRefusal(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-node-refuse",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	assignment := stableTestAssignment("node-renew-refuse")
	mustEnqueueAssignment(t, ctx, dir, assignment)
	claim := mustClaimAssignment(t, ctx, dir, session)
	lease := &engine.TaskLease{
		LeaseID:    "lease-node-refuse",
		LeaseToken: "token-node-refuse",
		Task:       assignment.Task,
		Attempt:    1,
		NodeType:   "xflow.function",
	}
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	fake := &groupFakeEngine{nodeRenewResult: false}
	core := &Core{engine: fake, runners: dir, pollWait: time.Second}

	resp, err := core.renewLease(ctx, protocol.RenewLeaseRequest{
		RunnerID:   session.RunnerID,
		SessionID:  session.SessionID,
		LeaseID:    string(lease.LeaseID),
		LeaseToken: string(lease.LeaseToken),
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("renewLease() error = %v, want a Renewed=false response", err)
	}
	if resp.Renewed {
		t.Fatal("renewLease() reported renewed for a lease the engine refused")
	}
	if !resp.Deadline.IsZero() {
		t.Error("renewLease() returned a deadline for a refused renewal")
	}
}

// TestRenewLeaseRejectsWithoutValidSession verifies auth enforcement on renew.
func TestRenewLeaseRejectsWithoutValidSession(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	_, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-grp",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.group"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	fake := &groupFakeEngine{groupRenewResult: true}
	core := &Core{engine: fake, runners: dir, pollWait: time.Second}

	_, err = core.renewLease(ctx, protocol.RenewLeaseRequest{
		RunnerID:   "runner-grp",
		SessionID:  "stale-session-id",
		LeaseID:    "lease-x",
		LeaseToken: "token-x",
		Extend:     30000,
	}, TransportInfo{})
	if !errors.Is(err, ErrRunnerSessionStale) {
		t.Fatalf("renewLease() error = %v, want ErrRunnerSessionStale", err)
	}
}

// TestHTTPGroupPollReturnsGroupPayloadJSON and related helpers
func mustClaimGroupAssignment(t *testing.T, ctx context.Context, dir *MemoryRunnerDirectory, session RunnerSession) Claim {
	t.Helper()
	claim, ok, err := dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:     session.RunnerID,
		SessionID:    session.SessionID,
		Capacity:     4,
		Capabilities: []protocol.Capability{{NodeType: "xflow.group", Features: []string{"group.exec.v1"}}},
		Now:          time.Now(),
	})
	if err != nil {
		t.Fatalf("ClaimForRunner() error = %v", err)
	}
	if !ok {
		t.Fatal("ClaimForRunner() ok=false, want claim")
	}
	return claim
}

// Verify GroupPayload is serialized correctly in the HTTP wire format.
func TestHTTPGroupPollReturnsGroupPayloadJSON(t *testing.T) {
	dir := NewMemoryRunnerDirectory()
	ctx := context.Background()
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-grp",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.group", Features: []string{"group.exec.v1"}}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	assignment := groupTestAssignment()
	mustEnqueueAssignment(t, ctx, dir, assignment)

	groupLease := &engine.TaskLease{
		LeaseID:    "lease-http-grp",
		LeaseToken: "token-http-grp",
		Task:       assignment.Task,
		Attempt:    1,
		NodeType:   "xflow.group",
	}
	groupPayload := &engine.GroupLeasePayload{
		ProtocolVersion: 1,
		GroupExecID:     "gexec-http",
		GroupID:         "exec-grp-1/grp1/0",
		GroupUnitIdx:    1,
		PackageHash:     "hash-http",
		IdempotencyKey:  "normal/exec-grp-1/grp1/0",
	}
	fake := &groupFakeEngine{
		groupBuildLease:   groupLease,
		groupBuildPayload: groupPayload,
	}

	server := httptest.NewServer(NewServer(fake, dir).Handler())
	defer server.Close()

	var resp protocol.PollTaskResponse
	postJSON(t, server.URL+protocol.PollTaskPath, protocol.PollTaskRequest{
		RunnerID:     session.RunnerID,
		SessionID:    session.SessionID,
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.group", Features: []string{"group.exec.v1"}}},
	}, http.StatusOK, &resp)
	if resp.Lease == nil {
		t.Fatal("HTTP poll: nil lease")
	}
	if resp.Lease.GroupPayload == nil {
		t.Fatal("HTTP poll: GroupPayload is nil")
	}

	// Verify JSON round-trip: marshal the response and ensure group_payload is present.
	raw, _ := json.Marshal(resp)
	var m map[string]any
	json.Unmarshal(raw, &m)
	if lm, ok := m["lease"].(map[string]any); ok {
		if _, has := lm["group_payload"]; !has {
			t.Fatal("HTTP poll JSON: lease.group_payload missing from wire response")
		}
	}
}

// replayingRunnerDirectory hands back a durable finalized lease as a replay on
// the FIRST claim and nothing afterwards, which is what RedisRunnerDirectory
// does via replayLease. MemoryRunnerDirectory has no replay path at all, so a
// test built on it can never reach pollTask's claim.Lease != nil branch --
// which is precisely why the whole unit suite stayed green while the group
// binary e2e went red on this bug.
type replayingRunnerDirectory struct {
	RunnerDirectory
	replay  *engine.TaskLease
	assign  Assignment
	served  bool
	cleared []AssignmentID
}

func (d *replayingRunnerDirectory) ClaimForRunner(_ context.Context, _ ClaimRequest) (Claim, bool, error) {
	if d.served {
		return Claim{}, false, nil
	}
	d.served = true
	return Claim{ClaimID: "claim-replay", Assignment: d.assign, Lease: d.replay}, true, nil
}

func (d *replayingRunnerDirectory) ClearAssignment(_ context.Context, id AssignmentID) error {
	d.cleared = append(d.cleared, id)
	return nil
}

// TestGroupPollReplayAfterCommitDropsAssignment covers the regression where a
// durable at-least-once replay arriving AFTER the group already committed took
// down the whole runner.
//
// Sequence: the group task was claimed, finalized, executed and committed, so
// its group lease is gone from the backend -- and only then does the directory
// hand the same finalized lease back as a replay (a legitimate at-least-once
// delivery). RecoverGroupLease therefore finds no live lease.
//
// Before the fix that error propagated out of pollTask, which the transport
// renders as HTTP 500; the runner's pollLoop treats any poll error as fatal
// and returns, so the runner stopped claiming work entirely and every
// downstream node of that group stalled until the caller timed out. The replay
// must instead be recognized as a duplicate of finished work: drop the
// assignment and keep serving polls.
func TestGroupPollReplayAfterCommitDropsAssignment(t *testing.T) {
	ctx := context.Background()
	assignment := groupTestAssignment()
	replayLease := &engine.TaskLease{
		LeaseID:    "lease-grp-done",
		LeaseToken: "token-grp-done",
		Task:       assignment.Task,
		Attempt:    1,
		NodeType:   "xflow.group",
	}
	dir := &replayingRunnerDirectory{
		RunnerDirectory: NewMemoryRunnerDirectory(),
		replay:          replayLease,
		assign:          assignment,
	}
	fake := &groupFakeEngine{groupRecoverErr: engine.ErrGroupLeaseNotActive}
	core := &Core{engine: fake, runners: dir, pollWait: time.Second}

	resp, err := core.pollTask(ctx, protocol.PollTaskRequest{
		RunnerID:  "runner-grp",
		SessionID: "session-grp",
		Capacity:  1,
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("replay pollTask() error = %v, want nil (a post-commit replay "+
			"is finished work, not a server error -- propagating it returns 500 "+
			"and the runner's pollLoop stops claiming work)", err)
	}
	if resp.Lease != nil {
		t.Fatalf("replay pollTask() returned lease %+v, want nil", resp.Lease)
	}
	if len(dir.cleared) != 1 || dir.cleared[0] != assignment.AssignmentID {
		t.Errorf("cleared assignments = %v, want [%s] -- a replay of finished "+
			"work must be dropped, not left to be replayed forever",
			dir.cleared, assignment.AssignmentID)
	}
}
