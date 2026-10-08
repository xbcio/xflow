package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

// The renewal path carries only the lease identity — LeaseID and LeaseToken,
// no task — while a map node and its batches share one identity and the
// by-token / by-id indexes hold one assignment per identity. These tests pin
// the renewal-side counterpart of the report-side sibling scan: a renewal that
// misses the index resolves to a live assignment of this runner (a batch over
// its parent map node), and the refresh that follows re-arms every assignment
// of the identity, not just the one the resolver returned.

// renewalLookupKey is what the runner's renewal request resolves through:
// lease identity only, no task.
func renewalLookupKey(leaseID, token string) LeaseLookupKey {
	return LeaseLookupKey{LeaseID: engine.LeaseID(leaseID), LeaseToken: engine.LeaseToken(token)}
}

// mapParentTask is the map node's own task, the one expansionBatchTask copies
// its identity from.
func mapParentTask() engine.Task {
	return engine.Task{ExecutionID: "exec-1", NodeName: "m", NodeIdx: 0, Type: engine.TaskTypeNodeExec, ActivationID: 1}
}

// batchLeasePayload is the payload the dispatch path attaches to a batch
// lease; the parent map node's lease carries none.
func batchLeasePayload(index int) *engine.SubgraphLeasePayload {
	return &engine.SubgraphLeasePayload{ProtocolVersion: 1, ParentNode: "m", ParentNodeIdx: 0, BatchIndex: index}
}

// finalizeRenewalLease enqueues, claims and finalizes task under the shared
// identity L1/tok-L1, attaching payload the way dispatchSubgraphLease does.
func finalizeRenewalLease(t *testing.T, ctx context.Context, dir sharedLeaseDirectory, session RunnerSession, task engine.Task, payload *engine.SubgraphLeasePayload) AssignmentID {
	t.Helper()
	id := BuildAssignmentID(&task)
	if ok, err := dir.EnqueueAssignment(ctx, Assignment{AssignmentID: id, Task: task, Routing: engine.TaskRouting{NodeType: "xflow.function"}}); err != nil || !ok {
		t.Fatalf("enqueue %q = %v, %v", id, ok, err)
	}
	req := redisDirectoryClaimRequest(session, 4)
	req.Capacity = 4
	req.ActiveLeaseIDs = []string{"L1"}
	claim, ok, err := dir.ClaimForRunner(ctx, req)
	if err != nil || !ok || claim.Assignment.AssignmentID != id {
		t.Fatalf("claim ok=%v err=%v got %q, want %q", ok, err, claim.Assignment.AssignmentID, id)
	}
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, &engine.TaskLease{LeaseID: "L1", LeaseToken: "tok-L1", Task: task, SubgraphPayload: payload}); err != nil {
		t.Fatal(err)
	}
	return id
}

func dropMemoryLeaseIndexes(dir *MemoryRunnerDirectory, session RunnerSession, leaseID, token string) {
	state := dir.runners[session.RunnerID]
	delete(state.leaseByID, engine.LeaseID(leaseID))
	delete(state.leaseByToken, engine.LeaseToken(token))
}

// testRenewalResolvesAfterIndexHolderReleased: the last finalized batch holds
// both indexes; its report releases it and takes the indexes with it. The
// remaining batches still renew, and each renewal used to be refused with
// "lease not found" — the runner then cancelled a healthy batch.
func testRenewalResolvesAfterIndexHolderReleased(t *testing.T, ctx context.Context, dir sharedLeaseDirectory, session RunnerSession) {
	finalizeRenewalLease(t, ctx, dir, session, mapParentTask(), nil)
	batches := []engine.Task{mapBatchTask("L1", 0), mapBatchTask("L1", 1)}
	for i, batch := range batches {
		finalizeRenewalLease(t, ctx, dir, session, batch, batchLeasePayload(i))
	}
	if err := dir.ReleaseLeased(ctx, ReleaseLeasedRequest{RunnerID: session.RunnerID, SessionID: session.SessionID,
		AssignmentID: BuildAssignmentID(&batches[1]), LeaseID: "L1", LeaseToken: "tok-L1", RemoveSeen: true}); err != nil {
		t.Fatal(err)
	}

	got, found, err := dir.LookupLease(ctx, session.RunnerID, session.SessionID, renewalLookupKey("L1", "tok-L1"))
	if err != nil || !found {
		t.Fatalf("batch renewal after the index holder released found=%v err=%v", found, err)
	}
	if got.SubgraphPayload == nil || got.Task.NodeName != batches[0].NodeName {
		t.Fatalf("renewal resolved to %q (payload=%v), want the live batch %q; the parent map node must not win",
			got.Task.NodeName, got.SubgraphPayload != nil, batches[0].NodeName)
	}
}

// testRenewalFallsBackToOwnRunnerAndPrefersBatch: the indexes name the last
// finalized assignment, which here belongs to a different runner, so the
// renewal cannot resolve through them. The fallback must pick this runner's own
// live batch — not the parent map node, whose absolute deadline feeds the
// renewal deadline backstop.
func testRenewalFallsBackToOwnRunnerAndPrefersBatch(t *testing.T, ctx context.Context, dir sharedLeaseDirectory, session, other RunnerSession) {
	finalizeRenewalLease(t, ctx, dir, session, mapParentTask(), nil)
	batch := mapBatchTask("L1", 0)
	finalizeRenewalLease(t, ctx, dir, session, batch, batchLeasePayload(0))
	finalizeRenewalLease(t, ctx, dir, other, mapBatchTask("L1", 1), batchLeasePayload(1))

	got, found, err := dir.LookupLease(ctx, session.RunnerID, session.SessionID, renewalLookupKey("L1", "tok-L1"))
	if err != nil || !found {
		t.Fatalf("renewal with a cross-runner index found=%v err=%v", found, err)
	}
	if got.SubgraphPayload == nil || got.Task.NodeName != batch.NodeName {
		t.Fatalf("renewal resolved to %q (payload=%v), want the runner's own batch %q",
			got.Task.NodeName, got.SubgraphPayload != nil, batch.NodeName)
	}
}

// testRenewalSurvivesIndexLossForPlainNode: an ordinary node lease whose index
// entries were dropped must still renew — the fallback is not batch-specific.
func testRenewalSurvivesIndexLossForPlainNode(t *testing.T, ctx context.Context, dir sharedLeaseDirectory, session RunnerSession, dropIndex func()) {
	task := engine.Task{ExecutionID: "exec-1", NodeName: "step1", NodeIdx: 0, Type: engine.TaskTypeNodeExec, ActivationID: 1}
	finalizeRenewalLease(t, ctx, dir, session, task, nil)
	dropIndex()

	got, found, err := dir.LookupLease(ctx, session.RunnerID, session.SessionID, renewalLookupKey("L1", "tok-L1"))
	if err != nil || !found {
		t.Fatalf("plain-node renewal with a lost index found=%v err=%v", found, err)
	}
	if got.Task.NodeName != task.NodeName || got.SubgraphPayload != nil {
		t.Fatalf("renewal resolved to %q (payload=%v), want the node's own lease", got.Task.NodeName, got.SubgraphPayload != nil)
	}
}

func TestRedisRunnerDirectoryRenewalResolvesAfterIndexHolderReleased(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	dir := NewRedisRunnerDirectory(rdb)
	testRenewalResolvesAfterIndexHolderReleased(t, ctx, dir, registerRedisDirectoryRunner(t, ctx, dir, "runner-1", 4))
}

func TestMemoryRunnerDirectoryRenewalResolvesAfterIndexHolderReleased(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	testRenewalResolvesAfterIndexHolderReleased(t, ctx, dir, registerMemoryBatchRunner(t, ctx, dir, "runner-1"))
}

func TestRedisRunnerDirectoryRenewalFallsBackToOwnRunnerAndPrefersBatch(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	dir := NewRedisRunnerDirectory(rdb)
	other := registerRedisDirectoryRunner(t, ctx, dir, "runner-2", 4)
	testRenewalFallsBackToOwnRunnerAndPrefersBatch(t, ctx, dir, registerRedisDirectoryRunner(t, ctx, dir, "runner-1", 4), other)
}

func TestMemoryRunnerDirectoryRenewalFallsBackToOwnRunnerAndPrefersBatch(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	other := registerMemoryBatchRunner(t, ctx, dir, "runner-2")
	testRenewalFallsBackToOwnRunnerAndPrefersBatch(t, ctx, dir, registerMemoryBatchRunner(t, ctx, dir, "runner-1"), other)
}

func TestRedisRunnerDirectoryRenewalSurvivesIndexLoss(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	dir := NewRedisRunnerDirectory(rdb)
	session := registerRedisDirectoryRunner(t, ctx, dir, "runner-1", 4)
	testRenewalSurvivesIndexLossForPlainNode(t, ctx, dir, session, func() {
		if err := rdb.HDel(ctx, dir.keys.leaseByToken, "tok-L1").Err(); err != nil {
			t.Fatal(err)
		}
		if err := rdb.HDel(ctx, dir.keys.leaseByID, "L1").Err(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestMemoryRunnerDirectoryRenewalSurvivesIndexLoss(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	session := registerMemoryBatchRunner(t, ctx, dir, "runner-1")
	testRenewalSurvivesIndexLossForPlainNode(t, ctx, dir, session, func() {
		dropMemoryLeaseIndexes(dir, session, "L1", "tok-L1")
	})
}

// finalizeSharedIdentityAssignment finalizes one more assignment under the
// shared identity L1. The claim carries ActiveLeaseIDs because otherwise every
// claim after the first replays the finalized lease of a sibling — replay
// matches on (runner, session, state==leased) alone — and there would be no
// claim left for FinalizeClaim to finalize.
func finalizeSharedIdentityAssignment(t *testing.T, ctx context.Context, directory *RedisRunnerDirectory, session RunnerSession, assignment Assignment, lease *engine.TaskLease) {
	t.Helper()
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	req := redisDirectoryClaimRequest(session, 1)
	req.Capacity = 3
	req.ActiveLeaseIDs = []string{"L1"}
	claim, ok, err := directory.ClaimForRunner(ctx, req)
	if err != nil || !ok {
		t.Fatalf("ClaimForRunner() ok=%v err=%v", ok, err)
	}
	if claim.Assignment.AssignmentID != assignment.AssignmentID {
		t.Fatalf("ClaimForRunner() claimed %q, want %q", claim.Assignment.AssignmentID, assignment.AssignmentID)
	}
	if err := directory.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim(): %v", err)
	}
}

// finalizeSharedIdentityGroup finalizes a map node and two of its batches under
// one identity, shaped like dispatchSubgraphLease: the parent without a
// payload, the batches with one.
func finalizeSharedIdentityGroup(t *testing.T, ctx context.Context, dir sharedLeaseDirectory, session RunnerSession) []AssignmentID {
	t.Helper()
	finalizeRenewalLease(t, ctx, dir, session, mapParentTask(), nil)
	batches := []engine.Task{mapBatchTask("L1", 0), mapBatchTask("L1", 1)}
	ids := make([]AssignmentID, 0, len(batches)+1)
	for i, batch := range batches {
		ids = append(ids, finalizeRenewalLease(t, ctx, dir, session, batch, batchLeasePayload(i)))
	}
	return ids
}

// TestRenewLeaseResolvesBatchRenewalAfterIndexHolderReleased drives the whole
// renewal endpoint: after the index-holding batch reported and released, a
// sibling batch's renewal must still succeed, and the engine renews the parent
// map node through the resolved batch lease.
func TestRenewLeaseResolvesBatchRenewalAfterIndexHolderReleased(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	session := registerMemoryBatchRunner(t, ctx, dir, "runner-renewal")
	finalizeSharedIdentityGroup(t, ctx, dir, session)
	holder := mapBatchTask("L1", 1)
	if err := dir.ReleaseLeased(ctx, ReleaseLeasedRequest{RunnerID: session.RunnerID, SessionID: session.SessionID,
		AssignmentID: BuildAssignmentID(&holder), LeaseID: "L1", LeaseToken: "tok-L1", RemoveSeen: true}); err != nil {
		t.Fatal(err)
	}

	fake := &groupFakeEngine{nodeRenewResult: true}
	core := &Core{engine: fake, runners: dir, pollWait: time.Second}
	resp, err := core.renewLease(ctx, protocol.RenewLeaseRequest{
		RunnerID:   session.RunnerID,
		SessionID:  session.SessionID,
		LeaseID:    "L1",
		LeaseToken: "tok-L1",
		Extend:     30000,
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("renewLease() error = %v", err)
	}
	if !resp.Renewed {
		t.Fatalf("renewLease() Renewed=false (err=%q); the runner would cancel a healthy batch", resp.Error)
	}
	if fake.nodeRenewedLease == nil || fake.nodeRenewedLease.SubgraphPayload == nil ||
		fake.nodeRenewedLease.SubgraphPayload.ParentNode != "m" {
		t.Fatalf("engine renewed %+v, want the batch lease whose payload names the parent m", fake.nodeRenewedLease)
	}
}

// TestRenewLeaseBatchRenewalIgnoresParentDeadline: the deadline backstop reads
// the resolved lease, and the parent map node here carries a past deadline.
// The fallback must resolve the batch renewal to a batch — whose lease has no
// deadline — or a long map would be timeout-killed from the renewal of a batch
// that is still making progress.
func TestRenewLeaseBatchRenewalIgnoresParentDeadline(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	session := registerMemoryBatchRunner(t, ctx, dir, "runner-renewal-deadline")

	parent := mapParentTask()
	parentID := BuildAssignmentID(&parent)
	if ok, err := dir.EnqueueAssignment(ctx, Assignment{AssignmentID: parentID, Task: parent, Routing: engine.TaskRouting{NodeType: "xflow.function"}}); err != nil || !ok {
		t.Fatalf("enqueue parent = %v, %v", ok, err)
	}
	claim := claimMemoryBatch(t, ctx, dir, session)
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, &engine.TaskLease{
		LeaseID: "L1", LeaseToken: "tok-L1", Task: parent,
		IssuedAt:          time.Now().Add(-10 * time.Minute),
		TTL:               60 * time.Second,
		ExecutionDeadline: time.Now().Add(-5 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	finalizeRenewalLease(t, ctx, dir, session, mapBatchTask("L1", 0), batchLeasePayload(0))
	dropMemoryLeaseIndexes(dir, session, "L1", "tok-L1")

	fake := &deadlineTestEngine{nodeRenewResult: true}
	core := &Core{engine: fake, runners: dir, pollWait: time.Second}
	resp, err := core.renewLease(ctx, protocol.RenewLeaseRequest{
		RunnerID:   session.RunnerID,
		SessionID:  session.SessionID,
		LeaseID:    "L1",
		LeaseToken: "tok-L1",
		Extend:     30000,
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("renewLease() error = %v", err)
	}
	if !resp.Renewed {
		t.Fatalf("renewLease() Renewed=false (err=%q), want a renewal through the batch", resp.Error)
	}
	if fake.commitTimeoutCalled {
		t.Fatal("the batch renewal fired the parent's deadline; the fallback resolved to the parent map node")
	}
}

// TestRedisRunnerDirectoryRefreshLeaseMetaRefreshesEverySharedAssignment: one
// successful renewal must re-arm the metadata of every assignment the runner
// holds under the identity. Refreshing only the index-resolved sibling lets the
// others expire at their finalized deadline while the engine lease keeps being
// extended, and their own renewal then fails.
func TestRedisRunnerDirectoryRefreshLeaseMetaRefreshesEverySharedAssignment(t *testing.T) {
	const claimTTL = 2 * time.Second
	ctx, server, directory, session := newRedisRunnerDirectoryLeaseMetaTestDirectory(t, claimTTL, 4)
	parent := mapParentTask()
	assignments := []Assignment{{AssignmentID: BuildAssignmentID(&parent), Task: parent, Routing: engine.TaskRouting{NodeType: "xflow.function"}}}
	for _, batch := range []engine.Task{mapBatchTask("L1", 0), mapBatchTask("L1", 1)} {
		assignments = append(assignments, Assignment{AssignmentID: BuildAssignmentID(&batch), Task: batch, Routing: engine.TaskRouting{NodeType: "xflow.function"}})
	}
	for i, assignment := range assignments {
		lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "L1", 3*time.Second)
		if i > 0 {
			lease.SubgraphPayload = batchLeasePayload(i - 1)
		}
		finalizeSharedIdentityAssignment(t, ctx, directory, session, assignment, lease)
	}
	for _, assignment := range assignments {
		assertRedisRunnerDirectoryLeaseMetaTTL(t, server, directory.keys.assignmentLeaseMetaKey(string(assignment.AssignmentID)), 3*time.Second+claimTTL)
	}

	// Burn most of the original window: the resolver alone would re-arm one of
	// the three.
	server.FastForward(4 * time.Second)
	if err := directory.RefreshLeaseMeta(ctx, session.RunnerID, session.SessionID, renewalLookupKey("L1", "token-L1"), 20*time.Second); err != nil {
		t.Fatalf("RefreshLeaseMeta() error = %v", err)
	}
	for _, assignment := range assignments {
		assertRedisRunnerDirectoryLeaseMetaTTL(t, server, directory.keys.assignmentLeaseMetaKey(string(assignment.AssignmentID)), 20*time.Second+claimTTL)
	}
}

// TestRenewLeaseRefreshesEverySharedAssignment is the end-to-end half: a
// renewal the engine accepted must leave every assignment of the identity with
// its directory expiry pushed forward.
func TestRenewLeaseRefreshesEverySharedAssignment(t *testing.T) {
	const claimTTL = 2 * time.Second
	ctx, server, directory, session := newRedisRunnerDirectoryLeaseMetaTestDirectory(t, claimTTL, 4)
	parent := mapParentTask()
	assignments := []Assignment{{AssignmentID: BuildAssignmentID(&parent), Task: parent, Routing: engine.TaskRouting{NodeType: "xflow.function"}}}
	for _, batch := range []engine.Task{mapBatchTask("L1", 0), mapBatchTask("L1", 1)} {
		assignments = append(assignments, Assignment{AssignmentID: BuildAssignmentID(&batch), Task: batch, Routing: engine.TaskRouting{NodeType: "xflow.function"}})
	}
	for i, assignment := range assignments {
		lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "L1", 3*time.Second)
		if i > 0 {
			lease.SubgraphPayload = batchLeasePayload(i - 1)
		}
		finalizeSharedIdentityAssignment(t, ctx, directory, session, assignment, lease)
	}
	server.FastForward(4 * time.Second)

	core := &Core{engine: &groupFakeEngine{nodeRenewResult: true}, runners: directory, pollWait: time.Second}
	resp, err := core.renewLease(ctx, protocol.RenewLeaseRequest{
		RunnerID:   session.RunnerID,
		SessionID:  session.SessionID,
		LeaseID:    "L1",
		LeaseToken: "token-L1",
		Extend:     30000,
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("renewLease() error = %v", err)
	}
	if !resp.Renewed {
		t.Fatalf("renewLease() Renewed=false (err=%q)", resp.Error)
	}
	for _, assignment := range assignments {
		assertRedisRunnerDirectoryLeaseMetaTTL(t, server, directory.keys.assignmentLeaseMetaKey(string(assignment.AssignmentID)), 30*time.Second+claimTTL)
	}
}
