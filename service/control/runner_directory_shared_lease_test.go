package control

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xbcio/xflow/engine"
)

// A map node and every batch it expanded share one lease identity, while the
// directory's by-token / by-id indexes hold one assignment per identity.

type sharedLeaseDirectory interface {
	RunnerDirectory
	LeaseLookup
}

// finalizeSharedLease enqueues, claims and finalizes task under lease L1.
func finalizeSharedLease(t *testing.T, ctx context.Context, dir sharedLeaseDirectory, session RunnerSession, task engine.Task) AssignmentID {
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
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, &engine.TaskLease{LeaseID: "L1", LeaseToken: "tok-L1", Task: task}); err != nil {
		t.Fatal(err)
	}
	return id
}

// echoedLookupKey builds the key reportResult builds from a runner's echo:
// the lease crosses the wire as JSON, which drops ActivationID and AutoDepth.
func echoedLookupKey(t *testing.T, task engine.Task) LeaseLookupKey {
	t.Helper()
	raw, err := json.Marshal(engine.TaskLease{LeaseID: "L1", LeaseToken: "tok-L1", Task: task})
	if err != nil {
		t.Fatal(err)
	}
	var echoed engine.TaskLease
	if err := json.Unmarshal(raw, &echoed); err != nil {
		t.Fatal(err)
	}
	return LeaseLookupKey{
		AssignmentID: BuildAssignmentID(&echoed.Task),
		LeaseID:      echoed.LeaseID,
		LeaseToken:   echoed.LeaseToken,
		NodeName:     echoed.Task.NodeName,
		NodeIdx:      echoed.Task.NodeIdx,
	}
}

func testSiblingBatchReportResolvesToItself(t *testing.T, dir sharedLeaseDirectory, session RunnerSession) {
	ctx := context.Background()
	batches := []engine.Task{mapBatchTask("L1", 0), mapBatchTask("L1", 1)}
	for _, b := range batches {
		finalizeSharedLease(t, ctx, dir, session, b)
	}
	for _, b := range batches {
		got, found, err := dir.LookupLease(ctx, session.RunnerID, session.SessionID, echoedLookupKey(t, b))
		if err != nil || !found {
			t.Fatalf("lookup %s found=%v err=%v", b.NodeName, found, err)
		}
		if got.Task.NodeName != b.NodeName {
			t.Fatalf("report for %s resolved to %s", b.NodeName, got.Task.NodeName)
		}
	}
	forged := echoedLookupKey(t, batches[0])
	forged.NodeName = "m/_batch/9"
	if _, found, err := dir.LookupLease(ctx, session.RunnerID, session.SessionID, forged); err != nil || found {
		t.Fatalf("lookup for a task the runner does not hold found=%v err=%v", found, err)
	}
}

func testMapReleaseKeepsBatchRecord(t *testing.T, dir sharedLeaseDirectory, session RunnerSession) {
	ctx := context.Background()
	mapTask := engine.Task{ExecutionID: "exec-1", NodeName: "m", NodeIdx: 0, Type: engine.TaskTypeNodeExec, ActivationID: 1}
	mapID := finalizeSharedLease(t, ctx, dir, session, mapTask)
	// The batch is claimed and finalized before the map node's own report
	// reaches ReleaseLeased.
	batch := mapBatchTask("L1", 0)
	finalizeSharedLease(t, ctx, dir, session, batch)

	if err := dir.ReleaseLeased(ctx, ReleaseLeasedRequest{RunnerID: session.RunnerID, SessionID: session.SessionID,
		AssignmentID: mapID, LeaseID: "L1", LeaseToken: "tok-L1", RemoveSeen: true}); err != nil {
		t.Fatal(err)
	}
	got, found, err := dir.LookupLease(ctx, session.RunnerID, session.SessionID, echoedLookupKey(t, batch))
	if err != nil || !found || got.Task.NodeName != batch.NodeName {
		t.Fatalf("batch lease after map release found=%v err=%v", found, err)
	}
	if _, found, _ := dir.LookupLease(ctx, session.RunnerID, session.SessionID, echoedLookupKey(t, mapTask)); found {
		t.Fatal("map lease still resolvable after its release")
	}
}

func TestRedisRunnerDirectorySiblingBatchReportResolvesToItself(t *testing.T) {
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	dir := NewRedisRunnerDirectory(rdb)
	testSiblingBatchReportResolvesToItself(t, dir, registerRedisDirectoryRunner(t, context.Background(), dir, "runner-1", 4))
}

func TestRedisRunnerDirectoryMapReleaseKeepsBatchRecord(t *testing.T) {
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	dir := NewRedisRunnerDirectory(rdb)
	testMapReleaseKeepsBatchRecord(t, dir, registerRedisDirectoryRunner(t, context.Background(), dir, "runner-1", 4))
}

func TestMemoryRunnerDirectorySiblingBatchReportResolvesToItself(t *testing.T) {
	dir := NewMemoryRunnerDirectory()
	testSiblingBatchReportResolvesToItself(t, dir, registerMemoryBatchRunner(t, context.Background(), dir, "runner-1"))
}

func TestMemoryRunnerDirectoryMapReleaseKeepsBatchRecord(t *testing.T) {
	dir := NewMemoryRunnerDirectory()
	testMapReleaseKeepsBatchRecord(t, dir, registerMemoryBatchRunner(t, context.Background(), dir, "runner-1"))
}

// The per-runner leased index is best effort (written by FinalizeClaim, pruned
// by replay, absent for leases finalized before it existed). A report must not
// be accepted or refused depending on it, so both backends must agree when it
// is short.
func testSiblingBatchReportSurvivesShortIndex(t *testing.T, dir sharedLeaseDirectory, session RunnerSession, dropIndex func()) {
	ctx := context.Background()
	batches := []engine.Task{mapBatchTask("L1", 0), mapBatchTask("L1", 1)}
	for _, b := range batches {
		finalizeSharedLease(t, ctx, dir, session, b)
	}
	dropIndex()
	got, found, err := dir.LookupLease(ctx, session.RunnerID, session.SessionID, echoedLookupKey(t, batches[0]))
	if err != nil || !found || got.Task.NodeName != batches[0].NodeName {
		t.Fatalf("report for %s with a short index found=%v err=%v", batches[0].NodeName, found, err)
	}
}

func TestRedisRunnerDirectorySiblingBatchReportSurvivesShortIndex(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	dir := NewRedisRunnerDirectory(rdb)
	session := registerRedisDirectoryRunner(t, ctx, dir, "runner-1", 4)
	testSiblingBatchReportSurvivesShortIndex(t, dir, session, func() {
		if err := rdb.Del(ctx, dir.keys.runnerLeasedAssignmentsKey(session.RunnerID)).Err(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestMemoryRunnerDirectorySiblingBatchReportSurvivesShortIndex(t *testing.T) {
	dir := NewMemoryRunnerDirectory()
	testSiblingBatchReportSurvivesShortIndex(t, dir, registerMemoryBatchRunner(t, context.Background(), dir, "runner-1"), func() {})
}
