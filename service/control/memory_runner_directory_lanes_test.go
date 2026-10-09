package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

const memoryLaneTestType = "xflow.sas.webscan-sink"

// memoryLaneSession registers a runner that can serve the lane node type, which
// is what makes the lane queues reachable for it: the claim walk resolves the
// lanes it visits from the registered capabilities and then filters by policy,
// exactly as the Redis directory does. The capacity is 2 so a case can attempt
// a second claim and reach the queue walk: with a single slot the headroom gate
// would answer first, and the case would no longer exercise the queue-level
// fences it means to.
func memoryLaneSession(t *testing.T, ctx context.Context, dir *MemoryRunnerDirectory, runnerID string, laneType string) RunnerSession {
	t.Helper()

	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     runnerID,
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: laneType}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
		Now:          time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register(%q) error = %v", runnerID, err)
	}
	return session
}

func memoryLaneClaimRequest(session RunnerSession, laneType string) ClaimRequest {
	return ClaimRequest{
		RunnerID:     session.RunnerID,
		SessionID:    session.SessionID,
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: laneType}},
		Now:          time.Unix(11, 0),
	}
}

func memoryLaneAssignment(id AssignmentID, laneType string) Assignment {
	assignment := testAssignment(id)
	assignment.Routing.NodeType = laneType
	return assignment
}

// memoryLanePlacement asserts how many copies of one assignment sit on each
// configured lane and on the legacy queue, and — when wantMarker is not nil —
// whether the marker records a placement and which one.
func memoryLanePlacement(t *testing.T, dir *MemoryRunnerDirectory, id AssignmentID, laneCopies map[string]int, legacyCopies int, wantMarker *string) {
	t.Helper()

	dir.mu.RLock()
	defer dir.mu.RUnlock()
	for _, lane := range dir.lanes {
		got := 0
		for _, assignment := range dir.laneQueues[lane] {
			if assignment.AssignmentID == id {
				got++
			}
		}
		if got != laneCopies[lane] {
			t.Fatalf("lane %q copies of %q = %d, want %d", lane, id, got, laneCopies[lane])
		}
	}
	got := 0
	for _, assignment := range dir.queue {
		if assignment.AssignmentID == id {
			got++
		}
	}
	if got != legacyCopies {
		t.Fatalf("legacy copies of %q = %d, want %d", id, got, legacyCopies)
	}
	if wantMarker == nil {
		return
	}
	marker, ok := dir.laneMarkers[id]
	if !ok {
		t.Fatalf("marker for %q is absent, want %q", id, *wantMarker)
	}
	if marker != *wantMarker {
		t.Fatalf("marker for %q = %q, want %q", id, marker, *wantMarker)
	}
}

// TestMemoryDirectoryPlacesEnqueuedWorkAcrossLanes is the in-memory half of the
// placement matrix: the same lanes, the same modes and the same node types the
// Redis enqueue matrix covers, resolved through the same function.
func TestMemoryDirectoryPlacesEnqueuedWorkAcrossLanes(t *testing.T) {
	laneType := memoryLaneTestType
	lanes := []string{laneType}
	legacyMarker := ""
	laneMarker := laneType
	cases := []struct {
		name          string
		lanes         []string
		mode          LaneWriteMode
		nodeType      string
		laneCopies    int
		legacyCopies  int
		wantMarker    *string
		wantNoMarkers bool
	}{
		{"no lanes stays on legacy and writes no marker", nil, "", laneType, 0, 1, nil, true},
		{"dual writes both copies and the lane marker", lanes, LaneWriteDual, laneType, 1, 1, &laneMarker, false},
		{"dual keeps an unowned type on legacy and marks legacy", lanes, LaneWriteDual, "xflow.sas.sink", 0, 1, &legacyMarker, false},
		{"lane-only writes the lane copy and the lane marker", lanes, LaneWriteLaneOnly, laneType, 1, 0, &laneMarker, false},
		{"lane-only keeps an unowned type on legacy and marks legacy", lanes, LaneWriteLaneOnly, "xflow.sas.sink", 0, 1, &legacyMarker, false},
		{"legacy-only keeps a lane type on legacy and writes no marker", lanes, LaneWriteLegacyOnly, laneType, 0, 1, nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := NewMemoryRunnerDirectory(
				WithMemoryRunnerDirectoryLanes(tc.lanes),
				WithMemoryRunnerDirectoryLaneWriteMode(tc.mode),
			)
			assignment := memoryLaneAssignment(AssignmentID("exec-memory-lanes/enqueue/activation-1"), tc.nodeType)
			mustEnqueueAssignment(t, ctx, dir, assignment)

			memoryLanePlacement(t, dir, assignment.AssignmentID,
				map[string]int{laneType: tc.laneCopies}, tc.legacyCopies, tc.wantMarker)
			dir.mu.RLock()
			markerCount := len(dir.laneMarkers)
			dir.mu.RUnlock()
			if tc.wantNoMarkers && markerCount != 0 {
				t.Fatalf("lane markers = %d, want none for this mode", markerCount)
			}
		})
	}
}

// TestMemoryDirectoryDeduplicatesAgainstALiveLaneCopy pins the liveness scan:
// a seen assignment that is still waiting on its lane is live, so a repeated
// enqueue is a duplicate rather than a fresh dispatch. A lane-only placement is
// what exposes this — with a legacy copy present the legacy queue alone would
// answer the liveness question.
func TestMemoryDirectoryDeduplicatesAgainstALiveLaneCopy(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory(
		WithMemoryRunnerDirectoryLanes([]string{memoryLaneTestType}),
		WithMemoryRunnerDirectoryLaneWriteMode(LaneWriteLaneOnly),
	)
	assignment := memoryLaneAssignment(AssignmentID("exec-memory-lanes/dedup/activation-1"), memoryLaneTestType)
	if enqueued, err := dir.EnqueueAssignment(ctx, assignment); err != nil || !enqueued {
		t.Fatalf("first EnqueueAssignment() = %v, %v, want true, nil", enqueued, err)
	}
	if enqueued, err := dir.EnqueueAssignment(ctx, assignment); err != nil || enqueued {
		t.Fatalf("second EnqueueAssignment() = %v, %v, want a duplicate, not a re-dispatch", enqueued, err)
	}
	marker := memoryLaneTestType
	memoryLanePlacement(t, dir, assignment.AssignmentID, map[string]int{memoryLaneTestType: 1}, 0, &marker)
}

// TestMemoryDirectoryClaimsTheLaneBeforeTheLegacyBacklog pins the walk order:
// the lane is visited before the legacy queue, so an entry freshly placed on a
// lane is not stuck behind a legacy backlog the same runner could have claimed.
// The backlog entry is written while the directory is still legacy-only — the
// point of the lane is precisely to overtake that kind of residue — and the
// mode is then switched to dual, as the rolling upgrade does.
func TestMemoryDirectoryClaimsTheLaneBeforeTheLegacyBacklog(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory(
		WithMemoryRunnerDirectoryLanes([]string{memoryLaneTestType}),
		WithMemoryRunnerDirectoryLaneWriteMode(LaneWriteLegacyOnly),
	)
	backlog := memoryLaneAssignment(AssignmentID("exec-memory-lanes/order/backlog-1"), memoryLaneTestType)
	mustEnqueueAssignment(t, ctx, dir, backlog)

	dir.laneWriteMode = LaneWriteDual
	lane := memoryLaneAssignment(AssignmentID("exec-memory-lanes/order/lane-1"), memoryLaneTestType)
	mustEnqueueAssignment(t, ctx, dir, lane)

	session := memoryLaneSession(t, ctx, dir, "runner-lane-order", memoryLaneTestType)
	claim, ok, err := dir.ClaimForRunner(ctx, memoryLaneClaimRequest(session, memoryLaneTestType))
	if err != nil {
		t.Fatalf("ClaimForRunner() error = %v", err)
	}
	if !ok {
		t.Fatal("ClaimForRunner() ok=false, want the lane entry")
	}
	if claim.Assignment.AssignmentID != lane.AssignmentID {
		t.Fatalf("claim = %q, want the lane entry %q, not the legacy backlog %q",
			claim.Assignment.AssignmentID, lane.AssignmentID, backlog.AssignmentID)
	}
}

// TestMemoryDirectoryDoesNotHandOutBothCopiesOfADualWrite is the in-flight
// fence: a dual-written entry has two queue copies, and claiming one of them
// must not make the other claimable. The Redis directory gets this from the
// record's 'queued' state inside its claim transition.
func TestMemoryDirectoryDoesNotHandOutBothCopiesOfADualWrite(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory(
		WithMemoryRunnerDirectoryLanes([]string{memoryLaneTestType}),
		WithMemoryRunnerDirectoryLaneWriteMode(LaneWriteDual),
	)
	assignment := memoryLaneAssignment(AssignmentID("exec-memory-lanes/dual/activation-1"), memoryLaneTestType)
	mustEnqueueAssignment(t, ctx, dir, assignment)

	session := memoryLaneSession(t, ctx, dir, "runner-lane-dual", memoryLaneTestType)
	first, ok, err := dir.ClaimForRunner(ctx, memoryLaneClaimRequest(session, memoryLaneTestType))
	if err != nil {
		t.Fatalf("first ClaimForRunner() error = %v", err)
	}
	if !ok {
		t.Fatal("first ClaimForRunner() ok=false, want the assignment")
	}
	if first.Assignment.AssignmentID != assignment.AssignmentID {
		t.Fatalf("first claim = %q, want %q", first.Assignment.AssignmentID, assignment.AssignmentID)
	}
	second, ok, err := dir.ClaimForRunner(ctx, memoryLaneClaimRequest(session, memoryLaneTestType))
	if err != nil {
		t.Fatalf("second ClaimForRunner() error = %v", err)
	}
	if ok {
		t.Fatalf("second ClaimForRunner() handed out %q again, want the stale copy skipped",
			second.Assignment.AssignmentID)
	}
}

// TestMemoryDirectoryRequeueKeepsTheLanePlacement mirrors the Redis requeue
// contract: an entry that was written to a lane goes back to that lane (and, in
// dual mode, to the legacy queue as well) rather than falling back to legacy.
func TestMemoryDirectoryRequeueKeepsTheLanePlacement(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory(
		WithMemoryRunnerDirectoryLanes([]string{memoryLaneTestType}),
		WithMemoryRunnerDirectoryLaneWriteMode(LaneWriteDual),
	)
	assignment := memoryLaneAssignment(AssignmentID("exec-memory-lanes/requeue/activation-1"), memoryLaneTestType)
	mustEnqueueAssignment(t, ctx, dir, assignment)

	session := memoryLaneSession(t, ctx, dir, "runner-lane-requeue", memoryLaneTestType)
	claim, ok, err := dir.ClaimForRunner(ctx, memoryLaneClaimRequest(session, memoryLaneTestType))
	if err != nil || !ok {
		t.Fatalf("ClaimForRunner() ok=%v err=%v, want claim", ok, err)
	}
	// The claim took the lane copy; the legacy copy and the marker survive.
	marker := memoryLaneTestType
	memoryLanePlacement(t, dir, assignment.AssignmentID, map[string]int{memoryLaneTestType: 0}, 1, &marker)

	if err := dir.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimRequeue); err != nil {
		t.Fatalf("ReleaseClaim(requeue) error = %v", err)
	}
	memoryLanePlacement(t, dir, assignment.AssignmentID, map[string]int{memoryLaneTestType: 1}, 1, &marker)
}

// TestMemoryDirectoryTerminalReleaseDropsEveryLaneCopy is the in-memory half of
// F1: the terminal release ends the assignment, so every queue copy and the
// lane marker go with it.
func TestMemoryDirectoryTerminalReleaseDropsEveryLaneCopy(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory(
		WithMemoryRunnerDirectoryLanes([]string{memoryLaneTestType}),
		WithMemoryRunnerDirectoryLaneWriteMode(LaneWriteDual),
	)
	assignment := memoryLaneAssignment(AssignmentID("exec-memory-lanes/terminal/activation-1"), memoryLaneTestType)
	mustEnqueueAssignment(t, ctx, dir, assignment)

	session := memoryLaneSession(t, ctx, dir, "runner-lane-terminal", memoryLaneTestType)
	claim, ok, err := dir.ClaimForRunner(ctx, memoryLaneClaimRequest(session, memoryLaneTestType))
	if err != nil || !ok {
		t.Fatalf("ClaimForRunner() ok=%v err=%v, want claim", ok, err)
	}
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, &engine.TaskLease{LeaseID: "lease-lane-terminal", LeaseToken: "token-lane-terminal"}); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}
	if err := dir.ReleaseLeased(ctx, ReleaseLeasedRequest{
		RunnerID:     session.RunnerID,
		SessionID:    session.SessionID,
		AssignmentID: assignment.AssignmentID,
		LeaseID:      "lease-lane-terminal",
		LeaseToken:   "token-lane-terminal",
		RemoveSeen:   true,
	}); err != nil {
		t.Fatalf("ReleaseLeased(RemoveSeen=true) error = %v", err)
	}
	memoryLanePlacement(t, dir, assignment.AssignmentID, map[string]int{memoryLaneTestType: 0}, 0, nil)
	dir.mu.RLock()
	_, markerPresent := dir.laneMarkers[assignment.AssignmentID]
	_, seen := dir.seen[assignment.AssignmentID]
	dir.mu.RUnlock()
	if markerPresent {
		t.Fatal("lane marker survived the terminal release")
	}
	if seen {
		t.Fatal("seen mark survived the terminal release")
	}
}

// TestMemoryDirectoryReleaseExpiredLeaseDropsEveryLaneCopy is the other half of
// the terminal cleanup: a stale finalized lease being released before engine
// reclaim must not leave queue copies behind either.
func TestMemoryDirectoryReleaseExpiredLeaseDropsEveryLaneCopy(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory(
		WithMemoryRunnerDirectoryLanes([]string{memoryLaneTestType}),
		WithMemoryRunnerDirectoryLaneWriteMode(LaneWriteDual),
	)
	assignment := memoryLaneAssignment(AssignmentID("exec-memory-lanes/expired/activation-1"), memoryLaneTestType)
	mustEnqueueAssignment(t, ctx, dir, assignment)

	session := memoryLaneSession(t, ctx, dir, "runner-lane-expired", memoryLaneTestType)
	claim, ok, err := dir.ClaimForRunner(ctx, memoryLaneClaimRequest(session, memoryLaneTestType))
	if err != nil || !ok {
		t.Fatalf("ClaimForRunner() ok=%v err=%v, want claim", ok, err)
	}
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, &engine.TaskLease{LeaseID: "lease-lane-expired", LeaseToken: "token-lane-expired"}); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}
	outcome, err := dir.ReleaseExpiredLease(ctx, ExpiredDirectoryLeaseRequest{
		AssignmentID: assignment.AssignmentID,
		LeaseID:      "lease-lane-expired",
		LeaseToken:   "token-lane-expired",
	})
	if err != nil {
		t.Fatalf("ReleaseExpiredLease() error = %v", err)
	}
	if outcome != ExpiredDirectoryLeaseReleased {
		t.Fatalf("ReleaseExpiredLease() = %v, want released", outcome)
	}
	memoryLanePlacement(t, dir, assignment.AssignmentID, map[string]int{memoryLaneTestType: 0}, 0, nil)
}

// TestMemoryDirectoryClearAssignmentDropsEveryLaneCopy pins the remaining drop
// path.
func TestMemoryDirectoryClearAssignmentDropsEveryLaneCopy(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory(
		WithMemoryRunnerDirectoryLanes([]string{memoryLaneTestType}),
		WithMemoryRunnerDirectoryLaneWriteMode(LaneWriteDual),
	)
	assignment := memoryLaneAssignment(AssignmentID("exec-memory-lanes/clear/activation-1"), memoryLaneTestType)
	mustEnqueueAssignment(t, ctx, dir, assignment)

	if err := dir.ClearAssignment(ctx, assignment.AssignmentID); err != nil {
		t.Fatalf("ClearAssignment() error = %v", err)
	}
	memoryLanePlacement(t, dir, assignment.AssignmentID, map[string]int{memoryLaneTestType: 0}, 0, nil)
}

// TestMemoryDirectoryReportsAssignmentQueueDepths mirrors the Redis depth
// reader: the dual-written lane entry counts once on its lane and once on
// legacy, and a configured lane that was never written reports a present zero.
func TestMemoryDirectoryReportsAssignmentQueueDepths(t *testing.T) {
	idleLaneType := "xflow.sas.ulp-result"
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory(
		WithMemoryRunnerDirectoryLanes([]string{memoryLaneTestType, idleLaneType}),
		WithMemoryRunnerDirectoryLaneWriteMode(LaneWriteDual),
	)
	mustEnqueueAssignment(t, ctx, dir,
		memoryLaneAssignment(AssignmentID("exec-memory-lanes/depth/on-lane"), memoryLaneTestType))
	mustEnqueueAssignment(t, ctx, dir,
		testAssignment(AssignmentID("exec-memory-lanes/depth/legacy-a")))

	depths, err := dir.AssignmentQueueDepths(ctx)
	if err != nil {
		t.Fatalf("AssignmentQueueDepths() error = %v", err)
	}
	want := map[string]int64{
		QueueLaneLegacy:    2,
		memoryLaneTestType: 1,
		idleLaneType:       0,
	}
	if len(depths) != len(want) {
		t.Fatalf("depths = %v, want %v", depths, want)
	}
	for lane, depth := range want {
		if depths[lane] != depth {
			t.Fatalf("depths[%q] = %d, want %d (all: %v)", lane, depths[lane], depth, depths)
		}
	}
}

func mustMemoryLaneClaim(t *testing.T, ctx context.Context, dir *MemoryRunnerDirectory, session RunnerSession, laneType string) Claim {
	t.Helper()

	claim, ok, err := dir.ClaimForRunner(ctx, memoryLaneClaimRequest(session, laneType))
	if err != nil || !ok {
		t.Fatalf("ClaimForRunner() ok=%v err=%v, want claim", ok, err)
	}
	return claim
}

// TestMemoryDirectoryReregisterRequeuesEachClaimExactlyOnce pins the register
// requeue against the one defect a batch prepend cannot avoid: with lanes in
// play each requeued entry must land exactly once, on the queue its own marker
// names. A leftover batch prepend in rebindHandoffsLocked would leave a second
// legacy copy of every requeued claim behind — and with no lanes configured the
// same residue would break the "empty configuration behaves as today" gate.
func TestMemoryDirectoryReregisterRequeuesEachClaimExactlyOnce(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory(
		WithMemoryRunnerDirectoryLanes([]string{memoryLaneTestType}),
		WithMemoryRunnerDirectoryLaneWriteMode(LaneWriteDual),
	)
	session := memoryLaneSession(t, ctx, dir, "runner-lane-rebind", memoryLaneTestType)

	first := memoryLaneAssignment(AssignmentID("exec-memory-lanes/rebind/first"), memoryLaneTestType)
	second := memoryLaneAssignment(AssignmentID("exec-memory-lanes/rebind/second"), memoryLaneTestType)
	mustEnqueueAssignment(t, ctx, dir, first)
	mustEnqueueAssignment(t, ctx, dir, second)

	firstClaim := mustMemoryLaneClaim(t, ctx, dir, session, memoryLaneTestType)
	secondClaim := mustMemoryLaneClaim(t, ctx, dir, session, memoryLaneTestType)
	claimed := map[AssignmentID]bool{
		firstClaim.Assignment.AssignmentID:  true,
		secondClaim.Assignment.AssignmentID: true,
	}
	if !claimed[first.AssignmentID] || !claimed[second.AssignmentID] {
		t.Fatalf("claims = %v, want one claim each for %q and %q", claimed, first.AssignmentID, second.AssignmentID)
	}

	// The session replacement requeues both active claims; each must come back
	// as exactly one copy per placement queue and nothing else.
	next := memoryLaneSession(t, ctx, dir, "runner-lane-rebind", memoryLaneTestType)
	if next.SessionID == session.SessionID {
		t.Fatalf("sessions should differ, both %q", session.SessionID)
	}
	marker := memoryLaneTestType
	for _, assignment := range []Assignment{first, second} {
		memoryLanePlacement(t, dir, assignment.AssignmentID, map[string]int{memoryLaneTestType: 1}, 1, &marker)
	}
}

// TestMemoryDirectoryStaleTokenReleaseFencesTheQueueResidue is the stale-token
// parity case: ReleaseLeased(RemoveSeen=false) leaves the queue residue and the
// seen mark behind for the caller's re-enqueue — the Redis transition writes
// 'released' for exactly this combination — but the residue must not be handed
// to a runner in the meantime ('retry'), and EnqueueAssignment must still admit
// the re-dispatch. Claiming the residue, or rejecting the re-enqueue, both
// strand the task forever.
func TestMemoryDirectoryStaleTokenReleaseFencesTheQueueResidue(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory(
		WithMemoryRunnerDirectoryLanes([]string{memoryLaneTestType}),
		WithMemoryRunnerDirectoryLaneWriteMode(LaneWriteDual),
	)
	assignment := memoryLaneAssignment(AssignmentID("exec-memory-lanes/released/activation-1"), memoryLaneTestType)
	mustEnqueueAssignment(t, ctx, dir, assignment)

	session := memoryLaneSession(t, ctx, dir, "runner-lane-released", memoryLaneTestType)
	claim := mustMemoryLaneClaim(t, ctx, dir, session, memoryLaneTestType)
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, &engine.TaskLease{LeaseID: "lease-lane-released", LeaseToken: "token-lane-released"}); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}
	if err := dir.ReleaseLeased(ctx, ReleaseLeasedRequest{
		RunnerID:     session.RunnerID,
		SessionID:    session.SessionID,
		AssignmentID: assignment.AssignmentID,
		LeaseID:      "lease-lane-released",
		LeaseToken:   "token-lane-released",
		RemoveSeen:   false,
	}); err != nil {
		t.Fatalf("ReleaseLeased(RemoveSeen=false) error = %v", err)
	}

	dir.mu.RLock()
	_, released := dir.released[assignment.AssignmentID]
	dir.mu.RUnlock()
	if !released {
		t.Fatal("stale-token release did not record the released state")
	}

	// The claim took the lane copy; the legacy residue stays visible to queue
	// depth (the Redis LLEN sees it too) but must not be claimable.
	depths, err := dir.AssignmentQueueDepths(ctx)
	if err != nil {
		t.Fatalf("AssignmentQueueDepths() error = %v", err)
	}
	if depths[QueueLaneLegacy] != 1 {
		t.Fatalf("legacy depth = %d, want the released residue counted (1)", depths[QueueLaneLegacy])
	}
	if held, ok, err := dir.ClaimForRunner(ctx, memoryLaneClaimRequest(session, memoryLaneTestType)); err != nil {
		t.Fatalf("ClaimForRunner() error = %v", err)
	} else if ok {
		t.Fatalf("ClaimForRunner() handed out the released residue %q, want a retry-style skip", held.Assignment.AssignmentID)
	}

	if enqueued, err := dir.EnqueueAssignment(ctx, assignment); err != nil {
		t.Fatalf("EnqueueAssignment() error = %v", err)
	} else if !enqueued {
		t.Fatal("EnqueueAssignment() enqueued=false; the released state must admit the re-dispatch")
	}
	marker := memoryLaneTestType
	memoryLanePlacement(t, dir, assignment.AssignmentID, map[string]int{memoryLaneTestType: 1}, 1, &marker)
	dir.mu.RLock()
	_, released = dir.released[assignment.AssignmentID]
	dir.mu.RUnlock()
	if released {
		t.Fatal("released state survived the re-dispatch")
	}

	healed := mustMemoryLaneClaim(t, ctx, dir, session, memoryLaneTestType)
	if healed.Assignment.AssignmentID != assignment.AssignmentID {
		t.Fatalf("claim = %q, want the re-dispatched assignment claimable again", healed.Assignment.AssignmentID)
	}
}
