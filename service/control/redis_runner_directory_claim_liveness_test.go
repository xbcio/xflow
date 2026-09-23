package control

import (
	"context"
	"fmt"
	"testing"

	"github.com/xbcio/xflow/types"
)

// batchRecordingExecutionStatusReader implements both the single read and the
// optional batch read, so a test can pin which one the caller chose.
//
// The reaper suite keeps its own single-read fake on purpose: that one exists to
// pin the fallback path for a reader that does not implement the batch form, and
// this one exists to pin the batch path. Folding them together would silently
// retire one of the two behaviours from coverage.
type batchRecordingExecutionStatusReader struct {
	statuses map[types.ExecutionID]types.ExecutionStatus
	batches  [][]types.ExecutionID
	singles  []types.ExecutionID
}

func newBatchRecordingExecutionStatusReader() *batchRecordingExecutionStatusReader {
	return &batchRecordingExecutionStatusReader{statuses: make(map[types.ExecutionID]types.ExecutionStatus)}
}

func (f *batchRecordingExecutionStatusReader) set(id types.ExecutionID, status types.ExecutionStatus) {
	f.statuses[id] = status
}

func (f *batchRecordingExecutionStatusReader) GetExecutionStatus(_ context.Context, id types.ExecutionID) (types.ExecutionStatus, bool, error) {
	f.singles = append(f.singles, id)
	status, ok := f.statuses[id]
	return status, ok, nil
}

func (f *batchRecordingExecutionStatusReader) GetExecutionStatuses(_ context.Context, ids []types.ExecutionID) ([]types.ExecutionStatus, error) {
	f.batches = append(f.batches, append([]types.ExecutionID(nil), ids...))
	out := make([]types.ExecutionStatus, len(ids))
	for i, id := range ids {
		out[i] = f.statuses[id]
	}
	return out, nil
}

// enqueueForExecution queues one assignment belonging to executionID.
func enqueueForExecution(t *testing.T, ctx context.Context, directory *RedisRunnerDirectory, executionID types.ExecutionID) AssignmentID {
	t.Helper()

	assignmentID := AssignmentID(fmt.Sprintf("%s/sink/5/0/0", executionID))
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, queuedReapTestAssignment(assignmentID, executionID))
	return assignmentID
}

// TestClaimForRunnerSkipsAssignmentsWhoseExecutionIsGone pins the defect this
// path had: the claim transition checks only that the assignment is queued and
// carries its payload, never that the execution it names still exists, so a
// queued record outliving its execution claims successfully.
//
// That mattered more than one wasted call. A materialized claim is a real lease
// over work that is gone, and the dispatch that follows finds nothing to run and
// walks the claim back — so on a queue whose head has outlived its executions,
// the claim path spent every poll on work that could not be executed and never
// reached the entries that could.
func TestClaimForRunnerSkipsAssignmentsWhoseExecutionIsGone(t *testing.T) {
	ctx := context.Background()
	reader := newBatchRecordingExecutionStatusReader()
	_, directory, _ := newQueuedReapDirectory(t, reader)
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-1", 4)

	const dead = 40 // inside one redisClaimQueuePage, so one poll sees all of it
	deadIDs := make([]AssignmentID, 0, dead)
	for i := 0; i < dead; i++ {
		deadIDs = append(deadIDs, enqueueForExecution(t, ctx, directory,
			types.ExecutionID(fmt.Sprintf("exec-gone-%d", i))))
	}
	reader.set("exec-live", types.ExecutionStatusRunning)
	liveID := enqueueForExecution(t, ctx, directory, "exec-live")

	claim, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 4))
	if err != nil {
		t.Fatalf("ClaimForRunner() error = %v", err)
	}
	if !ok {
		t.Fatal("ClaimForRunner() ok=false, want the live assignment behind the dead prefix")
	}
	if got := claim.Assignment.AssignmentID; got != liveID {
		t.Fatalf("ClaimForRunner() claimed %q, want only %q: a claim over an execution that no longer exists leases work that cannot run", got, liveID)
	}

	// The dead entries are skipped, not consumed. Removal is the reaper's job:
	// it owns the atomic transition that also clears the seen marker and the
	// claim maps, and a wrong removal here would lose work rather than defer it.
	for _, id := range deadIDs {
		state, err := directory.rdb.HGet(ctx, directory.keys.assignmentState, string(id)).Result()
		if err != nil {
			t.Fatalf("read state of %q: %v", id, err)
		}
		if state != redisAssignmentQueued {
			t.Fatalf("state of skipped %q = %q, want it left %q for the reaper", id, state, redisAssignmentQueued)
		}
	}

	if len(reader.batches) != 1 {
		t.Fatalf("batch status reads = %d, want 1 for the whole page", len(reader.batches))
	}
	if len(reader.singles) != 0 {
		t.Fatalf("single status reads = %v, want none when the batch form is available", reader.singles)
	}
	if got := len(reader.batches[0]); got != dead+1 {
		t.Fatalf("batch covered %d executions, want the whole candidate page (%d)", got, dead+1)
	}
}

// TestClaimForRunnerReachesLiveWorkBehindADeadPrefixLongerThanOnePage pins that
// skipping composes with paging. A page of dead assignments is not a claim, so
// the cursor must advance — without that, every poll re-walks the same dead
// prefix and the live entries behind it are unreachable no matter how many polls
// a runner makes.
func TestClaimForRunnerReachesLiveWorkBehindADeadPrefixLongerThanOnePage(t *testing.T) {
	ctx := context.Background()
	reader := newBatchRecordingExecutionStatusReader()
	_, directory, _ := newQueuedReapDirectory(t, reader)
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-1", 4)

	const dead = 150 // more than one redisClaimQueuePage
	for i := 0; i < dead; i++ {
		enqueueForExecution(t, ctx, directory, types.ExecutionID(fmt.Sprintf("exec-gone-%d", i)))
	}
	reader.set("exec-live", types.ExecutionStatusRunning)
	liveID := enqueueForExecution(t, ctx, directory, "exec-live")

	for poll := 0; poll < 10; poll++ {
		claim, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 4))
		if err != nil {
			t.Fatalf("ClaimForRunner() poll %d error = %v", poll, err)
		}
		if !ok {
			continue // a page of dead assignments reports no claim and advances the cursor
		}
		if got := claim.Assignment.AssignmentID; got != liveID {
			t.Fatalf("ClaimForRunner() poll %d claimed %q, want only %q", poll, got, liveID)
		}
		return
	}
	t.Fatalf("ClaimForRunner() never reached %q behind a %d-entry dead prefix", liveID, dead)
}

// TestReapDeadQueuedAssignmentsDrainsMoreThanOneLegacyPass pins the pass size
// against the arrival it has to clear.
//
// The predecessor value was calibrated for a queue growing by ~42 entries per
// minute; this deployment grows one by hundreds per minute, and the runner's claim
// path is no longer a drain — it skips what it cannot run, which is the correct
// division of labour but shifts the whole burden here. A pass smaller than the
// arrival rate lets the queue grow without bound, and a queue that grows without
// bound is one whose live tail a runner reaches later and later until it does not
// reach it at all.
func TestReapDeadQueuedAssignmentsDrainsMoreThanOneLegacyPass(t *testing.T) {
	ctx := context.Background()
	_, directory, _ := newQueuedReapDirectory(t, newBatchRecordingExecutionStatusReader())

	const legacyPassSize = 512
	const dead = legacyPassSize + 200
	for i := 0; i < dead; i++ {
		enqueueForExecution(t, ctx, directory, types.ExecutionID(fmt.Sprintf("exec-gone-%d", i)))
	}

	result, err := directory.ReapDeadQueuedAssignments(ctx, defaultDeadQueuedAssignmentReapBatch)
	if err != nil {
		t.Fatalf("ReapDeadQueuedAssignments() error = %v", err)
	}
	if result.Released <= legacyPassSize {
		t.Fatalf("released %d in one pass, want more than the legacy %d: a pass below the arrival rate grows the queue without bound",
			result.Released, legacyPassSize)
	}
	if result.Released != dead {
		t.Fatalf("released %d, want all %d dead entries", result.Released, dead)
	}
}

// TestClaimForRunnerKeepsItsCursorOnASuccessfulClaim pins that a claim no longer
// rewinds the resume position to the head of the queue.
//
// The cursor has to be somewhere other than zero for this to be observable, which
// is why it is reached by paging past a dead prefix first. Rewinding it sent every
// subsequent poll back over that same prefix — work the skipped-page path can step
// over in one read, but a cursor pinned at zero cannot.
func TestClaimForRunnerKeepsItsCursorOnASuccessfulClaim(t *testing.T) {
	ctx := context.Background()
	reader := newBatchRecordingExecutionStatusReader()
	_, directory, _ := newQueuedReapDirectory(t, reader)
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-1", 4)

	// One page of dead assignments, so the first poll takes the skipped-page path
	// and leaves the cursor at the start of the second page.
	for i := 0; i < redisClaimQueuePage; i++ {
		enqueueForExecution(t, ctx, directory, types.ExecutionID(fmt.Sprintf("exec-gone-%d", i)))
	}
	reader.set("exec-live", types.ExecutionStatusRunning)
	enqueueForExecution(t, ctx, directory, "exec-live")

	claim, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 4))
	if err != nil {
		t.Fatalf("ClaimForRunner() poll 1 error = %v", err)
	}
	if ok {
		t.Fatalf("ClaimForRunner() poll 1 claimed %q, want no claim from a page that is entirely dead", claim.Assignment.AssignmentID)
	}
	if got := directory.loadClaimCursor("runner-1"); got != redisClaimQueuePage {
		t.Fatalf("cursor after the skipped page = %d, want %d", got, redisClaimQueuePage)
	}

	claim, ok, err = directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 4))
	if err != nil {
		t.Fatalf("ClaimForRunner() poll 2 error = %v", err)
	}
	if !ok {
		t.Fatal("ClaimForRunner() poll 2 ok=false, want the live assignment on the second page")
	}
	if got := claim.Assignment.Task.ExecutionID; got != "exec-live" {
		t.Fatalf("ClaimForRunner() poll 2 claimed execution %q, want %q", got, "exec-live")
	}
	if got := directory.loadClaimCursor("runner-1"); got != redisClaimQueuePage {
		t.Fatalf("cursor after a successful claim = %d, want it left at %d rather than rewound to the head",
			got, redisClaimQueuePage)
	}
}

// TestReapDeadQueuedAssignmentsProbesThePageInOneCall pins the batched liveness
// probe. Asked per candidate, a full pass cost one sequential state-store read per
// candidate, which on a link whose round trip is tens of milliseconds is the
// entire cost of the pass — and it is spent almost entirely on entries that are
// dead, which is exactly the backlog the pass exists to drain.
func TestReapDeadQueuedAssignmentsProbesThePageInOneCall(t *testing.T) {
	ctx := context.Background()
	reader := newBatchRecordingExecutionStatusReader()
	_, directory, _ := newQueuedReapDirectory(t, reader)

	const queued = 8
	for i := 0; i < queued; i++ {
		enqueueForExecution(t, ctx, directory, types.ExecutionID(fmt.Sprintf("exec-gone-%d", i)))
	}

	result, err := directory.ReapDeadQueuedAssignments(ctx, 16)
	if err != nil {
		t.Fatalf("ReapDeadQueuedAssignments() error = %v", err)
	}
	if result.Released != queued {
		t.Fatalf("released %d assignments, want %d", result.Released, queued)
	}
	if len(reader.batches) != 1 {
		t.Fatalf("batch status reads = %d, want 1 for the whole candidate page", len(reader.batches))
	}
	if len(reader.singles) != 0 {
		t.Fatalf("single status reads = %v, want none when the batch form is available", reader.singles)
	}
}
