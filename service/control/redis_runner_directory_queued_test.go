package control

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/providers/distributed"
	backendlocal "github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// queuedReapProbe records one execution-liveness question the reaper asked,
// with the namespace it asked it under: the reaper must resolve the execution
// in the assignment's own namespace or it reads a different execution than the
// claim path does.
type queuedReapProbe struct {
	executionID types.ExecutionID
	namespace   namespace.Namespace
}

// fakeExecutionStatusReader is the engine.ExecutionStatusReader seam the reaper
// decides through. An execution absent from statuses is one whose transient
// keys have expired, which is the shape being reclaimed.
type fakeExecutionStatusReader struct {
	mu       sync.Mutex
	statuses map[types.ExecutionID]types.ExecutionStatus
	err      error
	probes   []queuedReapProbe
}

func newFakeExecutionStatusReader() *fakeExecutionStatusReader {
	return &fakeExecutionStatusReader{statuses: make(map[types.ExecutionID]types.ExecutionStatus)}
}

func (f *fakeExecutionStatusReader) set(id types.ExecutionID, status types.ExecutionStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statuses[id] = status
}

func (f *fakeExecutionStatusReader) GetExecutionStatus(ctx context.Context, id types.ExecutionID) (types.ExecutionStatus, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.probes = append(f.probes, queuedReapProbe{executionID: id, namespace: namespace.FromContext(ctx)})
	if f.err != nil {
		return "", false, f.err
	}
	status, ok := f.statuses[id]
	return status, ok, nil
}

func (f *fakeExecutionStatusReader) probed() []queuedReapProbe {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]queuedReapProbe(nil), f.probes...)
}

// queuedReapTestAssignment builds a queued assignment for one execution.
// redisDirectoryTestAssignment pins every assignment to the same execution,
// which cannot express "this one's execution is gone and that one's is not".
func queuedReapTestAssignment(id AssignmentID, executionID types.ExecutionID, namespaces ...namespace.Namespace) Assignment {
	assignment := redisDirectoryTestAssignment(id, namespaces...)
	assignment.Task.ExecutionID = executionID
	return assignment
}

// newQueuedReapDirectory builds a real RedisRunnerDirectory over miniredis, with
// the liveness probe installed unless reader is nil.
func newQueuedReapDirectory(t *testing.T, reader engine.ExecutionStatusReader) (*miniredis.Miniredis, *RedisRunnerDirectory, *redis.Client) {
	t.Helper()

	server, rdb := newRedisRunnerDirectoryTestClient(t)
	opts := []RedisRunnerDirectoryOption{WithRedisRunnerDirectoryClaimTTL(time.Minute)}
	if reader != nil {
		opts = append(opts, WithRedisRunnerDirectoryExecutionStatus(reader))
	}
	return server, NewRedisRunnerDirectory(rdb, opts...), rdb
}

// assertQueuedReapRemovedAssignment checks that the whole assignment record is
// gone: the queue entry, the state and payload fields, the dedupe marker, and
// both lease-metadata shapes.
func assertQueuedReapRemovedAssignment(t *testing.T, ctx context.Context, server *miniredis.Miniredis, rdb *redis.Client, directory *RedisRunnerDirectory, assignmentID AssignmentID) {
	t.Helper()

	id := string(assignmentID)
	if got := server.HGet(directory.keys.assignmentState, id); got != "" {
		t.Fatalf("assignment state = %q, want the record removed", got)
	}
	if got := server.HGet(directory.keys.assignmentData, id); got != "" {
		t.Fatalf("assignment data = %q, want the record removed", got)
	}
	if member, err := server.SIsMember(directory.keys.seen, id); err != nil {
		t.Fatalf("read seen set: %v", err)
	} else if member {
		t.Fatalf("seen still holds %q after the reap", id)
	}
	if got := server.HGet(directory.keys.assignmentLeaseMetaLegacy, id); got != "" {
		t.Fatalf("legacy lease metadata = %q, want the field dropped", got)
	}
	if exists, err := rdb.Exists(ctx, directory.keys.assignmentLeaseMetaKey(id)).Result(); err != nil {
		t.Fatalf("read lease metadata: %v", err)
	} else if exists != 0 {
		t.Fatalf("lease metadata key for %q survived the reap", id)
	}
	queue, err := rdb.LRange(ctx, directory.keys.queue, 0, -1).Result()
	if err != nil {
		t.Fatalf("read queue: %v", err)
	}
	for _, member := range queue {
		if member == id {
			t.Fatalf("queue still holds %q after the reap: %v", id, queue)
		}
	}
}

// TestRedisRunnerDirectoryReapsQueuedAssignmentWithGoneExecution is the primary
// path: the assignment is queued, its execution's status key is absent (the
// transient TTL expired), and the reaper removes the entire record while leaving
// the live queued assignment beside it alone.
func TestRedisRunnerDirectoryReapsQueuedAssignmentWithGoneExecution(t *testing.T) {
	ctx := context.Background()
	reader := newFakeExecutionStatusReader()
	server, directory, rdb := newQueuedReapDirectory(t, reader)

	dead := queuedReapTestAssignment("exec-dead/node/activation-1", "exec-dead")
	live := queuedReapTestAssignment("exec-live/node/activation-1", "exec-live")
	reader.set("exec-live", types.ExecutionStatusRunning)
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, dead)
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, live)

	// The two per-assignment records a queued write can leave behind, seeded so
	// the transition's cleanup of them is asserted rather than assumed.
	if err := rdb.HSet(ctx, directory.keys.assignmentLeaseMetaLegacy, string(dead.AssignmentID), "legacy").Err(); err != nil {
		t.Fatalf("seed legacy lease metadata: %v", err)
	}
	if err := rdb.Set(ctx, directory.keys.assignmentLeaseMetaKey(string(dead.AssignmentID)), "stale", time.Minute).Err(); err != nil {
		t.Fatalf("seed lease metadata: %v", err)
	}

	reap, err := directory.ReapDeadQueuedAssignments(ctx, 16)
	if err != nil {
		t.Fatalf("ReapDeadQueuedAssignments() error = %v", err)
	}
	if reap.Released != 1 {
		t.Fatalf("reclaimed = %d, want 1", reap.Released)
	}
	// The candidate count is the entries of the shape this pass drains, not the
	// entries of the hash it scans: both assignments are 'queued' and were
	// examined, and the live one is examined and correctly left alone. That gap
	// between inspected and released is the pass reporting that it is not a
	// reaper of everything it looks at.
	if reap.Inspected != 2 {
		t.Fatalf("inspected = %d, want 2 (both queued assignments examined)", reap.Inspected)
	}

	assertQueuedReapRemovedAssignment(t, ctx, server, rdb, directory, dead.AssignmentID)

	if got := server.HGet(directory.keys.assignmentState, string(live.AssignmentID)); got != redisAssignmentQueued {
		t.Fatalf("live assignment state = %q, want %q untouched", got, redisAssignmentQueued)
	}
	if got := server.HGet(directory.keys.assignmentData, string(live.AssignmentID)); got == "" {
		t.Fatal("live assignment payload was removed")
	}
	queue, err := rdb.LRange(ctx, directory.keys.queue, 0, -1).Result()
	if err != nil {
		t.Fatalf("read queue: %v", err)
	}
	if len(queue) != 1 || queue[0] != string(live.AssignmentID) {
		t.Fatalf("queue = %v, want exactly the live assignment %q", queue, live.AssignmentID)
	}
}

// TestRedisRunnerDirectoryQueuedReapKeepsLiveExecution is the safety property
// the whole reaper rests on: a queued assignment whose execution is present and
// non-terminal is claimable work and must survive the pass untouched. Delete the
// activeness check and this turns red.
func TestRedisRunnerDirectoryQueuedReapKeepsLiveExecution(t *testing.T) {
	ctx := context.Background()
	reader := newFakeExecutionStatusReader()
	server, directory, rdb := newQueuedReapDirectory(t, reader)

	assignment := queuedReapTestAssignment("exec-running/node/activation-1", "exec-running")
	reader.set("exec-running", types.ExecutionStatusRunning)
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)

	reap, err := directory.ReapDeadQueuedAssignments(ctx, 16)
	if err != nil {
		t.Fatalf("ReapDeadQueuedAssignments() error = %v", err)
	}
	if reap.Released != 0 {
		t.Fatalf("reclaimed = %d, want 0: the execution is still running", reap.Released)
	}
	// Inspected 1 with released 0 is the divergence case for this pass: it found
	// a candidate of its shape and released nothing, which is a different reading
	// from a pass that inspected nothing at all (see the leased case below).
	if reap.Inspected != 1 {
		t.Fatalf("inspected = %d, want 1: the queued assignment is a candidate of this pass", reap.Inspected)
	}
	if got := server.HGet(directory.keys.assignmentState, string(assignment.AssignmentID)); got != redisAssignmentQueued {
		t.Fatalf("assignment state = %q, want %q", got, redisAssignmentQueued)
	}
	if got := server.HGet(directory.keys.assignmentData, string(assignment.AssignmentID)); got == "" {
		t.Fatal("the assignment payload was removed")
	}
	if length, err := rdb.LLen(ctx, directory.keys.queue).Result(); err != nil {
		t.Fatalf("read queue length: %v", err)
	} else if length != 1 {
		t.Fatalf("queue length = %d, want 1", length)
	}
}

// TestRedisRunnerDirectoryQueuedReapReclaimsTerminalExecution covers the other
// half of the engine's activeness predicate. A terminal execution is never
// deleted by its own TTL, but no lease can be built on it either, so its queued
// assignment is just as immortal as one whose execution expired.
func TestRedisRunnerDirectoryQueuedReapReclaimsTerminalExecution(t *testing.T) {
	ctx := context.Background()

	for _, status := range []types.ExecutionStatus{
		types.ExecutionStatusSuccess,
		types.ExecutionStatusFailed,
		types.ExecutionStatusCanceled,
		types.ExecutionStatusTimeout,
	} {
		t.Run(string(status), func(t *testing.T) {
			reader := newFakeExecutionStatusReader()
			server, directory, _ := newQueuedReapDirectory(t, reader)

			executionID := types.ExecutionID("exec-terminal-" + string(status))
			reader.set(executionID, status)
			assignment := queuedReapTestAssignment(AssignmentID(string(executionID)+"/node/activation-1"), executionID)
			mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)

			reap, err := directory.ReapDeadQueuedAssignments(ctx, 16)
			if err != nil {
				t.Fatalf("ReapDeadQueuedAssignments() error = %v", err)
			}
			if reap.Released != 1 {
				t.Fatalf("reclaimed for a %q execution = %d, want 1", status, reap.Released)
			}
			if got := server.HGet(directory.keys.assignmentState, string(assignment.AssignmentID)); got != "" {
				t.Fatalf("assignment state = %q, want the record removed", got)
			}
		})
	}
}

// TestRedisRunnerDirectoryQueuedReapLeavesLeasedAssignmentAlone protects the
// v0.0.12 stranded-lease fix from this new path: a 'leased' assignment is
// capacity in use, and removing it would both lose the lease and hide the
// capacity leak the stranded reaper exists to repair.
func TestRedisRunnerDirectoryQueuedReapLeavesLeasedAssignmentAlone(t *testing.T) {
	ctx := context.Background()
	reader := newFakeExecutionStatusReader()
	server, directory, rdb := newQueuedReapDirectory(t, reader)

	const runnerID = "runner-leased-queued-reap"
	session := registerRedisDirectoryRunner(t, ctx, directory, runnerID, 1)
	assignment := queuedReapTestAssignment("exec-leased/node/activation-1", "exec-leased")
	lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-leased", time.Minute)
	finalizeRedisRunnerDirectoryLeaseMetaTestAssignment(t, ctx, directory, session, assignment, lease)
	assignmentID := string(assignment.AssignmentID)

	// The execution was never registered, so only the state fence keeps this
	// assignment out of the reaper's reach.
	reap, err := directory.ReapDeadQueuedAssignments(ctx, 16)
	if err != nil {
		t.Fatalf("ReapDeadQueuedAssignments() error = %v", err)
	}
	if reap.Released != 0 {
		t.Fatalf("reclaimed = %d, want 0: a leased assignment is not this reaper's to remove", reap.Released)
	}
	// Inspected 0 because a 'leased' entry is not a candidate of this pass's
	// shape. Counting every entry of the state hash would make inspected a
	// measure of how many assignments the directory holds, and the ratio against
	// released a measure of how few of them are dead — neither of which is what
	// an operator needs to read from a scope counter.
	if reap.Inspected != 0 {
		t.Fatalf("inspected = %d, want 0: a leased assignment is not a queued candidate", reap.Inspected)
	}
	if got := server.HGet(directory.keys.assignmentState, assignmentID); got != redisAssignmentLeased {
		t.Fatalf("assignment state = %q, want %q untouched", got, redisAssignmentLeased)
	}
	if got := server.HGet(directory.keys.assignmentData, assignmentID); got == "" {
		t.Fatal("the leased assignment payload was removed")
	}
	if exists, err := rdb.Exists(ctx, directory.keys.assignmentLeaseMetaKey(assignmentID)).Result(); err != nil {
		t.Fatalf("read lease metadata: %v", err)
	} else if exists == 0 {
		t.Fatal("the leased assignment's lease metadata was removed")
	}
	assertStrandedReapLeftLeaseCount(t, server, directory, runnerID, "1")
}

// TestRedisRunnerDirectoryQueuedReapLeavesClaimedAssignmentAlone covers the
// window between a claim and its finalization. The assignment has left 'queued'
// but still holds a runner's capacity, and the claim record is the only thing
// that can return it.
func TestRedisRunnerDirectoryQueuedReapLeavesClaimedAssignmentAlone(t *testing.T) {
	ctx := context.Background()
	reader := newFakeExecutionStatusReader()
	server, directory, _ := newQueuedReapDirectory(t, reader)

	const runnerID = "runner-claimed-queued-reap"
	session := registerRedisDirectoryRunner(t, ctx, directory, runnerID, 1)
	assignment := queuedReapTestAssignment("exec-claimed/node/activation-1", "exec-claimed")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	assignmentID := string(assignment.AssignmentID)

	reap, err := directory.ReapDeadQueuedAssignments(ctx, 16)
	if err != nil {
		t.Fatalf("ReapDeadQueuedAssignments() error = %v", err)
	}
	if reap.Released != 0 {
		t.Fatalf("reclaimed = %d, want 0: a claimed assignment is not this reaper's to remove", reap.Released)
	}
	if got := server.HGet(directory.keys.assignmentState, assignmentID); got != redisAssignmentClaimed {
		t.Fatalf("assignment state = %q, want %q untouched", got, redisAssignmentClaimed)
	}
	if got := server.HGet(directory.keys.assignmentClaim, assignmentID); got != string(claim.ClaimID) {
		t.Fatalf("assignment claim = %q, want %q: the claim record was destroyed", got, claim.ClaimID)
	}
}

// TestRedisRunnerDirectoryQueuedReapProbesAssignmentNamespace pins that the
// liveness probe is asked in the assignment's own namespace. Asking in the
// context's namespace — "default" on the sweeper's context — would read a
// different key for every non-default tenant and report a live execution as
// gone.
func TestRedisRunnerDirectoryQueuedReapProbesAssignmentNamespace(t *testing.T) {
	ctx := context.Background()
	reader := newFakeExecutionStatusReader()
	_, directory, _ := newQueuedReapDirectory(t, reader)

	const tenant = namespace.Namespace("tenant-a")
	assignment := queuedReapTestAssignment("exec-tenant/node/activation-1", "exec-tenant", tenant)
	reader.set("exec-tenant", types.ExecutionStatusRunning)
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)

	if _, err := directory.ReapDeadQueuedAssignments(ctx, 16); err != nil {
		t.Fatalf("ReapDeadQueuedAssignments() error = %v", err)
	}
	probes := reader.probed()
	if len(probes) != 1 {
		t.Fatalf("probes = %v, want exactly one", probes)
	}
	if probes[0].executionID != "exec-tenant" {
		t.Fatalf("probe execution = %q, want %q", probes[0].executionID, "exec-tenant")
	}
	if probes[0].namespace != tenant {
		t.Fatalf("probe namespace = %q, want %q: the probe must read the assignment's own key",
			probes[0].namespace, tenant)
	}
}

// TestRedisRunnerDirectoryQueuedReapWithoutProbeIsNoOp keeps every directory
// built without a state store out of this path entirely. Guessing "gone" from
// key names or from age would remove live work, so a missing probe has to be a
// no-op rather than a default.
func TestRedisRunnerDirectoryQueuedReapWithoutProbeIsNoOp(t *testing.T) {
	ctx := context.Background()
	server, directory, _ := newQueuedReapDirectory(t, nil)

	assignment := queuedReapTestAssignment("exec-noprobe/node/activation-1", "exec-noprobe")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)

	reap, err := directory.ReapDeadQueuedAssignments(ctx, 16)
	if err != nil {
		t.Fatalf("ReapDeadQueuedAssignments() error = %v", err)
	}
	if reap.Released != 0 {
		t.Fatalf("reclaimed = %d, want 0 without a liveness probe", reap.Released)
	}
	if got := server.HGet(directory.keys.assignmentState, string(assignment.AssignmentID)); got != redisAssignmentQueued {
		t.Fatalf("assignment state = %q, want %q untouched", got, redisAssignmentQueued)
	}
}

// TestRedisRunnerDirectoryQueuedReapSkipsUnclassifiableRecord covers the two
// records the reaper cannot prove dead: one with no payload at all, and one whose
// payload does not decode. Both are unclaimable, but neither names an execution
// this reaper can check, so both are left for a human rather than removed.
func TestRedisRunnerDirectoryQueuedReapSkipsUnclassifiableRecord(t *testing.T) {
	ctx := context.Background()
	reader := newFakeExecutionStatusReader()
	server, directory, _ := newQueuedReapDirectory(t, reader)

	const payloadless = "exec-payloadless/node/activation-1"
	const undecodable = "exec-undecodable/node/activation-1"
	if err := directory.rdb.HSet(ctx, directory.keys.assignmentState, payloadless, redisAssignmentQueued).Err(); err != nil {
		t.Fatalf("seed payloadless record: %v", err)
	}
	if err := directory.rdb.HSet(ctx, directory.keys.assignmentState, undecodable, redisAssignmentQueued).Err(); err != nil {
		t.Fatalf("seed undecodable record: %v", err)
	}
	if err := directory.rdb.HSet(ctx, directory.keys.assignmentData, undecodable, "{not json").Err(); err != nil {
		t.Fatalf("seed undecodable payload: %v", err)
	}

	reap, err := directory.ReapDeadQueuedAssignments(ctx, 16)
	if err != nil {
		t.Fatalf("ReapDeadQueuedAssignments() error = %v", err)
	}
	if reap.Released != 0 {
		t.Fatalf("reclaimed = %d, want 0 for records this reaper cannot classify", reap.Released)
	}
	for _, id := range []string{payloadless, undecodable} {
		if got := server.HGet(directory.keys.assignmentState, id); got != redisAssignmentQueued {
			t.Fatalf("assignment state for %q = %q, want %q untouched", id, got, redisAssignmentQueued)
		}
	}
}

// TestRedisRunnerDirectoryQueuedReapFailurePropagates keeps a state-store
// failure visible to the caller instead of being read as "nothing to do". The
// sweeper logs and retries it on the next cadence; a silent zero would hide a
// reaper that never runs at all.
func TestRedisRunnerDirectoryQueuedReapFailurePropagates(t *testing.T) {
	ctx := context.Background()
	reader := newFakeExecutionStatusReader()
	reader.err = errors.New("state store unavailable")
	server, directory, rdb := newQueuedReapDirectory(t, reader)

	assignment := queuedReapTestAssignment("exec-err/node/activation-1", "exec-err")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)

	reap, err := directory.ReapDeadQueuedAssignments(ctx, 16)
	if err == nil {
		t.Fatal("ReapDeadQueuedAssignments() error = nil, want the probe failure")
	}
	if reap.Released != 0 {
		t.Fatalf("reclaimed = %d, want 0", reap.Released)
	}
	if got := server.HGet(directory.keys.assignmentState, string(assignment.AssignmentID)); got != redisAssignmentQueued {
		t.Fatalf("assignment state = %q, want %q untouched", got, redisAssignmentQueued)
	}
	if length, err := rdb.LLen(ctx, directory.keys.queue).Result(); err != nil {
		t.Fatalf("read queue length: %v", err)
	} else if length != 1 {
		t.Fatalf("queue length = %d, want 1", length)
	}
}

// TestRedisRunnerDirectoryQueuedReapIsBoundedAndIdempotent keeps the pass
// proportional to its limit and safe to repeat: the limit is honoured exactly,
// the backlog drains across calls, and a drained directory releases nothing.
func TestRedisRunnerDirectoryQueuedReapIsBoundedAndIdempotent(t *testing.T) {
	ctx := context.Background()
	reader := newFakeExecutionStatusReader()
	_, directory, rdb := newQueuedReapDirectory(t, reader)

	const backlog = 5
	for i := 0; i < backlog; i++ {
		executionID := types.ExecutionID("exec-backlog-" + strconv.Itoa(i))
		assignment := queuedReapTestAssignment(AssignmentID(string(executionID)+"/node/activation-1"), executionID)
		mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	}

	reap, err := directory.ReapDeadQueuedAssignments(ctx, 2)
	if err != nil {
		t.Fatalf("first ReapDeadQueuedAssignments() error = %v", err)
	}
	if reap.Released != 2 {
		t.Fatalf("first reclaimed = %d, want exactly the limit of 2", reap.Released)
	}
	if length, err := rdb.LLen(ctx, directory.keys.queue).Result(); err != nil {
		t.Fatalf("read queue length: %v", err)
	} else if length != backlog-2 {
		t.Fatalf("queue length after a bounded pass = %d, want %d", length, backlog-2)
	}

	reap, err = directory.ReapDeadQueuedAssignments(ctx, 16)
	if err != nil {
		t.Fatalf("second ReapDeadQueuedAssignments() error = %v", err)
	}
	if reap.Released != backlog-2 {
		t.Fatalf("second reclaimed = %d, want the remaining %d", reap.Released, backlog-2)
	}

	reap, err = directory.ReapDeadQueuedAssignments(ctx, 16)
	if err != nil {
		t.Fatalf("third ReapDeadQueuedAssignments() error = %v", err)
	}
	if reap.Released != 0 {
		t.Fatalf("third reclaimed = %d, want 0 on a drained directory", reap.Released)
	}

	if length, err := rdb.LLen(ctx, directory.keys.queue).Result(); err != nil {
		t.Fatalf("read queue length: %v", err)
	} else if length != 0 {
		t.Fatalf("queue length = %d, want the backlog fully drained", length)
	}
	if states, err := rdb.HGetAll(ctx, directory.keys.assignmentState).Result(); err != nil {
		t.Fatalf("read assignment states: %v", err)
	} else if len(states) != 0 {
		t.Fatalf("assignment states = %v, want none left", states)
	}
}

// TestSelectRunnerDirectoryInjectsExecutionStatusProbe is the wiring check. The
// probe is what makes the reaper possible at all, and exactly one production
// call site injects it; if that stops happening every test above still passes
// while production silently reclaims nothing.
func TestSelectRunnerDirectoryInjectsExecutionStatusProbe(t *testing.T) {
	provider := redisBackendStub{Provider: backendlocal.New(), rdb: newMiniRedis(t)}

	directory := selectRunnerDirectory(Config{Backend: provider}, nil)
	redisDirectory, ok := directory.(*RedisRunnerDirectory)
	if !ok {
		t.Fatalf("selectRunnerDirectory() = %T, want *RedisRunnerDirectory", directory)
	}
	if redisDirectory.executions == nil {
		t.Fatal("the Redis directory was built without an execution-liveness probe: " +
			"the queued-assignment reaper would be a silent no-op in production")
	}
	if _, ok := directory.(DeadQueuedAssignmentReaper); !ok {
		t.Fatal("the Redis directory does not implement DeadQueuedAssignmentReaper")
	}
}

// TestRedisRunnerDirectoryQueuedReapAgainstRealStateStore runs the reaper
// against the real distributed state store rather than the fake probe. The
// execution is seeded as the transient status key it actually is, and the
// assignment must survive while that key is present and be reclaimed once it is
// gone — the exact transition a transient execution's TTL performs. This is what
// pins the join between the two key spaces: the fake proves the reaper's
// decision, this proves the decision is taken on real state.
func TestRedisRunnerDirectoryQueuedReapAgainstRealStateStore(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	provider, err := distributed.New(server.Addr(), nil)
	if err != nil {
		t.Fatalf("distributed.New() error = %v", err)
	}
	// The same assertion selectRunnerDirectory makes: the real store must expose
	// the liveness reader, or the reaper is dead in production.
	reader, ok := provider.State().(engine.ExecutionStatusReader)
	if !ok {
		t.Fatalf("distributed state store %T does not implement engine.ExecutionStatusReader", provider.State())
	}
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryExecutionStatus(reader))

	const executionID = types.ExecutionID("exec-real-state")
	assignment := queuedReapTestAssignment("exec-real-state/node/activation-1", executionID)
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)

	// The exact key rstate writes for an execution's lifecycle status, with the
	// transient TTL left to the deployment.
	statusKey := "xflow:ns:" + string(namespace.Default) + ":exec:{" + string(executionID) + "}:status"
	if err := rdb.Set(ctx, statusKey, string(types.ExecutionStatusRunning), time.Minute).Err(); err != nil {
		t.Fatalf("seed execution status: %v", err)
	}

	reap, err := directory.ReapDeadQueuedAssignments(ctx, 16)
	if err != nil {
		t.Fatalf("ReapDeadQueuedAssignments() error = %v", err)
	}
	if reap.Released != 0 {
		t.Fatalf("reclaimed = %d, want 0 while the execution's status key exists", reap.Released)
	}

	// The execution's transient keys expiring is what leaves the queue entry
	// behind, so deleting the key is the deterministic form of that TTL.
	if err := rdb.Del(ctx, statusKey).Err(); err != nil {
		t.Fatalf("expire execution status: %v", err)
	}

	reap, err = directory.ReapDeadQueuedAssignments(ctx, 16)
	if err != nil {
		t.Fatalf("ReapDeadQueuedAssignments() after expiry error = %v", err)
	}
	if reap.Released != 1 {
		t.Fatalf("reclaimed = %d, want 1 once the execution is gone", reap.Released)
	}
	if got := server.HGet(directory.keys.assignmentState, string(assignment.AssignmentID)); got != "" {
		t.Fatalf("assignment state = %q, want the record removed", got)
	}
}

// TestRedisRunnerDirectoryQueuedReapIsSafeAcrossReplicas runs two directory
// instances — two control-plane replicas — against the same Redis and the same
// backlog. Every entry must be removed exactly once: a reap that lost the race
// sees the state fence decline it and reports nothing, which is what keeps the
// total equal to the backlog instead of double-counting a removal.
func TestRedisRunnerDirectoryQueuedReapIsSafeAcrossReplicas(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)

	const backlog = 32
	first := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryExecutionStatus(newFakeExecutionStatusReader()))
	second := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryExecutionStatus(newFakeExecutionStatusReader()))
	for i := 0; i < backlog; i++ {
		executionID := types.ExecutionID("exec-race-" + strconv.Itoa(i))
		assignment := queuedReapTestAssignment(AssignmentID(string(executionID)+"/node/activation-1"), executionID)
		mustEnqueueRedisDirectoryAssignment(t, ctx, first, assignment)
	}

	replicas := []*RedisRunnerDirectory{first, second}
	totals := make([]int, len(replicas))
	var wg sync.WaitGroup
	for i, replica := range replicas {
		wg.Add(1)
		go func(i int, replica *RedisRunnerDirectory) {
			defer wg.Done()
			for round := 0; round < 8; round++ {
				reap, err := replica.ReapDeadQueuedAssignments(ctx, 8)
				if err != nil {
					t.Errorf("replica %d: ReapDeadQueuedAssignments() error = %v", i, err)
					return
				}
				totals[i] += reap.Released
			}
		}(i, replica)
	}
	wg.Wait()

	sum := 0
	for _, total := range totals {
		sum += total
	}
	if sum != backlog {
		t.Fatalf("replicas reclaimed %v (total %d), want %d exactly once each", totals, sum, backlog)
	}
	if length, err := rdb.LLen(ctx, first.keys.queue).Result(); err != nil {
		t.Fatalf("read queue length: %v", err)
	} else if length != 0 {
		t.Fatalf("queue length = %d, want 0", length)
	}
	if states, err := rdb.HGetAll(ctx, first.keys.assignmentState).Result(); err != nil {
		t.Fatalf("read assignment states: %v", err)
	} else if len(states) != 0 {
		t.Fatalf("assignment states = %v, want none left", states)
	}
}

// TestRedisRunnerDirectoryQueuedReapRestoresTheClaimableHead reproduces the
// reported shape end to end: a backlog whose head is dead assignments and whose
// tail is the only live one. Once the reaper has run, a single poll claims the
// live assignment; before it runs the head is a dead assignment the claim path
// has no way to tell apart from live work — which is exactly why the dead
// entries had to stop being the poller's problem.
func TestRedisRunnerDirectoryQueuedReapRestoresTheClaimableHead(t *testing.T) {
	ctx := context.Background()
	reader := newFakeExecutionStatusReader()
	_, directory, rdb := newQueuedReapDirectory(t, reader)

	const deadCount = 9
	for i := 0; i < deadCount; i++ {
		executionID := types.ExecutionID("exec-clog-" + strconv.Itoa(i))
		assignment := queuedReapTestAssignment(AssignmentID(string(executionID)+"/node/activation-1"), executionID)
		mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	}
	live := queuedReapTestAssignment("exec-tail/node/activation-1", "exec-tail")
	reader.set("exec-tail", types.ExecutionStatusRunning)
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, live)

	head, err := rdb.LRange(ctx, directory.keys.queue, 0, 0).Result()
	if err != nil {
		t.Fatalf("read queue head: %v", err)
	}
	if len(head) != 1 || head[0] == string(live.AssignmentID) {
		t.Fatalf("queue head = %v, want a dead assignment ahead of %q", head, live.AssignmentID)
	}

	reap, err := directory.ReapDeadQueuedAssignments(ctx, 64)
	if err != nil {
		t.Fatalf("ReapDeadQueuedAssignments() error = %v", err)
	}
	if reap.Released != deadCount {
		t.Fatalf("reclaimed = %d, want all %d dead assignments", reap.Released, deadCount)
	}
	if queue, err := rdb.LRange(ctx, directory.keys.queue, 0, -1).Result(); err != nil {
		t.Fatalf("read queue: %v", err)
	} else if len(queue) != 1 || queue[0] != string(live.AssignmentID) {
		t.Fatalf("queue = %v, want exactly the live assignment %q", queue, live.AssignmentID)
	}

	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-tail", 1)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	if claim.Assignment.Task.ExecutionID != "exec-tail" {
		t.Fatalf("first poll claimed execution %q, want the live %q",
			claim.Assignment.Task.ExecutionID, "exec-tail")
	}
}
