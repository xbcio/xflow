package control

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
)

// replayRoundTripHook counts client-server round trips: one per ordinary
// command and one per pipeline, whatever the pipeline carries. The replay path
// is the thing under test, so a pipeline that grows with the number of leases a
// runner holds must not grow its round-trip count.
type replayRoundTripHook struct {
	mu sync.Mutex
	n  int
}

func (h *replayRoundTripHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *replayRoundTripHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.add(1)
		return next(ctx, cmd)
	}
}

func (h *replayRoundTripHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.add(1)
		return next(ctx, cmds)
	}
}

func (h *replayRoundTripHook) add(n int) {
	h.mu.Lock()
	h.n += n
	h.mu.Unlock()
}

func (h *replayRoundTripHook) reset() {
	h.mu.Lock()
	h.n = 0
	h.mu.Unlock()
}

func (h *replayRoundTripHook) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.n
}

// replayLeasePollRoundTrips seeds live leases for one runner and measures the
// round trips one poll costs when the runner reports every one of them in
// flight and the queue is empty, so replayLease is the only part of the poll
// whose work scales with the lease count.
//
// The leases are written straight into the directory keys rather than produced
// through ClaimForRunner and FinalizeClaim: every finalized claim also leaves a
// handoff-debt entry whose own re-check is a separate, pre-existing read path.
// Writing the record the replay actually reads isolates the cost under test.
func replayLeasePollRoundTrips(t *testing.T, leases int) int {
	t.Helper()

	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	// Capacity leaves headroom for the queue probe once every seeded lease is
	// held, so both scenarios walk the same poll shape.
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-replay-rtt", leases+8)

	req := redisDirectoryClaimRequest(session, 1)
	for i := 0; i < leases; i++ {
		assignmentID := AssignmentID(fmt.Sprintf("exec-rtt/lease-%d/activation-1", i))
		assignment := redisDirectoryTestAssignment(assignmentID)
		rawAssignment, err := marshalRedisAssignment(assignment)
		if err != nil {
			t.Fatalf("marshalRedisAssignment(%q) error = %v", assignmentID, err)
		}
		lease := &engine.TaskLease{
			LeaseID:    engine.LeaseID(fmt.Sprintf("lease-%d", i)),
			LeaseToken: engine.LeaseToken(fmt.Sprintf("token-%d", i)),
			Task:       assignment.Task,
			NodeType:   assignment.Routing.NodeType,
			IssuedAt:   time.Unix(11, 0).UTC(),
			TTL:        time.Minute,
		}
		rawLease, err := marshalRedisLeaseMeta(lease)
		if err != nil {
			t.Fatalf("marshalRedisLeaseMeta(%q) error = %v", assignmentID, err)
		}
		id := string(assignmentID)
		if err := rdb.HSet(ctx, directory.keys.assignmentState, id, redisAssignmentLeased).Err(); err != nil {
			t.Fatalf("seed assignment state %q: %v", assignmentID, err)
		}
		if err := rdb.HSet(ctx, directory.keys.assignmentRunner, id, session.RunnerID).Err(); err != nil {
			t.Fatalf("seed assignment owner %q: %v", assignmentID, err)
		}
		if err := rdb.HSet(ctx, directory.keys.assignmentSession, id, session.SessionID).Err(); err != nil {
			t.Fatalf("seed assignment session %q: %v", assignmentID, err)
		}
		if err := rdb.HSet(ctx, directory.keys.assignmentData, id, rawAssignment).Err(); err != nil {
			t.Fatalf("seed assignment payload %q: %v", assignmentID, err)
		}
		if err := rdb.Set(ctx, directory.keys.assignmentLeaseMetaKey(id), rawLease, time.Minute).Err(); err != nil {
			t.Fatalf("seed assignment lease metadata %q: %v", assignmentID, err)
		}
		if err := rdb.SAdd(ctx, directory.keys.runnerLeasedAssignmentsKey(session.RunnerID), id).Err(); err != nil {
			t.Fatalf("seed assignment index %q: %v", assignmentID, err)
		}
		if err := rdb.HIncrBy(ctx, directory.keys.runnerLeaseCount, session.RunnerID, 1).Err(); err != nil {
			t.Fatalf("seed runner lease count %q: %v", assignmentID, err)
		}
		req.ActiveLeaseIDs = append(req.ActiveLeaseIDs, string(lease.LeaseID))
	}

	// The first poll warms the server-side script cache so the measured poll
	// does not pay a NOSCRIPT fallback; it must not change any state.
	if _, ok, err := directory.ClaimForRunner(ctx, req); err != nil || ok {
		t.Fatalf("warm-up ClaimForRunner() ok=%v err=%v, want no claim", ok, err)
	}

	hook := &replayRoundTripHook{}
	rdb.AddHook(hook)
	hook.reset()
	if _, ok, err := directory.ClaimForRunner(ctx, req); err != nil || ok {
		t.Fatalf("measured ClaimForRunner() ok=%v err=%v, want no claim", ok, err)
	}
	return hook.count()
}

// TestRedisRunnerDirectoryReplayLeaseReadsInConstantRoundTrips pins the shape
// of the replay read path: a poll whose leases are all in flight costs the same
// number of round trips whether the runner holds one lease or eight, and the
// whole poll stays within a small absolute bound. The old shape issued five
// serial reads per candidate, so the same fixture grew by 35 round trips.
func TestRedisRunnerDirectoryReplayLeaseReadsInConstantRoundTrips(t *testing.T) {
	const absoluteBound = 6 // script, registration, index, replay batch, queue probe, empty transition
	one := replayLeasePollRoundTrips(t, 1)
	eight := replayLeasePollRoundTrips(t, 8)
	if one != eight {
		t.Fatalf("poll round trips = %d with 1 lease and %d with 8 leases; the replay read path must not grow with the lease count", one, eight)
	}
	if one > absoluteBound {
		t.Fatalf("poll with 1 lease cost %d round trips, want at most %d", one, absoluteBound)
	}
}

// replayReadErrorHook fails the pipeline read of one hash key after letting the
// pipeline run, mirroring a connection or server error on that read.
type replayReadErrorHook struct {
	key string
	err error
}

func (h replayReadErrorHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h replayReadErrorHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return next
}

func (h replayReadErrorHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		err := next(ctx, cmds)
		for _, cmd := range cmds {
			args := cmd.Args()
			if len(args) < 2 || strings.ToLower(cmd.Name()) != "hget" {
				continue
			}
			if key, ok := args[1].(string); ok && key == h.key {
				cmd.SetErr(h.err)
				err = h.err
			}
		}
		return err
	}
}

// TestRedisRunnerDirectoryReplayLeaseDoesNotSwallowAReadErrorBehindAPrune
// pins the per-command error checks the pipelined replay needs: Exec reports
// only the first error and treats redis.Nil as one, so a candidate whose state
// field is missing must not hide a real error on the owner read issued in the
// same batch. The stale candidate here is pruned in the same pass; the read
// error must still fail the poll rather than be discarded with it.
func TestRedisRunnerDirectoryReplayLeaseDoesNotSwallowAReadErrorBehindAPrune(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-replay-error", 2)
	held := seedRedisDirectoryLeases(t, ctx, directory, session, "exec-replay-error/runner", 1)

	// The record is no longer leased, so a replay pass would prune its index
	// entry; the owner field is still there for the injected read error to hit.
	if err := rdb.HSet(ctx, directory.keys.assignmentState, string(held[0].assignmentID), "released").Err(); err != nil {
		t.Fatalf("HSet(assignment state) error = %v", err)
	}

	injected := errors.New("injected owner read failure")
	rdb.AddHook(replayReadErrorHook{key: directory.keys.assignmentRunner, err: injected})

	_, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 1))
	if err == nil {
		t.Fatalf("ClaimForRunner() ok=%v err=nil; a real read error on the owner field must fail the poll", ok)
	}
	if !errors.Is(err, injected) {
		t.Fatalf("ClaimForRunner() error = %v, want the injected read error wrapped", err)
	}
	if !strings.Contains(err.Error(), "read lease owner") {
		t.Fatalf("ClaimForRunner() error = %q, want the existing owner-read wording", err)
	}
}
