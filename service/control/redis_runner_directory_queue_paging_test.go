package control

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
)

// queueLRangeHook counts LRange reads that hit the shared assignment queue. The
// queue is shared across every runner, namespace and workflow, so a per-poll
// read of it grows with the whole pending backlog; these tests pin the poll to a
// bounded page and to no read at all when the runner has no headroom.
type queueLRangeHook struct {
	mu  sync.Mutex
	key string
	n   int
}

func (h *queueLRangeHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *queueLRangeHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.observe(cmd)
		return next(ctx, cmd)
	}
}

func (h *queueLRangeHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			h.observe(cmd)
		}
		return next(ctx, cmds)
	}
}

func (h *queueLRangeHook) observe(cmd redis.Cmder) {
	if strings.ToLower(cmd.Name()) != "lrange" {
		return
	}
	args := cmd.Args()
	if len(args) < 2 {
		return
	}
	if key, ok := args[1].(string); !ok || key != h.key {
		return
	}
	h.mu.Lock()
	h.n++
	h.mu.Unlock()
}

func (h *queueLRangeHook) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.n
}

func (h *queueLRangeHook) reset() {
	h.mu.Lock()
	h.n = 0
	h.mu.Unlock()
}

// ineligibleRedisDirectoryAssignment routes to a node type the registered test
// runner cannot serve, so it sits in the queue as an unclaimable prefix.
func ineligibleRedisDirectoryAssignment(id AssignmentID) Assignment {
	assignment := redisDirectoryTestAssignment(id)
	assignment.Routing.NodeType = "xflow.other"
	return assignment
}

// TestClaimForRunnerPagesPastAnIneligiblePrefix pins the resume behavior: an
// assignment buried behind a prefix longer than one page is still reached, and
// within a bounded number of polls. A pager that stopped at the first
// unclaimable page -- or advanced its cursor without ever wrapping -- would
// strand the trailing assignment and never return it.
func TestClaimForRunnerPagesPastAnIneligiblePrefix(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-1", 4)

	const prefix = 150 // more than one redisClaimQueuePage
	for i := 0; i < prefix; i++ {
		mustEnqueueRedisDirectoryAssignment(t, ctx, directory, ineligibleRedisDirectoryAssignment(
			AssignmentID(fmt.Sprintf("exec-1/ineligible-%d/activation-1", i))))
	}
	servable := AssignmentID("exec-1/servable/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, redisDirectoryTestAssignment(servable))

	for poll := 0; poll < 10; poll++ {
		claim, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 1))
		if err != nil {
			t.Fatalf("ClaimForRunner() poll %d error = %v", poll, err)
		}
		if !ok {
			// A page of purely unclaimable assignments reports no claim; the
			// cursor advances and the next poll resumes further into the queue.
			continue
		}
		if claim.Assignment.AssignmentID != servable {
			t.Fatalf("ClaimForRunner() poll %d claimed %q, want only %q", poll, claim.Assignment.AssignmentID, servable)
		}
		return
	}
	t.Fatalf("ClaimForRunner() never reached %q past a %d-entry ineligible prefix", servable, prefix)
}

// TestClaimForRunnerDrainsAQueueLargerThanOnePage pins that paging composes:
// claiming one assignment at a time still empties a backlog several pages long,
// with every assignment claimed exactly once.
func TestClaimForRunnerDrainsAQueueLargerThanOnePage(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	const total = 200 // > redisClaimQueuePage
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-1", total)

	for i := 0; i < total; i++ {
		mustEnqueueRedisDirectoryAssignment(t, ctx, directory, redisDirectoryTestAssignment(
			AssignmentID(fmt.Sprintf("exec-1/queued-%d/activation-1", i))))
	}

	claimed := make(map[AssignmentID]int, total)
	for i := 0; i < total*2; i++ {
		claim, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 1))
		if err != nil {
			t.Fatalf("ClaimForRunner() error = %v", err)
		}
		if !ok {
			break
		}
		claimed[claim.Assignment.AssignmentID]++
	}
	if len(claimed) != total {
		t.Fatalf("claimed %d distinct assignments, want %d", len(claimed), total)
	}
	for id, n := range claimed {
		if n != 1 {
			t.Fatalf("assignment %q claimed %d times, want 1", id, n)
		}
	}
}

// TestClaimForRunnerDoesNotStrandUnderConcurrentClaims runs two runners against
// the shared queue at once. Both claim from the same list, so each successful
// claim LREMs an element ahead of the other runner's cursor -- the exact shift
// that makes a positional cursor skip. Every assignment must still be claimed
// exactly once.
func TestClaimForRunnerDoesNotStrandUnderConcurrentClaims(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	const total = 200
	sessionA := registerRedisDirectoryRunner(t, ctx, directory, "runner-a", total)
	sessionB := registerRedisDirectoryRunner(t, ctx, directory, "runner-b", total)

	for i := 0; i < total; i++ {
		mustEnqueueRedisDirectoryAssignment(t, ctx, directory, redisDirectoryTestAssignment(
			AssignmentID(fmt.Sprintf("exec-1/concurrent-%d/activation-1", i))))
	}

	drain := func(session RunnerSession) []AssignmentID {
		var out []AssignmentID
		for i := 0; i < total*2; i++ {
			claim, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 1))
			if err != nil {
				t.Errorf("ClaimForRunner(%s) error = %v", session.RunnerID, err)
				return out
			}
			if !ok {
				return out
			}
			out = append(out, claim.Assignment.AssignmentID)
		}
		t.Errorf("ClaimForRunner(%s) did not drain within bound", session.RunnerID)
		return out
	}

	var wg sync.WaitGroup
	results := make([][]AssignmentID, 2)
	for i, session := range []RunnerSession{sessionA, sessionB} {
		wg.Add(1)
		go func(i int, session RunnerSession) {
			defer wg.Done()
			results[i] = drain(session)
		}(i, session)
	}
	wg.Wait()

	seen := make(map[AssignmentID]int, total)
	for _, ids := range results {
		for _, id := range ids {
			seen[id]++
		}
	}
	if len(seen) != total {
		t.Fatalf("claimed %d distinct assignments across two runners, want %d (stranded %d)",
			len(seen), total, total-len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("assignment %q claimed %d times across runners, want 1", id, n)
		}
	}
}

// TestClaimForRunnerSkipsQueueScanWithoutHeadroom pins the hoisted precheck: a
// runner whose capacity is fully used must not read the shared queue at all.
// Before the precheck every poll of a full runner read the entire backlog and
// then discarded it on the claim transition's headroom check.
func TestClaimForRunnerSkipsQueueScanWithoutHeadroom(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	hook := &queueLRangeHook{key: directory.keys.queue}
	rdb.AddHook(hook)

	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-1", 1)
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, redisDirectoryTestAssignment("exec-1/one/activation-1"))
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, redisDirectoryTestAssignment("exec-1/two/activation-1"))

	// Capacity 1: this claim consumes the runner's only slot.
	claimRedisDirectoryAssignment(t, ctx, directory, session, 1)

	hook.reset()
	_, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 1))
	if err != nil {
		t.Fatalf("ClaimForRunner() error = %v", err)
	}
	if ok {
		t.Fatal("ClaimForRunner() ok=true for a full runner, want no claim")
	}
	if n := hook.count(); n != 0 {
		t.Fatalf("queue LRange reads with no headroom = %d, want 0", n)
	}
}
