package rstate

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// The tests built on this rig race ExpireExecution against another writer of the
// same execution on a real Engine over the real Redis store (miniredis). The
// interleavings are decided by the store's own Lua scripts, which is the part a
// fake store cannot stand in for.
//
// There is deliberately no queue consumer. The rig reads the queue itself, so
// the only goroutines that touch an execution are the ones a test starts, and a
// task is leased exactly when the test asks for it.

// raceTimeout is the settings.timeout the race graphs declare. One nanosecond
// puts the deadline behind the clock the moment Submit stamps it, so every
// round starts with an execution that is already due and no test sleeps.
const raceTimeout = time.Nanosecond

// raceRounds is how many times a race is replayed. The detector multiplies the
// cost of a round several times over, so it gets fewer of them.
func raceRounds() int {
	if raceDetectorEnabled {
		return 50
	}
	return 200
}

// raceTogether starts every fn on its own goroutine and releases them at the
// same instant, then waits for all of them. Each fn records its result in
// variables the caller owns; asserting from inside would call t.Fatal off the
// test goroutine.
func raceTogether(fns ...func()) {
	var ready, done sync.WaitGroup
	start := make(chan struct{})
	for _, fn := range fns {
		ready.Add(1)
		done.Add(1)
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			fn()
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()
}

// raceQueue is a TaskQueue that only remembers what it was handed.
type raceQueue struct {
	mu    sync.Mutex
	tasks []*engine.Task
}

func (q *raceQueue) Enqueue(_ context.Context, t *engine.Task) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	cp := *t
	q.tasks = append(q.tasks, &cp)
	return nil
}

func (q *raceQueue) EnqueueDelayed(ctx context.Context, t *engine.Task, _ time.Duration) error {
	return q.Enqueue(ctx, t)
}

// take removes and returns every queued task of one execution, oldest first.
func (q *raceQueue) take(id types.ExecutionID) []*engine.Task {
	q.mu.Lock()
	defer q.mu.Unlock()
	var taken, kept []*engine.Task
	for _, task := range q.tasks {
		if task.ExecutionID == id {
			taken = append(taken, task)
		} else {
			kept = append(kept, task)
		}
	}
	q.tasks = kept
	return taken
}

// deadlineRaceRig is one Engine over one Redis store on its own miniredis, with
// the hooks and the queue the tests inspect. Each round builds a fresh one, so
// no round can inherit another's keys, and closes it, so 200 rounds do not hold
// 200 listeners open.
type deadlineRaceRig struct {
	t     *testing.T
	srv   *miniredis.Miniredis
	rdb   *redis.Client
	state *Store
	queue *raceQueue
	hooks *deadlineCompletionHooks
	eng   *engine.Engine
	once  sync.Once
}

func newDeadlineRaceRig(t *testing.T, opts ...engine.Option) *deadlineRaceRig {
	t.Helper()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	rig := &deadlineRaceRig{
		t:     t,
		srv:   srv,
		rdb:   rdb,
		state: New(rdb, nil, time.Minute),
		queue: &raceQueue{},
		hooks: &deadlineCompletionHooks{},
	}
	t.Cleanup(rig.close)
	opts = append([]engine.Option{engine.WithHooks(rig.hooks)}, opts...)
	rig.eng = engine.New(rig.state, rig.queue, opts...)
	return rig
}

// close releases the rig's Redis. It is safe to call twice, so a round can close
// its rig on the way out and the test's cleanup can still cover a round that
// failed half way.
func (r *deadlineRaceRig) close() {
	r.once.Do(func() {
		_ = r.rdb.Close()
		r.srv.Close()
	})
}

// raceGraph compiles a workflow whose settings.timeout is raceTimeout. A
// positive limit adds a workflow-scoped concurrency limit in queue mode.
func raceGraph(t *testing.T, name string, limit int, nodes ...types.NodeDef) *graph.Graph {
	t.Helper()
	return raceGraphWithTimeout(t, name, raceTimeout, limit, nodes...)
}

// raceGraphWithTimeout is raceGraph with the settings.timeout spelled out, for
// the tests whose second execution must not be due the moment it exists.
func raceGraphWithTimeout(t *testing.T, name string, timeout time.Duration, limit int, nodes ...types.NodeDef) *graph.Graph {
	t.Helper()
	settings := &types.WorkflowSettings{Timezone: "UTC", Timeout: timeout}
	if limit > 0 {
		settings.Concurrency = &types.ConcurrencySettings{Limit: limit}
	}
	g, err := graph.Compile(&types.WorkflowDef{
		Name:     name,
		Version:  "v1",
		Settings: settings,
		Nodes:    nodes,
	})
	if err != nil {
		t.Fatalf("Compile(%s) error = %v", name, err)
	}
	return g
}

// raceWorkGraph is a single-node workflow; the node's commit is the execution's
// last one.
func raceWorkGraph(t *testing.T, limit int) *graph.Graph {
	t.Helper()
	return raceGraph(t, "deadline-race-work", limit, types.NodeDef{Name: "work", Type: "test.race_work"})
}

// submit starts an execution and makes sure its initial task has reached the
// queue, so a test can lease it whether or not Submit flushed the outbox itself.
func (r *deadlineRaceRig) submit(ctx context.Context, g *graph.Graph) types.ExecutionID {
	r.t.Helper()
	id, err := r.eng.Submit(ctx, g, nil)
	if err != nil {
		r.t.Fatalf("Submit(%s) error = %v", g.Name(), err)
	}
	if err := r.eng.FlushOutbox(ctx, id); err != nil {
		r.t.Fatalf("FlushOutbox(%s) error = %v", id, err)
	}
	return id
}

// lease turns the one task the execution has queued into a lease.
func (r *deadlineRaceRig) lease(ctx context.Context, id types.ExecutionID) *engine.TaskLease {
	r.t.Helper()
	tasks := r.queue.take(id)
	if len(tasks) != 1 {
		r.t.Fatalf("execution %s has %d queued tasks, want exactly 1", id, len(tasks))
	}
	lease, err := r.eng.BuildTaskLease(ctx, tasks[0])
	if err != nil {
		r.t.Fatalf("BuildTaskLease(%s) error = %v", id, err)
	}
	return lease
}

func (r *deadlineRaceRig) snapshot(ctx context.Context, id types.ExecutionID) *engine.ExecutionSnapshot {
	r.t.Helper()
	snap, err := r.state.GetExecution(ctx, id)
	if err != nil || snap == nil {
		r.t.Fatalf("GetExecution(%s) = (%+v, %v)", id, snap, err)
	}
	return snap
}

// zcard reads a ZSET's size straight from Redis, so the answer cannot be
// repaired by the read path the way a List call repairs a concluded entry.
func (r *deadlineRaceRig) zcard(key string) int64 {
	r.t.Helper()
	n, err := r.rdb.ZCard(context.Background(), key).Result()
	if err != nil {
		r.t.Fatalf("ZCARD %s: %v", key, err)
	}
	return n
}

// drainProblem describes the first piece of bookkeeping the given executions'
// end left behind, or "" when there is none: a deadline index entry, a
// concurrency slot or queue place of their workflow, or a suspended node. The
// signal-named waiter key is not looked at: what the store leaves of it is the
// subject of the signal test's own subtest.
func (r *deadlineRaceRig) drainProblem(workflow string, ids ...types.ExecutionID) string {
	ns := namespace.Default
	if n := r.zcard(executionDeadlineIndexKey(ns)); n != 0 {
		members, _ := r.rdb.ZRange(context.Background(), executionDeadlineIndexKey(ns), 0, -1).Result()
		return fmt.Sprintf("deadline index still holds %d entries: %v", n, members)
	}
	tag := workflowDigestTag(ns, workflow)
	if n := r.zcard(concurrencySlotKeyForTag(ns, tag)); n != 0 {
		return fmt.Sprintf("%d concurrency slots of %q leaked", n, workflow)
	}
	if n := r.zcard(concurrencyQueueKeyForTag(ns, tag)); n != 0 {
		return fmt.Sprintf("%d queue places of %q leaked", n, workflow)
	}
	for _, id := range ids {
		if n, err := r.rdb.SCard(context.Background(), suspendedNodesKey(ns, id)).Result(); err != nil || n != 0 {
			return fmt.Sprintf("%s has %d suspended nodes left (err %v)", id, n, err)
		}
	}
	return ""
}

// requireDrained fails the test when drainProblem finds anything.
func (r *deadlineRaceRig) requireDrained(round int, workflow string, ids ...types.ExecutionID) {
	r.t.Helper()
	if problem := r.drainProblem(workflow, ids...); problem != "" {
		r.t.Fatalf("round %d: %s", round, problem)
	}
}

// requireAnnouncedOnly fails unless the execution's completion was announced at
// least once and only ever as want. Two writers racing for the terminal state
// may both call the hook, but never with a status the store did not settle on.
func (r *deadlineRaceRig) requireAnnouncedOnly(round int, id types.ExecutionID, want types.ExecutionStatus) {
	r.t.Helper()
	got := r.hooks.completionsOf(id)
	if len(got) == 0 {
		r.t.Fatalf("round %d: execution %s ended %q but its completion was never announced", round, id, want)
	}
	for _, status := range got {
		if status != want {
			r.t.Fatalf("round %d: execution %s ended %q but its completion was announced as %v", round, id, want, got)
		}
	}
}
