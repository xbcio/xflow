package local

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// The tests built on this rig race ExpireExecution against another writer of the
// same execution on a real Engine over the real in-memory store. They differ
// from the engine package's tests in what stands behind the Engine: nothing is
// faked, so the interleavings are decided by the store's own locking and CAS.
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

// deadlineRaceRig is one Engine over one in-memory store, with the hooks and the
// queue the tests inspect.
type deadlineRaceRig struct {
	t     *testing.T
	state *memoryState
	queue *raceQueue
	hooks *deadlineCascadeHooks
	eng   *engine.Engine
}

func newDeadlineRaceRig(t *testing.T, opts ...engine.Option) *deadlineRaceRig {
	t.Helper()
	b := New()
	rig := &deadlineRaceRig{
		t:     t,
		state: b.state,
		queue: &raceQueue{},
		hooks: &deadlineCascadeHooks{},
	}
	opts = append([]engine.Option{engine.WithHooks(rig.hooks)}, opts...)
	rig.eng = engine.New(rig.state, rig.queue, opts...)
	return rig
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

func (r *deadlineRaceRig) submit(ctx context.Context, g *graph.Graph) types.ExecutionID {
	r.t.Helper()
	id, err := r.eng.Submit(ctx, g, nil)
	if err != nil {
		r.t.Fatalf("Submit(%s) error = %v", g.Name(), err)
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

// requireDrained fails the test when the store still holds any bookkeeping an
// execution's end must have released: a deadline index entry, a concurrency
// slot or queue place, or a parked waiter.
func (r *deadlineRaceRig) requireDrained(round int) {
	r.t.Helper()
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	if n := len(r.state.deadlines); n != 0 {
		r.t.Fatalf("round %d: deadline index still holds %d entries: %v", round, n, r.state.deadlines)
	}
	if n := len(r.state.workflowSlots); n != 0 {
		r.t.Fatalf("round %d: %d workflow slot sets leaked", round, n)
	}
	if n := len(r.state.slotHolders); n != 0 {
		r.t.Fatalf("round %d: %d slot holders leaked", round, n)
	}
	if n := len(r.state.queued); n != 0 {
		r.t.Fatalf("round %d: %d queue places leaked", round, n)
	}
	// A suspended-index member is a leak unless its node already finished
	// successfully. A node woken by its timer or its Timeout is resumed through
	// the outbox, which never touches the index, so the member stays after the
	// node ran to success. That residue is a known, undecided defect (the
	// suspended_residue_as_it_stands subtests pin it); a node that is still
	// suspended, canceled, running or missing is not residue and is reported.
	for key := range r.state.suspended {
		if node := r.state.nodes[key]; node == nil || node.Status != types.NodeStatusSuccess {
			status := types.NodeStatus("missing")
			if node != nil {
				status = node.Status
			}
			r.t.Fatalf("round %d: suspended waiter %s leaked with its node %s", round, key, status)
		}
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
