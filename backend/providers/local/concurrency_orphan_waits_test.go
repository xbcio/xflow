package local

// The in-memory half of the waiting-shape orphan tests. The judgement is the
// same as rstate's (memory_state_orphan.go documents it as the exact mirror),
// so each test is the local equivalent of concurrency_orphan_waits_test.go:
// park through the real engine, remove the graph, backdate both clocks, then
// ask whether the shape is spared. The guards under test are
// hasSuspendedLocked and hasActiveLeaseLocked.

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// orphanWaitQueue is a queue that only remembers what it was handed, so a test
// delivers tasks exactly when it asks for them.
type orphanWaitQueue struct{ tasks []*engine.Task }

func (q *orphanWaitQueue) Enqueue(_ context.Context, t *engine.Task) error {
	q.tasks = append(q.tasks, t)
	return nil
}

func (q *orphanWaitQueue) EnqueueDelayed(ctx context.Context, t *engine.Task, _ time.Duration) error {
	return q.Enqueue(ctx, t)
}

func (q *orphanWaitQueue) takeFor(id types.ExecutionID) *engine.Task {
	for i, task := range q.tasks {
		if task.ExecutionID == id {
			q.tasks = append(q.tasks[:i], q.tasks[i+1:]...)
			return task
		}
	}
	return nil
}

type orphanWaitRig struct {
	ctx   context.Context
	state *memoryState
	queue *orphanWaitQueue
	eng   *engine.Engine
}

func newOrphanWaitRig(t *testing.T, opts ...engine.Option) *orphanWaitRig {
	t.Helper()
	state := newMemoryState()
	queue := &orphanWaitQueue{}
	return &orphanWaitRig{
		ctx:   namespace.WithNamespace(context.Background(), namespace.Default),
		state: state,
		queue: queue,
		eng:   engine.New(state, queue, opts...),
	}
}

func (r *orphanWaitRig) submit(t *testing.T, g *graph.Graph) types.ExecutionID {
	t.Helper()
	id, err := r.eng.Submit(r.ctx, g, map[string]any{"msg": "hello"})
	if err != nil {
		t.Fatalf("Submit(%s) error = %v", g.Name(), err)
	}
	return id
}

// orphanWaitGraph is a one-node workflow with a concurrency limit of one: an
// unlimited execution holds no slot and would be invisible to the sweep.
func orphanWaitGraph(t *testing.T, name string) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    name,
		Version: "1",
		Nodes:   []types.NodeDef{{Name: "start", Type: "xflow.wait"}},
		Settings: &types.WorkflowSettings{Concurrency: &types.ConcurrencySettings{
			Limit: 1,
			Mode:  types.ConcurrencyModeQueue,
		}},
	})
	if err != nil {
		t.Fatalf("Compile(%s) error = %v", name, err)
	}
	return g
}

// orphanLoseGraphAndAge constructs the harshest reclaim clock: graph gone, both
// age clocks older than any grace the tests use.
func orphanLoseGraphAndAge(t *testing.T, rig *orphanWaitRig, id types.ExecutionID) {
	t.Helper()
	rig.state.mu.Lock()
	defer rig.state.mu.Unlock()
	entry := rig.state.executions[id]
	if entry == nil {
		t.Fatalf("execution %q is gone", id)
	}
	entry.snap.Graph = nil
	entry.snap.UpdatedAt = time.Now().UTC().Add(-10 * time.Minute)
	rig.state.activityAt[id] = time.Now().UTC().Add(-10 * time.Minute)
}

func orphanLocalSweep(t *testing.T, rig *orphanWaitRig, req engine.OrphanReclaimRequest) engine.OrphanReclaimResult {
	t.Helper()
	if req.Grace <= 0 {
		req.Grace = time.Minute
	}
	res, err := rig.state.ReclaimConcurrencyOrphans(rig.ctx, req)
	if err != nil {
		t.Fatalf("ReclaimConcurrencyOrphans(%+v) error = %v", req, err)
	}
	return res
}

func (r *orphanWaitRig) parkSuspension(t *testing.T, name string, spec *types.SuspendSpec) types.ExecutionID {
	t.Helper()
	id := r.submit(t, orphanWaitGraph(t, name))
	task := r.queue.takeFor(id)
	if task == nil {
		t.Fatal("no queued root task")
	}
	lease, err := r.eng.BuildTaskLease(r.ctx, task)
	if err != nil {
		t.Fatalf("BuildTaskLease error = %v", err)
	}
	if err := r.eng.CommitTaskResult(r.ctx, lease, engine.TaskResult{Suspend: spec}); err != nil {
		t.Fatalf("CommitTaskResult(suspend) error = %v", err)
	}
	return id
}

// orphanAssertLocalSuspended pins the local park shape: the node is suspended,
// the in-memory suspension map names it, and no lease is left.
func orphanAssertLocalSuspended(t *testing.T, rig *orphanWaitRig, id types.ExecutionID) {
	t.Helper()
	rig.state.mu.Lock()
	defer rig.state.mu.Unlock()
	node := rig.state.nodes[string(id)+"/start"]
	if node == nil || node.Status != types.NodeStatusSuspended {
		t.Fatalf("node = %+v, want suspended", node)
	}
	if !rig.state.hasSuspendedLocked(id) {
		t.Fatal("suspension map does not name the parked node: the reclaim guard cannot see it")
	}
	if rig.state.hasActiveLeaseLocked(id, time.Now().UTC()) {
		t.Fatal("a parked node still reports a live lease")
	}
}

// TestMemoryOrphanReclaimSparesTimerAndSignalWait covers both park flavours
// with one shape assertion: a timer spec has no signal names but is registered
// in the same suspension map. Mutation: removing hasSuspendedLocked's check
// from evaluateOrphanLocked makes both subtests red.
func TestMemoryOrphanReclaimSparesTimerAndSignalWait(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec *types.SuspendSpec
	}{
		{"timer", &types.SuspendSpec{Mode: types.ModeTimer, Timer: time.Hour}},
		{"signal", &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"go"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newOrphanWaitRig(t)
			id := rig.parkSuspension(t, "memory-orphan-wait-"+tc.name, tc.spec)
			orphanAssertLocalSuspended(t, rig, id)
			orphanLoseGraphAndAge(t, rig, id)

			sweep := orphanLocalSweep(t, rig, engine.OrphanReclaimRequest{
				Mode: engine.OrphanReclaimSweep, Namespace: "default", Limit: 8,
			})
			if sweep.Inspected == 0 {
				t.Fatal("sweep inspected no candidate: 0 reclaimed proves nothing")
			}
			force := orphanLocalSweep(t, rig, engine.OrphanReclaimRequest{
				Mode: engine.OrphanReclaimForce, Namespace: "default", Limit: 1, ExecutionID: id,
			})
			if sweep.Reclaimed != 0 || force.Reclaimed != 0 {
				t.Fatalf("reclaimed sweep=%d force=%d while parked, want 0/0", sweep.Reclaimed, force.Reclaimed)
			}
			rig.state.mu.Lock()
			status := rig.state.executions[id].snap.Status
			rig.state.mu.Unlock()
			if status != types.ExecutionStatusRunning {
				t.Fatalf("status after guarded reclaim = %q, want running", status)
			}
		})
	}
}

// orphanLocalTimerThenNodeGraph is a timer wait followed by an ordinary node,
// so the execution is still running after the wake.
func orphanLocalTimerThenNodeGraph(t *testing.T, name string) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    name,
		Version: "1",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.wait"},
			{Name: "next", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "next", Input: "main"}}}},
		},
		Settings: &types.WorkflowSettings{Concurrency: &types.ConcurrencySettings{
			Limit: 1,
			Mode:  types.ConcurrencyModeQueue,
		}},
	})
	if err != nil {
		t.Fatalf("Compile(%s) error = %v", name, err)
	}
	return g
}

// TestMemoryOrphanReclaimConvergesAfterTimerResume is the local half of the
// timer wake path's suspension cleanup: the park registers the node in the
// suspension map, the delayed outbox entry drives the resume, and the accepted
// acquire removes the entry in the same locked transition that sets the node
// running and stamps activityAt. Signal delivery already contracts the same way
// (DeliverSignalWithOutbox deletes the entry before scheduling the resume).
//
// Without the acquire deletion the entry outlived the wait and
// hasSuspendedLocked kept sparing a graph-lost execution in which nothing was
// suspended: over-protection, so the orphan never healed. The assertions fail
// in exactly that direction.
//
// Mutation: removing the suspended-map deletion from acquireTaskLeaseLocked
// makes this test red.
func TestMemoryOrphanReclaimConvergesAfterTimerResume(t *testing.T) {
	rig := newOrphanWaitRig(t)
	id := rig.submit(t, orphanLocalTimerThenNodeGraph(t, "memory-timer-stale-member"))
	task := rig.queue.takeFor(id)
	if task == nil || task.NodeName != "start" {
		t.Fatalf("queued root = %+v, want start", task)
	}
	lease, err := rig.eng.BuildTaskLease(rig.ctx, task)
	if err != nil {
		t.Fatalf("BuildTaskLease error = %v", err)
	}
	if err := rig.eng.CommitTaskResult(rig.ctx, lease, engine.TaskResult{Suspend: &types.SuspendSpec{
		Mode: types.ModeTimer, Timer: 10 * time.Millisecond,
	}}); err != nil {
		t.Fatalf("CommitTaskResult(suspend) error = %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	if err := rig.eng.FlushOutbox(rig.ctx, id); err != nil {
		t.Fatalf("FlushOutbox error = %v", err)
	}
	resume := rig.queue.takeFor(id)
	if resume == nil || resume.Type != engine.TaskTypeNodeResume {
		t.Fatalf("timer wakeup = %+v, want a resume task", resume)
	}
	resumeLease, err := rig.eng.BuildTaskLease(rig.ctx, resume)
	if err != nil {
		t.Fatalf("BuildTaskLease(resume) error = %v", err)
	}
	if err := rig.eng.CommitTaskResult(rig.ctx, resumeLease, engine.TaskResult{
		Output: &types.Output{Data: map[string]any{"woke": true}},
	}); err != nil {
		t.Fatalf("CommitTaskResult(resume) error = %v", err)
	}

	rig.state.mu.Lock()
	node := rig.state.nodes[string(id)+"/start"]
	suspended := rig.state.hasSuspendedLocked(id)
	rig.state.mu.Unlock()
	if node == nil || node.Status != types.NodeStatusSuccess {
		t.Fatalf("start node after resume = %+v, want success", node)
	}
	if suspended {
		t.Fatal("the timer wake left its suspension entry: the resume acquire must clear it")
	}

	// The torn shape the parked test refuses to reclaim, now with nothing
	// suspended: the stale entry must no longer over-protect it.
	orphanLoseGraphAndAge(t, rig, id)
	res := orphanLocalSweep(t, rig, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, Namespace: "default", Limit: 8,
	})
	if res.Reclaimed != 1 {
		t.Fatalf("Reclaimed = %d, want 1: the resumed execution has nothing suspended and is graph-lost", res.Reclaimed)
	}
	rig.state.mu.Lock()
	status := rig.state.executions[id].snap.Status
	rig.state.mu.Unlock()
	if status != types.ExecutionStatusCanceled {
		t.Fatalf("status = %q, want canceled", status)
	}
}

// TestMemoryOrphanReclaimSparesSignalDeliveredExecution is the local half of
// the delivery → resume-acquire window. Delivery clears the suspension and
// writes the resume intent; it now also refreshes activityAt, with the same
// semantics as a lease acquisition. That stamp is the only protection until the
// resume is acquired, so the sweep and force must both spare the execution
// while it is inside the grace and converge it once it ages past.
// Mutation: removing the activityAt refresh from scheduleResumeOutbox makes the
// guarded half red.
func TestMemoryOrphanReclaimSparesSignalDeliveredExecution(t *testing.T) {
	rig := newOrphanWaitRig(t)
	id := rig.parkSuspension(t, "memory-orphan-delivered", &types.SuspendSpec{
		Mode: types.ModeSignal, Signals: []string{"go"},
	})
	orphanLoseGraphAndAge(t, rig, id)

	nodeName, _, committed, err := rig.state.DeliverSignalWithOutbox(rig.ctx, id, "go",
		map[string]any{"ok": true}, engine.ResumeIntent{NodeName: "start", NodeIdx: 0})
	if err != nil || !committed || nodeName != "start" {
		t.Fatalf("DeliverSignalWithOutbox = (%q, %v, %v), want (start, true, nil)", nodeName, committed, err)
	}
	rig.state.mu.Lock()
	suspended := rig.state.hasSuspendedLocked(id)
	rig.state.mu.Unlock()
	if suspended {
		t.Fatal("delivery left the node suspended: the shape under test is wrong")
	}

	sweep := orphanLocalSweep(t, rig, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, Namespace: "default", Limit: 8,
	})
	force := orphanLocalSweep(t, rig, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimForce, Namespace: "default", Limit: 1, ExecutionID: id,
	})
	if sweep.Reclaimed != 0 || force.Reclaimed != 0 {
		t.Fatalf("reclaimed sweep=%d force=%d after delivery, want 0/0: delivery must refresh activityAt", sweep.Reclaimed, force.Reclaimed)
	}
	rig.state.mu.Lock()
	status := rig.state.executions[id].snap.Status
	rig.state.mu.Unlock()
	if status != types.ExecutionStatusRunning {
		t.Fatalf("status after guarded reclaim = %q, want running", status)
	}

	// Age the delivery stamp: now nothing protects the pending resume.
	rig.state.mu.Lock()
	rig.state.activityAt[id] = time.Now().UTC().Add(-10 * time.Minute)
	rig.state.mu.Unlock()
	res := orphanLocalSweep(t, rig, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, Namespace: "default", Limit: 8,
	})
	if res.Reclaimed != 1 {
		t.Fatalf("Reclaimed = %d once the delivery stamp is past the grace, want 1", res.Reclaimed)
	}
	rig.state.mu.Lock()
	status = rig.state.executions[id].snap.Status
	rig.state.mu.Unlock()
	if status != types.ExecutionStatusCanceled {
		t.Fatalf("status = %q, want canceled", status)
	}
}

// TestMemoryOrphanReclaimSparesWaitingParentHoldingLease mirrors the rstate
// parent test: a sub-workflow or map parent is waiting, not suspended, and the
// live lease is its only guard. Once that lease expires the parent is
// unprotected and a graph-lost sweep converges it.
// Mutation: removing hasActiveLeaseLocked's check from evaluateOrphanLocked
// makes the guarded half red.
func TestMemoryOrphanReclaimSparesWaitingParentHoldingLease(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []engine.Option
		park func(t *testing.T, rig *orphanWaitRig, id types.ExecutionID, lease *engine.TaskLease)
	}{
		{
			name: "subworkflow",
			opts: []engine.Option{engine.WithDefaultLeaseTTL(5 * time.Minute)},
			park: func(t *testing.T, rig *orphanWaitRig, id types.ExecutionID, lease *engine.TaskLease) {
				accepted, err := rig.state.BeginSubWorkflowWait(rig.ctx, lease, engine.SubWorkflowLink{
					ChildExecutionID: "exec-child-1",
					ParentNode:       "start",
					Generation:       1,
					Depth:            1,
					LeaseID:          lease.LeaseID,
					LeaseToken:       lease.LeaseToken,
					Attempt:          lease.Attempt,
					ActivationID:     lease.Task.ActivationID,
				}, nil)
				if err != nil || !accepted {
					t.Fatalf("BeginSubWorkflowWait accepted=%v err=%v", accepted, err)
				}
			},
		},
		{
			name: "map",
			park: func(t *testing.T, rig *orphanWaitRig, id types.ExecutionID, lease *engine.TaskLease) {
				accepted, err := rig.state.BeginTaskExpansion(rig.ctx, lease)
				if err != nil || !accepted {
					t.Fatalf("BeginTaskExpansion accepted=%v err=%v", accepted, err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newOrphanWaitRig(t, tc.opts...)
			id := rig.submit(t, orphanWaitGraph(t, "memory-parent-"+tc.name))
			task := rig.queue.takeFor(id)
			if task == nil {
				t.Fatal("no queued root task")
			}
			lease, err := rig.eng.BuildTaskLease(rig.ctx, task)
			if err != nil {
				t.Fatalf("BuildTaskLease error = %v", err)
			}
			if _, claimed, err := rig.state.ClaimTaskLease(rig.ctx, lease); err != nil || !claimed {
				t.Fatalf("ClaimTaskLease claimed=%v err=%v", claimed, err)
			}
			tc.park(t, rig, id, lease)

			rig.state.mu.Lock()
			node := rig.state.nodes[string(id)+"/start"]
			suspended := rig.state.hasSuspendedLocked(id)
			rig.state.mu.Unlock()
			if node == nil || node.Status != types.NodeStatusWaiting {
				t.Fatalf("node = %+v, want waiting", node)
			}
			if suspended {
				t.Fatal("a waiting parent is registered as suspended")
			}

			orphanLoseGraphAndAge(t, rig, id)
			sweep := orphanLocalSweep(t, rig, engine.OrphanReclaimRequest{
				Mode: engine.OrphanReclaimSweep, Namespace: "default", Limit: 8,
			})
			force := orphanLocalSweep(t, rig, engine.OrphanReclaimRequest{
				Mode: engine.OrphanReclaimForce, Namespace: "default", Limit: 1, ExecutionID: id,
			})
			if sweep.Reclaimed != 0 || force.Reclaimed != 0 {
				t.Fatalf("reclaimed sweep=%d force=%d with a live wait lease, want 0/0", sweep.Reclaimed, force.Reclaimed)
			}

			// Send the wait lease into the past: protection ends with the lease.
			rig.state.mu.Lock()
			if ns := rig.state.nodes[string(id)+"/start"]; ns != nil {
				ns.LeaseIssuedAt = time.Now().UTC().Add(-10 * time.Minute)
			}
			rig.state.mu.Unlock()
			res := orphanLocalSweep(t, rig, engine.OrphanReclaimRequest{
				Mode: engine.OrphanReclaimSweep, Namespace: "default", Limit: 8,
			})
			if res.Reclaimed != 1 {
				t.Fatalf("Reclaimed = %d after the wait lease expired, want 1", res.Reclaimed)
			}
		})
	}
}

// TestMemoryOrphanSweepReclaimsBetweenNodeWindowOnceActivityAgesPastGrace
// mirrors the rstate inter-node test: the commit stamp covers the window only
// while it is inside the grace.
func TestMemoryOrphanSweepReclaimsBetweenNodeWindowOnceActivityAgesPastGrace(t *testing.T) {
	rig := newOrphanWaitRig(t)
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    "memory-between-nodes",
		Version: "1",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "next", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "next", Input: "main"}}}},
		},
		Settings: &types.WorkflowSettings{Concurrency: &types.ConcurrencySettings{
			Limit: 1,
			Mode:  types.ConcurrencyModeQueue,
		}},
	})
	if err != nil {
		t.Fatalf("Compile error = %v", err)
	}
	id := rig.submit(t, g)
	task := rig.queue.takeFor(id)
	lease, err := rig.eng.BuildTaskLease(rig.ctx, task)
	if err != nil {
		t.Fatalf("BuildTaskLease(start) error = %v", err)
	}
	if err := rig.eng.CommitTaskResult(rig.ctx, lease, engine.TaskResult{
		Output: &types.Output{Data: map[string]any{"step": 1}},
	}); err != nil {
		t.Fatalf("CommitTaskResult(start) error = %v", err)
	}
	if next := rig.queue.takeFor(id); next == nil || next.NodeName != "next" {
		t.Fatalf("next queued task = %+v, want next", next)
	}

	rig.state.mu.Lock()
	rig.state.executions[id].snap.Graph = nil
	rig.state.executions[id].snap.UpdatedAt = time.Now().UTC().Add(-10 * time.Minute)
	rig.state.mu.Unlock()

	res := orphanLocalSweep(t, rig, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, Namespace: "default", Limit: 8,
	})
	if res.Reclaimed != 0 {
		t.Fatalf("Reclaimed = %d inside the activity grace, want 0", res.Reclaimed)
	}

	rig.state.mu.Lock()
	rig.state.activityAt[id] = time.Now().UTC().Add(-10 * time.Minute)
	rig.state.mu.Unlock()
	res = orphanLocalSweep(t, rig, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, Namespace: "default", Limit: 8,
	})
	if res.Reclaimed != 1 {
		t.Fatalf("Reclaimed = %d past the activity grace, want 1", res.Reclaimed)
	}
}
