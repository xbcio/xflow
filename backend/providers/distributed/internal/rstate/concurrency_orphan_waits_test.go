package rstate

// Orphan reclaim versus the waiting shapes. Every test here drives the real
// engine over the Redis store, then constructs the harshest clock the reclaim
// judgement can see: the graph key is gone (the only condition under which any
// reclaim is possible), no lease is live, and both age clocks — updated_at_ms
// and activity_at_ms — are older than the grace. The question each one answers
// is whether that waiting shape is self-protected or swept.
//
// The shapes, and what protects them, are:
//
//   - timer wait (xflow.wait mode=timer): parked as a suspended node. The
//     suspend write SADDs suspended_nodes unconditionally, even though a timer
//     registers no signal names, so the SCARD guard protects it.
//   - signal wait: parked as a suspended node and protected the same way.
//   - sub-workflow parent / map parent: NOT suspended. They wait on a node
//     ("waiting") holding a live lease, so the ZCOUNT lease guard is the only
//     protection, and a parked wait keeps its lease only until the TTL expires.
//   - the commit → next acquire window: no lease at all; only the activity
//     clock covers it, and only while it is inside the grace.

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// orphanWaitGraph is a one-node admissible workflow used to park each waiting
// shape. "start" is the node the tests lease; its type never runs a handler
// here — only the store-side park transitions are exercised.
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

// orphanTwoNodeLimitGraph is engOrphanTwoNodeGraph plus a concurrency limit: a
// sweep only scans slot and queue members, so an unlimited execution is
// invisible to it and would make the inter-node test vacuous.
func orphanTwoNodeLimitGraph(t *testing.T, name string) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    name,
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
		t.Fatalf("Compile(%s) error = %v", name, err)
	}
	return g
}

// orphanParkSuspension submits one node, leases it, and commits the given
// suspend spec through the engine's real suspend path.
func orphanParkSuspension(t *testing.T, rig *engOrphanRig, name string, spec *types.SuspendSpec) (types.ExecutionID, *engine.TaskLease) {
	t.Helper()
	id := rig.submit(orphanWaitGraph(t, name))
	task := rig.queue.takeFor(id)
	if task == nil || task.NodeName != "start" {
		t.Fatalf("queued root = %+v, want start", task)
	}
	lease, err := rig.eng.BuildTaskLease(rig.ctx, task)
	if err != nil {
		t.Fatalf("BuildTaskLease error = %v", err)
	}
	if err := rig.eng.CommitTaskResult(rig.ctx, lease, engine.TaskResult{Suspend: spec}); err != nil {
		t.Fatalf("CommitTaskResult(suspend) error = %v", err)
	}
	return id, lease
}

// orphanAssertSuspendedShape pins the storage form of a park: the node is
// suspended, suspended_nodes names it, and the lease it was claimed under is
// gone from the expiry index.
func orphanAssertSuspendedShape(t *testing.T, rig *engOrphanRig, id types.ExecutionID) {
	t.Helper()
	node, err := rig.state.GetNode(rig.ctx, id, "start")
	if err != nil || node == nil {
		t.Fatalf("GetNode(start) = %+v err=%v, want a parked node", node, err)
	}
	if node.Status != types.NodeStatusSuspended {
		t.Fatalf("node status = %q, want suspended", node.Status)
	}
	member, err := rig.rdb.SIsMember(rig.ctx, suspendedNodesKey(namespace.Default, id), "start").Result()
	if err != nil {
		t.Fatalf("read suspended_nodes: %v", err)
	}
	if !member {
		t.Fatal("suspended_nodes does not name the parked node: the reclaim guard cannot see it")
	}
	live, err := rig.rdb.ZCount(rig.ctx, leaseExpiryZSetKey(namespace.Default, id),
		strconv.FormatInt(time.Now().UTC().UnixMilli(), 10), "+inf").Result()
	if err != nil {
		t.Fatalf("read lease index: %v", err)
	}
	if live != 0 {
		t.Fatalf("live lease entries = %d, want 0: a park releases its lease", live)
	}
}

// orphanMakeTornStale removes the graph and backdates both age clocks: the
// exact shape a sweep considers old enough, leaving only the shape-specific
// guards to decide.
func orphanMakeTornStale(t *testing.T, rig *engOrphanRig, id types.ExecutionID) {
	t.Helper()
	rig.dropRunningGraph(id)
	orphanAgeStatus(t, rig.ctx, rig.rdb, id, 10*time.Minute)
	orphanAgeActivity(t, rig.ctx, rig.rdb, id, 10*time.Minute)
}

// orphanSweepAndForce asks both modes the same question. Sweep is the periodic
// maintenance path; force is what Cancel's self-heal and the reset API use. A
// guard that lives only in one of them would show up as a disagreement here.
func orphanSweepAndForce(t *testing.T, rig *engOrphanRig, id types.ExecutionID) (engine.OrphanReclaimResult, engine.OrphanReclaimResult) {
	t.Helper()
	sweep := orphanReclaim(t, rig.ctx, rig.state, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, Namespace: "default", Limit: 8, Grace: time.Minute,
	})
	force := orphanReclaim(t, rig.ctx, rig.state, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimForce, Namespace: "default", Limit: 1,
		ExecutionID: id, Grace: time.Minute,
	})
	return sweep, force
}

// TestOrphanReclaimSparesTimerSuspendedExecution answers the timer half of the
// question directly: an xflow.wait timer park writes suspended_nodes with no
// signal names at all. The engine's suspend write (suspendTaskLeaseWithOutboxLua)
// SADDs the node unconditionally; the only timer-specific record is the delayed
// resume entry in the outbox, which is not due for an hour.
//
// Mutations that turn this red:
//   - remove `redis.call('SADD', KEYS[5], ARGV[9])` from
//     suspendTaskLeaseWithOutboxLua (the park stops registering the wait);
//   - remove the `SCARD` guard from reclaimOrphanExecutionLua (the park becomes
//     sweepable despite being registered).
func TestOrphanReclaimSparesTimerSuspendedExecution(t *testing.T) {
	rig := newEngOrphanRig(t)
	id, _ := orphanParkSuspension(t, rig, "orphan-timer-wait", &types.SuspendSpec{
		Mode: types.ModeTimer, Timer: time.Hour,
	})
	orphanAssertSuspendedShape(t, rig, id)

	// The timer wakeup is durable but not due: a resume entry scored one hour
	// out. Its presence is why a graph-lost timer wait is worth repairing only
	// after the wait ends, not while it is parked.
	ready, err := rig.rdb.ZRangeWithScores(rig.ctx, outboxReadyKey(namespace.Default, id), 0, -1).Result()
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	var timerEntries int
	for _, entry := range ready {
		if strings.Contains(entry.Member.(string), "/timer") && time.UnixMilli(int64(entry.Score)).After(time.Now().Add(59*time.Minute)) {
			timerEntries++
		}
	}
	if timerEntries != 1 {
		t.Fatalf("due-later timer outbox entries = %d (outbox %v), want 1", timerEntries, ready)
	}

	orphanMakeTornStale(t, rig, id)
	sweep, force := orphanSweepAndForce(t, rig, id)
	if sweep.Inspected == 0 {
		t.Fatal("sweep inspected no candidate: the execution is invisible to the sweep, so 0 reclaimed proves nothing")
	}
	if sweep.Reclaimed != 0 || force.Reclaimed != 0 {
		t.Fatalf("reclaimed sweep=%d force=%d while parked, want 0/0", sweep.Reclaimed, force.Reclaimed)
	}
	if got := orphanStatus(t, rig.ctx, rig.state, id); got != types.ExecutionStatusRunning {
		t.Fatalf("status after guarded reclaim = %q, want running", got)
	}
}

// orphanTimerThenNodeGraph is a timer wait followed by an ordinary node, so a
// test can observe the execution after the wake while it is still running.
func orphanTimerThenNodeGraph(t *testing.T, name string) *graph.Graph {
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

// TestOrphanReclaimConvergesAfterTimerResume covers the timer wake path's
// suspension cleanup. The park SADDs suspended_nodes; the delayed outbox entry
// later enqueues the resume task, and the accepted acquire SREMs the member in
// the same transition that sets the node running and stamps the activity clock.
// Signal delivery contracts the same way (deliverSignalWithOutboxLua SREMs when
// it consumes the waiter), and this pins the symmetry.
//
// Without the acquire SREM the member outlived the wait, and the reclaim
// script's SCARD guard kept sparing a graph-lost execution in which nothing was
// suspended — over-protection, not a false reclaim, but the orphan never
// healed. The assertions below fail in exactly that direction: the member must
// be gone, and the same torn shape the parked test protects must now converge.
//
// Mutation: removing the `SREM` from acquireTaskLeaseLua makes this test red.
func TestOrphanReclaimConvergesAfterTimerResume(t *testing.T) {
	rig := newEngOrphanRig(t)
	id := rig.submit(orphanTimerThenNodeGraph(t, "orphan-timer-stale-member"))
	task := rig.queue.takeFor(id)
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

	node, err := rig.state.GetNode(rig.ctx, id, "start")
	if err != nil || node == nil || node.Status != types.NodeStatusSuccess {
		t.Fatalf("start node after resume = %+v err=%v, want success", node, err)
	}
	member, err := rig.rdb.SIsMember(rig.ctx, suspendedNodesKey(namespace.Default, id), "start").Result()
	if err != nil {
		t.Fatalf("read suspended_nodes: %v", err)
	}
	if member {
		t.Fatal("the timer wake left its suspended_nodes member: the resume acquire must clear it")
	}

	// The torn shape the parked test refuses to reclaim, now with nothing
	// suspended: the stale member must no longer over-protect it.
	orphanMakeTornStale(t, rig, id)
	res := orphanReclaim(t, rig.ctx, rig.state, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, Namespace: "default", Limit: 8, Grace: time.Minute,
	})
	if res.Reclaimed != 1 {
		t.Fatalf("Reclaimed = %d, want 1: the resumed execution has nothing suspended and is graph-lost", res.Reclaimed)
	}
	if got := orphanStatus(t, rig.ctx, rig.state, id); got != types.ExecutionStatusCanceled {
		t.Fatalf("status = %q, want canceled", got)
	}
}

// TestOrphanReclaimSparesSignalSuspendedExecution is the signal-wait half: the
// same suspended_nodes registration protects it under the identical clock.
// Mutation: removing the `SCARD` guard from reclaimOrphanExecutionLua makes
// this test red.
func TestOrphanReclaimSparesSignalSuspendedExecution(t *testing.T) {
	rig := newEngOrphanRig(t)
	id, _ := orphanParkSuspension(t, rig, "orphan-signal-wait", &types.SuspendSpec{
		Mode: types.ModeSignal, Signals: []string{"go"},
	})
	orphanAssertSuspendedShape(t, rig, id)
	orphanMakeTornStale(t, rig, id)
	sweep, force := orphanSweepAndForce(t, rig, id)
	if sweep.Inspected == 0 {
		t.Fatal("sweep inspected no candidate: the execution is invisible to the sweep, so 0 reclaimed proves nothing")
	}
	if sweep.Reclaimed != 0 || force.Reclaimed != 0 {
		t.Fatalf("reclaimed sweep=%d force=%d while parked, want 0/0", sweep.Reclaimed, force.Reclaimed)
	}
	if got := orphanStatus(t, rig.ctx, rig.state, id); got != types.ExecutionStatusRunning {
		t.Fatalf("status after guarded reclaim = %q, want running", got)
	}
}

// TestOrphanReclaimSparesSignalDeliveredResumePendingExecution covers the
// delivery → resume-acquire window.
//
// Signal delivery consumes the waiter, SREMs suspended_nodes and writes a
// durable resume entry in one transition (deliverSignalWithOutboxLua), and that
// transition now stamps activity_at_ms too. The stamp is the only protection in
// the window: the suspension guard is gone by design and no lease exists until
// the resume task is acquired. A signal delivered late in a long wait would
// otherwise leave both clocks stale at the moment the park ends, and an orphan
// sweep under graph loss would converge an execution whose resume is already
// durable and pending.
//
// The test builds the harshest clock — graph gone, status clock past the grace —
// and ages the activity clock as well before delivering. Delivery must refresh
// activity, so both sweep and force refuse. Aging the stamp past the grace
// afterwards proves the block was the activity clock and nothing else.
//
// Mutation: removing the activity SET from writeOutbox in
// deliverSignalWithOutboxLua makes the guarded half red.
func TestOrphanReclaimSparesSignalDeliveredResumePendingExecution(t *testing.T) {
	rig := newEngOrphanRig(t)
	id, _ := orphanParkSuspension(t, rig, "orphan-signal-delivered", &types.SuspendSpec{
		Mode: types.ModeSignal, Signals: []string{"go"},
	})
	orphanMakeTornStale(t, rig, id)

	nodeName, _, committed, err := rig.state.DeliverSignalWithOutbox(rig.ctx, id, "go",
		map[string]any{"ok": true},
		engine.ResumeIntent{NodeName: "start", NodeIdx: 0})
	if err != nil {
		t.Fatalf("DeliverSignalWithOutbox error = %v", err)
	}
	if !committed || nodeName != "start" {
		t.Fatalf("delivery = (%q, committed=%v), want (start, true)", nodeName, committed)
	}
	if member, _ := rig.rdb.SIsMember(rig.ctx, suspendedNodesKey(namespace.Default, id), "start").Result(); member {
		t.Fatal("signal delivery left the node in suspended_nodes: the shape under test is wrong")
	}

	// The resume intent is durable and unconsumed; the delivery stamp is inside
	// the grace, so the execution is demonstrably not abandoned.
	sweep, force := orphanSweepAndForce(t, rig, id)
	if sweep.Reclaimed != 0 || force.Reclaimed != 0 {
		t.Fatalf("reclaimed sweep=%d force=%d after delivery, want 0/0: delivery must stamp the activity clock", sweep.Reclaimed, force.Reclaimed)
	}
	if got := orphanStatus(t, rig.ctx, rig.state, id); got != types.ExecutionStatusRunning {
		t.Fatalf("status after guarded reclaim = %q, want running", got)
	}

	// Age the delivery stamp: now nothing protects the pending resume.
	orphanAgeActivity(t, rig.ctx, rig.rdb, id, 10*time.Minute)
	res := orphanReclaim(t, rig.ctx, rig.state, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, Namespace: "default", Limit: 8, Grace: time.Minute,
	})
	if res.Reclaimed != 1 {
		t.Fatalf("Reclaimed = %d once the delivery stamp is past the grace, want 1", res.Reclaimed)
	}
	if got := orphanStatus(t, rig.ctx, rig.state, id); got != types.ExecutionStatusCanceled {
		t.Fatalf("status = %q, want canceled", got)
	}
}

// TestOrphanReclaimSparesWaitingParentHoldingLease covers both parent shapes:
// a sub-workflow node parked on its child, and a map node parked on its
// expansion. Neither is a "suspended node": both sit in waiting with a live
// lease. The ZCOUNT lease guard is their only protection while the child or
// the batches run.
//
// The second half pins what happens when that lease expires. For a sub-workflow
// wait the lease TTL is 5 minutes and the activity clock was stamped once at
// acquire, so at expiry the activity is older than the 2-minute default grace:
// a graph-lost parent becomes sweepable in the lease-expiry-to-redispatch
// window. The parked wait is not otherwise marked, and no activity refresh
// happens while it waits. Under graph loss this is the designed repair path —
// the execution cannot be routed at all — but the protection genuinely ends at
// lease expiry, which is reported as a limitation rather than hidden here.
//
// Mutation: removing the `ZCOUNT` guard from reclaimOrphanExecutionLua, or
// hasActiveLeaseLocked from the local backend, makes the first half red.
func TestOrphanReclaimSparesWaitingParentHoldingLease(t *testing.T) {
	for _, tc := range []struct {
		name string
		ttl  time.Duration
		park func(t *testing.T, rig *engOrphanRig, id types.ExecutionID, lease *engine.TaskLease)
	}{
		{
			name: "subworkflow",
			ttl:  5 * time.Minute, // engine.subWorkflowWaitTTL's ceiling
			park: func(t *testing.T, rig *engOrphanRig, id types.ExecutionID, lease *engine.TaskLease) {
				t.Helper()
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
					t.Fatalf("BeginSubWorkflowWait accepted=%v err=%v, want true/nil", accepted, err)
				}
			},
		},
		{
			name: "map",
			ttl:  engine.DefaultLeaseTTL,
			park: func(t *testing.T, rig *engOrphanRig, id types.ExecutionID, lease *engine.TaskLease) {
				t.Helper()
				accepted, err := rig.state.BeginTaskExpansion(rig.ctx, lease)
				if err != nil || !accepted {
					t.Fatalf("BeginTaskExpansion accepted=%v err=%v, want true/nil", accepted, err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newEngOrphanRig(t, engine.WithDefaultLeaseTTL(tc.ttl))
			id := rig.submit(orphanWaitGraph(t, "orphan-parent-"+tc.name))
			task := rig.queue.takeFor(id)
			if task == nil {
				t.Fatal("no queued root task")
			}
			lease, err := rig.eng.BuildTaskLease(rig.ctx, task)
			if err != nil {
				t.Fatalf("BuildTaskLease error = %v", err)
			}
			if _, claimed, err := rig.state.ClaimTaskLease(rig.ctx, lease); err != nil || !claimed {
				t.Fatalf("ClaimTaskLease claimed=%v err=%v, want true/nil", claimed, err)
			}
			tc.park(t, rig, id, lease)

			node, err := rig.state.GetNode(rig.ctx, id, "start")
			if err != nil || node == nil || node.Status != types.NodeStatusWaiting {
				t.Fatalf("node = %+v err=%v, want waiting", node, err)
			}
			if member, _ := rig.rdb.SIsMember(rig.ctx, suspendedNodesKey(namespace.Default, id), "start").Result(); member {
				t.Fatal("a waiting parent is registered as suspended; the shape under test is wrong")
			}

			orphanMakeTornStale(t, rig, id)
			sweep, force := orphanSweepAndForce(t, rig, id)
			if sweep.Reclaimed != 0 || force.Reclaimed != 0 {
				t.Fatalf("reclaimed sweep=%d force=%d with a live wait lease, want 0/0", sweep.Reclaimed, force.Reclaimed)
			}
			if got := orphanStatus(t, rig.ctx, rig.state, id); got != types.ExecutionStatusRunning {
				t.Fatalf("status after guarded reclaim = %q, want running", got)
			}

			// Expire the wait lease the way the lease sweeper would see it. The
			// clocks are already stale, so nothing protects the parent now.
			if err := rig.rdb.ZAdd(rig.ctx, leaseExpiryZSetKey(namespace.Default, id), redis.Z{
				Score:  float64(time.Now().Add(-time.Minute).UnixMilli()),
				Member: leaseExpiryMember(id, task.NodeName),
			}).Err(); err != nil {
				t.Fatalf("expire wait lease: %v", err)
			}
			res := orphanReclaim(t, rig.ctx, rig.state, engine.OrphanReclaimRequest{
				Mode: engine.OrphanReclaimSweep, Namespace: "default", Limit: 8, Grace: time.Minute,
			})
			if res.Reclaimed != 1 {
				t.Fatalf("Reclaimed = %d after the wait lease expired, want 1 (protection ends at lease expiry)", res.Reclaimed)
			}
		})
	}
}

// TestOrphanSweepReclaimsBetweenNodeWindowOnceActivityAgesPastGrace answers the
// inter-node half. An accepted commit stamps activity_at_ms and schedules the
// next node; while the next acquire happens inside the grace the execution is
// covered even with no lease and no graph. Nothing else protects that window,
// so a delivery delayed beyond the grace (an outbox backlog, a queue backlog, a
// retry backoff longer than the grace) leaves a graph-lost execution
// reclaimable. With the graph present the sweep never gets this far.
func TestOrphanSweepReclaimsBetweenNodeWindowOnceActivityAgesPastGrace(t *testing.T) {
	rig := newEngOrphanRig(t)
	g := orphanTwoNodeLimitGraph(t, "orphan-between-nodes")
	id := rig.submit(g)
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

	rig.dropRunningGraph(id)
	orphanAgeStatus(t, rig.ctx, rig.rdb, id, 10*time.Minute)

	// The commit's activity stamp is still inside the grace: refuse.
	res := orphanReclaim(t, rig.ctx, rig.state, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, Namespace: "default", Limit: 8, Grace: time.Minute,
	})
	if res.Reclaimed != 0 {
		t.Fatalf("Reclaimed = %d inside the activity grace, want 0", res.Reclaimed)
	}

	// Age the commit stamp: the delivery of "next" is now later than the grace,
	// and nothing marks the execution as anything but abandoned.
	orphanAgeActivity(t, rig.ctx, rig.rdb, id, 10*time.Minute)
	res = orphanReclaim(t, rig.ctx, rig.state, engine.OrphanReclaimRequest{
		Mode: engine.OrphanReclaimSweep, Namespace: "default", Limit: 8, Grace: time.Minute,
	})
	if res.Reclaimed != 1 {
		t.Fatalf("Reclaimed = %d past the activity grace, want 1", res.Reclaimed)
	}
}
