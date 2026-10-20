package statestoretest

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// RouteCheckedLeaseStore is a backend whose lease acquire re-decides the
// execution-liveness and node-route questions inside the same transition that
// writes the lease.
type RouteCheckedLeaseStore interface {
	engine.StateStore
	engine.RouteCheckedLeaseAcquirer
}

// RunRouteCheckedLeaseContract pins engine.RouteCheckedLeaseAcquirer: the
// acquire must report the execution verdict it observed, refuse an acquire for
// an absent or terminal execution, and apply exactly the node-route rules the
// engine used to check before asking for the lease (acyclic: a terminal node is
// never routable; cyclic: a terminal node re-enters only on a strictly higher
// activation, and a task must carry an activation).
//
// The same suite runs against every backend because the whole point of folding
// the checks into the store is that the two stores must agree on the verdict —
// the engine no longer re-reads the node to second-guess it. Each subtest gets
// a fresh store, so the assertions describe a single transition rather than a
// sequence another case could have left state in.
func RunRouteCheckedLeaseContract(t *testing.T, manifest *Manifest, newStore func(*testing.T) RouteCheckedLeaseStore) {
	t.Helper()
	manifest.Report(t)
	if !manifest.SkipUnlessDeclared(t, CapabilityRouteCheckedLeaseAcquirer) {
		return
	}
	ctx := context.Background()

	createRunning := func(t *testing.T, s RouteCheckedLeaseStore, id types.ExecutionID) {
		t.Helper()
		if err := s.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID:     id,
			Graph:  ContractGraph(),
			Status: types.ExecutionStatusRunning,
		}); err != nil {
			t.Fatalf("create execution %q: %v", id, err)
		}
	}
	setExecutionStatus := func(t *testing.T, s RouteCheckedLeaseStore, id types.ExecutionID, status types.ExecutionStatus) {
		t.Helper()
		if err := s.UpdateExecutionStatus(ctx, id, status, "contract"); err != nil {
			t.Fatalf("update execution %q to %q: %v", id, status, err)
		}
	}
	seedNode := func(t *testing.T, s RouteCheckedLeaseStore, id types.ExecutionID, status types.NodeStatus, activation int) {
		t.Helper()
		if err := s.UpsertNode(ctx, &engine.NodeSnapshot{
			ExecutionID:  id,
			Name:         "start",
			NodeIdx:      0,
			Status:       status,
			ActivationID: activation,
		}); err != nil {
			t.Fatalf("seed node %q/%q as %q (activation %d): %v", id, "start", status, activation, err)
		}
	}
	newLease := func(id types.ExecutionID, activation int, token string) *engine.TaskLease {
		return &engine.TaskLease{
			LeaseID:    engine.LeaseID("lease-" + token),
			LeaseToken: engine.LeaseToken(token),
			IssuedAt:   time.Now().UTC(),
			TTL:        time.Minute,
			Task: engine.Task{
				ExecutionID:  id,
				NodeName:     "start",
				NodeIdx:      0,
				Type:         engine.TaskTypeNodeExec,
				ActivationID: activation,
			},
		}
	}
	expectRefused := func(t *testing.T, res engine.LeaseAcquireResult, err error, reason string) {
		t.Helper()
		if err != nil {
			t.Fatalf("AcquireTaskLeaseChecked() error = %v, want a refusal with no error (%s)", err, reason)
		}
		if res.Acquired {
			t.Fatalf("AcquireTaskLeaseChecked() acquired the lease, want refusal: %s", reason)
		}
		if !res.ExecutionFound {
			t.Fatalf("ExecutionFound = false for a created execution: %s", reason)
		}
	}
	// expectNodeStatus also proves a refused route check is a decision rather
	// than a partial transition: the node is still in the state that caused the
	// refusal.
	expectNodeStatus := func(t *testing.T, s RouteCheckedLeaseStore, id types.ExecutionID, want types.NodeStatus) {
		t.Helper()
		node, err := s.GetNode(ctx, id, "start")
		if err != nil {
			t.Fatalf("GetNode: %v", err)
		}
		if node == nil {
			t.Fatalf("node %q/%q is missing after the acquire, want status %q", id, "start", want)
		}
		if node.Status != want {
			t.Fatalf("node status = %q after the refused acquire, want %q (no mutation)", node.Status, want)
		}
	}

	// 1. An execution that does not exist has no work to lease. The engine
	//    evicts the graph and reports execution_inactive on this verdict.
	t.Run("MissingExecutionIsReported", func(t *testing.T) {
		s := newStore(t)
		id := types.ExecutionID("route-checked-missing")
		res, err := s.AcquireTaskLeaseChecked(ctx, newLease(id, 1, "missing"), engine.LeaseAcquireOptions{})
		if err != nil {
			t.Fatalf("AcquireTaskLeaseChecked() error = %v", err)
		}
		if res.Acquired {
			t.Fatal("AcquireTaskLeaseChecked() acquired a lease for an execution that does not exist")
		}
		if res.ExecutionFound {
			t.Fatal("ExecutionFound = true for an execution that was never created")
		}
		if res.ExecutionStatus != "" {
			t.Fatalf("ExecutionStatus = %q with ExecutionFound=false, want empty", res.ExecutionStatus)
		}
		if res.Previous != nil {
			t.Fatalf("Previous = %+v for an execution that does not exist, want nil", res.Previous)
		}
	})

	// 2. A terminal execution must never receive a lease: the runner would
	//    execute and commit work the control plane already accounted for.
	t.Run("TerminalExecutionIsRefused", func(t *testing.T) {
		for _, terminal := range []types.ExecutionStatus{
			types.ExecutionStatusSuccess,
			types.ExecutionStatusFailed,
			types.ExecutionStatusCanceled,
			types.ExecutionStatusTimeout,
		} {
			t.Run(string(terminal), func(t *testing.T) {
				s := newStore(t)
				id := types.ExecutionID("route-checked-terminal-" + string(terminal))
				createRunning(t, s, id)
				setExecutionStatus(t, s, id, terminal)
				res, err := s.AcquireTaskLeaseChecked(ctx, newLease(id, 1, "terminal"), engine.LeaseAcquireOptions{})
				if err != nil {
					t.Fatalf("AcquireTaskLeaseChecked() error = %v", err)
				}
				if res.Acquired {
					t.Fatalf("AcquireTaskLeaseChecked() acquired a lease on a %q execution", terminal)
				}
				if !res.ExecutionFound {
					t.Fatalf("ExecutionFound = false for a created %q execution", terminal)
				}
				if res.ExecutionStatus != terminal {
					t.Fatalf("ExecutionStatus = %q, want %q", res.ExecutionStatus, terminal)
				}
			})
		}
	})

	// canceling and paused are live statuses: their in-flight work must still be
	// able to acquire a lease, which is exactly what IsTerminalExecutionStatus
	// says.
	t.Run("LiveExecutionStatusesStayAcquirable", func(t *testing.T) {
		for _, live := range []types.ExecutionStatus{
			types.ExecutionStatusCanceling,
			types.ExecutionStatusPaused,
		} {
			t.Run(string(live), func(t *testing.T) {
				s := newStore(t)
				id := types.ExecutionID("route-checked-live-" + string(live))
				createRunning(t, s, id)
				setExecutionStatus(t, s, id, live)
				res, err := s.AcquireTaskLeaseChecked(ctx, newLease(id, 1, "live"), engine.LeaseAcquireOptions{})
				if err != nil {
					t.Fatalf("AcquireTaskLeaseChecked() error = %v", err)
				}
				if !res.Acquired {
					t.Fatalf("a %q execution was refused a lease (%+v); canceling/paused are live statuses", live, res)
				}
				if !res.ExecutionFound || res.ExecutionStatus != live {
					t.Fatalf("ExecutionFound/Status = %v/%q, want true/%q", res.ExecutionFound, res.ExecutionStatus, live)
				}
			})
		}
	})

	// 3. The acyclic rule: a terminal node is never routable again, whatever
	//    activation the task carries. The activation-0/activation-1 pair is the
	//    half the old Lua guard missed (it only refused when the node's
	//    activation was at least the task's); the acyclic task with activation 0
	//    is the shape every acyclic root task has and must still be refusable.
	t.Run("AcyclicTerminalNodeIsRefusedAtEveryActivation", func(t *testing.T) {
		cases := []struct {
			name      string
			nodeAct   int
			taskAct   int
			nodeState types.NodeStatus
		}{
			{name: "SameActivation", nodeAct: 1, taskAct: 1},
			{name: "TaskWithoutActivation", nodeAct: 1, taskAct: 0},
			{name: "TerminalNodeBelowTaskActivation", nodeAct: 0, taskAct: 1},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				s := newStore(t)
				id := types.ExecutionID("route-checked-acyclic-" + tc.name)
				createRunning(t, s, id)
				seedNode(t, s, id, types.NodeStatusSuccess, tc.nodeAct)
				res, err := s.AcquireTaskLeaseChecked(ctx, newLease(id, tc.taskAct, "acyclic"), engine.LeaseAcquireOptions{})
				expectRefused(t, res, err, "an acyclic task must never route to a terminal node")
				expectNodeStatus(t, s, id, types.NodeStatusSuccess)
				if res.Previous == nil || res.Previous.Status != types.NodeStatusSuccess {
					t.Fatalf("Previous = %+v, want the terminal node snapshot", res.Previous)
				}
			})
		}
	})

	// 4. The cyclic rule: a terminal node re-enters only under a strictly higher
	//    activation, because that is a fresh execution of the node rather than a
	//    retry of the finished one.
	t.Run("CyclicTerminalNodeReentersOnHigherActivation", func(t *testing.T) {
		s := newStore(t)
		id := types.ExecutionID("route-checked-cyclic-reentry")
		createRunning(t, s, id)
		seedNode(t, s, id, types.NodeStatusSuccess, 1)
		res, err := s.AcquireTaskLeaseChecked(ctx, newLease(id, 2, "cyclic"), engine.LeaseAcquireOptions{AllowCycles: true})
		if err != nil {
			t.Fatalf("AcquireTaskLeaseChecked() error = %v", err)
		}
		if !res.Acquired {
			t.Fatalf("a cyclic re-entry at a higher activation was refused (%+v); the node is finished "+
				"at activation 1, so activation 2 is a fresh execution", res)
		}
		if res.Previous == nil || res.Previous.Status != types.NodeStatusSuccess || res.Previous.ActivationID != 1 {
			t.Fatalf("Previous = %+v, want success at activation 1", res.Previous)
		}
		node, err := s.GetNode(ctx, id, "start")
		if err != nil {
			t.Fatalf("GetNode: %v", err)
		}
		if node == nil || node.Status != types.NodeStatusRunning || node.ActivationID != 2 || node.Attempt != 1 {
			t.Fatalf("node after cyclic re-entry = %+v, want running at activation 2 with attempt 1 "+
				"(a re-entry is not a retry, so the attempt counter restarts)", node)
		}
	})

	t.Run("CyclicTerminalNodeSameActivationIsRefused", func(t *testing.T) {
		s := newStore(t)
		id := types.ExecutionID("route-checked-cyclic-same")
		createRunning(t, s, id)
		seedNode(t, s, id, types.NodeStatusSuccess, 2)
		res, err := s.AcquireTaskLeaseChecked(ctx, newLease(id, 2, "cyclic-same"), engine.LeaseAcquireOptions{AllowCycles: true})
		expectRefused(t, res, err, "the node already finished this activation")
		expectNodeStatus(t, s, id, types.NodeStatusSuccess)
	})

	// 5. committing and waiting are parked states held by another transition; a
	//    plain task must not take the node from under them.
	t.Run("CommittingAndWaitingNodesAreRefused", func(t *testing.T) {
		for _, parked := range []types.NodeStatus{
			types.NodeStatusCommitting,
			types.NodeStatusWaiting,
		} {
			t.Run(string(parked), func(t *testing.T) {
				s := newStore(t)
				id := types.ExecutionID("route-checked-parked-" + string(parked))
				createRunning(t, s, id)
				seedNode(t, s, id, parked, 1)
				res, err := s.AcquireTaskLeaseChecked(ctx, newLease(id, 1, "parked"), engine.LeaseAcquireOptions{AllowCycles: true})
				expectRefused(t, res, err, "a "+string(parked)+" node is owned by another transition")
				expectNodeStatus(t, s, id, parked)
				if res.Previous == nil || res.Previous.Status != parked {
					t.Fatalf("Previous = %+v, want the %q node snapshot", res.Previous, parked)
				}
			})
		}
	})

	// 6. The lease guards themselves are unchanged: a live owner keeps the node,
	//    an expired one loses it, and the attempt counter distinguishes a retry
	//    (same activation) from a re-entry.
	t.Run("LiveLeaseIsRefused", func(t *testing.T) {
		s := newStore(t)
		id := types.ExecutionID("route-checked-live-lease")
		createRunning(t, s, id)

		first := newLease(id, 1, "owner")
		res, err := s.AcquireTaskLeaseChecked(ctx, first, engine.LeaseAcquireOptions{})
		if err != nil || !res.Acquired {
			t.Fatalf("first acquire: acquired=%v err=%v", res.Acquired, err)
		}

		rival := newLease(id, 1, "rival")
		res, err = s.AcquireTaskLeaseChecked(ctx, rival, engine.LeaseAcquireOptions{})
		expectRefused(t, res, err, "the owner's lease is live")
		if res.Previous == nil || res.Previous.Status != types.NodeStatusRunning || res.Previous.LeaseToken != first.LeaseToken {
			t.Fatalf("Previous = %+v, want the live owner's token %q", res.Previous, first.LeaseToken)
		}
	})

	t.Run("ExpiredLeaseIsReclaimedAsARetry", func(t *testing.T) {
		s := newStore(t)
		id := types.ExecutionID("route-checked-expired-lease")
		createRunning(t, s, id)

		expired := newLease(id, 1, "expired-owner")
		expired.IssuedAt = time.Now().UTC().Add(-2 * time.Minute)
		expired.TTL = time.Minute
		if res, err := s.AcquireTaskLeaseChecked(ctx, expired, engine.LeaseAcquireOptions{}); err != nil || !res.Acquired {
			t.Fatalf("acquire with an already-expired lease: acquired=%v err=%v", res.Acquired, err)
		}
		retry := newLease(id, 1, "retry")
		res, err := s.AcquireTaskLeaseChecked(ctx, retry, engine.LeaseAcquireOptions{})
		if err != nil {
			t.Fatalf("acquire after expiry: %v", err)
		}
		if !res.Acquired {
			t.Fatalf("an expired lease was not reclaimable (%+v)", res)
		}
		node, err := s.GetNode(ctx, id, "start")
		if err != nil || node == nil {
			t.Fatalf("GetNode after reclaim: %v", err)
		}
		if node.Attempt != 2 {
			t.Fatalf("attempt after reclaiming one activation = %d, want 2: a retry within an "+
				"activation keeps counting", node.Attempt)
		}
	})

	t.Run("CyclicTaskNeedsAnActivation", func(t *testing.T) {
		s := newStore(t)
		id := types.ExecutionID("route-checked-cyclic-zero")
		createRunning(t, s, id)
		res, err := s.AcquireTaskLeaseChecked(ctx, newLease(id, 0, "cyclic-zero"), engine.LeaseAcquireOptions{AllowCycles: true})
		expectRefused(t, res, err, "a cyclic task cannot identify its node generation without an activation")
		if node, err := s.GetNode(ctx, id, "start"); err != nil {
			t.Fatalf("GetNode: %v", err)
		} else if node != nil {
			t.Fatalf("node = %+v after a refused cyclic acquire, want no node to exist", node)
		}
	})

	// The acyclic counterpart of the guard above: acyclic root tasks carry no
	// activation, so activation 0 must stay acquirable when cycles are off.
	t.Run("AcyclicTaskWithoutActivationIsAcquirable", func(t *testing.T) {
		s := newStore(t)
		id := types.ExecutionID("route-checked-acyclic-zero")
		createRunning(t, s, id)
		res, err := s.AcquireTaskLeaseChecked(ctx, newLease(id, 0, "acyclic-zero"), engine.LeaseAcquireOptions{})
		if err != nil {
			t.Fatalf("AcquireTaskLeaseChecked() error = %v", err)
		}
		if !res.Acquired {
			t.Fatalf("an acyclic task without an activation was refused (%+v); every acyclic root "+
				"task has this shape", res)
		}
	})
}
