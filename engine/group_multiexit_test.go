package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// ---------------------------------------------------------------------------
// Driving engine helpers: newDrivingEngine / drain / seedExecution /
// enqueueGroupRoot / assertExecuted / assertExecutionStatus / unitOf /
// buildGroupThenNodeGraph / buildGroupFanOutFanIn
// ---------------------------------------------------------------------------

// drivingState wraps fakeState with AtomicStateStore + GroupStateStore support
// to enable full pipeline (group exec → commit → advance → schedule → execute)
// testing within the engine package.
type drivingState struct {
	mu         sync.Mutex
	executions map[types.ExecutionID]*ExecutionSnapshot
	nodes      map[string]*NodeSnapshot
	inDegrees  map[string]int
	activeIns  map[string]int
	remaining  map[types.ExecutionID]int
	failed     map[types.ExecutionID]int
	advanced   map[string]bool
	scheduled  map[string]string
	outbox     map[types.ExecutionID]map[string]OutboxEntry
	outputs    map[string]map[string]any
	groupUnits map[string]*drivingGroupUnit
	doneCh     map[types.ExecutionID]chan struct{}
	executed   map[string]int // track node execution count: key="execID/nodeName"
}

type drivingGroupUnit struct {
	status     int // 0=pending, 1=running, 2=done
	leaseToken LeaseToken
	attempt    int
}

func newDrivingState() *drivingState {
	return &drivingState{
		executions: make(map[types.ExecutionID]*ExecutionSnapshot),
		nodes:      make(map[string]*NodeSnapshot),
		inDegrees:  make(map[string]int),
		activeIns:  make(map[string]int),
		remaining:  make(map[types.ExecutionID]int),
		failed:     make(map[types.ExecutionID]int),
		advanced:   make(map[string]bool),
		scheduled:  make(map[string]string),
		outbox:     make(map[types.ExecutionID]map[string]OutboxEntry),
		outputs:    make(map[string]map[string]any),
		groupUnits: make(map[string]*drivingGroupUnit),
		doneCh:     make(map[types.ExecutionID]chan struct{}),
		executed:   make(map[string]int),
	}
}

func drivingKey(id types.ExecutionID, idx int) string {
	return fmt.Sprintf("%s/%d", id, idx)
}

// --- StateStore ---

func (s *drivingState) CreateExecution(_ context.Context, e *ExecutionSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.createLocked(e)
	return nil
}

func (s *drivingState) createLocked(e *ExecutionSnapshot) {
	s.executions[e.ID] = e
	if e.Graph != nil {
		// Seed in-degree by UNIT index (correct post-migration behavior).
		for i := 0; i < e.Graph.UnitCount(); i++ {
			s.inDegrees[drivingKey(e.ID, i)] = e.Graph.UnitInDegreeAt(i)
		}
		if !e.Graph.AllowCycles() {
			s.remaining[e.ID] = e.Graph.UnitCount()
			s.failed[e.ID] = 0
		}
	}
}

func (s *drivingState) UpdateExecutionStatus(_ context.Context, id types.ExecutionID, status types.ExecutionStatus, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.executions[id]; ok {
		e.Status = status
	}
	if ch, ok := s.doneCh[id]; ok && types.IsTerminalExecutionStatus(status) {
		select {
		case <-ch:
		default:
			close(ch)
		}
	}
	return nil
}

func (s *drivingState) GetExecution(_ context.Context, id types.ExecutionID) (*ExecutionSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.executions[id], nil
}

func (s *drivingState) LoadGraph(_ context.Context, id types.ExecutionID) (*graph.Graph, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.executions[id]; ok {
		return e.Graph, nil
	}
	return nil, nil
}

func (s *drivingState) UpsertNode(_ context.Context, n *NodeSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(n.ExecutionID) + "/" + n.Name
	if existing, ok := s.nodes[key]; ok && types.IsTerminalNodeStatus(existing.Status) && n.ActivationID <= existing.ActivationID {
		return nil
	}
	cp := *n
	s.nodes[key] = &cp
	return nil
}

func (s *drivingState) GetNode(_ context.Context, id types.ExecutionID, name string) (*NodeSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nodes[string(id)+"/"+name], nil
}

func (s *drivingState) AcquireTaskLease(_ context.Context, lease *TaskLease) (*NodeSnapshot, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(lease.Task.ExecutionID) + "/" + lease.Task.NodeName
	current := s.nodes[key]
	if current != nil && types.IsTerminalNodeStatus(current.Status) {
		return current, false, nil
	}
	s.nodes[key] = &NodeSnapshot{
		ExecutionID:  lease.Task.ExecutionID,
		Name:         lease.Task.NodeName,
		NodeIdx:      lease.Task.NodeIdx,
		Status:       types.NodeStatusRunning,
		LeaseToken:   lease.LeaseToken,
		Attempt:      1,
		ActivationID: lease.Task.ActivationID,
	}
	return current, true, nil
}

func (s *drivingState) ListExpiredLeases(_ context.Context, _ time.Time) ([]ExpiredLease, error) {
	return nil, nil
}

func (s *drivingState) RevokeLease(_ context.Context, _ types.ExecutionID, _ string, _ LeaseToken) (bool, error) {
	return false, nil
}

func (s *drivingState) ClaimTaskLease(_ context.Context, lease *TaskLease) (*NodeSnapshot, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(lease.Task.ExecutionID) + "/" + lease.Task.NodeName
	ns := s.nodes[key]
	if ns == nil {
		return nil, false, nil
	}
	if types.IsTerminalNodeStatus(ns.Status) {
		return ns, true, nil
	}
	if ns.Status != types.NodeStatusRunning || ns.LeaseToken != lease.LeaseToken {
		return ns, false, nil
	}
	cp := *ns
	cp.Status = types.NodeStatusCommitting
	s.nodes[key] = &cp
	return &cp, true, nil
}

func (s *drivingState) DecrementInDegree(_ context.Context, id types.ExecutionID, nodeIdx int, portActive bool) (int, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := drivingKey(id, nodeIdx)
	s.inDegrees[key]--
	if portActive {
		s.activeIns[key]++
	}
	return s.inDegrees[key], s.activeIns[key], nil
}

func (s *drivingState) CheckCompletion(_ context.Context, _ types.ExecutionID, _ int) (bool, bool, error) {
	return false, false, nil
}

func (s *drivingState) PutOutput(_ context.Context, id types.ExecutionID, name string, data map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outputs[string(id)+"/"+name] = data
	return nil
}

func (s *drivingState) GetOutput(_ context.Context, id types.ExecutionID, name string) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.outputs[string(id)+"/"+name], nil
}

// Stub out interfaces not needed for the driving test.
func (s *drivingState) SuspendOrConsume(_ context.Context, _ types.ExecutionID, _ string, _ *types.SuspendSpec) (*types.SignalPayload, error) {
	return nil, nil
}
func (s *drivingState) DeliverSignal(_ context.Context, _ types.ExecutionID, _ string, _ map[string]any) (string, *types.SignalPayload, error) {
	return "", nil, nil
}
func (s *drivingState) AcquireResumeLock(_ context.Context, _ types.ExecutionID, _ string) (bool, error) {
	return true, nil
}
func (s *drivingState) RevokeSignal(_ context.Context, _ types.ExecutionID, _ string) (bool, error) {
	return false, nil
}
func (s *drivingState) ResuspendAtomic(_ context.Context, _ types.ExecutionID, _ string, _ string, _ string, _ *types.SuspendSpec) (*types.SignalPayload, error) {
	return nil, nil
}
func (s *drivingState) ListSuspendedNodes(_ context.Context, _ types.ExecutionID) ([]string, error) {
	return nil, nil
}
func (s *drivingState) WatchExecution(_ context.Context, _ types.ExecutionID) (<-chan ExecutionEvent, error) {
	ch := make(chan ExecutionEvent, 1)
	return ch, nil
}
func (s *drivingState) PublishExecutionEvent(_ context.Context, _ ExecutionEvent) error {
	return nil
}
func (s *drivingState) CreateSubExecution(_ context.Context, _ *SubExecution) error {
	return nil
}
func (s *drivingState) CompleteSubExecution(_ context.Context, _ types.ExecutionID, _ string, _ types.ExecutionID, _ types.ExecutionStatus, _ map[string]any) (bool, error) {
	return false, nil
}
func (s *drivingState) GetSubExecutionResults(_ context.Context, _ types.ExecutionID, _ string) ([]map[string]any, error) {
	return nil, nil
}

// --- AtomicStateStore ---

func (s *drivingState) CreateExecutionWithOutbox(_ context.Context, e *ExecutionSnapshot, entries []OutboxEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.createLocked(e)
	for _, entry := range entries {
		s.putOutboxLocked(e.ID, entry)
	}
	return nil
}

func (s *drivingState) putOutboxLocked(id types.ExecutionID, entry OutboxEntry) {
	m := s.outbox[id]
	if m == nil {
		m = make(map[string]OutboxEntry)
		s.outbox[id] = m
	}
	if _, exists := m[entry.ID]; !exists {
		m[entry.ID] = entry
	}
}

func (s *drivingState) ResetNodeForRetryWithOutbox(_ context.Context, _ types.ExecutionID, _ string, _ LeaseToken, _ OutboxEntry) (bool, error) {
	return false, nil
}

func (s *drivingState) RevokeLeaseWithOutbox(_ context.Context, _ types.ExecutionID, _ string, _ LeaseToken, _ OutboxEntry) (bool, error) {
	return false, nil
}

func (s *drivingState) CommitNode(_ context.Context, req CommitNodeRequest) (CommitNodeResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(req.ExecutionID) + "/" + req.NodeName
	ns := s.nodes[key]
	if ns == nil {
		return CommitNodeResult{Outcome: CommitOutcomeStaleToken}, nil
	}
	if types.IsTerminalNodeStatus(ns.Status) {
		return CommitNodeResult{Outcome: CommitOutcomeDuplicateTerminal}, nil
	}
	ns.Status = req.Status
	ns.Port = req.Port
	ns.ActivationID = req.ActivationID

	result := CommitNodeResult{Applied: true, Outcome: CommitOutcomeAccepted}

	// Decrement remaining.
	if g := s.executions[req.ExecutionID]; g != nil && g.Graph != nil && !g.Graph.AllowCycles() {
		s.remaining[req.ExecutionID]--
		if req.Status == types.NodeStatusFailed {
			s.failed[req.ExecutionID]++
		}
		if s.remaining[req.ExecutionID] == 0 {
			status := types.ExecutionStatusSuccess
			if s.failed[req.ExecutionID] > 0 {
				status = types.ExecutionStatusFailed
			}
			s.executions[req.ExecutionID].Status = status
			result.ExecutionDone = true
			result.ExecutionStatus = status
		}
	}

	// Write advance outbox if provided.
	if req.AdvanceTask != nil {
		entry := OutboxEntry{
			ID:   fmt.Sprintf("advance/%s/%s/%d", req.ExecutionID, req.NodeName, req.ActivationID),
			Task: *req.AdvanceTask,
		}
		s.putOutboxLocked(req.ExecutionID, entry)
		result.OutboxIDs = append(result.OutboxIDs, entry.ID)
	}
	return result, nil
}

func (s *drivingState) AdvanceNode(_ context.Context, req AdvanceNodeRequest) (AdvanceNodeResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(req.ExecutionID) + "/" + req.NodeName
	ns := s.nodes[key]
	if ns == nil || !types.IsTerminalNodeStatus(ns.Status) || ns.ActivationID != req.ActivationID {
		return AdvanceNodeResult{}, nil
	}
	advKey := fmt.Sprintf("adv/%s/%s/%d", req.ExecutionID, req.NodeName, req.ActivationID)
	if s.advanced[advKey] {
		return AdvanceNodeResult{}, nil
	}
	s.advanced[advKey] = true

	result := AdvanceNodeResult{Applied: true}
	for _, arrival := range req.Arrivals {
		if arrival.ArrivalCount <= 0 {
			continue
		}
		counterKey := drivingKey(req.ExecutionID, arrival.UnitIdx)
		previousActive := s.activeIns[counterKey]
		s.inDegrees[counterKey] -= arrival.ArrivalCount
		if arrival.ActiveCount > 0 {
			s.activeIns[counterKey] += arrival.ActiveCount
		}
		if s.scheduled[counterKey] != "" {
			continue
		}
		schedule := ""
		if arrival.MergeMode == "wait_any" && arrival.ActiveCount > 0 && previousActive == 0 {
			schedule = "execute"
		} else if s.inDegrees[counterKey] <= 0 {
			if s.activeIns[counterKey] > 0 {
				schedule = "execute"
			} else {
				schedule = "skip"
			}
		}
		if schedule == "" {
			continue
		}
		s.scheduled[counterKey] = schedule
		taskType := arrival.ExecTaskType
		if taskType == 0 {
			taskType = TaskTypeNodeExec
		}
		outboxID := fmt.Sprintf("exec/%s/%s/%d", req.ExecutionID, arrival.NodeName, req.ActivationID)
		if schedule == "skip" {
			taskType = TaskTypeNodeSkip
			outboxID = fmt.Sprintf("skip/%s/%s/%d", req.ExecutionID, arrival.NodeName, req.ActivationID)
		}
		entry := OutboxEntry{
			ID: outboxID,
			Task: Task{
				ExecutionID:  req.ExecutionID,
				NodeName:     arrival.NodeName,
				NodeIdx:      arrival.NodeIdx,
				UnitIdx:      arrival.UnitIdx,
				Type:         taskType,
				ActivationID: req.ActivationID,
			},
		}
		s.putOutboxLocked(req.ExecutionID, entry)
		result.OutboxIDs = append(result.OutboxIDs, outboxID)
	}
	return result, nil
}

func (s *drivingState) ListOutbox(_ context.Context, id types.ExecutionID, _ time.Time, limit int) ([]OutboxEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.outbox[id]
	if len(m) == 0 {
		return nil, nil
	}
	out := make([]OutboxEntry, 0, len(m))
	for _, e := range m {
		out = append(out, e)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (s *drivingState) AckOutbox(_ context.Context, id types.ExecutionID, entryID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m := s.outbox[id]; m != nil {
		delete(m, entryID)
	}
	return nil
}

func (s *drivingState) ListOutboxExecutions(_ context.Context, _ int) ([]types.ExecutionID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []types.ExecutionID
	for id, m := range s.outbox {
		if len(m) > 0 {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// --- GroupStateStore ---

func (s *drivingState) AcquireGroupLease(_ context.Context, lease *GroupLease) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("%s/%d", lease.ExecutionID, lease.GroupUnitIdx)
	st := s.groupUnits[key]
	if st == nil {
		st = &drivingGroupUnit{status: 0}
		s.groupUnits[key] = st
	}
	if st.status != 0 {
		return false, nil
	}
	st.status = 1
	st.leaseToken = lease.LeaseToken
	st.attempt = lease.Attempt
	return true, nil
}

func (s *drivingState) RenewGroupLease(_ context.Context, _ types.ExecutionID, _ int, _ LeaseToken, _ time.Time) (bool, error) {
	return true, nil
}

func (s *drivingState) CommitGroup(_ context.Context, req GroupCommitRequest) (GroupCommitResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("%s/%d", req.ExecutionID, req.GroupUnitIdx)
	st := s.groupUnits[key]
	if st == nil || st.leaseToken != req.LeaseToken || st.attempt != req.Attempt || st.status != 1 {
		return GroupCommitResult{Applied: false, Outcome: CommitOutcomeStaleToken}, nil
	}
	e := s.executions[req.ExecutionID]
	if e == nil || types.IsTerminalExecutionStatus(e.Status) {
		return GroupCommitResult{Applied: false, Outcome: CommitOutcomeExecutionInactive}, nil
	}
	// Store outputs.
	for _, ex := range req.Exits {
		s.outputs[string(req.ExecutionID)+"/"+ex.NodeName] = ex.Data
	}
	st.status = 2
	result := GroupCommitResult{Applied: true, Outcome: CommitOutcomeAccepted}
	// Decrement remaining.
	if e.Graph != nil && !e.Graph.AllowCycles() {
		s.remaining[req.ExecutionID]--
		if req.Outcome == GroupOutcomeFailed {
			s.failed[req.ExecutionID]++
		}
		if req.Fatal || s.remaining[req.ExecutionID] == 0 {
			status := types.ExecutionStatusSuccess
			if req.Fatal || s.failed[req.ExecutionID] > 0 {
				status = types.ExecutionStatusFailed
			}
			e.Status = status
			result.ExecutionDone = true
			result.ExecutionStatus = status
		}
	}
	// Downstream arrival counting (same logic as local backend).
	for _, arrival := range req.Downstream {
		if arrival.ArrivalCount <= 0 {
			continue
		}
		counterKey := drivingKey(req.ExecutionID, arrival.UnitIdx)
		previousActive := s.activeIns[counterKey]
		s.inDegrees[counterKey] -= arrival.ArrivalCount
		if arrival.ActiveCount > 0 {
			s.activeIns[counterKey] += arrival.ActiveCount
		}
		if s.scheduled[counterKey] != "" {
			continue
		}
		schedule := ""
		if arrival.MergeMode == "wait_any" && arrival.ActiveCount > 0 && previousActive == 0 {
			schedule = "execute"
		} else if s.inDegrees[counterKey] <= 0 {
			if s.activeIns[counterKey] > 0 {
				schedule = "execute"
			} else {
				schedule = "skip"
			}
		}
		if schedule == "" {
			continue
		}
		s.scheduled[counterKey] = schedule
		taskType := arrival.ExecTaskType
		if taskType == 0 {
			taskType = TaskTypeNodeExec
		}
		outboxID := fmt.Sprintf("gexec/%s/%s/0", req.ExecutionID, arrival.NodeName)
		if schedule == "skip" {
			taskType = TaskTypeNodeSkip
			outboxID = fmt.Sprintf("gskip/%s/%s/0", req.ExecutionID, arrival.NodeName)
		}
		entry := OutboxEntry{
			ID: outboxID,
			Task: Task{
				ExecutionID: req.ExecutionID,
				NodeName:    arrival.NodeName,
				NodeIdx:     arrival.NodeIdx,
				UnitIdx:     arrival.UnitIdx,
				Type:        taskType,
			},
		}
		s.putOutboxLocked(req.ExecutionID, entry)
		result.OutboxIDs = append(result.OutboxIDs, outboxID)
	}
	return result, nil
}

// --- Driving engine helpers ---

type drivingEngineOption func(*Engine)

func withGraph(id types.ExecutionID, g *graph.Graph) drivingEngineOption {
	return func(e *Engine) {
		e.cacheExecutionGraph(id, g)
	}
}

func newDrivingEngine(t *testing.T, state *drivingState, groupExec GroupExecutor, opts ...drivingEngineOption) (*Engine, *fakeQueue) {
	t.Helper()
	q := &fakeQueue{}
	eng := New(state, q, WithGroupExecutor(groupExec))
	for _, opt := range opts {
		opt(eng)
	}
	return eng, q
}

func seedExecution(t *testing.T, eng *Engine, state *drivingState, execID types.ExecutionID, g *graph.Graph) {
	t.Helper()
	err := state.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID:     execID,
		Graph:  g,
		Status: types.ExecutionStatusRunning,
	})
	if err != nil {
		t.Fatalf("seedExecution: %v", err)
	}
	eng.cacheExecutionGraph(execID, g)
}

func enqueueGroupRoot(t *testing.T, q *fakeQueue, execID types.ExecutionID, unitIdx int, groupName string) {
	t.Helper()
	q.Enqueue(context.Background(), &Task{
		ExecutionID: execID,
		NodeName:    groupName,
		UnitIdx:     unitIdx,
		Type:        TaskTypeGroupExec,
	})
}

// drain processes tasks from the queue by calling handleSystemTask, then
// flushing the outbox, until no more tasks are produced. For node exec tasks
// it simulates a successful commit.
func drain(t *testing.T, eng *Engine, state *drivingState, q *fakeQueue, execID types.ExecutionID) {
	t.Helper()
	ctx := context.Background()
	for iterations := 0; iterations < 100; iterations++ {
		// Flush outbox first (picks up advance/skip system tasks inline, enqueues external).
		if err := eng.FlushOutbox(ctx, execID); err != nil {
			t.Fatalf("drain: FlushOutbox: %v", err)
		}

		tasks := q.Drain()
		if len(tasks) == 0 {
			return
		}
		for _, task := range tasks {
			switch task.Type {
			case TaskTypeGroupExec:
				if _, err := eng.handleSystemTask(ctx, task, true); err != nil {
					t.Fatalf("drain: group exec %q: %v", task.NodeName, err)
				}
			case TaskTypeNodeExec:
				// Simulate successful node execution: acquire, claim, commit.
				simulateNodeExec(t, eng, state, task)
			case TaskTypeNodeSkip:
				if _, err := eng.handleSystemTask(ctx, task, true); err != nil {
					t.Fatalf("drain: skip %q: %v", task.NodeName, err)
				}
			default:
				if _, err := eng.handleSystemTask(ctx, task, true); err != nil {
					t.Fatalf("drain: task type %d %q: %v", task.Type, task.NodeName, err)
				}
			}
		}
	}
	t.Fatal("drain: did not converge within 100 iterations")
}

func simulateNodeExec(t *testing.T, eng *Engine, state *drivingState, task *Task) {
	t.Helper()
	ctx := context.Background()
	// Track execution count.
	state.mu.Lock()
	state.executed[string(task.ExecutionID)+"/"+task.NodeName]++
	state.mu.Unlock()

	// Acquire lease.
	lease := &TaskLease{
		Task:       *task,
		LeaseID:    "lease-1",
		LeaseToken: "token-1",
		IssuedAt:   time.Now(),
		TTL:        60 * time.Second,
	}
	_, acquired, err := state.AcquireTaskLease(ctx, lease)
	if err != nil {
		t.Fatalf("simulateNodeExec %q: acquire: %v", task.NodeName, err)
	}
	if !acquired {
		return // already terminal
	}
	// Claim lease.
	_, claimed, err := state.ClaimTaskLease(ctx, lease)
	if err != nil || !claimed {
		t.Fatalf("simulateNodeExec %q: claim: %v (claimed=%v)", task.NodeName, err, claimed)
	}
	// Commit node via engine's commit path.
	g, _, err := eng.loadActiveGraph(ctx, task.ExecutionID)
	if err != nil {
		t.Fatalf("simulateNodeExec %q: loadGraph: %v", task.NodeName, err)
	}
	if g == nil {
		t.Fatalf("simulateNodeExec %q: graph is nil", task.NodeName)
	}
	advance := &Task{
		ExecutionID:  task.ExecutionID,
		NodeName:     task.NodeName,
		NodeIdx:      task.NodeIdx,
		Type:         TaskTypeNodeAdvance,
		ActivationID: task.ActivationID,
	}
	_, err = state.CommitNode(ctx, CommitNodeRequest{
		ExecutionID:  task.ExecutionID,
		NodeName:     task.NodeName,
		NodeIdx:      task.NodeIdx,
		ActivationID: task.ActivationID,
		Status:       types.NodeStatusSuccess,
		Port:         "main",
		AdvanceTask:  advance,
	})
	if err != nil {
		t.Fatalf("simulateNodeExec %q: commit: %v", task.NodeName, err)
	}
}

func assertExecuted(t *testing.T, state *drivingState, execID types.ExecutionID, nodeName string, expected int) {
	t.Helper()
	state.mu.Lock()
	count := state.executed[string(execID)+"/"+nodeName]
	state.mu.Unlock()
	if count != expected {
		t.Fatalf("assertExecuted(%q): got %d, want %d", nodeName, count, expected)
	}
}

func assertExecutionStatus(t *testing.T, state *drivingState, execID types.ExecutionID, expected types.ExecutionStatus) {
	t.Helper()
	state.mu.Lock()
	e := state.executions[execID]
	state.mu.Unlock()
	if e == nil {
		t.Fatalf("assertExecutionStatus: execution %q not found", execID)
	}
	if e.Status != expected {
		t.Fatalf("assertExecutionStatus: got %q, want %q", e.Status, expected)
	}
}

func unitOf(g *graph.Graph, groupName string) int {
	for i := 0; i < g.UnitCount(); i++ {
		if g.UnitKindAt(i) == graph.UnitGroup {
			gm := g.GroupMetaAt(i)
			if gm.Name == groupName {
				return i
			}
		}
	}
	return -1
}

// --- Graph builders ---

// buildGroupThenNodeGraph: [group g (g.source->g.sink)] -main-> [node d]
// UnitCount==2: one group unit + one node unit.
func buildGroupThenNodeGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "group-then-node",
		Nodes: []types.NodeDef{
			{Name: "g.source", Type: "test.action", Kind: types.NodeKindAction},
			{Name: "g.sink", Type: "test.action", Kind: types.NodeKindAction},
			{Name: "d", Type: "test.action", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"g.source": {"main": {Targets: []types.Connection{{Node: "g.sink", Input: "main"}}}},
			"g.sink":   {"main": {Targets: []types.Connection{{Node: "d", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "g", Members: []string{"g.source", "g.sink"}}},
	})
	if err != nil {
		t.Fatalf("buildGroupThenNodeGraph: %v", err)
	}
	if g.UnitCount() != 2 {
		t.Fatalf("buildGroupThenNodeGraph: UnitCount=%d, want 2", g.UnitCount())
	}
	return g
}

// buildGroupFanOutFanIn: [group g (g.source, g.o1, g.o2)] -> x (via main), y (via alt); x->z, y->z
// z has unit in-degree==2 (wait_all or wait_any depending on mergeMode).
func buildGroupFanOutFanIn(t *testing.T, mergeMode string) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "group-fan-out-fan-in",
		Nodes: []types.NodeDef{
			{Name: "g.source", Type: "test.action", Kind: types.NodeKindAction},
			{Name: "g.o1", Type: "test.action", Kind: types.NodeKindAction},
			{Name: "g.o2", Type: "test.action", Kind: types.NodeKindAction},
			{Name: "x", Type: "test.action", Kind: types.NodeKindAction},
			{Name: "y", Type: "test.action", Kind: types.NodeKindAction},
			{Name: "z", Type: "xflow.merge", Kind: types.NodeKindAction, Parameters: map[string]any{"mode": mergeMode}},
		},
		Connections: types.Connections{
			"g.source": {"main": {Targets: []types.Connection{{Node: "g.o1", Input: "main"}, {Node: "g.o2", Input: "main"}}}},
			"g.o1":     {"main": {Targets: []types.Connection{{Node: "x", Input: "main"}}}},
			"g.o2":     {"alt": {Targets: []types.Connection{{Node: "y", Input: "main"}}}},
			"x":        {"main": {Targets: []types.Connection{{Node: "z", Input: "main"}}}},
			"y":        {"main": {Targets: []types.Connection{{Node: "z", Input: "alt"}}}},
		},
		Groups: []types.GroupDef{{Name: "g", Members: []string{"g.source", "g.o1", "g.o2"}}},
	})
	if err != nil {
		t.Fatalf("buildGroupFanOutFanIn: %v", err)
	}
	return g
}

// --- Tests ---

func TestGroupAdvancesDownstreamNode(t *testing.T) {
	g := buildGroupThenNodeGraph(t) // [group g] -main-> [node d], UnitCount==2
	fake := &fakeGroupExecutor{exits: []GroupExit{{NodeName: "g.sink", Port: "main", Data: map[string]any{"v": 1}}}}
	state := newDrivingState()
	eng, q := newDrivingEngine(t, state, fake)
	seedExecution(t, eng, state, "exec-1", g)
	enqueueGroupRoot(t, q, "exec-1", unitOf(g, "g"), "g")
	drain(t, eng, state, q, "exec-1")
	assertExecuted(t, state, "exec-1", "d", 1)
	assertExecutionStatus(t, state, "exec-1", types.ExecutionStatusSuccess)
}

func TestGroupMultiExit_WaitAll(t *testing.T) {
	g := buildGroupFanOutFanIn(t, "wait_all") // g -{main}->x, g -{alt}->y, x->z, y->z (z in-degree=2)
	fake := &fakeGroupExecutor{exits: []GroupExit{
		{NodeName: "g.o1", Port: "main", Data: map[string]any{"a": 1}},
		{NodeName: "g.o2", Port: "alt", Data: map[string]any{"b": 2}},
	}}
	state := newDrivingState()
	eng, q := newDrivingEngine(t, state, fake)
	seedExecution(t, eng, state, "exec-1", g)
	enqueueGroupRoot(t, q, "exec-1", unitOf(g, "g"), "g")
	drain(t, eng, state, q, "exec-1")
	assertExecuted(t, state, "exec-1", "z", 1) // wait_all: z waits for both x and y, fires exactly once
	assertExecutionStatus(t, state, "exec-1", types.ExecutionStatusSuccess)
}

func TestGroupMultiExit_WaitAny(t *testing.T) {
	g := buildGroupFanOutFanIn(t, "wait_any")
	fake := &fakeGroupExecutor{exits: []GroupExit{
		{NodeName: "g.o1", Port: "main", Data: map[string]any{"a": 1}},
		{NodeName: "g.o2", Port: "alt", Data: map[string]any{"b": 2}},
	}}
	state := newDrivingState()
	eng, q := newDrivingEngine(t, state, fake)
	seedExecution(t, eng, state, "exec-1", g)
	enqueueGroupRoot(t, q, "exec-1", unitOf(g, "g"), "g")
	drain(t, eng, state, q, "exec-1")
	assertExecuted(t, state, "exec-1", "z", 1) // wait_any: z fires on first active arrival, exactly once (HSETNX schedule dedup)
	assertExecutionStatus(t, state, "exec-1", types.ExecutionStatusSuccess)
}
