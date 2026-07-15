package memory

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// memoryState implements engine.StateStore using in-memory maps protected by a mutex.
// It is suitable for single-process (local) execution only.
type memoryState struct {
	mu         sync.Mutex
	executions map[types.ExecutionID]*execEntry
	nodes      map[string]*engine.NodeSnapshot // key: execID+"/"+name
	inDegrees  map[string]int                  // key: execID+"/"+nodeIdx
	activeIns  map[string]int                  // key: execID+"/"+nodeIdx
	remaining  map[types.ExecutionID]int
	failed     map[types.ExecutionID]int
	advanced   map[string]bool
	scheduled  map[string]string
	outbox     map[types.ExecutionID]map[string]memoryOutboxEntry
	deadOutbox map[types.ExecutionID]map[string]memoryOutboxEntry
	outputs    map[string]map[string]any     // key: execID+"/"+name
	suspended  map[string]*types.SuspendSpec // key: execID+"/"+nodeName
	signals    map[string]map[string]any     // pre-delivered: key: execID+"/"+signalName
	signalSets map[string]map[string]map[string]any
	resumed    map[string]bool                   // resume lock: key: execID+"/"+nodeName
	subExecs   map[string][]*engine.SubExecution // key: execID+"/"+nodeName

	// done channels allow Wait() callers to block until execution completes.
	doneCh        map[types.ExecutionID]chan struct{}
	eventWatchers map[types.ExecutionID][]chan engine.ExecutionEvent
}

type execEntry struct {
	snap   engine.ExecutionSnapshot
	closed bool // true once the done channel has been closed
}

func newMemoryState() *memoryState {
	return &memoryState{
		executions:    make(map[types.ExecutionID]*execEntry),
		nodes:         make(map[string]*engine.NodeSnapshot),
		inDegrees:     make(map[string]int),
		activeIns:     make(map[string]int),
		remaining:     make(map[types.ExecutionID]int),
		failed:        make(map[types.ExecutionID]int),
		advanced:      make(map[string]bool),
		scheduled:     make(map[string]string),
		outbox:        make(map[types.ExecutionID]map[string]memoryOutboxEntry),
		deadOutbox:    make(map[types.ExecutionID]map[string]memoryOutboxEntry),
		outputs:       make(map[string]map[string]any),
		suspended:     make(map[string]*types.SuspendSpec),
		signals:       make(map[string]map[string]any),
		signalSets:    make(map[string]map[string]map[string]any),
		resumed:       make(map[string]bool),
		subExecs:      make(map[string][]*engine.SubExecution),
		doneCh:        make(map[types.ExecutionID]chan struct{}),
		eventWatchers: make(map[types.ExecutionID][]chan engine.ExecutionEvent),
	}
}

// ---------------------------------------------------------------------------
// ExecutionStore
// ---------------------------------------------------------------------------

func (s *memoryState) CreateExecution(_ context.Context, e *engine.ExecutionSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.createExecutionLocked(e)
	return nil
}

// CreateExecutionWithOutbox atomically records an execution and its initial
// durable task intents under the memory reference model's single mutex.
func (s *memoryState) CreateExecutionWithOutbox(_ context.Context, e *engine.ExecutionSnapshot, entries []engine.OutboxEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.createExecutionLocked(e)
	for _, entry := range entries {
		s.putOutboxEntryLocked(e.ID, entry)
	}
	return nil
}

func (s *memoryState) createExecutionLocked(e *engine.ExecutionSnapshot) {
	cp := *e
	s.executions[e.ID] = &execEntry{snap: cp}
	s.doneCh[e.ID] = make(chan struct{})
	// Seed in-degree counters and O(1) completion counters from the compiled graph.
	if e.Graph != nil {
		for i, d := range e.Graph.InDegree {
			key := fmt.Sprintf("%s/%d", e.ID, i)
			s.inDegrees[key] = d
		}
		if !e.Graph.AllowCycles {
			s.remaining[e.ID] = len(e.Graph.Nodes)
			s.failed[e.ID] = 0
		}
	}
}

func (s *memoryState) UpdateExecutionStatus(_ context.Context, id types.ExecutionID, status types.ExecutionStatus, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.executions[id]
	if !ok {
		return nil
	}
	entry.snap.Status = status
	// Close the done channel on terminal status so Wait() unblocks.
	if isTerminalStatus(status) && !entry.closed {
		entry.closed = true
		if ch, ok := s.doneCh[id]; ok {
			close(ch)
		}
	}
	s.publishLocked(engine.ExecutionEvent{ExecutionID: id, Status: status})
	return nil
}

func (s *memoryState) GetExecution(_ context.Context, id types.ExecutionID) (*engine.ExecutionSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.executions[id]
	if !ok {
		return nil, nil
	}
	cp := entry.snap
	return &cp, nil
}

func (s *memoryState) LoadGraph(_ context.Context, id types.ExecutionID) (*graph.Graph, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.executions[id]
	if !ok {
		return nil, nil
	}
	return entry.snap.Graph, nil
}

// ---------------------------------------------------------------------------
// NodeStore
// ---------------------------------------------------------------------------

func (s *memoryState) UpsertNode(_ context.Context, n *engine.NodeSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(n.ExecutionID) + "/" + n.Name
	if existing, ok := s.nodes[key]; ok && isTerminalNode(existing.Status) && n.ActivationID <= existing.ActivationID {
		return nil // CAS: don't overwrite terminal state
	}
	if existing, ok := s.nodes[key]; ok && (existing.Status == types.NodeStatusCommitting || existing.Status == types.NodeStatusWaiting) && n.Status == types.NodeStatusRunning {
		return nil
	}
	cp := *n
	s.nodes[key] = &cp
	return nil
}

func (s *memoryState) GetNode(_ context.Context, id types.ExecutionID, name string) (*engine.NodeSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns := s.nodes[string(id)+"/"+name]
	return ns, nil
}

func cloneNodeSnapshot(ns *engine.NodeSnapshot) *engine.NodeSnapshot {
	if ns == nil {
		return nil
	}
	cp := *ns
	cp.Output = cloneData(ns.Output)
	cp.LeasePayload = cloneLeasePayload(ns.LeasePayload)
	return &cp
}

func cloneLeasePayload(payload *types.SignalPayload) *types.SignalPayload {
	if payload == nil {
		return nil
	}
	cp := *payload
	cp.Data = cloneData(payload.Data)
	if payload.All != nil {
		cp.All = make(map[string]map[string]any, len(payload.All))
		for name, data := range payload.All {
			cp.All[name] = cloneData(data)
		}
	}
	return &cp
}

func (s *memoryState) AcquireTaskLease(_ context.Context, lease *engine.TaskLease) (*engine.NodeSnapshot, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := string(lease.Task.ExecutionID) + "/" + lease.Task.NodeName
	current := s.nodes[key]
	if current != nil {
		if lease.Task.ActivationID > 0 && current.ActivationID > lease.Task.ActivationID {
			return cloneNodeSnapshot(current), false, nil
		}
		if isTerminalNode(current.Status) && (lease.Task.ActivationID <= 0 || current.ActivationID >= lease.Task.ActivationID) {
			return cloneNodeSnapshot(current), false, nil
		}
		if current.Status == types.NodeStatusCommitting || current.Status == types.NodeStatusWaiting {
			return cloneNodeSnapshot(current), false, nil
		}
		if current.Status == types.NodeStatusRunning && current.LeaseToken != "" {
			deadline := current.LeaseIssuedAt.Add(current.LeaseTTL)
			if current.LeaseIssuedAt.IsZero() || current.LeaseTTL <= 0 || lease.IssuedAt.Before(deadline) {
				return cloneNodeSnapshot(current), false, nil
			}
		}
	}

	attempt := 1
	if current != nil {
		attempt = current.Attempt + 1
	}
	s.nodes[key] = &engine.NodeSnapshot{
		ExecutionID:   lease.Task.ExecutionID,
		Name:          lease.Task.NodeName,
		NodeIdx:       lease.Task.NodeIdx,
		Status:        types.NodeStatusRunning,
		LeaseID:       lease.LeaseID,
		LeaseToken:    lease.LeaseToken,
		Attempt:       attempt,
		ActivationID:  lease.Task.ActivationID,
		AutoDepth:     lease.Task.AutoDepth,
		LeaseIssuedAt: lease.IssuedAt,
		LeaseTTL:      lease.TTL,
		LeaseTaskType: lease.Task.Type,
		LeasePayload:  cloneLeasePayload(lease.Task.Payload),
	}
	return cloneNodeSnapshot(current), true, nil
}

func (s *memoryState) ResetNodeForRetry(_ context.Context, id types.ExecutionID, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resetNodeForRetryLocked(id, name)
	return nil
}

// ResetNodeForRetryWithOutbox atomically creates the retry delivery intent
// when the current node is eligible to move back to pending.
func (s *memoryState) ResetNodeForRetryWithOutbox(_ context.Context, id types.ExecutionID, name string, token engine.LeaseToken, entry engine.OutboxEntry) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.resetNodeForRetryLocked(id, name, token) {
		return false, nil
	}
	s.putOutboxLocked(id, entry.ID, entry.Task, entry.AvailableAt)
	return true, nil
}

func (s *memoryState) resetNodeForRetryLocked(id types.ExecutionID, name string, token ...engine.LeaseToken) bool {
	key := string(id) + "/" + name
	ns := s.nodes[key]
	if ns == nil {
		return false
	}
	if ns.Status != types.NodeStatusRunning && ns.Status != types.NodeStatusCommitting && ns.Status != types.NodeStatusWaiting {
		return false
	}
	if len(token) > 0 && (token[0] == "" || ns.LeaseToken != token[0]) {
		return false
	}
	cp := *ns
	cp.Status = types.NodeStatusPending
	cp.LeaseID = ""
	cp.LeaseToken = ""
	cp.LeaseIssuedAt = time.Time{}
	cp.LeaseTTL = 0
	cp.LeaseTaskType = engine.TaskTypeNodeExec
	cp.LeasePayload = nil
	s.nodes[key] = &cp
	return true
}

func (s *memoryState) ListExpiredLeases(_ context.Context, before time.Time) ([]engine.ExpiredLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []engine.ExpiredLease
	for _, ns := range s.nodes {
		if ns.Status != types.NodeStatusRunning && ns.Status != types.NodeStatusCommitting && ns.Status != types.NodeStatusWaiting {
			continue
		}
		if ns.LeaseIssuedAt.IsZero() || ns.LeaseTTL <= 0 {
			continue
		}
		if ns.LeaseIssuedAt.Add(ns.LeaseTTL).After(before) {
			continue
		}
		out = append(out, engine.ExpiredLease{
			ExecutionID:  ns.ExecutionID,
			NodeName:     ns.Name,
			NodeIdx:      ns.NodeIdx,
			LeaseID:      ns.LeaseID,
			LeaseToken:   ns.LeaseToken,
			IssuedAt:     ns.LeaseIssuedAt,
			TTL:          ns.LeaseTTL,
			ActivationID: ns.ActivationID,
			AutoDepth:    ns.AutoDepth,
			TaskType:     ns.LeaseTaskType,
			Payload:      cloneLeasePayload(ns.LeasePayload),
		})
	}
	return out, nil
}

func (s *memoryState) RevokeLease(_ context.Context, id types.ExecutionID, name string, token engine.LeaseToken) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.revokeLeaseLocked(id, name, token), nil
}

// RevokeLeaseWithOutbox atomically fences lease revocation with the durable
// task intent needed to retry the exact task.
func (s *memoryState) RevokeLeaseWithOutbox(_ context.Context, id types.ExecutionID, name string, token engine.LeaseToken, entry engine.OutboxEntry) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.revokeLeaseLocked(id, name, token) {
		return false, nil
	}
	s.putOutboxLocked(id, entry.ID, entry.Task, entry.AvailableAt)
	return true, nil
}

func (s *memoryState) revokeLeaseLocked(id types.ExecutionID, name string, token engine.LeaseToken) bool {
	key := string(id) + "/" + name
	ns := s.nodes[key]
	if ns == nil {
		return false
	}
	if ns.Status != types.NodeStatusRunning && ns.Status != types.NodeStatusCommitting && ns.Status != types.NodeStatusWaiting {
		return false
	}
	if token == "" || ns.LeaseToken != token {
		return false
	}
	cp := *ns
	cp.Status = types.NodeStatusPending
	cp.LeaseID = ""
	cp.LeaseToken = ""
	cp.LeaseIssuedAt = time.Time{}
	cp.LeaseTTL = 0
	cp.LeaseTaskType = engine.TaskTypeNodeExec
	cp.LeasePayload = nil
	s.nodes[key] = &cp
	return true
}

func (s *memoryState) ClaimTaskLease(_ context.Context, lease *engine.TaskLease) (*engine.NodeSnapshot, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := string(lease.Task.ExecutionID) + "/" + lease.Task.NodeName
	ns := s.nodes[key]
	if ns == nil {
		return nil, false, nil
	}
	if isTerminalNode(ns.Status) {
		if lease.Task.ActivationID > 0 && ns.ActivationID != lease.Task.ActivationID {
			return ns, false, nil
		}
		return ns, true, nil
	}
	if lease.Task.ActivationID > 0 && ns.ActivationID != lease.Task.ActivationID {
		return ns, false, nil
	}
	if ns.Status != types.NodeStatusRunning || ns.LeaseToken == "" || ns.LeaseToken != lease.LeaseToken {
		return ns, false, nil
	}
	if lease.LeaseID != "" && ns.LeaseID != lease.LeaseID {
		return ns, false, nil
	}
	if lease.Attempt != 0 && ns.Attempt != lease.Attempt {
		return ns, false, nil
	}

	cp := *ns
	cp.Status = types.NodeStatusCommitting
	s.nodes[key] = &cp
	return cloneNodeSnapshot(&cp), true, nil
}

// SuspendTaskLease atomically parks a claimed lease after verifying the same
// lease that entered committing still owns the node. A sweeper that reset the
// lease, or a newer runner, therefore cannot be overwritten by a stale
// suspend result.
func (s *memoryState) SuspendTaskLease(_ context.Context, lease *engine.TaskLease, output map[string]any, storeOutput bool, spec *types.SuspendSpec, oldSignalName string) (*types.SignalPayload, bool, error) {
	if lease == nil || spec == nil {
		return nil, false, engine.ErrInvalidLeaseToken
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	key := string(lease.Task.ExecutionID) + "/" + lease.Task.NodeName
	node := s.nodes[key]
	if node == nil || node.Status != types.NodeStatusCommitting || node.LeaseID != lease.LeaseID || node.LeaseToken != lease.LeaseToken || node.Attempt != lease.Attempt || node.ActivationID != lease.Task.ActivationID {
		return nil, false, nil
	}
	if storeOutput {
		s.outputs[key] = cloneData(output)
	}

	delete(s.resumed, key)
	if oldSignalName != "" {
		delete(s.suspended, key)
	}

	var payload *types.SignalPayload
	if spec.Mode == types.ModeMultiSignal {
		for _, signalName := range spec.Signals {
			signalKey := string(lease.Task.ExecutionID) + "/" + signalName
			data, found := s.signals[signalKey]
			if !found {
				continue
			}
			delete(s.signals, signalKey)
			if ready := s.addMultiSignalLocked(lease.Task.ExecutionID, lease.Task.NodeName, signalName, data, spec); ready != nil {
				payload = ready
				delete(s.signalSets, key)
				break
			}
		}
	} else {
		for _, signalName := range spec.Signals {
			signalKey := string(lease.Task.ExecutionID) + "/" + signalName
			if data, found := s.signals[signalKey]; found {
				delete(s.signals, signalKey)
				payload = &types.SignalPayload{Triggered: types.SignalReceived, Name: signalName, Data: cloneData(data)}
				break
			}
		}
	}
	if payload == nil {
		s.suspended[key] = spec
	} else {
		delete(s.suspended, key)
	}

	cp := *node
	cp.Status = types.NodeStatusSuspended
	cp.LeaseID = ""
	cp.LeaseToken = ""
	cp.LeaseIssuedAt = time.Time{}
	cp.LeaseTTL = 0
	cp.LeaseTaskType = engine.TaskTypeNodeExec
	cp.LeasePayload = nil
	s.nodes[key] = &cp
	for _, entry := range engine.SuspendOutboxEntries(lease, spec, payload, time.Now().UTC()) {
		s.putOutboxLocked(lease.Task.ExecutionID, entry.ID, entry.Task, entry.AvailableAt)
	}
	return cloneLeasePayload(payload), true, nil
}

// SuspendTaskLeaseWithOutbox shares the fenced suspend transition above. The
// transition writes continuation intents while the memory-state mutex is held,
// so callers never observe a consumed pre-signal without a recoverable task.
func (s *memoryState) SuspendTaskLeaseWithOutbox(ctx context.Context, lease *engine.TaskLease, output map[string]any, storeOutput bool, spec *types.SuspendSpec, oldSignalName string) (bool, error) {
	_, committed, err := s.SuspendTaskLease(ctx, lease, output, storeOutput, spec, oldSignalName)
	return committed, err
}

// ---------------------------------------------------------------------------
// Scheduling counters
// ---------------------------------------------------------------------------

func (s *memoryState) DecrementInDegree(_ context.Context, id types.ExecutionID, nodeIdx int, portActive bool) (int, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("%s/%d", id, nodeIdx)
	s.inDegrees[key]--
	if portActive {
		s.activeIns[key]++
	}
	return s.inDegrees[key], s.activeIns[key], nil
}

func (s *memoryState) CheckCompletion(_ context.Context, id types.ExecutionID, totalNodes int) (bool, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := string(id) + "/"
	done := 0
	hasFailed := false
	for key, ns := range s.nodes {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if isTerminalNode(ns.Status) {
			done++
		}
		if ns.Status == types.NodeStatusFailed {
			hasFailed = true
		}
	}
	return done >= totalNodes, hasFailed, nil
}

// ---------------------------------------------------------------------------
// Suspend / signal
// ---------------------------------------------------------------------------

func (s *memoryState) SuspendOrConsume(_ context.Context, id types.ExecutionID, name string, spec *types.SuspendSpec) (*types.SignalPayload, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if spec != nil && spec.Mode == types.ModeMultiSignal {
		return s.suspendOrConsumeMultiLocked(id, name, spec), nil
	}
	// Check if any awaited signal was pre-delivered (keyed by signal name).
	for _, sigName := range spec.Signals {
		sigKey := string(id) + "/" + sigName
		if sig, ok := s.signals[sigKey]; ok {
			delete(s.signals, sigKey)
			return &types.SignalPayload{Triggered: types.SignalReceived, Name: sigName, Data: sig}, nil
		}
	}
	// Park the node.
	key := string(id) + "/" + name
	delete(s.resumed, key)
	s.suspended[key] = spec
	return nil, nil
}

func (s *memoryState) DeliverSignal(_ context.Context, id types.ExecutionID, signalName string, data map[string]any) (string, *types.SignalPayload, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := string(id) + "/"
	// Check if any suspended node is waiting for this signal.
	for key, spec := range s.suspended {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		for _, sig := range spec.Signals {
			if sig == signalName {
				nodeName := strings.TrimPrefix(key, prefix)
				if spec.Mode == types.ModeMultiSignal {
					payload := s.addMultiSignalLocked(id, nodeName, signalName, data, spec)
					if payload == nil {
						return "", nil, nil
					}
					delete(s.suspended, key)
					delete(s.signalSets, key)
					return nodeName, payload, nil
				}
				delete(s.suspended, key)
				return nodeName, nil, nil
			}
		}
	}
	// No suspended node yet — store for later consumption.
	s.signals[string(id)+"/"+signalName] = data
	return "", nil, nil
}

func (s *memoryState) AcquireResumeLock(_ context.Context, id types.ExecutionID, name string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(id) + "/" + name
	if s.resumed[key] {
		return false, nil
	}
	s.resumed[key] = true
	return true, nil
}

func (s *memoryState) RevokeSignal(_ context.Context, id types.ExecutionID, signalName string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sigKey := string(id) + "/" + signalName
	if _, ok := s.signals[sigKey]; !ok {
		return false, nil
	}
	// Check if any node that was waiting for this signal has already been resumed.
	// The suspended map keys are execID/nodeName → *SuspendSpec, and resumed keys are execID/nodeName.
	// We need to find the node waiting for this signal by checking suspended entries.
	for key, spec := range s.suspended {
		if !strings.HasPrefix(key, string(id)+"/") {
			continue
		}
		for _, sig := range spec.Signals {
			if sig == signalName {
				// Found the node waiting for this signal — check its resume lock.
				if s.resumed[key] {
					return false, nil
				}
				break
			}
		}
	}
	delete(s.signals, sigKey)
	return true, nil
}

func (s *memoryState) ResuspendAtomic(_ context.Context, id types.ExecutionID, nodeName string, oldSignalName string, newSignalName string, spec *types.SuspendSpec) (*types.SignalPayload, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	execID := string(id)

	// 1. Release the resume lock for this node.
	delete(s.resumed, execID+"/"+nodeName)

	// 2. Delete old waiter (the suspended entry keyed by node name).
	delete(s.suspended, execID+"/"+nodeName)

	// 3. Check if the new signal was pre-delivered.
	newSigKey := execID + "/" + newSignalName
	if sig, ok := s.signals[newSigKey]; ok {
		delete(s.signals, newSigKey)
		return &types.SignalPayload{Triggered: types.SignalReceived, Name: newSignalName, Data: sig}, nil
	}

	// 4. No signal available — register new waiter.
	s.suspended[execID+"/"+nodeName] = spec
	return nil, nil
}

func (s *memoryState) suspendOrConsumeMultiLocked(id types.ExecutionID, nodeName string, spec *types.SuspendSpec) *types.SignalPayload {
	key := string(id) + "/" + nodeName
	for _, sigName := range spec.Signals {
		sigKey := string(id) + "/" + sigName
		if sig, ok := s.signals[sigKey]; ok {
			delete(s.signals, sigKey)
			if payload := s.addMultiSignalLocked(id, nodeName, sigName, sig, spec); payload != nil {
				delete(s.signalSets, key)
				return payload
			}
		}
	}
	delete(s.resumed, key)
	s.suspended[key] = spec
	return nil
}

func (s *memoryState) addMultiSignalLocked(id types.ExecutionID, nodeName string, signalName string, data map[string]any, spec *types.SuspendSpec) *types.SignalPayload {
	key := string(id) + "/" + nodeName
	all := s.signalSets[key]
	if all == nil {
		all = make(map[string]map[string]any)
		s.signalSets[key] = all
	}
	all[signalName] = data
	if len(all) < signalQuorum(spec) {
		return nil
	}
	return &types.SignalPayload{
		Triggered: types.SignalReceived,
		Name:      signalName,
		Data:      data,
		All:       cloneSignalSet(all),
	}
}

func signalQuorum(spec *types.SuspendSpec) int {
	if spec == nil {
		return 1
	}
	if spec.Quorum > 0 {
		return spec.Quorum
	}
	if len(spec.Signals) > 0 {
		return len(spec.Signals)
	}
	return 1
}

func cloneSignalSet(in map[string]map[string]any) map[string]map[string]any {
	out := make(map[string]map[string]any, len(in))
	for name, payload := range in {
		cp := make(map[string]any, len(payload))
		for k, v := range payload {
			cp[k] = v
		}
		out[name] = cp
	}
	return out
}

// ---------------------------------------------------------------------------
// Cancel support
// ---------------------------------------------------------------------------

func (s *memoryState) ListSuspendedNodes(_ context.Context, id types.ExecutionID) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var nodes []string
	prefix := string(id) + "/"
	for key := range s.suspended {
		if strings.HasPrefix(key, prefix) {
			nodes = append(nodes, strings.TrimPrefix(key, prefix))
		}
	}
	return nodes, nil
}

// ---------------------------------------------------------------------------
// Output store
// ---------------------------------------------------------------------------

func (s *memoryState) PutOutput(_ context.Context, id types.ExecutionID, name string, data map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outputs[string(id)+"/"+name] = data
	return nil
}

func (s *memoryState) GetOutput(_ context.Context, id types.ExecutionID, name string) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.outputs[string(id)+"/"+name], nil
}

// ---------------------------------------------------------------------------
// Wait support
// ---------------------------------------------------------------------------

// GetAllOutputs returns all node outputs for an execution, keyed by node name.
func (s *memoryState) GetAllOutputs(id types.ExecutionID) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := string(id) + "/"
	result := make(map[string]any)
	for key, data := range s.outputs {
		if strings.HasPrefix(key, prefix) {
			nodeName := strings.TrimPrefix(key, prefix)
			result[nodeName] = data
		}
	}
	return result
}
func (s *memoryState) waitDone(id types.ExecutionID) <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch, ok := s.doneCh[id]
	if !ok {
		// Already completed or unknown — return a pre-closed channel.
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return ch
}

func (s *memoryState) PublishExecutionEvent(_ context.Context, event engine.ExecutionEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.publishLocked(event)
	return nil
}

func (s *memoryState) WatchExecution(ctx context.Context, id types.ExecutionID) (<-chan engine.ExecutionEvent, error) {
	ch := make(chan engine.ExecutionEvent, 8)
	s.mu.Lock()
	s.eventWatchers[id] = append(s.eventWatchers[id], ch)
	s.mu.Unlock()

	if done := ctx.Done(); done != nil {
		go func() {
			<-done
			s.mu.Lock()
			defer s.mu.Unlock()
			watchers := s.eventWatchers[id]
			for i, watcher := range watchers {
				if watcher == ch {
					s.eventWatchers[id] = append(watchers[:i], watchers[i+1:]...)
					close(ch)
					return
				}
			}
		}()
	}

	return ch, nil
}

func (s *memoryState) publishLocked(event engine.ExecutionEvent) {
	for _, watcher := range s.eventWatchers[event.ExecutionID] {
		select {
		case watcher <- event:
		default:
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func isTerminalStatus(s types.ExecutionStatus) bool { return types.IsTerminalExecutionStatus(s) }

func isTerminalNode(status types.NodeStatus) bool { return types.IsTerminalNodeStatus(status) }

// ---------------------------------------------------------------------------
// Sub-execution support
// ---------------------------------------------------------------------------

func (s *memoryState) CreateSubExecution(_ context.Context, sub *engine.SubExecution) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(sub.ParentExecID) + "/" + sub.ParentNode
	s.subExecs[key] = append(s.subExecs[key], sub)
	return nil
}

func (s *memoryState) CompleteSubExecution(_ context.Context, parentExecID types.ExecutionID, parentNode string, childExecID types.ExecutionID, status types.ExecutionStatus, result map[string]any) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(parentExecID) + "/" + parentNode
	subs := s.subExecs[key]
	allDone := true
	for _, sub := range subs {
		if sub.ChildExecID == childExecID {
			sub.Status = status
			sub.Result = result
		}
		if sub.Status == types.ExecutionStatusRunning {
			allDone = false
		}
	}
	return allDone, nil
}

func (s *memoryState) GetSubExecutionResults(_ context.Context, parentExecID types.ExecutionID, parentNode string) ([]map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(parentExecID) + "/" + parentNode
	subs := s.subExecs[key]
	results := make([]map[string]any, 0, len(subs))
	for _, sub := range subs {
		if sub.Result != nil {
			results = append(results, sub.Result)
		}
	}
	return results, nil
}
