package local

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// memoryState implements core.StateBackend using in-memory maps protected by a mutex.
// It is suitable for single-process (local) execution only.
type memoryState struct {
	mu         sync.Mutex
	executions map[types.ExecutionID]*execEntry
	nodes      map[string]*engine.NodeSnapshot // key: execID+"/"+name
	inDegrees  map[string]int                // key: execID+"/"+nodeIdx
	activeIns  map[string]int                // key: execID+"/"+nodeIdx
	outputs    map[string]map[string]any     // key: execID+"/"+name
	suspended  map[string]*node.SuspendSpec  // key: execID+"/"+nodeName
	signals    map[string]map[string]any     // pre-delivered: key: execID+"/"+signalName
	resumed    map[string]bool               // resume lock: key: execID+"/"+nodeName
	subExecs   map[string][]*engine.SubExecution // key: execID+"/"+nodeName

	// done channels allow Wait() callers to block until execution completes.
	doneCh map[types.ExecutionID]chan struct{}
}

type execEntry struct {
	snap   engine.ExecutionSnapshot
	closed bool // true once the done channel has been closed
}

func newMemoryState() *memoryState {
	return &memoryState{
		executions: make(map[types.ExecutionID]*execEntry),
		nodes:      make(map[string]*engine.NodeSnapshot),
		inDegrees:  make(map[string]int),
		activeIns:  make(map[string]int),
		outputs:    make(map[string]map[string]any),
		suspended:  make(map[string]*node.SuspendSpec),
		signals:    make(map[string]map[string]any),
		resumed:    make(map[string]bool),
		subExecs:   make(map[string][]*engine.SubExecution),
		doneCh:     make(map[types.ExecutionID]chan struct{}),
	}
}

// ---------------------------------------------------------------------------
// ExecutionStore
// ---------------------------------------------------------------------------

func (s *memoryState) CreateExecution(_ context.Context, e *engine.ExecutionSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *e
	s.executions[e.ID] = &execEntry{snap: cp}
	s.doneCh[e.ID] = make(chan struct{})
	// Seed in-degree counters from the compiled graph.
	for i, d := range e.Graph.InDegree {
		key := fmt.Sprintf("%s/%d", e.ID, i)
		s.inDegrees[key] = d
	}
	return nil
}

func (s *memoryState) UpdateExecutionStatus(_ context.Context, id types.ExecutionID, status types.Status, _ string) error {
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
	if existing, ok := s.nodes[key]; ok && isTerminalNode(existing.Status) {
		return nil // CAS: don't overwrite terminal state
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
		if ns.Status == "failed" {
			hasFailed = true
		}
	}
	return done >= totalNodes, hasFailed, nil
}

// ---------------------------------------------------------------------------
// Suspend / signal
// ---------------------------------------------------------------------------

func (s *memoryState) SuspendOrConsume(_ context.Context, id types.ExecutionID, name string, spec *node.SuspendSpec) (*node.SignalPayload, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Check if any awaited signal was pre-delivered (keyed by signal name).
	for _, sigName := range spec.Signals {
		sigKey := string(id) + "/" + sigName
		if sig, ok := s.signals[sigKey]; ok {
			delete(s.signals, sigKey)
			return &node.SignalPayload{Triggered: node.SignalReceived, Name: sigName, Data: sig}, nil
		}
	}
	// Park the node.
	s.suspended[string(id)+"/"+name] = spec
	return nil, nil
}

func (s *memoryState) DeliverSignal(_ context.Context, id types.ExecutionID, signalName string, data map[string]any) (string, error) {
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
				delete(s.suspended, key)
				nodeName := strings.TrimPrefix(key, prefix)
				return nodeName, nil
			}
		}
	}
	// No suspended node yet — store for later consumption.
	s.signals[string(id)+"/"+signalName] = data
	return "", nil
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

func (s *memoryState) ResuspendAtomic(_ context.Context, id types.ExecutionID, nodeName string, oldSignalName string, newSignalName string, spec *node.SuspendSpec) (*node.SignalPayload, error) {
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
		return &node.SignalPayload{Triggered: node.SignalReceived, Name: newSignalName, Data: sig}, nil
	}

	// 4. No signal available — register new waiter.
	s.suspended[execID+"/"+nodeName] = spec
	return nil, nil
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func isTerminalStatus(s types.Status) bool {
	switch s {
	case types.StatusSuccess, types.StatusFailed, types.StatusCanceled, types.StatusTimeout:
		return true
	}
	return false
}

func isTerminalNode(status string) bool {
	switch status {
	case "success", "failed", "skipped", "canceled", "continued":
		return true
	}
	return false
}

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

func (s *memoryState) CompleteSubExecution(_ context.Context, parentExecID types.ExecutionID, parentNode string, childExecID types.ExecutionID, status types.Status, result map[string]any) (bool, error) {
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
		if sub.Status == types.StatusRunning {
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
