package engine

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// ---------------------------------------------------------------------------
// fakeState — in-memory StateBackend for unit tests
// ---------------------------------------------------------------------------

type fakeState struct {
	mu         sync.Mutex
	executions map[types.ExecutionID]*ExecutionSnapshot
	nodes      map[string]*NodeSnapshot // key: execID+"/"+name
	inDegrees  map[string]int           // key: execID+"/"+nodeIdx
	activeIns  map[string]int           // key: execID+"/"+nodeIdx (count of active arrivals)
	outputs    map[string]map[string]any
	suspended  map[string]*node.SuspendSpec // key: execID+"/"+nodeName
	signals    map[string]map[string]any    // pre-delivered signals: key: execID+"/"+signalName
	resumed    map[string]bool              // resume lock: key: execID+"/"+nodeName
}

func newFakeState() *fakeState {
	return &fakeState{
		executions: make(map[types.ExecutionID]*ExecutionSnapshot),
		nodes:      make(map[string]*NodeSnapshot),
		inDegrees:  make(map[string]int),
		activeIns:  make(map[string]int),
		outputs:    make(map[string]map[string]any),
		suspended:  make(map[string]*node.SuspendSpec),
		signals:    make(map[string]map[string]any),
		resumed:    make(map[string]bool),
	}
}

func (f *fakeState) CreateExecution(_ context.Context, e *ExecutionSnapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.executions[e.ID] = e
	return nil
}

func (f *fakeState) UpdateExecutionStatus(_ context.Context, id types.ExecutionID, status types.Status, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e, ok := f.executions[id]; ok {
		e.Status = status
	}
	return nil
}

func (f *fakeState) GetExecution(_ context.Context, id types.ExecutionID) (*ExecutionSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.executions[id], nil
}

func (f *fakeState) LoadGraph(_ context.Context, id types.ExecutionID) (*graph.Graph, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e, ok := f.executions[id]; ok {
		return e.Graph, nil
	}
	return nil, nil
}

func (f *fakeState) UpsertNode(_ context.Context, n *NodeSnapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := string(n.ExecutionID) + "/" + n.Name
	if existing, ok := f.nodes[key]; ok && isTerminal(existing.Status) {
		return nil // CAS: don't overwrite terminal state
	}
	cp := *n
	f.nodes[key] = &cp
	return nil
}

func (f *fakeState) GetNode(_ context.Context, id types.ExecutionID, name string) (*NodeSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ns := f.nodes[string(id)+"/"+name]
	return ns, nil
}

func (f *fakeState) DecrementInDegree(_ context.Context, id types.ExecutionID, nodeIdx int, portActive bool) (int, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := fmt.Sprintf("%s/%d", id, nodeIdx)
	f.inDegrees[key]--
	if portActive {
		f.activeIns[key]++
	}
	return f.inDegrees[key], f.activeIns[key], nil
}

func (f *fakeState) CheckCompletion(_ context.Context, id types.ExecutionID, totalNodes int) (bool, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	prefix := string(id) + "/"
	done := 0
	hasFailed := false
	for key, ns := range f.nodes {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if isTerminal(ns.Status) {
			done++
		}
		if ns.Status == "failed" {
			hasFailed = true
		}
	}
	return done >= totalNodes, hasFailed, nil
}

func (f *fakeState) SuspendOrConsume(_ context.Context, id types.ExecutionID, name string, spec *node.SuspendSpec) (*node.SignalPayload, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Check if any of the awaited signals was pre-delivered (keyed by signal name).
	for _, sigName := range spec.Signals {
		sigKey := string(id) + "/" + sigName
		if sig, ok := f.signals[sigKey]; ok {
			delete(f.signals, sigKey)
			return &node.SignalPayload{Triggered: node.SignalReceived, Name: sigName, Data: sig}, nil
		}
	}
	// No pre-delivered signal — park the node.
	f.suspended[string(id)+"/"+name] = spec
	return nil, nil
}

func (f *fakeState) DeliverSignal(_ context.Context, id types.ExecutionID, signalName string, data map[string]any) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	prefix := string(id) + "/"
	// Check if any suspended node is waiting for this signal.
	for key, spec := range f.suspended {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		for _, s := range spec.Signals {
			if s == signalName {
				delete(f.suspended, key)
				nodeName := strings.TrimPrefix(key, prefix)
				return nodeName, nil
			}
		}
	}
	// No suspended node yet — store for later consumption.
	f.signals[string(id)+"/"+signalName] = data
	return "", nil
}

func (f *fakeState) AcquireResumeLock(_ context.Context, id types.ExecutionID, name string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := string(id) + "/" + name
	if f.resumed[key] {
		return false, nil
	}
	f.resumed[key] = true
	return true, nil
}

func (f *fakeState) RevokeSignal(_ context.Context, id types.ExecutionID, signalName string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	sigKey := string(id) + "/" + signalName
	if _, ok := f.signals[sigKey]; !ok {
		return false, nil
	}
	if f.resumed[sigKey] {
		return false, nil
	}
	delete(f.signals, sigKey)
	return true, nil
}

func (f *fakeState) ResuspendAtomic(_ context.Context, id types.ExecutionID, nodeName string, oldSignalName string, newSignalName string, spec *node.SuspendSpec) (*node.SignalPayload, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	execID := string(id)

	// 1. Release the resume lock for this node.
	delete(f.resumed, execID+"/"+nodeName)

	// 2. Delete old waiter (the suspended entry keyed by node name).
	delete(f.suspended, execID+"/"+nodeName)

	// 3. Check if the new signal was pre-delivered.
	newSigKey := execID + "/" + newSignalName
	if sig, ok := f.signals[newSigKey]; ok {
		delete(f.signals, newSigKey)
		return &node.SignalPayload{Triggered: node.SignalReceived, Name: newSignalName, Data: sig}, nil
	}

	// 4. No signal available — register new waiter.
	f.suspended[execID+"/"+nodeName] = spec
	return nil, nil
}

func (f *fakeState) PutOutput(_ context.Context, id types.ExecutionID, name string, data map[string]any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.outputs[string(id)+"/"+name] = data
	return nil
}

func (f *fakeState) GetOutput(_ context.Context, id types.ExecutionID, name string) (map[string]any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.outputs[string(id)+"/"+name], nil
}

func (f *fakeState) ListSuspendedNodes(_ context.Context, id types.ExecutionID) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var nodes []string
	prefix := string(id) + "/"
	for key := range f.suspended {
		if strings.HasPrefix(key, prefix) {
			nodes = append(nodes, strings.TrimPrefix(key, prefix))
		}
	}
	return nodes, nil
}

// InitInDegrees seeds the in-degree counters from a compiled graph.
// Must be called after Submit so the counters are ready before ExecuteNode.
func (f *fakeState) InitInDegrees(id types.ExecutionID, g *graph.Graph) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, d := range g.InDegree {
		key := fmt.Sprintf("%s/%d", id, i)
		f.inDegrees[key] = d
	}
}

// ---------------------------------------------------------------------------
// fakeQueue — in-memory TaskQueue for unit tests
// ---------------------------------------------------------------------------

type fakeQueue struct {
	mu    sync.Mutex
	tasks []*Task
}

func (q *fakeQueue) Enqueue(_ context.Context, t *Task) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.tasks = append(q.tasks, t)
	return nil
}

func (q *fakeQueue) EnqueueDelayed(_ context.Context, t *Task, _ time.Duration) error {
	return q.Enqueue(context.Background(), t)
}

// Drain removes and returns all queued tasks.
func (q *fakeQueue) Drain() []*Task {
	q.mu.Lock()
	defer q.mu.Unlock()
	tasks := q.tasks
	q.tasks = nil
	return tasks
}

// ---------------------------------------------------------------------------
// fakeRegistry — in-memory HandlerRegistry for unit tests
// ---------------------------------------------------------------------------

type fakeRegistry struct {
	handlers map[string]node.TaskHandler
}

func (r *fakeRegistry) Get(_ types.ExecutionID, _ string, nodeType string) (node.TaskHandler, error) {
	h, ok := r.handlers[nodeType]
	if !ok {
		return nil, fmt.Errorf("no handler for type: %s", nodeType)
	}
	return h, nil
}
