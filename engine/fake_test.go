package engine

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// ---------------------------------------------------------------------------
// fakeState — in-memory StateStore for unit tests
// ---------------------------------------------------------------------------

type fakeState struct {
	mu         sync.Mutex
	executions map[types.ExecutionID]*ExecutionSnapshot
	nodes      map[string]*NodeSnapshot // key: execID+"/"+name
	inDegrees  map[string]int           // key: execID+"/"+nodeIdx
	activeIns  map[string]int           // key: execID+"/"+nodeIdx (count of active arrivals)
	outputs    map[string]map[string]any
	suspended  map[string]*types.SuspendSpec // key: execID+"/"+nodeName
	signals    map[string]map[string]any     // pre-delivered signals: key: execID+"/"+signalName
	signalSets map[string]map[string]map[string]any
	resumed    map[string]bool            // resume lock: key: execID+"/"+nodeName
	subExecs   map[string][]*SubExecution // key: execID+"/"+nodeName
	watchers   map[types.ExecutionID][]chan ExecutionEvent
}

func newFakeState() *fakeState {
	return &fakeState{
		executions: make(map[types.ExecutionID]*ExecutionSnapshot),
		nodes:      make(map[string]*NodeSnapshot),
		inDegrees:  make(map[string]int),
		activeIns:  make(map[string]int),
		outputs:    make(map[string]map[string]any),
		suspended:  make(map[string]*types.SuspendSpec),
		signals:    make(map[string]map[string]any),
		signalSets: make(map[string]map[string]map[string]any),
		resumed:    make(map[string]bool),
		subExecs:   make(map[string][]*SubExecution),
		watchers:   make(map[types.ExecutionID][]chan ExecutionEvent),
	}
}

func (f *fakeState) CreateExecution(_ context.Context, e *ExecutionSnapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.executions[e.ID] = e
	// Initialize in-degree counters from the graph.
	if e.Graph != nil {
		for i, deg := range e.Graph.InDegree {
			key := fmt.Sprintf("%s/%d", e.ID, i)
			f.inDegrees[key] = deg
		}
	}
	return nil
}

func (f *fakeState) UpdateExecutionStatus(_ context.Context, id types.ExecutionID, status types.ExecutionStatus, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e, ok := f.executions[id]; ok {
		e.Status = status
	}
	f.publishLocked(ExecutionEvent{ExecutionID: id, Status: status})
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
	if existing, ok := f.nodes[key]; ok && types.IsTerminalNodeStatus(existing.Status) && n.ActivationID <= existing.ActivationID {
		return nil // CAS: don't overwrite terminal state
	}
	if existing, ok := f.nodes[key]; ok && existing.Status == types.NodeStatusCommitting && n.Status == types.NodeStatusRunning {
		return nil
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

func cloneNodeSnapshot(ns *NodeSnapshot) *NodeSnapshot {
	if ns == nil {
		return nil
	}
	cp := *ns
	return &cp
}

func (f *fakeState) AcquireTaskLease(_ context.Context, lease *TaskLease) (*NodeSnapshot, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	key := string(lease.Task.ExecutionID) + "/" + lease.Task.NodeName
	current := f.nodes[key]
	if current != nil {
		if lease.Task.ActivationID > 0 && current.ActivationID > lease.Task.ActivationID {
			return cloneNodeSnapshot(current), false, nil
		}
		if types.IsTerminalNodeStatus(current.Status) && (lease.Task.ActivationID <= 0 || current.ActivationID >= lease.Task.ActivationID) {
			return cloneNodeSnapshot(current), false, nil
		}
		if current.Status == types.NodeStatusCommitting {
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
	f.nodes[key] = &NodeSnapshot{
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
	}
	return cloneNodeSnapshot(current), true, nil
}

func (f *fakeState) ResetNodeForRetry(_ context.Context, id types.ExecutionID, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := string(id) + "/" + name
	ns := f.nodes[key]
	if ns == nil {
		return nil
	}
	if ns.Status != types.NodeStatusRunning && ns.Status != types.NodeStatusCommitting {
		return nil
	}
	cp := *ns
	cp.Status = types.NodeStatusPending
	cp.LeaseID = ""
	cp.LeaseToken = ""
	cp.LeaseIssuedAt = time.Time{}
	cp.LeaseTTL = 0
	f.nodes[key] = &cp
	return nil
}

func (f *fakeState) ListExpiredLeases(_ context.Context, before time.Time) ([]ExpiredLease, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []ExpiredLease
	for _, ns := range f.nodes {
		if ns.Status != types.NodeStatusRunning {
			continue
		}
		if ns.LeaseIssuedAt.IsZero() || ns.LeaseTTL <= 0 {
			continue
		}
		if ns.LeaseIssuedAt.Add(ns.LeaseTTL).After(before) {
			continue
		}
		out = append(out, ExpiredLease{
			ExecutionID:  ns.ExecutionID,
			NodeName:     ns.Name,
			NodeIdx:      ns.NodeIdx,
			LeaseID:      ns.LeaseID,
			LeaseToken:   ns.LeaseToken,
			IssuedAt:     ns.LeaseIssuedAt,
			TTL:          ns.LeaseTTL,
			ActivationID: ns.ActivationID,
			AutoDepth:    ns.AutoDepth,
		})
	}
	return out, nil
}

func (f *fakeState) RevokeLease(_ context.Context, id types.ExecutionID, name string, token LeaseToken) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := string(id) + "/" + name
	ns := f.nodes[key]
	if ns == nil {
		return false, nil
	}
	if ns.Status != types.NodeStatusRunning {
		return false, nil
	}
	if ns.LeaseToken != token || token == "" {
		return false, nil
	}
	cp := *ns
	cp.Status = types.NodeStatusPending
	cp.LeaseID = ""
	cp.LeaseToken = ""
	cp.LeaseIssuedAt = time.Time{}
	cp.LeaseTTL = 0
	f.nodes[key] = &cp
	return true, nil
}

func (f *fakeState) ClaimTaskLease(_ context.Context, lease *TaskLease) (*NodeSnapshot, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	key := string(lease.Task.ExecutionID) + "/" + lease.Task.NodeName
	ns := f.nodes[key]
	if ns == nil {
		return nil, false, nil
	}
	if types.IsTerminalNodeStatus(ns.Status) {
		if lease.Task.ActivationID > 0 && ns.ActivationID != lease.Task.ActivationID {
			return ns, false, nil
		}
		return ns, true, nil
	}
	if lease.Task.ActivationID > 0 && ns.ActivationID != lease.Task.ActivationID {
		return ns, false, nil
	}
	if ns.LeaseToken == "" || ns.LeaseToken != lease.LeaseToken {
		return ns, false, nil
	}

	cp := *ns
	cp.Status = types.NodeStatusCommitting
	cp.LeaseToken = ""
	f.nodes[key] = &cp
	return &cp, true, nil
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
		if types.IsTerminalNodeStatus(ns.Status) {
			done++
		}
		if ns.Status == types.NodeStatusFailed {
			hasFailed = true
		}
	}
	return done >= totalNodes, hasFailed, nil
}

func (f *fakeState) SuspendOrConsume(_ context.Context, id types.ExecutionID, name string, spec *types.SuspendSpec) (*types.SignalPayload, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if spec != nil && spec.Mode == types.ModeMultiSignal {
		return f.suspendOrConsumeMultiLocked(id, name, spec), nil
	}
	// Check if any of the awaited signals was pre-delivered (keyed by signal name).
	for _, sigName := range spec.Signals {
		sigKey := string(id) + "/" + sigName
		if sig, ok := f.signals[sigKey]; ok {
			delete(f.signals, sigKey)
			return &types.SignalPayload{Triggered: types.SignalReceived, Name: sigName, Data: sig}, nil
		}
	}
	// No pre-delivered signal — park the node.
	f.suspended[string(id)+"/"+name] = spec
	return nil, nil
}

func (f *fakeState) DeliverSignal(_ context.Context, id types.ExecutionID, signalName string, data map[string]any) (string, *types.SignalPayload, error) {
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
				nodeName := strings.TrimPrefix(key, prefix)
				if spec.Mode == types.ModeMultiSignal {
					payload := f.addMultiSignalLocked(id, nodeName, signalName, data, spec)
					if payload == nil {
						return "", nil, nil
					}
					delete(f.suspended, key)
					delete(f.signalSets, key)
					return nodeName, payload, nil
				}
				delete(f.suspended, key)
				return nodeName, nil, nil
			}
		}
	}
	// No suspended node yet — store for later consumption.
	f.signals[string(id)+"/"+signalName] = data
	return "", nil, nil
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

func (f *fakeState) ResuspendAtomic(_ context.Context, id types.ExecutionID, nodeName string, oldSignalName string, newSignalName string, spec *types.SuspendSpec) (*types.SignalPayload, error) {
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
		return &types.SignalPayload{Triggered: types.SignalReceived, Name: newSignalName, Data: sig}, nil
	}

	// 4. No signal available — register new waiter.
	f.suspended[execID+"/"+nodeName] = spec
	return nil, nil
}

func (f *fakeState) suspendOrConsumeMultiLocked(id types.ExecutionID, nodeName string, spec *types.SuspendSpec) *types.SignalPayload {
	key := string(id) + "/" + nodeName
	for _, sigName := range spec.Signals {
		sigKey := string(id) + "/" + sigName
		if sig, ok := f.signals[sigKey]; ok {
			delete(f.signals, sigKey)
			if payload := f.addMultiSignalLocked(id, nodeName, sigName, sig, spec); payload != nil {
				delete(f.signalSets, key)
				return payload
			}
		}
	}
	f.suspended[key] = spec
	return nil
}

func (f *fakeState) addMultiSignalLocked(id types.ExecutionID, nodeName string, signalName string, data map[string]any, spec *types.SuspendSpec) *types.SignalPayload {
	key := string(id) + "/" + nodeName
	all := f.signalSets[key]
	if all == nil {
		all = make(map[string]map[string]any)
		f.signalSets[key] = all
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
	handlers map[string]types.ActionHandler
}

func (r *fakeRegistry) Get(_ types.ExecutionID, _ string, nodeType string, _ int) (types.ActionHandler, error) {
	h, ok := r.handlers[nodeType]
	if !ok {
		return nil, fmt.Errorf("no handler for type: %s", nodeType)
	}
	return h, nil
}

// ---------------------------------------------------------------------------
// fakeState — Sub-execution support
// ---------------------------------------------------------------------------

func (f *fakeState) CreateSubExecution(_ context.Context, sub *SubExecution) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := string(sub.ParentExecID) + "/" + sub.ParentNode
	f.subExecs[key] = append(f.subExecs[key], sub)
	return nil
}

func (f *fakeState) CompleteSubExecution(_ context.Context, parentExecID types.ExecutionID, parentNode string, childExecID types.ExecutionID, status types.ExecutionStatus, result map[string]any) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := string(parentExecID) + "/" + parentNode
	subs := f.subExecs[key]
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

func (f *fakeState) GetSubExecutionResults(_ context.Context, parentExecID types.ExecutionID, parentNode string) ([]map[string]any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := string(parentExecID) + "/" + parentNode
	subs := f.subExecs[key]
	results := make([]map[string]any, 0, len(subs))
	for _, sub := range subs {
		if sub.Result != nil {
			results = append(results, sub.Result)
		}
	}
	return results, nil
}

func (f *fakeState) PublishExecutionEvent(_ context.Context, event ExecutionEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.publishLocked(event)
	return nil
}

func (f *fakeState) WatchExecution(ctx context.Context, id types.ExecutionID) (<-chan ExecutionEvent, error) {
	ch := make(chan ExecutionEvent, 8)
	f.mu.Lock()
	f.watchers[id] = append(f.watchers[id], ch)
	f.mu.Unlock()
	if done := ctx.Done(); done != nil {
		go func() {
			<-done
			f.mu.Lock()
			defer f.mu.Unlock()
			watchers := f.watchers[id]
			for i, watcher := range watchers {
				if watcher == ch {
					f.watchers[id] = append(watchers[:i], watchers[i+1:]...)
					close(ch)
					return
				}
			}
		}()
	}
	return ch, nil
}

func (f *fakeState) publishLocked(event ExecutionEvent) {
	for _, watcher := range f.watchers[event.ExecutionID] {
		select {
		case watcher <- event:
		default:
		}
	}
}
