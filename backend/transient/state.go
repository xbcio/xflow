package transient

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

var ErrTransientSuspendUnsupported = engine.ErrSuspendUnsupported

type state struct {
	mu            sync.Mutex
	activeTTL     time.Duration
	completionTTL time.Duration
	executions    map[types.ExecutionID]*execEntry
	nodes         map[string]*engine.NodeSnapshot
	inDegrees     map[string]int
	activeIns     map[string]int
	outputs       map[string]map[string]any
	subExecs      map[string][]*engine.SubExecution
	doneCh        map[types.ExecutionID]chan struct{}
	eventWatchers map[types.ExecutionID][]chan engine.ExecutionEvent
	activeTimers  map[types.ExecutionID]*activeTimer
	cleanupTimers map[types.ExecutionID]*time.Timer
	onCleanup     func(types.ExecutionID)
}

type execEntry struct {
	snap   engine.ExecutionSnapshot
	err    string
	closed bool
}

type activeTimer struct {
	timer      *time.Timer
	generation uint64
}

func newState(activeTTL, completionTTL time.Duration) *state {
	return &state{
		activeTTL:     activeTTL,
		completionTTL: completionTTL,
		executions:    make(map[types.ExecutionID]*execEntry),
		nodes:         make(map[string]*engine.NodeSnapshot),
		inDegrees:     make(map[string]int),
		activeIns:     make(map[string]int),
		outputs:       make(map[string]map[string]any),
		subExecs:      make(map[string][]*engine.SubExecution),
		doneCh:        make(map[types.ExecutionID]chan struct{}),
		eventWatchers: make(map[types.ExecutionID][]chan engine.ExecutionEvent),
		activeTimers:  make(map[types.ExecutionID]*activeTimer),
		cleanupTimers: make(map[types.ExecutionID]*time.Timer),
	}
}

func (s *state) SetCleanupHook(fn func(types.ExecutionID)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onCleanup = fn
}

func (s *state) CreateExecution(_ context.Context, e *engine.ExecutionSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if timer := s.cleanupTimers[e.ID]; timer != nil {
		timer.Stop()
		delete(s.cleanupTimers, e.ID)
	}

	cp := *e
	s.executions[e.ID] = &execEntry{snap: cp}
	s.doneCh[e.ID] = make(chan struct{})
	for i, d := range e.Graph.InDegree {
		key := fmt.Sprintf("%s/%d", e.ID, i)
		s.inDegrees[key] = d
	}
	s.touchActiveLocked(e.ID)
	return nil
}

func (s *state) UpdateExecutionStatus(_ context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.executions[id]
	if !ok {
		return nil
	}
	entry.snap.Status = status
	entry.err = errMsg
	if isTerminalStatus(status) {
		s.stopActiveTimerLocked(id)
		if !entry.closed {
			entry.closed = true
			if ch, ok := s.doneCh[id]; ok {
				close(ch)
			}
		}
		s.scheduleCleanupLocked(id)
	} else {
		s.touchActiveLocked(id)
	}
	s.publishLocked(engine.ExecutionEvent{ExecutionID: id, Status: status})
	return nil
}

func (s *state) GetExecution(_ context.Context, id types.ExecutionID) (*engine.ExecutionSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.executions[id]
	if !ok {
		return nil, nil
	}
	cp := entry.snap
	return &cp, nil
}

func (s *state) LoadGraph(_ context.Context, id types.ExecutionID) (*graph.Graph, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.executions[id]
	if !ok {
		return nil, nil
	}
	return entry.snap.Graph, nil
}

func (s *state) UpsertNode(_ context.Context, n *engine.NodeSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.executionExistsLocked(n.ExecutionID) {
		return engine.ErrExecutionInactive
	}
	key := string(n.ExecutionID) + "/" + n.Name
	if existing, ok := s.nodes[key]; ok && isTerminalNode(existing.Status) && n.ActivationID <= existing.ActivationID {
		return nil
	}
	if existing, ok := s.nodes[key]; ok && existing.Status == types.NodeStatusCommitting && n.Status == types.NodeStatusRunning {
		return nil
	}
	cp := *n
	s.nodes[key] = &cp
	s.touchActiveLocked(n.ExecutionID)
	return nil
}

func (s *state) GetNode(_ context.Context, id types.ExecutionID, name string) (*engine.NodeSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nodes[string(id)+"/"+name], nil
}

func (s *state) ResetNodeForRetry(_ context.Context, id types.ExecutionID, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := string(id) + "/" + name
	ns := s.nodes[key]
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
	s.nodes[key] = &cp
	s.touchActiveLocked(id)
	return nil
}

func (s *state) ListExpiredLeases(_ context.Context, before time.Time) ([]engine.ExpiredLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []engine.ExpiredLease
	for _, ns := range s.nodes {
		if ns.Status != types.NodeStatusRunning {
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
		})
	}
	return out, nil
}

func (s *state) RevokeLease(_ context.Context, id types.ExecutionID, name string, token engine.LeaseToken) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := string(id) + "/" + name
	ns := s.nodes[key]
	if ns == nil {
		return false, nil
	}
	if ns.Status != types.NodeStatusRunning {
		return false, nil
	}
	if token == "" || ns.LeaseToken != token {
		return false, nil
	}
	cp := *ns
	cp.Status = types.NodeStatusPending
	cp.LeaseID = ""
	cp.LeaseToken = ""
	cp.LeaseIssuedAt = time.Time{}
	cp.LeaseTTL = 0
	s.nodes[key] = &cp
	s.touchActiveLocked(id)
	return true, nil
}

func (s *state) ClaimTaskLease(_ context.Context, lease *engine.TaskLease) (*engine.NodeSnapshot, bool, error) {
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
	if ns.LeaseToken == "" || ns.LeaseToken != lease.LeaseToken {
		return ns, false, nil
	}

	cp := *ns
	cp.Status = types.NodeStatusCommitting
	cp.LeaseToken = ""
	s.nodes[key] = &cp
	s.touchActiveLocked(lease.Task.ExecutionID)
	return &cp, true, nil
}

func (s *state) DecrementInDegree(_ context.Context, id types.ExecutionID, nodeIdx int, portActive bool) (int, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.executionExistsLocked(id) {
		return 0, 0, engine.ErrExecutionInactive
	}
	key := fmt.Sprintf("%s/%d", id, nodeIdx)
	s.inDegrees[key]--
	if portActive {
		s.activeIns[key]++
	}
	s.touchActiveLocked(id)
	return s.inDegrees[key], s.activeIns[key], nil
}

func (s *state) CheckCompletion(_ context.Context, id types.ExecutionID, totalNodes int) (bool, bool, error) {
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

func (s *state) SuspendOrConsume(_ context.Context, _ types.ExecutionID, _ string, _ *types.SuspendSpec) (*types.SignalPayload, error) {
	return nil, ErrTransientSuspendUnsupported
}

func (s *state) DeliverSignal(_ context.Context, _ types.ExecutionID, _ string, _ map[string]any) (string, *types.SignalPayload, error) {
	return "", nil, ErrTransientSuspendUnsupported
}

func (s *state) ResuspendAtomic(_ context.Context, _ types.ExecutionID, _ string, _ string, _ string, _ *types.SuspendSpec) (*types.SignalPayload, error) {
	return nil, ErrTransientSuspendUnsupported
}

func (s *state) RevokeSignal(_ context.Context, _ types.ExecutionID, _ string) (bool, error) {
	return false, ErrTransientSuspendUnsupported
}

func (s *state) AcquireResumeLock(_ context.Context, _ types.ExecutionID, _ string) (bool, error) {
	return false, ErrTransientSuspendUnsupported
}

func (s *state) ListSuspendedNodes(_ context.Context, _ types.ExecutionID) ([]string, error) {
	return nil, nil
}

func (s *state) PutOutput(_ context.Context, id types.ExecutionID, name string, data map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.executionExistsLocked(id) {
		return engine.ErrExecutionInactive
	}
	s.outputs[string(id)+"/"+name] = data
	s.touchActiveLocked(id)
	return nil
}

func (s *state) GetOutput(_ context.Context, id types.ExecutionID, name string) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.outputs[string(id)+"/"+name], nil
}

func (s *state) PublishExecutionEvent(_ context.Context, event engine.ExecutionEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.publishLocked(event)
	return nil
}

func (s *state) WatchExecution(ctx context.Context, id types.ExecutionID) (<-chan engine.ExecutionEvent, error) {
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

func (s *state) CreateSubExecution(_ context.Context, sub *engine.SubExecution) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.executionExistsLocked(sub.ParentExecID) {
		return engine.ErrExecutionInactive
	}
	key := string(sub.ParentExecID) + "/" + sub.ParentNode
	s.subExecs[key] = append(s.subExecs[key], sub)
	s.touchActiveLocked(sub.ParentExecID)
	return nil
}

func (s *state) CompleteSubExecution(_ context.Context, parentExecID types.ExecutionID, parentNode string, childExecID types.ExecutionID, status types.ExecutionStatus, result map[string]any) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.executionExistsLocked(parentExecID) {
		return false, engine.ErrExecutionInactive
	}
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
	s.touchActiveLocked(parentExecID)
	return allDone, nil
}

func (s *state) GetSubExecutionResults(_ context.Context, parentExecID types.ExecutionID, parentNode string) ([]map[string]any, error) {
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

func (s *state) waitDone(ctx context.Context, id types.ExecutionID) (types.Result, error) {
	done := s.doneChannel(id)
	select {
	case <-ctx.Done():
		return types.Result{}, ctx.Err()
	case <-done:
	}

	snap, err := s.GetExecution(ctx, id)
	if err != nil {
		return types.Result{}, err
	}
	if snap == nil {
		return types.Result{ExecutionID: id, Status: types.ExecutionStatusFailed}, nil
	}

	result := types.Result{ExecutionID: id, Status: snap.Status}
	if snap.Status == types.ExecutionStatusSuccess {
		result.Output = s.getAllOutputs(id)
	}
	result.Error = s.executionError(id)
	return result, nil
}

func (s *state) executionError(id types.ExecutionID) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.executions[id]
	if !ok {
		return ""
	}
	return entry.err
}

func (s *state) executionTerminal(id types.ExecutionID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.executions[id]
	return ok && isTerminalStatus(entry.snap.Status)
}

func (s *state) executionExists(id types.ExecutionID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.executionExistsLocked(id)
}

func (s *state) executionExistsLocked(id types.ExecutionID) bool {
	_, ok := s.executions[id]
	return ok
}

func (s *state) doneChannel(id types.ExecutionID) <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	ch, ok := s.doneCh[id]
	if !ok {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return ch
}

func (s *state) getAllOutputs(id types.ExecutionID) map[string]any {
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

func (s *state) publishLocked(event engine.ExecutionEvent) {
	for _, watcher := range s.eventWatchers[event.ExecutionID] {
		select {
		case watcher <- event:
		default:
		}
	}
}

func (s *state) scheduleCleanupLocked(id types.ExecutionID) {
	if _, exists := s.cleanupTimers[id]; exists {
		return
	}
	s.cleanupTimers[id] = time.AfterFunc(s.completionTTL, func() {
		s.cleanupExecution(id)
	})
}

func (s *state) cleanupExecution(id types.ExecutionID) {
	s.mu.Lock()
	cleaned, hook := s.cleanupExecutionLocked(id)
	s.mu.Unlock()
	if cleaned && hook != nil {
		hook(id)
	}
}

func (s *state) touchActiveLocked(id types.ExecutionID) {
	if s.activeTTL <= 0 {
		return
	}
	entry, ok := s.executions[id]
	if !ok {
		return
	}
	if isTerminalStatus(entry.snap.Status) {
		s.stopActiveTimerLocked(id)
		return
	}
	var generation uint64 = 1
	if active := s.activeTimers[id]; active != nil {
		active.timer.Stop()
		generation = active.generation + 1
	}
	s.activeTimers[id] = &activeTimer{
		generation: generation,
		timer: time.AfterFunc(s.activeTTL, func() {
			s.expireActiveExecution(id, generation)
		}),
	}
}

func (s *state) activeGeneration(id types.ExecutionID) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if active := s.activeTimers[id]; active != nil {
		return active.generation
	}
	return 0
}

func (s *state) refreshActiveGenerationForTest(id types.ExecutionID, generation uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if active := s.activeTimers[id]; active != nil && active.generation == generation {
		s.touchActiveLocked(id)
	}
}

func (s *state) expireActiveGenerationForTest(id types.ExecutionID, generation uint64) {
	s.expireActiveExecution(id, generation)
}

func (s *state) stopActiveTimerLocked(id types.ExecutionID) {
	if active := s.activeTimers[id]; active != nil {
		active.timer.Stop()
		delete(s.activeTimers, id)
	}
}

func (s *state) expireActiveExecution(id types.ExecutionID, generation uint64) {
	s.mu.Lock()

	active := s.activeTimers[id]
	if active == nil || active.generation != generation {
		s.mu.Unlock()
		return
	}
	delete(s.activeTimers, id)
	entry, ok := s.executions[id]
	if !ok {
		s.mu.Unlock()
		return
	}
	if isTerminalStatus(entry.snap.Status) {
		s.mu.Unlock()
		return
	}
	if !entry.closed {
		entry.closed = true
		if ch, ok := s.doneCh[id]; ok {
			close(ch)
		}
	}
	cleaned, hook := s.cleanupExecutionLocked(id)
	s.mu.Unlock()
	if cleaned && hook != nil {
		hook(id)
	}
}

func (s *state) cleanupExecutionLocked(id types.ExecutionID) (bool, func(types.ExecutionID)) {
	s.stopActiveTimerLocked(id)

	if timer := s.cleanupTimers[id]; timer != nil {
		timer.Stop()
		delete(s.cleanupTimers, id)
	}

	prefix := string(id) + "/"
	_, hadExecution := s.executions[id]
	_, hadDone := s.doneCh[id]
	_, hadWatchers := s.eventWatchers[id]
	delete(s.executions, id)
	delete(s.doneCh, id)
	for key := range s.nodes {
		if strings.HasPrefix(key, prefix) {
			delete(s.nodes, key)
		}
	}
	for key := range s.inDegrees {
		if strings.HasPrefix(key, prefix) {
			delete(s.inDegrees, key)
		}
	}
	for key := range s.activeIns {
		if strings.HasPrefix(key, prefix) {
			delete(s.activeIns, key)
		}
	}
	for key := range s.outputs {
		if strings.HasPrefix(key, prefix) {
			delete(s.outputs, key)
		}
	}
	for key := range s.subExecs {
		if strings.HasPrefix(key, prefix) {
			delete(s.subExecs, key)
		}
	}
	for _, watcher := range s.eventWatchers[id] {
		close(watcher)
	}
	delete(s.eventWatchers, id)
	return hadExecution || hadDone || hadWatchers, s.onCleanup
}
func isTerminalStatus(status types.ExecutionStatus) bool {
	return types.IsTerminalExecutionStatus(status)
}

func isTerminalNode(status types.NodeStatus) bool { return types.IsTerminalNodeStatus(status) }
