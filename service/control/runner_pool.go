package control

import (
	"sync"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

type RunnerPool struct {
	mu      sync.Mutex
	runners map[string]*runnerState
}

type RunnerSnapshot struct {
	RunnerID      string
	Capacity      int
	InFlight      int
	Capabilities  []protocol.Capability
	LastHeartbeat time.Time
}

type runnerState struct {
	snapshot RunnerSnapshot
	queue    []engine.TaskLease
}

func NewRunnerPool() *RunnerPool {
	return &RunnerPool{runners: make(map[string]*runnerState)}
}

func (p *RunnerPool) Register(runnerID string, capacity int, capabilities []protocol.Capability) {
	p.mu.Lock()
	defer p.mu.Unlock()

	state := p.runners[runnerID]
	if state == nil {
		state = &runnerState{}
		p.runners[runnerID] = state
	}
	state.snapshot.RunnerID = runnerID
	state.snapshot.Capacity = capacity
	state.snapshot.Capabilities = cloneCapabilities(capabilities)
}

func (p *RunnerPool) Heartbeat(runnerID string, capacity, inFlight int, at time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	state := p.runners[runnerID]
	if state == nil {
		return false
	}
	state.snapshot.Capacity = capacity
	state.snapshot.InFlight = inFlight
	state.snapshot.LastHeartbeat = at
	return true
}

func (p *RunnerPool) Assign(lease engine.TaskLease) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, state := range p.runners {
		if !canRun(state.snapshot.Capabilities, lease) {
			continue
		}
		state.queue = append(state.queue, lease)
		return true
	}
	return false
}

func (p *RunnerPool) Poll(runnerID string, capacity int, capabilities []protocol.Capability) (engine.TaskLease, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	state := p.runners[runnerID]
	if state == nil {
		return engine.TaskLease{}, false
	}
	state.snapshot.Capacity = capacity
	if capabilities != nil {
		state.snapshot.Capabilities = cloneCapabilities(capabilities)
	}
	for i, lease := range state.queue {
		if !canRun(state.snapshot.Capabilities, lease) {
			continue
		}
		state.queue = append(state.queue[:i], state.queue[i+1:]...)
		return lease, true
	}
	return engine.TaskLease{}, false
}

func (p *RunnerPool) Runner(runnerID string) (RunnerSnapshot, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	state := p.runners[runnerID]
	if state == nil {
		return RunnerSnapshot{}, false
	}
	snapshot := state.snapshot
	snapshot.Capabilities = cloneCapabilities(snapshot.Capabilities)
	return snapshot, true
}

func canRun(capabilities []protocol.Capability, lease engine.TaskLease) bool {
	for _, capability := range capabilities {
		if capability.NodeType == lease.NodeType {
			return true
		}
	}
	return false
}

func cloneCapabilities(capabilities []protocol.Capability) []protocol.Capability {
	if len(capabilities) == 0 {
		return nil
	}
	clone := make([]protocol.Capability, len(capabilities))
	copy(clone, capabilities)
	return clone
}
