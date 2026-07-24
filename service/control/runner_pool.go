package control

import (
	"sync"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

type RunnerPool struct {
	mu      sync.Mutex
	runners map[string]*runnerState
}

type RunnerSnapshot struct {
	RunnerID      string
	Capacity      int
	InFlight      int
	Labels        map[string]string
	Capabilities  []protocol.Capability
	LastHeartbeat time.Time
}

type runnerState struct {
	snapshot RunnerSnapshot
	queue    []engine.TaskLease
	// policy is the authorization envelope the authenticator bound to this
	// runner at register time. Dispatcher checks lease.NodeType against it
	// before assigning work — a token/mTLS-authenticated runner that lists
	// xflow.function does not receive xflow.database leases.
	policy RunnerPolicy
	// notify is a 1-capacity signal channel. AssignRouted sends non-blocking
	// after enqueue; the stream handler drains the queue on receive.
	notify  chan struct{}
	session *streamSession
}

func NewRunnerPool() *RunnerPool {
	return &RunnerPool{runners: make(map[string]*runnerState)}
}

func (p *RunnerPool) Register(runnerID string, capacity int, capabilities []protocol.Capability) {
	p.RegisterWithPolicy(runnerID, capacity, capabilities, RunnerPolicy{AllowedNodeTypes: []string{"*"}})
}

func (p *RunnerPool) RegisterWithLabels(runnerID string, capacity int, capabilities []protocol.Capability, labels map[string]string) {
	p.RegisterWithLabelsAndPolicy(runnerID, capacity, capabilities, labels, RunnerPolicy{AllowedNodeTypes: []string{"*"}})
}

// RegisterWithPolicy is the auth-aware Register. The permissive default binds
// to Register callers who never enabled auth so existing tests / dev deploys
// stay unchanged.
func (p *RunnerPool) RegisterWithPolicy(runnerID string, capacity int, capabilities []protocol.Capability, policy RunnerPolicy) {
	p.RegisterWithLabelsAndPolicy(runnerID, capacity, capabilities, nil, policy)
}

func (p *RunnerPool) RegisterWithLabelsAndPolicy(runnerID string, capacity int, capabilities []protocol.Capability, labels map[string]string, policy RunnerPolicy) {
	p.mu.Lock()
	defer p.mu.Unlock()

	state := p.runners[runnerID]
	if state == nil {
		state = &runnerState{notify: make(chan struct{}, 1)}
		p.runners[runnerID] = state
	}
	state.snapshot.RunnerID = runnerID
	state.snapshot.Capacity = capacity
	state.snapshot.Labels = cloneLabels(labels)
	state.snapshot.Capabilities = cloneCapabilities(capabilities)
	state.policy = policy
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

// Assign finds the runner with the most free headroom that advertises the
// lease's capability and reserves a slot. Returns ErrNoMatchingRunner if no
// runner registered the type, ErrNoCapacity if every capable runner is at its
// concurrency ceiling. Reserving a slot via queue length is what gives the
// queue layer backpressure semantics: when every runner is saturated the
// dispatcher returns a retryable error and the queue layer requeues with
// backoff instead of silently dropping the task.
func (p *RunnerPool) Assign(lease engine.TaskLease) error {
	return p.AssignRouted(engine.TaskRouting{NodeType: lease.NodeType, NodeVersion: lease.NodeVersion}, func() (*engine.TaskLease, error) {
		return &lease, nil
	})
}

// AssignRouted reserves capacity for routing metadata, then builds and queues
// the concrete lease only after a runner is known to be available.
func (p *RunnerPool) AssignRouted(routing engine.TaskRouting, build func() (*engine.TaskLease, error)) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var best *runnerState
	bestHeadroom := 0
	foundCapable := false
	for _, state := range p.runners {
		if !canRunRouting(state.snapshot.Capabilities, routing) {
			continue
		}
		if !matchesRunnerSelector(state.snapshot.Labels, routing.RunnerSelector) {
			continue
		}
		// Policy check: skip runners whose authorization envelope forbids
		// this node type. A capable-but-forbidden runner is treated the
		// same as no runner for placement purposes; it does not count
		// toward foundCapable so the dispatcher returns
		// ErrNoMatchingRunner rather than ErrNoCapacity when every
		// capable runner is unauthorized.
		if !state.policy.Allows(routing.NodeType) {
			continue
		}
		foundCapable = true
		h := state.headroom()
		if h <= 0 {
			continue
		}
		if best == nil || h > bestHeadroom {
			best = state
			bestHeadroom = h
		}
	}
	if best == nil {
		if foundCapable {
			return ErrNoCapacity
		}
		return ErrNoMatchingRunner
	}
	lease, err := build()
	if err != nil {
		return err
	}
	if lease == nil {
		return ErrNoMatchingRunner
	}
	best.queue = append(best.queue, *lease)
	if best.session != nil {
		select {
		case best.notify <- struct{}{}:
		default:
		}
	}
	return nil
}

// headroom returns how many more leases this runner can absorb beyond the
// in-flight count. Counting queued-but-not-yet-polled leases against the
// budget keeps the dispatcher from piling up work on a single runner that
// happens to heartbeat fast.
func (s *runnerState) headroom() int {
	cap := s.snapshot.Capacity - s.snapshot.InFlight - len(s.queue)
	if cap < 0 {
		return 0
	}
	return cap
}

func (p *RunnerPool) Poll(runnerID string, capacity int, capabilities []protocol.Capability) (engine.TaskLease, bool) {
	return p.PollWithLabels(runnerID, capacity, capabilities, nil)
}

func (p *RunnerPool) PollWithLabels(runnerID string, capacity int, capabilities []protocol.Capability, labels map[string]string) (engine.TaskLease, bool) {
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
	if labels != nil {
		state.snapshot.Labels = cloneLabels(labels)
	}
	// No RunnerSelector re-check here: AssignRouted already filtered by
	// selector (and by capability/policy) before choosing this runner and
	// queuing the lease, so anything in state.queue already matches.
	for i, lease := range state.queue {
		if !canRunRouting(state.snapshot.Capabilities, engine.TaskRouting{NodeType: lease.NodeType, NodeVersion: lease.NodeVersion}) {
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
	snapshot.Labels = cloneLabels(snapshot.Labels)
	return snapshot, true
}

func matchesRunnerSelector(labels map[string]string, selector *types.RunnerSelector) bool {
	if selector == nil || len(selector.MatchLabels) == 0 {
		return true
	}
	for key, want := range selector.MatchLabels {
		if labels[key] != want {
			return false
		}
	}
	return true
}

func cloneCapabilities(capabilities []protocol.Capability) []protocol.Capability {
	if len(capabilities) == 0 {
		return nil
	}
	clone := make([]protocol.Capability, len(capabilities))
	copy(clone, capabilities)
	return clone
}

func cloneLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	clone := make(map[string]string, len(labels))
	for key, value := range labels {
		clone[key] = value
	}
	return clone
}

func (p *RunnerPool) bindSession(runnerID string, s *streamSession) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if state, ok := p.runners[runnerID]; ok {
		state.session = s
	}
}
