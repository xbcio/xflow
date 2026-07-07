package control

import (
	"sync"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

// streamSession is the server-side handle to one active Connect stream. The
// Core holds it in RunnerPool so AssignRouted can wake the drain loop and the
// recv loop can replenish credit on RESULT frames. _credit is guarded by mu
// because drainInto (send loop) and consumeResult (recv loop) run in different
// goroutines.
type streamSession struct {
	runnerID string
	send     chan<- protocol.ServerFrame
	done     <-chan struct{}
	mu       sync.Mutex
	_credit  int
}

func newStreamSession(runnerID string, send chan<- protocol.ServerFrame, done <-chan struct{}, credit int) *streamSession {
	return &streamSession{runnerID: runnerID, send: send, done: done, _credit: credit}
}

// credit returns the current credit count (mutex-protected).
func (s *streamSession) credit() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s._credit
}

// drainInto pops queued leases and sends TASK frames while credit > 0.
// Returns number pushed. Non-blocking on send: if send channel is full, the
// lease is re-queued and drainInto stops.
//
// Lock ordering: session.mu is acquired briefly to read/decrement credit;
// p.mu is acquired separately for queue operations. We never hold p.mu while
// taking session.mu — that would invert consumeResult's order and deadlock.
func (p *RunnerPool) drainInto(s *streamSession) int {
	pushed := 0
	for {
		// Check credit without holding p.mu.
		s.mu.Lock()
		if s._credit <= 0 {
			s.mu.Unlock()
			return pushed
		}
		s.mu.Unlock()

		// Pop next runnable lease from the queue.
		p.mu.Lock()
		state, ok := p.runners[s.runnerID]
		if !ok || state == nil {
			p.mu.Unlock()
			return pushed
		}
		idx := -1
		for i, l := range state.queue {
			if canRun(state.snapshot.Capabilities, engine.TaskRouting{NodeType: l.NodeType, NodeVersion: l.NodeVersion}) {
				idx = i
				break
			}
		}
		if idx < 0 {
			p.mu.Unlock()
			return pushed
		}
		lease := state.queue[idx]
		state.queue = append(state.queue[:idx], state.queue[idx+1:]...)
		p.mu.Unlock()

		// Attempt non-blocking send; re-queue on channel full.
		fr := protocol.ServerFrame{Task: &protocol.TaskFrame{Lease: &lease}}
		select {
		case s.send <- fr:
			s.mu.Lock()
			s._credit--
			s.mu.Unlock()
			pushed++
		default:
			// Channel full — put the lease back at the front.
			p.mu.Lock()
			state, ok = p.runners[s.runnerID]
			if ok && state != nil {
				state.queue = append([]engine.TaskLease{lease}, state.queue...)
			}
			p.mu.Unlock()
			return pushed
		}
	}
}

// consumeResult replenishes one credit and wakes the drain loop.
// Lock ordering: p.mu first, then session.mu (same order as drainInto's
// p.mu acquisition; credit check in drainInto releases session.mu before
// taking p.mu, so no deadlock).
func (p *RunnerPool) consumeResult(runnerID, _ string) {
	p.mu.Lock()
	state, ok := p.runners[runnerID]
	if ok && state.session != nil {
		state.session.mu.Lock()
		state.session._credit++
		state.session.mu.Unlock()
		select {
		case state.notify <- struct{}{}:
		default:
		}
	}
	p.mu.Unlock()
}

// notifyChan returns a read-only view of the runner's notify channel for the
// send loop to select on. Returns nil if the runner is gone.
func (p *RunnerPool) notifyChan(runnerID string) <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	if state, ok := p.runners[runnerID]; ok {
		return state.notify
	}
	return nil
}
