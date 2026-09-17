package runner

import (
	"sync"

	"github.com/xbcio/xflow/service/protocol"
)

const (
	runnerControlActive   = "active"
	runnerControlDraining = "draining"
)

// runnerControlGate is the runner-side convergence layer for graceful drain.
// It never provides server-side safety: a control-plane directory independently
// fences new claims. Its job is to stop ordinary polling promptly, keep recovery
// polling alive for pre-drain leases, and handle duplicate/out-of-order protocol
// responses without reopening admission accidentally.
type runnerControlGate struct {
	mu         sync.RWMutex
	generation uint64
	desired    string
}

func newRunnerControlGate() *runnerControlGate {
	return &runnerControlGate{desired: runnerControlActive}
}

func (g *runnerControlGate) apply(directive *protocol.RunnerControlDirective) {
	if g == nil || directive == nil {
		return
	}
	desired := directive.DesiredState
	// A malformed directive must fail closed locally. Old servers omit the
	// entire directive (nil), whereas a present but unknown value is not safe to
	// interpret as ACTIVE. RecoveryOnly additionally tightens an ACTIVE-looking
	// directive and can never create new work.
	if desired != runnerControlActive && desired != runnerControlDraining {
		desired = runnerControlDraining
	}
	if directive.RecoveryOnly {
		desired = runnerControlDraining
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	if directive.Generation < g.generation {
		return
	}
	if directive.Generation == g.generation {
		if g.desired == runnerControlDraining {
			// Conflicting equal-generation ACTIVE must not reopen scheduling.
			return
		}
		if desired != runnerControlDraining {
			return
		}
	}
	g.generation = directive.Generation
	g.desired = desired
}

func (g *runnerControlGate) recoveryOnly() bool {
	if g == nil {
		return false
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.desired == runnerControlDraining
}

// drainingGeneration reports the exact directive generation this process has
// already applied. A heartbeat may use it as drain evidence without trusting a
// response that merely arrived but has not converged the local gate yet.
func (g *runnerControlGate) drainingGeneration() (uint64, bool) {
	if g == nil {
		return 0, false
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.desired != runnerControlDraining {
		return 0, false
	}
	return g.generation, true
}

func (r *Runner) applyRunnerControl(directive *protocol.RunnerControlDirective) {
	if r.controlGate != nil {
		r.controlGate.apply(directive)
	}
}
