package backend

import "context"

// LeaderElector is an optional backend capability for coordinating
// leader-only background work (e.g. lease sweeping, reconciliation) across
// multiple ControlPlane replicas that share the same backend.
//
// Detected via type-assertion on backend.Provider, mirroring Waiter. A
// backend that cannot have more than one replica (e.g. in-memory) returns
// AlwaysLeader instead of implementing real coordination.
type LeaderElector interface {
	// Campaign blocks until this instance becomes leader or ctx is canceled.
	Campaign(ctx context.Context) error
	// IsLeader reports current leadership without blocking. Must return false
	// when leadership state is unknown (e.g. lost connection to the
	// coordination backend) rather than assume leadership.
	IsLeader() bool
	// Resign releases leadership voluntarily, e.g. during graceful shutdown,
	// so another replica can take over without waiting for a lease to expire.
	Resign(ctx context.Context) error
	// Notify returns a channel that emits on every leadership change: true
	// when leadership is acquired, false when it is lost. Never closed.
	Notify() <-chan bool
}

// AlwaysLeader is the LeaderElector for backends that cannot have more than
// one replica contending for the same state (e.g. in-memory). Campaign
// returns immediately; IsLeader is always true.
type AlwaysLeader struct{}

func (AlwaysLeader) Campaign(context.Context) error { return nil }
func (AlwaysLeader) IsLeader() bool                 { return true }
func (AlwaysLeader) Resign(context.Context) error   { return nil }

// AlwaysLeader.Notify returns a buffered channel pre-loaded with true (the
// current leadership state). Because AlwaysLeader never loses leadership, no
// further values are ever sent — the channel correctly "emits on every change"
// (zero changes = zero subsequent sends). The channel is never closed per the
// interface contract. Consumers must use select with a ctx.Done case (as
// runLeaderCampaign does); a bare for-range would block after the initial read,
// which is expected since there is nothing further to report (#9).
func (AlwaysLeader) Notify() <-chan bool {
	ch := make(chan bool, 1)
	ch <- true
	return ch
}
