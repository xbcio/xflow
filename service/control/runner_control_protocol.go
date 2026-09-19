package control

import (
	"context"

	"github.com/xbcio/xflow/service/protocol"
)

// runnerControlDirective projects the directory-owned desired state onto the
// runner protocol. It is deliberately best effort: a failed projection must not
// turn a heartbeat or successful registration into a transport error, because
// the directory's atomic ClaimForRunner gate remains the safety authority.
func (c *Core) runnerControlDirective(ctx context.Context, runnerID string) *protocol.RunnerControlDirective {
	directory, ok := c.runners.(RunnerControlDirectory)
	if !ok || directory == nil {
		return nil
	}
	// Poll and register consume only the desired state and generation. The full
	// projection additionally aggregates fleet-wide handoff and deactivation debt
	// from whole-key-space hashes, which this caller discards, so prefer the
	// lightweight read whenever the directory provides it.
	if states, ok := c.runners.(RunnerControlStateDirectory); ok && states != nil {
		state, found, err := states.RunnerControlState(ctx, runnerID)
		if err != nil || !found {
			return nil
		}
		return runnerControlDirectiveFrom(state)
	}
	snapshot, found, err := directory.RunnerControl(ctx, runnerID)
	if err != nil || !found {
		return nil
	}
	return runnerControlDirectiveFrom(RunnerControlState{
		DesiredState: snapshot.DesiredState,
		Generation:   snapshot.Generation,
	})
}

func runnerControlDirectiveFrom(state RunnerControlState) *protocol.RunnerControlDirective {
	desired := state.DesiredState
	if desired == "" {
		desired = RunnerDesiredStateActive
	}
	return &protocol.RunnerControlDirective{
		DesiredState: string(desired),
		Generation:   state.Generation,
		RecoveryOnly: desired == RunnerDesiredStateDraining,
	}
}
