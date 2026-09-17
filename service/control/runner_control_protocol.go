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
	snapshot, found, err := directory.RunnerControl(ctx, runnerID)
	if err != nil || !found {
		return nil
	}
	desired := snapshot.DesiredState
	if desired == "" {
		desired = RunnerDesiredStateActive
	}
	return &protocol.RunnerControlDirective{
		DesiredState: string(desired),
		Generation:   snapshot.Generation,
		RecoveryOnly: desired == RunnerDesiredStateDraining,
	}
}
