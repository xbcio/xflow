package rstate

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// TestTransientStateRejectsSuspend verifies the defense-in-depth guards added
// to the asynq Store suspend entry points. Transient mode disables suspend
// at the engine layer (engine.WithSuspendDisabled), so these methods are
// unreachable in normal transient operation; the guards ensure a direct
// StateStore caller cannot park a transient waiter that would never resume.
func TestTransientStateRejectsSuspend(t *testing.T) {
	rdb := newRedisStateTestClient(t)
	state := New(rdb, nil, time.Hour)
	state.transient = true
	state.transientTTL = time.Minute
	state.transientCompletionTTL = 30 * time.Second

	ctx := context.Background()
	id := types.ExecutionID("exec-transient-suspend")
	spec := &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"approval"}}
	lease := &engine.TaskLease{Task: engine.Task{ExecutionID: id, NodeName: "wait"}}

	if _, err := state.SuspendOrConsume(ctx, id, "wait", spec); !errors.Is(err, engine.ErrSuspendUnsupported) {
		t.Fatalf("SuspendOrConsume() error = %v, want ErrSuspendUnsupported", err)
	}
	if _, _, err := state.SuspendTaskLease(ctx, lease, nil, false, spec, ""); !errors.Is(err, engine.ErrSuspendUnsupported) {
		t.Fatalf("SuspendTaskLease() error = %v, want ErrSuspendUnsupported", err)
	}
	if _, err := state.ResuspendAtomic(ctx, id, "wait", "", "approval", spec); !errors.Is(err, engine.ErrSuspendUnsupported) {
		t.Fatalf("ResuspendAtomic() error = %v, want ErrSuspendUnsupported", err)
	}
}
