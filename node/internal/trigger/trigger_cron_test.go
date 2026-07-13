package trigger

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

func TestCronTriggerDescriptor(t *testing.T) {
	n := CronTrigger()
	desc := n.Descriptor()
	if desc.Type != "xflow.trigger.cron" || desc.Kind != types.NodeKindTrigger {
		t.Fatalf("descriptor = %+v", desc)
	}
}

func TestCronTriggerCloseCancelsInFlightDedup(t *testing.T) {
	rt := newBlockingDedupRuntime()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr := CronTrigger().Cron("@every 1s")
	sub, err := tr.Activate(ctx, &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "cron",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}

	if !rt.waitStarted(2500 * time.Millisecond) {
		t.Fatal("cron callback did not start")
	}

	closed := make(chan error, 1)
	go func() {
		closed <- sub.Close(context.Background())
	}()

	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		rt.release()
		cancel()
		t.Fatal("Close did not return after cancelling the activation context")
	}
}

type blockingDedupRuntime struct {
	started   chan struct{}
	releaseCh chan struct{}
}

func newBlockingDedupRuntime() *blockingDedupRuntime {
	return &blockingDedupRuntime{
		started:   make(chan struct{}, 1),
		releaseCh: make(chan struct{}),
	}
}

func (r *blockingDedupRuntime) Emit(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
	return "", errors.New("Emit should not be called")
}

func (r *blockingDedupRuntime) Dedup(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	select {
	case r.started <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-r.releaseCh:
		return false, nil
	}
}

func (r *blockingDedupRuntime) TryLock(context.Context, string, time.Duration) (types.TriggerLock, bool, error) {
	return fakeTriggerLock{}, true, nil
}

func (r *blockingDedupRuntime) State(context.Context, string) types.TriggerState { return nil }

func (r *blockingDedupRuntime) waitStarted(timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-r.started:
		return true
	case <-timer.C:
		return false
	}
}

func (r *blockingDedupRuntime) release() {
	select {
	case <-r.releaseCh:
	default:
		close(r.releaseCh)
	}
}
