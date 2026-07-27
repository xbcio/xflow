package xflow

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/registry"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

func TestTriggerReconcileWorkflow(t *testing.T) {
	t.Run("rolls back subscriptions when a later trigger handler is not registered", func(t *testing.T) {
		var activateCount int
		closed := make(chan struct{}, 1)
		missingType := "test.trigger.reconcile.rollback.unregistered.missing"

		first := node.DefineTrigger("test.trigger.reconcile.rollback.unregistered.first", func(context.Context, *types.TriggerActivateInput) (types.TriggerSubscription, error) {
			activateCount++
			return types.CloseFunc(func(context.Context) error {
				closed <- struct{}{}
				return nil
			}), nil
		})
		registry.RegisterTrigger(first)
		runtime := newTriggerRuntime(nil, local.New().TriggerPrimitives())
		rec := backend.WorkflowRecord{
			ID: "wf-rollback-unregistered",
			Definition: &types.WorkflowDef{
				Nodes: []types.NodeDef{
					{Name: "first", Kind: types.NodeKindTrigger, Type: first.Descriptor().Type},
					{Name: "second", Kind: types.NodeKindTrigger, Type: missingType},
				},
			},
		}

		err := runtime.ReconcileWorkflow(context.Background(), rec)
		wantErr := `trigger handler "test.trigger.reconcile.rollback.unregistered.missing" not registered`
		if err == nil || err.Error() != wantErr {
			t.Fatalf("ReconcileWorkflow() error = %v, want %q", err, wantErr)
		}
		if activateCount != 1 {
			t.Fatalf("activation count = %d, want 1", activateCount)
		}
		select {
		case <-closed:
		default:
			t.Fatal("first subscription was not closed after missing handler failure")
		}
		if got := len(runtime.subs); got != 0 {
			t.Fatalf("subscription count = %d, want 0", got)
		}
	})

	t.Run("rolls back subscriptions when activation fails partway through", func(t *testing.T) {
		var activateCount int
		closed := make(chan struct{}, 1)
		wantErr := errors.New("activate second trigger")

		first := node.DefineTrigger("test.trigger.reconcile.rollback.first", func(context.Context, *types.TriggerActivateInput) (types.TriggerSubscription, error) {
			activateCount++
			return types.CloseFunc(func(context.Context) error {
				closed <- struct{}{}
				return nil
			}), nil
		})
		second := node.DefineTrigger("test.trigger.reconcile.rollback.second", func(context.Context, *types.TriggerActivateInput) (types.TriggerSubscription, error) {
			activateCount++
			return nil, wantErr
		})
		registry.RegisterTrigger(first)
		registry.RegisterTrigger(second)
		runtime := newTriggerRuntime(nil, local.New().TriggerPrimitives())
		rec := backend.WorkflowRecord{
			ID: "wf-rollback",
			Definition: &types.WorkflowDef{
				Nodes: []types.NodeDef{
					{Name: "first", Kind: types.NodeKindTrigger, Type: first.Descriptor().Type},
					{Name: "second", Kind: types.NodeKindTrigger, Type: second.Descriptor().Type},
				},
			},
		}

		err := runtime.ReconcileWorkflow(context.Background(), rec)
		if !errors.Is(err, wantErr) {
			t.Fatalf("ReconcileWorkflow() error = %v, want %v", err, wantErr)
		}
		if activateCount != 2 {
			t.Fatalf("activation count = %d, want 2", activateCount)
		}
		select {
		case <-closed:
		default:
			t.Fatal("first subscription was not closed after partial activation failure")
		}
		if got := len(runtime.subs); got != 0 {
			t.Fatalf("subscription count = %d, want 0", got)
		}
	})

	t.Run("does not duplicate subscriptions on repeated reconcile", func(t *testing.T) {
		var activateCount int

		trigger := node.DefineTrigger("test.trigger.reconcile.idempotent", func(context.Context, *types.TriggerActivateInput) (types.TriggerSubscription, error) {
			activateCount++
			return types.CloseFunc(func(context.Context) error { return nil }), nil
		})
		registry.RegisterTrigger(trigger)
		runtime := newTriggerRuntime(nil, local.New().TriggerPrimitives())
		rec := backend.WorkflowRecord{
			ID: "wf-idempotent",
			Definition: &types.WorkflowDef{
				Nodes: []types.NodeDef{
					{Name: "trigger", Kind: types.NodeKindTrigger, Type: trigger.Descriptor().Type},
				},
			},
		}

		if err := runtime.ReconcileWorkflow(context.Background(), rec); err != nil {
			t.Fatalf("first ReconcileWorkflow() error = %v", err)
		}
		if err := runtime.ReconcileWorkflow(context.Background(), rec); err != nil {
			t.Fatalf("second ReconcileWorkflow() error = %v", err)
		}
		if activateCount != 1 {
			t.Fatalf("activation count = %d, want 1", activateCount)
		}
	})

	t.Run("serializes concurrent duplicate reconcile for the same workflow", func(t *testing.T) {
		prev := runtime.GOMAXPROCS(8)
		defer runtime.GOMAXPROCS(prev)

		var activateCount atomic.Int32
		firstActivated := make(chan struct{})
		releaseFirst := make(chan struct{})

		trigger := node.DefineTrigger("test.trigger.reconcile.concurrent", func(context.Context, *types.TriggerActivateInput) (types.TriggerSubscription, error) {
			call := activateCount.Add(1)
			if call == 1 {
				close(firstActivated)
				<-releaseFirst
			}
			return types.CloseFunc(func(context.Context) error { return nil }), nil
		})
		registry.RegisterTrigger(trigger)
		runtime := newTriggerRuntime(nil, local.New().TriggerPrimitives())
		rec := backend.WorkflowRecord{
			ID: "wf-concurrent-idempotent",
			Definition: &types.WorkflowDef{
				Nodes: []types.NodeDef{
					{Name: "trigger", Kind: types.NodeKindTrigger, Type: trigger.Descriptor().Type},
				},
			},
		}

		errCh := make(chan error, 17)
		go func() {
			errCh <- runtime.ReconcileWorkflow(context.Background(), rec)
		}()

		select {
		case <-firstActivated:
		case <-time.After(time.Second):
			t.Fatal("first activation did not start")
		}

		var wg sync.WaitGroup
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errCh <- runtime.ReconcileWorkflow(context.Background(), rec)
			}()
		}

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(50 * time.Millisecond):
		}

		close(releaseFirst)

		for i := 0; i < cap(errCh); i++ {
			if err := <-errCh; err != nil {
				t.Fatalf("ReconcileWorkflow() error = %v", err)
			}
		}
		if got := activateCount.Load(); got != 1 {
			t.Fatalf("activation count = %d, want 1", got)
		}
		if got := len(runtime.subs); got != 1 {
			t.Fatalf("subscription count = %d, want 1", got)
		}
	})
}
