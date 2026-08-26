package xflow

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/node/trigger"
	"github.com/xbcio/xflow/types"
)

// TestConcurrentAddWorkflow_NoRace exercises concurrent AddWorkflow calls to
// verify that the Engine.mu serialization prevents "concurrent map read and map
// write" panics on directHandlerNames. Run with -race.
func TestConcurrentAddWorkflow_NoRace(t *testing.T) {
	eng, err := NewLocal()
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Stop()

	const goroutines = 16
	var wg sync.WaitGroup
	errs := make([]error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			wf := Workflow("concurrent-wf")
			start := wf.Node("start", node.Start())
			action := wf.LocalNode("action", &echoControlHandler{})
			wf.Connect(start, action)
			_, errs[idx] = eng.AddWorkflow(context.Background(), wf)
		}(i)
	}
	wg.Wait()

	for i, e := range errs {
		if e != nil {
			t.Errorf("goroutine %d: AddWorkflow() error = %v", i, e)
		}
	}
}

// TestConcurrentAddWorkflow_DifferentWorkflows exercises concurrent AddWorkflow
// with distinct workflow definitions that register different direct handler
// names. Run with -race to detect map races.
func TestConcurrentAddWorkflow_DifferentWorkflows(t *testing.T) {
	eng, err := NewLocal()
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Stop()

	const goroutines = 8
	var wg sync.WaitGroup
	errs := make([]error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := "wf-" + string(rune('A'+idx))
			wf := Workflow(name)
			start := wf.Node("start", node.Start())
			nodeName := "action-" + string(rune('A'+idx))
			action := wf.LocalNode(nodeName, &echoControlHandler{})
			wf.Connect(start, action)
			_, errs[idx] = eng.AddWorkflow(context.Background(), wf)
		}(i)
	}
	wg.Wait()

	for i, e := range errs {
		if e != nil {
			t.Errorf("goroutine %d: AddWorkflow() error = %v", i, e)
		}
	}
}

// TestTriggerRuntimeClose_NoRace verifies that Close does not race on
// len(r.subs) access. Run with -race.
func TestTriggerRuntimeClose_NoRace(t *testing.T) {
	trg := trigger.Define("test.trigger.close.race", func(ctx context.Context, in *types.TriggerActivateInput) (types.TriggerSubscription, error) {
		return types.CloseFunc(func(context.Context) error { return nil }), nil
	})
	registry.RegisterTrigger(trg)

	rt := newTriggerRuntime(nil, local.New().TriggerPrimitives())
	rec := backend.WorkflowRecord{
		ID: "wf-close-race",
		Definition: &types.WorkflowDef{
			Nodes: []types.NodeDef{
				{Name: "t1", Kind: types.NodeKindTrigger, Type: trg.Descriptor().Type},
			},
		},
	}
	if err := rt.ReconcileWorkflow(context.Background(), rec); err != nil {
		t.Fatal(err)
	}

	// Concurrently Close and ReconcileWorkflow to trigger the data race if
	// len(r.subs) is read before taking the lock.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = rt.Close(context.Background())
	}()
	go func() {
		defer wg.Done()
		_ = rt.ReconcileWorkflow(context.Background(), rec)
	}()
	wg.Wait()
}

// TestReconcileWorkflow_NilDefinition ensures ReconcileWorkflow does not panic
// when rec.Definition is nil.
func TestReconcileWorkflow_NilDefinition(t *testing.T) {
	rt := newTriggerRuntime(nil, local.New().TriggerPrimitives())
	rec := backend.WorkflowRecord{
		ID:         "wf-nil-def",
		Definition: nil,
	}
	err := rt.ReconcileWorkflow(context.Background(), rec)
	if err != nil {
		t.Fatalf("ReconcileWorkflow(nil definition) = %v, want nil", err)
	}
}

// TestWait_PersistentBackendError verifies that Wait returns the backend error
// instead of spinning until context timeout.
//
// This test is deliberately NOT built with NewLocal(). It used to be, and that
// made it a test of the context package. NewLocal's provider is *local.Backend,
// which implements backend.Waiter, so newFromConfig assigns it to cfg.waiter —
// and Wait's very first statement is `if e.waiter != nil { return
// e.waiter.WaitDone(...) }`. The consecutiveErrs / maxConsecutiveErrors block
// this test is named after never ran. What it actually exercised was
// local.Backend.WaitDone selecting on a channel that never closes for a
// nonexistent id, so ctx.Err() came back after 600ms. Go guarantees that value
// is context.DeadlineExceeded; no implementation of the polling fallback could
// have changed it, and deleting the whole fallback left the test green.
//
// Constructing the Engine directly leaves waiter nil, which is the only way
// into the fallback, and a StateStore that always fails is the only way to
// reach the giving-up branch.
func TestWait_PersistentBackendError(t *testing.T) {
	base := local.New()
	wantErr := errors.New("boom: state store unreachable")
	eng := &Engine{eng: engine.New(&erroringStateStore{
		StateStore: base.State(),
		err:        wantErr,
	}, base.Queue())}

	// 5 attempts at one 500ms tick apart is ~2s; this bound is a failure
	// ceiling, not the thing being waited on. If it ever fires, the assertions
	// below report DeadlineExceeded and say so rather than passing.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := eng.Wait(ctx, "nonexistent-exec-id")
	if err == nil {
		t.Fatal("Wait() = nil, want the persistent backend error")
	}
	// The count is not "at least a few" — maxConsecutiveErrors is 5 and every
	// attempt fails, so 5 is the only correct answer. Lowering the threshold or
	// forgetting to reset the counter both move this number.
	if !strings.Contains(err.Error(), "persistent backend error after 5 attempts") {
		t.Fatalf("Wait() = %v, want it to give up naming 5 attempts on the "+
			"persistent-backend-error path", err)
	}
	// %w, not %v: a caller that wants to distinguish a store outage from a
	// workflow failure has to be able to unwrap to the cause.
	if !errors.Is(err, wantErr) {
		t.Fatalf("Wait() = %v, want it to wrap %v", err, wantErr)
	}
}

// erroringStateStore fails every GetExecution while delegating the rest of the
// StateStore surface to a real implementation, so Wait's fallback loop sees a
// backend that is down rather than one that merely has nothing to report.
// Returning (nil, nil) — which is what a missing execution looks like — would
// keep the loop polling forever and prove nothing.
type erroringStateStore struct {
	engine.StateStore
	err error
}

func (s *erroringStateStore) GetExecution(context.Context, types.ExecutionID) (*engine.ExecutionSnapshot, error) {
	return nil, s.err
}
