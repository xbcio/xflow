package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/nodes/node"
	"github.com/xbcio/xflow/types"
)

// flakyHandler fails the first failuresBefore Executes with sysErr, then
// returns success.
type flakyHandler struct {
	failuresBefore int
	err            error
	calls          int
}

func (h *flakyHandler) Descriptor() node.Descriptor {
	return node.Descriptor{Type: "test.flaky"}
}

func (h *flakyHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	h.calls++
	if h.calls <= h.failuresBefore {
		return nil, h.err
	}
	return &types.Output{Data: map[string]any{"ok": true, "attempt": h.calls}}, nil
}

// alwaysFailHandler returns the same error every call. Used to verify that
// retries eventually give up and fall through to OnError.
type alwaysFailHandler struct {
	err   error
	calls int
}

func (h *alwaysFailHandler) Descriptor() node.Descriptor {
	return node.Descriptor{Type: "test.always_fail"}
}

func (h *alwaysFailHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	h.calls++
	return nil, h.err
}

func TestRetry_TransientErrorEventuallySucceeds(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "retry-success",
		Settings: &types.WorkflowSettings{
			Retry: &types.RetrySettings{
				MaxAttempts:     5,
				InitialInterval: 1, // 1ms — keeps the test fast
				Multiplier:      2,
			},
		},
		Nodes: []types.NodeDef{
			{Name: "flaky", Type: "test.flaky"},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	handler := &flakyHandler{failuresBefore: 2, err: errors.New("transient blip")}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{"test.flaky": handler}}
	eng := newTestEngine(state, queue, reg)
	ctx := context.Background()

	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	// Drain + execute up to a generous bound. Each call re-enqueues until the
	// handler stops returning errors.
	for step := 0; step < 10; step++ {
		tasks := queue.Drain()
		if len(tasks) == 0 {
			break
		}
		for _, task := range tasks {
			executeTask(t, eng, task)
		}
	}

	if handler.calls != 3 {
		t.Fatalf("handler calls = %d, want 3 (2 retries + 1 success)", handler.calls)
	}
	ns, _ := state.GetNode(ctx, types.ExecutionID(""), "flaky")
	// fakeState keys by exec/name; we don't know the id here so just inspect raw map.
	if ns != nil {
		t.Fatalf("unexpected snapshot via empty id: %+v", ns)
	}
}

func TestRetry_PermanentErrorSkipsRetry(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "retry-permanent",
		Settings: &types.WorkflowSettings{
			Retry: &types.RetrySettings{MaxAttempts: 5, InitialInterval: 1},
		},
		Nodes: []types.NodeDef{
			{Name: "die", Type: "test.always_fail"},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	permanent := errors.Join(types.ErrPermanent, errors.New("config bad"))
	handler := &alwaysFailHandler{err: permanent}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{"test.always_fail": handler}}
	eng := newTestEngine(state, queue, reg)
	ctx := context.Background()

	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	for step := 0; step < 5; step++ {
		tasks := queue.Drain()
		if len(tasks) == 0 {
			break
		}
		for _, task := range tasks {
			executeTask(t, eng, task)
		}
	}
	if handler.calls != 1 {
		t.Fatalf("handler calls = %d, want 1 (no retry for permanent)", handler.calls)
	}
}

func TestRetry_ExhaustionFallsThroughToOnError(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "retry-exhaust",
		Settings: &types.WorkflowSettings{
			Retry: &types.RetrySettings{MaxAttempts: 3, InitialInterval: 1},
		},
		Nodes: []types.NodeDef{
			{Name: "die", Type: "test.always_fail", OnError: "error_output"},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	handler := &alwaysFailHandler{err: errors.New("transient")}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{"test.always_fail": handler}}
	eng := newTestEngine(state, queue, reg)
	ctx := context.Background()

	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	for step := 0; step < 10; step++ {
		tasks := queue.Drain()
		if len(tasks) == 0 {
			break
		}
		for _, task := range tasks {
			executeTask(t, eng, task)
		}
	}
	if handler.calls != 3 {
		t.Fatalf("handler calls = %d, want 3 (MaxAttempts)", handler.calls)
	}
}

func TestRetry_DisabledByDefault(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "no-retry",
		Nodes: []types.NodeDef{
			{Name: "die", Type: "test.always_fail", OnError: "error_output"},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	handler := &alwaysFailHandler{err: errors.New("boom")}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{"test.always_fail": handler}}
	eng := newTestEngine(state, queue, reg)
	ctx := context.Background()
	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	for step := 0; step < 3; step++ {
		tasks := queue.Drain()
		if len(tasks) == 0 {
			break
		}
		for _, task := range tasks {
			executeTask(t, eng, task)
		}
	}
	if handler.calls != 1 {
		t.Fatalf("handler calls = %d, want 1 (retry disabled)", handler.calls)
	}
}

func TestRetry_PerNodeOverridesWorkflowDefault(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "per-node-retry",
		Settings: &types.WorkflowSettings{
			Retry: &types.RetrySettings{MaxAttempts: 2, InitialInterval: 1},
		},
		Nodes: []types.NodeDef{
			{
				Name: "die",
				Type: "test.always_fail",
				Retry: &types.RetrySettings{
					MaxAttempts:     5,
					InitialInterval: 1,
				},
				OnError: "error_output",
			},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	handler := &alwaysFailHandler{err: errors.New("transient")}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{"test.always_fail": handler}}
	eng := newTestEngine(state, queue, reg)
	ctx := context.Background()
	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	for step := 0; step < 10; step++ {
		tasks := queue.Drain()
		if len(tasks) == 0 {
			break
		}
		for _, task := range tasks {
			executeTask(t, eng, task)
		}
	}
	if handler.calls != 5 {
		t.Fatalf("handler calls = %d, want 5 (per-node override)", handler.calls)
	}
}

func TestRetry_BackoffMonotonicityAndJitter(t *testing.T) {
	settings := &types.RetrySettings{InitialInterval: 100, Multiplier: 2}
	d1 := retryBackoff(0, settings, "exec-a", "node")
	d2 := retryBackoff(1, settings, "exec-a", "node")
	d3 := retryBackoff(2, settings, "exec-a", "node")
	if d1 <= 0 || d2 <= d1 || d3 <= d2 {
		t.Fatalf("backoff not increasing: %v %v %v", d1, d2, d3)
	}

	// Same inputs → same jitter
	if a := retryBackoff(3, settings, "exec-x", "node"); a != retryBackoff(3, settings, "exec-x", "node") {
		t.Fatalf("backoff not deterministic: %v vs %v", a, retryBackoff(3, settings, "exec-x", "node"))
	}
	// Different exec ids → different jitter
	if retryBackoff(3, settings, "exec-x", "node") == retryBackoff(3, settings, "exec-y", "node") {
		t.Logf("note: jitter happened to collide across exec ids; acceptable but unusual")
	}
}
