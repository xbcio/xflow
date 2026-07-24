package execution

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
	"google.golang.org/grpc"
)

// sentinelPool is a ResourcePool used to assert that the Runner injects the
// exact pool it was constructed with into the handler's context. SQL/GRPC are
// never called by these tests.
type sentinelPool struct{ id string }

func (s sentinelPool) SQL(context.Context, string, string) (*sql.DB, error) { return nil, nil }
func (s sentinelPool) GRPC(context.Context, string, bool, ...grpc.DialOption) (*grpc.ClientConn, error) {
	return nil, nil
}
func (s sentinelPool) Close(context.Context) error { return nil }

// poolRecordingHandler captures the ResourcePool that the Runner attached to
// its context. It is safe for concurrent use; each Execute call records the
// observed pool under the call's execution id.
type poolRecordingHandler struct {
	mu     sync.Mutex
	seen   map[string]types.ResourcePool
	seenID map[string]string
}

func newPoolRecordingHandler() *poolRecordingHandler {
	return &poolRecordingHandler{seen: make(map[string]types.ResourcePool), seenID: make(map[string]string)}
}

func (h *poolRecordingHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.pool-probe"}
}

func (h *poolRecordingHandler) Execute(ctx context.Context, input *types.Input) (*types.Output, error) {
	pool := types.ResourcePoolFromContext(ctx)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seen[input.ExecutionID] = pool
	if pool != nil {
		if sp, ok := pool.(sentinelPool); ok {
			h.seenID[input.ExecutionID] = sp.id
		}
	}
	return &types.Output{Data: map[string]any{"ok": true}}, nil
}

func (h *poolRecordingHandler) observed(id string) (types.ResourcePool, string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.seen[id], h.seenID[id]
}

// TestRunner_WithResourcePoolAttachesPoolToHandlerContext pins the contract at
// execution/runner.go:48 — when a Runner is constructed via WithResourcePool(p),
// Execute must attach p to the context that reaches the handler. This is the
// integration-level guard for the runner→ctx→handler wiring that the default
// pool options (Test 1) rely on at runtime.
func TestRunner_WithResourcePoolAttachesPoolToHandlerContext(t *testing.T) {
	want := sentinelPool{id: "runner-pool"}
	rec := newPoolRecordingHandler()
	runner := NewRunner(singleHandlerRegistry{handler: rec}, WithResourcePool(want))

	lease := &engine.TaskLease{
		Task:     engine.Task{ExecutionID: "exec-pool", NodeName: "probe"},
		Input:    &types.Input{ExecutionID: "exec-pool", NodeName: "probe"},
		NodeType: "test.pool-probe",
	}
	if _, err := runner.Execute(context.Background(), lease); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	got, gotID := rec.observed("exec-pool")
	if got == nil {
		t.Fatal("handler observed nil pool, want the sentinel pool injected by the Runner")
	}
	if gotID != want.id {
		t.Fatalf("handler observed pool id = %q, want %q", gotID, want.id)
	}
}

// TestRunner_NoPoolLeavesCtxWithoutPool pins the complementary contract: a
// Runner constructed WITHOUT WithResourcePool must not inject a pool, so
// types.ResourcePoolFromContext(ctx) returns nil inside the handler.
// Resource-aware nodes (DatabaseNode/GRPCNode) rely on this to error with
// "no resource pool configured" (see Test 2).
func TestRunner_NoPoolLeavesCtxWithoutPool(t *testing.T) {
	rec := newPoolRecordingHandler()
	runner := NewRunner(singleHandlerRegistry{handler: rec})

	lease := &engine.TaskLease{
		Task:     engine.Task{ExecutionID: "exec-nopool", NodeName: "probe"},
		Input:    &types.Input{ExecutionID: "exec-nopool", NodeName: "probe"},
		NodeType: "test.pool-probe",
	}
	if _, err := runner.Execute(context.Background(), lease); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	got, _ := rec.observed("exec-nopool")
	if got != nil {
		t.Fatalf("handler observed non-nil pool %T, want nil (no pool injected)", got)
	}
}

// TestRunner_NilPoolOptionIsNoOp pins that WithResourcePool(nil) does NOT
// attach a nil pool to the context in a way that would break the no-pool
// contract. types.WithResourcePool(nil) is a no-op (returns ctx unchanged), so
// the handler must see nil. This guards against a future change that injects
// a nil pool value into the context, which ResourcePoolFromContext would then
// return as nil anyway — but the explicit assertion makes the intent clear.
func TestRunner_NilPoolOptionIsNoOp(t *testing.T) {
	rec := newPoolRecordingHandler()
	runner := NewRunner(singleHandlerRegistry{handler: rec}, WithResourcePool(nil))

	lease := &engine.TaskLease{
		Task:     engine.Task{ExecutionID: "exec-nilpool", NodeName: "probe"},
		Input:    &types.Input{ExecutionID: "exec-nilpool", NodeName: "probe"},
		NodeType: "test.pool-probe",
	}
	if _, err := runner.Execute(context.Background(), lease); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	got, _ := rec.observed("exec-nilpool")
	if got != nil {
		t.Fatalf("handler observed non-nil pool %T with WithResourcePool(nil), want nil", got)
	}
}

// TestRunner_ExecuteReturnsHandlerError pins that when the handler returns a
// non-nil error, Execute propagates it as TaskResult.Error without swallowing.
// This is adjacent to the pool wiring contract: it ensures the runner's error
// surface is stable when resource-aware nodes fail (e.g. the no-pool path).
func TestRunner_ExecuteReturnsHandlerError(t *testing.T) {
	want := errors.New("handler boom")
	runner := NewRunner(singleHandlerRegistry{handler: errHandler{err: want}})
	lease := &engine.TaskLease{
		Task:     engine.Task{ExecutionID: "exec-err", NodeName: "probe"},
		Input:    &types.Input{ExecutionID: "exec-err", NodeName: "probe"},
		NodeType: "test.err",
	}
	res, err := runner.Execute(context.Background(), lease)
	if err != nil {
		t.Fatalf("Execute() error = %v, want nil (handler errors flow via TaskResult)", err)
	}
	if !errors.Is(res.Error, want) {
		t.Fatalf("TaskResult.Error = %v, want %v", res.Error, want)
	}
}

// errHandler is a minimal ActionHandler that returns a fixed error, used to
// assert the runner's error-propagation contract.
type errHandler struct{ err error }

func (errHandler) Descriptor() types.Descriptor { return types.Descriptor{Type: "test.err"} }
func (h errHandler) Execute(context.Context, *types.Input) (*types.Output, error) {
	return nil, h.err
}

// compile-time guard: sentinelPool must satisfy types.ResourcePool.
var _ types.ResourcePool = sentinelPool{}
