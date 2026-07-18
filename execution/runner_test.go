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

// credRecordingHandler captures the credential map that the resolver returned
// for a given name. It records the value observed on the input it actually
// received, so it can assert the runner attached the resolver before dispatch.
type credRecordingHandler struct {
	mu     sync.Mutex
	seen   map[string]map[string]any
	seenOK map[string]bool
}

func newCredRecordingHandler() *credRecordingHandler {
	return &credRecordingHandler{
		seen:   make(map[string]map[string]any),
		seenOK: make(map[string]bool),
	}
}

func (h *credRecordingHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.cred-probe"}
}

func (h *credRecordingHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	v := input.Credential("db")
	h.seen[input.ExecutionID] = v
	h.seenOK[input.ExecutionID] = v != nil
	return &types.Output{Data: map[string]any{"ok": true}}, nil
}

func (h *credRecordingHandler) observed(id string) (map[string]any, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.seen[id], h.seenOK[id]
}

// TestRunner_WithCredentialResolverAttachesToInput pins the contract added in
// task-42: when a Runner is constructed via WithCredentialResolver(fn),
// Execute must call input.SetCredentialResolver(fn) so the handler observes
// the resolved credential map via input.Credential(name).
func TestRunner_WithCredentialResolverAttachesToInput(t *testing.T) {
	want := map[string]any{"dsn": "user:pass@tcp(db:3306)/xflow", "driver": "mysql"}
	resolver := func(name string) map[string]any {
		if name == "db" {
			return want
		}
		return nil
	}
	rec := newCredRecordingHandler()
	runner := NewRunner(singleHandlerRegistry{handler: rec}, WithCredentialResolver(resolver))

	lease := &engine.TaskLease{
		Task:     engine.Task{ExecutionID: "exec-cred", NodeName: "probe"},
		Input:    &types.Input{ExecutionID: "exec-cred", NodeName: "probe"},
		NodeType: "test.cred-probe",
	}
	if _, err := runner.Execute(context.Background(), lease); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	got, ok := rec.observed("exec-cred")
	if !ok {
		t.Fatal("handler observed nil credential, want the resolver value injected by the Runner")
	}
	if got["dsn"] != want["dsn"] {
		t.Fatalf("handler observed dsn = %v, want %v", got["dsn"], want["dsn"])
	}
}

// TestRunner_NilCredentialResolverIsNoOp pins the complementary contract: a
// Runner constructed WITHOUT WithCredentialResolver must leave
// input.Credential(name) returning nil (existing behavior). Guards against a
// future change that injects a nil resolver in a way that panics or surprises.
func TestRunner_NilCredentialResolverIsNoOp(t *testing.T) {
	rec := newCredRecordingHandler()
	runner := NewRunner(singleHandlerRegistry{handler: rec})

	lease := &engine.TaskLease{
		Task:     engine.Task{ExecutionID: "exec-nocred", NodeName: "probe"},
		Input:    &types.Input{ExecutionID: "exec-nocred", NodeName: "probe"},
		NodeType: "test.cred-probe",
	}
	if _, err := runner.Execute(context.Background(), lease); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	got, ok := rec.observed("exec-nocred")
	if ok {
		t.Fatalf("handler observed non-nil credential %v, want nil (no resolver injected)", got)
	}
}

// resuspendCredHandler is a SuspendingHandler that exercises the
// TaskTypeNodeResume resuspend path. OnResume returns Resuspend=true with
// non-nil Data, forcing executeSuspending to clone the input via
// cloneInputWithData. PrepareSuspend then records whether the cloned input
// retains the credential resolver.
type resuspendCredHandler struct {
	mu          sync.Mutex
	prepareSeen map[string]map[string]any
	prepareOK   map[string]bool
}

func newResuspendCredHandler() *resuspendCredHandler {
	return &resuspendCredHandler{
		prepareSeen: make(map[string]map[string]any),
		prepareOK:   make(map[string]bool),
	}
}

func (*resuspendCredHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.cred-resuspend"}
}

func (h *resuspendCredHandler) OnResume(_ context.Context, _ *types.Input, _ *types.SignalPayload) (*types.Output, error) {
	// Resuspend with non-nil Data so executeSuspending runs
	// cloneInputWithData(lease.Input, output.Data) before PrepareSuspend.
	return &types.Output{Data: map[string]any{"step": 2}, Resuspend: true}, nil
}

// Execute is unreachable on the NodeResume resuspend path but is required to
// satisfy types.ActionHandler.
func (h *resuspendCredHandler) Execute(context.Context, *types.Input) (*types.Output, error) {
	return nil, errors.New("resuspendCredHandler.Execute must not be called on the resume path")
}

func (h *resuspendCredHandler) PrepareSuspend(_ context.Context, input *types.Input) (*types.SuspendSpec, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	v := input.Credential("db")
	h.prepareSeen[input.ExecutionID] = v
	h.prepareOK[input.ExecutionID] = v != nil
	return &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"approval"}}, nil
}

func (h *resuspendCredHandler) prepareObserved(id string) (map[string]any, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.prepareSeen[id], h.prepareOK[id]
}

// TestRunner_CredentialResolverSurvivesCloneInputWithData pins the contract
// documented in task-41 design §7: the credential resolver applied to
// lease.Input survives the shallow clone performed by cloneInputWithData in
// the resuspend path, so PrepareSuspend receives an input whose
// Credential(name) still resolves. The unexported credential func field is
// copied by value (it is a pointer), so the cloned input retains the resolver.
func TestRunner_CredentialResolverSurvivesCloneInputWithData(t *testing.T) {
	want := map[string]any{"dsn": "user:pass@tcp(db:3306)/xflow", "driver": "mysql"}
	resolver := func(name string) map[string]any {
		if name == "db" {
			return want
		}
		return nil
	}
	rec := newResuspendCredHandler()
	runner := NewRunner(singleHandlerRegistry{handler: rec}, WithCredentialResolver(resolver))

	lease := &engine.TaskLease{
		Task: engine.Task{
			ExecutionID: "exec-resuspend",
			NodeName:    "probe",
			Type:        engine.TaskTypeNodeResume,
			Payload:     &types.SignalPayload{Name: "approval"},
		},
		Input:    &types.Input{ExecutionID: "exec-resuspend", NodeName: "probe"},
		NodeType: "test.cred-resuspend",
	}
	res, err := runner.Execute(context.Background(), lease)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if res.Suspend == nil {
		t.Fatal("Execute() Suspend = nil, want a suspend spec from PrepareSuspend on the resuspend path")
	}
	got, ok := rec.prepareObserved("exec-resuspend")
	if !ok {
		t.Fatal("PrepareSuspend observed nil credential on cloned input, want the resolver value")
	}
	if got["dsn"] != want["dsn"] {
		t.Fatalf("PrepareSuspend observed dsn = %v, want %v (resolver must survive cloneInputWithData)", got["dsn"], want["dsn"])
	}
}
