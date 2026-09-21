package xflow

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend"
	backendlocal "github.com/xbcio/xflow/backend/providers/local"
	enginecore "github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/objectstore"
	"github.com/xbcio/xflow/types"
)

func TestFAFExecutesAsyncWithoutDurableState(t *testing.T) {
	handler := &fafBlockingHandler{
		inputs:  make(chan *types.Input, 1),
		release: make(chan struct{}),
	}
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()
	defer close(handler.release)

	action := node.Define("test.faf.async", handler.Execute)
	wf := Workflow("faf-async").FAF()
	wf.Node("send", action.New(map[string]any{"channel": "alerts"})).Timeout(time.Second)
	workflowID, err := eng.AddWorkflow(context.Background(), wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}
	// FAF must not touch the core engine after registration. Keeping the state
	// reference lets this test still prove the synthetic execution was not
	// persisted while a nil core would immediately expose any accidental call.
	state := eng.eng.State()
	eng.eng = nil

	accepted := make(chan error, 1)
	go func() {
		accepted <- eng.FireAndForget(
			context.Background(),
			workflowID,
			map[string]any{"message": "hello"},
			WithRuntimeVars(map[string]any{"request_id": "req-1"}),
			WithTraceID("trace-1"),
			WithSpanID("span-1"),
		)
	}()

	select {
	case err := <-accepted:
		if err != nil {
			t.Fatalf("FireAndForget() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("FireAndForget() waited for the blocking handler")
	}

	select {
	case input := <-handler.inputs:
		if got := input.Data["message"]; got != "hello" {
			t.Fatalf("Input.Data[message] = %#v, want hello", got)
		}
		if got := input.Params["channel"]; got != "alerts" {
			t.Fatalf("Input.Params[channel] = %#v, want alerts", got)
		}
		if input.Runtime == nil || input.Runtime.Vars["request_id"] != "req-1" {
			t.Fatalf("Input.Runtime = %#v, want request runtime", input.Runtime)
		}
		if got := input.Vars["request_id"]; got != "req-1" {
			t.Fatalf("Input.Vars[request_id] = %#v, want req-1", got)
		}
		if input.TraceID != "trace-1" || input.SpanID != "span-1" {
			t.Fatalf("Input trace/span = %q/%q, want trace-1/span-1", input.TraceID, input.SpanID)
		}
		if input.WorkflowName != "faf-async" || input.WorkflowVersion != "v1" {
			t.Fatalf("Input workflow identity = %q/%q, want faf-async/v1", input.WorkflowName, input.WorkflowVersion)
		}
		if input.Timeout != time.Second {
			t.Fatalf("Input.Timeout = %s, want %s", input.Timeout, time.Second)
		}
		if input.ExecutionID == "" {
			t.Fatal("Input.ExecutionID is empty; expected an ephemeral runner identity")
		}

		ephemeralID := types.ExecutionID(input.ExecutionID)
		snapshot, err := state.GetExecution(context.Background(), ephemeralID)
		if err != nil {
			t.Fatalf("State().GetExecution(%q) error = %v", ephemeralID, err)
		}
		if snapshot != nil {
			t.Fatalf("FireAndForget persisted execution state: %#v", snapshot)
		}

		waitCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if _, err := eng.Wait(waitCtx, ephemeralID); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Wait() error = %v, want context deadline for non-persistent FAF execution", err)
		}
	case <-time.After(time.Second):
		t.Fatal("FAF handler did not execute")
	}
}

func TestFAFRejectsNormalInvokeAndNonFAF(t *testing.T) {
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	fafHandler := &fafNoopHandler{nodeType: "test.faf.invoke-rejection"}
	faf := Workflow("faf-invoke-rejection").FAF()
	faf.LocalNode("only", fafHandler)
	fafID, err := eng.AddWorkflow(context.Background(), faf)
	if err != nil {
		t.Fatalf("AddWorkflow(FAF) error = %v", err)
	}

	// The SDK guard must run before core invocation; nil makes an accidental
	// call to e.eng.Invoke fail immediately rather than returning the same core
	// sentinel by coincidence.
	eng.eng = nil
	if id, err := eng.Invoke(context.Background(), fafID, EntryNode("only"), nil); id != "" || !errors.Is(err, enginecore.ErrFAFRequiresDirectDispatch) {
		t.Fatalf("Invoke(FAF) = (%q, %v), want empty id and ErrFAFRequiresDirectDispatch", id, err)
	}
	if fafHandler.executed.Load() {
		t.Fatal("Invoke(FAF) executed the handler")
	}

	normalHandler := &fafNoopHandler{nodeType: "test.faf.non-faf"}
	normal := Workflow("not-faf")
	normal.LocalNode("only", normalHandler)
	normalID, err := eng.AddWorkflow(context.Background(), normal)
	if err != nil {
		t.Fatalf("AddWorkflow(non-FAF) error = %v", err)
	}
	if err := eng.FireAndForget(context.Background(), normalID, nil); !errors.Is(err, ErrFAFWorkflowRequired) {
		t.Fatalf("FireAndForget(non-FAF) error = %v, want ErrFAFWorkflowRequired", err)
	}
	if normalHandler.executed.Load() {
		t.Fatal("FireAndForget(non-FAF) executed the handler")
	}
}

func TestFAFRejectsNonLocalNonOwningAndSuspendingHandlers(t *testing.T) {
	t.Run("non-local", func(t *testing.T) {
		provider := backendlocal.New()
		eng, err := newFromConfig(&engineConfig{allowDirectHandlers: false}, provider)
		if err != nil {
			t.Fatalf("newFromConfig() error = %v", err)
		}
		defer eng.Stop()

		handler := &fafNoopHandler{nodeType: "test.faf.non-local"}
		action := node.Define(handler.nodeType, handler.Execute)
		wf := Workflow("faf-non-local").FAF()
		wf.Node("only", action.New(nil))
		workflowID, err := eng.AddWorkflow(context.Background(), wf)
		if err != nil {
			t.Fatalf("AddWorkflow() error = %v", err)
		}
		if err := eng.FireAndForget(context.Background(), workflowID, nil); !errors.Is(err, ErrFAFLocalEngineRequired) {
			t.Fatalf("FireAndForget() error = %v, want ErrFAFLocalEngineRequired", err)
		}
		if handler.executed.Load() {
			t.Fatal("non-local FireAndForget executed the handler")
		}
	})

	t.Run("non-owning", func(t *testing.T) {
		provider := backendlocal.New()
		owner, err := newFromConfig(&engineConfig{allowDirectHandlers: true}, provider)
		if err != nil {
			t.Fatalf("newFromConfig() error = %v", err)
		}
		defer owner.Stop()

		handler := &fafNoopHandler{nodeType: "test.faf.non-owning"}
		wf := Workflow("faf-non-owning").FAF()
		wf.LocalNode("only", handler)
		workflowID, err := owner.AddWorkflow(context.Background(), wf)
		if err != nil {
			t.Fatalf("AddWorkflow() error = %v", err)
		}
		facade := newNonOwningEngineFacade(owner.eng, provider)
		if err := facade.FireAndForget(context.Background(), workflowID, nil); !errors.Is(err, ErrFAFLocalEngineRequired) {
			t.Fatalf("FireAndForget() error = %v, want ErrFAFLocalEngineRequired", err)
		}
		if handler.executed.Load() {
			t.Fatal("non-owning FireAndForget executed the handler")
		}
	})

	t.Run("suspending handler", func(t *testing.T) {
		eng, err := NewLocal()
		if err != nil {
			t.Fatalf("NewLocal() error = %v", err)
		}
		defer eng.Stop()

		handler := &fafSuspendingHandler{}
		wf := Workflow("faf-suspending").FAF()
		wf.LocalNode("only", handler)
		workflowID, err := eng.AddWorkflow(context.Background(), wf)
		if err != nil {
			t.Fatalf("AddWorkflow() error = %v", err)
		}
		if err := eng.FireAndForget(context.Background(), workflowID, nil); !errors.Is(err, ErrFAFSuspendingHandlerUnsupported) {
			t.Fatalf("FireAndForget() error = %v, want ErrFAFSuspendingHandlerUnsupported", err)
		}
		if handler.executed.Load() {
			t.Fatal("FireAndForget called Execute on a suspending handler")
		}
	})
}

func TestFAFUsesOwnedLocalResourcePool(t *testing.T) {
	pool := stubPool{id: "faf-owned"}
	handler := &fafPoolHandler{pools: make(chan types.ResourcePool, 1)}
	eng, err := NewLocal(WithResourcePool(pool))
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	action := node.Define("test.faf.resource-pool", handler.Execute)
	wf := Workflow("faf-resource-pool").FAF()
	wf.Node("only", action.New(nil)).Timeout(time.Second)
	workflowID, err := eng.AddWorkflow(context.Background(), wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}
	if err := eng.FireAndForget(context.Background(), workflowID, nil); err != nil {
		t.Fatalf("FireAndForget() error = %v", err)
	}

	select {
	case got := <-handler.pools:
		if got != pool {
			t.Fatalf("FAF handler resource pool = %T(%[1]v), want owned pool %T(%[2]v)", got, pool)
		}
	case <-time.After(time.Second):
		t.Fatal("FAF handler did not observe the owned local resource pool")
	}
}

func TestFAFLogsPanickingHandler(t *testing.T) {
	logger := &fafLogger{errors: make(chan fafLogEntry, 1)}
	eng, err := NewLocal(WithLogger(logger))
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	handler := &fafPanicHandler{nodeType: "test.faf.panic"}
	action := node.Define(handler.nodeType, handler.Execute)
	wf := Workflow("faf-panic").FAF()
	wf.Node("only", action.New(nil)).Timeout(time.Second)
	workflowID, err := eng.AddWorkflow(context.Background(), wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}
	if err := eng.FireAndForget(context.Background(), workflowID, nil); err != nil {
		t.Fatalf("FireAndForget() error = %v", err)
	}

	select {
	case entry := <-logger.errors:
		if entry.msg != "fire-and-forget action failed" {
			t.Fatalf("logger message = %q, want async FAF failure message", entry.msg)
		}
		if got := entry.value("workflow_id"); got != string(workflowID) {
			t.Fatalf("logger workflow_id = %#v, want %q", got, workflowID)
		}
		if got, ok := entry.value("err").(error); !ok || got.Error() != "panic: handler panicked" {
			t.Fatalf("logger err = %#v, want contained handler panic", entry.value("err"))
		}
	case <-time.After(time.Second):
		t.Fatal("panicking FAF handler was not reported through the logger")
	}
}

func TestFAFLogsAsynchronousFailures(t *testing.T) {
	logger := &fafLogger{errors: make(chan fafLogEntry, 1)}
	eng, err := NewLocal(WithLogger(logger))
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	handler := &fafErrorHandler{nodeType: "test.faf.failure"}
	action := node.Define(handler.nodeType, handler.Execute)
	wf := Workflow("faf-failure").FAF()
	wf.Node("only", action.New(nil))
	workflowID, err := eng.AddWorkflow(context.Background(), wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}
	if err := eng.FireAndForget(context.Background(), workflowID, nil); err != nil {
		t.Fatalf("FireAndForget() error = %v", err)
	}

	select {
	case entry := <-logger.errors:
		if entry.msg != "fire-and-forget action failed" {
			t.Fatalf("logger message = %q, want async FAF failure message", entry.msg)
		}
		if got := entry.value("workflow_id"); got != string(workflowID) {
			t.Fatalf("logger workflow_id = %#v, want %q", got, workflowID)
		}
		if got, ok := entry.value("err").(error); !ok || got.Error() != "handler failed" {
			t.Fatalf("logger err = %#v, want handler failed", entry.value("err"))
		}
	case <-time.After(time.Second):
		t.Fatal("asynchronous FAF failure was not logged")
	}
}

func TestFAFPinsLocalHandlerToWorkflowIdentity(t *testing.T) {
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	first := &fafNamedHandler{name: "first", calls: make(chan string, 1)}
	second := &fafNamedHandler{name: "second", calls: make(chan string, 1)}
	firstWorkflow := Workflow("faf-handler-first").FAF()
	firstWorkflow.LocalNode("only", first)
	firstID, err := eng.AddWorkflow(context.Background(), firstWorkflow)
	if err != nil {
		t.Fatalf("AddWorkflow(first) error = %v", err)
	}
	// The second registration intentionally shadows the process-global LocalNode
	// entry for "only". FireAndForget(firstID) must still execute first, not the
	// most recently registered callback.
	secondWorkflow := Workflow("faf-handler-second").FAF()
	secondWorkflow.LocalNode("only", second)
	secondID, err := eng.AddWorkflow(context.Background(), secondWorkflow)
	if err != nil {
		t.Fatalf("AddWorkflow(second) error = %v", err)
	}

	for _, tc := range []struct {
		id    types.WorkflowID
		want  string
		calls <-chan string
	}{
		{id: firstID, want: "first", calls: first.calls},
		{id: secondID, want: "second", calls: second.calls},
	} {
		if err := eng.FireAndForget(context.Background(), tc.id, nil); err != nil {
			t.Fatalf("FireAndForget(%q) error = %v", tc.id, err)
		}
		select {
		case got := <-tc.calls:
			if got != tc.want {
				t.Fatalf("FireAndForget(%q) ran %q handler, want %q", tc.id, got, tc.want)
			}
		case <-time.After(time.Second):
			t.Fatalf("FireAndForget(%q) did not run its bound handler", tc.id)
		}
	}
}

func TestFAFRejectsWorkflowWithoutLocalHandlerBinding(t *testing.T) {
	provider := backendlocal.New()
	owner, err := newFromConfig(&engineConfig{allowDirectHandlers: true}, provider)
	if err != nil {
		t.Fatalf("newFromConfig(owner) error = %v", err)
	}
	defer owner.Stop()

	handler := &fafNoopHandler{nodeType: "test.faf.unbound"}
	wf := Workflow("faf-unbound").FAF()
	wf.LocalNode("only", handler)
	workflowID, err := owner.AddWorkflow(context.Background(), wf)
	if err != nil {
		t.Fatalf("owner.AddWorkflow() error = %v", err)
	}

	// Simulate a local process that can read a persisted FAF record but did not
	// register its callback. It must fail closed instead of resolving the shared
	// node-name registry, whose handler identity is not authoritative here.
	unbound := &Engine{
		registry:            provider.Registry(),
		workflowRegistry:    provider.WorkflowRegistry(),
		allowDirectHandlers: true,
		fafHandlers:         make(map[types.WorkflowID]fafHandlerBinding),
	}
	if err := unbound.FireAndForget(context.Background(), workflowID, nil); !errors.Is(err, ErrFAFHandlerBindingRequired) {
		t.Fatalf("unbound FireAndForget() error = %v, want ErrFAFHandlerBindingRequired", err)
	}
	if handler.executed.Load() {
		t.Fatal("unbound FireAndForget executed a shared-registry handler")
	}
}

func TestFAFSnapshotsNestedInputAndRuntimeValues(t *testing.T) {
	handler := &fafSnapshotHandler{
		started: make(chan struct{}),
		release: make(chan struct{}),
		seen:    make(chan fafSnapshot, 1),
	}
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	wf := Workflow("faf-input-snapshot").FAF()
	wf.LocalNode("only", handler)
	workflowID, err := eng.AddWorkflow(context.Background(), wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}

	inputNested := map[string]any{"value": "before"}
	inputSliceNested := map[string]any{"value": "before"}
	input := map[string]any{
		"nested": inputNested,
		"slice":  []any{inputSliceNested},
	}
	runtimeNested := map[string]any{"value": "before"}
	runtimeSliceNested := map[string]any{"value": "before"}
	runtimeVars := map[string]any{
		"nested": runtimeNested,
		"slice":  []any{runtimeSliceNested},
	}

	if err := eng.FireAndForget(context.Background(), workflowID, input, WithRuntimeVars(runtimeVars)); err != nil {
		t.Fatalf("FireAndForget() error = %v", err)
	}
	select {
	case <-handler.started:
	case <-time.After(time.Second):
		t.Fatal("FAF handler did not start")
	}

	// The handler is blocked until after these mutations, so this is both a
	// stable snapshot assertion and a race-free reproduction of post-acceptance
	// caller mutation.
	inputNested["value"] = "after"
	inputSliceNested["value"] = "after"
	runtimeNested["value"] = "after"
	runtimeSliceNested["value"] = "after"
	close(handler.release)

	select {
	case got := <-handler.seen:
		if got.dataNested != "before" || got.dataSliceNested != "before" {
			t.Fatalf("Input.Data observed nested values %+v, want all before", got)
		}
		if got.runtimeNested != "before" || got.runtimeSliceNested != "before" {
			t.Fatalf("Input.Runtime observed nested values %+v, want all before", got)
		}
		if got.varsNested != "before" || got.varsSliceNested != "before" {
			t.Fatalf("Input.Vars observed nested values %+v, want all before", got)
		}
	case <-time.After(time.Second):
		t.Fatal("FAF handler did not finish")
	}
}

func TestFAFScriptFileRejectedBeforeArtifactWrite(t *testing.T) {
	eng, recorder := newFAFArtifactPreflightEngine(t)
	wf := Workflow("faf-script-file").FAF()
	wf.Node("script", node.ScriptFile(writeFAFArtifactScript(t)).Language("js").Runtime("goja"))
	assertFAFArtifactPreflightRejected(t, eng, recorder, "faf-script-file", wf)
}

func TestFAFScriptArtifactRejectedBeforePersistence(t *testing.T) {
	eng, recorder := newFAFArtifactPreflightEngine(t)
	wf := Workflow("faf-script-artifact").FAF()
	wf.Node("script", node.ScriptArtifact("sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef").Language("js").Runtime("goja"))
	assertFAFArtifactPreflightRejected(t, eng, recorder, "faf-script-artifact", wf)
}

func TestFAFMapBodyScriptFileRejectedBeforePersistence(t *testing.T) {
	eng, recorder := newFAFArtifactPreflightEngine(t)
	body := Workflow("faf-map-body-script-file-body")
	body.Node("script", node.ScriptFile(writeFAFArtifactScript(t)).Language("js").Runtime("goja"))

	wf := Workflow("faf-map-body-script-file").FAF()
	wf.Node("items", node.Map("$input.items", 1)).Body(body)
	assertFAFArtifactPreflightRejected(t, eng, recorder, "faf-map-body-script-file", wf)
}

func TestFAFMapBodyScriptArtifactRejectedBeforePersistence(t *testing.T) {
	eng, recorder := newFAFArtifactPreflightEngine(t)
	body := Workflow("faf-map-body-script-artifact-body")
	body.Node("script", node.ScriptArtifact("sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef").Language("js").Runtime("goja"))

	wf := Workflow("faf-map-body-script-artifact").FAF()
	wf.Node("items", node.Map("$input.items", 1)).Body(body)
	assertFAFArtifactPreflightRejected(t, eng, recorder, "faf-map-body-script-artifact", wf)
}

func newFAFArtifactPreflightEngine(t *testing.T) (*Engine, *fafArtifactPreflightRecorder) {
	t.Helper()
	writes := &fafArtifactWriteRecorder{Store: objectstore.NewFSStore(t.TempDir())}
	artifacts := store.NewArtifactStore(writes, nil)
	eng, err := NewLocal(WithArtifactStore(artifacts))
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	t.Cleanup(eng.Stop)
	registrations := &fafWorkflowRegistrationRecorder{WorkflowRegistry: eng.workflowRegistry}
	eng.workflowRegistry = registrations
	return eng, &fafArtifactPreflightRecorder{writes: writes, registrations: registrations}
}

func writeFAFArtifactScript(t *testing.T) string {
	t.Helper()
	scriptPath := filepath.Join(t.TempDir(), "must-not-be-persisted.js")
	if err := os.WriteFile(scriptPath, []byte(`({ok: true})`), 0o600); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return scriptPath
}

func assertFAFArtifactPreflightRejected(t *testing.T, eng *Engine, recorder *fafArtifactPreflightRecorder, workflowName string, wf *WorkflowBuilder) {
	t.Helper()
	if _, err := eng.AddWorkflow(context.Background(), wf); !errors.Is(err, ErrFAFArtifactUnsupported) {
		t.Fatalf("AddWorkflow(FAF artifact-backed script) error = %v, want ErrFAFArtifactUnsupported", err)
	}
	if recorder.writes.puts != 0 {
		t.Fatalf("FAF artifact preflight wrote %d artifact-store objects", recorder.writes.puts)
	}
	if recorder.registrations.adds != 0 {
		t.Fatalf("FAF artifact preflight called workflow registration %d times", recorder.registrations.adds)
	}
	if _, err := eng.workflowRegistry.GetWorkflowByKey(context.Background(), "default/"+workflowName+"@v1"); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("GetWorkflowByKey() error = %v, want backend.ErrWorkflowNotFound", err)
	}
}

type fafArtifactWriteRecorder struct {
	objectstore.Store
	puts int
}

func (r *fafArtifactWriteRecorder) PutObject(ctx context.Context, key string, body io.Reader, size int64, opts objectstore.PutOptions) (*objectstore.Object, error) {
	r.puts++
	return r.Store.PutObject(ctx, key, body, size, opts)
}

type fafArtifactPreflightRecorder struct {
	writes        *fafArtifactWriteRecorder
	registrations *fafWorkflowRegistrationRecorder
}

type fafWorkflowRegistrationRecorder struct {
	backend.WorkflowRegistry
	adds int
}

func (r *fafWorkflowRegistrationRecorder) AddWorkflow(ctx context.Context, rec backend.WorkflowRecord) (backend.WorkflowRecord, error) {
	r.adds++
	return r.WorkflowRegistry.AddWorkflow(ctx, rec)
}

type fafNamedHandler struct {
	name  string
	calls chan string
}

func (*fafNamedHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.faf.identity", Kind: types.NodeKindAction}
}

func (h *fafNamedHandler) Execute(context.Context, *types.Input) (*types.Output, error) {
	h.calls <- h.name
	return &types.Output{}, nil
}

type fafSnapshot struct {
	dataNested         string
	dataSliceNested    string
	runtimeNested      string
	runtimeSliceNested string
	varsNested         string
	varsSliceNested    string
}

type fafSnapshotHandler struct {
	started chan struct{}
	release chan struct{}
	seen    chan fafSnapshot
}

func (*fafSnapshotHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.faf.snapshot", Kind: types.NodeKindAction}
}

func (h *fafSnapshotHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	close(h.started)
	<-h.release
	snapshot := fafSnapshot{
		dataNested:         input.Data["nested"].(map[string]any)["value"].(string),
		dataSliceNested:    input.Data["slice"].([]any)[0].(map[string]any)["value"].(string),
		runtimeNested:      input.Runtime.Vars["nested"].(map[string]any)["value"].(string),
		runtimeSliceNested: input.Runtime.Vars["slice"].([]any)[0].(map[string]any)["value"].(string),
		varsNested:         input.Vars["nested"].(map[string]any)["value"].(string),
		varsSliceNested:    input.Vars["slice"].([]any)[0].(map[string]any)["value"].(string),
	}
	h.seen <- snapshot
	return &types.Output{}, nil
}

type fafBlockingHandler struct {
	inputs  chan *types.Input
	release chan struct{}
}

func (*fafBlockingHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.faf.async", Kind: types.NodeKindAction}
}

func (h *fafBlockingHandler) Execute(ctx context.Context, input *types.Input) (*types.Output, error) {
	h.inputs <- input
	select {
	case <-h.release:
		return &types.Output{Data: map[string]any{"accepted": true}}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type fafNoopHandler struct {
	nodeType string
	executed atomic.Bool
}

func (h *fafNoopHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: h.nodeType, Kind: types.NodeKindAction}
}

func (h *fafNoopHandler) Execute(context.Context, *types.Input) (*types.Output, error) {
	h.executed.Store(true)
	return &types.Output{}, nil
}

type fafSuspendingHandler struct {
	executed atomic.Bool
}

func (*fafSuspendingHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.faf.suspending", Kind: types.NodeKindAction}
}

func (h *fafSuspendingHandler) Execute(context.Context, *types.Input) (*types.Output, error) {
	h.executed.Store(true)
	return nil, errors.New("Execute must not run for FAF")
}

func (*fafSuspendingHandler) PrepareSuspend(context.Context, *types.Input) (*types.SuspendSpec, error) {
	return &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"resume"}}, nil
}

func (*fafSuspendingHandler) OnResume(context.Context, *types.Input, *types.SignalPayload) (*types.Output, error) {
	return nil, errors.New("OnResume must not run for FAF")
}

type fafPoolHandler struct {
	pools chan types.ResourcePool
}

func (*fafPoolHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.faf.resource-pool", Kind: types.NodeKindAction}
}

func (h *fafPoolHandler) Execute(ctx context.Context, _ *types.Input) (*types.Output, error) {
	h.pools <- types.ResourcePoolFromContext(ctx)
	return &types.Output{}, nil
}

type fafPanicHandler struct {
	nodeType string
}

func (h *fafPanicHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: h.nodeType, Kind: types.NodeKindAction}
}

func (*fafPanicHandler) Execute(context.Context, *types.Input) (*types.Output, error) {
	panic("handler panicked")
}

type fafErrorHandler struct {
	nodeType string
}

func (h *fafErrorHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: h.nodeType, Kind: types.NodeKindAction}
}

func (*fafErrorHandler) Execute(context.Context, *types.Input) (*types.Output, error) {
	return nil, errors.New("handler failed")
}

type fafLogEntry struct {
	msg  string
	args []any
}

func (e fafLogEntry) value(key string) any {
	for i := 0; i+1 < len(e.args); i += 2 {
		if e.args[i] == key {
			return e.args[i+1]
		}
	}
	return nil
}

type fafLogger struct {
	errors chan fafLogEntry
}

func (*fafLogger) Debug(string, ...any)  {}
func (*fafLogger) Debugf(string, ...any) {}
func (*fafLogger) Info(string, ...any)   {}
func (*fafLogger) Infof(string, ...any)  {}
func (*fafLogger) Warn(string, ...any)   {}
func (*fafLogger) Warnf(string, ...any)  {}
func (*fafLogger) Errorf(string, ...any) {}
func (*fafLogger) Panic(string, ...any)  {}
func (*fafLogger) Panicf(string, ...any) {}
func (l *fafLogger) Error(msg string, args ...any) {
	select {
	case l.errors <- fafLogEntry{msg: msg, args: args}:
	default:
	}
}
