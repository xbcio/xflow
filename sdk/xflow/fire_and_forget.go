package xflow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	enginecore "github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

var (
	// ErrFAFWorkflowRequired is returned when FireAndForget is called for a
	// workflow that did not explicitly opt into zero-persistence execution.
	ErrFAFWorkflowRequired = errors.New("xflow: FireAndForget requires a FAF workflow")

	// ErrFAFLocalEngineRequired is returned when FireAndForget is called on a
	// cluster engine or a non-owning Server facade. FAF handlers can execute only
	// in the owned local process that registered them.
	ErrFAFLocalEngineRequired = errors.New("xflow: FireAndForget requires an owned local engine")

	// ErrFAFActionRequired is returned when the compiled FAF graph is not the
	// single action shape that the direct runner can execute.
	ErrFAFActionRequired = errors.New("xflow: FireAndForget requires exactly one action node")

	// ErrFAFSuspendingHandlerUnsupported is returned before dispatch when the
	// sole FAF action can suspend. FAF has no persisted identity to resume.
	ErrFAFSuspendingHandlerUnsupported = errors.New("xflow: FireAndForget does not support suspending handlers")

	// ErrFAFHandlerBindingRequired is returned when the requested FAF workflow
	// was not registered by this owned local Engine, so no exact local callback
	// is available to run safely.
	ErrFAFHandlerBindingRequired = errors.New("xflow: FireAndForget requires a handler bound by this local engine")

	// ErrFAFPayloadSnapshotUnsupported is returned before asynchronous work starts
	// when a caller-owned payload contains a value that FAF cannot isolate.
	ErrFAFPayloadSnapshotUnsupported = errors.New("xflow: FireAndForget payload cannot be safely snapshotted")
)

// fafHandlerBinding is the immutable local callback identity captured after a
// FAF workflow is fully registered. The graph and node identity guard against a
// stale binding being reused if an external registry later replaces its record.
type fafHandlerBinding struct {
	handler     types.ActionHandler
	graphHash   string
	nodeName    string
	nodeType    string
	nodeVersion int
}

// FireAndForget starts an eligible FAF workflow directly in this process.
//
// Unlike Invoke, it creates no execution, node, output, queue, outbox, or lease
// state. It uses the exact action handler pinned when this local Engine
// registered the workflow, then runs that handler asynchronously with an
// ephemeral lease. Accepted work is best-effort:
// callers receive no execution ID or result, and asynchronous failures are only
// reported through the Engine logger.
func (e *Engine) FireAndForget(ctx context.Context, workflowID types.WorkflowID, input map[string]any, opts ...InvokeOption) error {
	if e == nil || e.nonOwning || !e.allowDirectHandlers || e.workflowRegistry == nil || e.registry == nil {
		return ErrFAFLocalEngineRequired
	}
	if ctx == nil {
		return errors.New("xflow: FireAndForget requires a non-nil context")
	}

	rec, err := e.workflowRegistry.GetWorkflow(ctx, workflowID)
	if err != nil {
		return err
	}
	if rec.Graph == nil {
		return fmt.Errorf("xflow: workflow %q has no compiled graph", workflowID)
	}
	if !rec.Graph.FAF() {
		return ErrFAFWorkflowRequired
	}
	if rec.Graph.NodeCount() != 1 {
		return fmt.Errorf("%w: workflow %q has %d nodes", ErrFAFActionRequired, workflowID, rec.Graph.NodeCount())
	}

	node := rec.Graph.NodeAt(0)
	if node.Kind != "" && node.Kind != types.NodeKindAction {
		return fmt.Errorf("%w: node %q has kind %q", ErrFAFActionRequired, node.Name, node.Kind)
	}

	cfg := &invokeConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.execTTL > 0 {
		ctx = enginecore.WithExecutionTTL(ctx, cfg.execTTL)
	}
	if cfg.traceID != "" {
		ctx = enginecore.WithTraceID(ctx, cfg.traceID)
	}
	if cfg.spanID != "" {
		ctx = enginecore.WithSpanID(ctx, cfg.spanID)
	}
	ctx = enginecore.WithWorkflowDef(ctx, rec.Definition)

	handler, err := e.fafHandlerFor(workflowID, rec.Graph.Hash(), node.Name, node.Type, node.Version)
	if err != nil {
		return err
	}
	if _, ok := handler.(types.SuspendingHandler); ok {
		return fmt.Errorf("%w: node %q", ErrFAFSuspendingHandlerUnsupported, node.Name)
	}

	snapshot := newFAFPayloadSnapshot()
	runtimeSource := cfg.fafRuntime
	if runtimeSource == nil {
		runtimeSource = cfg.runtime
	}
	runtime, err := snapshot.runtime(runtimeSource, "runtime")
	if err != nil {
		return err
	}
	data, err := snapshot.mapValue(input, "input")
	if err != nil {
		return err
	}

	id := enginecore.NewExecutionID()
	traceID, _ := enginecore.TraceIDFromContext(ctx)
	spanID, _ := enginecore.SpanIDFromContext(ctx)
	timeout := fafNodeTimeout(node.Timeout)
	issuedAt := time.Now().UTC()
	lease := &enginecore.TaskLease{
		Task: enginecore.Task{
			ExecutionID:  id,
			NodeName:     node.Name,
			NodeIdx:      0,
			Type:         enginecore.TaskTypeNodeExec,
			ActivationID: 1,
		},
		Input: &types.Input{
			Params:          node.Parameters,
			Data:            data,
			Vars:            fafMergeVars(rec.Graph.Vars(), runtime),
			Config:          rec.Graph.Config(),
			Runtime:         runtime,
			ExecutionID:     string(id),
			NodeName:        node.Name,
			TraceID:         traceID,
			SpanID:          spanID,
			WorkflowName:    rec.Graph.Name(),
			WorkflowVersion: rec.Graph.WorkflowVersion(),
			Timeout:         timeout,
		},
		NodeType:    node.Type,
		NodeVersion: node.Version,
		IssuedAt:    issuedAt,
		Namespace:   fafNamespace(rec.Namespace),
	}
	if timeout > 0 {
		lease.ExecutionDeadline = issuedAt.Add(timeout)
	}

	runnerOpts := make([]execution.RunnerOption, 0, 2)
	if e.resourcePool != nil {
		runnerOpts = append(runnerOpts, execution.WithResourcePool(e.resourcePool))
	}
	if resolver := artifactCodeResolverFor(e.artifactStore); resolver != nil {
		runnerOpts = append(runnerOpts, execution.WithArtifactCodeResolver(resolver))
	}
	// The local registration binding was checked before accepting the invocation;
	// a later shared-registry change cannot turn accepted work into another
	// workflow's action.
	runner := execution.NewRunner(fafHandlerRegistry{handler: fafPanicSafeHandler{handler: handler}}, runnerOpts...)

	go e.runFireAndForget(ctx, runner, lease, workflowID)
	return nil
}

func (e *Engine) fafHandlerFor(workflowID types.WorkflowID, graphHash string, nodeName, nodeType string, nodeVersion int) (types.ActionHandler, error) {
	e.mu.Lock()
	binding, ok := e.fafHandlers[workflowID]
	e.mu.Unlock()
	if !ok || binding.handler == nil {
		return nil, fmt.Errorf("%w: workflow %q", ErrFAFHandlerBindingRequired, workflowID)
	}
	if binding.graphHash != graphHash ||
		binding.nodeName != nodeName ||
		binding.nodeType != nodeType ||
		binding.nodeVersion != nodeVersion {
		return nil, fmt.Errorf("%w: workflow %q was replaced or no longer matches its local registration", ErrFAFHandlerBindingRequired, workflowID)
	}
	return binding.handler, nil
}

func fafNodeTimeout(timeout time.Duration) time.Duration {
	switch {
	case timeout < 0:
		return 0
	case timeout == 0:
		return enginecore.DefaultNodeTimeout
	default:
		return timeout
	}
}

func fafMergeVars(static map[string]any, runtime *types.Runtime) map[string]any {
	if static == nil && (runtime == nil || runtime.Vars == nil) {
		return nil
	}
	vars := cloneMap(static)
	if vars == nil {
		vars = make(map[string]any, len(runtime.Vars))
	}
	if runtime != nil {
		for key, value := range runtime.Vars {
			vars[key] = value
		}
	}
	return vars
}

func fafNamespace(value string) namespace.Namespace {
	if value == "" {
		return namespace.Default
	}
	return namespace.Namespace(value)
}

func (e *Engine) runFireAndForget(ctx context.Context, runner *execution.Runner, lease *enginecore.TaskLease, workflowID types.WorkflowID) {
	runCtx := context.WithoutCancel(ctx)
	if deadline, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithDeadline(runCtx, deadline)
		defer cancel()
	}

	defer func() {
		if recovered := recover(); recovered != nil {
			e.logFireAndForgetFailure(workflowID, lease, fmt.Errorf("panic: %v", recovered))
		}
	}()

	result, err := runner.Execute(runCtx, lease)
	if err != nil {
		e.logFireAndForgetFailure(workflowID, lease, err)
		return
	}
	if result.Error != nil {
		e.logFireAndForgetFailure(workflowID, lease, result.Error)
		return
	}
	if result.Output != nil && result.Output.Error != nil {
		e.logFireAndForgetFailure(workflowID, lease, fmt.Errorf("%s", result.Output.Error.Message))
	}
}

func (e *Engine) logFireAndForgetFailure(workflowID types.WorkflowID, lease *enginecore.TaskLease, err error) {
	if e.logger == nil {
		return
	}
	e.logger.Error("fire-and-forget action failed",
		"workflow_id", string(workflowID),
		"node_name", lease.Task.NodeName,
		"node_type", lease.NodeType,
		"err", err,
	)
}

// fafPanicSafeHandler contains panics at the exact Execute call boundary.
// execution.Runner invokes timed handlers in a child goroutine, so recovering
// only around Runner.Execute would leave handler panics process-fatal.
type fafPanicSafeHandler struct {
	handler types.ActionHandler
}

func (h fafPanicSafeHandler) Descriptor() types.Descriptor {
	return h.handler.Descriptor()
}

func (h fafPanicSafeHandler) Execute(ctx context.Context, input *types.Input) (output *types.Output, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic: %v", recovered)
		}
	}()
	return h.handler.Execute(ctx, input)
}

type fafHandlerRegistry struct {
	handler types.ActionHandler
}

func (r fafHandlerRegistry) Get(types.ExecutionID, string, string, int) (types.ActionHandler, error) {
	return r.handler, nil
}

// fafPayloadSnapshot isolates mutable caller-owned data before FAF starts its
// asynchronous runner. It intentionally accepts only values it can reproduce
// without retaining a mutable reference to the caller.
type fafPayloadSnapshot struct {
	seen map[fafSnapshotVisit]reflect.Value
}

type fafSnapshotVisit struct {
	typ reflect.Type
	ptr uintptr
	len int
	cap int
}

func newFAFPayloadSnapshot() *fafPayloadSnapshot {
	return &fafPayloadSnapshot{seen: make(map[fafSnapshotVisit]reflect.Value)}
}

func (s *fafPayloadSnapshot) runtime(runtime *types.Runtime, path string) (*types.Runtime, error) {
	if runtime == nil {
		return nil, nil
	}
	vars, err := s.mapValue(runtime.Vars, path+".vars")
	if err != nil {
		return nil, err
	}
	return &types.Runtime{Vars: vars}, nil
}

func (s *fafPayloadSnapshot) mapValue(value map[string]any, path string) (map[string]any, error) {
	if value == nil {
		return nil, nil
	}
	cloned, err := s.value(reflect.ValueOf(value), path)
	if err != nil {
		return nil, err
	}
	result, ok := cloned.Interface().(map[string]any)
	if !ok {
		return nil, fafSnapshotUnsupported(path, "produced %s instead of map[string]any", cloned.Type())
	}
	return result, nil
}

func (s *fafPayloadSnapshot) value(value reflect.Value, path string) (reflect.Value, error) {
	if !value.IsValid() {
		return value, nil
	}

	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		return s.value(value.Elem(), path)
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		visit := fafSnapshotVisit{typ: value.Type(), ptr: value.Pointer()}
		if prior, ok := s.seen[visit]; ok {
			return prior, nil
		}
		cloned := reflect.MakeMapWithSize(value.Type(), value.Len())
		s.seen[visit] = cloned
		iter := value.MapRange()
		for iter.Next() {
			key := iter.Key()
			if err := fafSnapshotImmutable(key, path+"[key]"); err != nil {
				return reflect.Value{}, err
			}
			copied, err := s.value(iter.Value(), path+"[value]")
			if err != nil {
				return reflect.Value{}, err
			}
			copied, err = fafSnapshotAssignable(copied, value.Type().Elem(), path+"[value]")
			if err != nil {
				return reflect.Value{}, err
			}
			cloned.SetMapIndex(key, copied)
		}
		return cloned, nil
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		visit := fafSnapshotVisit{
			typ: value.Type(),
			ptr: value.Pointer(),
			len: value.Len(),
			cap: value.Cap(),
		}
		if visit.ptr != 0 {
			if prior, ok := s.seen[visit]; ok {
				return prior, nil
			}
		}
		cloned := reflect.MakeSlice(value.Type(), value.Len(), value.Cap())
		if visit.ptr != 0 {
			s.seen[visit] = cloned
		}
		// A slice can be resliced through its capacity, so copy all accessible
		// elements rather than only its current length. Including len and cap in
		// the identity lets overlapping views retain their distinct shapes.
		source := value.Slice(0, value.Cap())
		target := cloned.Slice(0, cloned.Cap())
		for i := 0; i < source.Len(); i++ {
			copied, err := s.value(source.Index(i), fmt.Sprintf("%s[%d]", path, i))
			if err != nil {
				return reflect.Value{}, err
			}
			copied, err = fafSnapshotAssignable(copied, value.Type().Elem(), fmt.Sprintf("%s[%d]", path, i))
			if err != nil {
				return reflect.Value{}, err
			}
			target.Index(i).Set(copied)
		}
		return cloned, nil
	case reflect.Array:
		cloned := reflect.New(value.Type()).Elem()
		for i := 0; i < value.Len(); i++ {
			copied, err := s.value(value.Index(i), fmt.Sprintf("%s[%d]", path, i))
			if err != nil {
				return reflect.Value{}, err
			}
			copied, err = fafSnapshotAssignable(copied, value.Type().Elem(), fmt.Sprintf("%s[%d]", path, i))
			if err != nil {
				return reflect.Value{}, err
			}
			cloned.Index(i).Set(copied)
		}
		return cloned, nil
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		visit := fafSnapshotVisit{typ: value.Type(), ptr: value.Pointer()}
		if prior, ok := s.seen[visit]; ok {
			return prior, nil
		}
		cloned := reflect.New(value.Type().Elem())
		s.seen[visit] = cloned
		copied, err := s.value(value.Elem(), path+".*")
		if err != nil {
			return reflect.Value{}, err
		}
		copied, err = fafSnapshotAssignable(copied, value.Type().Elem(), path+".*")
		if err != nil {
			return reflect.Value{}, err
		}
		cloned.Elem().Set(copied)
		return cloned, nil
	case reflect.Struct:
		cloned := reflect.New(value.Type()).Elem()
		for i := 0; i < value.NumField(); i++ {
			field := value.Type().Field(i)
			if !field.IsExported() {
				if err := fafSnapshotImmutable(value.Field(i), path+"."+field.Name); err != nil {
					return reflect.Value{}, err
				}
			}
		}
		// A whole-value copy safely preserves verified immutable private fields;
		// mutable exported fields are replaced below with their deep snapshots.
		cloned.Set(value)
		for i := 0; i < value.NumField(); i++ {
			field := value.Type().Field(i)
			if !field.IsExported() {
				continue
			}
			copied, err := s.value(value.Field(i), path+"."+field.Name)
			if err != nil {
				return reflect.Value{}, err
			}
			copied, err = fafSnapshotAssignable(copied, field.Type, path+"."+field.Name)
			if err != nil {
				return reflect.Value{}, err
			}
			cloned.Field(i).Set(copied)
		}
		return cloned, nil
	case reflect.Bool, reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return value, nil
	case reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return reflect.Value{}, fafSnapshotUnsupported(path, "contains unsupported %s", value.Type())
	default:
		return reflect.Value{}, fafSnapshotUnsupported(path, "contains unsupported %s", value.Type())
	}
}

func fafSnapshotAssignable(value reflect.Value, typ reflect.Type, path string) (reflect.Value, error) {
	if !value.IsValid() {
		return reflect.Value{}, fafSnapshotUnsupported(path, "contains an invalid value")
	}
	if value.Type().AssignableTo(typ) {
		return value, nil
	}
	return reflect.Value{}, fafSnapshotUnsupported(path, "cannot assign %s to %s", value.Type(), typ)
}

// fafSnapshotImmutable verifies that a field retained by value cannot conceal
// a mutable caller-owned reference. It is also used for map keys, whose identity
// must remain stable instead of being cloned into a different key.
func fafSnapshotImmutable(value reflect.Value, path string) error {
	if !value.IsValid() {
		return nil
	}
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return nil
		}
		return fafSnapshotImmutable(value.Elem(), path)
	case reflect.Array:
		for i := 0; i < value.Len(); i++ {
			if err := fafSnapshotImmutable(value.Index(i), fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
		return nil
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			field := value.Type().Field(i)
			if err := fafSnapshotImmutable(value.Field(i), path+"."+field.Name); err != nil {
				return err
			}
		}
		return nil
	case reflect.Bool, reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return nil
	default:
		return fafSnapshotUnsupported(path, "contains mutable or unsupported %s", value.Type())
	}
}

func fafSnapshotUnsupported(path, format string, args ...any) error {
	return fmt.Errorf("%w: %s %s", ErrFAFPayloadSnapshotUnsupported, path, fmt.Sprintf(format, args...))
}
