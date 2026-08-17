package apiserver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// workflowControlModule mounts the workflow/control HTTP API (submit, invoke,
// inspect, signal, revoke-signal, cancel, wait). Stage 3 migrated these routes
// out of control.Server so the control plane serves only the runner protocol;
// the control API now lives behind the apiserver module boundary. The module
// delegates to control.EngineFacade (the *engine.Engine behind the
// ControlPlane) for every engine call.
type workflowControlModule struct {
	authzHolder
	cp   *control.ControlPlane
	eng  control.EngineFacade
	auth WorkflowAuthenticator
	log  engine.Logger
	// tracer instruments submit/invoke so their span context is persisted on
	// the execution snapshot (via engine.WithTraceCarrier) and later inherited
	// by the asynchronous dispatch span — closing the submit→dispatch trace
	// causality gap. NoopTracer when tracing is disabled.
	tracer tracing.Tracer
}

func newWorkflowControlModule(cp *control.ControlPlane, auth WorkflowAuthenticator, log engine.Logger, tracer tracing.Tracer) *workflowControlModule {
	if tracer == nil {
		tracer = tracing.NoopTracer{}
	}
	return &workflowControlModule{cp: cp, eng: cp.Engine(), auth: auth, log: log, tracer: tracer}
}

func (m *workflowControlModule) Name() string { return "workflow-control" }

func (m *workflowControlModule) RegisterHTTP(mux *http.ServeMux) {
	// B3 authz path: when a PrincipalAuthenticator is configured, the module
	// enforces resource/operation-level authorization with append-only audit
	// before each handler runs. Without it, it falls back to the legacy
	// bearer-only WorkflowAuthenticator (fail-closed if RequireWorkflowAuth).
	if m.principalAuth != nil {
		m.registerAuthzRoutes(mux)
		return
	}
	auth := m.auth
	if auth == nil {
		auth = DisabledWorkflowAuth{}
	}
	wrap := func(op string, h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if err := auth.AuthenticateRequest(r); err != nil {
				if m.log != nil {
					m.log.Error("workflow_api_auth_denied",
						"op", op, "remote_addr", r.RemoteAddr, "err", err)
				}
				writeError(w, http.StatusUnauthorized, "unauthorized")
				return
			}
			h(w, r)
		}
	}
	// Routes are registered as Go 1.22 mux patterns (spec §1.3): every
	// execution sub-shape is its own method-qualified pattern, so the mux —
	// not a hand-rolled TrimPrefix parser — selects the handler. The
	// per-execution handlers read {id} via r.PathValue.
	mux.HandleFunc("POST "+PathWorkflows, wrap("submit_workflow", m.handleSubmitWorkflow))
	mux.HandleFunc("/v1/workflows/invoke", wrap("invoke_workflow", m.handleInvoke))
	// Registration is a separate, explicit step from submit-and-execute: it
	// persists the compiled workflow graph so the control plane can resolve it on
	// seed. /v1/workflows/register (exact) is the POST target; the
	// /v1/workflows/register/ subtree carries the DELETE {id} path.
	mux.HandleFunc("/v1/workflows/register", wrap("register_workflow", m.handleRegisterWorkflow))
	mux.HandleFunc("/v1/workflows/register/", wrap("deregister_workflow", m.handleDeregisterWorkflow))
	// POST /v1/executions is the entry-seed endpoint (runner protocol face,
	// spec §0.1); the per-execution GET/signal/cancel/wait surface is the
	// method-qualified patterns below. ServeMux treats exact and {id} patterns
	// as distinct from the /v1/executions/{id}/ 404 catch.
	mux.HandleFunc("POST "+PathExecutions, wrap("seed_execution", m.handleSeedExecution))
	mux.HandleFunc("GET "+PathExecutionByID, wrap("execution.read", m.handleInspectByID))
	mux.HandleFunc("GET "+PathExecutionWait, wrap("execution.read", m.handleWaitByID))
	mux.HandleFunc("POST "+PathExecutionSignals, wrap("execution.signal", m.handleSignalByID))
	mux.HandleFunc("POST "+PathExecutionCancel, wrap("execution.cancel", m.handleCancelByID))
	mux.HandleFunc("POST /v1/executions/{id}/revoke-signal", wrap("execution.revoke", m.handleRevokeSignalByID))
	// 404 catch: a path-only subtree that refuses any unrecognized execution
	// verb with 404. Under Go 1.22 ServeMux a method-qualified pattern answers
	// a method mismatch (e.g. POST /wait, GET /cancel) with 405, which leaks
	// that the route exists; this catch restores the old default-branch 404 so
	// the authorization boundary does not leak existence. It does no parsing —
	// the method-qualified patterns above are more specific and win for valid
	// shapes, so the catch only fires for shapes no handler should serve.
	//
	// Inherited policy: registering this catch makes 404-on-method-mismatch the
	// policy for the ENTIRE /v1/executions/{id}/ subtree, including routes
	// later tasks add under it (e.g. DELETE /v1/executions/{id}/signals/{name}
	// per spec §7). A wrong-method request to any execution sub-route lands here
	// and gets 404, not 405 — a behavior choice that must be revisited only by a
	// task that owns the API-SPECIFICATION.md §4.2 status-code table (which
	// currently has no 405 row). The /v1/management/* subtree DELIBERATELY
	// differs: its handlers return 405 via requireMethod, as they did before
	// this task. Anyone adding a route under /v1/executions/{id}/ inherits the
	// 404 policy without opting in; do not add a method-qualified pattern there
	// expecting 405 for a mismatch.
	mux.HandleFunc("/v1/executions/{id}/", wrap("execution", m.handleExecutionNotFound))
}

// registerAuthzRoutes mounts the control routes behind the B3 authz wrapper:
// authenticate → resolve operation/resource → authorize (default-deny) → audit
// admission → handler → audit outcome. Mutations fail-closed if the admission
// audit cannot be persisted.
//
// Each execution sub-shape is its own method-qualified mux pattern bound to its
// stable operation (spec §1.3), so the operation is known from the pattern
// itself rather than a hand-rolled resolver. An unknown shape falls to the
// /v1/executions/{id}/ 404 catch below, which denies with 404 "route not
// found" BEFORE authz runs — no existence leak, no audit row for a
// non-existent operation (mirrors the old resolveExecutionRoute ok=false path):
//   - GET  /v1/executions/{id}            → execution.read      (non-mutation)
//   - GET  /v1/executions/{id}/wait       → execution.read      (non-mutation)
//   - POST /v1/executions/{id}/signals    → execution.signal    (mutation)
//   - POST /v1/executions/{id}/revoke-signal → execution.revoke (mutation)
//   - POST /v1/executions/{id}/cancel     → execution.cancel    (mutation)
func (m *workflowControlModule) registerAuthzRoutes(mux *http.ServeMux) {
	authz := m.authzWrap
	mux.HandleFunc("POST "+PathWorkflows, authz(OpWorkflowCreate, true, m.handleSubmitWorkflow, newExecutionIDResolver()))
	mux.HandleFunc("/v1/workflows/invoke", authz(OpWorkflowInvoke, true, m.handleInvoke, newExecutionIDResolver()))
	// Register/deregister persist and remove the compiled workflow graph. Both
	// are mutations under the workflow scope. The authz wrapper injects the
	// principal's namespace into the request context; the handlers resolve it via
	// namespace.FromContext — never from the client body.
	mux.HandleFunc("/v1/workflows/register", authz(OpWorkflowRegister, true, m.handleRegisterWorkflow, nil))
	mux.HandleFunc("/v1/workflows/register/", authz(OpWorkflowRegister, true, m.handleDeregisterWorkflow, nil))
	// Entry-seed endpoint (exact path, POST only). The authz wrapper injects
	// the principal's namespace into the request context; handleSeedExecution
	// reads it via namespace.FromContext — never from the client body.
	mux.HandleFunc("POST "+PathExecutions, authz(OpExecutionSeed, true, m.handleSeedExecution, newExecutionIDResolver()))
	mux.HandleFunc("GET "+PathExecutionByID, authz(OpExecutionRead, false, m.handleInspectByID, execIDResolver("")))
	mux.HandleFunc("GET "+PathExecutionWait, authz(OpExecutionRead, false, m.handleWaitByID, execIDResolver("wait")))
	mux.HandleFunc("POST "+PathExecutionSignals, authz(OpExecutionSignal, true, m.handleSignalByID, execIDResolver("signals")))
	mux.HandleFunc("POST /v1/executions/{id}/revoke-signal", authz(OpExecutionRevoke, true, m.handleRevokeSignalByID, execIDResolver("revoke-signal")))
	mux.HandleFunc("POST "+PathExecutionCancel, authz(OpExecutionCancel, true, m.handleCancelByID, execIDResolver("cancel")))
	// 404 catch for unrecognized execution verbs. Unwrapped (no authz): the old
	// resolver's ok=false path returned 404 before authz ran and wrote no audit
	// row, and the catch preserves that. A method-qualified pattern alone would
	// 405 a method mismatch (existence leak); the path-only catch wins for any
	// {id}/verb shape no method-qualified pattern serves and returns 404. This
	// is the authz-mode twin of the bare-mode catch in RegisterHTTP; the
	// inherited 404-on-method-mismatch policy documented there applies here too.
	mux.HandleFunc("/v1/executions/{id}/", m.handleExecutionNotFound)
}

// execIDResolver builds the resource resolver for an execution sub-route: it
// reads the {id} the mux matched and returns the execution-scoped resource
// string the audit row carries (suffix is the sub-route verb, "" for inspect).
// ResourceNamespace is left empty — the authoritative IDOR defense is the
// namespace-scoped store read (the authz wrapper injects the principal's
// namespace so a cross-namespace execID resolves to not-found → 404).
func execIDResolver(suffix string) func(*http.Request) (string, string, string, string) {
	return func(r *http.Request) (string, string, string, string) {
		id := r.PathValue("id")
		resource := "execution/" + id
		if suffix != "" {
			resource += "/" + suffix
		}
		return resource, "", id, ""
	}
}

// handleExecutionNotFound is the 404 catch for the /v1/executions/{id}/
// subtree: any execution sub-shape no method-qualified pattern serves lands
// here and is refused with 404 "route not found" — no existence leak. It
// replaces the default branch of the old hand-rolled handleExecution parser.
func (m *workflowControlModule) handleExecutionNotFound(w http.ResponseWriter, r *http.Request) {
	writeError(w, http.StatusNotFound, "route not found")
}

// handleInspectByID / handleWaitByID / handleSignalByID / handleCancelByID /
// handleRevokeSignalByID are mux-pattern adapters: they pull the {id} path
// value the mux matched and delegate to the per-execution handler. They exist
// so each execution sub-shape is registered as its own method-qualified
// pattern (spec §1.3) and the mux — not a hand-rolled parser — selects the
// operation.
func (m *workflowControlModule) handleInspectByID(w http.ResponseWriter, r *http.Request) {
	m.handleInspect(w, r, types.ExecutionID(r.PathValue("id")))
}

func (m *workflowControlModule) handleWaitByID(w http.ResponseWriter, r *http.Request) {
	m.handleWait(w, r, types.ExecutionID(r.PathValue("id")))
}

func (m *workflowControlModule) handleSignalByID(w http.ResponseWriter, r *http.Request) {
	m.handleSignal(w, r, types.ExecutionID(r.PathValue("id")))
}

func (m *workflowControlModule) handleCancelByID(w http.ResponseWriter, r *http.Request) {
	m.handleCancel(w, r, types.ExecutionID(r.PathValue("id")))
}

func (m *workflowControlModule) handleRevokeSignalByID(w http.ResponseWriter, r *http.Request) {
	m.handleRevokeSignal(w, r, types.ExecutionID(r.PathValue("id")))
}

// newExecutionIDResolver is the resource resolver for the workflow create/invoke
// routes. Unlike the /v1/executions/ subtree — which resolves the targeted
// execution id from the path — create/invoke have no path id, so the resolver
// pre-allocates a fresh execution id (R3.1). That id is stamped onto the
// admission audit row by authzWrap and, via engine.WithExecutionID injected by
// the same wrapper, reused by engine Submit/Invoke, so the audit row and the
// persisted execution share one id (closing the audit↔execution correlation
// gap that left reconcile Probe reading an empty ExecutionID).
//
// resource/workflowID/namespace stay empty: create/invoke authz was decided on
// operation alone before this change, and this resolver only supplies the
// correlation id, not authz inputs.
func newExecutionIDResolver() func(*http.Request) (string, string, string, string) {
	return func(*http.Request) (string, string, string, string) {
		return "", "", string(engine.NewExecutionID()), ""
	}
}

// auditDeny / auditReconcile / statusRecorder live in authz_wrap.go, shared
// with managementModule via the embedded authzHolder.

// errorResponse is the BARE (non-enveloped) failure shape used ONLY by the
// runner-protocol-face endpoints that must not be enveloped — chiefly the
// entry-seed route POST /v1/executions (see API-SPECIFICATION.md §0.1 + §8.2).
// Its 409 stale_generation body {"error":"stale_generation"} is a load-bearing
// contract: service/protocol/entry_seed_runtime.go distinguishes a genuine
// admission conflict (body has state=="conflict") from a generation-fence
// rejection (no state field) and commits or withholds a Kafka offset based on
// that. Enveloping it would risk silent message loss during a generation
// upgrade. User-facing handlers use writeFail/writeData, never this type.
type errorResponse struct {
	Error string `json:"error"`
}

type submitWorkflowRequest struct {
	Workflow *types.WorkflowDef `json:"workflow"`
	Params   map[string]any     `json:"params,omitempty"`
}

type submitWorkflowResponse struct {
	ExecutionID types.ExecutionID `json:"execution_id"`
}

type invokeRequest struct {
	Workflow *types.WorkflowDef `json:"workflow"`
	Entry    string             `json:"entry"`
	Input    map[string]any     `json:"input,omitempty"`
}

type invokeResponse struct {
	ExecutionID types.ExecutionID `json:"execution_id"`
}

type signalRequest struct {
	Name string         `json:"name"`
	Data map[string]any `json:"data,omitempty"`
}

type waitTimeoutResponse struct {
	ExecutionID types.ExecutionID     `json:"execution_id"`
	Status      types.ExecutionStatus `json:"status"`
	TimedOut    bool                  `json:"timed_out"`
}

func (m *workflowControlModule) handleSubmitWorkflow(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req submitWorkflowRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Workflow == nil {
		writeError(w, http.StatusBadRequest, "workflow is required")
		return
	}
	g, err := graph.Compile(req.Workflow)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// xflow.workflow.submit starts the inbound trace for a workflow execution.
	// Its SpanContext is persisted on the execution snapshot (via
	// engine.WithTraceCarrier) so the later, asynchronous dispatch span can
	// inherit it as a real W3C remote parent — closing the submit→dispatch
	// causality gap without faking a parent from trace_id/span_id strings.
	tracer := m.tracer
	if tracer == nil {
		tracer = tracing.NoopTracer{}
	}
	ctx, span := tracer.Start(r.Context(), "xflow.workflow.submit")
	defer span.End()
	ctx = engine.WithTraceCarrier(ctx, tracing.InjectCarrier(ctx))
	// Attach the original workflow definition so the durable SQL execution
	// projection (buildExecutionRecord) can persist workflow_def (NOT NULL).
	// Without this, production mode with a SQL store fails submit with a
	// NOT-NULL violation on xflow_executions.workflow_def.
	ctx = engine.WithWorkflowDef(ctx, req.Workflow)
	id, err := m.eng.Submit(ctx, g, req.Params)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, submitWorkflowResponse{ExecutionID: id})
}

func (m *workflowControlModule) handleInvoke(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req invokeRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Workflow == nil {
		writeError(w, http.StatusBadRequest, "workflow is required")
		return
	}
	if req.Entry == "" {
		writeError(w, http.StatusBadRequest, "entry is required")
		return
	}
	g, err := graph.Compile(req.Workflow)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// xflow.workflow.invoke mirrors submit for the explicit-entry path. Its
	// SpanContext is persisted for asynchronous dispatch causality.
	tracer := m.tracer
	if tracer == nil {
		tracer = tracing.NoopTracer{}
	}
	ctx, span := tracer.Start(r.Context(), "xflow.workflow.invoke", "entry", req.Entry)
	defer span.End()
	ctx = engine.WithTraceCarrier(ctx, tracing.InjectCarrier(ctx))
	// Attach the original workflow definition so the durable SQL execution
	// projection can persist workflow_def (see handleSubmitWorkflow).
	ctx = engine.WithWorkflowDef(ctx, req.Workflow)
	id, err := m.eng.Invoke(ctx, g, req.Entry, req.Input)
	if err != nil {
		// An unknown entry node is a client error (400), not a 404: the
		// missing resource is the entry in the submitted graph, not an
		// execution in the store.
		if errors.Is(err, engine.ErrEntryNotFound) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, invokeResponse{ExecutionID: id})
}

// registerWorkflowResponse echoes the persisted workflow ID (server-assigned
// when the request did not carry one) along with the non-fatal diagnostics
// graph.Compile collected.
//
// Warnings are surfaced here rather than on submit because register is the
// design-time step: the author is holding the definition and can still change
// it. A warning names only nodes and referenced node names -- never a node's
// output or a parameter value, which routinely carry credentials.
type registerWorkflowResponse struct {
	WorkflowID types.WorkflowID `json:"workflow_id"`
	Warnings   []string         `json:"warnings,omitempty"`
}

// handleRegisterWorkflow serves POST /v1/workflows/register. It is a NEW,
// explicit step distinct from submit-and-execute (/v1/workflows): it compiles
// the submitted definition and persists the compiled graph in the server-side
// workflow registry so later tasks can resolve the graph on seed and derive
// entry activations. The authoritative namespace is resolved server-side from
// namespace.FromContext (injected by the authz wrapper), NEVER from the request
// body, so a caller cannot register into another namespace.
func (m *workflowControlModule) handleRegisterWorkflow(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var def types.WorkflowDef
	if !decodeJSON(w, r, &def) {
		return
	}
	// Namespace is authoritative from the context, never the body.
	ns := namespace.FromContext(r.Context())

	id, warnings, err := m.registerWorkflow(r.Context(), ns, &def)
	if err != nil {
		var compileErr *WorkflowCompileError
		if errors.As(err, &compileErr) {
			writeError(w, http.StatusBadRequest, compileErr.Unwrap().Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, registerWorkflowResponse{WorkflowID: id, Warnings: warnings})
}

// WorkflowCompileError marks a registration failure that the caller can fix by
// changing the definition, as opposed to a server-side failure. The HTTP
// handler maps it to 400 and surfaces the compiler's message; every other error
// collapses to a generic 500, since it may name internal state.
type WorkflowCompileError struct{ err error }

func (e *WorkflowCompileError) Error() string { return e.err.Error() }
func (e *WorkflowCompileError) Unwrap() error { return e.err }

// registerWorkflow is the definition -> persisted graph path both entry points
// share: the HTTP handler above and APIServer.RegisterWorkflow, which an
// embedded server calls in-process.
//
// Sharing it is the point. Registration identity is three coupled choices --
// workflowRegistryKey, definitionHash, and the entry-activation derivation --
// and a second implementation that picked any of them differently would
// register a workflow the dispatcher then failed to resolve. ns is supplied by
// the caller and written onto def; it is never read from def itself, so an
// in-process caller cannot register into another namespace either.
func (m *workflowControlModule) registerWorkflow(ctx context.Context, ns namespace.Namespace, def *types.WorkflowDef) (types.WorkflowID, []string, error) {
	registry := m.registry()
	if registry == nil {
		if m.log != nil {
			m.log.Error("register_workflow_no_registry")
		}
		return "", nil, errors.New("apiserver: no workflow registry configured")
	}
	g, err := graph.Compile(def)
	if err != nil {
		return "", nil, &WorkflowCompileError{err: err}
	}
	def.Namespace = string(ns)

	rec, err := registry.AddWorkflow(ctx, backend.WorkflowRecord{
		Key:            workflowRegistryKey(string(ns), def.Name, def.Version),
		Namespace:      string(ns),
		Name:           def.Name,
		Version:        def.Version,
		DefinitionHash: definitionHash(def),
		Definition:     def,
		Graph:          g,
	})
	if err != nil {
		if m.log != nil {
			m.log.Error("register_workflow_failed", "err", err)
		}
		return "", nil, err
	}
	// Derive the node-generic entry activations for this workflow version so the
	// reconciler can assign remote-hosted trigger entry units to runners. The
	// namespace is the same server-side value used for the registry record. A
	// derivation failure fails the register rather than leaving a
	// registered-but-unactivated workflow — the manager is fail-closed on a group
	// package that cannot be projected. Nil-guarded: an embedded control plane
	// without an EntryActivationStore exposes no manager and skips this.
	if mgr := m.entryActivationManager(); mgr != nil {
		if err := mgr.AddOrUpdateWorkflow(ctx, ns, rec.ID, def.Version, g); err != nil {
			if m.log != nil {
				m.log.Error("register_workflow_derive_activations_failed", "err", err)
			}
			return "", nil, err
		}
	}
	return rec.ID, g.Warnings(), nil
}

// handleDeregisterWorkflow serves DELETE /v1/workflows/register/{id}. It removes
// the persisted record for the given workflow id. The id is a server-assigned
// opaque identifier taken from the path; namespace isolation is enforced by the
// registry record (a later task may add per-namespace scoping on removal).
func (m *workflowControlModule) handleDeregisterWorkflow(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodDelete) {
		return
	}
	id := strings.Trim(strings.TrimPrefix(r.URL.Path, "/v1/workflows/register/"), "/")
	if id == "" || strings.Contains(id, "/") {
		writeError(w, http.StatusNotFound, "workflow not found")
		return
	}
	// The namespace is resolved server-side, never from the body or the path.
	err := m.deregisterWorkflow(r.Context(), namespace.FromContext(r.Context()), types.WorkflowID(id))
	if err != nil {
		if errors.Is(err, backend.ErrWorkflowNotFound) {
			writeError(w, http.StatusNotFound, "workflow not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"removed": true})
}

// deregisterWorkflow is the clear-activations -> remove-record path both entry
// points share: the HTTP handler above and APIServer.ReplaceWorkflow, which an
// embedded server reaches in-process.
//
// It is shared for the same reason registerWorkflow is: removal has an ordering
// contract that a second implementation would be free to get wrong, and getting
// it wrong strands a Desired activation whose workflow no longer exists.
func (m *workflowControlModule) deregisterWorkflow(ctx context.Context, ns namespace.Namespace, id types.WorkflowID) error {
	registry := m.registry()
	if registry == nil {
		if m.log != nil {
			m.log.Error("deregister_workflow_no_registry")
		}
		return errors.New("apiserver: no workflow registry configured")
	}
	// Fetch the record BEFORE removal so the entry-activation manager can derive
	// the same trigger entry units (from the persisted compiled graph) to clear,
	// and so an already-removed workflow reports not-found. When no activation
	// manager is wired this lookup is skipped entirely.
	var toDeactivate *backend.WorkflowRecord
	if mgr := m.entryActivationManager(); mgr != nil {
		rec, err := registry.GetWorkflow(ctx, id)
		if err != nil {
			if !errors.Is(err, backend.ErrWorkflowNotFound) && m.log != nil {
				m.log.Error("deregister_workflow_lookup_failed", "err", err)
			}
			return err
		}
		toDeactivate = &rec
	}
	// Clear the desired entry activations BEFORE removing the registry record so
	// the two steps are independently retryable and no orphaned Desired activation
	// can survive a partial failure. If this clear fails we return with the
	// registry record still present, so a retry re-fetches the graph and re-clears;
	// if we removed the registry record first and then failed here, the retry
	// would report not-found on GetWorkflow and never reach the clear, stranding a
	// Desired activation the reconciler keeps honoring for entry seeds. The clear
	// is desired-state only (Desired=false); the reconciler delivers the
	// Deactivate and fences the owner.
	if toDeactivate != nil {
		mgr := m.entryActivationManager()
		if err := mgr.RemoveWorkflow(ctx, ns, toDeactivate.ID, toDeactivate.Version, toDeactivate.Graph); err != nil {
			if m.log != nil {
				m.log.Error("deregister_workflow_clear_activations_failed", "err", err)
			}
			return err
		}
	}
	// Remove the registry record. The manager clear above is idempotent (a repeat
	// re-marks an already-!Desired record), so a failure here is safely retryable:
	// the caller retries, re-clears harmlessly, and re-removes.
	if err := registry.RemoveWorkflow(ctx, id); err != nil {
		if !errors.Is(err, backend.ErrWorkflowNotFound) && m.log != nil {
			m.log.Error("deregister_workflow_failed", "err", err)
		}
		return err
	}
	return nil
}

// replaceWorkflow registers def, and if a DIFFERENT definition already occupies
// its (namespace, name, version) key, deregisters that one first and registers
// again.
//
// It exists for the deployment that owns its workflow key outright: an embedded
// control plane whose workflow definition is built from the host's own
// configuration. Such a definition changes whenever the configuration does — a
// new topic, a rebuilt wasm artifact — and the registry rejects a changed
// definition under an existing key as a conflict. With only registerWorkflow to
// call, that host can never start again: it fails on every boot, with the old
// definition still registered and running.
//
// The removal is deliberate and destructive, which is why it is not the
// behaviour of registerWorkflow. Two hosts sharing one key with DIFFERENT
// definitions — a rolling deploy mid-flight — will each replace the other until
// the rollout settles, deactivating and re-deriving the entry activations each
// time. Identical definitions never reach the removal at all: they match on
// hash and register idempotently.
func (m *workflowControlModule) replaceWorkflow(ctx context.Context, ns namespace.Namespace, def *types.WorkflowDef) (types.WorkflowID, []string, error) {
	id, warnings, err := m.registerWorkflow(ctx, ns, def)
	if err == nil || !errors.Is(err, backend.ErrWorkflowConflict) {
		return id, warnings, err
	}
	registry := m.registry()
	if registry == nil {
		return "", nil, err
	}
	// The key carries the namespace, so this lookup cannot reach another
	// namespace's record even though the registry is not otherwise scoped.
	existing, lookupErr := registry.GetWorkflowByKey(ctx, workflowRegistryKey(string(ns), def.Name, def.Version))
	if lookupErr != nil {
		// The caller's contract is "conflict", not "lookup failed".
		return "", nil, err
	}
	if delErr := m.deregisterWorkflow(ctx, ns, existing.ID); delErr != nil {
		return "", nil, delErr
	}
	if m.log != nil {
		m.log.Info("replace_workflow_removed_conflicting", "workflow_id", string(existing.ID), "name", def.Name, "version", def.Version)
	}
	return m.registerWorkflow(ctx, ns, def)
}

// registry returns the control plane's workflow registry, or nil when no
// ControlPlane is wired (unit tests with a fake facade) or none is configured.
func (m *workflowControlModule) registry() backend.WorkflowRegistry {
	if m.cp == nil {
		return nil
	}
	return m.cp.WorkflowRegistry()
}

// entryActivationManager returns the control plane's node-generic
// entry-activation manager, or nil when no ControlPlane is wired (unit tests
// with a fake facade) or no EntryActivationStore is configured (embedded /
// in-process). Callers nil-guard: registering/deregistering a workflow derives/
// clears entry activations only when a manager is present.
func (m *workflowControlModule) entryActivationManager() *control.EntryActivationManager {
	if m.cp == nil {
		return nil
	}
	return m.cp.EntryActivationManager()
}

// workflowRegistryKey builds the registry conflict-detection key from the
// server-issued namespace + workflow name + version. It mirrors the SDK's
// namespace/name@version identity so a definition registered twice is idempotent.
func workflowRegistryKey(ns, name, version string) string {
	return fmt.Sprintf("%s/%s@%s", ns, name, version)
}

// definitionHash returns a stable SHA-256 fingerprint over the JSON-encoded
// definition. It is used by the registry for conflict detection (a re-register
// of an identical definition is idempotent; a changed definition under the same
// key is rejected as a conflict). Marshal errors collapse to an empty hash,
// which the registry treats as a distinct (always-conflicting) value.
func definitionHash(def *types.WorkflowDef) string {
	data, err := json.Marshal(def)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// handleSeedExecution serves POST /v1/executions: it atomically seeds an
// authoritative namespace is injected into the request context by the authz
// wrapper (from the authenticated principal) and resolved server-side in the
// control Core — it is NEVER read from the request body, so a forged or
// cross-namespace admission key fails closed. The ResultHash is recomputed
// server-side from the request's outcome + exits; a client-supplied hash is not
// trusted.
func (m *workflowControlModule) handleSeedExecution(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req protocol.SeedExecutionRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.AdmissionKey == "" || req.WorkflowID == "" || req.EntryUnitID == "" || req.Outcome == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "admission_key, workflow_id, entry_unit_id and outcome are required"})
		return
	}

	exits := make([]engine.BoundaryExit, 0, len(req.Exits))
	for _, ex := range req.Exits {
		exits = append(exits, engine.BoundaryExit{
			NodeName: ex.NodeName,
			Port:     ex.Port,
			Data:     ex.Data,
		})
	}
	outcome := engine.GroupOutcome(req.Outcome)

	engReq := engine.SeedExecutionFromEntryRequest{
		AdmissionKey:    engine.AdmissionKey(req.AdmissionKey),
		WorkflowID:      types.WorkflowID(req.WorkflowID),
		WorkflowVersion: req.WorkflowVersion,
		EntryUnitID:     req.EntryUnitID,
		Outcome:         outcome,
		Exits:           exits,
		Error:           req.Error,
		Generation:      req.Generation,
		// ResultHash is computed server-side; the client cannot supply it.
		ResultHash: engine.ComputeResultHash(outcome, exits),
	}

	// Route through the ControlPlane's Core so the server-side namespace
	// resolution and generation fence (spec §11.6) always apply. Namespace is
	// taken from the request context (injected by the authz wrapper), not the
	// body. Fall back to the raw engine only when no ControlPlane is wired
	// (unit tests with a fake facade); that path has no generation fence.
	var resp engine.SeedExecutionFromEntryResponse
	var err error
	if m.cp != nil {
		resp, err = m.cp.SeedExecutionFromEntry(r.Context(), engReq)
	} else {
		resp, err = m.eng.SeedExecutionFromEntry(r.Context(), engReq)
	}
	if err != nil {
		// A stale activation generation for a not-yet-accepted admission key is a
		// fencing rejection, not an internal fault — map it to 409 so the runner
		// can distinguish "you lost the activation" from a transient server error.
		// The body is the BARE errorResponse (not enveloped): entry-seed is a
		// runner-protocol-face endpoint (spec §0.1) and its 409 body shape is a
		// load-bearing offset-safety contract (spec §8.2). Enveloping it would
		// risk silent message loss during a generation upgrade.
		if errors.Is(err, control.ErrStaleGeneration) {
			writeJSON(w, http.StatusConflict, errorResponse{Error: "stale_generation"})
			return
		}
		// The seed references a workflow/entry unit the control plane cannot
		// resolve (not registered, version mismatch, or unknown entry unit). This
		// is a fail-closed rejection, not an internal fault — map it to 404 so the
		// runner can distinguish it from a transient server error. The generic
		// reason string leaks no internal detail. Bare errorResponse per §0.1/§8.2.
		if errors.Is(err, control.ErrEntrySeedWorkflowUnknown) {
			writeJSON(w, http.StatusNotFound, errorResponse{Error: "workflow_unknown"})
			return
		}
		if m.log != nil {
			m.log.Error("seed_execution_failed", "err", err)
		}
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "internal server error"})
		return
	}
	if resp.State == engine.AdmissionStateConflict {
		writeJSON(w, http.StatusConflict, protocol.SeedExecutionResponse{
			State:       "conflict",
			ExecutionID: string(resp.ExecutionID),
		})
		return
	}
	writeJSON(w, http.StatusOK, protocol.SeedExecutionResponse{
		State:       string(resp.State),
		ExecutionID: string(resp.ExecutionID),
		Duplicate:   resp.Duplicate,
	})
}

func (m *workflowControlModule) handleInspect(w http.ResponseWriter, r *http.Request, id types.ExecutionID) {
	detail, err := m.eng.Inspect(r.Context(), id)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

func (m *workflowControlModule) handleSignal(w http.ResponseWriter, r *http.Request, id types.ExecutionID) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req signalRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if err := m.eng.DeliverSignal(r.Context(), id, req.Name, req.Data); err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"accepted": true})
}

func (m *workflowControlModule) handleCancel(w http.ResponseWriter, r *http.Request, id types.ExecutionID) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	if err := m.eng.Cancel(r.Context(), id); err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"accepted": true})
}

func (m *workflowControlModule) handleRevokeSignal(w http.ResponseWriter, r *http.Request, id types.ExecutionID) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	// Body is optional; a JSON {"name":...} overrides the query parameter.
	name := r.URL.Query().Get("name")
	if r.ContentLength != 0 {
		var req signalRequest
		if !decodeJSON(w, r, &req) {
			return
		}
		if req.Name != "" {
			name = req.Name
		}
	} else {
		_ = r.Body.Close()
	}
	if name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if err := m.eng.RevokeSignal(r.Context(), id, name); err != nil {
		if errors.Is(err, engine.ErrSignalConsumed) {
			writeError(w, http.StatusConflict, "signal already consumed or not found")
			return
		}
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"revoked": true})
}

// handleWait long-polls an execution until it reaches a terminal state or the
// timeout elapses. It uses http.ResponseController to extend the connection's
// write deadline beyond the server's default WriteTimeout so a long poll does
// not get cut off mid-flight. The poll timeout is capped at 10 minutes.
func (m *workflowControlModule) handleWait(w http.ResponseWriter, r *http.Request, id types.ExecutionID) {
	timeout := parseWaitTimeout(r)
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Now().Add(timeout))

	ctx := r.Context()
	deadline := time.Now().Add(timeout)
	const pollInterval = 200 * time.Millisecond

	for {
		detail, err := m.eng.Inspect(ctx, id)
		if err != nil {
			writeEngineError(w, err)
			return
		}
		if types.IsTerminalExecutionStatus(detail.Status) {
			writeJSON(w, http.StatusOK, detail)
			return
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			writeJSON(w, http.StatusAccepted, waitTimeoutResponse{
				ExecutionID: id,
				Status:      detail.Status,
				TimedOut:    true,
			})
			return
		}
		wait := remaining
		if wait > pollInterval {
			wait = pollInterval
		}
		select {
		case <-ctx.Done():
			writeJSON(w, http.StatusAccepted, waitTimeoutResponse{
				ExecutionID: id,
				Status:      detail.Status,
				TimedOut:    true,
			})
			return
		case <-time.After(wait):
		}
	}
}

// parseWaitTimeout resolves the ?timeout= query parameter for handleWait. The
// default is 5s when absent or unparseable; the value is capped at 10m.
func parseWaitTimeout(r *http.Request) time.Duration {
	const (
		defaultWait = 5 * time.Second
		maxWait     = 10 * time.Minute
	)
	q := r.URL.Query().Get("timeout")
	if q == "" {
		return defaultWait
	}
	d, err := time.ParseDuration(q)
	if err != nil || d <= 0 {
		return defaultWait
	}
	if d > maxWait {
		return maxWait
	}
	return d
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	defer func() { _ = r.Body.Close() }()
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return false
	}
	return true
}

func requireMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method == method {
		return true
	}
	writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	return false
}

// writeEngineError maps typed engine/store errors to HTTP responses. Every
// unclassified failure is collapsed to a generic 500 message — the underlying
// error (Redis text, internal paths, backend details) must never reach a client.
func writeEngineError(w http.ResponseWriter, err error) {
	if errors.Is(err, engine.ErrExecutionInactive) ||
		errors.Is(err, engine.ErrExecutionNotFound) ||
		errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeError(w, http.StatusInternalServerError, "internal server error")
}

// writeJSON writes a bare JSON body (no envelope). entry-seed (POST
// /v1/executions) is the one caller that must keep using it permanently — it
// is a runner-protocol-face endpoint whose 409 body shape is a load-bearing
// offset-safety contract (spec §0.1 + §8.2). The remaining user-face callers
// (submit/invoke/register/inspect/management/supply success paths) still use
// it today and are pending conversion to writeData/writeFail in the
// route-migration tasks (§9.3); they are neither converted nor bugs.
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// writeError is the transitional shim: it envelopes the failure and derives a
// code from the status. Call sites migrate to writeFail with an explicit,
// stable code as their route is converted.
//
// It takes no *http.Request, so responses from it carry an empty trace_id and
// no echoed X-Request-Id. That is the tell for a call site that still needs
// migrating.
func writeError(w http.ResponseWriter, status int, message string) {
	writeFail(w, nil, status, codeForStatus(status), message)
}
