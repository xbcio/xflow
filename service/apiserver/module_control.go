package apiserver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"

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
	// executions is the execution store backing the collection read
	// GET /v1/executions (spec §3.3). It is the one dependency of this module
	// that does NOT come from the ControlPlane, so it is injected
	// post-construction by APIServer.New (Config.Store) exactly the way the
	// management module's registration-code stores are. Nil means no execution
	// store is wired — the collection route is then registered but answers 500,
	// never an empty page (an empty 200 would read as "this namespace has no
	// executions", which is a different and false statement).
	//
	// It is deliberately a narrow store.Executions rather than a whole
	// store.Store: this module reads executions and nothing else, and the
	// narrower field is what makes that auditable.
	executions store.Executions
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
				// §7: the presented credential is never echoed; the message is generic.
				writeFail(w, r, http.StatusUnauthorized, "unauthorized", "unauthorized")
				return
			}
			h(w, r)
		}
	}
	// Routes are Go 1.22 method-qualified mux patterns (spec §1.3): the mux —
	// not a hand-rolled TrimPrefix parser — selects the handler, and every
	// {id} is read via r.PathValue. The literal "execute" segment
	// (PathWorkflowExecute) wins over a hypothetical {id} at the same depth,
	// so POST /v1/workflows/execute is never captured as a workflow id.
	//
	// §9.1 semantic inversion: POST /v1/workflows now REGISTERS a definition
	// (was compile-and-execute). The compile-and-execute semantics moved to
	// POST /v1/workflows/execute, which merges the old submit + invoke shapes.
	mux.HandleFunc("POST "+PathWorkflows, wrap("register_workflow", m.handleRegisterWorkflow))
	// GET /v1/workflows is the offset-paginated collection read (spec §3.3). It
	// is mounted in this legacy branch as well as in registerAuthzRoutes: here
	// the namespace is namespace.FromContext's Default rather than a verified
	// principal's, which is the same namespace every other bare-mode workflow
	// handler in this branch already uses.
	mux.HandleFunc("GET "+PathWorkflows, wrap("list_workflows", m.handleListWorkflows))
	mux.HandleFunc("GET "+PathWorkflowByID, wrap("read_workflow", m.handleGetWorkflow))
	mux.HandleFunc("PUT "+PathWorkflowByID, wrap("replace_workflow", m.handleReplaceWorkflow))
	mux.HandleFunc("DELETE "+PathWorkflowByID, wrap("deregister_workflow", m.handleDeregisterWorkflow))
	mux.HandleFunc("POST "+PathWorkflowExecute, wrap("execute_workflow", m.handleExecuteWorkflow))
	mux.HandleFunc("POST "+PathWorkflowExecuteByID, wrap("execute_workflow_by_id", m.handleExecuteWorkflowByID))
	// POST /v1/executions is the entry-seed endpoint (runner protocol face,
	// spec §0.1); the per-execution GET/signal/cancel/wait surface is the
	// method-qualified patterns below. ServeMux treats exact and {id} patterns
	// as distinct from the /v1/executions/{id}/ 404 catch.
	mux.HandleFunc("POST "+PathExecutions, wrap("seed_execution", m.handleSeedExecution))
	// GET /v1/executions is the offset-paginated execution collection read
	// (spec §3.3). It is mounted in this legacy branch as well as in
	// registerAuthzRoutes, for the same reason GET /v1/workflows is: without a
	// PrincipalAuthenticator this branch has no verified principal, so the
	// scope is namespace.FromContext's Default — the same namespace every other
	// bare-mode handler in this branch already uses. The route is registered
	// even when no execution store is wired; the handler then answers 500
	// rather than serving a false empty page.
	mux.HandleFunc("GET "+PathExecutions, wrap("execution.read", m.handleListExecutions))
	mux.HandleFunc("GET "+PathExecutionByID, wrap("execution.read", m.handleInspectByID))
	mux.HandleFunc("GET "+PathExecutionWait, wrap("execution.read", m.handleWaitByID))
	mux.HandleFunc("POST "+PathExecutionSignals, wrap("execution.signal", m.handleSignalByID))
	mux.HandleFunc("POST "+PathExecutionCancel, wrap("execution.cancel", m.handleCancelByID))
	// §9.1: revoke-signal moved from POST /v1/executions/{id}/revoke-signal
	// (verb stuck in a path segment) to DELETE /v1/executions/{id}/signals/{name}
	// — the signal name travels in the path, not the body (spec §7 route table).
	mux.HandleFunc("DELETE "+PathExecutionSignalByID, wrap("execution.revoke", m.handleRevokeSignalByID))
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
//   - DELETE /v1/executions/{id}/signals/{name} → execution.revoke (mutation)
//   - POST /v1/executions/{id}/cancel     → execution.cancel    (mutation)
func (m *workflowControlModule) registerAuthzRoutes(mux *http.ServeMux) {
	authz := m.authzWrap
	// §9.1 semantic inversion: POST /v1/workflows now REGISTERS. The
	// compile-and-execute route moved to POST /v1/workflows/execute (merging
	// submit+invoke). Each route is its own method-qualified pattern bound to a
	// stable Op so the authz wrapper resolves the operation from the pattern
	// itself (spec §1.3). The authz wrapper injects the principal's namespace
	// into the request context; handlers resolve it via namespace.FromContext —
	// never from the client body (spec §6.2).
	mux.HandleFunc("POST "+PathWorkflows, authz(OpWorkflowRegister, true, m.handleRegisterWorkflow, nil))
	// GET /v1/workflows (the collection read) reuses OpWorkflowRead — the very
	// operation GET /v1/workflows/{id} carries — so the two scopes cannot drift:
	// a principal that may read one workflow may list them, and one that may not
	// is refused by the same default-deny check before the handler runs. No
	// resource resolver is passed: a collection is not one resource, and the
	// scope of the read is the principal's own namespace, resolved inside the
	// handler via namespace.FromContext (never from a query parameter).
	mux.HandleFunc("GET "+PathWorkflows, authz(OpWorkflowRead, false, m.handleListWorkflows, nil))
	mux.HandleFunc("GET "+PathWorkflowByID, authz(OpWorkflowRead, false, m.handleGetWorkflow, workflowIDResolver()))
	mux.HandleFunc("PUT "+PathWorkflowByID, authz(OpWorkflowDefinitionUpdate, true, m.handleReplaceWorkflow, workflowIDResolver()))
	mux.HandleFunc("DELETE "+PathWorkflowByID, authz(OpWorkflowRegister, true, m.handleDeregisterWorkflow, workflowIDResolver()))
	mux.HandleFunc("POST "+PathWorkflowExecute, authz(OpWorkflowCreate, true, m.handleExecuteWorkflow, newExecutionIDResolver()))
	mux.HandleFunc("POST "+PathWorkflowExecuteByID, authz(OpWorkflowInvoke, true, m.handleExecuteWorkflowByID, workflowIDResolver()))
	// Entry-seed endpoint (exact path, POST only). The authz wrapper injects
	// the principal's namespace into the request context; handleSeedExecution
	// reads it via namespace.FromContext — never from the client body.
	mux.HandleFunc("POST "+PathExecutions, authz(OpExecutionSeed, true, m.handleSeedExecution, newExecutionIDResolver()))
	// GET /v1/executions (the collection read) reuses OpExecutionRead — the very
	// operation GET /v1/executions/{id} carries, and deliberately NOT
	// OpExecutionSeed, which is the mutation the POST route on this same exact
	// path carries. The mux picks between them by method before the wrapper
	// runs: the authz wrapper here is the static-op form, so `isMutation` is a
	// compile-time literal (false) rather than something resolved from the
	// request, and a GET can never be admitted as a mutation. No resource
	// resolver is passed: a collection is not one resource, and the scope of
	// the read is the principal's own namespace, resolved inside the handler
	// via namespace.FromContext (never from a query parameter).
	mux.HandleFunc("GET "+PathExecutions, authz(OpExecutionRead, false, m.handleListExecutions, nil))
	mux.HandleFunc("GET "+PathExecutionByID, authz(OpExecutionRead, false, m.handleInspectByID, execIDResolver("")))
	mux.HandleFunc("GET "+PathExecutionWait, authz(OpExecutionRead, false, m.handleWaitByID, execIDResolver("wait")))
	mux.HandleFunc("POST "+PathExecutionSignals, authz(OpExecutionSignal, true, m.handleSignalByID, execIDResolver("signals")))
	// §9.1: revoke-signal → DELETE /v1/executions/{id}/signals/{name}. The Op
	// (execution.revoke) is unchanged; the audit-target resource string now
	// mirrors the new path (execution/{id}/signals/{name}) so the audit row is
	// coherent with the route the caller hit (spec §6). The resolver reads both
	// {id} and {name} from the path the mux matched.
	mux.HandleFunc("DELETE "+PathExecutionSignalByID, authz(OpExecutionRevoke, true, m.handleRevokeSignalByID, execSignalResolver()))
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

// execSignalResolver is the resource resolver for
// DELETE /v1/executions/{id}/signals/{name} (spec §9.1 revoke migration). The
// audit-target resource string mirrors the new path so the audit row is
// coherent with the route the caller hit: execution/{id}/signals/{name}. It
// reads both {id} and {name} from the path the mux matched. ResourceNamespace
// is left empty for the same IDOR reason as execIDResolver.
func execSignalResolver() func(*http.Request) (string, string, string, string) {
	return func(r *http.Request) (string, string, string, string) {
		id := r.PathValue("id")
		name := r.PathValue("name")
		return "execution/" + id + "/signals/" + name, "", id, ""
	}
}

// workflowIDResolver builds the resource resolver for a /v1/workflows/{id}
// route: it reads the {id} the mux matched and returns the workflow-scoped
// resource string the audit row carries. ResourceNamespace is left empty —
// the authoritative IDOR defense is the namespace-scoped registry read (the
// authz wrapper injects the principal's namespace so a cross-namespace id
// resolves to not-found → 404).
func workflowIDResolver() func(*http.Request) (string, string, string, string) {
	return func(r *http.Request) (string, string, string, string) {
		id := r.PathValue("id")
		return "workflow/" + id, "", id, ""
	}
}

// handleExecutionNotFound is the 404 catch for the /v1/executions/{id}/
// subtree: any execution sub-shape no method-qualified pattern serves lands
// here and is refused with 404 "route not found" — no existence leak. It
// replaces the default branch of the old hand-rolled handleExecution parser.
// Enveloped (spec §3) with a stable code so a caller can distinguish a
// shape-mismatch 404 from an execution_not_found 404.
func (m *workflowControlModule) handleExecutionNotFound(w http.ResponseWriter, r *http.Request) {
	writeFail(w, r, http.StatusNotFound, "route_not_found", "route not found")
}

// handleInspectByID / handleWaitByID / handleSignalByID / handleCancelByID /
// handleRevokeSignalByID are mux-pattern adapters: they pull the {id} path
// value the mux matched and delegate to the per-execution handler. They exist
// so each execution sub-shape is registered as its own method-qualified
// pattern (spec §1.3) and the mux — not a hand-rolled parser — selects the
// operation. handleRevokeSignalByID also pulls {name} (the signal moved from
// the request body into the path under the §9.1 migration).
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

// executeWorkflowRequest is the inline-definition direct-run body for
// POST /v1/workflows/execute (spec §7 + §9.1). It merges the old submit
// ({workflow, params}) and invoke ({workflow, entry, input}) shapes: when
// Entry is empty the graph's default start node is used (Submit); when set,
// that entry node drives the explicit-entry path (Invoke). A single route
// serves both so the verb-layer collapse in §1.2 does not split them back
// across two paths.
type executeWorkflowRequest struct {
	Workflow *types.WorkflowDef `json:"workflow"`
	Entry    string             `json:"entry,omitempty"`
	Input    map[string]any     `json:"input,omitempty"`
	Params   map[string]any     `json:"params,omitempty"`
}

type executeWorkflowResponse struct {
	ExecutionID types.ExecutionID `json:"execution_id"`
}

// executeRegisteredRequest is the body for POST /v1/workflows/{id}/execute
// (spec §7): run an already-registered workflow by id. Entry/Input/Params are
// optional; when Entry is empty the registered graph's default start node is
// used.
type executeRegisteredRequest struct {
	Entry  string         `json:"entry,omitempty"`
	Input  map[string]any `json:"input,omitempty"`
	Params map[string]any `json:"params,omitempty"`
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

// handleExecuteWorkflow serves POST /v1/workflows/execute (spec §7 + §9.1): it
// compiles an INLINE definition and immediately starts an execution. It merges
// the old submit ({workflow, params}) and invoke ({workflow, entry, input})
// shapes — when Entry is empty the graph's default start node is used (Submit);
// when set, that entry node drives the explicit-entry path (Invoke).
//
// This is the compile-and-execute semantics that USED to live on
// POST /v1/workflows before the §9.1 semantic inversion moved that path to
// register. A caller on the old POST /v1/workflows semantics gets silently
// different behavior now (register, not execute); this route is where the
// execute behavior moved.
//
// Failure codes are stable snake_case per spec §3.2. A compile error's message
// carries only node names / referenced node names / parameter names — never a
// node's output or a parameter value, which routinely carry credentials
// (spec §3.5 / branch specialization).
func (m *workflowControlModule) handleExecuteWorkflow(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req executeWorkflowRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Workflow == nil {
		writeFail(w, r, http.StatusBadRequest, "workflow_invalid", "workflow is required")
		return
	}
	g, err := graph.Compile(req.Workflow)
	if err != nil {
		writeFail(w, r, http.StatusBadRequest, "workflow_compile_failed", err.Error())
		return
	}
	// xflow.workflow.execute starts the inbound trace for a workflow execution.
	// Its SpanContext is persisted on the execution snapshot (via
	// engine.WithTraceCarrier) so the later, asynchronous dispatch span can
	// inherit it as a real W3C remote parent — closing the submit→dispatch
	// causality gap without faking a parent from trace_id/span_id strings.
	tracer := m.tracer
	if tracer == nil {
		tracer = tracing.NoopTracer{}
	}
	// The old submit/invoke endpoints had distinct span names
	// (xflow.workflow.submit / xflow.workflow.invoke); the §9.1 merge collapsed
	// them into one route, so the span name is now uniform. The entry attribute
	// (empty for Submit, set for Invoke) carries the submit/invoke distinction.
	ctx, span := tracer.Start(r.Context(), "xflow.workflow.execute", "entry", req.Entry)
	defer span.End()
	ctx = engine.WithTraceCarrier(ctx, tracing.InjectCarrier(ctx))
	// Attach the original workflow definition so the durable SQL execution
	// projection (buildExecutionRecord) can persist workflow_def (NOT NULL).
	// Without this, production mode with a SQL store fails submit with a
	// NOT-NULL violation on xflow_executions.workflow_def.
	ctx = engine.WithWorkflowDef(ctx, req.Workflow)
	var id types.ExecutionID
	if req.Entry != "" {
		id, err = m.eng.Invoke(ctx, g, req.Entry, req.Input)
		// An unknown entry node is a client error (400): the missing resource is
		// the entry in the submitted graph, not an execution in the store.
		if err != nil {
			if errors.Is(err, engine.ErrEntryNotFound) {
				writeFail(w, r, http.StatusBadRequest, "workflow_entry_not_found", err.Error())
				return
			}
			writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
	} else {
		id, err = m.eng.Submit(ctx, g, req.Params)
		if err != nil {
			writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
	}
	writeData(w, r, http.StatusOK, executeWorkflowResponse{ExecutionID: id})
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

// handleRegisterWorkflow serves POST /v1/workflows (spec §7). After the §9.1
// semantic inversion this is the REGISTER route — it compiles the submitted
// definition and persists the compiled graph in the server-side workflow
// registry so later tasks can resolve the graph on seed and derive entry
// activations. (The old compile-and-execute semantics that lived here moved to
// POST /v1/workflows/execute.)
//
// The authoritative namespace is resolved server-side from
// namespace.FromContext (injected by the authz wrapper), NEVER from the
// request body, so a caller cannot register into another namespace (spec §6.2).
//
// 201 + Location: register creates a workflow resource (spec §4.2). Failure
// codes are stable snake_case per spec §3.2; a compile error's message carries
// only node names / referenced node names / parameter names (spec §3.5).
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
			writeFail(w, r, http.StatusBadRequest, "workflow_compile_failed", compileErr.Unwrap().Error())
			return
		}
		if errors.Is(err, backend.ErrWorkflowConflict) {
			writeFail(w, r, http.StatusConflict, "workflow_conflict", "workflow definition conflicts with an existing registration")
			return
		}
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	w.Header().Set("Location", "/v1/workflows/"+string(id))
	writeData(w, r, http.StatusCreated, registerWorkflowResponse{WorkflowID: id, Warnings: warnings})
}

// WorkflowCompileError marks a registration failure that the caller can fix by
// changing the definition, as opposed to a server-side failure. The HTTP
// handler maps it to 400 and surfaces the compiler's message; every other error
// collapses to a generic 500, since it may name internal state.
type WorkflowCompileError struct{ err error }

func (e *WorkflowCompileError) Error() string { return e.err.Error() }
func (e *WorkflowCompileError) Unwrap() error { return e.err }

var errWorkflowIDMismatch = errors.New("workflow id does not match path")

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
	rec, warnings, err := m.buildWorkflowRecord(ns, "", def)
	if err != nil {
		return "", nil, err
	}
	return m.addWorkflowRecord(ctx, ns, registry, rec, warnings)
}

func (m *workflowControlModule) buildWorkflowRecord(ns namespace.Namespace, id types.WorkflowID, def *types.WorkflowDef) (backend.WorkflowRecord, []string, error) {
	if def != nil {
		// Namespace is server-authoritative and must be fixed before compilation
		// and hashing so every derived representation uses the authenticated
		// identity rather than an untrusted body value.
		def.Namespace = string(ns)
	}
	g, err := graph.Compile(def)
	if err != nil {
		return backend.WorkflowRecord{}, nil, &WorkflowCompileError{err: err}
	}

	rec := backend.WorkflowRecord{
		ID:             id,
		Key:            workflowRegistryKey(string(ns), def.Name, def.Version),
		Namespace:      string(ns),
		Name:           def.Name,
		Version:        def.Version,
		DefinitionHash: definitionHash(def),
		Definition:     def,
		Graph:          g,
	}
	return rec, g.Warnings(), nil
}

func (m *workflowControlModule) addWorkflowRecord(ctx context.Context, ns namespace.Namespace, registry backend.WorkflowRegistry, rec backend.WorkflowRecord, warnings []string) (types.WorkflowID, []string, error) {
	if registry == nil {
		if m.log != nil {
			m.log.Error("register_workflow_no_registry")
		}
		return "", nil, errors.New("apiserver: no workflow registry configured")
	}
	stored, err := registry.AddWorkflow(ctx, rec)
	if err != nil {
		if m.log != nil {
			m.log.Error("register_workflow_failed", "err", err)
		}
		return "", nil, err
	}
	if err := m.deriveWorkflowActivations(ctx, ns, stored); err != nil {
		if m.log != nil {
			m.log.Error("register_workflow_derive_activations_failed", "err", err)
		}
		return "", nil, err
	}
	return stored.ID, warnings, nil
}

func (m *workflowControlModule) deriveWorkflowActivations(ctx context.Context, ns namespace.Namespace, rec backend.WorkflowRecord) error {
	// Derive the node-generic entry activations for this workflow version so the
	// reconciler can assign remote-hosted trigger entry units to runners. The
	// registry revision is carried into desired state as a monotonic fence: a
	// delayed projection from an older replace can then never overwrite this one.
	// Nil-guarded: an embedded control plane without an EntryActivationStore
	// exposes no manager and skips this.
	if mgr := m.entryActivationManager(); mgr != nil {
		return mgr.AddOrUpdateWorkflowRevision(ctx, ns, rec.ID, rec.Version, rec.RegistryRevision, rec.Graph)
	}
	return nil
}

// handleDeregisterWorkflow serves DELETE /v1/workflows/{id} (spec §7 +
// §9.1). It removes the persisted record for the given workflow id. The id is
// read via the mux {id} pattern (spec §1.3 — no hand-rolled TrimPrefix); the
// literal "execute" segment is registered as POST only, so a DELETE to
// /v1/workflows/execute cannot be misread as an id here. Namespace isolation
// is enforced by the registry record (a cross-namespace id resolves to
// not-found → 404).
//
// The clear-activations→remove ordering contract lives in deregisterWorkflow
// (shared with ReplaceWorkflow); this handler only parses the path and maps
// failures. Failure codes are stable snake_case per spec §3.2.
func (m *workflowControlModule) handleDeregisterWorkflow(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodDelete) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		writeFail(w, r, http.StatusNotFound, "workflow_not_found", "workflow not found")
		return
	}
	// The namespace is resolved server-side, never from the body or the path.
	err := m.deregisterWorkflow(r.Context(), namespace.FromContext(r.Context()), types.WorkflowID(id))
	if err != nil {
		if errors.Is(err, backend.ErrWorkflowNotFound) {
			writeFail(w, r, http.StatusNotFound, "workflow_not_found", "workflow not found")
			return
		}
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeData(w, r, http.StatusOK, map[string]bool{"removed": true})
}

// workflowListItem is one row of the GET /v1/workflows page. It is a summary
// projection of backend.WorkflowRecord, not the record itself.
//
// The definition and the compiled graph are deliberately absent: a page of
// full definitions is O(page × definition size) on the wire, and the UI reads
// one workflow's definition at a time through GET /v1/workflows/{id}. What a
// table row needs is the identity (id, name, version), a change detector
// (definition_hash), and the compare-and-swap token PUT /v1/workflows/{id}
// expects (registry_revision).
type workflowListItem struct {
	ID               string `json:"id"`
	Name             string `json:"name"`
	Version          string `json:"version"`
	DefinitionHash   string `json:"definition_hash"`
	RegistryRevision uint64 `json:"registry_revision"`
}

// handleListWorkflows serves GET /v1/workflows (spec §3.3 page list; it closes
// the first of the two blockers §9.6 records for the list endpoints — the
// registry's per-namespace index now exists).
//
// Namespace: resolved server-side from the authenticated principal via
// namespace.FromContext (injected by the authz wrapper), exactly as
// handleGetWorkflow does. There is deliberately no `namespace` query parameter
// and no body — a caller can only ever list its own namespace — and the
// registry takes ns as a required explicit argument that it validates, so even
// a bad value here cannot widen the read beyond the caller's namespace.
//
// Pagination: offset, 1-based, through the shared pageParams helper (default
// 20, hard cap 200). Cursor pagination is reserved for machine scans (spec
// §3.3); this is a page list, so it must not grow a second pagination dialect.
//
// Envelope: writeList, i.e. the mandated {list,total} collection shape —
// never a bare array. The two pre-existing list endpoints (listRunners,
// listRegistrationCodes) still return bare arrays, which violates §3.3; that
// is not copied here.
func (m *workflowControlModule) handleListWorkflows(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	registry := m.registry()
	if registry == nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	ns := namespace.FromContext(r.Context())
	page, pageSize := pageParams(r)

	// fail is the single failure exit below. The registry error is logged (it
	// may name internal storage detail) and never reaches the body: spec §3.5
	// requires a generic message on a 500.
	fail := func(event string, err error) {
		if m.log != nil {
			m.log.Error(event, "err", err)
		}
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
	}

	// One page of ids, sliced by the registry itself: the registry owns the
	// namespace's enumeration order (newest registry revision first, then id
	// ascending) and the offset/limit contract, so the handler never
	// re-implements paging over an unbounded slice.
	ids, err := registry.ListWorkflows(r.Context(), ns, backend.WorkflowListOptions{
		Offset: pageOffset(page, pageSize),
		Limit:  pageSize,
	})
	if err != nil {
		fail("list_workflows_failed", err)
		return
	}
	total, err := countWorkflows(r.Context(), registry, ns)
	if err != nil {
		fail("count_workflows_failed", err)
		return
	}
	items, err := m.workflowListItems(r.Context(), registry, ns, ids)
	if err != nil {
		fail("list_workflows_rows_failed", err)
		return
	}
	writeList(w, r, items, total)
}

// countWorkflows returns the number of workflows registered in ns — the
// `total` of the collection envelope.
//
// backend.WorkflowRegistry exposes no count primitive, so the only exact source
// reachable from this layer is a namespace-scoped id enumeration with Limit 0,
// which the interface documents as "unbounded", not as "none". That makes
// `total` EXACT: spec §3.3 defines it as the size of the collection after
// filtering, and a truncated or "at least" value would silently understate the
// collection a UI paginates over — a list that claims fewer pages than exist
// is a wrong list, not a conservative one.
//
// The cost is worth naming rather than hiding: this is O(N) ids, where N is
// the number of workflows in the caller's OWN namespace, and it runs on every
// page request. It grants no extra data access (the caller can already page
// through exactly that set, and the registry refuses any namespace but the one
// passed), but it does turn one request into O(N) index work — the 200 cap
// bounds the response, not this read. A count primitive on the registry (ZCARD
// over the distributed index, a map length in the in-memory one) removes the
// amplification entirely; that is a backend/ change and is deliberately left
// as a follow-up rather than reached for from here.
func countWorkflows(ctx context.Context, registry backend.WorkflowRegistry, ns namespace.Namespace) (int, error) {
	ids, err := registry.ListWorkflows(ctx, ns, backend.WorkflowListOptions{})
	if err != nil {
		return 0, err
	}
	return len(ids), nil
}

// workflowListItems reads one summary row per id.
//
// An id that no longer resolves is skipped, not fatal: ListWorkflows confirms
// liveness per page, but the two reads are not one transaction, so a record
// removed between them is a legitimate race. A row the caller can no longer
// read is worse than a page that is one row shorter (and total, read
// separately, may then exceed offset+len(list) by exactly that race).
//
// A record whose namespace is not the caller's is also skipped. That is the
// same defense handleGetWorkflow applies — a foreign record is reported as
// absent, never projected — and it is load-bearing on the in-memory registry,
// whose GetWorkflow ignores the context namespace; on the distributed registry
// a foreign id cannot be resolved in the caller's slot at all, so both
// backends answer "not yours" the same way.
func (m *workflowControlModule) workflowListItems(ctx context.Context, registry backend.WorkflowRegistry, ns namespace.Namespace, ids []types.WorkflowID) ([]workflowListItem, error) {
	items := make([]workflowListItem, 0, len(ids))
	for _, id := range ids {
		rec, err := registry.GetWorkflow(ctx, id)
		if err != nil {
			if errors.Is(err, backend.ErrWorkflowNotFound) {
				continue
			}
			return nil, err
		}
		if rec.Namespace != "" && ns != "" && rec.Namespace != string(ns) {
			continue
		}
		items = append(items, workflowListItem{
			ID:               string(rec.ID),
			Name:             rec.Name,
			Version:          rec.Version,
			DefinitionHash:   rec.DefinitionHash,
			RegistryRevision: rec.RegistryRevision,
		})
	}
	return items, nil
}

// executionListItem is one row of a GET /v1/executions page. It is a summary
// projection of store.ExecutionRecord, not the record itself.
//
// The heavy columns are deliberately absent. store.ExecutionRecord carries
// workflow_def (the full definition), params, and runtime — three JSON blobs
// whose size is proportional to the workflow, not to the row. A page of them is
// O(page × workflow size) on the wire for a table that renders none of it; a
// client that needs one execution's definition reads it through
// GET /v1/executions/{id}. What a row actually needs is the identity
// (execution_id), the tenant it belongs to (namespace), what it ran
// (workflow_name), where it is (status), how it ended (error), and when it was
// created / last touched.
//
// workflow_name and not a workflow id: the executions table stores no workflow
// id / key / hash column, so a name is the only workflow identity a row can
// honestly project. That is also why no workflow filter is offered — see
// executionListFilterFrom.
type executionListItem struct {
	ExecutionID  string                `json:"execution_id"`
	Namespace    string                `json:"namespace"`
	WorkflowName string                `json:"workflow_name"`
	Status       types.ExecutionStatus `json:"status"`
	Error        string                `json:"error"`
	CreatedAt    time.Time             `json:"created_at"`
	UpdatedAt    time.Time             `json:"updated_at"`
}

// executionListFilterFrom parses and validates everything about a list request
// that is a business input rather than a page number: the status filter and the
// created_after / created_before bounds.
//
// Only these three filters exist, and that is a data-model fact, not a
// conservative default. The store's doc on ListExecutions spells out the two
// rejections at column level: the executions table has no workflow id / key /
// hash column (only workflow_name, which is a display string, not an identity —
// two namespaces can hold the same name and it is not immutable across a
// re-registration), and runner_id is not a column of that table at all (it
// lives in the enrollment/identity tables and in per-node lease columns, none of
// which is a per-execution projection of "who ran this"). Advertising either
// parameter would promise a narrowing the store cannot perform — an
// unrecognized query parameter is silently ignored, so the caller would get the
// whole namespace back and read it as a filtered page.
//
// Unlike page/page_size — whose malformed values degrade to defaults because
// they come straight off a frontend table component and can only ever make the
// page smaller (spec §3.3, pageParams) — a malformed FILTER is a 400. The
// distinction is deliberate and load-bearing in both directions:
//
//   - an unknown status served as an empty page is indistinguishable from "no
//     executions have that status", which is exactly what store.ExecutionFilter
//     refuses to do (ExecutionFilter.Validate);
//   - a created_after that failed to parse and silently became "unbounded"
//     widens the result set, and widening a tenant-scoped enumeration is the
//     direction that costs data, not convenience.
//
// ok=false means the handler has already written the failure response.
//
// The returned filter is passed straight to the store, which re-validates the
// status itself: this function is the HTTP-layer mapping, not a replacement for
// the store's own refusal.
func executionListFilterFrom(w http.ResponseWriter, r *http.Request) (store.ExecutionFilter, bool) {
	q := r.URL.Query()

	var filter store.ExecutionFilter
	if raw := q.Get("status"); raw != "" {
		status := types.ExecutionStatus(raw)
		if !knownExecutionListStatus(status) {
			writeFail(w, r, http.StatusBadRequest, "execution_status_invalid",
				"status must be one of pending, running, success, failed, canceling, canceled, timeout")
			return store.ExecutionFilter{}, false
		}
		filter.Status = status
	}

	createdAfter, ok := parseExecutionListTime(w, r, q.Get("created_after"), "created_after")
	if !ok {
		return store.ExecutionFilter{}, false
	}
	filter.CreatedAfter = createdAfter

	createdBefore, ok := parseExecutionListTime(w, r, q.Get("created_before"), "created_before")
	if !ok {
		return store.ExecutionFilter{}, false
	}
	filter.CreatedBefore = createdBefore

	// The bounds are exclusive at both ends (ExecutionFilter), so an empty
	// window is a legitimate request that honestly matches nothing. It is NOT a
	// 400: both endpoints are the caller's own values and the answer leaks
	// nothing. It is worth naming because the empty page it produces is
	// indistinguishable from "no executions in this window", and that is the
	// truth in both cases.
	return filter, true
}

// knownExecutionListStatus reports whether s is one of the lifecycle statuses
// an execution row can hold. It mirrors store's own known-status set (that
// function is unexported) and exists so the refusal is a 400 mapped to a stable
// snake_case code at the HTTP layer instead of a 500 reached through whatever
// error text the store happens to return.
func knownExecutionListStatus(s types.ExecutionStatus) bool {
	switch s {
	case types.ExecutionStatusPending,
		types.ExecutionStatusRunning,
		types.ExecutionStatusSuccess,
		types.ExecutionStatusFailed,
		types.ExecutionStatusCanceling,
		types.ExecutionStatusCanceled,
		types.ExecutionStatusTimeout:
		return true
	}
	return false
}

// parseExecutionListTime parses one RFC3339 date-time bound. An absent or empty
// parameter is the zero time, which ExecutionFilter documents as "unbounded on
// that side"; anything else must parse, or the request is a 400. See
// executionListFilterFrom for why a bad bound is refused rather than ignored.
func parseExecutionListTime(w http.ResponseWriter, r *http.Request, raw, param string) (time.Time, bool) {
	if raw == "" {
		return time.Time{}, true
	}
	// RFC3339Nano accepts everything RFC3339 does, plus a fractional second —
	// the form a client naturally produces from a Go time.Time. Accepting both
	// is not a relaxation: the instants compared are identical either way.
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		writeFail(w, r, http.StatusBadRequest, "execution_list_time_invalid",
			param+" must be an RFC3339 date-time")
		return time.Time{}, false
	}
	return t, true
}

// handleListExecutions serves GET /v1/executions (spec §3.3 page list). It is
// the second of the two endpoints API-SPECIFICATION.md §9.6 recorded as
// unwired, and the first one whose blocker was a missing COLUMN rather than a
// missing index: the executions table now carries namespace, and the store's
// ListExecutions/CountExecutions were landed for exactly this caller.
//
// Namespace: resolved server-side from the authenticated principal via
// namespace.FromContext (injected by the authz wrapper), exactly as
// handleListWorkflows and handleInspectByID do. There is deliberately no
// `namespace` query parameter and no body — a caller can only ever list its own
// namespace. That is the whole security property here: an unscoped version of
// this route is a cross-tenant enumeration endpoint. Three independent layers
// hold it, in this order:
//
//  1. the scope check refuses a bad scope with 403 execution_namespace_invalid
//     (and the store would refuse it again);
//  2. ns is passed to both store calls as a required explicit argument, and the
//     store fails closed on an empty or malformed one rather than widening to
//     every namespace;
//  3. rows whose namespace is the unattributed sentinel "" match nothing — they
//     are unreachable from every scope × filter combination, deliberately, so a
//     pre-migration row is never shown to a tenant that merely might own it.
//
// Pagination: offset, 1-based, through the shared pageParams helper (default
// 20, hard cap 200). The cap is a SECURITY control per org policy §2 — a
// sensitive-data enumeration endpoint may not permit full-table traversal — so
// an oversized page_size is CLAMPED to 200, not rejected. Cursor pagination is
// reserved for machine scans (spec §3.3); this is a page list, so it must not
// grow a second pagination dialect.
//
// Ordering is the store's, not this handler's: created_at DESC, id DESC. The id
// tiebreak is what makes the order TOTAL, so offset paging cannot duplicate or
// skip a row when several executions share a created_at tick.
//
// Envelope: writeList, i.e. the mandated {list,total} collection shape — never
// a bare array. `total` comes from CountExecutions with the SAME namespace and
// the SAME filter as the page, so the two cannot disagree about which rows are
// in scope.
func (m *workflowControlModule) handleListExecutions(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	st := m.executions
	if st == nil {
		if m.log != nil {
			m.log.Error("list_executions_no_store")
		}
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	ns := namespace.FromContext(r.Context())
	// Fail closed on a scope that cannot be enforced. FromContext returns the
	// Default namespace for an absent one, so reaching here with an empty or
	// malformed value means the caller's identity could not be resolved into a
	// tenant at all — a 403, not a fallback to "all namespaces", and never a
	// 200 with an empty list. The store would refuse the same value; doing it
	// here keeps the refusal a stable business code instead of a 500.
	if err := store.ValidateNamespaceScope(ns); err != nil {
		if m.log != nil {
			m.log.Error("list_executions_namespace_invalid", "err", err)
		}
		writeFail(w, r, http.StatusForbidden, "execution_namespace_invalid", "forbidden")
		return
	}
	page, pageSize := pageParams(r)
	filter, ok := executionListFilterFrom(w, r)
	if !ok {
		return
	}

	// fail is the single failure exit below. The store error is logged and
	// never reaches the body: spec §3.5 requires a generic message on a 500.
	fail := func(event string, err error) {
		if m.log != nil {
			m.log.Error(event, "err", err)
		}
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
	}

	// One page, sliced by the store itself (ListOptions is passed through and
	// normalized there), so the handler never re-implements paging over an
	// unbounded slice.
	recs, err := st.ListExecutions(r.Context(), ns, filter, store.ListOptions{
		Offset: pageOffset(page, pageSize),
		Limit:  pageSize,
	})
	if err != nil {
		fail("list_executions_failed", err)
		return
	}
	total, err := st.CountExecutions(r.Context(), ns, filter)
	if err != nil {
		fail("count_executions_failed", err)
		return
	}
	items := make([]executionListItem, 0, len(recs))
	for _, rec := range recs {
		// The store already scoped this page by exact namespace equality. The
		// re-check is defense in depth against a store whose scope handling
		// regresses: this endpoint is the one place where such a regression
		// becomes a cross-tenant disclosure rather than a 404, so it is worth
		// the redundant comparison. A foreign row is skipped, not fatal.
		if rec.Namespace != string(ns) {
			if m.log != nil {
				m.log.Error("list_executions_row_namespace_mismatch",
					"execution_id", string(rec.ExecutionID))
			}
			continue
		}
		items = append(items, executionListItem{
			ExecutionID:  string(rec.ExecutionID),
			Namespace:    rec.Namespace,
			WorkflowName: rec.WorkflowName,
			Status:       rec.Status,
			Error:        rec.Error,
			CreatedAt:    rec.CreatedAt,
			UpdatedAt:    rec.UpdatedAt,
		})
	}
	writeList(w, r, items, executionListTotal(total))
}

// executionListTotal narrows the store's int64 count to the envelope's int.
// The envelope's `total` is an int (spec §3.3), so the conversion is required,
// not optional. The upper guard is a no-op on every platform this repository
// builds for (64-bit: int and int64 have the same width, so the condition is
// never taken) and is kept so the line is self-evidently not a silent truncation
// if a 32-bit target is ever added.
func executionListTotal(total int64) int {
	if maxInt := int64(^uint(0) >> 1); maxInt > 0 && total > maxInt {
		return int(maxInt)
	}
	if total < 0 {
		return 0
	}
	return int(total)
}

// handleGetWorkflow serves GET /v1/workflows/{id} (spec §7 + Addition 1): it
// returns the stored record's definition. The namespace comes from the
// authenticated principal via namespace.FromContext (injected by the authz
// wrapper), never from the request body (spec §6.2); a cross-namespace id
// resolves to not-found → 404, never leaking existence.
func (m *workflowControlModule) handleGetWorkflow(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	id := types.WorkflowID(r.PathValue("id"))
	registry := m.registry()
	if registry == nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	rec, err := registry.GetWorkflow(r.Context(), id)
	if err != nil {
		if errors.Is(err, backend.ErrWorkflowNotFound) {
			writeFail(w, r, http.StatusNotFound, "workflow_not_found", "workflow not found")
			return
		}
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	// The namespace-scoped read is the authoritative IDOR defense: a
	// cross-namespace id resolves to a record whose Namespace does not match
	// the principal's, so it is reported as not-found rather than returned.
	ns := namespace.FromContext(r.Context())
	if rec.Namespace != "" && ns != "" && rec.Namespace != string(ns) {
		writeFail(w, r, http.StatusNotFound, "workflow_not_found", "workflow not found")
		return
	}
	writeData(w, r, http.StatusOK, rec.Definition)
}

// handleReplaceWorkflow serves PUT /v1/workflows/{id} (spec §7 + Addition 1):
// a full update of the resource identified by the path id. The path id is the
// authoritative target; the body may omit id, but if it supplies one, it must
// match the path. The replacement definition's name/version form the new key
// (with the principal's namespace, never the body's — spec §6.2), so a rename
// or version change is allowed only when that destination key is not already
// occupied by a different workflow.
func (m *workflowControlModule) handleReplaceWorkflow(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPut) {
		return
	}
	id := types.WorkflowID(r.PathValue("id"))
	if id == "" {
		writeFail(w, r, http.StatusNotFound, "workflow_not_found", "workflow not found")
		return
	}
	var def types.WorkflowDef
	if !decodeJSON(w, r, &def) {
		return
	}
	if def.ID != "" && def.ID != string(id) {
		writeFail(w, r, http.StatusBadRequest, "workflow_id_mismatch", "workflow id does not match path")
		return
	}
	ns := namespace.FromContext(r.Context())
	mutationID := sanitizeRequestID(r.Header.Get("X-Request-Id"))
	if mutationID != "" {
		mutationID = "http:" + mutationID
	}
	replacedID, warnings, err := m.replaceWorkflowByID(r.Context(), ns, id, &def, mutationID)
	if err != nil {
		var compileErr *WorkflowCompileError
		if errors.As(err, &compileErr) {
			writeFail(w, r, http.StatusBadRequest, "workflow_compile_failed", compileErr.Unwrap().Error())
			return
		}
		if errors.Is(err, backend.ErrWorkflowNotFound) {
			writeFail(w, r, http.StatusNotFound, "workflow_not_found", "workflow not found")
			return
		}
		if errors.Is(err, backend.ErrWorkflowConflict) {
			writeFail(w, r, http.StatusConflict, "workflow_conflict", "workflow definition conflicts with an existing registration")
			return
		}
		if errors.Is(err, errWorkflowIDMismatch) {
			writeFail(w, r, http.StatusBadRequest, "workflow_id_mismatch", "workflow id does not match path")
			return
		}
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeData(w, r, http.StatusOK, registerWorkflowResponse{WorkflowID: replacedID, Warnings: warnings})
}

// handleExecuteWorkflowByID serves POST /v1/workflows/{id}/execute (spec §7):
// run an already-registered workflow by id. It resolves the stored compiled
// graph from the registry and submits it. The id is read via the mux {id}
// pattern; the namespace comes from the authenticated principal (spec §6.2),
// and a cross-namespace id resolves to not-found → 404.
func (m *workflowControlModule) handleExecuteWorkflowByID(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	id := types.WorkflowID(r.PathValue("id"))
	// Body is optional: Entry/Input/Params may all be absent.
	var req executeRegisteredRequest
	if r.ContentLength != 0 {
		if !decodeJSON(w, r, &req) {
			return
		}
	} else {
		_ = r.Body.Close()
	}
	registry := m.registry()
	if registry == nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	rec, err := registry.GetWorkflow(r.Context(), id)
	if err != nil {
		if errors.Is(err, backend.ErrWorkflowNotFound) {
			writeFail(w, r, http.StatusNotFound, "workflow_not_found", "workflow not found")
			return
		}
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	ns := namespace.FromContext(r.Context())
	if rec.Namespace != "" && ns != "" && rec.Namespace != string(ns) {
		writeFail(w, r, http.StatusNotFound, "workflow_not_found", "workflow not found")
		return
	}
	// Attach the stored definition so the durable SQL execution projection can
	// persist workflow_def (NOT NULL) — mirroring the inline-execute path.
	ctx := engine.WithWorkflowDef(r.Context(), rec.Definition)
	ctx = engine.WithTraceCarrier(ctx, tracing.InjectCarrier(ctx))
	var execID types.ExecutionID
	if req.Entry != "" {
		execID, err = m.eng.Invoke(ctx, rec.Graph, req.Entry, req.Input)
		if err != nil {
			if errors.Is(err, engine.ErrEntryNotFound) {
				writeFail(w, r, http.StatusBadRequest, "workflow_entry_not_found", err.Error())
				return
			}
			writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
	} else {
		execID, err = m.eng.Submit(ctx, rec.Graph, req.Params)
		if err != nil {
			writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
	}
	writeData(w, r, http.StatusOK, executeWorkflowResponse{ExecutionID: execID})
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
		if err := mgr.RemoveWorkflowRevision(ctx, ns, toDeactivate.ID, toDeactivate.Version, toDeactivate.RegistryRevision, toDeactivate.Graph); err != nil {
			if m.log != nil {
				m.log.Error("deregister_workflow_clear_activations_failed", "err", err)
			}
			return err
		}
	}
	// Remove the registry record. If removal reports a failure after the desired
	// activations were cleared, read the record back before compensating. Some
	// registries can fail before mutation while others can durably remove the
	// record and then fail during follow-up cleanup. Restoring activations in the
	// latter case would create an orphan, so only restore when the exact revision
	// fetched above is still authoritative.
	if err := registry.RemoveWorkflow(ctx, id); err != nil {
		if !errors.Is(err, backend.ErrWorkflowNotFound) && m.log != nil {
			m.log.Error("deregister_workflow_failed", "err", err)
		}
		if toDeactivate != nil {
			current, lookupErr := registry.GetWorkflow(ctx, id)
			switch {
			case lookupErr == nil && sameWorkflowRevision(current, *toDeactivate):
				if restoreErr := m.deriveWorkflowActivations(ctx, ns, *toDeactivate); restoreErr != nil && m.log != nil {
					m.log.Error("deregister_workflow_restore_activations_failed", "workflow_id", string(id), "original_err", err, "err", restoreErr)
				}
			case lookupErr != nil && !errors.Is(lookupErr, backend.ErrWorkflowNotFound) && m.log != nil:
				m.log.Error("deregister_workflow_restore_lookup_failed", "workflow_id", string(id), "original_err", err, "err", lookupErr)
			}
		}
		return err
	}
	return nil
}

// sameWorkflowRevision identifies the persisted workflow revision whose
// activation projection was cleared. A matching ID alone is insufficient:
// another writer may have replaced the record between RemoveWorkflow and the
// compensating read, in which case re-projecting the old graph would overwrite
// the new workflow's desired activation state.
func sameWorkflowRevision(a, b backend.WorkflowRecord) bool {
	return a.ID == b.ID &&
		a.Key == b.Key &&
		a.Namespace == b.Namespace &&
		a.Name == b.Name &&
		a.Version == b.Version &&
		a.DefinitionHash == b.DefinitionHash &&
		a.RegistryRevision == b.RegistryRevision
}

// replaceWorkflow registers def, and if a DIFFERENT definition already occupies
// its (namespace, name, version) key, atomically supersedes it. The embedded API
// deliberately gives a changed definition a new ID; HTTP PUT, by contrast,
// preserves its path ID in replaceWorkflowByID below.
func (m *workflowControlModule) replaceWorkflow(ctx context.Context, ns namespace.Namespace, def *types.WorkflowDef) (types.WorkflowID, []string, error) {
	registry := m.registry()
	if registry == nil {
		return "", nil, errors.New("apiserver: no workflow registry configured")
	}
	replacement, warnings, err := m.buildWorkflowRecord(ns, "", def)
	if err != nil {
		return "", nil, err
	}
	ctx = namespace.WithNamespace(ctx, ns)
	existing, err := registry.GetWorkflowByKey(ctx, replacement.Key)
	if errors.Is(err, backend.ErrWorkflowNotFound) {
		return m.addWorkflowRecord(ctx, ns, registry, replacement, warnings)
	}
	if err != nil {
		return "", nil, err
	}
	if existing.DefinitionHash == replacement.DefinitionHash {
		return m.addWorkflowRecord(ctx, ns, registry, replacement, warnings)
	}

	// A replacement ID must be fixed before the CAS so response-loss retries use
	// the same semantic request fingerprint instead of allocating a second ID.
	replacement.ID = types.WorkflowID(uuid.NewString())
	return m.compareAndReplaceWorkflow(ctx, ns, registry, existing, replacement, "embedded:"+uuid.NewString(), warnings)
}

// replaceWorkflowByID implements HTTP PUT /v1/workflows/{id}. The path ID is
// authoritative; name/version may rename the record only when the destination
// key is free. All conflict checks and index changes occur in the registry's
// single atomic compare-and-swap rather than a racy lookup/remove/add sequence.
func (m *workflowControlModule) replaceWorkflowByID(ctx context.Context, ns namespace.Namespace, id types.WorkflowID, def *types.WorkflowDef, mutationID string) (types.WorkflowID, []string, error) {
	if id == "" {
		return "", nil, backend.ErrWorkflowNotFound
	}
	registry := m.registry()
	if registry == nil {
		if m.log != nil {
			m.log.Error("replace_workflow_no_registry")
		}
		return "", nil, errors.New("apiserver: no workflow registry configured")
	}

	ctx = namespace.WithNamespace(ctx, ns)
	existing, err := registry.GetWorkflow(ctx, id)
	if err != nil {
		if !errors.Is(err, backend.ErrWorkflowNotFound) && m.log != nil {
			m.log.Error("replace_workflow_lookup_failed", "workflow_id", string(id), "err", err)
		}
		return "", nil, err
	}
	if existing.Namespace != "" && ns != "" && existing.Namespace != string(ns) {
		return "", nil, backend.ErrWorkflowNotFound
	}
	if def.ID != "" && def.ID != string(id) {
		return "", nil, errWorkflowIDMismatch
	}
	def.ID = string(id)
	def.Namespace = string(ns)
	replacement, warnings, err := m.buildWorkflowRecord(ns, id, def)
	if err != nil {
		return "", nil, err
	}
	if mutationID == "" {
		mutationID = "http:" + uuid.NewString()
	}
	return m.compareAndReplaceWorkflow(ctx, ns, registry, existing, replacement, mutationID, warnings)
}

func (m *workflowControlModule) compareAndReplaceWorkflow(
	ctx context.Context,
	ns namespace.Namespace,
	registry backend.WorkflowRegistry,
	existing backend.WorkflowRecord,
	replacement backend.WorkflowRecord,
	mutationID string,
	warnings []string,
) (types.WorkflowID, []string, error) {
	durableRegistry, ok := registry.(backend.DurableWorkflowReplaceCapability)
	if !ok || m.workflowActivationProjectionWorker() == nil {
		if m.log != nil {
			m.log.Error("replace_workflow_durable_projection_capability_missing")
		}
		return "", nil, backend.ErrWorkflowReplaceUnsupported
	}
	request := backend.WorkflowReplaceRequest{
		MutationID:  mutationID,
		Expected:    backend.RevisionOfWorkflow(existing),
		Replacement: replacement,
	}
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}
	result, err := durableRegistry.CompareAndReplaceWorkflow(ctx, request)
	if errors.Is(err, backend.ErrWorkflowMutationIndeterminate) {
		// A timeout or disconnect may have happened after commit. Resolve the
		// operation ledger with the same MutationID on a context detached from the
		// request; compensating or issuing a new mutation would be unsafe.
		resolveCtx, cancel := context.WithTimeout(namespace.WithNamespace(context.WithoutCancel(ctx), ns), workflowProjectionTimeout)
		defer cancel()
		for attempt := 0; attempt < 3 && err != nil; attempt++ {
			result, err = durableRegistry.CompareAndReplaceWorkflow(resolveCtx, request)
			if err != nil && !errors.Is(err, backend.ErrWorkflowMutationIndeterminate) {
				break
			}
		}
	}
	if err != nil {
		if m.log != nil && !errors.Is(err, backend.ErrWorkflowConflict) {
			m.log.Error("replace_workflow_atomic_failed", "workflow_id", string(existing.ID), "err", err)
		}
		return "", nil, err
	}

	// The registry commit, operation ledger, and projection intent are now one
	// authority transaction. Drive that intent synchronously for low latency,
	// but keep it pending when projection or ack fails so the control-plane
	// recovery worker can finish after a crash. Never compensate a committed CAS.
	projectionCtx, cancel := context.WithTimeout(namespace.WithNamespace(context.WithoutCancel(ctx), ns), workflowProjectionTimeout)
	defer cancel()
	applied, err := m.workflowActivationProjectionWorker().ProjectPending(projectionCtx, ns, request.MutationID, 3)
	if err == nil && !applied {
		err = errors.New("workflow activation projection is already in progress")
	}
	if err != nil {
		if m.log != nil {
			m.log.Error("replace_workflow_project_activations_failed", "workflow_id", string(result.Current.ID), "registry_revision", result.Current.RegistryRevision, "err", err)
		}
		return "", nil, err
	}
	return result.Current.ID, warnings, nil
}

const workflowProjectionTimeout = 10 * time.Second

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
func (m *workflowControlModule) workflowActivationProjectionWorker() *control.WorkflowActivationProjectionWorker {
	if m.cp == nil {
		return nil
	}
	return m.cp.WorkflowActivationProjectionWorker()
}

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
	if !decodeSeedJSON(w, r, &req) {
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
		ReplicaIndex:    req.ReplicaIndex,
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

// handleInspect serves GET /v1/executions/{id} (spec §7). It delegates to
// inspectExecution — the single shared inspect implementation also used by the
// management route (spec §7.2) — so the two routes return byte-identical bodies
// for the same execution. Two implementations were a drift source and had
// already diverged once.
func (m *workflowControlModule) handleInspect(w http.ResponseWriter, r *http.Request, id types.ExecutionID) {
	inspectExecution(w, r, m.eng, id)
}

// inspectExecution is the single inspect implementation shared by the
// executions-family route (GET /v1/executions/{id}, OpExecutionRead) and the
// management route (GET /v1/management/executions/{id}, OpManagementRead) per
// spec §7.2. Both Ops stay distinct so an ops token and a business token can be
// granted separately; only the implementation is merged.
//
// The detail is returned in the response envelope (spec §3) via writeData. A
// not-found engine error maps to 404 execution_not_found (writeExecEngineFail);
// an unclassified error collapses to 500 internal_error — never leaking Redis
// text, internal paths, or node output (spec §3.5).
func inspectExecution(w http.ResponseWriter, r *http.Request, eng control.EngineFacade, id types.ExecutionID) {
	detail, err := eng.Inspect(r.Context(), id)
	if err != nil {
		writeExecEngineFail(w, r, err)
		return
	}
	writeData(w, r, http.StatusOK, detail)
}

// handleSignal serves POST /v1/executions/{id}/signals (spec §7). A request
// without a name is 400 signal_invalid (spec §3.2); a not-found execution is
// 404 execution_not_found; everything else is 500 internal_error. Success is
// enveloped via writeData. The signal payload is delivered to the engine, but
// never appears in message/data — message carries only the stable code's text
// (spec §3.5).
func (m *workflowControlModule) handleSignal(w http.ResponseWriter, r *http.Request, id types.ExecutionID) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req signalRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Name == "" {
		writeFail(w, r, http.StatusBadRequest, "signal_invalid", "signal name is required")
		return
	}
	if err := m.eng.DeliverSignal(r.Context(), id, req.Name, req.Data); err != nil {
		writeExecEngineFail(w, r, err)
		return
	}
	writeData(w, r, http.StatusOK, map[string]bool{"accepted": true})
}

// handleCancel serves POST /v1/executions/{id}/cancel (spec §7). A not-found
// execution is 404 execution_not_found; unclassified failures collapse to 500
// internal_error. Success is enveloped via writeData.
func (m *workflowControlModule) handleCancel(w http.ResponseWriter, r *http.Request, id types.ExecutionID) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	if err := m.eng.Cancel(r.Context(), id); err != nil {
		writeExecEngineFail(w, r, err)
		return
	}
	writeData(w, r, http.StatusOK, map[string]bool{"accepted": true})
}

// handleRevokeSignal serves DELETE /v1/executions/{id}/signals/{name}
// (spec §7 + §9.1). The §9.1 migration moved the signal name from the request
// body into the path segment, so the handler reads it via r.PathValue("name")
// and the body is unused — the old body carried a signalRequest whose Data
// field RevokeSignal never consumed, so nothing is silently lost. The authz
// resolver (execSignalResolver) reads the same {name} so the audit-target
// resource string mirrors the path (spec §6).
//
// A consumed-or-not-found signal is 409 signal_consumed (stable snake_case,
// spec §3.2); a not-found execution is 404 execution_not_found; unclassified
// failures collapse to 500 internal_error. Success is enveloped via
// writeData.
func (m *workflowControlModule) handleRevokeSignal(w http.ResponseWriter, r *http.Request, id types.ExecutionID) {
	if !requireMethod(w, r, http.MethodDelete) {
		return
	}
	// The {name} segment is read from the path the mux matched. A path with an
	// empty name (/signals/) does not match this method-qualified pattern and
	// falls to the /v1/executions/{id}/ 404 catch, so name is non-empty here;
	// the guard is defensive against a future registration change.
	name := r.PathValue("name")
	if name == "" {
		writeFail(w, r, http.StatusBadRequest, "signal_invalid", "signal name is required")
		return
	}
	if err := m.eng.RevokeSignal(r.Context(), id, name); err != nil {
		if errors.Is(err, engine.ErrSignalConsumed) {
			writeFail(w, r, http.StatusConflict, "signal_consumed", "signal already consumed or not found")
			return
		}
		writeExecEngineFail(w, r, err)
		return
	}
	writeData(w, r, http.StatusOK, map[string]bool{"revoked": true})
}

// handleWait long-polls an execution until it reaches a terminal state or the
// timeout elapses. It uses http.ResponseController to extend the connection's
// write deadline beyond the server's default WriteTimeout so a long poll does
// not get cut off mid-flight. The poll timeout is capped at 10 minutes. Both
// the terminal-detail success (200) and the timeout response (202) are
// enveloped via writeData (spec §3); a not-found execution is 404
// execution_not_found, unclassified failures 500 internal_error.
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
			writeExecEngineFail(w, r, err)
			return
		}
		if types.IsTerminalExecutionStatus(detail.Status) {
			writeData(w, r, http.StatusOK, detail)
			return
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			writeData(w, r, http.StatusAccepted, waitTimeoutResponse{
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
			writeData(w, r, http.StatusAccepted, waitTimeoutResponse{
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

// decodeSeedJSON decodes the request body for the entry-seed route
// (POST /v1/executions). It is a DEDICATED bare decoder, deliberately NOT the
// shared decodeJSON helper: entry-seed is a runner-protocol-face endpoint whose
// response shape must stay un-enveloped (spec §0.1 + §8.2), and the shared
// helper now writes the user-face envelope via writeFail. Routing the seed
// through the shared helper would slip envelope keys (success/code/trace_id)
// into the seed's 400 body — harmless to the 409 offset-safety discriminator
// (which only reads 409 bodies) but a violation of §0.1 and an inconsistent
// shape next to the seed's other bare errorResponse bodies
// (stale_generation / workflow_unknown / internal server error). A malformed
// seed body yields the same bare errorResponse shape as those, via writeJSON.
func decodeSeedJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	defer func() { _ = r.Body.Close() }()
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON"})
		return false
	}
	return true
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	defer func() { _ = r.Body.Close() }()
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		writeFail(w, r, http.StatusBadRequest, "bad_request", "invalid JSON")
		return false
	}
	return true
}

func requireMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method == method {
		return true
	}
	writeFail(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
	return false
}

// writeExecEngineFail is the enveloped engine-error mapper for the executions
// family: it maps typed not-found errors to 404 execution_not_found and
// everything else to 500 internal_error, via writeFail (so trace_id is stamped
// and X-Request-Id is echoed — spec §3/§5.2). It carries only execution ID /
// node names in messages, never node output or credentials (spec §3.5).
func writeExecEngineFail(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, engine.ErrExecutionInactive) ||
		errors.Is(err, engine.ErrExecutionNotFound) ||
		errors.Is(err, store.ErrNotFound) {
		writeFail(w, r, http.StatusNotFound, "execution_not_found", "execution not found")
		return
	}
	writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
}

// writeJSON writes a bare JSON body (no envelope). entry-seed (POST
// /v1/executions) is the one caller that must keep using it permanently — it
// is a runner-protocol-face endpoint whose 409 body shape is a load-bearing
// offset-safety contract (spec §0.1 + §8.2). The other remaining callers are
// /healthz and /readyz (spec §7: not enveloped, load-balancer contract) and
// the entry-seed errorResponse bodies (§0.1). User-face success/failure
// surfaces use writeData/writeFail via the envelope (§3).
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
