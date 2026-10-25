package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/workflowhash"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/observability/metrics"
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
	// registrationMetrics counts every registration attempt this module
	// handles: the HTTP routes and APIServer.RegisterWorkflow /
	// ReplaceWorkflow, which an embedded host reaches in-process. Both entry
	// points funnel through registerWorkflow/replaceWorkflow below, so the
	// counter has one emit point and the two cannot report different numbers.
	// Injected post-construction by APIServer.New (Config.Metrics), the same
	// shape the management module's metrics field uses; a nil Metrics counts
	// nothing, which is the dev default.
	registrationMetrics metrics.WorkflowRegistrationMetrics
	// paramValidation is Config.ParamValidation (defaulted to warn): what
	// registration and inline execution do with ParamSpec issues. A module
	// built without APIServer.New (unit tests) has "" and behaves as warn.
	paramValidation types.ParamValidationMode
	// paramValidationMetrics counts nodes skipped for an unknown type.
	paramValidationMetrics metrics.ParamValidationMetrics
	// descriptorLookup resolves node Descriptors for ParamSpec validation;
	// nil means execution.RegistryDescriptorLookup.
	descriptorLookup execution.DescriptorLookup
	// nodeDescriptors feeds GET /v1/node-types; nil means
	// registry.Descriptors (see module_node_types.go).
	nodeDescriptors nodeDescriptorSource
	// runnerNodeTypes feeds the runner-reported part of a namespace-scoped
	// GET /v1/node-types; nil means runners contribute nothing.
	// newWorkflowControlModule sets it to the control plane's live runner
	// node types, so every APIServer (embedded or cmd/server) serves them.
	runnerNodeTypes runnerNodeTypeSource
	// runnerNodeTypesNow is the liveness clock of the constructor-wired
	// runnerNodeTypes; nil means time.Now. A test seam only.
	runnerNodeTypesNow func() time.Time
	// webhookRoutesHosted reports whether THIS process serves the server-hosted
	// webhook entry route, i.e. whether a webhook entry unit of a definition
	// registered here has anywhere to run. It is the same condition the module
	// wiring uses to mount the route (see APIServer.New), injected rather than
	// recomputed so registration and route existence cannot disagree: when it is
	// true a server-hosted webhook entry must declare its signing secret, and
	// when it is false the definition keeps working on the embedded engine's
	// WebhookRuntime with no secret at all.
	webhookRoutesHosted bool
	// schedules serves the schedule routes (see module_schedules.go). Nil means
	// no overlay store is configured: the routes are still registered — an
	// operator gets "not configured on this server" rather than a 404 that reads
	// as an old server — and every one of them refuses.
	//
	// It is injected post-construction by APIServer.New (Config.Schedules), the
	// same shape the executions store uses, because a constructor parameter
	// would touch every test call site for no behavioral reason.
	schedules *control.ScheduleManager
	// overviewOnce/overviewCache hold GET /v1/overview's 30-second response
	// cache. Lazily initialized so a module built directly in a test (instead of
	// through newWorkflowControlModule) still works; the once keeps concurrent
	// first requests from racing two caches into existence.
	overviewOnce  sync.Once
	overviewCache *overviewCache
}

// overviewCacheFor returns the module's overview cache, creating it on first
// use.
func (m *workflowControlModule) overviewCacheFor() *overviewCache {
	m.overviewOnce.Do(func() { m.overviewCache = newOverviewCache() })
	return m.overviewCache
}

func newWorkflowControlModule(cp *control.ControlPlane, auth WorkflowAuthenticator, log engine.Logger, tracer tracing.Tracer) *workflowControlModule {
	if tracer == nil {
		tracer = tracing.NoopTracer{}
	}
	m := &workflowControlModule{cp: cp, eng: cp.Engine(), auth: auth, log: log, tracer: tracer}
	m.runnerNodeTypes = func(ctx context.Context, ns namespace.Namespace) ([]control.AggregatedDescriptor, error) {
		now := time.Now
		if m.runnerNodeTypesNow != nil {
			now = m.runnerNodeTypesNow
		}
		return cp.LiveRunnerNodeTypes(ctx, ns, now())
	}
	return m
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
		class, admitted := admissionRouteClass(op)
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
			if admitted {
				release, ok := m.admission.enter(w, r, class)
				if !ok {
					return
				}
				defer release()
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
	// Node-type schemas (editor forms) are mounted in both branches, like
	// GET /v1/workflows; the server registry they project is process-global,
	// not namespaced. This branch has no principal to authorize a namespace
	// against, so a ?namespace= other than the request's own (Default) is
	// refused inside the handler (see nodeTypesScope).
	mux.HandleFunc("GET "+PathNodeTypes, wrap("read_node_types", m.handleListNodeTypes))
	mux.HandleFunc("GET "+PathNodeTypeByType, wrap("read_node_types", m.handleGetNodeType))
	mux.HandleFunc("POST "+PathWorkflowExecute, wrap("execute_workflow", m.handleExecuteWorkflow))
	mux.HandleFunc("POST "+PathWorkflowExecuteByID, wrap("execute_workflow_by_id", m.handleExecuteWorkflowByID))
	// GET /v1/workflows/{id}/versions is the version-history read (versioning
	// design §2.2). It is mounted in this legacy branch as well as in
	// registerAuthzRoutes, like the other workflow reads: here the namespace is
	// namespace.FromContext's Default, exactly as in handleGetWorkflow.
	mux.HandleFunc("GET "+PathWorkflowVersions, wrap("list_workflow_versions", m.handleListWorkflowVersions))
	mux.HandleFunc("GET "+PathWorkflowVersionByID, wrap("read_workflow_version", m.handleGetWorkflowVersion))
	mux.HandleFunc("POST "+PathWorkflowRollback, wrap("rollback_workflow", m.handleRollbackWorkflow))
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
	// GET /v1/executions/stats is the bounded status aggregate. Go 1.22's mux
	// prefers the literal segment over {id} at the same depth, so "stats" is
	// never read as an execution id; the /v1/executions/{id}/ catch below needs
	// a trailing slash and cannot intercept it either.
	mux.HandleFunc("GET "+PathExecutionStats, wrap("execution.read", m.handleExecutionStats))
	// GET /v1/overview is the tenant dashboard read. It reuses the execution
	// read scope through its own operation; see authz.go's OpOverviewRead.
	mux.HandleFunc("GET "+PathOverview, wrap("overview.read", m.handleOverview))
	mux.HandleFunc("GET "+PathExecutionByID, wrap("execution.read", m.handleInspectByID))
	mux.HandleFunc("GET "+PathExecutionWait, wrap("execution.read", m.handleWaitByID))
	mux.HandleFunc("POST "+PathExecutionSignals, wrap("execution.signal", m.handleSignalByID))
	mux.HandleFunc("POST "+PathExecutionCancel, wrap("execution.cancel", m.handleCancelByID))
	mux.HandleFunc("POST "+PathExecutionPause, wrap("execution.pause", m.handlePauseByID))
	mux.HandleFunc("POST "+PathExecutionResume, wrap("execution.resume", m.handleResumeByID))
	mux.HandleFunc("POST "+PathExecutionRerun, wrap("execution.rerun", m.handleRerunByID))
	m.registerScheduleRoutesBare(mux, wrap)
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
//   - POST /v1/executions/{id}/pause      → execution.pause     (mutation)
//   - POST /v1/executions/{id}/resume     → execution.resume    (mutation)
func (m *workflowControlModule) registerAuthzRoutes(mux *http.ServeMux) {
	authz := m.authzWrap
	// §9.1 semantic inversion: POST /v1/workflows now REGISTERS. The
	// compile-and-execute route moved to POST /v1/workflows/execute (merging
	// submit+invoke). Each route is its own method-qualified pattern bound to a
	// stable Op so the authz wrapper resolves the operation from the pattern
	// itself (spec §1.3). The authz wrapper injects the principal's namespace
	// into the request context; handlers resolve it via namespace.FromContext —
	// never from the client body (spec §6.2).
	mux.HandleFunc("POST "+PathWorkflows, m.authzWrapWithPreAdmission(OpWorkflowRegister, true, m.handleRegisterWorkflow, nil, rejectFAFWorkflowRegistrationBeforeAdmission))
	// GET /v1/workflows (the collection read) reuses OpWorkflowRead — the very
	// operation GET /v1/workflows/{id} carries — so the two scopes cannot drift:
	// a principal that may read one workflow may list them, and one that may not
	// is refused by the same default-deny check before the handler runs. The
	// required scope is the read-only workflow.read (a write-scope principal is
	// allowed too; see principalAllowsOperation). No resource resolver is
	// passed: a collection is not one resource, and the scope of the read is the
	// principal's own namespace, resolved inside the handler via
	// namespace.FromContext (never from a query parameter).
	mux.HandleFunc("GET "+PathWorkflows, authz(OpWorkflowRead, false, m.handleListWorkflows, nil))
	mux.HandleFunc("GET "+PathWorkflowByID, authz(OpWorkflowRead, false, m.handleGetWorkflow, workflowIDResolver()))
	mux.HandleFunc("PUT "+PathWorkflowByID, m.authzWrapWithPreAdmission(OpWorkflowDefinitionUpdate, true, m.handleReplaceWorkflow, workflowIDResolver(), rejectFAFWorkflowRegistrationBeforeAdmission))
	mux.HandleFunc("DELETE "+PathWorkflowByID, authz(OpWorkflowRegister, true, m.handleDeregisterWorkflow, workflowIDResolver()))
	// GET /v1/node-types[/{type}] reuses OpWorkflowRead: a principal that may
	// read workflow definitions may read the node schemas they are written
	// against, so it requires the same read-only workflow.read scope. The
	// resolver carries an optional ?namespace= as the resource namespace, so a
	// scoped request (the only one that merges runner-reported types, which
	// belong to tenant pools) is authorized against that namespace; an unscoped
	// one is decided on the operation alone.
	mux.HandleFunc("GET "+PathNodeTypes, authz(OpWorkflowRead, false, m.handleListNodeTypes, nodeTypesNamespaceResolver()))
	mux.HandleFunc("GET "+PathNodeTypeByType, authz(OpWorkflowRead, false, m.handleGetNodeType, nodeTypesNamespaceResolver()))
	// GET /v1/current-user reports the verified principal itself. No resource
	// resolver: the resource IS the caller, so there is no id to resolve and no
	// namespace to compare — the wrapper already bound the response to the
	// principal it verified. It requires authentication only (no scope), because
	// the console derives its capabilities from the scopes reported here. It is
	// mounted ONLY on this branch (the legacy branch below serves no identity at
	// all), which is why it has no counterpart registration there.
	mux.HandleFunc("GET "+PathCurrentUser, authz(OpCurrentUserRead, false, m.handleCurrentUser, nil))
	mux.HandleFunc("POST "+PathWorkflowExecute, authz(OpWorkflowCreate, true, m.handleExecuteWorkflow, inlineExecutionIDResolver()))
	mux.HandleFunc("POST "+PathWorkflowExecuteByID, authz(OpWorkflowInvoke, true, m.handleExecuteWorkflowByID, invokeByIDResolver()))
	// GET /v1/workflows/{id}/versions (the version-history read, versioning
	// design §2.2) reuses OpWorkflowRead — the operation GET /v1/workflows/{id}
	// carries — so the two scopes cannot drift: a principal that may read one
	// workflow may read its version history, and one that may not is refused by
	// the same default-deny check. It therefore requires the same read-only
	// workflow.read scope. The resolver scopes the audit row to the workflow id
	// the route addresses, like the by-id read above.
	mux.HandleFunc("GET "+PathWorkflowVersions, authz(OpWorkflowRead, false, m.handleListWorkflowVersions, workflowIDResolver()))
	mux.HandleFunc("GET "+PathWorkflowVersionByID, authz(OpWorkflowRead, false, m.handleGetWorkflowVersion, workflowIDResolver()))
	// POST /v1/workflows/{id}/rollback (versioning design §2.4) reuses
	// OpWorkflowDefinitionUpdate — a rollback IS a definition update, and giving
	// it an operation of its own would take it away from every deployment token
	// that can already replace a definition, a silent UI degradation for a route
	// that grants no more than PUT /v1/workflows/{id} does.
	mux.HandleFunc("POST "+PathWorkflowRollback, authz(OpWorkflowDefinitionUpdate, true, m.handleRollbackWorkflow, workflowIDResolver()))
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
	mux.HandleFunc("GET "+PathExecutionStats, authz(OpExecutionRead, false, m.handleExecutionStats, nil))
	// GET /v1/overview: OpOverviewRead maps to the same "execution" scope as
	// the execution reads. No resource resolver: the aggregate is over the
	// principal's own namespace, resolved inside the handler. The cluster
	// segment is a second, in-handler decision on OpManagementRunnerList.
	mux.HandleFunc("GET "+PathOverview, authz(OpOverviewRead, false, m.handleOverview, nil))
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
	// Pause and resume are operator dispatch controls (design §3): the two
	// transitions are separate operations with separate scopes from
	// execution.cancel, because suspending a pipeline is not the same trust as
	// signaling one.
	mux.HandleFunc("POST "+PathExecutionPause, authz(OpExecutionPause, true, m.handlePauseByID, execIDResolver("pause")))
	mux.HandleFunc("POST "+PathExecutionResume, authz(OpExecutionResume, true, m.handleResumeByID, execIDResolver("resume")))
	// Rerun derives a NEW execution from a terminal one. The audited resource is
	// the execution being rerun (the one the caller addressed), not the execution
	// the call creates: the created id is in the response, and an audit row that
	// named it would be unrereadable from the request that produced it.
	mux.HandleFunc("POST "+PathExecutionRerun, authz(OpExecutionRerun, true, m.handleRerunByID, execIDResolver("rerun")))
	// Schedules are the runtime overlays on a workflow's trigger nodes. They are
	// registered here rather than as a module of their own because they address
	// the workflow subtree and resolve the same registry record this module
	// already resolves.
	m.registerScheduleRoutes(mux)
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

func (m *workflowControlModule) handlePauseByID(w http.ResponseWriter, r *http.Request) {
	m.handlePause(w, r, types.ExecutionID(r.PathValue("id")))
}

func (m *workflowControlModule) handleResumeByID(w http.ResponseWriter, r *http.Request) {
	m.handleResume(w, r, types.ExecutionID(r.PathValue("id")))
}

func (m *workflowControlModule) handleRerunByID(w http.ResponseWriter, r *http.Request) {
	m.handleRerun(w, r, types.ExecutionID(r.PathValue("id")))
}

func (m *workflowControlModule) handleRevokeSignalByID(w http.ResponseWriter, r *http.Request) {
	m.handleRevokeSignal(w, r, types.ExecutionID(r.PathValue("id")))
}

// newExecutionIDResolver is the resource resolver for the entry-seed route
// POST /v1/executions. Unlike the /v1/executions/{id}/ subtree — which resolves
// the targeted execution id from the path — a seed has no path id, so the
// resolver pre-allocates a fresh execution id (R3.1). That id is stamped onto the
// admission audit row by authzWrap and, via engine.WithExecutionID injected by
// the same wrapper, reused by the engine, so the audit row and the persisted
// execution share one id (closing the audit↔execution correlation gap that left
// reconcile Probe reading an empty ExecutionID).
//
// It is NOT the resolver of the execute routes: a submission carrying an
// Idempotency-Key runs under engine.IdempotentExecutionID rather than a fresh id,
// so the execute routes resolve through inlineExecutionIDResolver and
// invokeByIDResolver, which derive the same keyed id the engine will (see
// resolveExecutionID); entry-seed accepts no key, so a fresh id is exact here.
//
// resource/workflowID/namespace stay empty: seed authz was decided on operation
// alone, and this resolver only supplies the correlation id, not authz inputs.
func newExecutionIDResolver() func(*http.Request) (string, string, string, string) {
	return func(*http.Request) (string, string, string, string) {
		return "", "", string(engine.NewExecutionID()), ""
	}
}

// inlineExecutionIDResolver is the resource resolver for the inline direct-run
// route POST /v1/workflows/execute. Like newExecutionIDResolver it hands the
// wrapper a fresh pre-allocated id (R3.1) — except for a keyed submission, which
// runs under engine.IdempotentExecutionID: the resolver then derives that id
// itself, from the same namespace, scope and key the engine will (see
// resolveExecutionID), so the admission/outcome rows and the execution stay one
// id instead of the rows naming a phantom no execution ever runs under.
//
// The scope is the handler's own (inlineIdempotencyScope over the definition
// name in the request body), so resolver and handler cannot drift. Deriving it
// needs the body, which the handler reads after this resolver ran — hence the
// read-and-restore, bounded by inlineResolveBodyLimit because this runs before
// authorize; on any body that does not decode, or exceeds the bound, the
// handler will refuse the request before it can claim anything, and a fresh id
// is the right shape for a submission that creates no execution.
func inlineExecutionIDResolver() func(*http.Request) (string, string, string, string) {
	return func(r *http.Request) (string, string, string, string) {
		if idempotencyKeyForResolve(r) == "" {
			return "", "", string(engine.NewExecutionID()), ""
		}
		var req executeWorkflowRequest
		if !peekJSONBody(r, &req) || req.Workflow == nil {
			return "", "", string(engine.NewExecutionID()), ""
		}
		return "", "", string(resolveExecutionID(r, inlineIdempotencyScope(r.Context(), req.Workflow))), ""
	}
}

// inlineResolveBodyLimit bounds how many body bytes the inline resolver buffers
// before authorize runs. The resolver reads a keyed request's definition to name
// its key scope, and it runs before the authorizer decides, so a principal that
// may not even hold workflow.create must not be able to park an unbounded body
// in memory for the length of a decision. The bound matches
// MaxEditorMetadataBytes, the size the server already accepts for one
// definition's editor payload. A body over the bound falls back to the fresh id
// — restoring the body whole so the handler still serves it — which costs the
// keyed request's audit rows the derived id (they name the fresh one), a
// bounded, deliberate trade.
const inlineResolveBodyLimit = 1 << 20

// peekJSONBody decodes the request body into dst and puts the bytes back for the
// handler. A resource resolver runs before the handler and the body is a single
// stream, so a resolver that needs a field must restore what it read. Decoding
// mirrors the handler exactly — same bytes, same json.Decoder.Decode acceptance —
// so whatever decodes here decodes there; a read error is not fatal either,
// because the truncated bytes are restored and handed to the same decoder, and
// the handler then fails precisely the cases the peek fails.
//
// The read is bounded by inlineResolveBodyLimit: one byte past the bound is
// enough to detect an oversized body without buffering it, and an oversized body
// is still handed back whole — consumed prefix in front of the unread remainder
// — with a false result.
func peekJSONBody(r *http.Request, dst any) bool {
	if r.Body == nil {
		return false
	}
	raw, _ := io.ReadAll(io.LimitReader(r.Body, inlineResolveBodyLimit+1))
	if len(raw) > inlineResolveBodyLimit {
		r.Body = io.NopCloser(io.MultiReader(bytes.NewReader(raw), r.Body))
		return false
	}
	r.Body = io.NopCloser(bytes.NewReader(raw))
	return json.NewDecoder(bytes.NewReader(raw)).Decode(dst) == nil
}

// invokeByIDResolver is the resource resolver for POST /v1/workflows/{id}/execute
// — invoke a REGISTERED workflow by id (spec §7).
//
// It differs from newExecutionIDResolver in exactly one slot: the resource names
// the path's workflow, so the admission audit row carries a real resource and
// workflow_id. It differs from workflowIDResolver in the executionID slot, and
// that difference is the point: workflowIDResolver returns the WORKFLOW id there,
// which authzWrap then injects as the execution id and engine.Submit/Invoke
// adopts rather than minting a fresh one (engine.preallocOrNewExecutionID). Every
// by-id invoke of one workflow then collapses onto a single execution id, and
// because the engine keys its outbox, node state and advance markers by that id,
// the second and later invokes are admitted (200, with an execution id) but never
// dispatched — they never reach a terminal status and the caller burns its full
// budget. That is not theoretical: the webscan RemoteExecutor invokes this route
// once per batch (executor/remote.go), and a map node claimed as a single group
// lease serves a whole batch, so any scan wider than one batch hits it.
//
// A fresh id keeps engine semantics (Submit/Invoke are "start a NEW execution")
// and stays coherent with what the engine now does, without giving up the
// resource/workflow dimensions an audit row is meant to carry — except when the
// request is keyed: the engine then runs the submission under
// engine.IdempotentExecutionID (derived so a retry finds the claim), and
// resolveExecutionID derives the same id here so the audit rows still name the
// execution that runs.
func invokeByIDResolver() func(*http.Request) (string, string, string, string) {
	return func(r *http.Request) (string, string, string, string) {
		id := r.PathValue("id")
		return "workflow/" + id, id,
			string(resolveExecutionID(r, engine.WorkflowIdempotencyScope(types.WorkflowID(id)))), ""
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
	Test     bool               `json:"test,omitempty"`
}

type executeWorkflowResponse struct {
	ExecutionID types.ExecutionID `json:"execution_id"`
	// Status is where the submission landed: "running" for an execution that
	// started, "queued" for one waiting for a workflow-concurrency slot
	// (settings.concurrency with mode=queue). Without it a caller cannot tell
	// the two apart from the response, and "accepted" would be a lie for the
	// parked case — nothing runs until a slot frees.
	//
	// It is the execution's status read back after the submission, not a
	// recording of the admission decision: a submission that finished before the
	// response was written reports that instead, which is fresher and no less
	// true.
	Status types.ExecutionStatus `json:"status,omitempty"`
	// QueuePosition is the place in line, 1-based, and it is present only while
	// the execution is queued — 1 is the head, the submission the next release
	// promotes. A caller holding a position knows its work has not started and
	// how much is ahead of it.
	QueuePosition int `json:"queue_position,omitempty"`
	// ParamIssues is set only by the inline POST /v1/workflows/execute: a
	// registered workflow was validated when it was registered.
	ParamIssues []graph.ParamIssue `json:"param_issues,omitempty"`
	// Duplicate is true when the request's Idempotency-Key had already started
	// this execution: the answer is the original execution — its id and its
	// current status, which may be queued, paused or terminal — and this request
	// created nothing. It is the same answer a replayed webhook entry gives.
	Duplicate bool `json:"duplicate,omitempty"`
}

// submissionResponse builds the answer to a submission that succeeded.
//
// The status and the place in line are read back rather than threaded out of
// Submit/Invoke. Those are public SDK entry points whose signatures are fixed,
// and a second pair that returned more would be a second definition of what a
// submission returns; the read hits the store the submission just wrote to, so
// it cannot lag behind it, and it costs one read instead of Inspect's walk over
// every node of the graph.
//
// The read-back is best effort on purpose. The execution exists and its ID is
// the answer to the request — a status that could not be read is a missing
// detail, not a failed submission, so it is logged and the response goes out
// without it.
func (m *workflowControlModule) submissionResponse(ctx context.Context, id types.ExecutionID, issues []graph.ParamIssue) executeWorkflowResponse {
	resp := executeWorkflowResponse{ExecutionID: id, ParamIssues: issues}
	detail, err := m.eng.InspectStatus(ctx, id)
	if err != nil {
		if m.log != nil {
			m.log.Error("execute_status_readback_failed", "execution_id", string(id), "err", err)
		}
		return resp
	}
	resp.Status = detail.Status
	if detail.Status == types.ExecutionStatusQueued {
		resp.QueuePosition = detail.QueuePosition
	}
	return resp
}

// executeRegisteredRequest is the body for POST /v1/workflows/{id}/execute
// (spec §7): run an already-registered workflow by id. Entry/Input/Params are
// optional; when Entry is empty the registered graph's default start node is
// used.
type executeRegisteredRequest struct {
	Entry  string         `json:"entry,omitempty"`
	Input  map[string]any `json:"input,omitempty"`
	Params map[string]any `json:"params,omitempty"`
	Test   bool           `json:"test,omitempty"`
	// Version selects which version of the ADDRESSED workflow to run
	// (versioning design §2.3). It is a body field rather than a query
	// parameter or header because it belongs with entry/params/test — "what to
	// run this time" — and the idempotency fingerprint already covers the body
	// plus the resolved graph version.
	//
	// Omitted or equal to the current version runs the live record, i.e. the
	// behavior this route had before versions existed. A retired version runs
	// from the graph snapshot captured when it was retired, never a recompile.
	// The NAME is always the path record's: (namespace, rec.Name, version), so
	// a version string can never redirect the call to another workflow, and an
	// unknown version is 404 workflow_version_not_found.
	Version string `json:"version,omitempty"`
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

// concurrencyRetryAfterSeconds is the Retry-After a refused submission carries.
//
// The server cannot know when a slot frees: the refused submission is not
// parked anywhere, so there is no release time to promise. One second is a
// floor that keeps a client from spinning, not a prediction, and the header is
// documented that way in the OpenAPI spec.
const concurrencyRetryAfterSeconds = 1

// writeConcurrencyRefused answers a submission the workflow's own concurrency
// limit refused (engine.ErrConcurrencyLimit).
//
// 429 rather than 503: the request is well-formed and the server is healthy —
// this one workflow is at capacity, which is state the caller created and can
// wait out. Retry is the correct client behaviour, so the failure carries a
// Retry-After even though it is only a floor.
func writeConcurrencyRefused(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Retry-After", strconv.Itoa(concurrencyRetryAfterSeconds))
	writeFail(w, r, http.StatusTooManyRequests, "workflow_concurrency_limit",
		"workflow is at its concurrency limit")
}

// writeConcurrencyQueueFull answers a QUEUE-mode submission that would exceed
// the workflow's max_queued backlog (engine.ErrConcurrencyQueueFull).
//
// 429 like the limit refusal, and retryable for the same reason: the backlog is
// the workflow's own state, the request is well-formed and the server is
// healthy. It carries its own code because the two wait for different things —
// a slot to free versus the queue to drain — and a caller that cannot tell them
// apart cannot size its backoff.
func writeConcurrencyQueueFull(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Retry-After", strconv.Itoa(concurrencyRetryAfterSeconds))
	writeFail(w, r, http.StatusTooManyRequests, "workflow_concurrency_queue_full",
		"workflow concurrency queue is full")
}

// writeConcurrencyUnavailable answers a QUEUE-mode submission against a store
// that cannot park executions (engine.ErrConcurrencyQueueUnsupported).
//
// 503, not 429: retrying does not change the answer until the deployment does.
// The workflow asked for a capability this server does not have, which is the
// server's state rather than the caller's, and the code says so explicitly
// instead of hiding it behind internal_error — an operator reading the code
// alone has to be able to tell "this backend cannot queue" from "the request
// was wrong".
func writeConcurrencyUnavailable(w http.ResponseWriter, r *http.Request) {
	writeFail(w, r, http.StatusServiceUnavailable, "workflow_concurrency_unavailable",
		"workflow concurrency queue is not available on this server")
}

// writeConcurrencyLimitsUnavailable answers a submission against a store that
// cannot enforce a concurrency limit AT ALL (engine.ErrConcurrencyUnsupported).
//
// It shares the queue case's status and code deliberately: both mean "the
// definition asked for a limit and this deployment cannot honour it", and a
// caller's move is identical — the answer changes when the deployment does, not
// when the caller retries. Only the message differs, because the two say which
// capability is missing: none, versus the queue half of it.
//
// 503 rather than 500 is the point of the mapping. The engine fails closed here
// (running unlimited would defeat a limit the author declared to protect a
// downstream), so this is a designed refusal with a known cause — filing it
// under internal_error would send an operator looking for a bug in a
// deployment that is behaving exactly as specified.
func writeConcurrencyLimitsUnavailable(w http.ResponseWriter, r *http.Request) {
	writeFail(w, r, http.StatusServiceUnavailable, "workflow_concurrency_unavailable",
		"workflow concurrency is not available on this server")
}

// writeConcurrencyFailure maps the concurrency sentinels a submission can
// return onto their responses, reporting whether it wrote one. The call sites
// keep their own error handling for everything else, which is what lets the
// four submit/invoke paths share one mapping instead of repeating three
// errors.Is checks each.
func writeConcurrencyFailure(w http.ResponseWriter, r *http.Request, err error) bool {
	switch {
	case errors.Is(err, engine.ErrConcurrencyLimit):
		writeConcurrencyRefused(w, r)
	case errors.Is(err, engine.ErrConcurrencyQueueFull):
		writeConcurrencyQueueFull(w, r)
	case errors.Is(err, engine.ErrConcurrencyQueueUnsupported):
		writeConcurrencyUnavailable(w, r)
	case errors.Is(err, engine.ErrConcurrencyUnsupported):
		writeConcurrencyLimitsUnavailable(w, r)
	default:
		return false
	}
	return true
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
	// An inline definition is not a server resource, so it has no server-assigned
	// id. Accepting one would let the caller choose the execution's workflow_id
	// attribution, contradict WorkflowDef.id's "server-assigned" contract, and
	// return an id of any length where ExecutionDetail declares maxLength 64
	// (and the SQL column is VARCHAR(64)). Attribution is available where it is
	// truthful: POST /v1/workflows/{id}/execute stamps the path id.
	if req.Workflow.ID != "" {
		writeFail(w, r, http.StatusBadRequest, "workflow_invalid",
			"workflow id is not accepted for an inline execute; use POST /v1/workflows/{id}/execute")
		return
	}
	// Namespace is server-authoritative, exactly as on the register path
	// (buildWorkflowRecord): the definition is compiled, hashed, and recorded
	// under the caller's namespace, so the body's value is overwritten before
	// any of them happen. Left as sent, an inline body naming another tenant
	// would be hashed and persisted under that tenant, and the asynchronous
	// dispatch of its sub-workflows would resolve them out of that tenant's
	// registry.
	req.Workflow.Namespace = string(namespace.FromContext(r.Context()))
	// Compiled in the caller's namespace (server-issued by the authz wrapper,
	// never the body): an inline definition carrying an xflow.subworkflow node
	// pins its reference against the same registry the register path uses.
	g, err := graph.CompileWithResolver(r.Context(), req.Workflow, m.subWorkflowResolver(namespace.FromContext(r.Context())))
	if err != nil {
		writeFail(w, r, http.StatusBadRequest, "workflow_compile_failed", err.Error())
		return
	}
	// The inline definition gets the same ParamSpec check a registered one
	// does; it is the other path a node definition enters the server by.
	paramIssues, err := m.checkWorkflowParams(r.Context(), g.ExpandedDefinition())
	if err != nil {
		if !writeParamInvalid(w, r, err) {
			writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		}
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
	ctx = engine.WithTestRun(ctx, req.Test)
	digest, err := definitionDigest(req.Workflow)
	if err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	idem, ok := keyedSubmission(w, r, inlineIdempotencyScope(ctx, req.Workflow), digest)
	if !ok {
		return
	}
	if idem != nil {
		ctx = engine.WithIdempotency(ctx, idem)
	}
	var id types.ExecutionID
	if req.Entry != "" {
		id, err = m.eng.Invoke(ctx, g, req.Entry, req.Input)
		if err != nil {
			if !writeSubmitFailure(w, r, err) {
				writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
			}
			return
		}
	} else {
		id, err = m.eng.Submit(ctx, g, req.Params)
		if err != nil {
			if !writeSubmitFailure(w, r, err) {
				writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
			}
			return
		}
	}
	resp := m.submissionResponse(ctx, id, paramIssues)
	resp.Duplicate = idem != nil && idem.Duplicate
	writeData(w, r, http.StatusOK, resp)
}

// registerWorkflowResponse echoes the persisted workflow ID (server-assigned
// when the request did not carry one) along with the non-fatal diagnostics
// graph.Compile collected.
//
// Warnings are surfaced here rather than on submit because register is the
// design-time step: the author is holding the definition and can still change
// it. A warning names only nodes and referenced node names -- never a node's
// output or a parameter value, which routinely carry credentials.
//
// ParamIssues carries the ParamSpec validator's findings (warn mode: every
// issue; enforce mode: warnings only, since an error rejects the request). It
// is deliberately separate from the compiler's Warnings: the two have
// different shapes and different consumers (the editor maps each ParamIssue
// onto the field its Path names).
type registerWorkflowResponse struct {
	WorkflowID  types.WorkflowID   `json:"workflow_id"`
	Warnings    []string           `json:"warnings,omitempty"`
	ParamIssues []graph.ParamIssue `json:"param_issues,omitempty"`
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
	def, metadata, ok := decodeWorkflowDefinitionWithMetadata(w, r)
	if !ok {
		return
	}
	// A registered workflow is a server-assigned resource: the id is minted by
	// the registry, and accepting a body-supplied one would let the caller
	// choose the identity every execution of this workflow is attributed to.
	// Refuse it exactly as the inline execute route does; PUT already requires
	// a supplied id to match the path.
	if def.ID != "" {
		writeFail(w, r, http.StatusBadRequest, "workflow_invalid",
			"workflow id is not accepted on registration; it is server-assigned")
		return
	}
	// Namespace is authoritative from the context, never the body.
	ns := namespace.FromContext(r.Context())

	id, diag, err := m.registerWorkflowWithMetadata(r.Context(), ns, def, metadata)
	if err != nil {
		if writeParamInvalid(w, r, err) {
			return
		}
		var compileErr *WorkflowCompileError
		if errors.As(err, &compileErr) {
			writeFail(w, r, http.StatusBadRequest, "workflow_compile_failed", compileErr.Unwrap().Error())
			return
		}
		if writeWorkflowConflict(w, r, err) {
			return
		}
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	w.Header().Set("Location", "/v1/workflows/"+string(id))
	writeData(w, r, http.StatusCreated, registerWorkflowResponse{WorkflowID: id, Warnings: diag.Warnings, ParamIssues: diag.ParamIssues})
}

// writeWorkflowConflict maps a registration or replacement conflict to its
// faithful 409 code and reports whether it wrote a response.
//
// The three conflict kinds send the caller to different fixes, so they are not
// collapsed into one code. A key conflict is resolved by changing the
// definition; a name conflict by addressing the record that already holds the
// name (PUT its id, or delete it); a version conflict by publishing under a
// new label. The typed errors unwrap to ErrWorkflowConflict for the
// classifiers that only need "this cannot succeed as presented", so the
// specific checks MUST run first and the generic one last.
func writeWorkflowConflict(w http.ResponseWriter, r *http.Request, err error) bool {
	switch {
	case errors.Is(err, backend.ErrWorkflowNameConflict):
		// The holder id is deliberately not echoed: it is another record's
		// identity and this response may reach a caller that cannot read it.
		writeFail(w, r, http.StatusConflict, "workflow_name_conflict", "another live workflow already holds this namespace and name")
		return true
	case errors.Is(err, backend.ErrWorkflowVersionImmutable):
		writeFail(w, r, http.StatusConflict, "workflow_version_immutable", "workflow version was already published with a different definition")
		return true
	case errors.Is(err, backend.ErrWorkflowConflict):
		writeFail(w, r, http.StatusConflict, "workflow_conflict", "workflow definition conflicts with an existing registration")
		return true
	}
	return false
}

// WorkflowCompileError marks a registration failure that the caller can fix by
// changing the definition, as opposed to a server-side failure. The HTTP
// handler maps it to 400 and surfaces the compiler's message; every other error
// collapses to a generic 500, since it may name internal state.
type WorkflowCompileError struct{ err error }

func (e *WorkflowCompileError) Error() string { return e.err.Error() }
func (e *WorkflowCompileError) Unwrap() error { return e.err }

var errWorkflowIDMismatch = errors.New("workflow id does not match path")

// errWorkflowVersionsUnsupported means the configured workflow registry does
// not implement backend.WorkflowVersionCapability. That is a server-side
// misconfiguration, not a client error: the version routes fail closed with a
// 500 rather than serving an empty history, which would read as "this workflow
// has no retired versions" — a different and false statement.
var errWorkflowVersionsUnsupported = errors.New("workflow registry does not support version history")

// backend.ErrWorkflowVersionUnavailable means an archived version's payload no
// longer reproduces under the current compiler: recomputing the runtime hash
// from the stored definition does not yield the hash the version was published
// with. The version label is immutable, so installing or running "the same
// version" with different semantics would silently break every pinned replay
// and subworkflow reference that names it. Callers fail closed (409
// workflow_version_unavailable) instead.
//
// The sentinel itself lives in backend because the embedded SDK raises the same
// condition from the same recomputation; this module maps it to the wire code.

var errFAFWorkflowRegistration = errors.New("workflow registration does not support options.faf")

// validateWorkflowRegistrationDefinition rejects execution modes that cannot
// be represented by the durable workflow registry. It returns a
// WorkflowCompileError so callers preserve the existing invalid-definition
// contract: HTTP reports 400 workflow_compile_failed and the in-process API
// reports a caller-fixable registration error.
func validateWorkflowRegistrationDefinition(def *types.WorkflowDef) error {
	if def != nil && def.Options != nil && def.Options.FAF {
		return &WorkflowCompileError{err: errFAFWorkflowRegistration}
	}
	return nil
}

// rejectFAFWorkflowRegistrationBeforeAdmission preserves the request bytes for
// handleRegisterWorkflow/handleReplaceWorkflow while inspecting the first JSON
// value with the same decoder behavior as decodeJSON. Invalid bodies are left
// for the handler so their established validation and audit behavior is
// unchanged; a valid FAF definition is refused before authzWrap writes an
// admission audit row.
//
// The read is bounded by MaxRequestBodyBytes with the same http.MaxBytesReader
// guard decodeJSON uses. This hook runs BEFORE the handler's decoder, so an
// unbounded read here would buffer an over-limit body first and make the
// decodeJSON cap unreachable. Overflow is refused here with decodeJSON's exact
// 413 (fixed code and message, no size echo), and the body is deliberately not
// restored: nothing downstream may resume reading it.
func rejectFAFWorkflowRegistrationBeforeAdmission(w http.ResponseWriter, r *http.Request) bool {
	if r.Body == nil {
		return true
	}
	body := r.Body
	raw, readErr := io.ReadAll(http.MaxBytesReader(w, body, MaxRequestBodyBytes))
	_ = body.Close()
	if readErr != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(readErr, &tooLarge) {
			writeFail(w, r, http.StatusRequestEntityTooLarge, "request_too_large", "request body too large")
			return false
		}
	}
	// Non-overflow reads keep the pre-existing contract: the handler sees the
	// bytes read so far (for a mid-stream read error, decodeJSON then reports
	// the malformed body). The overflow path above returned before this point.
	r.Body = io.NopCloser(bytes.NewReader(raw))
	if readErr != nil {
		return true
	}

	var def types.WorkflowDef
	if err := json.NewDecoder(bytes.NewReader(raw)).Decode(&def); err != nil {
		return true
	}
	if err := validateWorkflowRegistrationDefinition(&def); err != nil {
		writeFail(w, r, http.StatusBadRequest, "workflow_compile_failed", err.Error())
		return false
	}
	return true
}

// registerWorkflow is the definition -> persisted graph path both entry points
// share: the HTTP handler above and APIServer.RegisterWorkflow, which an
// embedded server calls in-process.
//
// Sharing it is the point. Registration identity is three coupled choices --
// workflowRegistryKey, the runtime hash, and the entry-activation derivation --
// and a second implementation that picked any of them differently would
// register a workflow the dispatcher then failed to resolve. ns is supplied by
// the caller and written onto def; it is never read from def itself, so an
// in-process caller cannot register into another namespace either.
func (m *workflowControlModule) registerWorkflow(ctx context.Context, ns namespace.Namespace, def *types.WorkflowDef) (id types.WorkflowID, diag registrationDiagnostics, err error) {
	return m.registerWorkflowWithMetadata(ctx, ns, def, nil)
}

// registerWorkflowWithMetadata is registerWorkflow, additionally accepting
// the editor_metadata sibling the HTTP POST /v1/workflows body may carry
// (§D2). Embedded/SDK callers have no such input and go through
// registerWorkflow, which passes nil.
func (m *workflowControlModule) registerWorkflowWithMetadata(ctx context.Context, ns namespace.Namespace, def *types.WorkflowDef, metadata *types.WorkflowEditorMetadata) (id types.WorkflowID, diag registrationDiagnostics, err error) {
	defer func() { m.observeRegistration(ctx, ns, "add", err) }()
	registry := m.registry()
	if registry == nil {
		if m.log != nil {
			m.log.Error("register_workflow_no_registry")
		}
		return "", registrationDiagnostics{}, errors.New("apiserver: no workflow registry configured")
	}
	rec, diag, err := m.buildWorkflowRecord(ctx, ns, "", def, metadata)
	if err != nil {
		return "", registrationDiagnostics{}, err
	}
	return m.addWorkflowRecord(ctx, ns, registry, rec, diag)
}

// registrationOutcome maps a registration failure to the closed outcome enum
// xflow_workflow_registration_total is partitioned on, by asking what would
// have to change for the call to succeed: nothing that a retry supplies
// (invalid_definition, conflict), or the backend itself (error).
//
// It is deliberately the same split the HTTP handlers already make — a
// *WorkflowCompileError is a 400 and everything else is a 500 — so an operator
// reading status codes and an operator reading the counter see one taxonomy,
// not two. Replace conflicts are split out of the bare conflict below because a
// typed one names a racing writer (stale_revision and friends), which is a
// different operator response from "your key is occupied".
func registrationOutcome(err error) string {
	var compileErr *WorkflowCompileError
	if errors.As(err, &compileErr) {
		return "invalid_definition"
	}
	var paramErr *execution.ParamIssuesError
	if errors.As(err, &paramErr) {
		return "invalid_definition"
	}
	var replaceConflict *backend.WorkflowReplaceConflictError
	if errors.As(err, &replaceConflict) {
		return "conflict"
	}
	if errors.Is(err, backend.ErrWorkflowConflict) {
		return "conflict"
	}
	return "error"
}

// observeRegistration counts one registration attempt. The namespace is
// asserted here rather than read from ctx because the in-process entry point
// carries the target namespace as an argument, not in the context — reading
// ctx would label the embedded host's registrations "default" regardless of the
// namespace they actually registered into.
func (m *workflowControlModule) observeRegistration(ctx context.Context, ns namespace.Namespace, operation string, err error) {
	outcome := "registered"
	if err != nil {
		outcome = registrationOutcome(err)
	}
	m.registrationMetrics.ObserveRegistration(namespace.WithNamespace(ctx, ns), operation, outcome)
}

func (m *workflowControlModule) buildWorkflowRecord(ctx context.Context, ns namespace.Namespace, id types.WorkflowID, def *types.WorkflowDef, metadata *types.WorkflowEditorMetadata) (backend.WorkflowRecord, registrationDiagnostics, error) {
	if err := validateWorkflowRegistrationDefinition(def); err != nil {
		return backend.WorkflowRecord{}, registrationDiagnostics{}, err
	}
	if err := validateEditorMetadataSize(metadata); err != nil {
		return backend.WorkflowRecord{}, registrationDiagnostics{}, err
	}
	if def != nil {
		// Namespace is server-authoritative and must be fixed before compilation
		// and hashing so every derived representation uses the authenticated
		// identity rather than an untrusted body value.
		def.Namespace = string(ns)
	}
	g, err := graph.CompileWithResolver(ctx, def, m.subWorkflowResolver(ns))
	if err != nil {
		return backend.WorkflowRecord{}, registrationDiagnostics{}, &WorkflowCompileError{err: err}
	}
	// A server-hosted webhook entry with no signing secret would be an
	// unauthenticated endpoint that creates executions, so it is refused here
	// rather than admitted at request time (spec §6, decision 5). The check is
	// gated on this process actually serving the route: a deployment that does
	// not has nothing to protect.
	if m.webhookRoutesHosted {
		if err := validateServerHostedWebhookSecrets(g); err != nil {
			return backend.WorkflowRecord{}, registrationDiagnostics{}, err
		}
	}
	// ParamSpec validation runs after the compiler accepted the definition, so
	// a structural error keeps its workflow_compile_failed answer. It checks
	// the template-expanded def (templated nodes only get a type there) and
	// never modifies def: the HTTP path writes no defaults.
	paramIssues, err := m.checkWorkflowParams(ctx, g.ExpandedDefinition())
	if err != nil {
		return backend.WorkflowRecord{}, registrationDiagnostics{}, err
	}
	// Editor metadata keys are validated against the compiled definition's
	// nodes: a key matching no node is dropped silently (stale debris from a
	// renamed/removed node), and a key resolved only by NodeDef.Name (the
	// node has no ID) is kept but surfaces a NODE_METADATA_KEYED_BY_NAME
	// warning alongside the compiler's own warnings.
	metadata, metadataDiagnostics := types.ValidateEditorMetadata(def, metadata)
	// The registry conflict hash is the runtime hash every registration path
	// shares, so the SDK and HTTP agree on one workflow's identity. Builtin
	// Defaults are filled in memory before hashing, never into def: an omitted
	// builtin param hashes like the Default the SDK builder writes.
	hash, err := workflowhash.Runtime(def, hashParamSpecs)
	if err != nil {
		return backend.WorkflowRecord{}, registrationDiagnostics{}, err
	}
	// The audit fingerprint covers the full definition plus its editor
	// metadata sibling. It is never the conflict hash: a metadata-only change
	// must still differ here so a replace writes it as a new revision (D3).
	audit, err := workflowhash.Audit(def, metadata)
	if err != nil {
		return backend.WorkflowRecord{}, registrationDiagnostics{}, err
	}

	rec := backend.WorkflowRecord{
		ID:               id,
		Key:              workflowRegistryKey(string(ns), def.Name, def.Version),
		Namespace:        string(ns),
		Name:             def.Name,
		Version:          def.Version,
		DefinitionHash:   hash,
		AuditFingerprint: audit,
		Definition:       def,
		EditorMetadata:   metadata,
		Graph:            g,
	}
	warnings := append(append([]string(nil), g.Warnings()...), metadataDiagnostics...)
	return rec, registrationDiagnostics{Warnings: warnings, ParamIssues: paramIssues}, nil
}

func (m *workflowControlModule) addWorkflowRecord(ctx context.Context, ns namespace.Namespace, registry backend.WorkflowRegistry, rec backend.WorkflowRecord, diag registrationDiagnostics) (types.WorkflowID, registrationDiagnostics, error) {
	if registry == nil {
		if m.log != nil {
			m.log.Error("register_workflow_no_registry")
		}
		return "", registrationDiagnostics{}, errors.New("apiserver: no workflow registry configured")
	}
	// ReconcileAdd is the registration the SDK Engine runs too: a stored hash
	// in a legacy format (an HTTP "sha256:" record) or a stale v1 hash is
	// recomputed from the stored definition, and a match is an idempotent
	// registration that CAS-upgrades the stored hash. Only a real runtime
	// difference is ErrWorkflowConflict.
	stored, _, err := workflowhash.ReconcileAdd(ctx, registry, rec, hashParamSpecs)
	if err != nil {
		if m.log != nil {
			m.log.Error("register_workflow_failed", "err", err)
		}
		return "", registrationDiagnostics{}, err
	}
	if err := m.deriveWorkflowActivations(ctx, ns, stored); err != nil {
		if m.log != nil {
			m.log.Error("register_workflow_derive_activations_failed", "err", err)
		}
		return "", registrationDiagnostics{}, err
	}
	return stored.ID, diag, nil
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
//
// Triggers is a derived summary, not stored data: the distinct trigger kinds
// named by the definition's nodes. It is always present ([] when there is no
// trigger) so a client can render the column without a null check. The
// derivation reads rec.Definition.Nodes only — never the compiled graph and
// never the node registry — so it costs nothing beyond the record read this
// row already performs and cannot fail independently.
//
// Description is the stored definition's one-line description (zero extra IO:
// the record this row already read carries it) and is omitted when empty.
// CronExpressions is the same kind of derived trigger summary as Triggers,
// narrowed to the `expression` parameter of `xflow.trigger.cron` nodes: a
// workflow list can render a human-readable schedule without reading each
// definition, and no other trigger parameter leaves the server.
//
// LatestExecution is the store-backed "most recent run" projection, present
// only when the caller passes the execution.read check AND an execution store
// is wired AND implements the batch reader AND the read succeeds. It carries
// exactly four fields — no definition, params, output, or credential can reach
// the wire through it. Absent means UNKNOWN (no permission, no store, no
// capability, no executions, or a degraded read), never "this workflow never
// ran".
//
// Stats and RecentExecutions are the ?include=stats,recent projections. They
// are execution data behind the same execution.read scope as
// LatestExecution, so they are attached only after the same second
// authorization check and are omitted (never a 403) when the caller lacks it
// or the backend cannot answer. Stats present with runs=0 is a known zero;
// stats absent means the aggregate could not be read. RecentExecutions is
// omitted when the workflow has no executions in scope.
type workflowListItem struct {
	ID               string                    `json:"id"`
	Name             string                    `json:"name"`
	Version          string                    `json:"version"`
	DefinitionHash   string                    `json:"definition_hash"`
	RegistryRevision uint64                    `json:"registry_revision"`
	Description      string                    `json:"description,omitempty"`
	Triggers         []string                  `json:"triggers"`
	CronExpressions  []string                  `json:"cron_expressions,omitempty"`
	LatestExecution  *workflowLatestExecution  `json:"latest_execution,omitempty"`
	Stats            *workflowExecutionStats   `json:"stats,omitempty"`
	RecentExecutions []workflowRecentExecution `json:"recent_executions,omitempty"`
	// UpdatedAt is when the registry last registered or replaced this record.
	// Pointer + omitempty so a legacy record written before the registry
	// stamped it reports UNKNOWN by omitting the field rather than claiming
	// the zero instant. The UI treats absence as "unknown", not "never edited".
	UpdatedAt *time.Time `json:"updated_at,omitempty"`
}

// workflowLatestExecution is the summary of one workflow's most recent
// execution. It is a projection of store.ExecutionRecord with the heavy and
// sensitive columns — workflow_def, params, runtime, error — deliberately
// absent; a list row that needed those would read the execution directly.
type workflowLatestExecution struct {
	ExecutionID string                `json:"execution_id"`
	Status      types.ExecutionStatus `json:"status"`
	CreatedAt   time.Time             `json:"created_at"`
	UpdatedAt   time.Time             `json:"updated_at"`
}

// handleListWorkflows serves GET /v1/workflows (spec §3.3 page list). It is the
// endpoint whose absence §9.6 formerly recorded as a blocker — the registry's
// per-namespace index now exists and this handler consumes it.
//
// Namespace: resolved server-side from the authenticated principal via
// namespace.FromContext (injected by the authz wrapper), exactly as
// handleGetWorkflow does. There is deliberately no `namespace` query parameter
// and no body — a caller can only ever list its own namespace — and the
// registry takes ns as a required explicit argument that it validates, so even
// a bad value here cannot widen the read beyond the caller's namespace. The
// `q` name filter is applied strictly INSIDE that resolved scope: it narrows
// the caller's own collection and can never reach another namespace's records.
//
// Pagination: offset, 1-based, through the shared pageParams helper (default
// 20, hard cap 200). Cursor pagination is reserved for machine scans (spec
// §3.3); this is a page list, so it must not grow a second pagination dialect.
//
// Filtering: `q` is a case-insensitive substring match on the workflow name.
// It is applied BEFORE pagination, so offset/page_size and the reported total
// describe the filtered collection. The unfiltered path keeps its O(page)
// record reads; the filtering path is inherently O(N) record reads, where N is
// the number of workflows in the caller's OWN namespace — see
// workflowListItemsFiltered for the trade-off and why it cannot leak.
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

	include, ok := workflowListIncludeFrom(w, r)
	if !ok {
		return
	}
	// A row carrying stats costs one entry in a per-workflow aggregate; the
	// sample cap keeps the total cost proportional to the page, so the page is
	// tightened before either branch reads it.
	if include.stats && pageSize > maxWorkflowListPageSizeWithStats {
		pageSize = maxWorkflowListPageSizeWithStats
	}
	// The execution-derived fields are execution.read data. The second check is
	// evaluated once per request (never per row); a denial simply leaves the
	// fields off, because a workflow.read principal may still read the page.
	// latest_execution is gated here too, and for the same reason as the
	// ?include= aggregates: a row's most recent run status is execution data,
	// so a workflow.read-only principal must not learn it through the list.
	attachExecutionProjections := func(items []workflowListItem) {
		if !m.executionReadAuthorized(r) {
			return
		}
		m.attachLatestExecutions(r.Context(), ns, items)
		if include.requested() {
			m.attachExecutionStats(r.Context(), ns, items, include)
		}
	}

	q, ok := workflowListQueryFrom(w, r)
	if !ok {
		return
	}
	if q != "" {
		// Filtering needs the whole namespace enumeration, so it cannot reuse
		// the registry-sliced page read below. It reads each record exactly
		// once and paginates the matches itself (see the function doc).
		items, total, err := m.workflowListItemsFiltered(r.Context(), registry, ns, q, page, pageSize)
		if err != nil {
			fail("list_workflows_search_failed", err)
			return
		}
		attachExecutionProjections(items)
		writeList(w, r, items, total)
		return
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
	attachExecutionProjections(items)
	writeList(w, r, items, total)
}

// maxWorkflowListQueryLen bounds the `q` name filter. It is a request
// validation limit, not a truncation point: an over-long value is refused
// (400) rather than silently shortened, because a shortened query would answer
// a different question than the caller asked. 128 characters is far beyond any
// real workflow name the registry accepts and keeps the filter from becoming a
// way to ship large strings into the O(N) scan.
const maxWorkflowListQueryLen = 128

// workflowListQueryFrom parses the optional `q` name filter.
//
// A missing, empty, or whitespace-only value means "no filter" — the same
// convention the execution list filters use for their empty values, and the
// only reading under which clearing a search box returns the full collection.
// A value longer than maxWorkflowListQueryLen (counted in characters, matching
// the contract's maxLength) is a 400: like the execution list's malformed
// filters, silently ignoring it would answer the unfiltered collection as if
// it were the search result.
//
// ok=false means the failure response has already been written.
func workflowListQueryFrom(w http.ResponseWriter, r *http.Request) (string, bool) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if utf8.RuneCountInString(q) > maxWorkflowListQueryLen {
		writeFail(w, r, http.StatusBadRequest, "invalid_argument",
			"q must be at most 128 characters")
		return "", false
	}
	return q, true
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
//
// The rows are returned WITHOUT the optional latest_execution projection: the
// handler attaches it after the per-request execution.read check, so a
// workflow.read-only caller never sees it. It cannot fail the list: a missing
// store, a store without the capability, or a failing read all degrade to
// "field absent".
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
		items = append(items, workflowListItemFrom(rec))
	}
	return items, nil
}

// workflowListItemsFiltered serves the `q` name search. It is the ONLY path in
// this handler that reads every record in the namespace, and it is deliberate:
//
//   - The registry exposes no name index and backend.WorkflowListOptions has no
//     name/query field (adding one would touch both registry implementations
//     and, on the Redis side, the workflowreg Lua state machine). Enumerating
//     the namespace's ids and reading each record is the implementation the
//     audit approved for this size of collection.
//   - Matching MUST happen before pagination, otherwise page 2 of "alpha" could
//     be empty while matching workflows sit on page 5 of the unfiltered order.
//     So the matches are collected first and sliced here with exactly the same
//     pageOffset/pageSize semantics as the unfiltered path.
//   - Each id is read ONCE and the row is built in the same pass, which is the
//     "avoid reading the same record twice" requirement: total is the length of
//     the collected matches, not a second enumeration.
//
// Cost and scope: O(N) GetWorkflow calls per request, where N is the number of
// workflows in the caller's OWN namespace (the registry refuses any other
// namespace, and the per-record namespace check below is the same defense the
// unfiltered path applies). No client-supplied namespace participates. The
// result order is the registry's enumeration order (newest revision first, then
// id ascending), identical to the unfiltered ordering.
func (m *workflowControlModule) workflowListItemsFiltered(ctx context.Context, registry backend.WorkflowRegistry, ns namespace.Namespace, q string, page, pageSize int) ([]workflowListItem, int, error) {
	ids, err := registry.ListWorkflows(ctx, ns, backend.WorkflowListOptions{})
	if err != nil {
		return nil, 0, err
	}
	needle := strings.ToLower(q)
	matched := make([]workflowListItem, 0, len(ids))
	for _, id := range ids {
		rec, err := registry.GetWorkflow(ctx, id)
		if err != nil {
			if errors.Is(err, backend.ErrWorkflowNotFound) {
				continue
			}
			return nil, 0, err
		}
		if rec.Namespace != "" && ns != "" && rec.Namespace != string(ns) {
			continue
		}
		if !workflowNameMatches(rec.Name, needle) {
			continue
		}
		matched = append(matched, workflowListItemFrom(rec))
	}

	total := len(matched)
	offset := pageOffset(page, pageSize)
	if offset >= total {
		return []workflowListItem{}, total, nil
	}
	end := offset + pageSize
	if end > total {
		end = total
	}
	pageItems := matched[offset:end]
	return pageItems, total, nil
}

// workflowNameMatches reports whether name contains needle, case-insensitively.
// needle must already be lower-cased; the caller lower-cases the query once
// instead of once per record.
func workflowNameMatches(name, needle string) bool {
	return strings.Contains(strings.ToLower(name), needle)
}

// workflowListItemFrom projects one registry record onto the list row shape.
// Triggers are derived from the stored definition's nodes; the compiled Graph
// is deliberately not consulted (it is the runtime projection, not the stored
// definition, and a list read must not depend on it).
func workflowListItemFrom(rec backend.WorkflowRecord) workflowListItem {
	item := workflowListItem{
		ID:               string(rec.ID),
		Name:             rec.Name,
		Version:          rec.Version,
		DefinitionHash:   rec.DefinitionHash,
		RegistryRevision: rec.RegistryRevision,
		Triggers:         workflowTriggerKinds(rec.Definition),
	}
	if !rec.UpdatedAt.IsZero() {
		updated := rec.UpdatedAt
		item.UpdatedAt = &updated
	}
	if rec.Definition != nil {
		item.Description = rec.Definition.Description
		item.CronExpressions = workflowCronExpressions(rec.Definition)
	}
	return item
}

// triggerNodeTypePrefix is the node-type namespace of the builtin triggers.
// Only node types below it are reported in WorkflowListItem.triggers; the
// entry marker xflow.start is NOT a trigger and does not match.
const triggerNodeTypePrefix = "xflow.trigger."

// workflowTriggerKinds returns the distinct trigger kinds named by def, sorted
// lexicographically so the field is stable across reads.
//
// A nil definition (a record whose definition was never stored, or a fixture)
// has no triggers and yields [] — never nil, because the field is always
// serialized and a client must never receive null for an array. The same
// non-nil guarantee applies to a definition with no trigger nodes.
//
// "Kind" here is the node-type suffix (timer, cron, webhook, kafka, redis).
// The suffix is taken verbatim: an unknown xflow.trigger.* type is reported as
// its own kind rather than dropped, so a future trigger kind becomes visible
// without an apiserver change, and a definition carrying a typo is visible as
// that typo instead of silently looking trigger-less.
func workflowTriggerKinds(def *types.WorkflowDef) []string {
	kinds := make([]string, 0)
	if def == nil {
		return kinds
	}
	seen := make(map[string]struct{}, len(def.Nodes))
	for _, n := range def.Nodes {
		kind, ok := strings.CutPrefix(n.Type, triggerNodeTypePrefix)
		if !ok || kind == "" {
			continue
		}
		if _, dup := seen[kind]; dup {
			continue
		}
		seen[kind] = struct{}{}
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	return kinds
}

// cronTriggerNodeType is the one trigger kind whose expression is projected on
// the workflow list: it is the only trigger whose schedule is a short,
// user-meaningful string a table row can render.
const cronTriggerNodeType = "xflow.trigger.cron"

// workflowCronExpressions returns the distinct `expression` parameters of the
// definition's cron trigger nodes, sorted lexicographically so the field is
// stable across reads.
//
// Only the expression leaves the server: a cron node's other parameters are
// not part of a list row's contract. A node with a missing, non-string or
// blank expression contributes nothing, and a definition with no cron node
// yields nil — the field is omitted rather than serialized as an empty list.
func workflowCronExpressions(def *types.WorkflowDef) []string {
	if def == nil {
		return nil
	}
	var out []string
	seen := make(map[string]struct{})
	for _, n := range def.Nodes {
		if n.Type != cronTriggerNodeType {
			continue
		}
		expression, ok := n.Parameters["expression"].(string)
		if !ok {
			continue
		}
		expression = strings.TrimSpace(expression)
		if expression == "" {
			continue
		}
		if _, dup := seen[expression]; dup {
			continue
		}
		seen[expression] = struct{}{}
		out = append(out, expression)
	}
	sort.Strings(out)
	return out
}

// attachLatestExecutions fills LatestExecution on the given rows with ONE
// batch store call for the whole page.
//
// Every failure mode degrades to "field absent" and never fails the list:
//
//   - no execution store wired (dev/embedded without Config.Store) — absent;
//   - a store that does not implement the optional batch reader — absent;
//   - a read error — logged under workflow_list_latest_execution_failed and
//     absent. A page that renders without one column is strictly better than a
//     500 that renders no page, and the failure is visible in the log.
//
// Security: the namespace passed to the store is the one the handler resolved
// from the authenticated principal's context — there is no client-supplied
// namespace anywhere in this path. Because it is a defense-in-depth layer on
// top of the store's own scoping, a returned record is only projected when its
// persisted Namespace is exactly this ns and its persisted WorkflowID matches
// the row it is attached to. That second check is what makes crossing two
// workflows' rows (a map keyed wrongly by a broken backend) a visible omission
// rather than one workflow showing another's latest run.
func (m *workflowControlModule) attachLatestExecutions(ctx context.Context, ns namespace.Namespace, items []workflowListItem) {
	if len(items) == 0 || m.executions == nil {
		return
	}
	reader, ok := m.executions.(store.LatestExecutionReader)
	if !ok {
		return
	}
	ids := make([]types.WorkflowID, 0, len(items))
	for _, item := range items {
		ids = append(ids, types.WorkflowID(item.ID))
	}
	latest, err := reader.LatestExecutions(ctx, ns, ids)
	if err != nil {
		// Only the event, the caller's namespace and the error are logged: the
		// rows' contents (names, hashes) must not reach the log.
		if m.log != nil {
			m.log.Error("workflow_list_latest_execution_failed", "namespace", string(ns), "err", err)
		}
		return
	}
	for i := range items {
		id := types.WorkflowID(items[i].ID)
		rec := latest[id]
		if rec == nil {
			continue
		}
		// Fail closed on a foreign or mis-keyed record: the store's scope and
		// the map key are the authority, and this only ever withholds.
		if rec.Namespace != string(ns) || rec.WorkflowID != id {
			continue
		}
		items[i].LatestExecution = &workflowLatestExecution{
			ExecutionID: string(rec.ExecutionID),
			Status:      rec.Status,
			CreatedAt:   rec.CreatedAt,
			UpdatedAt:   rec.UpdatedAt,
		}
	}
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
// (workflow_id + workflow_name), where it is (status), how it ended (error),
// and when it was created / last touched.
//
// workflow_id is the identity and workflow_name is the display string. The row
// stores both; `?workflow_id=` narrows on the id, because a name is not an
// identity (two namespaces can hold the same name, and it can change under the
// same id). A row recorded without a definition id projects workflow_id "" —
// unattributed, and matched by no workflow filter.
type executionListItem struct {
	ExecutionID  string                `json:"execution_id"`
	Namespace    string                `json:"namespace"`
	WorkflowID   string                `json:"workflow_id"`
	WorkflowName string                `json:"workflow_name"`
	Status       types.ExecutionStatus `json:"status"`
	Error        string                `json:"error"`
	CreatedAt    time.Time             `json:"created_at"`
	UpdatedAt    time.Time             `json:"updated_at"`
	// QueuePosition is the row's 1-based place in its workflow's concurrency
	// queue, present only while status is queued. It is not a stored column —
	// the queue owner holds it — so the handler reads it back for the queued
	// rows of a page and leaves it 0 for every other row. The cost is bounded:
	// at most one InspectStatus per queued row, and InspectStatus is the
	// status-only projection that does not walk the graph. 0 / absent means
	// UNKNOWN, never "head of the line": the head is 1.
	QueuePosition int `json:"queue_position,omitempty"`
}

// executionListFilterFrom parses and validates everything about a list request
// that is a business input rather than a page number: the status filter, the
// workflow_id identity filter, and the created_after / created_before bounds.
//
// These are the filters the data model actually supports. The store's doc on
// ListExecutions spells out the remaining rejection at column level: runner_id
// is not a column of xflow_executions at all (it lives in the enrollment/identity
// tables and in per-node lease columns, none of which is a per-execution
// projection of "who ran this"). Advertising it would promise a narrowing the
// store cannot perform — an unrecognized query parameter is silently ignored,
// so the caller would get the whole namespace back and read it as a filtered
// page. A workflow NAME filter is likewise not offered: workflow_name is a
// display string, not an identity (two namespaces can hold the same name and it
// is not immutable across a re-registration); workflow_id is the identity.
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
//     direction that costs data, not convenience;
//   - an over-long workflow_id can never equal a stored id (the column is
//     VARCHAR(64)); ignoring it would answer with the whole namespace as if it
//     were the filtered page.
//
// ok=false means the handler has already written the failure response.
//
// The returned filter is passed straight to the store, which re-validates the
// status and the workflow id itself: this function is the HTTP-layer mapping,
// not a replacement for the store's own refusal.
func executionListFilterFrom(w http.ResponseWriter, r *http.Request) (store.ExecutionFilter, bool) {
	q := r.URL.Query()

	var filter store.ExecutionFilter
	if raw := q.Get("status"); raw != "" {
		status := types.ExecutionStatus(raw)
		if !knownExecutionListStatus(status) {
			writeFail(w, r, http.StatusBadRequest, "execution_status_invalid",
				"status must be one of pending, queued, running, success, failed, canceling, canceled, timeout")
			return store.ExecutionFilter{}, false
		}
		filter.Status = status
	}

	// Empty means "no workflow narrowing", the same convention status uses. A
	// value that cannot match a stored row is refused rather than ignored, so a
	// typo cannot read back as an unfiltered page.
	if raw := q.Get("workflow_id"); raw != "" {
		if err := store.ValidateExecutionWorkflowIDFilter(raw); err != nil {
			writeFail(w, r, http.StatusBadRequest, "execution_workflow_id_invalid",
				"workflow_id must be at most 64 characters")
			return store.ExecutionFilter{}, false
		}
		filter.WorkflowID = types.WorkflowID(raw)
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
		types.ExecutionStatusQueued,
		types.ExecutionStatusPaused,
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
// Queue position: a queued row carries its live place in line (1-based) in
// `queue_position`, read back per row from the queue owner rather than stored.
// The cost is bounded by the page: at most pageSize status reads, issued only
// for rows the page says are queued, and each is InspectStatus — the
// graph-free projection — not Inspect. A read that fails omits the field; it
// never fails the page and never changes a row's status. Absent/0 means
// UNKNOWN, not the head of the line.
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
	// queueReadFailures counts the queued rows whose place in line could not
	// be read. It is reported once for the page, below: a queue owner that is
	// down would otherwise turn one page into pageSize identical log lines,
	// which is noise that hides the other rows' diagnostics rather than more
	// information about this one.
	var queueReadFailures int
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
		item := executionListItem{
			ExecutionID:  string(rec.ExecutionID),
			Namespace:    rec.Namespace,
			WorkflowID:   string(rec.WorkflowID),
			WorkflowName: rec.WorkflowName,
			Status:       rec.Status,
			Error:        rec.Error,
			CreatedAt:    rec.CreatedAt,
			UpdatedAt:    rec.UpdatedAt,
		}
		// The place in line is the one column of a row that is not stored: it
		// belongs to the live queue, so it is read back per queued row. The
		// read is best effort for the same reason submissionResponse's is —
		// the row exists and its other columns are the answer; a position that
		// could not be read is a missing detail, not a failed page. Re-reading
		// the STATUS as well as the position is deliberate: the row said
		// queued a moment ago, and an execution promoted or finished since
		// then must not report the position it no longer has.
		if rec.Status == types.ExecutionStatusQueued && m.eng != nil {
			detail, err := m.eng.InspectStatus(r.Context(), rec.ExecutionID)
			switch {
			case err != nil:
				queueReadFailures++
			case detail.Status == types.ExecutionStatusQueued:
				item.QueuePosition = detail.QueuePosition
			}
		}
		items = append(items, item)
	}
	if queueReadFailures > 0 && m.log != nil {
		m.log.Error("list_executions_queue_position_unavailable",
			"failed_rows", queueReadFailures, "page_rows", len(recs))
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
	writeData(w, r, http.StatusOK, workflowResponseBody(rec))
}

// rollbackWorkflowRequest is the body of POST /v1/workflows/{id}/rollback
// (versioning design §2.4). An omitted or empty version means the current
// version, which is the no-op branch.
type rollbackWorkflowRequest struct {
	Version string `json:"version,omitempty"`
}

// WorkflowRollbackResult reports the outcome of a rollback. Unchanged is the
// idempotent no-op — the requested version already is the current one, or a
// replayed rollback found it installed — mirroring the identical-PUT no-op.
//
// It is exported and shared by the HTTP route and APIServer.RollbackWorkflow,
// so the wire shape and the in-process result cannot drift apart.
type WorkflowRollbackResult struct {
	WorkflowID types.WorkflowID `json:"workflow_id"`
	Version    string           `json:"version"`
	Unchanged  bool             `json:"unchanged"`
}

// handleRollbackWorkflow serves POST /v1/workflows/{id}/rollback (versioning
// design §2.4): install a retired version of the addressed workflow as its
// current record, under the same id.
//
// The namespace comes from the verified principal, the version from the body,
// and the mutation id from X-Request-Id when the client supplies one — exactly
// like PUT /v1/workflows/{id}, because a rollback IS a replacement and inherits
// that route's idempotent-replay contract: replaying the same request with the
// same X-Request-Id does not produce a second revision.
func (m *workflowControlModule) handleRollbackWorkflow(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	id := types.WorkflowID(r.PathValue("id"))
	if id == "" {
		writeFail(w, r, http.StatusNotFound, "workflow_not_found", "workflow not found")
		return
	}
	var req rollbackWorkflowRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	mutationID := sanitizeRequestID(r.Header.Get("X-Request-Id"))
	if mutationID != "" {
		mutationID = "http:" + mutationID
	}
	out, err := m.rollbackWorkflow(r.Context(), namespace.FromContext(r.Context()), id, req.Version, mutationID)
	if err != nil {
		switch {
		case errors.Is(err, backend.ErrWorkflowNotFound):
			writeFail(w, r, http.StatusNotFound, "workflow_not_found", "workflow not found")
		case errors.Is(err, backend.ErrWorkflowVersionNotFound):
			writeFail(w, r, http.StatusNotFound, "workflow_version_not_found", "workflow version not found")
		case errors.Is(err, backend.ErrWorkflowVersionUnavailable):
			writeFail(w, r, http.StatusConflict, "workflow_version_unavailable", "workflow version is not reproducible under the current compiler")
		default:
			if writeWorkflowConflict(w, r, err) {
				return
			}
			if m.log != nil {
				m.log.Error("rollback_workflow_failed", "workflow_id", string(id), "version", req.Version, "err", err)
			}
			writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		}
		return
	}
	writeData(w, r, http.StatusOK, out)
}

// workflowVersionItem is one row of the GET /v1/workflows/{id}/versions page.
// It is a label projection of backend.WorkflowVersionInfo, mirroring the
// workflowListItem/workflowRecord split.
//
// The definition and the compiled graph are deliberately absent, for the same
// reason a workflow list row omits them: a page of full definitions is
// O(page × definition size) on the wire, and the version list exists to show
// which labels exist and which one is live. What a row needs is the label
// (version), a change detector (definition_hash), the revision the version was
// live at (registry_revision, which is also its retirement order), and the two
// state flags. The definition of one version is read with the detail route
// GET /v1/workflows/{id}/versions/{version}.
type workflowVersionItem struct {
	Version          string `json:"version"`
	DefinitionHash   string `json:"definition_hash"`
	RegistryRevision uint64 `json:"registry_revision"`
	// Current marks the single version the live record represents; Archived
	// marks a retired version held in history. They are complementary, and
	// both are carried so a row's state is readable without inferring it from
	// position.
	Current  bool `json:"current"`
	Archived bool `json:"archived"`
}

// listWorkflowVersions resolves the version history of the workflow addressed
// by id, in the registry capability's order: current version first, then
// retired versions newest-retirement first.
//
// It is the one implementation behind the HTTP route and
// APIServer.ListWorkflowVersions, for the same reason registerWorkflow is:
// the namespace scope check and the capability assertion are policy, and a
// second copy would be free to skip one.
func (m *workflowControlModule) listWorkflowVersions(ctx context.Context, ns namespace.Namespace, id types.WorkflowID) ([]backend.WorkflowVersionInfo, error) {
	registry := m.registry()
	if registry == nil {
		if m.log != nil {
			m.log.Error("list_workflow_versions_no_registry", "workflow_id", string(id))
		}
		return nil, errors.New("apiserver: no workflow registry configured")
	}
	// Resolve the live record first, exactly as handleGetWorkflow does: the
	// namespace comparison below is the authoritative IDOR defense and has to
	// happen before any history is read. It cannot be delegated to the
	// capability — the in-memory registry's GetWorkflow ignores the context
	// namespace, so its history would otherwise be readable across namespaces
	// by anyone who guessed or leaked an id.
	rec, err := registry.GetWorkflow(ctx, id)
	if err != nil {
		return nil, err
	}
	if rec.Namespace != "" && ns != "" && rec.Namespace != string(ns) {
		return nil, backend.ErrWorkflowNotFound
	}
	versions, ok := registry.(backend.WorkflowVersionCapability)
	if !ok {
		if m.log != nil {
			m.log.Error("list_workflow_versions_capability_missing", "workflow_id", string(id))
		}
		return nil, errWorkflowVersionsUnsupported
	}
	return versions.ListWorkflowVersions(ctx, id)
}

// handleListWorkflowVersions serves GET /v1/workflows/{id}/versions (versioning
// design §2.2): the version history of one registered workflow.
//
// Namespace: resolved server-side from the authenticated principal
// (namespace.FromContext), never from the query string; a cross-namespace id
// resolves to 404 workflow_not_found, same as GET /v1/workflows/{id}.
//
// Pagination: offset, 1-based, through the shared pageParams/pageOffset helpers
// (default 20, hard cap 200). The history is bounded by the registry's
// retention limit (backend.DefaultWorkflowVersionRetention plus the live
// record), so paging is a presentation concern, not a traversal guard — but it
// is the same dialect as every other page list, not a second one.
//
// Envelope: writeList — the mandated {list,total} collection shape. total is
// the exact history size, independent of the page requested.
func (m *workflowControlModule) handleListWorkflowVersions(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	id := types.WorkflowID(r.PathValue("id"))
	if id == "" {
		writeFail(w, r, http.StatusNotFound, "workflow_not_found", "workflow not found")
		return
	}
	versions, err := m.listWorkflowVersions(r.Context(), namespace.FromContext(r.Context()), id)
	if err != nil {
		if errors.Is(err, backend.ErrWorkflowNotFound) {
			writeFail(w, r, http.StatusNotFound, "workflow_not_found", "workflow not found")
			return
		}
		if m.log != nil {
			m.log.Error("list_workflow_versions_failed", "workflow_id", string(id), "err", err)
		}
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	page, pageSize := pageParams(r)
	start := pageOffset(page, pageSize)
	if start > len(versions) {
		start = len(versions)
	}
	end := start + pageSize
	if end > len(versions) {
		end = len(versions)
	}
	items := make([]workflowVersionItem, 0, end-start)
	for _, v := range versions[start:end] {
		items = append(items, workflowVersionItem{
			Version:          v.Version,
			DefinitionHash:   v.DefinitionHash,
			RegistryRevision: v.RegistryRevision,
			Current:          v.Current,
			Archived:         v.Archived,
		})
	}
	writeList(w, r, items, len(versions))
}

// getWorkflowVersion resolves one version of the workflow addressed by id.
// version may name the current version — which resolves to the live record
// itself — or a retired one, which resolves to the payload captured when it
// was retired. An empty version is treated as the current one.
//
// It is listWorkflowVersions' read-one sibling, and carries the same namespace
// scope check for the same reason: the in-memory registry does not scope its
// lookups by the context namespace, so the IDOR defense has to sit above the
// capability. The capability's own contract distinguishes ErrWorkflowNotFound
// (the workflow has no live record) from ErrWorkflowVersionNotFound (it exists
// but never published that version), and both pass through unwrapped.
func (m *workflowControlModule) getWorkflowVersion(ctx context.Context, ns namespace.Namespace, id types.WorkflowID, version string) (backend.WorkflowRecord, error) {
	registry := m.registry()
	if registry == nil {
		if m.log != nil {
			m.log.Error("get_workflow_version_no_registry", "workflow_id", string(id))
		}
		return backend.WorkflowRecord{}, errors.New("apiserver: no workflow registry configured")
	}
	rec, err := registry.GetWorkflow(ctx, id)
	if err != nil {
		return backend.WorkflowRecord{}, err
	}
	if rec.Namespace != "" && ns != "" && rec.Namespace != string(ns) {
		return backend.WorkflowRecord{}, backend.ErrWorkflowNotFound
	}
	if version == "" || version == rec.Version {
		return rec, nil
	}
	versions, ok := registry.(backend.WorkflowVersionCapability)
	if !ok {
		if m.log != nil {
			m.log.Error("get_workflow_version_capability_missing", "workflow_id", string(id))
		}
		return backend.WorkflowRecord{}, errWorkflowVersionsUnsupported
	}
	return versions.GetWorkflowVersion(ctx, id, version)
}

// archivedWorkflowVersion resolves a retired version of the workflow addressed
// by id, verified against the current compiler before any caller acts on it: it
// recomputes the runtime hash from the stored definition and requires it to
// equal the hash the version was published with (versioning design Q10).
//
// A version label is immutable, so a mismatch means hashing or compilation
// semantics have drifted since the version was published — the archived
// payload would execute as the same label with different semantics. That is
// exactly what the label promises will not happen, so both consumers of this
// helper (execute-by-version and rollback) fail closed with
// backend.ErrWorkflowVersionUnavailable rather than silently install or run a
// divergent definition.
//
// Execution then proceeds from the archived graph snapshot (rec.Graph) as
// captured at retirement; the payload is never recompiled.
func (m *workflowControlModule) archivedWorkflowVersion(ctx context.Context, ns namespace.Namespace, id types.WorkflowID, version string) (backend.WorkflowRecord, error) {
	rec, err := m.getWorkflowVersion(ctx, ns, id, version)
	if err != nil {
		return backend.WorkflowRecord{}, err
	}
	hash, err := workflowhash.Runtime(rec.Definition, hashParamSpecs)
	if err != nil {
		// A definition the current compiler cannot even hash cannot be
		// confirmed as the published one; that is the same fail-closed answer.
		if m.log != nil {
			m.log.Error("archived_workflow_version_hash_failed", "workflow_id", string(id), "version", version, "err", err)
		}
		return backend.WorkflowRecord{}, fmt.Errorf("%w: version %q: %v", backend.ErrWorkflowVersionUnavailable, version, err)
	}
	if hash != rec.DefinitionHash {
		if m.log != nil {
			m.log.Error("archived_workflow_version_hash_drift",
				"workflow_id", string(id), "version", version,
				"stored_hash", rec.DefinitionHash, "current_hash", hash)
		}
		return backend.WorkflowRecord{}, fmt.Errorf("%w: version %q", backend.ErrWorkflowVersionUnavailable, version)
	}
	return rec, nil
}

// handleGetWorkflowVersion serves GET /v1/workflows/{id}/versions/{version}
// (versioning design §2.2): one version's full record, in the same wire shape
// as GET /v1/workflows/{id} — this is where the definition the version list
// deliberately omits is read back.
//
// The current version resolves to the live record; a retired version resolves
// to the payload captured when it was retired, so a version remains readable
// (and pinnable for a replay) after it has been replaced. An unknown version is
// 404 workflow_version_not_found — deliberately distinct from workflow_not_found
// so a caller can tell "this workflow is gone" from "this workflow never
// published that version". A cross-namespace id is reported as
// workflow_not_found, exactly as the by-id read does.
func (m *workflowControlModule) handleGetWorkflowVersion(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	id := types.WorkflowID(r.PathValue("id"))
	version := r.PathValue("version")
	if id == "" {
		writeFail(w, r, http.StatusNotFound, "workflow_not_found", "workflow not found")
		return
	}
	if version == "" {
		writeFail(w, r, http.StatusNotFound, "workflow_version_not_found", "workflow version not found")
		return
	}
	rec, err := m.getWorkflowVersion(r.Context(), namespace.FromContext(r.Context()), id, version)
	if err != nil {
		switch {
		case errors.Is(err, backend.ErrWorkflowNotFound):
			writeFail(w, r, http.StatusNotFound, "workflow_not_found", "workflow not found")
		case errors.Is(err, backend.ErrWorkflowVersionNotFound):
			writeFail(w, r, http.StatusNotFound, "workflow_version_not_found", "workflow version not found")
		default:
			if m.log != nil {
				m.log.Error("get_workflow_version_failed", "workflow_id", string(id), "version", version, "err", err)
			}
			writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		}
		return
	}
	writeData(w, r, http.StatusOK, workflowResponseBody(rec))
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
	def, metadata, ok := decodeWorkflowDefinitionWithMetadata(w, r)
	if !ok {
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
	replacedID, diag, err := m.replaceWorkflowByIDWithMetadata(r.Context(), ns, id, def, mutationID, metadata)
	if err != nil {
		if writeParamInvalid(w, r, err) {
			return
		}
		var compileErr *WorkflowCompileError
		if errors.As(err, &compileErr) {
			writeFail(w, r, http.StatusBadRequest, "workflow_compile_failed", compileErr.Unwrap().Error())
			return
		}
		if errors.Is(err, backend.ErrWorkflowNotFound) {
			writeFail(w, r, http.StatusNotFound, "workflow_not_found", "workflow not found")
			return
		}
		if writeWorkflowConflict(w, r, err) {
			return
		}
		if errors.Is(err, errWorkflowIDMismatch) {
			writeFail(w, r, http.StatusBadRequest, "workflow_id_mismatch", "workflow id does not match path")
			return
		}
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeData(w, r, http.StatusOK, registerWorkflowResponse{WorkflowID: replacedID, Warnings: diag.Warnings, ParamIssues: diag.ParamIssues})
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
	// An archived version runs from its history payload instead of the live
	// record (versioning design §2.3). req.Version empty or equal to the
	// current version keeps the live record above — byte-for-byte the behavior
	// this route had before versions existed — and only a version the live
	// record does not carry reaches the history lookup, whose failure codes the
	// switch maps. The model name in every branch is rec.Name's, never the
	// body's, so the version field cannot point the call at another workflow.
	// Retention may evict the oldest retired versions, so the lookup can also
	// fail for a version that once existed: workflow_version_not_found is the
	// designed answer there, and because this resolution precedes the
	// idempotency lookup below, a keyed retry pinned to an evicted version
	// fails closed instead of replaying the original execution as a duplicate.
	if req.Version != "" && req.Version != rec.Version {
		rec, err = m.archivedWorkflowVersion(r.Context(), ns, id, req.Version)
		if err != nil {
			switch {
			case errors.Is(err, backend.ErrWorkflowNotFound):
				writeFail(w, r, http.StatusNotFound, "workflow_not_found", "workflow not found")
			case errors.Is(err, backend.ErrWorkflowVersionNotFound):
				writeFail(w, r, http.StatusNotFound, "workflow_version_not_found", "workflow version not found")
			case errors.Is(err, backend.ErrWorkflowVersionUnavailable):
				writeFail(w, r, http.StatusConflict, "workflow_version_unavailable", "workflow version is not reproducible under the current compiler")
			default:
				if m.log != nil {
					m.log.Error("execute_workflow_version_failed", "workflow_id", string(id), "version", req.Version, "err", err)
				}
				writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
			}
			return
		}
	}
	// Attach the stored definition so the durable SQL execution projection can
	// persist workflow_def (NOT NULL) — mirroring the inline-execute path. The
	// path id is the authoritative identity of this execution: always stamp it
	// onto a copy, overriding any id a legacy stored definition still carries,
	// so a client-supplied definition id can never hijack attribution. The
	// registry's record is never mutated.
	var subDef *types.WorkflowDef
	if rec.Definition != nil {
		stamped := *rec.Definition
		stamped.ID = string(id)
		subDef = &stamped
	}
	ctx := engine.WithWorkflowDef(r.Context(), subDef)
	ctx = engine.WithTraceCarrier(ctx, tracing.InjectCarrier(ctx))
	ctx = engine.WithTestRun(ctx, req.Test)
	// A keyed request is scoped to the registered workflow, so one key means one
	// execution whichever face (this route, the SDK) a retry comes in through.
	// The version is not in the scope: a retry after an upgrade has to find the
	// original execution, and the definition's version is part of the
	// fingerprint instead.
	idem, ok := keyedSubmission(w, r, engine.WorkflowIdempotencyScope(id), "")
	if !ok {
		return
	}
	if idem != nil {
		ctx = engine.WithIdempotency(ctx, idem)
	}
	var execID types.ExecutionID
	if req.Entry != "" {
		execID, err = m.eng.Invoke(ctx, rec.Graph, req.Entry, req.Input)
		if err != nil {
			if !writeSubmitFailure(w, r, err) {
				writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
			}
			return
		}
	} else {
		execID, err = m.eng.Submit(ctx, rec.Graph, req.Params)
		if err != nil {
			if !writeSubmitFailure(w, r, err) {
				writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
			}
			return
		}
	}
	resp := m.submissionResponse(ctx, execID, nil)
	resp.Duplicate = idem != nil && idem.Duplicate
	writeData(w, r, http.StatusOK, resp)
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
	// Clear the schedules' overlays BEFORE removing the registry record, in the
	// same order — and for the same reason — as the entry-activation clear
	// above: a partial failure then leaves work a retry finishes, instead of an
	// orphan nothing can reach. An overlay outliving its workflow is exactly that
	// kind of orphan: every schedule route resolves the workflow first, so once
	// the record is gone the leftover row is unreachable and invisible, and it
	// would be silently inherited if the id were ever registered again.
	//
	// The cost of this order is that a failed removal leaves the workflow
	// running with its schedules back on the definition's timing. That is
	// visible (the schedule API reports the overlay gone) and correctable (the
	// operator re-applies it), which is the trade the activation clear already
	// makes.
	if m.schedules != nil {
		if _, err := m.schedules.DeleteWorkflowOverrides(ctx, ns, id); err != nil {
			if m.log != nil {
				m.log.Error("deregister_workflow_clear_schedules_failed", "err", err)
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

// deregisterWorkflowByKey resolves (ns, name, version) through the registry's
// key index and removes the record by id.
//
// It exists for an embedded host that knows the identity it registered under —
// the name and version it derives from its own configuration — but not the id,
// which the server issues as a UUID. Resolution is scoped to ns and matched on
// the exact key the registration path writes, so a host can never reach another
// namespace's record by name. An unknown key is backend.ErrWorkflowNotFound.
//
// The removal itself is deregisterWorkflow, so the
// clear-activations-before-remove ordering contract is identical to
// DELETE /v1/workflows/{id}.
func (m *workflowControlModule) deregisterWorkflowByKey(ctx context.Context, ns namespace.Namespace, name, version string) error {
	if name == "" || version == "" {
		return backend.ErrWorkflowNotFound
	}
	registry := m.registry()
	if registry == nil {
		if m.log != nil {
			m.log.Error("deregister_workflow_no_registry")
		}
		return errors.New("apiserver: no workflow registry configured")
	}
	rec, err := registry.GetWorkflowByKey(ctx, workflowRegistryKey(string(ns), name, version))
	if err != nil {
		if !errors.Is(err, backend.ErrWorkflowNotFound) && m.log != nil {
			m.log.Error("deregister_workflow_key_lookup_failed", "err", err)
		}
		return err
	}
	return m.deregisterWorkflow(ctx, ns, rec.ID)
}

// replaceWorkflow registers def, and if a DIFFERENT definition already occupies
// its (namespace, name, version) key, atomically supersedes it. The embedded API
// deliberately gives a changed definition a new ID; HTTP PUT, by contrast,
// preserves its path ID in replaceWorkflowByID below.
func (m *workflowControlModule) replaceWorkflow(ctx context.Context, ns namespace.Namespace, def *types.WorkflowDef) (id types.WorkflowID, diag registrationDiagnostics, err error) {
	return m.replaceWorkflowWithMetadata(ctx, ns, def, nil)
}

// replaceWorkflowWithMetadata is replaceWorkflow, additionally accepting the
// editor_metadata sibling an HTTP POST /v1/workflows body may carry when it
// resolves to a replace (§D2). Embedded/SDK callers have no such input and go
// through replaceWorkflow, which passes nil.
func (m *workflowControlModule) replaceWorkflowWithMetadata(ctx context.Context, ns namespace.Namespace, def *types.WorkflowDef, metadata *types.WorkflowEditorMetadata) (id types.WorkflowID, diag registrationDiagnostics, err error) {
	defer func() { m.observeRegistration(ctx, ns, "replace", err) }()
	registry := m.registry()
	if registry == nil {
		return "", registrationDiagnostics{}, errors.New("apiserver: no workflow registry configured")
	}
	replacement, diag, err := m.buildWorkflowRecord(ctx, ns, "", def, metadata)
	if err != nil {
		return "", registrationDiagnostics{}, err
	}
	ctx = namespace.WithNamespace(ctx, ns)
	existing, err := registry.GetWorkflowByKey(ctx, replacement.Key)
	if errors.Is(err, backend.ErrWorkflowNotFound) {
		return m.addWorkflowRecord(ctx, ns, registry, replacement, diag)
	}
	if err != nil {
		return "", registrationDiagnostics{}, err
	}
	if sameAuditFingerprint(existing, replacement) {
		return m.addWorkflowRecord(ctx, ns, registry, replacement, diag)
	}

	// A replacement ID must be fixed before the CAS so response-loss retries use
	// the same semantic request fingerprint instead of allocating a second ID.
	replacement.ID = types.WorkflowID(uuid.NewString())
	return m.compareAndReplaceWorkflow(ctx, ns, registry, existing, replacement, "embedded:"+uuid.NewString(), diag)
}

// replaceWorkflowByID implements HTTP PUT /v1/workflows/{id}. The path ID is
// authoritative; name/version may rename the record only when the destination
// key is free. All conflict checks and index changes occur in the registry's
// single atomic compare-and-swap rather than a racy lookup/remove/add sequence.
func (m *workflowControlModule) replaceWorkflowByID(ctx context.Context, ns namespace.Namespace, id types.WorkflowID, def *types.WorkflowDef, mutationID string) (newID types.WorkflowID, diag registrationDiagnostics, err error) {
	return m.replaceWorkflowByIDWithMetadata(ctx, ns, id, def, mutationID, nil)
}

// replaceWorkflowByIDWithMetadata is replaceWorkflowByID, additionally
// accepting the editor_metadata sibling the HTTP PUT /v1/workflows/{id} body
// may carry (§D2). A metadata-only change still differs under
// sameAuditFingerprint, so it is written as a normal new revision (D3); the
// runtime hash and compiled graph are unaffected either way.
func (m *workflowControlModule) replaceWorkflowByIDWithMetadata(ctx context.Context, ns namespace.Namespace, id types.WorkflowID, def *types.WorkflowDef, mutationID string, metadata *types.WorkflowEditorMetadata) (newID types.WorkflowID, diag registrationDiagnostics, err error) {
	defer func() { m.observeRegistration(ctx, ns, "replace_by_id", err) }()
	if id == "" {
		return "", registrationDiagnostics{}, backend.ErrWorkflowNotFound
	}
	registry := m.registry()
	if registry == nil {
		if m.log != nil {
			m.log.Error("replace_workflow_no_registry")
		}
		return "", registrationDiagnostics{}, errors.New("apiserver: no workflow registry configured")
	}

	ctx = namespace.WithNamespace(ctx, ns)
	existing, err := registry.GetWorkflow(ctx, id)
	if err != nil {
		if !errors.Is(err, backend.ErrWorkflowNotFound) && m.log != nil {
			m.log.Error("replace_workflow_lookup_failed", "workflow_id", string(id), "err", err)
		}
		return "", registrationDiagnostics{}, err
	}
	if existing.Namespace != "" && ns != "" && existing.Namespace != string(ns) {
		return "", registrationDiagnostics{}, backend.ErrWorkflowNotFound
	}
	if def.ID != "" && def.ID != string(id) {
		return "", registrationDiagnostics{}, errWorkflowIDMismatch
	}
	def.ID = string(id)
	def.Namespace = string(ns)
	replacement, diag, err := m.buildWorkflowRecord(ctx, ns, id, def, metadata)
	if err != nil {
		return "", registrationDiagnostics{}, err
	}
	// An identical PUT is the no-op replace, not a new revision (Doc C §5.1 #2).
	// The shortcut must not pre-empt operation-ledger replay: a record whose
	// stored definition already carries the path id was last written by PUT,
	// possibly under this very client mutation id, and a retry of that
	// mutation must replay through CompareAndReplaceWorkflow (which is the
	// ledger lookup). A POST-written record (no stored id) has no HTTP ledger
	// entry, and a request without a client mutation id gets a fresh one that
	// no ledger entry can match, so both are safe to short-circuit. A
	// PUT-written record may still have an unprojected intent from a mutation
	// that committed but failed to project; a retry then falls through to the
	// CAS so it projects the current definition instead of reporting success
	// while activations lag.
	if unchangedExceptStampedID(existing, replacement, id) &&
		(existing.Definition.ID == "" || (mutationID == "" && !m.activationProjectionMayBePending(ctx, ns, registry))) {
		return existing.ID, diag, nil
	}
	if mutationID == "" {
		mutationID = "http:" + uuid.NewString()
	}
	return m.compareAndReplaceWorkflow(ctx, ns, registry, existing, replacement, mutationID, diag)
}

// rollbackWorkflow installs a retired version as the workflow's current
// record, under the same id, through the same atomic compare-and-swap PUT
// /v1/workflows/{id} uses (versioning design §2.4). Reusing the CAS is the
// point: activation projection, index rescoring, and idempotent mutation replay
// all come for free, and the replaced current version is appended to history by
// the same script — the exact dual of the retirement that put the target there.
//
// The installed record is the archived payload with only its identity fixed to
// the live record: same id, key ns/<live name>@<version>, the name the live
// record currently holds. Its definition hash is the hash the version was
// PUBLISHED with, never a recomputation — archivedWorkflowVersion has already
// proven a recomputation yields that hash, and if it does not, this fails
// closed with backend.ErrWorkflowVersionUnavailable rather than installing a
// same-label-different-semantics definition.
//
// version empty or equal to the current version is the no-op branch: the
// requested version already is the current record, so nothing is written.
func (m *workflowControlModule) rollbackWorkflow(ctx context.Context, ns namespace.Namespace, id types.WorkflowID, version, mutationID string) (WorkflowRollbackResult, error) {
	registry := m.registry()
	if registry == nil {
		if m.log != nil {
			m.log.Error("rollback_workflow_no_registry", "workflow_id", string(id))
		}
		return WorkflowRollbackResult{}, errors.New("apiserver: no workflow registry configured")
	}
	ctx = namespace.WithNamespace(ctx, ns)
	existing, err := registry.GetWorkflow(ctx, id)
	if err != nil {
		return WorkflowRollbackResult{}, err
	}
	if existing.Namespace != "" && ns != "" && existing.Namespace != string(ns) {
		return WorkflowRollbackResult{}, backend.ErrWorkflowNotFound
	}
	if version == "" || version == existing.Version {
		return WorkflowRollbackResult{WorkflowID: existing.ID, Version: existing.Version, Unchanged: true}, nil
	}
	archived, err := m.archivedWorkflowVersion(ctx, ns, id, version)
	if err != nil {
		return WorkflowRollbackResult{}, err
	}
	replacement := archived
	replacement.ID = existing.ID
	replacement.Namespace = string(ns)
	replacement.Name = existing.Name
	replacement.Key = workflowRegistryKey(string(ns), existing.Name, version)
	// A caller-supplied mutation id (X-Request-Id, prefixed by the handler)
	// makes the retry of a response-lost rollback replay instead of writing a
	// second revision; without one, each attempt is its own mutation, exactly
	// as on PUT /v1/workflows/{id}.
	if mutationID == "" {
		mutationID = "http:" + uuid.NewString()
	}
	newID, _, err := m.compareAndReplaceWorkflow(ctx, ns, registry, existing, replacement, mutationID, registrationDiagnostics{})
	if err != nil {
		return WorkflowRollbackResult{}, err
	}
	return WorkflowRollbackResult{WorkflowID: newID, Version: version, Unchanged: false}, nil
}

func (m *workflowControlModule) compareAndReplaceWorkflow(
	ctx context.Context,
	ns namespace.Namespace,
	registry backend.WorkflowRegistry,
	existing backend.WorkflowRecord,
	replacement backend.WorkflowRecord,
	mutationID string,
	diag registrationDiagnostics,
) (types.WorkflowID, registrationDiagnostics, error) {
	durableRegistry, ok := registry.(backend.DurableWorkflowReplaceCapability)
	if !ok || m.workflowActivationProjectionWorker() == nil {
		if m.log != nil {
			m.log.Error("replace_workflow_durable_projection_capability_missing")
		}
		return "", registrationDiagnostics{}, backend.ErrWorkflowReplaceUnsupported
	}
	request := backend.WorkflowReplaceRequest{
		MutationID:  mutationID,
		Expected:    backend.RevisionOfWorkflow(existing),
		Replacement: replacement,
	}
	if err := ctx.Err(); err != nil {
		return "", registrationDiagnostics{}, err
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
		return "", registrationDiagnostics{}, err
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
		return "", registrationDiagnostics{}, err
	}
	return result.Current.ID, diag, nil
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

// subWorkflowResolver returns the compile-time resolver for definitions
// registered in ns, or nil when no registry is wired (an engine-less unit-test
// facade).
//
// The registration compile is where an xflow.subworkflow reference is pinned:
// the resolver turns the author's reference into a (version, definition hash)
// binding, and the persisted graph carries it. Nil is not a silent pass — a
// definition with such a node then compiles unresolved and the engine refuses
// to run it — but with no registry there is nothing to resolve against, which
// is the same posture every other registry-dependent registration step takes.
func (m *workflowControlModule) subWorkflowResolver(ns namespace.Namespace) graph.SubWorkflowResolver {
	registry := m.registry()
	if registry == nil {
		return nil
	}
	return control.NewSubWorkflowResolver(registry).BoundToNamespace(ns)
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

// hashParamSpecs is the ParamSpec lookup registration hashes with: the
// builtin-only table, never the live node registry, so the hash does not depend
// on what a process registered and matches the SDK's hash for one workflow.
var hashParamSpecs workflowhash.ParamSpecLookup = node.BuiltinParamSpecs

// unchangedExceptStampedID reports whether replacement differs from existing
// at most by the path id PUT stamps onto the definition: same registry key,
// and the stored definition, which carries either no id (written by POST) or
// the path id (written by PUT), hashes identically once given that id.
func unchangedExceptStampedID(existing, replacement backend.WorkflowRecord, id types.WorkflowID) bool {
	if existing.Key != replacement.Key || existing.Definition == nil {
		return false
	}
	if existing.Definition.ID != "" && existing.Definition.ID != string(id) {
		return false
	}
	stamped := *existing.Definition
	stamped.ID = string(id)
	return sameAuditFingerprint(backend.WorkflowRecord{Definition: &stamped, EditorMetadata: existing.EditorMetadata}, replacement)
}

// activationProjectionMayBePending reports whether ns may hold an unprojected
// workflow activation intent. The outbox lists intents per namespace, not per
// workflow, so any pending intent counts; a listing error also counts, keeping
// the identical-PUT shortcut conservative. A registry without an outbox has no
// intents to wait on.
func (m *workflowControlModule) activationProjectionMayBePending(ctx context.Context, ns namespace.Namespace, registry backend.WorkflowRegistry) bool {
	outbox, ok := registry.(backend.WorkflowActivationProjectionOutbox)
	if !ok {
		return false
	}
	refs, _, err := outbox.ListPendingWorkflowActivationProjections(ctx, ns, 0, 1)
	if err != nil {
		if m.log != nil {
			m.log.Error("replace_workflow_list_pending_projections_failed", "namespace", string(ns), "err", err)
		}
		return true
	}
	return len(refs) > 0
}

// sameAuditFingerprint reports whether replacement is the same full definition
// as existing, editor metadata included. Replace no-op checks compare audit
// fingerprints rather than the runtime conflict hash, so a metadata-only
// replace is still written.
//
// existing's fingerprint is recomputed from its stored definition whenever
// there is one: a record written before registrations carried a fingerprint
// has none, and recomputing is the same value for one that does. Without a
// stored definition the stored fingerprint is used, and without either the
// check falls back to the runtime hash.
func sameAuditFingerprint(existing, replacement backend.WorkflowRecord) bool {
	if replacement.AuditFingerprint == "" {
		return false
	}
	if existing.Definition != nil {
		audit, err := workflowhash.Audit(existing.Definition, existing.EditorMetadata)
		return err == nil && audit == replacement.AuditFingerprint
	}
	if existing.AuditFingerprint != "" {
		return existing.AuditFingerprint == replacement.AuditFingerprint
	}
	return existing.DefinitionHash != "" && existing.DefinitionHash == replacement.DefinitionHash
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
	//
	// The admission result mapping is shared with the server-hosted webhook
	// route (entry_seed_admission.go): both callers admit through this same
	// entry and must answer with the same statuses and bodies.
	resp, err := seedAdmission(r.Context(), m.cp, m.eng, engReq)
	writeSeedAdmissionResult(w, m.log, resp, err)
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
//
// The delivered payload records the authenticated caller's subject under
// types.VerifiedActorKey so a handler that acts on a person's behalf — an
// approval deciding who voted — can read a server-verified identity instead of
// one the caller supplied. A request that supplies the reserved key itself is
// 400 signal_invalid.
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
	// A signal that carries a reserved key is claiming a fact only this server
	// can establish. Refuse it instead of overwriting: a regression that drops
	// the injection below must fail loudly rather than fall back to trusting the
	// caller's claim about who they are.
	if _, reserved := req.Data[types.VerifiedActorKey]; reserved {
		writeFail(w, r, http.StatusBadRequest, "signal_invalid", "signal payload is invalid")
		return
	}
	data := req.Data
	if p, ok := principalFromRequest(r); ok && p.Subject != "" {
		data = withVerifiedActor(data, p.Subject)
	}
	if err := m.eng.DeliverSignal(r.Context(), id, req.Name, data); err != nil {
		writeExecEngineFail(w, r, err)
		return
	}
	writeData(w, r, http.StatusOK, map[string]bool{"accepted": true})
}

// withVerifiedActor returns data carrying the verified actor under the reserved
// key. The map is copied so the decoded request body is not mutated.
//
// A signal delivered without a verified principal is passed through unchanged:
// no actor can be established, and handlers must treat that as unattributable
// rather than as an absence of identity to be filled in from the payload.
func withVerifiedActor(data map[string]any, actor string) map[string]any {
	out := make(map[string]any, len(data)+1)
	for k, v := range data {
		out[k] = v
	}
	out[types.VerifiedActorKey] = actor
	return out
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

// executionController is the optional engine capability behind POST
// /v1/executions/{id}/pause and /resume.
//
// It is deliberately separate from control.EngineFacade rather than a widening
// of it. The facade is what the runner-protocol server and the control modules
// share, and its test doubles (and integrations) implement it method by method;
// a capability assertion here keeps those doubles source-compatible and turns a
// facade that cannot suspend anything into a 501 instead of a compile break.
// That is the same shape service/control uses for its optional engine
// capabilities.
type executionController interface {
	Pause(ctx context.Context, id types.ExecutionID) error
	Resume(ctx context.Context, id types.ExecutionID) error
}

// executionControl resolves the optional capability, reporting the engine's own
// unsupported sentinel when the facade does not carry it — the store-level
// absence (a backend with no ExecutionPauser) and the facade-level absence are
// one operator-facing fact, so they share one code and one message.
func (m *workflowControlModule) executionControl() (executionController, error) {
	ctrl, ok := m.eng.(executionController)
	if !ok {
		return nil, engine.ErrPauseUnsupported
	}
	return ctrl, nil
}

// handlePause serves POST /v1/executions/{id}/pause (design §3 API). It
// suspends dispatch of a running execution and answers with the execution view,
// so the caller sees the transition it just made rather than having to poll for
// it.
//
// The refusals are the interesting part. A queued execution cannot be paused
// (409 execution_not_pausable): it has not started, so there is no dispatch to
// hold, and its concurrency slot is a place in line rather than a running
// claim — cancel is the operation for it. A finished execution is the same 409.
// Pausing an already-paused execution is the store's idempotent success, not a
// 409, so a retried request is safe. An absent execution is 404, never 409: the
// store reports "absent" and "wrong status" with one sentinel, and the engine
// separates them before the transition so a typo'd id cannot read as a live
// execution that refuses to pause.
func (m *workflowControlModule) handlePause(w http.ResponseWriter, r *http.Request, id types.ExecutionID) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	ctrl, err := m.executionControl()
	if err != nil {
		writeExecutionControlFail(w, r, err)
		return
	}
	if err := ctrl.Pause(r.Context(), id); err != nil {
		writeExecutionControlFail(w, r, err)
		return
	}
	inspectExecution(w, r, m.eng, id)
}

// handleResume serves POST /v1/executions/{id}/resume (design §3 API): it
// releases a paused execution's dispatch and answers with the execution view.
//
// Only a paused execution can be resumed — anything else is 409
// execution_not_paused — so a resume is not a general "start this again". The
// held work is delivered before the view is read, so the view shows an
// execution that is moving.
func (m *workflowControlModule) handleResume(w http.ResponseWriter, r *http.Request, id types.ExecutionID) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	ctrl, err := m.executionControl()
	if err != nil {
		writeExecutionControlFail(w, r, err)
		return
	}
	if err := ctrl.Resume(r.Context(), id); err != nil {
		writeExecutionControlFail(w, r, err)
		return
	}
	inspectExecution(w, r, m.eng, id)
}

// writeExecutionControlFail maps the pause/resume verdicts onto stable failure
// shapes. The two 409s get distinct codes on purpose: "not pausable" and "not
// paused" are different operator mistakes, and one code for both would invite a
// caller to retry the wrong transition.
//
// A store that cannot hold dispatch at all is 501 execution_control_unsupported
// — the same code whether the missing capability is the backend's or the
// facade's, because the operator-facing fact is identical: this deployment has
// no pause. Everything else collapses to the shared engine-error mapping.
func writeExecutionControlFail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, engine.ErrExecutionNotPausable):
		writeFail(w, r, http.StatusConflict, "execution_not_pausable",
			"execution is not in a pausable status")
		return
	case errors.Is(err, engine.ErrExecutionNotPaused):
		writeFail(w, r, http.StatusConflict, "execution_not_paused", "execution is not paused")
		return
	case errors.Is(err, engine.ErrPauseUnsupported):
		writeFail(w, r, http.StatusNotImplemented, "execution_control_unsupported",
			"execution control is not supported")
		return
	}
	writeExecEngineFail(w, r, err)
}

// executionRerunner is the optional engine capability behind POST
// /v1/executions/{id}/rerun. Like executionController it is an assertion rather
// than a widening of control.EngineFacade, so a facade that cannot seed an
// execution answers 501 instead of failing to compile.
type executionRerunner interface {
	Rerun(ctx context.Context, req engine.RerunRequest) (engine.RerunResult, error)
}

// executionRerun resolves the optional capability, reporting the engine's own
// unsupported sentinel when the facade does not carry it. A store without the
// seeding capability and a facade without the method are one operator-facing
// fact ("this deployment cannot rerun"), so they share one code and message.
func (m *workflowControlModule) executionRerun() (executionRerunner, error) {
	rerunner, ok := m.eng.(executionRerunner)
	if !ok {
		return nil, engine.ErrRerunUnsupported
	}
	return rerunner, nil
}

// withParentWorkflowDef attaches the workflow definition a rerun inherits from
// the execution it derives from, so the durable projection of the NEW
// execution records its workflow id. The parent's workflow_id column is the
// sole authority: it is the identity the parent was recorded with server-side.
// The stored definition payload is never read for an id — a legacy submission
// could choose it, and inheriting it would reopen the attribution hijack this
// path exists to close. A row whose column is empty stays unattributed
// (fail-closed), matching B1's "never guess an old row's ownership"; a graph
// name is never promoted to an identity.
func (m *workflowControlModule) withParentWorkflowDef(ctx context.Context, id types.ExecutionID) context.Context {
	if m.executions == nil {
		return ctx
	}
	rec, err := m.executions.GetExecution(ctx, id)
	if err != nil || rec == nil || rec.WorkflowID == "" {
		return ctx
	}
	var def types.WorkflowDef
	if len(rec.WorkflowDef) > 0 {
		// The payload only enriches the definition (name, version); the id is
		// never taken from it, so an unparsable payload costs nothing here.
		_ = json.Unmarshal(rec.WorkflowDef, &def)
	}
	def.ID = string(rec.WorkflowID)
	if def.Name == "" {
		def.Name = rec.WorkflowName
	}
	return engine.WithWorkflowDef(ctx, &def)
}

// handleRerun serves POST /v1/executions/{id}/rerun (design §3 API).
//
// It creates a NEW execution derived from a terminal one, starting at
// from_node: everything upstream of that node is copied from the execution
// being rerun — its results, not its runtime state — and the node and its
// downstream run again. The execution being rerun is never modified, which is
// what makes this safe to offer for any terminal execution: no lease, fence or
// audit row of the original is disturbed.
//
// The request carries the node and, optionally, whether a successful execution
// is an acceptable source. The response is the new execution's id and status:
// 200 with an envelope, matching the submission path, because what the caller
// acts on next is an execution id, not this endpoint's URL.
func (m *workflowControlModule) handleRerun(w http.ResponseWriter, r *http.Request, id types.ExecutionID) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	// The body is required: without a from_node there is no rerun point, and
	// defaulting one would silently re-run the whole workflow instead.
	var req rerunRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.FromNode == "" {
		writeFail(w, r, http.StatusBadRequest, "from_node_required", "from_node is required")
		return
	}
	rerunner, err := m.executionRerun()
	if err != nil {
		writeExecutionRerunFail(w, r, err)
		return
	}
	// The trace carrier is propagated so the new execution's asynchronous
	// dispatch spans stay under this request's trace, exactly as the execute
	// path does. Beyond the carrier, a rerun needs the API layer to attach the
	// parent's workflow definition: the seed carries only a compiled graph, so
	// the durable SQL projection would record no workflow_id without it. The
	// engine attaches the identity inherited by the new snapshot; this layer
	// supplies the row-authoritative definition that projection reads.
	//
	// An Idempotency-Key makes the rerun safe to retry: the engine scopes the key
	// to the parent execution, and the fingerprint covers the parent, the
	// from-node and include_success, so a retry of the same request is answered
	// with the rerun it already created, and the same key on a different request
	// is a conflict.
	idemKey, ok := idempotencyKeyFromRequest(w, r)
	if !ok {
		return
	}
	ctx := engine.WithTraceCarrier(r.Context(), tracing.InjectCarrier(r.Context()))
	// A rerun carries no definition of its own, so projectSeededRerun would
	// leave the new SQL row unattributed. The parent row is the exact source of
	// the workflow identity (a rerun runs the workflow it derives from); a row
	// that cannot be read leaves the child unattributed rather than guessing.
	ctx = m.withParentWorkflowDef(ctx, id)
	res, err := rerunner.Rerun(ctx, engine.RerunRequest{
		ExecutionID:    id,
		FromNode:       req.FromNode,
		IncludeSuccess: req.IncludeSuccess,
		IdempotencyKey: idemKey,
	})
	if err != nil {
		writeExecutionRerunFail(w, r, err)
		return
	}
	resp := m.rerunResponse(ctx, res)
	resp.Duplicate = res.Duplicate
	writeData(w, r, http.StatusOK, resp)
}

// rerunRequest is the body for POST /v1/executions/{id}/rerun (design §3 API).
type rerunRequest struct {
	// FromNode is the node the new execution starts at. Required.
	FromNode string `json:"from_node"`
	// IncludeSuccess accepts a successful execution as the source. Failed,
	// canceled and timeout executions are always accepted.
	IncludeSuccess bool `json:"include_success,omitempty"`
}

// rerunExecutionResponse reports the execution a rerun created.
type rerunExecutionResponse struct {
	ExecutionID types.ExecutionID `json:"execution_id"`
	// Status is the new execution's status, read back best effort: a rerun that
	// could not be read still reports the id the caller has to act on.
	Status types.ExecutionStatus `json:"status,omitempty"`
	// QueuePosition is the new execution's place in its workflow's line, 0 when
	// it started immediately.
	QueuePosition int `json:"queue_position,omitempty"`
	// Duplicate is true when the request's Idempotency-Key had already created
	// this rerun: nothing was created by this call, and the answer is the
	// original rerun execution's id and current status.
	Duplicate bool `json:"duplicate,omitempty"`
}

// rerunResponse builds the reply from what the rerun reported plus a status
// read-back. The read-back is best effort for the same reason submission's is:
// the created execution id is the answer, and the status is how the caller
// learns whether the work started or is waiting for a slot.
func (m *workflowControlModule) rerunResponse(ctx context.Context, res engine.RerunResult) rerunExecutionResponse {
	resp := rerunExecutionResponse{ExecutionID: res.ExecutionID, QueuePosition: res.QueuePosition}
	if res.QueuePosition > 0 {
		resp.Status = types.ExecutionStatusQueued
	}
	detail, err := m.eng.InspectStatus(ctx, res.ExecutionID)
	if err != nil {
		if m.log != nil {
			m.log.Error("rerun_status_readback_failed", "execution_id", string(res.ExecutionID), "err", err)
		}
		return resp
	}
	resp.Status = detail.Status
	if detail.Status != types.ExecutionStatusQueued {
		// A promoted execution is no longer waiting: a position reported by the
		// admission would name a place it has already left.
		resp.QueuePosition = 0
	}
	return resp
}

// writeExecutionRerunFail maps the rerun verdicts onto stable failure shapes.
//
// The refusals are deliberately distinct: "the execution is not one you can
// rerun from" (409), "the node you named is not in it" (400), "its definition
// cannot be seeded" (409) and "no result to copy" (409) are four different
// operator mistakes, and the concurrency refusals keep their 429s because a
// reached limit is a retry, not a failed rerun.
func writeExecutionRerunFail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case writeIdempotencyFailure(w, r, err):
		return
	case writeConcurrencyFailure(w, r, err):
		return
	case errors.Is(err, engine.ErrExecutionNotRerunnable):
		writeFail(w, r, http.StatusConflict, "execution_not_rerunnable",
			"execution is not in a rerunnable status")
		return
	case errors.Is(err, engine.ErrRerunNodeNotFound):
		writeFail(w, r, http.StatusBadRequest, "rerun_node_not_found",
			"from_node is not in the execution's graph")
		return
	case errors.Is(err, engine.ErrRerunUpstreamIncomplete):
		writeFail(w, r, http.StatusConflict, "rerun_upstream_incomplete",
			"a node upstream of the rerun point has no result to copy")
		return
	case errors.Is(err, engine.ErrRerunGraphUnsupported):
		writeFail(w, r, http.StatusConflict, "rerun_graph_unsupported",
			"the execution's graph does not support rerun from a node")
		return
	case errors.Is(err, engine.ErrRerunUnsupported):
		writeFail(w, r, http.StatusNotImplemented, "execution_rerun_unsupported",
			"execution rerun is not supported")
		return
	}
	writeExecEngineFail(w, r, err)
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
//
// The raw body is bounded by MaxRequestBodyBytes with the same
// http.MaxBytesReader guard decodeJSON uses, so an unbounded seed body cannot
// be buffered before decoding. Overflow answers 413 in this face's own bare
// shape — {"error":"request_too_large"}, the same stable code the enveloped
// routes use — not the enveloped body, for the reason above. The runner
// protocol client already maps a 413 to ErrRunnerRequestTooLarge and stops
// resending, so this answer is the contract it expects.
func decodeSeedJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	defer func() { _ = r.Body.Close() }()
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxRequestBodyBytes)).Decode(dst); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, errorResponse{Error: "request_too_large"})
			return false
		}
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON"})
		return false
	}
	return true
}

// MaxRequestBodyBytes caps every request body decoded through decodeJSON.
//
// 8 MiB covers a registration carrying an inline WorkflowDef plus a 1 MiB
// editor_metadata blob, with headroom for execution parameters. This is a
// denial-of-service bound, NOT business validation: a body under the cap can
// still be refused by the field-level rules each handler applies.
const MaxRequestBodyBytes int64 = 8 << 20

// decodeJSON decodes a JSON request body, refusing bodies above
// MaxRequestBodyBytes before they are buffered in full.
//
// Over-limit bodies answer 413 request_too_large with a fixed message: the
// response never echoes the limit, the body, or the reader's internal error, so
// a caller learns only that its request was too large. Exceeding the cap is the
// documented reason http.MaxBytesReader marks the connection non-reusable;
// that is standard net/http behavior and is deliberately not worked around.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	defer func() { _ = r.Body.Close() }()
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxRequestBodyBytes)).Decode(dst); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeFail(w, r, http.StatusRequestEntityTooLarge, "request_too_large", "request body too large")
			return false
		}
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
