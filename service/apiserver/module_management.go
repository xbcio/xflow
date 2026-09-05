package apiserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
)

// managementModule mounts the ops management HTTP API: leader status,
// single-runner lookup and (when supported) runner listing,
// single-execution inspect, dead-letter list/replay, registration-code
// create/list/revoke/audit, and the process liveness/readiness probes. It is
// opt-in (registered only via WithManagement) because it exposes runner
// directory, execution state, and dead-letter operations that must sit
// behind authz.
//
// Per R1 the underlying store interface exposes no list API, so this module
// provides no execution listing. Runner listing IS exposed, but only when the
// configured directory structurally supports enumeration; otherwise the route
// answers 501 rather than pretending the fleet is empty.
type managementModule struct {
	authzHolder
	cp  *control.ControlPlane
	eng control.EngineFacade
	// metrics wires the OutboxObserver (replay outcome counter + pending/dead
	// gauge) into the shared DeadLetterManager so the API and CLI paths
	// produce identical telemetry. Nil leaves metrics off (dev).
	metrics *metrics.Metrics
	// dlMgr is the shared, lazily-constructed dead-letter manager. It is
	// built once on first use with the metrics observer + durable audit
	// projector and reused for every subsequent list/replay so the API never
	// builds a fresh manager per request with nil metrics.
	dlMgr     *control.DeadLetterManager
	dlMgrOnce sync.Once
	ready     ReadinessChecker
	// codes / issued back the registration-code management API (create / list /
	// revoke / audit). Whether these four routes are MOUNTED depends only on
	// m.principalAuth (see RegisterHTTP) — never on codes/issued being non-nil.
	//
	// That is deliberate, not an oversight (Task 8 addendum Ruling W): the
	// production dead-constant guard (paths_test.go newFullGuardMux) builds a
	// PrincipalAuth-only server with no registration-code store configured at
	// all, and TestUserFacingPathsHaveMuxRegistration requires every
	// UserFacingPaths entry to resolve to an actually-registered route on THAT
	// mux. Gating registration on codes/issued would make the three new routes
	// vanish there, and the only "fix" for that red — dropping the paths back
	// out of UserFacingPaths — would silently reopen the exact dead-constant
	// hole the guard exists to catch (and nothing would ever catch THAT,
	// addendum Ruling 4: there is no guard in the reverse direction).
	//
	// So instead: nil codes means the routes exist and are reachable by an
	// authorized caller, but every handler answers 404 route_not_found itself.
	// Same externally observed behavior as "the feature does not exist on this
	// server", reached without touching the mount condition.
	codes  control.RegistrationCodeStore
	issued control.IssuedIdentityStore
	// runners is the structural probe result for GET PathManagementRunners
	// (see runnerLister below). Nil means the configured directory does not
	// support enumeration, and the route answers 501 rather than an empty list.
	runners runnerLister
}

// runnerLister is the structural probe for a runner directory that can
// enumerate. The concrete directory does not implement it today; the interface
// exists so the route can answer honestly either way, and so a directory that
// grows the capability lights the route up without further plumbing.
type runnerLister interface {
	ListRunners(ctx context.Context) ([]string, error)
}

func newManagementModule(cp *control.ControlPlane) *managementModule {
	m := &managementModule{cp: cp, eng: cp.Engine()}
	if dir := cp.RunnerDirectory(); dir != nil {
		if l, ok := dir.(runnerLister); ok {
			m.runners = l
		}
	}
	return m
}

func (m *managementModule) Name() string { return "management" }

func (m *managementModule) RegisterHTTP(mux *http.ServeMux) {
	// Task 8 blocker 2: when a PrincipalAuthenticator is configured, the
	// leader/runner/executions routes go through the B3 authz wrapper so each
	// gets its own stable operation + scope + admission/outcome audit, not a
	// blanket management.read. leader → management.leader.read, runner →
	// management.runner.read, execution inspect → management.read (single-
	// resource lookup; no list API). dead-letters self-wrap below. healthz/
	// readyz stay open for probes (minimal public info only).
	//
	// When PrincipalAuth is nil (dev / behind an external gateway) the routes
	// are served directly and the namespace defaults to namespace.Default.
	if m.principalAuth != nil {
		mux.HandleFunc("GET "+PathManagementLeader, m.authzWrap(OpManagementLeaderRead, false, m.handleLeader, func(*http.Request) (string, string, string, string) {
			return "management/leader", "", "", ""
		}))
		mux.HandleFunc("GET "+PathManagementRunnerByID, m.authzWrap(OpManagementRunnerRead, false, m.handleRunner, func(r *http.Request) (string, string, string, string) {
			id := r.PathValue("id")
			return "management/runner/" + id, "", "", ""
		}))
		// Runner listing (Task 9): its own Op/scope, separate from
		// OpManagementRunnerRead — a token that may look up one known runner
		// should not thereby be able to enumerate the whole fleet. The handler
		// itself decides 200 vs 501 depending on whether the configured runner
		// directory structurally supports enumeration (m.runners); that
		// decision must NOT gate whether the route is mounted, or an
		// unauthenticated caller would get a different status than an
		// authenticated one hitting an unsupported backend, which is exactly
		// the kind of authz-wrapper bypass TestEveryProtectedUserPathRejectsUnauthenticated
		// exists to catch.
		mux.HandleFunc("GET "+PathManagementRunners, m.authzWrap(OpManagementRunnerList, false, m.handleListRunners, func(*http.Request) (string, string, string, string) {
			return "management/runners", "", "", ""
		}))
		// Namespace boundary (Task 7.3): the execution-inspect route injects the
		// verified principal's Namespace into the request context. Inspect reads
		// from the principal's namespace namespace; a cross-namespace execID resolves
		// to not-found → 404, which is the IDOR defense and does not leak
		// existence.
		mux.HandleFunc("GET "+PathManagementExecByID, m.authzWrap(OpManagementRead, false, m.handleExecution, func(r *http.Request) (string, string, string, string) {
			id := r.PathValue("id")
			return "management/execution/" + id, "", id, ""
		}))
		// Registration-code CRUD + audit (Task 8). Unlike leader/runner/exec
		// above, these four have NO bare fallback in the else branch below, and
		// unlike dead-letters they do not self-wrap unconditionally either: per
		// the Task 8 addendum (Ruling Y, security-critical), a server with no
		// PrincipalAuthenticator configured must not expose an endpoint that
		// mints runner credentials at all — not even a bare (unauthenticated)
		// version of it. So the mount is gated on principalAuth alone (never on
		// m.codes/m.issued — see the struct field comment above for why), and
		// principalAuth==nil leaves these routes genuinely unregistered → 404.
		//
		// Namespace boundary (Task 8 fix1 Important-3): all four leave
		// ResourceNamespace empty in their authzWrap resolver funcs below, and
		// — unlike handleExecution's cross-namespace-read-404 pattern — there is
		// NO namespace-scoped store read backing that emptiness here.
		// Registration-code management is a deliberate platform-level global
		// operation (spec §2.3.4: list ALL codes); the store reads by id or
		// unconditionally, and xflow_registration_codes carries no owning
		// namespace column (AllowedNamespaces is what a code GRANTS, not who it
		// belongs to). The entire boundary for these four operations is the
		// scope check (management.registration_code.create/list/revoke/audit)
		// itself — see the matching note on NamespaceAwareAuthorizer in
		// authz.go. Do not add a namespace ceiling check here or a namespace
		// filter to the store; that would conflict with the global-listing
		// design.
		//
		// Trust assumption this places on scope-granting policy (confirmed
		// against cmd/server/main.go's auth-tokens-file loader): that file is
		// free-form JSON binding a token to an arbitrary (Namespace, Scopes)
		// pair — nothing in code stops an operator from granting
		// management.registration_code.create to a token whose Namespace is a
		// single tenant, and that token could then mint a code with
		// allowed_namespaces:["*"]. These four scopes must only ever be granted
		// to platform administrators, never to a tenant-scoped principal; the
		// boundary is entirely in how scopes are provisioned, not enforced in
		// this code. That gap in operator configuration is a separate,
		// deliberately out-of-scope concern for this fix (tracked for final
		// review), not something this comment claims is closed.
		mux.HandleFunc("POST "+PathManagementRegistrationCodes, m.authzWrap(OpRegistrationCodeCreate, true, m.handleCreateRegistrationCode, func(*http.Request) (string, string, string, string) {
			return "management/registration-codes", "", "", ""
		}))
		mux.HandleFunc("GET "+PathManagementRegistrationCodes, m.authzWrap(OpRegistrationCodeList, false, m.handleListRegistrationCodes, func(*http.Request) (string, string, string, string) {
			return "management/registration-codes", "", "", ""
		}))
		mux.HandleFunc("DELETE "+PathManagementRegistrationCodeByID, m.authzWrap(OpRegistrationCodeRevoke, true, m.handleRevokeRegistrationCode, func(r *http.Request) (string, string, string, string) {
			return "management/registration-codes/" + r.PathValue("id"), "", "", ""
		}))
		mux.HandleFunc("GET "+PathManagementRegistrationCodeAudit, m.authzWrap(OpRegistrationCodeAudit, false, m.handleRegistrationCodeAudit, func(r *http.Request) (string, string, string, string) {
			return "management/registration-codes/" + r.PathValue("id") + "/audit", "", "", ""
		}))
	} else {
		mux.HandleFunc("GET "+PathManagementLeader, m.handleLeader)
		mux.HandleFunc("GET "+PathManagementRunnerByID, m.handleRunner)
		mux.HandleFunc("GET "+PathManagementExecByID, m.handleExecution)
	}
	mux.HandleFunc("/v1/management/dead-letters/", m.handleDeadLetters)
	mux.HandleFunc("GET "+PathHealthz, m.handleHealthz)
	mux.HandleFunc("GET "+PathReadyz, m.handleReadyz)
}

type leaderResponse struct {
	IsLeader bool `json:"is_leader"`
}

type readyResponse struct {
	Ready  bool `json:"ready"`
	Leader bool `json:"leader"`
}

// ReadinessChecker is the narrow probe contract used by /readyz. Implementations
// should verify only dependencies required by this API server replica to accept
// traffic, and must return quickly when ctx is canceled.
type ReadinessChecker interface {
	CheckReadiness(ctx context.Context) error
}

type readinessFunc func(context.Context) error

func (f readinessFunc) CheckReadiness(ctx context.Context) error { return f(ctx) }

type compositeReadinessChecker []ReadinessChecker

func (c compositeReadinessChecker) CheckReadiness(ctx context.Context) error {
	for _, checker := range c {
		if checker == nil {
			continue
		}
		if err := checker.CheckReadiness(ctx); err != nil {
			return err
		}
	}
	return nil
}

var errAPIServerShuttingDown = errors.New("apiserver: shutting down")

type apiServerReadiness struct {
	shuttingDown atomic.Bool
	checker      ReadinessChecker
}

func newAPIServerReadiness(cp *control.ControlPlane, cfg Config) *apiServerReadiness {
	checks := make([]ReadinessChecker, 0, 3)
	checks = appendUniqueReadinessChecker(checks, cfg.ReadinessChecker)
	if checker, ok := cfg.Store.(ReadinessChecker); ok {
		checks = appendUniqueReadinessChecker(checks, checker)
	}
	checks = appendUniqueReadinessChecker(checks, redisDependencyReadiness(cp))
	return &apiServerReadiness{checker: compositeReadinessChecker(checks)}
}

func appendUniqueReadinessChecker(checks []ReadinessChecker, checker ReadinessChecker) []ReadinessChecker {
	if checker == nil {
		return checks
	}
	for _, existing := range checks {
		if sameReadinessChecker(existing, checker) {
			return checks
		}
	}
	return append(checks, checker)
}

func sameReadinessChecker(a, b ReadinessChecker) bool {
	typ := reflect.TypeOf(a)
	return typ != nil && typ == reflect.TypeOf(b) && typ.Comparable() && a == b
}

func (r *apiServerReadiness) CheckReadiness(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if r.shuttingDown.Load() {
		return errAPIServerShuttingDown
	}
	if r.checker == nil {
		return nil
	}
	return r.checker.CheckReadiness(ctx)
}

func (r *apiServerReadiness) markShuttingDown() {
	if r != nil {
		r.shuttingDown.Store(true)
	}
}

type redisReadinessProvider interface {
	RedisClient() redis.Cmdable
}

func redisDependencyReadiness(cp *control.ControlPlane) ReadinessChecker {
	if cp == nil || cp.Backend() == nil {
		return nil
	}
	provider, ok := cp.Backend().(redisReadinessProvider)
	if !ok {
		return nil
	}
	client := provider.RedisClient()
	if client == nil {
		return readinessFunc(func(context.Context) error {
			return errors.New("redis client unavailable")
		})
	}
	return readinessFunc(func(ctx context.Context) error {
		return client.Ping(ctx).Err()
	})
}

const readinessCheckTimeout = 2 * time.Second

// handleLeader reports whether this replica currently holds leadership. GET
// only; any other method yields 405.
func (m *managementModule) handleLeader(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	writeData(w, r, http.StatusOK, leaderResponse{IsLeader: m.cp.IsLeader()})
}

// handleRunner looks up a single runner snapshot by id. See handleListRunners
// for the separate listing route, which is only supported when the configured
// directory implements runnerLister. The id is the {id} path value the mux
// matched.
func (m *managementModule) handleRunner(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		writeFail(w, r, http.StatusNotFound, "runner_not_found", "runner not found")
		return
	}
	dir := m.cp.RunnerDirectory()
	if dir == nil {
		writeFail(w, r, http.StatusNotFound, "runner_not_found", "runner not found")
		return
	}
	snap, ok := dir.Runner(r.Context(), id)
	if !ok {
		writeFail(w, r, http.StatusNotFound, "runner_not_found", "runner not found")
		return
	}
	writeData(w, r, http.StatusOK, snap)
}

// runnerListItem is the JSON projection for one entry in GET
// PathManagementRunners.
type runnerListItem struct {
	RunnerID string `json:"runner_id"`
	// Enrolled reports whether this runner's credential came from the enrollment
	// endpoint rather than a static policy file. It is what tells an operator
	// which half of the fleet a revoked registration code would affect.
	Enrolled bool `json:"enrolled"`
}

// handleListRunners enumerates the runner directory, when the configured
// directory structurally supports it (m.runners != nil — see runnerLister).
//
// Two distinct "this doesn't work" conditions live in this handler and they
// are deliberately NOT unified (Task 9 addendum Ruling 6):
//   - m.runners == nil: the runner directory does not implement runnerLister.
//     This is spec §2.3.2's mandated 501 runner_listing_unsupported — the
//     feature exists (single-runner lookup does work) but this backend cannot
//     answer "list them all". Returning an empty array here would make "no
//     runners online" and "this backend cannot answer" the same picture, and
//     those are exactly the two cases an operator needs to tell apart.
//   - m.issued.List returning an error (see below): a different question
//     ("who among them enrolled") that this backend also cannot answer, for a
//     different reason (a corrupted issued-identity row, not a missing
//     capability). It gets its own 500, not folded into the 501 above.
//
// This is unlike registrationCodeUnavailable's 404 (m.codes == nil): that 404
// means the registration-code feature is not configured on this server at
// all. The 501 here means the feature IS configured but the directory
// implementation cannot enumerate. Different conditions, kept separate.
func (m *managementModule) handleListRunners(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	if m.runners == nil {
		// Not "no runners" — "this backend cannot answer the question". The two
		// must not collapse into the same response.
		writeFail(w, r, http.StatusNotImplemented, "runner_listing_unsupported",
			"the configured runner directory does not support enumeration")
		return
	}
	ids, err := m.runners.ListRunners(r.Context())
	if err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	enrolled := map[string]bool{}
	if m.issued != nil {
		list, err := m.issued.List(r.Context())
		if err != nil {
			// Not "nobody is enrolled" -- "we cannot tell who is enrolled".
			// Reporting enrolled:false for the whole fleet here would misinform
			// the exact decision this field exists to inform (which runners a
			// revoked registration code affects). Same principle as the 501
			// above: a question we cannot answer must not be answered wrongly
			// (Task 9 addendum Ruling 4). This is a distinct failure mode from
			// m.issued == nil below: List erroring means the store IS
			// configured but a row in it is corrupt (store.ErrEnrollScopeCorrupted
			// after Task 7's fix) and cannot be trusted, not that enrollment
			// data is genuinely absent.
			writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		for _, id := range list {
			enrolled[id.RunnerID] = true
		}
	}
	// m.issued == nil (this server has no issued-identity store configured at
	// all) falls through with enrolled left empty -- every runner reports
	// false. That is a true value, not a degraded one: without an
	// issued-identity store there genuinely are no enroll-issued runners to
	// report, unlike the List-error case above where the data exists but
	// cannot be read.
	out := make([]runnerListItem, 0, len(ids))
	for _, id := range ids {
		out = append(out, runnerListItem{RunnerID: id, Enrolled: enrolled[id]})
	}
	writeData(w, r, http.StatusOK, out)
}

// handleExecution inspects a single execution by id. It delegates to
// inspectExecution — the single shared inspect implementation also used by the
// executions-family route (spec §7.2) — so GET /v1/management/executions/{id}
// (OpManagementRead) and GET /v1/executions/{id} (OpExecutionRead) return
// byte-identical bodies for the same execution. The two Ops stay distinct so
// an ops token and a business token may be granted separately; only the
// implementation is merged. The id is the {id} path value the mux matched.
func (m *managementModule) handleExecution(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		writeFail(w, r, http.StatusNotFound, "execution_not_found", "execution not found")
		return
	}
	inspectExecution(w, r, m.eng, types.ExecutionID(id))
}

// handleHealthz is a liveness probe. It only confirms the process is serving
// HTTP and emits no sensitive information.
func (m *managementModule) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReadyz is a readiness probe. Non-leader replicas remain ready: the
// leader bit is informational, not part of the readiness decision. Readiness
// fails closed only when this process is shutting down or an explicitly
// checkable required dependency reports an error. Legacy embedded/local setups
// with no checkable dependency remain compatible and report ready while serving.
func (m *managementModule) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), readinessCheckTimeout)
	defer cancel()

	ready := true
	status := http.StatusOK
	if m.ready != nil {
		if err := m.ready.CheckReadiness(ctx); err != nil {
			ready = false
			status = http.StatusServiceUnavailable
		}
	}
	writeJSON(w, status, readyResponse{Ready: ready, Leader: m.cp.IsLeader()})
}

// deadLetterListResponse is the JSON shape for a dead-letter list page.
type deadLetterListResponse struct {
	Entries    []engine.OutboxEntry `json:"entries"`
	NextCursor string               `json:"next_cursor,omitempty"`
}

// deadLetterReplayRequest is the request body for replay.
type deadLetterReplayRequest struct {
	EntryID   string `json:"entry_id"`
	RequestID string `json:"request_id"`
	Reason    string `json:"reason"`
}

// deadLetterReplayResponse is the JSON shape for a replay result.
type deadLetterReplayResponse struct {
	Outcome      string `json:"outcome"`
	AuditID      string `json:"audit_id,omitempty"`
	ExecutionID  string `json:"execution_id,omitempty"`
	NodeID       string `json:"node_id,omitempty"`
	ActivationID string `json:"activation_id,omitempty"`
}

// handleDeadLetters routes dead-letter requests through the B3 authz wrapper.
//   - GET  /v1/management/dead-letters/{execID}              → list (cursor page)
//   - POST /v1/management/dead-letters/{execID}/replay       → replay
//
// When no PrincipalAuthenticator is configured the routes return 404 so a
// dev/preview server without authz never exposes the privileged replay path.
func (m *managementModule) handleDeadLetters(w http.ResponseWriter, r *http.Request) {
	if m.principalAuth == nil {
		writeFail(w, r, http.StatusNotFound, "route_not_found", "route not found")
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/v1/management/dead-letters/")
	rest = strings.Trim(rest, "/")
	if rest == "" {
		writeFail(w, r, http.StatusNotFound, "route_not_found", "execution id required")
		return
	}
	parts := strings.Split(rest, "/")
	execID := parts[0]
	resource := "dead-letters/" + execID

	switch {
	case len(parts) == 1 && r.Method == http.MethodGet:
		m.authzWrap(OpDeadLetterList, false, m.handleDeadLetterList, func(*http.Request) (string, string, string, string) {
			return resource, "", execID, ""
		})(w, r)
	case len(parts) == 2 && parts[1] == "replay" && r.Method == http.MethodPost:
		m.authzWrap(OpDeadLetterReplay, true, m.handleDeadLetterReplay, func(*http.Request) (string, string, string, string) {
			return resource, "", execID, ""
		})(w, r)
	default:
		writeFail(w, r, http.StatusNotFound, "route_not_found", "route not found")
	}
}

func (m *managementModule) handleDeadLetterList(w http.ResponseWriter, r *http.Request) {
	execID := m.deadLetterExecID(r)
	mgr, err := m.deadLetterManager()
	if err != nil {
		writeFail(w, r, http.StatusServiceUnavailable, "dead_letter_unavailable", "dead-letter backend unavailable")
		return
	}
	// IDOR defense (Task 7.3): confirm the execution belongs to the caller's
	// namespace before listing its dead-letters. The authz wrapper injected the
	// principal's Namespace into r.Context(), so Inspect is namespace-scoped: a
	// cross-namespace execID resolves to not-found → 404, never leaking that the
	// execution exists in another namespace. This matches the executions/ endpoint
	// behavior and the design §5.1 requirement (404, not 403).
	if _, err := m.eng.Inspect(r.Context(), types.ExecutionID(execID)); err != nil {
		writeExecEngineFail(w, r, err)
		return
	}
	q := r.URL.Query()
	limit := 0
	if v := q.Get("limit"); v != "" {
		// Best-effort parse; an invalid limit falls back to the store default.
		var n int
		_, _ = fmt.Sscanf(v, "%d", &n)
		if n > 0 {
			limit = n
		}
	}
	list, err := mgr.List(r.Context(), types.ExecutionID(execID), engine.DeadLetterPage{
		Cursor: q.Get("cursor"),
		Limit:  limit,
	})
	if err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	// §3.3 cursor exception: the cursor-paginated payload {entries,next_cursor}
	// stays as-is, but per §3.1 the user-face success body is enveloped — the
	// payload rides inside data, the cursor pagination shape is preserved. The
	// CLI's apiDeadLetterClient decodes the envelope and extracts data.
	writeData(w, r, http.StatusOK, deadLetterListResponse{
		Entries:    list.Entries,
		NextCursor: list.NextCursor,
	})
}

func (m *managementModule) handleDeadLetterReplay(w http.ResponseWriter, r *http.Request) {
	execID := m.deadLetterExecID(r)
	mgr, err := m.deadLetterManager()
	if err != nil {
		writeFail(w, r, http.StatusServiceUnavailable, "dead_letter_unavailable", "dead-letter backend unavailable")
		return
	}
	var req deadLetterReplayRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.EntryID == "" || req.Reason == "" {
		writeFail(w, r, http.StatusBadRequest, "bad_request", "entry_id and reason are required")
		return
	}
	// IDOR defense (Task 7.3): confirm the execution belongs to the caller's
	// namespace before replaying one of its dead-letters. Namespace-scoped via the
	// authz-injected context; cross-namespace execID → 404 (no existence leak).
	if _, err := m.eng.Inspect(r.Context(), types.ExecutionID(execID)); err != nil {
		writeExecEngineFail(w, r, err)
		return
	}
	// Principal is server-injected by the authz wrapper; the operator is taken
	// from it, never self-reported. The manager re-checks the scope.
	p, ok := principalFromRequest(r)
	if !ok {
		writeFail(w, r, http.StatusUnauthorized, "unauthorized", "unauthorized")
		return
	}
	res, derr := mgr.Replay(r.Context(), control.DeadLetterReplayPrincipal{
		Subject:   p.Subject,
		Namespace: p.Namespace,
		Scopes:    p.Scopes,
	}, engine.ReplayDeadLetterRequest{
		ExecutionID: types.ExecutionID(execID),
		EntryID:     req.EntryID,
		RequestID:   req.RequestID,
		Reason:      req.Reason,
	})
	if derr != nil {
		if res.Outcome == engine.ReplayInvalidRequest {
			// Generic: the underlying error text may echo caller-supplied
			// entry-id / reason fragments; keep the message generic and rely
			// on the stable code. §3.5 forbids surfacing internals.
			writeFail(w, r, http.StatusBadRequest, "bad_request", "invalid replay request")
			return
		}
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	if res.Outcome == engine.ReplayNotFound {
		// §4.1 binds success to 2xx strictly, so a 404 must carry a failure
		// envelope -- data:null, no outcome field. Nothing reads the outcome on
		// this path: the CLI's do() returns on any non-2xx before decoding, and
		// both HTTP tests discard the body. The outcome survives in the stable
		// code so a client can still branch on it.
		writeFail(w, r, http.StatusNotFound, "dead_letter_not_found", "dead-letter entry not found")
		return
	}
	writeData(w, r, http.StatusOK, deadLetterReplayResponse{
		Outcome:      string(res.Outcome),
		AuditID:      res.AuditID,
		ExecutionID:  string(res.ExecutionID),
		NodeID:       res.NodeID,
		ActivationID: res.ActivationID,
	})
}

// deadLetterExecID extracts the execution id the authz wrapper resolved for
// this request (stored on the resource string). Falls back to parsing the
// path again if the context value is absent.
func (m *managementModule) deadLetterExecID(r *http.Request) string {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/management/dead-letters/")
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) == 0 {
		return ""
	}
	return parts[0]
}

// deadLetterManager lazily builds a shared DeadLetterManager from the control
// plane's backend state when it implements engine.DeadLetterStore. The manager
// is constructed exactly once via sync.Once (cached on the module) with:
//   - a non-nil metrics observer (when cfg.Metrics is set) so the replay
//     outcome counter and pending/dead-lettered gauge are emitted at the
//     single outlet the CLI also uses;
//   - a durable receipt projector audit sink (when the configured AuditSink
//     is backed by a SQL store) so replay receipts are durably projected to
//     SQL idempotently for reconcile; otherwise the stdout/stderr sink is the
//     dev-only projection (Redis receipt remains authoritative).
//
// Returns an error when the configured backend cannot serve dead-letter
// operations (e.g. the in-memory backend used in dev); the HTTP route surfaces
// 503 in that case.
func (m *managementModule) deadLetterManager() (*control.DeadLetterManager, error) {
	m.dlMgrOnce.Do(func() {
		if m.cp.Backend() == nil {
			return
		}
		state := m.cp.Backend().State()
		store, ok := state.(engine.DeadLetterStore)
		if !ok {
			return
		}
		var observer engine.OutboxObserver
		if m.metrics != nil {
			observer = metrics.NewOutboxMetrics(m.metrics)
		}
		audit := m.deadLetterAuditSink(observer)
		m.dlMgr = control.NewDeadLetterManager(store, observer, audit)
	})
	if m.dlMgr != nil {
		return m.dlMgr, nil
	}
	if m.cp.Backend() == nil {
		return nil, errors.New("management: no backend")
	}
	state := m.cp.Backend().State()
	if _, ok := state.(engine.DeadLetterStore); !ok {
		return nil, errors.New("management: backend state does not implement DeadLetterStore")
	}
	return nil, errors.New("management: dead-letter manager initialization failed")
}

// deadLetterAuditSink selects the durable receipt-projector sink when the
// configured audit sink is SQL-backed, falling back to the stderr projection
// otherwise. The Redis receipt is always authoritative; the sink is the
// secondary durable projection reconciled against it.
func (m *managementModule) deadLetterAuditSink(observer engine.OutboxObserver) engine.DeadLetterAuditSink {
	if sql, ok := m.audit.(*SQLAuditSink); ok {
		if ra := sql.ReceiptAppender(); ra != nil {
			return control.NewProjectorAuditSink(control.NewReceiptProjector(ra), observer)
		}
	}
	return control.NewStdoutDeadLetterAuditSink(func(line string) {
		// G0/dev projection: replay receipts are emitted to stderr. The
		// authoritative receipt is the Redis record written atomically by the
		// DeadLetterStore. A durable SQL projection of replay receipts is the
		// reconcile target; until a SQL sink is configured, stderr is the
		// audit trail.
		fmt.Fprintln(os.Stderr, line)
	})
}

// registrationCodeUnavailable answers 404 route_not_found when this server was
// built without a registration-code store. The route IS mounted (see
// RegisterHTTP's field comment on codes/issued for why), so an authorized
// caller reaches this function rather than a genuinely-missing pattern; the
// response is the same 404 either way, which is the honest answer for a
// feature that does not exist on this server.
func registrationCodeUnavailable(w http.ResponseWriter, r *http.Request) {
	writeFail(w, r, http.StatusNotFound, "route_not_found", "route not found")
}

type registrationCodeCreateRequest struct {
	AllowedNamespaces []string `json:"allowed_namespaces"`
	AllowedNodeTypes  []string `json:"allowed_node_types"`
}

// registrationCodeCreateResponse is the ONLY place the plaintext code ever
// appears. It is not recoverable afterwards — not from the list endpoint, not
// from the database.
type registrationCodeCreateResponse struct {
	ID   string `json:"id"`
	Code string `json:"code"`
}

// registrationCodeView is the list projection. It deliberately carries neither
// the plaintext nor the hash: publishing sha256(code) would make every code
// offline-crackable by anyone who can read the list.
type registrationCodeView struct {
	ID                string   `json:"id"`
	AllowedNamespaces []string `json:"allowed_namespaces"`
	AllowedNodeTypes  []string `json:"allowed_node_types"`
	Revoked           bool     `json:"revoked"`
	CreatedAt         string   `json:"created_at"`
}

type enrollAuditView struct {
	Success  bool   `json:"success"`
	Reason   string `json:"reason,omitempty"`
	RunnerID string `json:"runner_id,omitempty"`
	SourceIP string `json:"source_ip,omitempty"`
	At       string `json:"at"`
}

func (m *managementModule) handleCreateRegistrationCode(w http.ResponseWriter, r *http.Request) {
	if m.codes == nil {
		registrationCodeUnavailable(w, r)
		return
	}
	var req registrationCodeCreateRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	id, plaintext, err := control.GenerateRegistrationCode()
	if err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	code := control.RegistrationCode{
		ID:                id,
		CodeHash:          control.HashSecret(plaintext),
		AllowedNamespaces: req.AllowedNamespaces,
		AllowedNodeTypes:  req.AllowedNodeTypes,
		CreatedAt:         time.Now().UTC(),
	}
	if err := m.codes.Create(r.Context(), code); err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeData(w, r, http.StatusOK, registrationCodeCreateResponse{ID: id, Code: plaintext})
}

func (m *managementModule) handleListRegistrationCodes(w http.ResponseWriter, r *http.Request) {
	if m.codes == nil {
		registrationCodeUnavailable(w, r)
		return
	}
	list, err := m.codes.List(r.Context())
	if err != nil {
		// store.ErrEnrollScopeCorrupted (a scope column that failed to decode)
		// or any other store failure must not reach the caller as err.Error() —
		// that could echo internal storage detail (org security policy §7:
		// production exceptions return a generic message, detail stays
		// server-side). This is deliberately NOT swallowed into an empty list:
		// a corrupted row failing the WHOLE list is the store's contract (Task
		// 7), and turning that into a silent empty page would hide the
		// corruption from the one surface an operator could act on it from.
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	out := make([]registrationCodeView, 0, len(list))
	for _, c := range list {
		out = append(out, registrationCodeView{
			ID:                c.ID,
			AllowedNamespaces: c.AllowedNamespaces,
			AllowedNodeTypes:  c.AllowedNodeTypes,
			Revoked:           c.Revoked,
			CreatedAt:         c.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	writeData(w, r, http.StatusOK, out)
}

func (m *managementModule) handleRevokeRegistrationCode(w http.ResponseWriter, r *http.Request) {
	if m.codes == nil {
		registrationCodeUnavailable(w, r)
		return
	}
	id := r.PathValue("id")
	if id == "" {
		writeFail(w, r, http.StatusNotFound, "registration_code_not_found", "registration code not found")
		return
	}
	err := m.codes.Revoke(r.Context(), id)
	if errors.Is(err, control.ErrRegistrationCodeNotFound) {
		writeFail(w, r, http.StatusNotFound, "registration_code_not_found", "registration code not found")
		return
	}
	if err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeData(w, r, http.StatusOK, map[string]string{"id": id, "status": "revoked"})
}

func (m *managementModule) handleRegistrationCodeAudit(w http.ResponseWriter, r *http.Request) {
	if m.codes == nil {
		registrationCodeUnavailable(w, r)
		return
	}
	id := r.PathValue("id")
	if id == "" {
		writeFail(w, r, http.StatusNotFound, "registration_code_not_found", "registration code not found")
		return
	}
	records, err := m.codes.EnrollAudit(r.Context(), id)
	if err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	out := make([]enrollAuditView, 0, len(records))
	for _, rec := range records {
		out = append(out, enrollAuditView{
			Success:  rec.Success,
			Reason:   rec.Reason,
			RunnerID: rec.RunnerID,
			SourceIP: rec.SourceIP,
			At:       rec.At.UTC().Format(time.RFC3339),
		})
	}
	writeData(w, r, http.StatusOK, out)
}
