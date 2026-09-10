package apiserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
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
	// registrationCodeTTL is the deployment ceiling on how long a newly minted
	// registration code may live. Zero means no ceiling, which is what a server
	// upgraded without the flag carries — see resolveRegistrationCodeExpiry on
	// why that default has to be the passive one.
	//
	// It applies at MINT time only. Enforcement of an already-minted code's
	// deadline lives in RegistrationCodeStore.ResolveByPlaintext, which needs
	// no configuration at all, so lowering this value never retroactively
	// shortens a code already in someone's hands.
	registrationCodeTTL time.Duration
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
	// isNilValue, not dir != nil: RunnerDirectory returns an interface, and a
	// nil *MemoryRunnerDirectory stored in it is a non-nil interface that
	// passes dir != nil and then panics inside ListRunners. Same guard
	// AuditReconcilable uses, for the same reason.
	if dir := cp.RunnerDirectory(); !isNilValue(dir) {
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
		// Namespace boundary (Task 8 fix1 Important-3, superseded by Task 2's H1
		// fix): all four leave ResourceNamespace empty in their authzWrap
		// resolver funcs below, and — unlike handleExecution's
		// cross-namespace-read-404 pattern — there is no namespace-scoped
		// store read backing that emptiness via ResourceNamespace equality.
		// The boundary instead runs through the store's own owner scoping
		// (store.OwnerScope / RegistrationCode.OwnerNamespace, landed by Task
		// 1): each handler below projects the requesting principal onto an
		// OwnerScope via ownerScopeFor, so a tenant principal sees, revokes,
		// and audits only the codes it minted, and create stamps
		// OwnerNamespace with the principal's own namespace. A principal
		// additionally holding the matching *_global scope
		// (ScopeRegistrationCodeCreateGlobal/ListGlobal/RevokeGlobal/
		// AuditGlobal) gets OwnerScope{All: true} instead — the platform-wide,
		// list-ALL-codes behavior spec §2.3.4 describes. Do not remove
		// ownerScopeFor's per-handler call or the ceiling check in
		// resolveRequestedNamespaces to "simplify back" to a bare scope check;
		// that is the H1 privilege-escalation path this fix closes.
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
		// Runner identity revocation (Task 6): kills one runner's issued
		// identity so its next authenticated call is rejected by T5's gate in
		// IssuedIdentityAuthenticator.authenticate(). Like the registration-code
		// routes above (and for the same reason, Ruling Y) this has NO bare
		// fallback in the else branch below: a server with no
		// PrincipalAuthenticator configured must not expose an unauthenticated
		// way to kill a runner's credential. The mount is gated on
		// principalAuth alone, never on m.issued being non-nil (see the struct
		// field comment above) — a nil store leaves the route reachable but
		// its handler answers not_implemented.
		mux.HandleFunc("POST "+PathManagementRunnerRevokeIdentity, m.authzWrap(OpManagementRunnerRevokeIdentity, true, m.handleRevokeRunnerIdentity, func(r *http.Request) (string, string, string, string) {
			return "management/runners/" + r.PathValue("id") + "/revoke-identity", "", "", ""
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
	// This is a third distinct failure mode, alongside the two documented above
	// this function: the directory structurally supports enumeration (we got
	// past the m.runners == nil check) but answering THIS call failed -- e.g. a
	// transient backend outage. That is "the directory itself cannot answer
	// right now", not "the directory has zero runners"; degrading it to an
	// empty list (fix1 mutation drill: swapping this branch for
	// writeData(w, r, http.StatusOK, []runnerListItem{}) reproduces exactly
	// that regression) would silently manufacture the "nothing is online"
	// picture this handler's 501 branch exists to avoid. Generic 500 + no
	// err.Error() in the body, same as the m.issued.List branch below (org
	// security policy §7: production exceptions return a generic message,
	// detail stays server-side) -- TestRunnersListReturns500WhenListRunnersFails
	// pins both the status and the no-leak requirement.
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

// handleRevokeRunnerIdentity kills a runner's issued identity, so its next
// authenticated call is rejected by T5's gate in
// IssuedIdentityAuthenticator.authenticate() (RevokedAt is checked there,
// after the constant-time token compare). This handler does not re-prove that
// gate — it only drives the store call and reports the HTTP outcome.
//
// A nil m.issued (server has no issued-identity store configured) answers 501
// rather than 404: the route IS mounted (see RegisterHTTP's comment on why
// mounting never depends on m.issued being non-nil), but this backend cannot
// perform the operation at all, which is a different fact than "no such
// runner".
//
// The route registers "" as its resource namespace, which permanently disables
// NamespaceAwareAuthorizer's own ceiling for it, so ownership is enforced here
// and in the store instead: ownerScopeFor confines a tenant principal to the
// identities issued from its own namespace's codes, and only
// ScopeManagementRunnerRevokeIdentityGlobal lifts that. Removing either half
// reopens a cross-tenant DoS — one tenant knocking another's entire fleet
// offline.
func (m *managementModule) handleRevokeRunnerIdentity(w http.ResponseWriter, r *http.Request) {
	if m.issued == nil {
		writeFail(w, r, http.StatusNotImplemented, "not_implemented",
			"issued identity store is not configured")
		return
	}
	runnerID := r.PathValue("id")
	if runnerID == "" {
		writeFail(w, r, http.StatusNotFound, "runner_not_found", "runner not found")
		return
	}
	scope, ok := ownerScopeFor(r, ScopeManagementRunnerRevokeIdentityGlobal)
	if !ok {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	err := m.issued.Revoke(r.Context(), runnerID, scope)
	if errors.Is(err, control.ErrIssuedIdentityNotFound) {
		writeFail(w, r, http.StatusNotFound, "runner_not_found", "runner not found")
		return
	}
	if err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	// The runner id is an operator-supplied identifier, not a credential, so
	// logging it is fine. The raw token never appears anywhere in this
	// package; the store holds only its hash (TokenHash).
	slog.Info("runner identity revoked", "runner_id", runnerID, "owner_scope_all", scope.All)
	writeData(w, r, http.StatusOK, map[string]string{"runner_id": runnerID, "status": "revoked"})
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
	// ExpiresInSeconds is the requested lifetime. A pointer because absent and
	// 0 are different requests: absent means "use the deployment default",
	// while 0 explicitly asks for a code that never expires — and under a
	// deployment ceiling that is a request the server must refuse rather than
	// silently reinterpret. Seconds rather than a duration string ("24h")
	// because the OpenAPI contract has no unambiguous duration type and a
	// third-party client should not have to reimplement Go's parser.
	ExpiresInSeconds *int64 `json:"expires_in_seconds,omitempty"`
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
//
// Build it with newRegistrationCodeView, never by literal: the two slice
// fields need normalizing and a literal is exactly how that gets skipped.
type registrationCodeView struct {
	ID                string   `json:"id"`
	AllowedNamespaces []string `json:"allowed_namespaces"`
	AllowedNodeTypes  []string `json:"allowed_node_types"`
	Revoked           bool     `json:"revoked"`
	CreatedAt         string   `json:"created_at"`
	// ExpiresAt is omitted for a code that never expires, mirroring the
	// domain's zero value. There is deliberately no computed "expired" boolean
	// beside it: a second representation of the same fact is a second thing
	// that can drift from ResolveByPlaintext's verdict.
	ExpiresAt string `json:"expires_at,omitempty"`
}

// newRegistrationCodeView projects a stored code onto its wire shape.
//
// It exists for the two slice fields. A nil []string marshals to JSON `null`,
// not `[]`, and the OpenAPI schema declares both as arrays — so a code whose
// AllowedNodeTypes was never set (the create handler persists req.AllowedNodeTypes
// verbatim, and that field is optional) serialized a body the published
// contract rejects. RegistrationCode.Clone() preserves nil rather than
// widening it to empty, deliberately and correctly, so the normalization has
// to happen here, at the boundary where "no elements" stops being a Go
// distinction and becomes a wire one.
//
// The two are NOT interchangeable on the wire even though they are in Go: a
// generated client that types the field as a non-nullable array fails to
// decode `null`, and one that types it as nullable forces every caller to
// handle a state the server never means. `[]` is the honest encoding of "this
// code allows nothing" — which, for AllowedNodeTypes, is exactly what
// RunnerPolicy.Allows reports for an empty set.
func newRegistrationCodeView(c control.RegistrationCode) registrationCodeView {
	view := registrationCodeView{
		ID:                c.ID,
		AllowedNamespaces: emptyIfNil(c.AllowedNamespaces),
		AllowedNodeTypes:  emptyIfNil(c.AllowedNodeTypes),
		Revoked:           c.Revoked,
		CreatedAt:         c.CreatedAt.UTC().Format(time.RFC3339),
	}
	if !c.ExpiresAt.IsZero() {
		view.ExpiresAt = c.ExpiresAt.UTC().Format(time.RFC3339)
	}
	return view
}

// emptyIfNil returns s, or an empty non-nil slice when s is nil, so the field
// marshals to `[]` rather than `null`. It does not copy: the caller owns a
// cloned RegistrationCode already (both stores clone on the way out), and the
// view is marshaled and discarded.
func emptyIfNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

type enrollAuditView struct {
	Success  bool   `json:"success"`
	Reason   string `json:"reason,omitempty"`
	RunnerID string `json:"runner_id,omitempty"`
	SourceIP string `json:"source_ip,omitempty"`
	At       string `json:"at"`
}

// ownerScopeFor projects the request's principal onto the store's OwnerScope.
// The bool reports whether a principal was present at all; false means the
// route ran without authz middleware, which is a wiring bug and must be a 500
// rather than a silent widening.
func ownerScopeFor(r *http.Request, globalScope string) (control.OwnerScope, bool) {
	p, ok := principalFromRequest(r)
	if !ok {
		return control.OwnerScope{}, false
	}
	if p.HasScope(globalScope) {
		return control.OwnerScope{All: true}, true
	}
	// A principal with no namespace and no _global scope can see nothing. That
	// is fail-closed on purpose: OwnerScope{} would be rejected downstream by
	// Validate, so this returns the same false the missing-principal case does
	// and the caller turns it into a 500 — an unroutable configuration, not an
	// empty page that looks like "you have no codes".
	if p.Namespace == "" {
		return control.OwnerScope{}, false
	}
	return control.OwnerScope{Namespace: p.Namespace}, true
}

func (m *managementModule) handleCreateRegistrationCode(w http.ResponseWriter, r *http.Request) {
	if m.codes == nil {
		registrationCodeUnavailable(w, r)
		return
	}
	p, ok := principalFromRequest(r)
	if !ok {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	var req registrationCodeCreateRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	requested, err := resolveRequestedNamespaces(p, req.AllowedNamespaces)
	if err != nil {
		if errors.Is(err, errRegistrationCodeMissingNamespaces) {
			// A malformed request body, not a scope violation: the caller is
			// entitled to mint a global code, it just didn't say for which
			// namespaces. That is bad_request, not namespace_forbidden.
			writeFail(w, r, http.StatusBadRequest, "bad_request", err.Error())
			return
		}
		// The reason is a client-side scope error, not internal state, so it is
		// safe to name the offending namespace back. It is one the caller sent.
		writeFail(w, r, http.StatusForbidden, "namespace_forbidden", err.Error())
		return
	}
	id, plaintext, err := control.GenerateRegistrationCode()
	if err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	now := time.Now().UTC()
	expiresAt, err := resolveRegistrationCodeExpiry(now, m.registrationCodeTTL, req.ExpiresInSeconds)
	if err != nil {
		// 400, not 403: because the ceiling clamps _global creators too, there
		// is no principal for whom this same request would succeed. 403 would
		// wrongly imply "ask for a bigger scope"; the only way through is for
		// an operator to change --registration-code-ttl, which is a deployment
		// action, not an authorization one.
		writeFail(w, r, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	code := control.RegistrationCode{
		ID:                id,
		CodeHash:          control.HashSecret(plaintext),
		OwnerNamespace:    p.Namespace,
		AllowedNamespaces: requested,
		AllowedNodeTypes:  req.AllowedNodeTypes,
		CreatedAt:         now,
		ExpiresAt:         expiresAt,
	}
	if err := m.codes.Create(r.Context(), code); err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeData(w, r, http.StatusOK, registrationCodeCreateResponse{ID: id, Code: plaintext})
}

// maxRegistrationCodeTTLSeconds is where seconds stop fitting in a
// time.Duration (~292 years). It is an overflow guard, not a policy: a
// requested lifetime beyond it would wrap negative and mint a code that is
// already expired, which reads as "the server ignored my request".
const maxRegistrationCodeTTLSeconds = int64(math.MaxInt64) / int64(time.Second)

// resolveRegistrationCodeExpiry enforces the deployment ceiling on how long a
// newly minted code may live, and returns the absolute deadline to persist.
// The zero time means "never expires".
//
// ceiling <= 0 means no deployment ceiling, and MUST leave an absent request
// as "never": that is what makes a plain upgrade — new binary, no new flag —
// change nothing about codes minted before or after it. The reverse default
// would silently start expiring every code an operator mints the day they
// upgrade.
//
// Requests over the ceiling are REFUSED, not silently clamped down to it. A
// caller that asked for 90 days and got 24 hours without being told has a
// code that dies two months before it expects to, and nothing in the response
// says so. resolveRequestedNamespaces makes the same choice on the namespace
// axis for the same reason.
//
// An explicit 0 under a ceiling is likewise refused rather than reinterpreted:
// 0 means "never expires", the deployment has declared that no such code may
// be minted, and rewriting the request into the ceiling would hide the fact
// that the caller asked for something else entirely.
//
// This function does not look at the principal. That is the point: a holder of
// registration_code.create_global is clamped exactly like a tenant, so the
// only way to widen the ceiling is to change the flag — an auditable operator
// action on the host, not a scope someone can be granted.
func resolveRegistrationCodeExpiry(now time.Time, ceiling time.Duration, requestedSeconds *int64) (time.Time, error) {
	if requestedSeconds == nil {
		if ceiling <= 0 {
			return time.Time{}, nil
		}
		return now.Add(ceiling).UTC(), nil
	}
	req := *requestedSeconds
	if req < 0 {
		return time.Time{}, fmt.Errorf("expires_in_seconds must not be negative, got %d", req)
	}
	if req == 0 {
		if ceiling <= 0 {
			return time.Time{}, nil
		}
		return time.Time{}, fmt.Errorf(
			"expires_in_seconds 0 requests a code that never expires, but this server caps registration code lifetime at %s",
			ceiling)
	}
	if req > maxRegistrationCodeTTLSeconds {
		return time.Time{}, fmt.Errorf("expires_in_seconds %d is out of range", req)
	}
	want := time.Duration(req) * time.Second
	if ceiling > 0 && want > ceiling {
		return time.Time{}, fmt.Errorf(
			"expires_in_seconds %d exceeds this server's registration code lifetime cap of %s",
			req, ceiling)
	}
	return now.Add(want).UTC(), nil
}

// errRegistrationCodeMissingNamespaces is the sentinel a _global creator hits
// when it omits allowed_namespaces. Distinct from every other error this
// function returns (all namespace_forbidden-worthy) so the handler can answer
// 400 bad_request instead — this is a malformed request body, not a scope
// violation the caller lacks the right to make.
var errRegistrationCodeMissingNamespaces = errors.New("allowed_namespaces is required for a global registration code")

// resolveRequestedNamespaces enforces the ceiling: a code may never grant a
// namespace its creator does not itself hold.
//
// The allow decision reuses RunnerPolicy.AllowsNamespace by projecting the
// principal into a one-element policy, rather than comparing strings here. Two
// implementations of "is this namespace allowed" would drift, and the drift
// would be a privilege escalation — the same reasoning RegistrationCode.Policy
// documents.
//
// Note what the projection does to "*": AllowsNamespace compares
// namespace.Namespace("*") against the principal's single allowed namespace, so
// a wildcard request is simply a namespace nobody is named, and is rejected by
// the ordinary path. It is not special-cased, which is why it cannot be
// special-cased wrong — PROVIDED p.Namespace itself is a legal namespace name.
// That proviso is the guard immediately below: p.Namespace comes from a token
// file (cmd/server's loadAuthTokenMappings), and unlike every other reader of
// a principal's namespace, this function is the first to place p.Namespace on
// the *policy* side of AllowsNamespace. On the policy side "*" means match
// everything, so an unvalidated p.Namespace of "*" would hand a tenant
// principal a ceiling of "everything" — H1 again, through the one string this
// fix forgot to re-check. namespace.Validate rejects "*" along with every
// other illegal character, and covers the create_global branch too: that
// branch never builds a ceiling, but handleCreateRegistrationCode still
// stamps OwnerNamespace: p.Namespace regardless of branch, so an illegal
// value must not reach either branch.
func resolveRequestedNamespaces(p Principal, requested []string) ([]string, error) {
	if err := namespace.Validate(namespace.Namespace(p.Namespace)); err != nil {
		return nil, fmt.Errorf("principal namespace %q is not a legal namespace name: %w", p.Namespace, err)
	}
	if p.HasScope(ScopeRegistrationCodeCreateGlobal) {
		// A platform operator may mint anything, including "*". Everything else
		// still has to be a legal namespace name.
		if len(requested) == 0 {
			// Unlike the tenant branch below, there is no safe default to fill
			// in here: the tenant default is "my own namespace", but a _global
			// creator has no namespace-of-its-own reading to fall back to, and
			// silently expanding an omission to "*" is exactly the implicit
			// maximal grant this whole fix exists to eliminate. Make the
			// operator say what it wants.
			return nil, errRegistrationCodeMissingNamespaces
		}
		for _, ns := range requested {
			if ns == "*" {
				continue
			}
			if err := namespace.Validate(namespace.Namespace(ns)); err != nil {
				return nil, fmt.Errorf("namespace %q is not a legal namespace name", ns)
			}
		}
		return append([]string(nil), requested...), nil
	}
	if len(requested) == 0 {
		// Empty is NOT a safe default: AllowsNamespace reads an empty
		// AllowedNamespaces as "the default namespace ONLY", which is a grant
		// this principal does not hold unless it happens to BE the default
		// namespace. Fill it in explicitly.
		return []string{p.Namespace}, nil
	}
	ceiling := control.RunnerPolicy{AllowedNamespaces: []string{p.Namespace}}
	for _, ns := range requested {
		if ns != "*" {
			if err := namespace.Validate(namespace.Namespace(ns)); err != nil {
				return nil, fmt.Errorf("namespace %q is not a legal namespace name", ns)
			}
		}
		if !ceiling.AllowsNamespace(namespace.Namespace(ns)) {
			return nil, fmt.Errorf("namespace %q exceeds the caller's own namespace grant", ns)
		}
	}
	return append([]string(nil), requested...), nil
}

func (m *managementModule) handleListRegistrationCodes(w http.ResponseWriter, r *http.Request) {
	if m.codes == nil {
		registrationCodeUnavailable(w, r)
		return
	}
	scope, ok := ownerScopeFor(r, ScopeRegistrationCodeListGlobal)
	if !ok {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	list, err := m.codes.List(r.Context(), scope)
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
		out = append(out, newRegistrationCodeView(c))
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
	scope, ok := ownerScopeFor(r, ScopeRegistrationCodeRevokeGlobal)
	if !ok {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	err := m.codes.Revoke(r.Context(), id, scope)
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
	scope, ok := ownerScopeFor(r, ScopeRegistrationCodeAuditGlobal)
	if !ok {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	records, err := m.codes.EnrollAudit(r.Context(), id, scope)
	if errors.Is(err, control.ErrRegistrationCodeNotFound) {
		writeFail(w, r, http.StatusNotFound, "registration_code_not_found", "registration code not found")
		return
	}
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
