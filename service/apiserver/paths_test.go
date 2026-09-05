package apiserver

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/store/objectstore"
	"github.com/xbcio/xflow/types"
)

// bodyFor returns the request body a real client would send for the given
// (method, path), so the positive cases reach the handler instead of failing
// at JSON decode. Negative cases never reach a handler, so they get no body.
func bodyFor(method, path string) io.Reader {
	if method == http.MethodPost && path == "/v1/executions/ex-1/signals" {
		var buf bytes.Buffer
		_ = json.NewEncoder(&buf).Encode(signalRequest{Name: "s1"})
		return &buf
	}
	return nil
}

// TestExecutionRouteShapesAfterMuxPattern proves the Go 1.22 mux pattern
// registration resolves every path shape the hand-rolled TrimPrefix parser
// resolved, and rejects the ones it rejected. A silently widened route is an
// authz hole: resolveExecutionRoute's default branch was what made an
// unrecognized shape 404 instead of reaching a handler, and the mux-pattern
// conversion must preserve that refusal.
//
// Per Decision B: the bare trailing-slash case "/v1/executions/" asserts
// "not 2xx" rather than a pin to 404 — under ServeMux a trailing-slash path
// may be answered with a 301 clean-path redirect, and what the case defends is
// "no handler ran". The other three negatives ("/wait/extra", "POST /wait",
// "GET /cancel") keep their exact 404 assertions: they are the authorization
// boundary, and the mux must still refuse them with 404 (not 405, which would
// leak that the route exists).
func TestExecutionRouteShapesAfterMuxPattern(t *testing.T) {
	cases := []struct {
		method, path string
		wantStatus   int
	}{
		{http.MethodGet, "/v1/executions/ex-1", http.StatusOK},
		{http.MethodGet, "/v1/executions/ex-1/wait", http.StatusOK},
		{http.MethodPost, "/v1/executions/ex-1/cancel", http.StatusOK},
		{http.MethodPost, "/v1/executions/ex-1/signals", http.StatusOK},
		// Shapes that must NOT resolve — each was rejected by the old parser's
		// default branch.
		{http.MethodGet, "/v1/executions/", http.StatusNotFound},
		{http.MethodGet, "/v1/executions/ex-1/wait/extra", http.StatusNotFound},
		{http.MethodPost, "/v1/executions/ex-1/wait", http.StatusNotFound},
		{http.MethodGet, "/v1/executions/ex-1/cancel", http.StatusNotFound},
	}

	// inspect returns a terminal status so handleWait returns immediately
	// instead of long-polling; DeliverSignal/Cancel succeed so the positive
	// cases reach 200.
	f := &fakeControlFacade{
		inspect: engine.ExecutionDetail{ExecutionID: "ex-1", Status: types.ExecutionStatusSuccess},
	}
	mux := newControlMux(f)

	for _, c := range cases {
		req := httptest.NewRequest(c.method, c.path, bodyFor(c.method, c.path))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)

		// Decision B: the bare trailing-slash path may be answered with a 301
		// clean-path redirect rather than 404; assert "no handler ran" (not 2xx).
		if c.path == "/v1/executions/" {
			if rec.Code >= 200 && rec.Code < 300 {
				t.Errorf("%s %s: status = %d, want not 2xx (no handler should run)", c.method, c.path, rec.Code)
			}
			continue
		}
		if rec.Code != c.wantStatus {
			t.Errorf("%s %s: status = %d, want %d", c.method, c.path, rec.Code, c.wantStatus)
		}
	}
}

// guardSample is one (path constant, HTTP method, concrete sample path) triple
// used by the dead-constant guard. The sample substitutes real-looking values
// for every {placeholder} so a ServeMux pattern can actually match it. Samples
// deliberately do NOT end in "/" — a trailing slash triggers ServeMux's
// clean-path 301 redirect, and what Handler() returns for that case is a
// redirect handler whose pattern semantics differ from a real registration
// (see TestTrailingSlashRedirectHandlerPattern).
type guardSample struct {
	constName string // the Go constant's name, for error messages
	pathConst string // the path constant value (e.g. "/v1/workflows/{id}")
	method    string
	sample    string
}

// guardSamples is the hand-written table mapping every UserFacingPaths entry
// to the (method, sample) the dead-constant guard probes. Step 3 of the guard
// asserts this table covers UserFacingPaths in lockstep — adding a path
// constant without adding a row here is the same drift defect the guard exists
// to catch (field-by-field-copy-drops-new-fields shape).
var guardSamples = []guardSample{
	{"PathWorkflows", PathWorkflows, http.MethodPost, "/v1/workflows"},
	{"PathWorkflowByID", PathWorkflowByID, http.MethodGet, "/v1/workflows/wf-1"},
	{"PathWorkflowExecute", PathWorkflowExecute, http.MethodPost, "/v1/workflows/execute"},
	{"PathWorkflowExecuteByID", PathWorkflowExecuteByID, http.MethodPost, "/v1/workflows/wf-1/execute"},

	{"PathExecutionByID", PathExecutionByID, http.MethodGet, "/v1/executions/ex-1"},
	{"PathExecutionCancel", PathExecutionCancel, http.MethodPost, "/v1/executions/ex-1/cancel"},
	{"PathExecutionSignals", PathExecutionSignals, http.MethodPost, "/v1/executions/ex-1/signals"},
	{"PathExecutionSignalByID", PathExecutionSignalByID, http.MethodDelete, "/v1/executions/ex-1/signals/s1"},
	{"PathExecutionWait", PathExecutionWait, http.MethodGet, "/v1/executions/ex-1/wait"},

	{"PathSupplyByName", PathSupplyByName, http.MethodGet, "/v1/supplies/rules"},
	// The sample digest MUST be format-valid (sha256:<64 hex>): the artifact
	// route validates the digest in the route resolver BEFORE authz, and an
	// invalid digest yields 404 (route_not_found) from authzWrapResolved rather
	// than the 401 the behavioral guard expects. The mux.Handler probe in guard
	// 1 does not run the resolver, so any sample shape would pass there; this
	// sample is shaped for the stricter behavioral half. The hex is the sha256
	// of the empty string — a real, well-formed digest that no tenant references.
	{"PathArtifactByDigest", PathArtifactByDigest, http.MethodGet, "/v1/artifacts/sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},

	{"PathManagementLeader", PathManagementLeader, http.MethodGet, "/v1/management/leader"},
	{"PathManagementRunnerByID", PathManagementRunnerByID, http.MethodGet, "/v1/management/runners/r-1"},
	{"PathManagementExecByID", PathManagementExecByID, http.MethodGet, "/v1/management/executions/ex-1"},
	{"PathManagementDeadLetters", PathManagementDeadLetters, http.MethodGet, "/v1/management/dead-letters/ex-1"},
	{"PathManagementDLReplay", PathManagementDLReplay, http.MethodPost, "/v1/management/dead-letters/ex-1/replay"},

	// Registration-code routes (Task 8). newFullGuardMux's Config carries no
	// RegistrationCodes/IssuedIdentities store, so mgmt.codes is nil on this
	// fixture — but per Ruling W the routes are mounted on principalAuth alone,
	// so they still resolve here; guard 2's behavioral half below still gets its
	// 401 (the authz wrapper runs before the nil-store handler body).
	{"PathManagementRegistrationCodes", PathManagementRegistrationCodes, http.MethodPost, "/v1/management/registration-codes"},
	{"PathManagementRegistrationCodeByID", PathManagementRegistrationCodeByID, http.MethodDelete, "/v1/management/registration-codes/rc-1"},
	{"PathManagementRegistrationCodeAudit", PathManagementRegistrationCodeAudit, http.MethodGet, "/v1/management/registration-codes/rc-1/audit"},

	{"PathHealthz", PathHealthz, http.MethodGet, "/healthz"},
	{"PathReadyz", PathReadyz, http.MethodGet, "/readyz"},
}

// newFullGuardMux builds the production-shape user-facing mux via apiserver.New
// + WithManagement, so all four user-facing modules (workflow-control,
// management, supply, artifact) register through their real RegisterHTTP paths
// — the same code APIServer.Handler() runs in production. The principal
// authenticator is a fail-closed stub that denies every request: guard 1 probes
// mux.Handler (which never invokes handlers, so the deny never fires), and
// guard 2's behavioral half asserts every protected route returns 401 without
// credentials — so the stub's deny is the point, not a limitation.
//
// Supply/artifact modules register ONLY when PrincipalAuth + their store are
// both set, so this fixture must configure all of them or PathSupplyByName /
// PathArtifactByDigest would be silently uncovered.
func newFullGuardMux(t *testing.T) *http.ServeMux {
	t.Helper()
	dir := t.TempDir()
	as := store.NewArtifactStore(objectstore.NewFSStore(dir), &memArtifactIndex{})
	srv, err := New(Config{
		PrincipalAuth: staticPrincipalAuth{err: ErrWorkflowUnauthenticated},
		Authorizer:    ScopeAuthorizer{},
		AuditSink:     NewInMemoryAuditSink(),
		Supplies:      memstore.New(),
		Artifacts:     as,
	}, WithManagement())
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	h := srv.Handler()
	mux, ok := h.(*http.ServeMux)
	if !ok {
		t.Fatalf("srv.Handler() = %T, want *http.ServeMux (no middleware/tracer configured)", h)
	}
	return mux
}

// TestUserFacingPathsHaveMuxRegistration is the §2.2 dead-constant guard: every
// exported user-facing path constant must resolve to a registered route on the
// production mux. A constant with no registration is a dead contract — code that
// builds a URL against it gets a 404, and nothing told the author. This guard
// was born green (T8 cleaned the last dead constants); it exists to prevent the
// next one, not to fix a present defect.
//
// It probes (*http.ServeMux).Handler(req), whose second return is the matched
// pattern — non-empty for both method-qualified patterns and prefix
// registrations, empty for unregistered paths. Prefix registrations
// (/v1/artifacts/, /v1/supplies/, /v1/management/dead-letters/) are why a
// string-compare-against-registered-patterns guard would false-positive; this
// probe does not.
func TestUserFacingPathsHaveMuxRegistration(t *testing.T) {
	mux := newFullGuardMux(t)

	// Step 3: the hand-written table MUST cover every UserFacingPaths entry, or
	// adding a path constant without a row here would silently escape the guard.
	tablePaths := make(map[string]string, len(guardSamples)) // pathConst -> constName
	for _, gs := range guardSamples {
		if prev, dup := tablePaths[gs.pathConst]; dup {
			t.Fatalf("duplicate table entry for %q (also %s)", gs.pathConst, prev)
		}
		tablePaths[gs.pathConst] = gs.constName
	}
	var missing []string
	for _, p := range UserFacingPaths {
		if _, ok := tablePaths[p]; !ok {
			missing = append(missing, p)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Fatalf("guardSamples table is missing UserFacingPaths entries (add them or the guard silently skips them): %v", missing)
	}

	for _, gs := range guardSamples {
		req := httptest.NewRequest(gs.method, gs.sample, nil)
		_, pattern := mux.Handler(req)
		if pattern == "" {
			t.Errorf("%s = %q: %s %s matched no registered route (dead constant or wrong method/sample); "+
				"this is the defect the guard exists to catch", gs.constName, gs.pathConst, gs.method, gs.sample)
		}
	}
}

// TestTrailingSlashRedirectHandlerPattern documents the one ServeMux edge the
// dead-constant guard's samples must avoid: a bare-prefix path (e.g.
// /v1/artifacts with no trailing slash and no segment) is answered by
// ServeMux's clean-path 301 redirect, and Handler() reports that redirect as a
// NON-empty pattern. A guard that probed a bare-prefix sample would therefore
// report an unregistered path as registered (false green). guardSamples avoids
// this by always including a real segment after any prefix registration
// (/v1/artifacts/sha256:abc, not /v1/artifacts). This test pins WHY so a future
// author does not relax the rule.
//
// Conclusion (empirically verified under Go 1.22 ServeMux):
//   - bare prefix without trailing slash → 301 redirect → non-empty pattern
//     (would mask a dead constant; samples must not use this shape).
//   - method-qualified exact pattern + a wrong-method trailing-slash request →
//     empty pattern (no redirect synthesized for a method mismatch).
//   - genuinely unregistered path → empty pattern (the guard's dead-constant
//     signal).
func TestTrailingSlashRedirectHandlerPattern(t *testing.T) {
	mux := newFullGuardMux(t)

	// The masking case: /v1/artifacts/ is registered as a subtree prefix. A
	// request to the bare /v1/artifacts (no segment, no trailing slash) is
	// answered by a clean-path 301 redirect, which Handler() reports as a
	// non-empty pattern. A guard sample shaped like this would mask a dead
	// constant, which is exactly why guardSamples uses /v1/artifacts/sha256:abc.
	req := httptest.NewRequest(http.MethodGet, "/v1/artifacts", nil)
	_, pattern := mux.Handler(req)
	if pattern == "" {
		t.Fatalf("bare-prefix GET /v1/artifacts returned empty pattern; ServeMux clean-path redirect behavior changed — re-evaluate the guard's sample-shape rule")
	}

	// The guard's actual sample shape (with a real segment) matches the prefix
	// directly, not via a redirect — so it proves the route genuinely serves
	// that shape, not that a redirect exists.
	goodReq := httptest.NewRequest(http.MethodGet, "/v1/artifacts/sha256:abc", nil)
	_, goodPattern := mux.Handler(goodReq)
	if goodPattern == "" {
		t.Fatalf("GET /v1/artifacts/sha256:abc returned empty pattern; the prefix registration is missing")
	}

	// A genuinely unregistered path returns empty — this is the signal the
	// dead-constant guard relies on for the "no route" verdict.
	unk := httptest.NewRequest(http.MethodGet, "/v1/does-not-exist/x", nil)
	_, unkPattern := mux.Handler(unk)
	if unkPattern != "" {
		t.Fatalf("unregistered path returned non-empty pattern %q; the guard's empty-pattern signal is broken", unkPattern)
	}
}
