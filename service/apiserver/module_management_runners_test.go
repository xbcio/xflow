package apiserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store"
)

func TestRunnerListOpHasScope(t *testing.T) {
	if scopeForOperation(OpManagementRunnerList) == "" {
		t.Fatal("scopeForOperation(OpManagementRunnerList) is empty; the route is unreachable")
	}
	// It must be its own scope, not an alias of the single-runner read: a token
	// that may look up one known runner should not thereby enumerate all of them.
	if scopeForOperation(OpManagementRunnerList) == scopeForOperation(OpManagementRunnerRead) {
		t.Fatal("runner list and runner read must not share a scope")
	}
}

// TestRunnerPathIsInUserFacingPaths is this task's own path-registration
// assertion (addendum Ruling 1): this repo has no test that guards the
// Path*-to-UserFacingPaths direction in general — paths_test.go's guardSamples
// and the OpenAPI contract guard both only check the OPPOSITE direction
// (UserFacingPaths ⊆ guardSamples / contract ⊆ registered). Without this test,
// dropping PathManagementRunners back out of UserFacingPaths would go
// undetected.
func TestRunnerPathIsInUserFacingPaths(t *testing.T) {
	for _, p := range UserFacingPaths {
		if p == PathManagementRunners {
			return
		}
	}
	t.Fatalf("%q missing from UserFacingPaths; nothing else in this package guards the Path*-to-UserFacingPaths direction", PathManagementRunners)
}

func TestRunnersListReturns501WhenDirectoryCannotEnumerate(t *testing.T) {
	// The runner directory has no list API. Returning an empty array would make
	// "nothing is online" and "this backend cannot answer" indistinguishable,
	// and those are exactly the two cases an operator is trying to tell apart.
	h := newRunnerListTestServer(t, nil)
	defer h.srv.Close()
	body := h.doJSON(t, http.MethodGet, PathManagementRunners, "", http.StatusNotImplemented)
	// This substring check is a forward-looking guard, not evidence about
	// today's behavior: writeFail never populates envelope.go's `Data` field,
	// and its `omitempty` tag makes the "data" key disappear entirely from a
	// failure body, so `"data":[]` cannot appear here under the current
	// envelope implementation regardless of what this handler does. Its real
	// job is to catch a FUTURE envelope change that starts emitting `data` on
	// failure responses. The teeth for THIS test come from doJSON's embedded
	// status-code comparison (fix1 review finding, table row 4).
	if strings.Contains(body, `"data":[]`) {
		t.Fatalf("501 body must not look like an empty result set: %q", body)
	}
	// spec §2.3.2 / judgment 13 pin the exact error code; brief's original
	// "runner_list_unsupported" spelling disagreed with the spec and was
	// corrected in the addendum (Ruling 6) to "runner_listing_unsupported".
	if !strings.Contains(body, `"runner_listing_unsupported"`) {
		t.Fatalf("501 body missing spec-mandated error code runner_listing_unsupported: %q", body)
	}
}

func TestRunnersListEnumeratesWhenSupported(t *testing.T) {
	lister := &stubRunnerLister{ids: []string{"runner-a", "runner-b"}}
	h := newRunnerListTestServer(t, lister)
	defer h.srv.Close()
	body := h.doJSON(t, http.MethodGet, PathManagementRunners, "", http.StatusOK)
	for _, id := range lister.ids {
		if !strings.Contains(body, id) {
			t.Fatalf("body %q missing runner %q", body, id)
		}
	}
}

// TestRunnersListMarksEnrolledRunners exercises the Enrolled projection: a
// runner_id present in the issued-identity store must show enrolled:true, one
// absent from it must show enrolled:false.
func TestRunnersListMarksEnrolledRunners(t *testing.T) {
	lister := &stubRunnerLister{ids: []string{"runner-a", "runner-b"}}
	h := newRunnerListTestServer(t, lister)
	defer h.srv.Close()
	h.issued.ids = []string{"runner-a"}
	body := h.doJSON(t, http.MethodGet, PathManagementRunners, "", http.StatusOK)
	if !strings.Contains(body, `"runner_id":"runner-a","enrolled":true`) {
		t.Fatalf("body %q: runner-a should be enrolled:true", body)
	}
	if !strings.Contains(body, `"runner_id":"runner-b","enrolled":false`) {
		t.Fatalf("body %q: runner-b should be enrolled:false", body)
	}
}

var errStubIssuedListFailed = errors.New("stub: issued identity list failed")

// TestRunnersListReturns500WhenIssuedListFails pins addendum Ruling 4: a
// failing m.issued.List (e.g. store.ErrEnrollScopeCorrupted after Task 7) must
// not be silently downgraded to enrolled:false for the whole fleet. That would
// misinform exactly the decision the Enrolled field exists to inform (which
// runners a revoked registration code affects) — reporting a wrong value is
// worse than refusing to answer.
func TestRunnersListReturns500WhenIssuedListFails(t *testing.T) {
	lister := &stubRunnerLister{ids: []string{"runner-a"}}
	h := newRunnerListTestServer(t, lister)
	defer h.srv.Close()
	h.issued.err = errStubIssuedListFailed
	body := h.doJSON(t, http.MethodGet, PathManagementRunners, "", http.StatusInternalServerError)
	// Same caveat as the 501 test's "data":[] check above: writeFail never
	// writes a body containing `"enrolled":false` (that key only appears on
	// the 200 success path's runnerListItem JSON), so this substring cannot
	// occur under the current implementation independent of correctness. It is
	// a forward-looking guard against a future change that starts emitting
	// per-runner fields on a failure body; the actual teeth here are doJSON's
	// embedded 500 status-code comparison (fix1 review finding, table row 4).
	if strings.Contains(body, `"enrolled":false`) {
		t.Fatalf("500 body must not report enrolled:false for any runner when the issued-identity list itself failed: %q", body)
	}
}

// TestRunnersListReturns500WhenListRunnersFails pins fix1 review finding
// (table row 2): m.runners.ListRunners itself failing (the directory
// structurally supports enumeration but cannot answer this particular call,
// e.g. a transient backend outage) must surface as a generic 500 -- not a
// silent downgrade to an empty list, which would look identical to "the
// directory truthfully has zero runners" and is exactly the ambiguity the
// 501 branch above this one exists to avoid (see handleListRunners' own
// top-of-function comment). Before this test, stubRunnerLister had no way to
// fail, so this branch was unexercised: an implementation change from
// `ids, err := ...; if err != nil { 500 }` to something that swallows err
// would have gone undetected.
func TestRunnersListReturns500WhenListRunnersFails(t *testing.T) {
	const uniqueDetail = "xzq-runnerlist-storage-detail-that-must-not-leak"
	lister := &stubRunnerLister{err: fmt.Errorf("runnerDirectory: backend unreachable (%s)", uniqueDetail)}
	h := newRunnerListTestServer(t, lister)
	defer h.srv.Close()
	body := h.doJSON(t, http.MethodGet, PathManagementRunners, "", http.StatusInternalServerError)
	// The teeth here: a 500 that echoed err.Error() would still be a 500, so
	// the status-code assertion above alone would not catch a leak. This is
	// the assertion doing the actual work, per org security policy §7
	// (production exceptions return a generic message; detail stays
	// server-side) -- mirrors TestListRegistrationCodesStoreFailureIsGeneric's
	// uniqueDetail technique for the sibling m.codes.List error branch.
	if strings.Contains(body, uniqueDetail) {
		t.Fatalf("response body leaked the runner directory's internal error detail: %q", body)
	}
}

// TestNewManagementModuleRunnerProbeWiresTheDefaultDirectory verifies that
// newManagementModule wires m.runners when the default RunnerDirectory
// satisfies runnerLister. Before MemoryRunnerDirectory grew ListRunners this
// test asserted the opposite — that no shipped directory satisfied
// runnerLister, so m.runners stayed nil and the route answered 501. The
// default directory satisfies it now, so the probe must wire it and the
// route must answer an honest empty list.
func TestNewManagementModuleRunnerProbeWiresTheDefaultDirectory(t *testing.T) {
	m := newManagementModule(fakeControlPlaneForAuthz(t))
	if m.runners == nil {
		t.Fatal("m.runners = nil, want non-nil: MemoryRunnerDirectory implements runnerLister")
	}
	ids, err := m.runners.ListRunners(context.Background())
	if err != nil {
		t.Fatalf("ListRunners: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("ListRunners on a fresh control plane = %v, want empty", ids)
	}
}

// listableRunnerDirectory is a control.RunnerDirectory test double that ALSO
// implements runnerLister, used only by
// TestNewManagementModuleRunnerProbeWiresListableDirectory below to prove the
// structural probe in newManagementModule actually wires a capable directory
// into m.runners -- not merely that the assignment line compiles. It embeds a
// real control.NewMemoryRunnerDirectory() so every RunnerDirectory method
// this test doesn't care about has a working, non-nil-receiver implementation
// (control.Config.RunnerDirectory is passed straight into
// selectRunnerDirectory and used for real directory operations, not just the
// probe).
type listableRunnerDirectory struct {
	control.RunnerDirectory
	ids []string
}

func (d listableRunnerDirectory) ListRunners(context.Context) ([]string, error) {
	return d.ids, nil
}

// TestNewManagementModuleRunnerProbeWiresListableDirectory pins fix1 review
// finding (table row 1), positive half: control.Config.RunnerDirectory has
// top priority in selectRunnerDirectory (service/control/controlplane.go:
// 150-153), so passing a listableRunnerDirectory there makes
// cp.RunnerDirectory() return a value that satisfies runnerLister, and
// newManagementModule's structural probe must wire it into m.runners. The
// assertion goes all the way to an HTTP round trip (not just `m.runners !=
// nil`): a non-nil field only proves an assignment happened, it does not
// prove the probed value is the one actually serving the request. Flipping
// the `ok` check in newManagementModule's probe (or deleting the probe block)
// makes this test fail with a 501 instead of 200.
func TestNewManagementModuleRunnerProbeWiresListableDirectory(t *testing.T) {
	dir := listableRunnerDirectory{
		RunnerDirectory: control.NewMemoryRunnerDirectory(),
		ids:             []string{"runner-probed-a", "runner-probed-b"},
	}
	cp, err := control.NewControlPlane(control.Config{
		Backend:         local.New(),
		RunnerDirectory: dir,
	})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	m := newManagementModule(cp)
	if m.runners == nil {
		t.Fatal("m.runners is nil after newManagementModule; the structural probe did not wire the listable directory")
	}
	m.principalAuth = staticPrincipalAuth{principal: Principal{
		Subject:   "ops",
		Namespace: "namespaceA",
		Scopes:    []string{scopeForOperation(OpManagementRunnerList)},
	}}
	m.authorizer = ScopeAuthorizer{}
	m.audit = NewInMemoryAuditSink()
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL+PathManagementRunners, strings.NewReader(""))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", PathManagementRunners, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	body := string(data)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (probed directory should have served the list); body = %q", resp.StatusCode, body)
	}
	for _, id := range dir.ids {
		if !strings.Contains(body, id) {
			t.Fatalf("body %q missing probed runner %q; the endpoint did not serve from the probed directory", body, id)
		}
	}
}

// stubRunnerLister is a minimal runnerLister test double. err, when set,
// makes ListRunners fail instead of returning ids (used by
// TestRunnersListReturns500WhenListRunnersFails; every other test in this
// file leaves err nil and gets ids back, matching the original brief stub's
// shape).
type stubRunnerLister struct {
	ids []string
	err error
}

func (s *stubRunnerLister) ListRunners(context.Context) ([]string, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.ids, nil
}

// stubIssuedIdentityLister is a minimal control.IssuedIdentityStore test
// double. Only List is exercised by this file; Issue/Lookup/Revoke/Renew are
// unused stubs.
type stubIssuedIdentityLister struct {
	ids []string
	err error
}

func (s *stubIssuedIdentityLister) Issue(context.Context, store.IssuedIdentity) error {
	return errors.New("stubIssuedIdentityLister: Issue not implemented")
}

func (s *stubIssuedIdentityLister) Lookup(context.Context, string) (store.IssuedIdentity, bool, error) {
	return store.IssuedIdentity{}, false, errors.New("stubIssuedIdentityLister: Lookup not implemented")
}

func (s *stubIssuedIdentityLister) List(context.Context) ([]store.IssuedIdentity, error) {
	if s.err != nil {
		return nil, s.err
	}
	out := make([]store.IssuedIdentity, 0, len(s.ids))
	for _, id := range s.ids {
		out = append(out, store.IssuedIdentity{RunnerID: id})
	}
	return out, nil
}

func (s *stubIssuedIdentityLister) Revoke(context.Context, string) error {
	return errors.New("stubIssuedIdentityLister: Revoke not implemented")
}

func (s *stubIssuedIdentityLister) Renew(context.Context, string, time.Time) error {
	return errors.New("stubIssuedIdentityLister: Renew not implemented")
}

// runnerListTestServer builds a managementModule wired with principalAuth
// holding OpManagementRunnerList's scope, an in-memory stub issued-identity
// store, and lister (nil means "directory cannot enumerate" -> m.runners stays
// nil).
type runnerListTestServer struct {
	srv    *httptest.Server
	issued *stubIssuedIdentityLister
}

func newRunnerListTestServer(t *testing.T, lister runnerLister) *runnerListTestServer {
	t.Helper()
	m := newManagementModule(fakeControlPlaneForAuthz(t))
	m.runners = lister
	issued := &stubIssuedIdentityLister{}
	m.issued = issued
	m.principalAuth = staticPrincipalAuth{principal: Principal{
		Subject:   "ops",
		Namespace: "namespaceA",
		Scopes:    []string{scopeForOperation(OpManagementRunnerList)},
	}}
	m.authorizer = ScopeAuthorizer{}
	m.audit = NewInMemoryAuditSink()
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)
	return &runnerListTestServer{srv: httptest.NewServer(mux), issued: issued}
}

func (h *runnerListTestServer) doJSON(t *testing.T, method, path, body string, wantStatus int) string {
	t.Helper()
	var reader *strings.Reader
	if body != "" {
		reader = strings.NewReader(body)
	} else {
		reader = strings.NewReader("")
	}
	req, err := http.NewRequest(method, h.srv.URL+path, reader)
	if err != nil {
		t.Fatalf("NewRequest %s %s: %v", method, path, err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := h.srv.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body for %s %s: %v", method, path, err)
	}
	got := string(data)
	if resp.StatusCode != wantStatus {
		t.Fatalf("%s %s: status = %d, want %d; body = %q", method, path, resp.StatusCode, wantStatus, got)
	}
	return got
}
