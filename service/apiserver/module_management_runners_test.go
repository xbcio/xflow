package apiserver

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
	if strings.Contains(body, `"enrolled":false`) {
		t.Fatalf("500 body must not report enrolled:false for any runner when the issued-identity list itself failed: %q", body)
	}
}

type stubRunnerLister struct{ ids []string }

func (s *stubRunnerLister) ListRunners(context.Context) ([]string, error) { return s.ids, nil }

// stubIssuedIdentityLister is a minimal control.IssuedIdentityStore test
// double. Only List is exercised by this file; Issue/Lookup are unused stubs.
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
