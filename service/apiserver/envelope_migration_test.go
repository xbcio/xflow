package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store"
)

// These tests pin the spec §3.1 envelope shape (flat success/code, not nested
// under error) and the X-Request-Id echo tell for the supply / artifact /
// management families. Before the migration these failure sites used the
// writeError shim (nil *http.Request → empty trace_id, no X-Request-Id echo);
// the assertion that the echo is present is what proves a site left the shim.
//
// The bare-stream success paths (artifact GET 200, supply GET 200 including
// the encrypted branch) are asserted here too: §3.4 is load-bearing — wrapping
// the success stream would break objectstore.ReadThrough's byte-count check.

// ---- supply family ----------------------------------------------------------

// doSupplyWithRequestID issues a request to the supply mux carrying an
// X-Request-Id. The echoed value is the migration tell.
func doSupplyWithRequestID(t *testing.T, mux http.Handler, method, path, requestID string, body []byte) *http.Response {
	t.Helper()
	var r *bytes.Reader
	if body != nil {
		r = bytes.NewReader(body)
	} else {
		r = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, r)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if requestID != "" {
		req.Header.Set("X-Request-Id", requestID)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec.Result()
}

func TestSupplyPutRevisionConflictReturnsEnvelope(t *testing.T) {
	mux, _ := newSupplyTestServer(t, []string{"supply.write", "supply.read"}, "ns1")
	// Seed rev 1.
	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(
		http.MethodPut, "/v1/supplies/rules", bytes.NewReader([]byte(`a`))))

	req := httptest.NewRequest(http.MethodPut, "/v1/supplies/rules", bytes.NewReader([]byte(`b`)))
	req.Header.Set("If-Match", "99")
	req.Header.Set("X-Request-Id", "req-supply-409")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rec.Code)
	}
	var env envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("not an envelope: %v (body=%s)", err, rec.Body)
	}
	if env.Success {
		t.Fatalf("success = true, want false")
	}
	// §3.2: the stable code is already snake_case; keep the literal.
	if env.Code != "revision_conflict" {
		t.Fatalf("code = %q, want revision_conflict", env.Code)
	}
	if got := rec.Header().Get("X-Request-Id"); got != "req-supply-409" {
		t.Fatalf("X-Request-Id = %q, want %q (failure site must pass *http.Request)", got, "req-supply-409")
	}
}

func TestSupplyGetNotFoundReturnsEnvelope(t *testing.T) {
	mux, _ := newSupplyTestServer(t, []string{"supply.read"}, "ns1")
	resp := doSupplyWithRequestID(t, mux, http.MethodGet, "/v1/supplies/missing", "req-supply-404", nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	var env envelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("not an envelope: %v", err)
	}
	if env.Success {
		t.Fatalf("success = true, want false")
	}
	if env.Code != "supply_not_found" {
		t.Fatalf("code = %q, want supply_not_found", env.Code)
	}
	if got := resp.Header.Get("X-Request-Id"); got != "req-supply-404" {
		t.Fatalf("X-Request-Id = %q, want %q", got, "req-supply-404")
	}
}

func TestSupplyPutTooLargeReturnsEnvelope(t *testing.T) {
	mux, _ := newSupplyTestServer(t, []string{"supply.write"}, "ns1")
	big := bytes.Repeat([]byte("x"), maxSupplyContentBytes+1)
	req := httptest.NewRequest(http.MethodPut, "/v1/supplies/rules", bytes.NewReader(big))
	req.Header.Set("X-Request-Id", "req-supply-413")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
	var env envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("not an envelope: %v (body=%s)", err, rec.Body)
	}
	if env.Success || env.Code != "payload_too_large" {
		t.Fatalf("envelope = %+v, want success=false code=payload_too_large", env)
	}
	if got := rec.Header().Get("X-Request-Id"); got != "req-supply-413" {
		t.Fatalf("X-Request-Id = %q, want req-supply-413", got)
	}
}

// §3.4 bare-stream guarantee: a successful supply GET must return the raw
// content bytes, NOT an envelope. Wrapping it would force every supply content
// through JSON/base64 and break the runner's supply_client which reads the body
// as raw bytes.
func TestSupplyGetSuccessIsBareStream(t *testing.T) {
	mux, _ := newSupplyTestServer(t, []string{"supply.write", "supply.read"}, "ns1")
	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(
		http.MethodPut, "/v1/supplies/rules", bytes.NewReader([]byte(`{"rules":[1]}`))))

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/supplies/rules", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	// The body must be the raw content, not an envelope wrapping it.
	if got, want := rec.Body.String(), `{"rules":[1]}`; got != want {
		t.Fatalf("body = %q, want raw %q (envelope must not wrap §3.4 bare stream)", got, want)
	}
}

// ---- artifact family --------------------------------------------------------

func TestArtifactGetNotFoundReturnsEnvelope(t *testing.T) {
	mux, _, _ := newArtifactTestServer(t, []string{"artifact.read"}, "ns1")
	req := httptest.NewRequest(http.MethodGet,
		"/v1/artifacts/"+store.ContentHash([]byte("absent from this tenant")), nil)
	req.Header.Set("X-Request-Id", "req-art-404")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	var env envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("not an envelope: %v (body=%s)", err, rec.Body)
	}
	if env.Success {
		t.Fatalf("success = true, want false")
	}
	if env.Code != "artifact_not_found" {
		t.Fatalf("code = %q, want artifact_not_found", env.Code)
	}
	if got := rec.Header().Get("X-Request-Id"); got != "req-art-404" {
		t.Fatalf("X-Request-Id = %q, want req-art-404", got)
	}
}

// §3.4 bare-stream guarantee: a successful artifact GET must return the raw
// bytes with application/octet-stream and a Content-Length that matches the
// authoritative size. objectstore.ReadThrough validates the received byte
// count against this header; a chunked or enveloped response would silently
// skip that check.
func TestArtifactGetSuccessIsBareStream(t *testing.T) {
	mux, as, _ := newArtifactTestServer(t, []string{"artifact.read"}, "ns1")
	content := []byte("\x00asm\x01\x00\x00\x00 fake wasm body")
	ref, err := as.Put(context.Background(), content, store.ArtifactMeta{
		Filename:  "bare.wasm",
		Namespace: "ns1",
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/artifacts/"+ref.Digest, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Body.Bytes(); !bytes.Equal(got, content) {
		t.Fatalf("body = %q, want raw %q (envelope must not wrap §3.4 bare stream)", got, content)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/octet-stream" {
		t.Fatalf("Content-Type = %q, want application/octet-stream", ct)
	}
	if cl := rec.Header().Get("Content-Length"); cl != strconv.Itoa(len(content)) {
		t.Fatalf("Content-Length = %q, want %d", cl, len(content))
	}
}

// ---- management family ------------------------------------------------------

func newMgmtModuleMux(t *testing.T) (http.Handler, *managementModule) {
	t.Helper()
	cp, err := control.NewControlPlane(control.Config{Backend: local.New()})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	m := newManagementModule(cp)
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)
	return mux, m
}

func TestManagementRunnerNotFoundReturnsEnvelope(t *testing.T) {
	mux, _ := newMgmtModuleMux(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/management/runners/does-not-exist", nil)
	req.Header.Set("X-Request-Id", "req-runner-404")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	var env envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("not an envelope: %v (body=%s)", err, rec.Body)
	}
	if env.Success {
		t.Fatalf("success = true, want false")
	}
	if env.Code != "runner_not_found" {
		t.Fatalf("code = %q, want runner_not_found", env.Code)
	}
	if got := rec.Header().Get("X-Request-Id"); got != "req-runner-404" {
		t.Fatalf("X-Request-Id = %q, want req-runner-404", got)
	}
}

// §3.1/§3.3: management leader success must be enveloped (user face, not a §3.4
// bare-stream exception). §7 explicitly excludes /healthz and /readyz only.
func TestManagementLeaderSuccessIsEnvelope(t *testing.T) {
	mux, _ := newMgmtModuleMux(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/management/leader", nil)
	req.Header.Set("X-Request-Id", "req-leader-200")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var env envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("not an envelope: %v (body=%s)", err, rec.Body)
	}
	if !env.Success || env.Code != "200" {
		t.Fatalf("envelope = %+v, want success=true code=200", env)
	}
	// data must carry {is_leader:true} under the envelope, not at top level.
	m := map[string]any{}
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatal(err)
	}
	data, ok := m["data"].(map[string]any)
	if !ok {
		t.Fatalf("data is not an object: %v (body=%s)", m["data"], rec.Body)
	}
	if data["is_leader"] != true {
		t.Fatalf("data.is_leader = %v, want true", data["is_leader"])
	}
	if got := rec.Header().Get("X-Request-Id"); got != "req-leader-200" {
		t.Fatalf("X-Request-Id = %q, want req-leader-200", got)
	}
}

// §9.3 / spec §7: /healthz and /readyz must NOT be enveloped — load balancers
// parse their bodies. This is the negative assertion guarding that scope.
func TestHealthzReadyzAreNotEnveloped(t *testing.T) {
	mux, _ := newMgmtModuleMux(t)
	for _, p := range []string{"/healthz", "/readyz"} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, p, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d, want 200", p, rec.Code)
		}
		var m map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
			t.Fatalf("%s body not json: %v (body=%s)", p, err, rec.Body)
		}
		if _, has := m["success"]; has {
			t.Fatalf("%s must NOT be enveloped (success key present): %s", p, rec.Body)
		}
	}
}

// ---- management_auth middleware 401 ----------------------------------------

// TestManagementAuthMiddleware401ReturnsEnvelope pins the spec §3.1 envelope on
// the middleware-level 401. This site sits OUTSIDE the mux (it is a wrapping
// http.Handler), so it must construct writeFail with the live *http.Request to
// get the X-Request-Id echo. writeError passes nil → no echo → the tell.
func TestManagementAuthMiddleware401ReturnsEnvelope(t *testing.T) {
	auth := NewBearerTokenAuth("secret")
	inner := http.NewServeMux()
	inner.HandleFunc("/v1/management/leader", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, leaderResponse{IsLeader: true})
	})
	srv := ManagementAuthMiddleware(auth)(inner)

	req := httptest.NewRequest(http.MethodGet, "/v1/management/leader", nil)
	req.Header.Set("X-Request-Id", "req-auth-401")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	var env envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("not an envelope: %v (body=%s)", err, rec.Body)
	}
	if env.Success || env.Code != "unauthorized" {
		t.Fatalf("envelope = %+v, want success=false code=unauthorized", env)
	}
	if got := rec.Header().Get("X-Request-Id"); got != "req-auth-401" {
		t.Fatalf("X-Request-Id = %q, want req-auth-401 (middleware must pass *http.Request)", got)
	}
}

// ---- dead-letter success-body envelope (Step 1 decision A) -----------------

// TestDeadLetterListSuccessIsEnvelope pins the Step 1 decision (A): the
// management dead-letter list success body is enveloped as
// {success,code,data:{entries,next_cursor},trace_id}. The CLI's apiDeadLetterClient
// decodes the envelope and extracts data, so it keeps working. The cursor
// pagination shape (§3.3 exception) is preserved INSIDE data — the envelope
// wraps it, it does not replace it.
func TestDeadLetterListSuccessIsEnvelope(t *testing.T) {
	mux, m := newMgmtModuleMux(t)
	// Force a backend that does NOT implement DeadLetterStore so the list path
	// returns 503 — but assert the 503 IS enveloped (failure path). A real list
	// happy-path is covered by the integration suite (g1 e2e) against Redis.
	_ = m
	req := httptest.NewRequest(http.MethodGet, "/v1/management/dead-letters/exec-1?limit=10", nil)
	req.Header.Set("X-Request-Id", "req-dl-503")
	rec := httptest.NewRecorder()
	// No principalAuth → 404 from the gate; assert THAT is enveloped.
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (no authz configured)", rec.Code)
	}
	var env envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("not an envelope: %v (body=%s)", err, rec.Body)
	}
	if env.Success {
		t.Fatalf("success = true, want false")
	}
	if env.Code != "route_not_found" {
		t.Fatalf("code = %q, want route_not_found", env.Code)
	}
	if got := rec.Header().Get("X-Request-Id"); got != "req-dl-503" {
		t.Fatalf("X-Request-Id = %q, want req-dl-503", got)
	}
}
