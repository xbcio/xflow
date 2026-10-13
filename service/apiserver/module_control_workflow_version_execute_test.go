package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

// This file covers the version field of POST /v1/workflows/{id}/execute
// (versioning design §2.3). The properties pinned here are the ones a mistake
// would not surface as a compile error:
//
//   - an omitted or current version runs the live record (the pre-versions
//     behavior, byte for byte);
//   - a retired version runs FROM HISTORY, so the execution's graph carries
//     the retired version label — proving the archived payload, not a live
//     lookup, produced the run;
//   - the version field can never retarget the call: another workflow's
//     version label on this id is 404 workflow_version_not_found, and a
//     cross-namespace id is the by-id 404 workflow_not_found;
//   - an archived version whose stored payload no longer reproduces under the
//     current compiler is refused with 409 workflow_version_unavailable
//     (fail closed, never silently recompiled), while the current version of
//     the same workflow still runs;
//   - the version field and the Idempotency-Key header compose (design §2.7):
//     a keyed retry pinned to a retired version replays the original execution
//     even after an intervening publish, while an unpinned retry that now
//     resolves another version is 409 idempotency_key_conflict.

// newVersionExecuteTestServer builds the local-provider server fixture that can
// both register/replace workflows and record their executions, so a test can
// read back WHICH version ran from the execution snapshot's graph.
func newVersionExecuteTestServer(t *testing.T) (*httptest.Server, *local.Backend) {
	t.Helper()
	provider := local.New()
	cp, err := control.NewControlPlane(control.Config{Backend: provider})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	srv, err := New(Config{
		Concurrency:   4,
		PrincipalAuth: NewBearerPrincipalAuthMulti(listTestPrincipals()),
		Authorizer:    NamespaceAwareAuthorizer{},
		AuditSink:     NewInMemoryAuditSink(),
	}, WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)
	return httpSrv, provider
}

func postExecuteByID(t *testing.T, base, token, id string, body any) *http.Response {
	t.Helper()
	return postExecuteByIDKeyed(t, base, token, id, "", body)
}

// postExecuteByIDKeyed is postExecuteByID with an Idempotency-Key header; an
// empty key sends the header-less request the original helper sends.
func postExecuteByIDKeyed(t *testing.T, base, token, id, key string, body any) *http.Response {
	t.Helper()
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatalf("encode request: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, base+"/v1/workflows/"+id+"/execute", &buf)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set(IdempotencyKeyHeader, key)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/workflows/%s/execute: %v", id, err)
	}
	return resp
}

// executedWorkflowVersion issues the request already made and reads the version
// label off the created execution's graph snapshot — the execution's own record
// of which version it was submitted against.
func executedWorkflowVersion(t *testing.T, provider *local.Backend, resp *http.Response) string {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("execute status = %d, want 200 (body=%s)", resp.StatusCode, body)
	}
	var out executeWorkflowResponse
	if env := decodeEnvelope(t, resp, &out); !env.Success {
		t.Fatalf("execute envelope = %+v, want success", env)
	}
	if out.ExecutionID == "" {
		t.Fatal("execute returned an empty execution_id")
	}
	snap, err := provider.State().GetExecution(context.Background(), out.ExecutionID)
	if err != nil {
		t.Fatalf("GetExecution(%q): %v", out.ExecutionID, err)
	}
	if snap == nil {
		t.Fatalf("execution %q was not created", out.ExecutionID)
	}
	if snap.Graph == nil {
		t.Fatal("execution snapshot carries no graph")
	}
	return snap.Graph.WorkflowVersion()
}

// TestExecuteWorkflowByIDResolvesTheRequestedVersion pins the three resolution
// branches of the version field against the same replaced workflow: omitted and
// current run the live v2, and the retired v1 runs from its history payload.
func TestExecuteWorkflowByIDResolvesTheRequestedVersion(t *testing.T) {
	srv, provider := newVersionExecuteTestServer(t)
	id := registerListWorkflow(t, srv.URL, "tok-a", "vrun", "v1")
	publishRun(t, srv.URL, "tok-a", id, "vrun", "v2")

	cases := []struct {
		name    string
		version string
		want    string
	}{
		{name: "omitted runs the current version", version: "", want: "v2"},
		{name: "explicit current version", version: "v2", want: "v2"},
		{name: "retired version runs from history", version: "v1", want: "v1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := postExecuteByID(t, srv.URL, "tok-a", id, executeRegisteredRequest{Version: tc.version})
			if got := executedWorkflowVersion(t, provider, resp); got != tc.want {
				t.Fatalf("executed version = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestExecuteWorkflowByIDVersionCannotRetargetOrCrossNamespace pins that the
// version field selects a version OF THE ADDRESSED WORKFLOW and nothing else:
// another workflow's version label is not found on this id, and a
// cross-namespace id stays the by-id 404.
func TestExecuteWorkflowByIDVersionCannotRetargetOrCrossNamespace(t *testing.T) {
	srv, _ := newVersionExecuteTestServer(t)
	idA := registerListWorkflow(t, srv.URL, "tok-a", "vrun-a", "v1")
	registerListWorkflow(t, srv.URL, "tok-a", "vrun-b", "v2")

	cases := []struct {
		name     string
		token    string
		id       string
		version  string
		wantCode int
		want     string
	}{
		{
			name: "unknown version", token: "tok-a", id: idA, version: "v9",
			wantCode: http.StatusNotFound, want: "workflow_version_not_found",
		},
		{
			// vrun-b is registered at v2 in the SAME namespace; addressing it
			// through vrun-a's id must not resolve to the sibling workflow.
			name: "sibling workflow's version label", token: "tok-a", id: idA, version: "v2",
			wantCode: http.StatusNotFound, want: "workflow_version_not_found",
		},
		{
			name: "cross-namespace id", token: "tok-b", id: idA, version: "v1",
			wantCode: http.StatusNotFound, want: "workflow_not_found",
		},
		{
			name: "unknown workflow id", token: "tok-a", id: "wf-does-not-exist", version: "v1",
			wantCode: http.StatusNotFound, want: "workflow_not_found",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := postExecuteByID(t, srv.URL, tc.token, tc.id, executeRegisteredRequest{Version: tc.version})
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != tc.wantCode {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("status = %d, want %d (body=%s)", resp.StatusCode, tc.wantCode, body)
			}
			if env := decodeEnvelope(t, resp, nil); env.Success || env.Code != tc.want {
				t.Fatalf("envelope = %+v, want code %q", env, tc.want)
			}
		})
	}
}

// TestExecuteWorkflowByIDRefusesAnArchivedVersionThatNoLongerReproduces pins
// the fail-closed rule for a retired version whose stored payload does not
// reproduce under the current compiler (Q10). The retired payload is seeded
// with a hash the current compiler cannot produce — the shape a
// hash/compiler-semantics change leaves behind — and executing it must be 409
// workflow_version_unavailable, not a silent recompile and not a 500.
//
// The same workflow's CURRENT version keeps running: the check is scoped to
// archived payloads, so a drifted history never takes down the live workflow.
func TestExecuteWorkflowByIDRefusesAnArchivedVersionThatNoLongerReproduces(t *testing.T) {
	srv, provider := newVersionExecuteTestServer(t)

	def := listTestWorkflow("vrun-drift", "v1")
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	const driftID = "wf-drift-version"
	if _, err := provider.WorkflowRegistry().AddWorkflow(namespace.WithNamespace(context.Background(), "namespaceA"), backend.WorkflowRecord{
		ID:             driftID,
		Key:            "namespaceA/vrun-drift@v1",
		Namespace:      "namespaceA",
		Name:           "vrun-drift",
		Version:        "v1",
		DefinitionHash: "sha256:stale-drift",
		Definition:     def,
		Graph:          g,
	}); err != nil {
		t.Fatalf("seed drifted v1: %v", err)
	}

	// Replace it with an ordinary v2, retiring the drifted payload into history.
	publishRun(t, srv.URL, "tok-a", driftID, "vrun-drift", "v2")

	resp := postExecuteByID(t, srv.URL, "tok-a", driftID, executeRegisteredRequest{Version: "v1"})
	func() {
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusConflict {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("archived execute status = %d, want 409 (body=%s)", resp.StatusCode, body)
		}
		if env := decodeEnvelope(t, resp, nil); env.Success || env.Code != "workflow_version_unavailable" {
			t.Fatalf("envelope = %+v, want workflow_version_unavailable", env)
		}
	}()

	// The live version is unaffected by the drifted history.
	resp = postExecuteByID(t, srv.URL, "tok-a", driftID, executeRegisteredRequest{})
	if got := executedWorkflowVersion(t, provider, resp); got != "v2" {
		t.Fatalf("current version executed as %q, want v2 (the drift check must not leak to the live record)", got)
	}
}

// executeByIDOutcome is what a successful execute response says: which
// execution answered, whether the answer replayed an earlier keyed submission,
// and which version actually ran, read from the execution's own graph snapshot.
type executeByIDOutcome struct {
	executionID types.ExecutionID
	duplicate   bool
	version     string
}

func readExecuteByIDOutcome(t *testing.T, provider *local.Backend, resp *http.Response) executeByIDOutcome {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("execute status = %d, want 200 (body=%s)", resp.StatusCode, body)
	}
	var out executeWorkflowResponse
	if env := decodeEnvelope(t, resp, &out); !env.Success {
		t.Fatalf("execute envelope = %+v, want success", env)
	}
	if out.ExecutionID == "" {
		t.Fatal("execute returned an empty execution_id")
	}
	snap, err := provider.State().GetExecution(context.Background(), out.ExecutionID)
	if err != nil {
		t.Fatalf("GetExecution(%q): %v", out.ExecutionID, err)
	}
	if snap == nil || snap.Graph == nil {
		t.Fatalf("execution %q has no graph snapshot", out.ExecutionID)
	}
	return executeByIDOutcome{executionID: out.ExecutionID, duplicate: out.Duplicate, version: snap.Graph.WorkflowVersion()}
}

// TestExecuteWorkflowByIDVersionWithIdempotencyKeyPinsReplays pins the
// interaction of the version field with the Idempotency-Key header on this
// route (versioning design §2.7). The key's fingerprint comes from the version
// the request resolved, so a pinned retry replays the original execution even
// after an intervening publish — duplicate=true, same execution id — while an
// unpinned retry that now resolves a newer version is refused with 409
// idempotency_key_conflict. These are the same two answers the SDK's keyed
// Invoke gives (TestInvokeWorkflowVersionIdempotencyReplay); it is the same
// submission underneath.
func TestExecuteWorkflowByIDVersionWithIdempotencyKeyPinsReplays(t *testing.T) {
	srv, provider := newVersionExecuteTestServer(t)
	id := registerListWorkflow(t, srv.URL, "tok-a", "vreplay", "v1")

	// Pinned v1, keyed: the first submission runs v1 and opens the key.
	first := readExecuteByIDOutcome(t, provider, postExecuteByIDKeyed(t, srv.URL, "tok-a", id, "http-pinned", executeRegisteredRequest{Version: "v1"}))
	if first.duplicate || first.version != "v1" {
		t.Fatalf("first keyed pinned execute = %+v, want a fresh v1 execution", first)
	}

	// Publishing v2 retires v1; the pinned retry resolves the same archived
	// payload, so the fingerprint matches and the answer is the original run.
	publishRun(t, srv.URL, "tok-a", id, "vreplay", "v2")
	replay := readExecuteByIDOutcome(t, provider, postExecuteByIDKeyed(t, srv.URL, "tok-a", id, "http-pinned", executeRegisteredRequest{Version: "v1"}))
	if !replay.duplicate || replay.executionID != first.executionID || replay.version != "v1" {
		t.Fatalf("pinned keyed replay = %+v, want a duplicate of %+v", replay, first)
	}

	// Unpinned, a fresh key: the first submission runs v2; publishing v3 makes
	// the retry resolve v3, whose fingerprint differs, so it is a conflict.
	second := readExecuteByIDOutcome(t, provider, postExecuteByIDKeyed(t, srv.URL, "tok-a", id, "http-unpinned", executeRegisteredRequest{}))
	if second.duplicate || second.version != "v2" || second.executionID == first.executionID {
		t.Fatalf("first keyed unpinned execute = %+v, want a fresh v2 execution", second)
	}
	publishRun(t, srv.URL, "tok-a", id, "vreplay", "v3")

	resp := postExecuteByIDKeyed(t, srv.URL, "tok-a", id, "http-unpinned", executeRegisteredRequest{})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("unpinned keyed retry status = %d, want 409 (body=%s)", resp.StatusCode, body)
	}
	if env := decodeEnvelope(t, resp, nil); env.Success || env.Code != "idempotency_key_conflict" {
		t.Fatalf("unpinned keyed retry envelope = %+v, want idempotency_key_conflict", env)
	}
}
