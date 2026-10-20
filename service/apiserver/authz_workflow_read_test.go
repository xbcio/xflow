package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

// This file covers B5: the workflow scope is split into a read-only half
// (workflow.read) and the pre-existing write half (workflow). The properties
// pinned here are:
//
//   - a read-only credential reaches every definition read the editor needs
//     (current-user, workflow list / by-id, node-types, version history) and is
//     refused every mutation (register, replace/publish, execute, rollback,
//     delete);
//   - the write scope implies the read scope, so a token issued before the split
//     (which carries only "workflow") keeps reading — this is also what the
//     console's view gate assumes (admin access.ts accepts either scope);
//   - an unrelated scope grants neither half;
//   - GET /v1/current-user needs authentication only, so every credentialed
//     population can read the scopes its capabilities are derived from.

// workflowSplitPrincipals is the token registry for the split matrix: tok-rw is
// a pre-split credential (write only; read implied), tok-ro holds exactly the
// read half, and tok-none holds neither.
func workflowSplitPrincipals() []TokenPrincipalMapping {
	return []TokenPrincipalMapping{
		{Token: "tok-rw", Subject: "op-rw", Namespace: "namespaceA", Scopes: []string{ScopeWorkflow}},
		{Token: "tok-ro", Subject: "op-ro", Namespace: "namespaceA", Scopes: []string{ScopeWorkflowRead}},
		{Token: "tok-none", Subject: "op-none", Namespace: "namespaceA", Scopes: []string{"execution"}},
	}
}

// doSplitRequest issues one request against base with the given bearer token.
func doSplitRequest(t *testing.T, base, token, method, path string, body any) *http.Response {
	t.Helper()
	var r io.Reader
	if body != nil {
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
		r = &buf
	}
	req, err := http.NewRequest(method, base+path, r)
	if err != nil {
		t.Fatalf("new request %s %s: %v", method, path, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

// TestWorkflowReadScopeRouteMatrix is the B5 acceptance table: a principal (and
// a token) holding workflow.read alone reads every editor read path and is
// refused every workflow mutation. Refusals must be 403 from the authz wrapper —
// never a handler outcome — so a 400/404/409 here would mean the request got
// past authorization.
func TestWorkflowReadScopeRouteMatrix(t *testing.T) {
	srv, _ := newWorkflowReplaceDependencyTestServer(t, nil, nil, workflowSplitPrincipals())
	id := registerListWorkflow(t, srv.URL, "tok-rw", "split-matrix", "v1")
	workflowPath := PathWorkflows + "/" + id

	cases := []struct {
		name   string
		method string
		path   string
		body   any
		want   int
	}{
		{"current user", http.MethodGet, PathCurrentUser, nil, http.StatusOK},
		{"list workflows", http.MethodGet, PathWorkflows, nil, http.StatusOK},
		{"read workflow", http.MethodGet, workflowPath, nil, http.StatusOK},
		{"list node types", http.MethodGet, PathNodeTypes, nil, http.StatusOK},
		{"list versions", http.MethodGet, workflowPath + "/versions", nil, http.StatusOK},
		{"read version", http.MethodGet, workflowPath + "/versions/v1", nil, http.StatusOK},
		// Mutations: all denied, including "publish", which is a replace with a
		// new version label.
		{"register workflow", http.MethodPost, PathWorkflows, listTestWorkflow("split-other", "v1"), http.StatusForbidden},
		{"replace/publish workflow", http.MethodPut, workflowPath, listTestWorkflow("split-matrix", "v2"), http.StatusForbidden},
		{"execute inline", http.MethodPost, PathWorkflowExecute, map[string]any{}, http.StatusForbidden},
		{"execute by id", http.MethodPost, workflowPath + "/execute", map[string]any{}, http.StatusForbidden},
		{"rollback", http.MethodPost, workflowPath + "/rollback", rollbackWorkflowRequest{Version: "v1"}, http.StatusForbidden},
		{"delete workflow", http.MethodDelete, workflowPath, nil, http.StatusForbidden},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := doSplitRequest(t, srv.URL, "tok-ro", tc.method, tc.path, tc.body)
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != tc.want {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("%s %s with workflow.read = %d, want %d (body=%s)",
					tc.method, tc.path, resp.StatusCode, tc.want, body)
			}
		})
	}

	// The read-only credential can read its own scopes — the P3a capabilities
	// projection depends on it, and before B5 this exact request was a 403
	// because current-user demanded the write scope.
	resp := doSplitRequest(t, srv.URL, "tok-ro", http.MethodGet, PathCurrentUser, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s with workflow.read = %d, want 200", PathCurrentUser, resp.StatusCode)
	}
	var envelope struct {
		Data map[string]any `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode current-user body: %v", err)
	}
	scopes, ok := envelope.Data["scopes"].([]any)
	if !ok || len(scopes) != 1 || scopes[0] != ScopeWorkflowRead {
		t.Errorf("current-user scopes = %#v, want [%s]", envelope.Data["scopes"], ScopeWorkflowRead)
	}
}

// TestWorkflowWriteScopeImpliesRead pins the compatibility half of the split: a
// token carrying only the write scope (every token issued before B5) still
// reads definitions, because the console's view gate accepts either scope and a
// write grant has always entailed a read.
func TestWorkflowWriteScopeImpliesRead(t *testing.T) {
	srv, _ := newWorkflowReplaceDependencyTestServer(t, nil, nil, workflowSplitPrincipals())
	id := registerListWorkflow(t, srv.URL, "tok-rw", "split-implied", "v1")

	for _, path := range []string{
		PathWorkflows,
		PathWorkflows + "/" + id,
		PathNodeTypes,
		PathWorkflows + "/" + id + "/versions",
		PathWorkflows + "/" + id + "/versions/v1",
	} {
		resp := doSplitRequest(t, srv.URL, "tok-rw", http.MethodGet, path, nil)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s with workflow (write) = %d, want 200", path, resp.StatusCode)
		}
	}
}

// TestWorkflowReadDeniedWithoutEitherHalf pins that the read half is not
// implied by unrelated scopes: an execution-only principal is refused every
// workflow read (and current-user stays reachable, by design).
func TestWorkflowReadDeniedWithoutEitherHalf(t *testing.T) {
	srv, _ := newWorkflowReplaceDependencyTestServer(t, nil, nil, workflowSplitPrincipals())
	id := registerListWorkflow(t, srv.URL, "tok-rw", "split-denied", "v1")

	for _, path := range []string{
		PathWorkflows,
		PathWorkflows + "/" + id,
		PathNodeTypes,
		PathWorkflows + "/" + id + "/versions",
		PathWorkflows + "/" + id + "/versions/v1",
	} {
		resp := doSplitRequest(t, srv.URL, "tok-none", http.MethodGet, path, nil)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("GET %s with execution only = %d, want 403", path, resp.StatusCode)
		}
	}

	resp := doSplitRequest(t, srv.URL, "tok-none", http.MethodGet, PathCurrentUser, nil)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET %s with execution only = %d, want 200 (authentication-only route)", PathCurrentUser, resp.StatusCode)
	}
}

// TestScopeAuthorizerWorkflowReadSplit is the table-driven authorizer contract
// behind the route matrix, run against both shipped authorizers so the two
// cannot drift on the split.
func TestScopeAuthorizerWorkflowReadSplit(t *testing.T) {
	cases := []struct {
		name   string
		scopes []string
		op     string
		want   Decision
	}{
		{"write implies read", []string{ScopeWorkflow}, OpWorkflowRead, DecisionAllow},
		{"read-only reads", []string{ScopeWorkflowRead}, OpWorkflowRead, DecisionAllow},
		{"read-only cannot create", []string{ScopeWorkflowRead}, OpWorkflowCreate, DecisionDeny},
		{"read-only cannot invoke", []string{ScopeWorkflowRead}, OpWorkflowInvoke, DecisionDeny},
		{"read-only cannot register", []string{ScopeWorkflowRead}, OpWorkflowRegister, DecisionDeny},
		{"read-only cannot update", []string{ScopeWorkflowRead}, OpWorkflowDefinitionUpdate, DecisionDeny},
		{"unrelated scope cannot read", []string{"execution"}, OpWorkflowRead, DecisionDeny},
		{"no scopes cannot read", nil, OpWorkflowRead, DecisionDeny},
		{"current-user needs no scope", nil, OpCurrentUserRead, DecisionAllow},
	}

	authorizers := map[string]Authorizer{
		"ScopeAuthorizer":          ScopeAuthorizer{},
		"NamespaceAwareAuthorizer": NamespaceAwareAuthorizer{},
	}
	for name, authorizer := range authorizers {
		for _, tc := range cases {
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				dec, err := authorizer.Authorize(context.Background(), AuthorizationRequest{
					Principal: Principal{Subject: "alice", Namespace: "namespaceA", Scopes: tc.scopes},
					Operation: tc.op,
				})
				if err != nil {
					t.Fatalf("Authorize(%s): %v", tc.op, err)
				}
				if dec != tc.want {
					t.Fatalf("Authorize(%s, scopes=%v) = %q, want %q", tc.op, tc.scopes, dec, tc.want)
				}
			})
		}
	}
}
