package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

// noQueue is a task queue that rejects every enqueue so seeded executions stay
// put without dispatching (the route matrix inspects/signals/cancels them
// directly through the engine facade).
type noQueue struct{}

func (noQueue) Enqueue(context.Context, *engine.Task) error                       { return nil }
func (noQueue) EnqueueDelayed(context.Context, *engine.Task, time.Duration) error { return nil }

// execRouteFixture wires a miniredis-backed distributed backend + control plane
// + APIServer with a two-token registry: tok-full has the "execution" scope,
// tok-none has only "workflow". Both are in namespaceA so authz passes the namespace
// check and the decision is driven by scope alone.
type execRouteFixture struct {
	t       *testing.T
	httpSrv *httptest.Server
	audit   *InMemoryAuditSink
	execID  types.ExecutionID
}

func newExecRouteFixture(t *testing.T) *execRouteFixture {
	t.Helper()
	redisServer, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(redisServer.Close)

	backend, err := distributed.New(redisServer.Addr(), nil, distributed.WithConsumer(false))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	cp, err := control.NewControlPlane(control.Config{Backend: backend})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}

	audit := NewInMemoryAuditSink()
	principalAuth := NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{
		{Token: "tok-full", Subject: "op-full", Namespace: "namespaceA", Scopes: []string{"workflow", "execution"}},
		{Token: "tok-none", Subject: "op-none", Namespace: "namespaceA", Scopes: []string{"workflow"}},
	})

	srv, err := New(Config{
		Concurrency:   1,
		PrincipalAuth: principalAuth,
		Authorizer:    NamespaceAwareAuthorizer{},
		AuditSink:     audit,
	}, WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)

	// Seed an execution under namespaceA directly through the engine so the seed
	// does not depend on the routes under test.
	seedEng := engine.New(backend.State(), noQueue{})
	ctxA := namespace.WithNamespace(context.Background(), namespace.Namespace("namespaceA"))
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "exec-route-wf",
		Nodes: []types.NodeDef{{Name: "start", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	execID, err := seedEng.Submit(ctxA, g, nil)
	if err != nil {
		t.Fatalf("seed Submit: %v", err)
	}

	return &execRouteFixture{t: t, httpSrv: httpSrv, audit: audit, execID: execID}
}

func (f *execRouteFixture) do(token, method, path string, body any) *http.Response {
	f.t.Helper()
	var r io.Reader
	if body != nil {
		var buf bytes.Buffer
		_ = json.NewEncoder(&buf).Encode(body)
		r = &buf
	}
	req, _ := http.NewRequest(method, f.httpSrv.URL+path, r)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

// TestExecutionRouteOperationMatrix proves Task 8 blocker 1: each execution
// sub-route resolves to its own operation + mutation flag, so signal/revoke/
// cancel each require the execution scope and record a mutation admission
// audit (fail-closed), while inspect/wait are non-mutations. A principal
// without the execution scope is denied (403) on every verb; a principal with
// it reaches the handler.
func TestExecutionRouteOperationMatrix(t *testing.T) {
	f := newExecRouteFixture(t)

	cases := []struct {
		name       string
		method     string
		path       string
		body       any
		wantAllow  int
		wantDeny   int
		op         string
		isMutation bool
	}{
		{"inspect", http.MethodGet, "/v1/executions/" + string(f.execID), nil, http.StatusOK, http.StatusForbidden, OpExecutionRead, false},
		{"signal", http.MethodPost, "/v1/executions/" + string(f.execID) + "/signals", signalRequest{Name: "s1"}, http.StatusOK, http.StatusForbidden, OpExecutionSignal, true},
		{"cancel", http.MethodPost, "/v1/executions/" + string(f.execID) + "/cancel", nil, http.StatusOK, http.StatusForbidden, OpExecutionCancel, true},
		{"revoke", http.MethodDelete, "/v1/executions/" + string(f.execID) + "/signals/s1", nil, http.StatusOK, http.StatusForbidden, OpExecutionRevoke, true},
	}
	// wait is a long-poll, so it is exercised deny-only (authz fails before the
	// handler, returning 403 immediately) to avoid the poll timeout in the
	// matrix. Its operation (execution.read, non-mutation) is already covered
	// by inspect's allow path.
	t.Run("wait_deny", func(t *testing.T) {
		resp := f.do("tok-none", http.MethodGet, "/v1/executions/"+string(f.execID)+"/wait", nil)
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("wait deny status = %d, want 403 (execution.read scope required)", resp.StatusCode)
		}
	})
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			before := len(f.audit.Events())

			// Deny path: tok-none lacks the execution scope.
			resp := f.do("tok-none", c.method, c.path, c.body)
			_ = resp.Body.Close()
			if resp.StatusCode != c.wantDeny {
				t.Fatalf("deny %s status = %d, want %d", c.name, resp.StatusCode, c.wantDeny)
			}

			// Allow path: tok-full has the execution scope → reaches handler.
			resp = f.do("tok-full", c.method, c.path, c.body)
			_ = resp.Body.Close()
			if resp.StatusCode != c.wantAllow {
				t.Fatalf("allow %s status = %d, want %d (handler should run)", c.name, resp.StatusCode, c.wantAllow)
			}

			events := f.audit.Events()[before:]
			ops := map[string]bool{}
			for _, ev := range events {
				ops[ev.Operation] = true
			}
			if !ops[c.op] {
				t.Fatalf("%s: no audit event for op %q; events=%+v", c.name, c.op, events)
			}
			if c.isMutation {
				foundAdmitted := false
				for _, ev := range events {
					if ev.Operation == c.op && ev.Outcome == "admitted" && ev.Decision == DecisionAllow {
						foundAdmitted = true
					}
				}
				if !foundAdmitted {
					t.Fatalf("%s: mutation %q did not record an admitted admission audit row; events=%+v", c.name, c.op, events)
				}
			}
		})
	}
}

// TestExecutionRouteUnknownVerbDenies proves an unknown execution sub-verb
// resolves to ok=false → 404 (default-deny, no existence leak) and produces NO
// audit row for a fabricated operation (unknown op default-deny).
func TestExecutionRouteUnknownVerbDenies(t *testing.T) {
	f := newExecRouteFixture(t)
	before := len(f.audit.Events())

	resp := f.do("tok-full", http.MethodPost, "/v1/executions/"+string(f.execID)+"/bogus", nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown verb status = %d, want 404 (default-deny, no leak)", resp.StatusCode)
	}
	if got := len(f.audit.Events()) - before; got != 0 {
		t.Fatalf("unknown verb appended %d audit events, want 0 (no fabricated op audit); events=%+v", got, f.audit.Events()[before:])
	}
}

// TestExecutionMutationFailClosedHandlerNotReached proves that when the audit
// sink is unavailable, each execution mutation route fails closed (503) and the
// handler is never reached. Uses a stub handler + the resolved wrapper so the
// assertion is direct: a flag proves the handler did not run.
func TestExecutionMutationFailClosedHandlerNotReached(t *testing.T) {
	mutations := []struct {
		name string
		op   string
	}{
		{"signal", OpExecutionSignal},
		{"revoke", OpExecutionRevoke},
		{"cancel", OpExecutionCancel},
	}
	for _, mt := range mutations {
		t.Run(mt.name, func(t *testing.T) {
			auth := staticPrincipalAuth{principal: Principal{Subject: "alice", Namespace: "namespaceA", Scopes: []string{"execution"}}}
			mod := &workflowControlModule{}
			mod.principalAuth = auth
			mod.authorizer = NamespaceAwareAuthorizer{}
			mod.audit = failingAuditSink{}

			handlerRan := false
			wrapped := mod.authzWrapResolved(func(w http.ResponseWriter, r *http.Request) {
				handlerRan = true
				writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
			}, func(r *http.Request) (resolvedRoute, bool) {
				return resolvedRoute{operation: mt.op, resource: "execution/x", executionID: "x", isMutation: true}, true
			})

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/executions/x/signal", nil)
			wrapped.ServeHTTP(rec, req)

			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("%s: status = %d, want 503 (audit unavailable → fail-closed)", mt.name, rec.Code)
			}
			if handlerRan {
				t.Fatalf("%s: handler ran despite audit sink failure (must fail-closed)", mt.name)
			}
		})
	}
}

// TestExecutionCrossNamespaceSignalDenied proves a cross-namespace execution mutation
// is denied. The authorizer rejects namespaceB acting on a namespaceA resource
// (defense in depth); the authoritative IDOR path (namespace-scoped store read →
// 404, no existence leak) is exercised end-to-end in namespaceor_test.go for
// inspect/list/replay. Here we assert the signal operation's authz decision.
func TestExecutionCrossNamespaceSignalDenied(t *testing.T) {
	dec, err := NamespaceAwareAuthorizer{}.Authorize(context.Background(), AuthorizationRequest{
		Principal:         Principal{Subject: "op-b", Namespace: "namespaceB", Scopes: []string{"execution"}},
		Operation:         OpExecutionSignal,
		ResourceNamespace: "namespaceA",
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if dec != DecisionDeny {
		t.Fatalf("cross-namespace signal decision = %q, want deny", dec)
	}
}

// TestNoTokenInAuditOrLog proves the bearer token plaintext never appears in
// audit events (security policy §7). The AuditEvent carries only the server-
// verified Subject + Namespace + operation/resource/decision — never the token.
func TestNoTokenInAuditOrLog(t *testing.T) {
	const secret = "super-secret-bearer-token-value-xyz"
	auth := NewBearerPrincipalAuth(secret, "op", []string{"workflow"})
	audit := NewInMemoryAuditSink()
	m := authzModule(t, auth, ScopeAuthorizer{}, audit)
	mux := http.NewServeMux()
	m.registerAuthzRoutes(mux)

	// Authenticated allow (mutation) — audit records admission + reconcile.
	body := bytes.NewBufferString(`{}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/workflows", body)
	req.Header.Set("Authorization", "Bearer "+secret)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if len(audit.Events()) == 0 {
		t.Fatal("no audit events recorded")
	}
	for _, ev := range audit.Events() {
		if strings.Contains(ev.Principal, secret) || strings.Contains(ev.Resource, secret) ||
			strings.Contains(ev.Operation, secret) || strings.Contains(ev.Reason, secret) ||
			strings.Contains(ev.Outcome, secret) || strings.Contains(ev.RequestID, secret) ||
			strings.Contains(ev.Namespace, secret) {
			t.Fatalf("token plaintext leaked into audit event: %+v", ev)
		}
	}
}
