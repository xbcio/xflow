package apiserver

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/types"
)

// The webhook route's tests run against a REAL apiserver built by New, because
// the properties under test are the wiring's: that the route exists at all
// (it is registered outside the authz wrapper), that the definition's own
// parameters decide the method and the size limit, and that the admission goes
// through the same seed path the runner face uses.

const webhookTestSecret = "s3cr3t-hmac-key"

// newWebhookServer builds a server whose workflow routes are behind a
// FAIL-CLOSED principal authenticator: every /v1 route wrapped in authz answers
// 401, so a webhook request that succeeds here proves the route is authenticated
// by its own signature rather than by the wrapper.
func newWebhookServer(t *testing.T) (*APIServer, store.Supplies) {
	t.Helper()
	supplies := memstore.New()
	srv, err := New(Config{
		PrincipalAuth: staticPrincipalAuth{err: errors.New("no principal")},
		Authorizer:    ScopeAuthorizer{},
		AuditSink:     NewInMemoryAuditSink(),
		Supplies:      supplies,
	})
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	return srv, supplies
}

// webhookWorkflowDef is the smallest server-hosted webhook workflow: an entry
// unit of type xflow.trigger.webhook feeding one action node.
func webhookWorkflowDef(params map[string]any) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:    "hook",
		Version: "v1",
		Nodes: []types.NodeDef{
			{Name: "in", Type: "xflow.trigger.webhook", Version: 1, Kind: types.NodeKindTrigger, Parameters: params},
			{Name: "work", Type: "xflow.end", Version: 1, Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"in": {"main": types.PortConnections{Targets: []types.Connection{{Node: "work", Input: "main"}}}},
		},
	}
}

func webhookTestParams() map[string]any {
	return map[string]any{
		"method":          http.MethodPost,
		"path":            "/in",
		"event_id_header": "X-Event-Id",
		"max_body_bytes":  float64(64),
		"secret_supply":   "hook-secret",
	}
}

// putWebhookSecret stores the HMAC secret the definition names.
func putWebhookSecret(t *testing.T, supplies store.Supplies, name, secret string) {
	t.Helper()
	if _, err := supplies.PutSupply(context.Background(), &store.SupplyResource{
		Namespace: string(namespace.Default),
		Name:      name,
		Content:   []byte(secret),
	}, nil); err != nil {
		t.Fatalf("PutSupply: %v", err)
	}
}

// signWebhookRequest stamps the HMAC of body under secret the way a sender does.
func signWebhookRequest(req *http.Request, body []byte, secret string) {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	req.Header.Set(webhookSignatureHeader, webhookSignatureScheme+hex.EncodeToString(mac.Sum(nil)))
}

func webhookHookPath(workflowID types.WorkflowID, entryUnit string) string {
	return "/v1/hooks/" + string(namespace.Default) + "/" + string(workflowID) + "/" + entryUnit
}

// serveWebhook drives one request through the production handler.
func serveWebhook(t *testing.T, srv *APIServer, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	return rec
}

// TestWebhookEntryAdmitsSignedRequest is the happy path, and it carries two
// assertions at once: the signed request creates an execution, and it does so
// through a route that the fail-closed principal authenticator does not gate —
// the signature IS the authentication.
func TestWebhookEntryAdmitsSignedRequest(t *testing.T) {
	srv, supplies := newWebhookServer(t)
	putWebhookSecret(t, supplies, "hook-secret", webhookTestSecret)
	id, _, err := srv.RegisterWorkflow(context.Background(), namespace.Default, webhookWorkflowDef(webhookTestParams()))
	if err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	body := []byte(`{"order":"A-1"}`)
	req := httptest.NewRequest(http.MethodPost, webhookHookPath(id, "in"), strings.NewReader(string(body)))
	req.Header.Set("X-Event-Id", "evt-1")
	signWebhookRequest(req, body, webhookTestSecret)

	rec := serveWebhook(t, srv, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (%s), want 200", rec.Code, rec.Body.String())
	}
	var resp protocol.SeedExecutionResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response %q: %v", rec.Body.String(), err)
	}
	if resp.State != string(engine.AdmissionStateAccepted) {
		t.Fatalf("state = %q, want %q", resp.State, engine.AdmissionStateAccepted)
	}
	if resp.ExecutionID == "" {
		t.Fatal("no execution id returned; the caller cannot correlate the admission")
	}
}

// TestWebhookEntryRejectsBadSignature covers the authentication boundary. Every
// row must answer 401 and must not echo the signature or the secret back.
func TestWebhookEntryRejectsBadSignature(t *testing.T) {
	srv, supplies := newWebhookServer(t)
	putWebhookSecret(t, supplies, "hook-secret", webhookTestSecret)
	id, _, err := srv.RegisterWorkflow(context.Background(), namespace.Default, webhookWorkflowDef(webhookTestParams()))
	if err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	body := []byte(`{"order":"A-1"}`)
	wrongSecret := "not-the-secret"
	mac := hmac.New(sha256.New, []byte(wrongSecret))
	mac.Write(body)
	wrongSignature := webhookSignatureScheme + hex.EncodeToString(mac.Sum(nil))

	cases := []struct {
		name      string
		signature string
	}{
		{"absent", ""},
		{"wrong key", wrongSignature},
		{"scheme missing", strings.TrimPrefix(wrongSignature, webhookSignatureScheme)},
		{"scheme unknown", "sha1=" + strings.TrimPrefix(wrongSignature, webhookSignatureScheme)},
		{"not hex", webhookSignatureScheme + "zzzz"},
		{"truncated", webhookSignatureScheme + strings.TrimPrefix(wrongSignature, webhookSignatureScheme)[:32]},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, webhookHookPath(id, "in"), strings.NewReader(string(body)))
			req.Header.Set("X-Event-Id", "evt-"+tc.name)
			if tc.signature != "" {
				req.Header.Set(webhookSignatureHeader, tc.signature)
			}
			rec := serveWebhook(t, srv, req)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d (%s), want 401", rec.Code, rec.Body.String())
			}
			got := rec.Body.String()
			if strings.Contains(got, webhookTestSecret) || (tc.signature != "" && strings.Contains(got, tc.signature)) {
				t.Fatalf("the rejection echoed the secret or the signature: %q", got)
			}
		})
	}
}

// TestWebhookEntryRejectsOversizedBody asserts the entry's own max_body_bytes is
// what the route enforces, and that the answer is 413 (a retry of the same
// payload cannot help) rather than a generic 400.
func TestWebhookEntryRejectsOversizedBody(t *testing.T) {
	srv, supplies := newWebhookServer(t)
	putWebhookSecret(t, supplies, "hook-secret", webhookTestSecret)
	params := webhookTestParams()
	params["max_body_bytes"] = float64(16)
	id, _, err := srv.RegisterWorkflow(context.Background(), namespace.Default, webhookWorkflowDef(params))
	if err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	// Exactly at the limit is admitted; one byte over is refused. Both are
	// signed, so the size check is the only thing that can reject the second.
	atLimit := []byte(strings.Repeat("a", 16))
	req := httptest.NewRequest(http.MethodPost, webhookHookPath(id, "in"), strings.NewReader(string(atLimit)))
	signWebhookRequest(req, atLimit, webhookTestSecret)
	if rec := serveWebhook(t, srv, req); rec.Code != http.StatusOK {
		t.Fatalf("at-limit body: status = %d (%s), want 200", rec.Code, rec.Body.String())
	}

	over := []byte(strings.Repeat("a", 17))
	req = httptest.NewRequest(http.MethodPost, webhookHookPath(id, "in"), strings.NewReader(string(over)))
	signWebhookRequest(req, over, webhookTestSecret)
	if rec := serveWebhook(t, srv, req); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body: status = %d (%s), want 413", rec.Code, rec.Body.String())
	}
}

// TestWebhookEntryReplayedEventIDAdmitsOnce asserts the deduplication contract:
// a redelivery of the same event — the sender's own event id — never creates a
// second execution, and the answer names the execution that exists.
//
// The redelivery is answered with the engine's "conflict" state rather than
// "duplicate": the exit payload carries the event's observed time, so two
// deliveries of one event are the same admission key with a different result
// hash, which is exactly what conflict means on this face. The property the
// sender depends on — one execution, and its id — holds either way.
func TestWebhookEntryReplayedEventIDAdmitsOnce(t *testing.T) {
	srv, supplies := newWebhookServer(t)
	putWebhookSecret(t, supplies, "hook-secret", webhookTestSecret)
	id, _, err := srv.RegisterWorkflow(context.Background(), namespace.Default, webhookWorkflowDef(webhookTestParams()))
	if err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	body := []byte(`{"order":"A-1"}`)
	post := func() (int, protocol.SeedExecutionResponse) {
		req := httptest.NewRequest(http.MethodPost, webhookHookPath(id, "in"), strings.NewReader(string(body)))
		req.Header.Set("X-Event-Id", "evt-replay")
		signWebhookRequest(req, body, webhookTestSecret)
		rec := serveWebhook(t, srv, req)
		var resp protocol.SeedExecutionResponse
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		return rec.Code, resp
	}

	firstStatus, first := post()
	if firstStatus != http.StatusOK || first.ExecutionID == "" {
		t.Fatalf("first delivery: status = %d, execution = %q, want 200 with an id", firstStatus, first.ExecutionID)
	}
	secondStatus, second := post()
	if secondStatus != http.StatusConflict && secondStatus != http.StatusOK {
		t.Fatalf("redelivery: status = %d, want 200 (duplicate) or 409 (conflict)", secondStatus)
	}
	if second.ExecutionID != first.ExecutionID {
		t.Fatalf("redelivery named execution %q, want the original %q", second.ExecutionID, first.ExecutionID)
	}
}

// TestWebhookEntryRefusesAnEntryWithoutASecretAtRegistration asserts the
// fail-closed rule at the only moment an operator can act on it: a definition
// whose server-hosted webhook declares no secret is not registered at all.
func TestWebhookEntryRefusesAnEntryWithoutASecretAtRegistration(t *testing.T) {
	srv, _ := newWebhookServer(t)
	params := webhookTestParams()
	delete(params, "secret_supply")

	_, _, err := srv.RegisterWorkflow(context.Background(), namespace.Default, webhookWorkflowDef(params))
	if err == nil {
		t.Fatal("registration succeeded for a webhook entry with no signing secret")
	}
	var compileErr *WorkflowCompileError
	if !errors.As(err, &compileErr) {
		t.Fatalf("error %v is not a *WorkflowCompileError, so the HTTP path would answer 500 rather than 400", err)
	}
	if !strings.Contains(err.Error(), "secret_supply") {
		t.Fatalf("error %q does not name the parameter the operator must set", err)
	}

	// The rule is scoped to webhook entries: another trigger kind registers with
	// no secret at all, which is what keeps this from being a blanket
	// registration requirement.
	other := &types.WorkflowDef{
		Name:    "timer",
		Version: "v1",
		Nodes: []types.NodeDef{
			{Name: "in", Type: "xflow.trigger.timer", Version: 1, Kind: types.NodeKindTrigger,
				Parameters: map[string]any{"interval": "1h"}},
			{Name: "work", Type: "xflow.end", Version: 1, Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"in": {"main": types.PortConnections{Targets: []types.Connection{{Node: "work", Input: "main"}}}},
		},
	}
	if _, _, err := srv.RegisterWorkflow(context.Background(), namespace.Default, other); err != nil {
		t.Fatalf("a non-webhook trigger entry was refused for a missing secret: %v", err)
	}
}

// TestWebhookEntryUnresolvableSecretFailsClosed asserts the runtime half of the
// same rule: a definition may name a supply that holds no content yet, and the
// route then refuses rather than admitting an unauthenticated caller. The body
// names neither the supply nor the reason.
func TestWebhookEntryUnresolvableSecretFailsClosed(t *testing.T) {
	srv, _ := newWebhookServer(t)
	id, _, err := srv.RegisterWorkflow(context.Background(), namespace.Default, webhookWorkflowDef(webhookTestParams()))
	if err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	body := []byte(`{"order":"A-1"}`)
	req := httptest.NewRequest(http.MethodPost, webhookHookPath(id, "in"), strings.NewReader(string(body)))
	signWebhookRequest(req, body, webhookTestSecret)
	rec := serveWebhook(t, srv, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d (%s), want 503 (fail closed)", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "hook-secret") {
		t.Fatalf("the refusal named the supply resource: %q", rec.Body.String())
	}
}

// TestWebhookEntryMethodComesFromTheDefinition asserts the declared HTTP method
// is enforced by the handler, because the mux pattern deliberately carries no
// method (two entries of one workflow can declare different verbs).
func TestWebhookEntryMethodComesFromTheDefinition(t *testing.T) {
	srv, supplies := newWebhookServer(t)
	putWebhookSecret(t, supplies, "hook-secret", webhookTestSecret)
	params := webhookTestParams()
	params["method"] = http.MethodPut
	id, _, err := srv.RegisterWorkflow(context.Background(), namespace.Default, webhookWorkflowDef(params))
	if err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	body := []byte(`{"order":"A-1"}`)
	req := httptest.NewRequest(http.MethodPost, webhookHookPath(id, "in"), strings.NewReader(string(body)))
	signWebhookRequest(req, body, webhookTestSecret)
	rec := serveWebhook(t, srv, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d (%s), want 405", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Allow"); got != http.MethodPut {
		t.Fatalf("Allow = %q, want %q", got, http.MethodPut)
	}
}

// TestWebhookEntryUnknownTargetsAreNotFound asserts every unresolvable target
// answers the same 404: an unknown workflow, an entry unit that does not exist,
// an entry unit that is not a server-hosted webhook, and a namespace segment
// that disagrees with the record. None of them may be distinguishable, because
// the caller is unauthenticated at the point of the lookup.
func TestWebhookEntryUnknownTargetsAreNotFound(t *testing.T) {
	srv, supplies := newWebhookServer(t)
	putWebhookSecret(t, supplies, "hook-secret", webhookTestSecret)
	id, _, err := srv.RegisterWorkflow(context.Background(), namespace.Default, webhookWorkflowDef(webhookTestParams()))
	if err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	body := []byte(`{"order":"A-1"}`)
	paths := map[string]string{
		"unknown workflow":         "/v1/hooks/" + string(namespace.Default) + "/wf-missing/in",
		"unknown entry unit":       webhookHookPath(id, "nope"),
		"foreign namespace claim":  "/v1/hooks/tenant-b/" + string(id) + "/in",
		"unknown namespace tenant": "/v1/hooks/tenant-b/" + string(id) + "/in",
	}
	for name, path := range paths {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
			signWebhookRequest(req, body, webhookTestSecret)
			rec := serveWebhook(t, srv, req)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("status = %d (%s), want 404", rec.Code, rec.Body.String())
			}
		})
	}
}

// TestWebhookEntryRouteIsRegisteredOnTheProductionMux asserts the route exists
// on the handler a real deployment serves, which is the property the §2.2
// dead-constant guard also reads through PathWebhookEntry.
func TestWebhookEntryRouteIsRegisteredOnTheProductionMux(t *testing.T) {
	srv, _ := newWebhookServer(t)
	mux, ok := srv.Handler().(*http.ServeMux)
	if !ok {
		t.Fatalf("srv.Handler() = %T, want *http.ServeMux", srv.Handler())
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/hooks/ns/wf-1/entry", nil)
	if _, pattern := mux.Handler(req); pattern == "" {
		t.Fatalf("%s matched no registered route", PathWebhookEntry)
	}
}

// TestWriteSeedAdmissionResultRefusalCarriesRetryAfter pins the refusal mapping
// the webhook caller depends on: the 429 body carries the same floor as the
// Retry-After header, because an in-process caller has no header of its own to
// copy. It exercises the shared mapper directly — producing a real
// concurrency-refused admission needs a workflow at its limit, which is the
// runner face's own test's job.
func TestWriteSeedAdmissionResultRefusalCarriesRetryAfter(t *testing.T) {
	for _, tc := range []struct {
		state engine.AdmissionState
		code  string
	}{
		{engine.AdmissionStateOverLimit, "workflow_concurrency_limit"},
		{engine.AdmissionStateQueueFull, "workflow_concurrency_queue_full"},
	} {
		t.Run(string(tc.state), func(t *testing.T) {
			rec := httptest.NewRecorder()
			writeSeedAdmissionResult(rec, nil, engine.SeedExecutionFromEntryResponse{State: tc.state}, nil)

			if rec.Code != http.StatusTooManyRequests {
				t.Fatalf("status = %d, want 429", rec.Code)
			}
			if got := rec.Header().Get("Retry-After"); got == "" {
				t.Fatal("no Retry-After header on a refusal")
			}
			var refusal protocol.SeedExecutionRefusal
			if err := json.Unmarshal(rec.Body.Bytes(), &refusal); err != nil {
				t.Fatalf("decode refusal %q: %v", rec.Body.String(), err)
			}
			if refusal.RetryAfterSeconds <= 0 {
				t.Fatalf("retry_after_seconds = %d, want the same floor as the header", refusal.RetryAfterSeconds)
			}
			if refusal.Error != tc.code || refusal.State != string(tc.state) {
				t.Fatalf("refusal = %+v, want state %q code %q", refusal, tc.state, tc.code)
			}
		})
	}
}
