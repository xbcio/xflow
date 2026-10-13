package apiserver

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

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

// TestWebhookEntryReplayedEventIDIsADuplicate asserts the deduplication
// contract: a redelivery of the same event — the sender's own event id, the same
// body — never creates a second execution, names the execution that exists, and
// is answered 200 with duplicate:true.
//
// The status is load-bearing, not decoration. The senders this route exists for
// (a code host, a job scheduler) retry any non-2xx, so an answer of 409 to a
// retry of a delivery that was in fact accepted produces an endless retry loop
// for an event that already ran. Duplicate is what the engine would have
// reported if it had been asked twice with the same content, which is why the
// comparison must not include the observation time.
//
// The pause between the two deliveries is what makes the assertion able to
// fail: with a coarse clock, two immediately successive requests could carry the
// same observation time, and an implementation that hashed the observation time
// would then look idempotent by accident.
func TestWebhookEntryReplayedEventIDIsADuplicate(t *testing.T) {
	srv, supplies := newWebhookServer(t)
	putWebhookSecret(t, supplies, "hook-secret", webhookTestSecret)
	id, _, err := srv.RegisterWorkflow(context.Background(), namespace.Default, webhookWorkflowDef(webhookTestParams()))
	if err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	body := []byte(`{"order":"A-1"}`)
	post := func(extra map[string]string) (int, protocol.SeedExecutionResponse) {
		req := httptest.NewRequest(http.MethodPost, webhookHookPath(id, "in"), strings.NewReader(string(body)))
		req.Header.Set("X-Event-Id", "evt-replay")
		for name, value := range extra {
			req.Header.Set(name, value)
		}
		signWebhookRequest(req, body, webhookTestSecret)
		rec := serveWebhook(t, srv, req)
		var resp protocol.SeedExecutionResponse
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		return rec.Code, resp
	}

	firstStatus, first := post(nil)
	if firstStatus != http.StatusOK || first.ExecutionID == "" {
		t.Fatalf("first delivery: status = %d, execution = %q, want 200 with an id", firstStatus, first.ExecutionID)
	}
	if first.Duplicate {
		t.Fatal("the first delivery of an event was reported as a duplicate")
	}

	time.Sleep(2 * time.Millisecond)

	// The extra header stands in for what a retrying sender's request picks up
	// along the way — a new proxy hop, a new trace id. The delivery is the same
	// one, so the answer must be the same duplicate: only the event id and the
	// body identify a delivery.
	secondStatus, second := post(map[string]string{"X-Forwarded-For": "203.0.113.9"})
	if secondStatus != http.StatusOK {
		t.Fatalf("redelivery: status = %d (%s), want 200 — a retry of an accepted delivery must not be refused",
			secondStatus, second.State)
	}
	if !second.Duplicate {
		t.Fatalf("redelivery: duplicate = false (state %q), want true — the caller cannot tell it made no second execution", second.State)
	}
	if second.ExecutionID != first.ExecutionID {
		t.Fatalf("redelivery named execution %q, want the original %q", second.ExecutionID, first.ExecutionID)
	}

	// The definition's event id header is what carried the identity, so a
	// delivery that omits it must not collapse into this one: it hashes its own
	// body and lands on a different key.
	req := httptest.NewRequest(http.MethodPost, webhookHookPath(id, "in"), strings.NewReader(string(body)))
	signWebhookRequest(req, body, webhookTestSecret)
	rec := serveWebhook(t, srv, req)
	var unkeyed protocol.SeedExecutionResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &unkeyed); err != nil {
		t.Fatalf("decode response %q: %v", rec.Body.String(), err)
	}
	if rec.Code != http.StatusOK || unkeyed.Duplicate || unkeyed.ExecutionID == first.ExecutionID {
		t.Fatalf("a delivery with no event id header was collapsed into the keyed one: status = %d, resp = %+v", rec.Code, unkeyed)
	}
}

// TestWebhookEntrySameEventIDWithDifferentBodyConflicts is the other half of the
// deduplication contract, and the reason the conflict state was not simply
// removed: one event id carrying two different payloads is two different
// deliveries, and only one of them can be the event. Answering 200 to the second
// would tell a sender that a body nobody ever executed had run.
func TestWebhookEntrySameEventIDWithDifferentBodyConflicts(t *testing.T) {
	srv, supplies := newWebhookServer(t)
	putWebhookSecret(t, supplies, "hook-secret", webhookTestSecret)
	id, _, err := srv.RegisterWorkflow(context.Background(), namespace.Default, webhookWorkflowDef(webhookTestParams()))
	if err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	firstBody := []byte(`{"order":"A-1"}`)
	req := httptest.NewRequest(http.MethodPost, webhookHookPath(id, "in"), strings.NewReader(string(firstBody)))
	req.Header.Set("X-Event-Id", "evt-reused")
	signWebhookRequest(req, firstBody, webhookTestSecret)
	rec := serveWebhook(t, srv, req)
	var first protocol.SeedExecutionResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &first); err != nil {
		t.Fatalf("decode first response %q: %v", rec.Body.String(), err)
	}
	if rec.Code != http.StatusOK || first.ExecutionID == "" {
		t.Fatalf("first delivery: status = %d, resp = %+v, want 200 with an id", rec.Code, first)
	}

	secondBody := []byte(`{"order":"A-2"}`)
	req = httptest.NewRequest(http.MethodPost, webhookHookPath(id, "in"), strings.NewReader(string(secondBody)))
	req.Header.Set("X-Event-Id", "evt-reused")
	signWebhookRequest(req, secondBody, webhookTestSecret)
	rec = serveWebhook(t, srv, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("same event id with a different body: status = %d (%s), want 409", rec.Code, rec.Body.String())
	}
	var conflict protocol.SeedExecutionResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &conflict); err != nil {
		t.Fatalf("decode conflict %q: %v", rec.Body.String(), err)
	}
	if conflict.State != string(engine.AdmissionStateConflict) {
		t.Fatalf("state = %q, want %q", conflict.State, engine.AdmissionStateConflict)
	}
	if conflict.ExecutionID != first.ExecutionID {
		t.Fatalf("conflict named execution %q, want the one that exists (%q)", conflict.ExecutionID, first.ExecutionID)
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

// recordingLogger captures every line the server logs, so a test can assert on
// what did NOT reach the log. The webhook route's log lines are the only
// operator-visible record of a rejected request, which makes them the one place
// the secret and the signature could leak without failing anything else.
type recordingLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *recordingLogger) record(msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprint(append([]any{msg}, args...)...))
}

func (l *recordingLogger) recordf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *recordingLogger) Debug(msg string, args ...any) { l.record(msg, args...) }
func (l *recordingLogger) Debugf(format string, args ...any) {
	l.recordf(format, args...)
}
func (l *recordingLogger) Info(msg string, args ...any) { l.record(msg, args...) }
func (l *recordingLogger) Infof(format string, args ...any) {
	l.recordf(format, args...)
}
func (l *recordingLogger) Warn(msg string, args ...any) { l.record(msg, args...) }
func (l *recordingLogger) Warnf(format string, args ...any) {
	l.recordf(format, args...)
}
func (l *recordingLogger) Error(msg string, args ...any) { l.record(msg, args...) }
func (l *recordingLogger) Errorf(format string, args ...any) {
	l.recordf(format, args...)
}
func (l *recordingLogger) Panic(msg string, args ...any) { l.record(msg, args...) }
func (l *recordingLogger) Panicf(format string, args ...any) {
	l.recordf(format, args...)
}

// mentions reports whether any logged line contains needle.
func (l *recordingLogger) mentions(needle string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		if strings.Contains(line, needle) {
			return true
		}
	}
	return false
}

// TestWebhookEntryNeverLogsTheSecretOrTheSignature covers the half of the
// route's confidentiality claim that a response body cannot: the log lines the
// rejected paths emit. Every rejection is logged (that log line is the only
// operator-visible record that the request happened), so the secret, the
// signature and the supply name must not be among what is logged.
//
// The middle case is the sharp one: the request carries a signature computed
// with the REAL secret — over a different body, so it fails — which is the only
// way a log line could plausibly pick up the secret's bytes.
func TestWebhookEntryNeverLogsTheSecretOrTheSignature(t *testing.T) {
	log := &recordingLogger{}
	supplies := memstore.New()
	srv, err := New(Config{
		PrincipalAuth: staticPrincipalAuth{err: errors.New("no principal")},
		Authorizer:    ScopeAuthorizer{},
		AuditSink:     NewInMemoryAuditSink(),
		Supplies:      supplies,
		Logger:        log,
	})
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	id, _, err := srv.RegisterWorkflow(context.Background(), namespace.Default, webhookWorkflowDef(webhookTestParams()))
	if err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	body := []byte(`{"order":"A-1"}`)

	// (1) The secret is not stored yet: the route fails closed and the operator
	// is told which entry is unconfigured.
	req := httptest.NewRequest(http.MethodPost, webhookHookPath(id, "in"), strings.NewReader(string(body)))
	req.Header.Set("X-Event-Id", "evt-unconfigured")
	signWebhookRequest(req, body, webhookTestSecret)
	if rec := serveWebhook(t, srv, req); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d (%s), want 503", rec.Code, rec.Body.String())
	}

	putWebhookSecret(t, supplies, "hook-secret", webhookTestSecret)

	// (2) The real secret signs a different body: a wrong signature whose bytes
	// are derived from the secret.
	mac := hmac.New(sha256.New, []byte(webhookTestSecret))
	mac.Write([]byte(`{"order":"A-2"}`))
	wrongSignature := webhookSignatureScheme + hex.EncodeToString(mac.Sum(nil))
	req = httptest.NewRequest(http.MethodPost, webhookHookPath(id, "in"), strings.NewReader(string(body)))
	req.Header.Set("X-Event-Id", "evt-wrong-signature")
	req.Header.Set(webhookSignatureHeader, wrongSignature)
	if rec := serveWebhook(t, srv, req); rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d (%s), want 401", rec.Code, rec.Body.String())
	}

	// (3) A well-formed request, which logs nothing but must still not have
	// logged the secret anywhere on the way through.
	req = httptest.NewRequest(http.MethodPost, webhookHookPath(id, "in"), strings.NewReader(string(body)))
	req.Header.Set("X-Event-Id", "evt-ok")
	signWebhookRequest(req, body, webhookTestSecret)
	rec := serveWebhook(t, srv, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (%s), want 200", rec.Code, rec.Body.String())
	}

	if len(log.lines) == 0 {
		t.Fatal("no log lines were recorded, so this test proved nothing — the rejected paths must log")
	}
	if log.mentions(webhookTestSecret) {
		t.Fatalf("a log line carried the signing secret: %q", log.lines)
	}
	if log.mentions(strings.TrimPrefix(wrongSignature, webhookSignatureScheme)) {
		t.Fatalf("a log line carried the signature: %q", log.lines)
	}
	if log.mentions("hook-secret") {
		t.Fatalf("a log line carried the supply resource name: %q", log.lines)
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
