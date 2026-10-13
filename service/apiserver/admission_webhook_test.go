package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/types"
)

// TestAPIAdmissionWebhookEntry covers the webhook route: the rate is charged
// per namespace+workflow and only to signed requests, a refusal answers in the
// route's own {"error": code} shape with Retry-After, and the shared inflight
// bound sheds it with 503.
func TestAPIAdmissionWebhookEntry(t *testing.T) {
	supplies := memstore.New()
	srv, err := New(Config{
		APILimits:     APILimits{Rate: admissionTestRate, Burst: 1, MaxInflight: 2},
		PrincipalAuth: staticPrincipalAuth{err: errors.New("no principal")},
		Authorizer:    ScopeAuthorizer{},
		AuditSink:     NewInMemoryAuditSink(),
		Supplies:      supplies,
	})
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	putWebhookSecret(t, supplies, "hook-secret", webhookTestSecret)
	wfA, _, err := srv.RegisterWorkflow(context.Background(), namespace.Default, webhookWorkflowDef(webhookTestParams()))
	if err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	defB := webhookWorkflowDef(webhookTestParams())
	defB.Name = "hook-b"
	wfB, _, err := srv.RegisterWorkflow(context.Background(), namespace.Default, defB)
	if err != nil {
		t.Fatalf("RegisterWorkflow b: %v", err)
	}

	send := func(id types.WorkflowID, event string, signed bool) *httptest.ResponseRecorder {
		body := []byte(`{"n":"` + event + `"}`)
		req := httptest.NewRequest(http.MethodPost, webhookHookPath(id, "in"), bytes.NewReader(body))
		req.Header.Set("X-Event-Id", event)
		if signed {
			signWebhookRequest(req, body, webhookTestSecret)
		}
		return serveWebhook(t, srv, req)
	}

	// An unsigned request is refused by the signature and does not spend the
	// sender's budget.
	if rec := send(wfA, "evt-0", false); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unsigned status = %d, want 401", rec.Code)
	}
	if rec := send(wfA, "evt-1", true); rec.Code != http.StatusOK {
		t.Fatalf("first signed status = %d (%s), want 200", rec.Code, rec.Body.String())
	}
	rec := send(wfA, "evt-2", true)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("second signed status = %d (%s), want 429", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("webhook 429 carried no Retry-After")
	}
	var body errorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body.Error != "rate_limited" {
		t.Fatalf("webhook 429 body = %q, want {\"error\":\"rate_limited\"}", rec.Body.String())
	}
	if rec := send(wfB, "evt-3", true); rec.Code != http.StatusOK {
		t.Fatalf("another workflow in the namespace was charged: status = %d", rec.Code)
	}

	// The webhook shares the control routes' global bound: one replica, one bound.
	var execReleases []func()
	for i := 0; i < 2; i++ {
		release, ok := srv.ctrl.admission.acquire(routeClassExecute)
		if !ok {
			t.Fatalf("execute slot %d refused below the bound", i)
		}
		execReleases = append(execReleases, release)
	}
	rec = send(wfB, "evt-4", true)
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("webhook with the bound full = %d Retry-After %q, want 503 and 1", rec.Code, rec.Header().Get("Retry-After"))
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body.Error != "server_overloaded" {
		t.Fatalf("webhook 503 body = %q, want {\"error\":\"server_overloaded\"}", rec.Body.String())
	}
	for _, release := range execReleases {
		release()
	}

	// And it has its own sub-limit — a quarter of the bound, at least 1 — so a
	// webhook flood is shed while authenticated execute work still has room.
	webRelease, ok := srv.ctrl.admission.acquire(routeClassWebhook)
	if !ok {
		t.Fatal("webhook slot refused on an idle replica")
	}
	rec = send(wfB, "evt-5", true)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("webhook past its sub-limit = %d (%s), want 503", rec.Code, rec.Body.String())
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body.Error != "server_overloaded" {
		t.Fatalf("webhook sub-limit 503 body = %q, want {\"error\":\"server_overloaded\"}", rec.Body.String())
	}
	execRelease, ok := srv.ctrl.admission.acquire(routeClassExecute)
	if !ok {
		t.Fatal("the webhook sub-limit consumed the execute class's share of the bound")
	}
	execRelease()
	webRelease()
}
