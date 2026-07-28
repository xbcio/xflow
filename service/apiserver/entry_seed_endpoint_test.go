package apiserver

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
)

// newSeedTestServer builds an apiserver over an in-memory local backend control
// plane. When withAuth is true a bearer PrincipalAuthenticator is configured so
// unauthenticated requests are rejected before the handler runs.
func newSeedTestServer(t *testing.T, withAuth bool) *httptest.Server {
	t.Helper()
	backend := local.New()
	cp, err := control.NewControlPlane(control.Config{Backend: backend})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	cfg := Config{Concurrency: 1}
	if withAuth {
		cfg.PrincipalAuth = NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{
			{Token: "tok-full", Subject: "op-full", Namespace: "namespaceA", Scopes: []string{"workflow", "execution"}},
		})
		cfg.Authorizer = NamespaceAwareAuthorizer{}
		cfg.AuditSink = NewInMemoryAuditSink()
	}
	srv, err := New(cfg, WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)
	return httpSrv
}

func postSeed(t *testing.T, base, token string, body any) *http.Response {
	t.Helper()
	var r io.Reader
	if body != nil {
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
		r = &buf
	}
	req, _ := http.NewRequest(http.MethodPost, base+"/v1/executions", r)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/executions: %v", err)
	}
	return resp
}

func TestSeedExecutionEndpointAcceptedThenDuplicate(t *testing.T) {
	srv := newSeedTestServer(t, false)

	body := protocol.SeedExecutionRequest{
		ProtocolVersion: protocol.EntrySeedProtocolVersion,
		WorkflowID:      "wf-1",
		WorkflowVersion: "v1",
		EntryUnitID:     "kafka-in",
		AdmissionKey:    "default/wf-1/v1/kafka-in/topic/0/5-5",
		Outcome:         "success",
		Exits:           []protocol.BoundaryExit{{NodeName: "kafka-in", Port: "main", Data: map[string]any{"offset": float64(5)}}},
	}

	resp := postSeed(t, srv.URL, "", body)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out protocol.SeedExecutionResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.State != "accepted" {
		t.Fatalf("state = %q, want accepted", out.State)
	}
	if out.ExecutionID == "" {
		t.Fatal("execution_id must be non-empty")
	}
	if out.Duplicate {
		t.Fatal("first seed must not be duplicate")
	}
	firstID := out.ExecutionID

	// Second identical POST → duplicate:true, same execution id.
	resp2 := postSeed(t, srv.URL, "", body)
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("second status = %d, want 200", resp2.StatusCode)
	}
	var out2 protocol.SeedExecutionResponse
	if err := json.NewDecoder(resp2.Body).Decode(&out2); err != nil {
		t.Fatal(err)
	}
	if !out2.Duplicate {
		t.Fatal("second seed with same key+hash must be duplicate")
	}
	if out2.ExecutionID != firstID {
		t.Fatalf("duplicate execution_id = %q, want %q", out2.ExecutionID, firstID)
	}
}

func TestSeedExecutionEndpointRejectsMissingFields(t *testing.T) {
	srv := newSeedTestServer(t, false)
	// Missing AdmissionKey / Outcome etc.
	resp := postSeed(t, srv.URL, "", protocol.SeedExecutionRequest{WorkflowID: "wf-1"})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for missing required fields", resp.StatusCode)
	}
}

func TestSeedExecutionEndpointRequiresAuth(t *testing.T) {
	srv := newSeedTestServer(t, true)
	body := protocol.SeedExecutionRequest{
		WorkflowID:   "wf-1",
		EntryUnitID:  "kafka-in",
		AdmissionKey: "namespaceA/wf-1/v1/kafka-in/topic/0/5-5",
		Outcome:      "success",
	}
	// No token → 401.
	resp := postSeed(t, srv.URL, "", body)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 for unauthenticated seed", resp.StatusCode)
	}
}
