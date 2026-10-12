package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	xbackend "github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// newSeedTestServer builds an apiserver over an in-memory local backend control
// plane. When withAuth is true a bearer PrincipalAuthenticator is configured so
// unauthenticated requests are rejected before the handler runs.
//
// The control plane resolves the seed's graph + downstream from the workflow
// registry server-side (spec §11.5), so the helper registers a "wf-1"/v1
// workflow whose entry unit is "kafka-in" feeding a downstream "store" node.
// The registry is keyed by workflow ID, so one record serves both the no-auth
// and with-auth tests.
func newSeedTestServer(t *testing.T, withAuth bool) *httptest.Server {
	t.Helper()
	return newSeedTestServerWithWorkflow(t, seedWorkflowDef(), withAuth)
}

// newSeedTestServerWithWorkflow is the same server over an arbitrary
// definition, so a test whose subject is a workflow SETTING (a concurrency
// limit) can register the definition that expresses it and still reuse the
// registry/control-plane/auth wiring above.
func newSeedTestServerWithWorkflow(t *testing.T, def *types.WorkflowDef, withAuth bool) *httptest.Server {
	t.Helper()
	backend := local.New()
	registerSeedWorkflowDef(t, backend, def)
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

// seedWorkflowDef is the entry-unit seed workflow the endpoint tests use:
// entry trigger "kafka-in" → action "store".
func seedWorkflowDef() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:    "wf-1",
		Version: "v1",
		Nodes: []types.NodeDef{
			{Name: "kafka-in", Kind: types.NodeKindTrigger},
			{Name: "store", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{"kafka-in": {"main": types.PortConnections{Targets: []types.Connection{{Node: "store", Input: "main"}}}}},
	}
}

// registerSeedWorkflow registers the "wf-1"/v1 workflow used by the seed
// endpoint tests: entry trigger "kafka-in" → action "store".
func registerSeedWorkflow(t *testing.T, be *local.Backend) {
	t.Helper()
	registerSeedWorkflowDef(t, be, seedWorkflowDef())
}

// registerSeedWorkflowDef compiles and registers def under its own name/version.
// The registry is keyed by workflow ID, so the ID and the hash are derived from
// the definition rather than fixed: two definitions registered in one test would
// otherwise collide on both.
func registerSeedWorkflowDef(t *testing.T, be *local.Backend, def *types.WorkflowDef) {
	t.Helper()
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile seed workflow: %v", err)
	}
	if _, err := be.WorkflowRegistry().AddWorkflow(context.Background(), xbackend.WorkflowRecord{
		ID:             types.WorkflowID(def.Name),
		Key:            def.Name + "@" + def.Version,
		Name:           def.Name,
		Version:        def.Version,
		DefinitionHash: "sha256:" + def.Name,
		Definition:     def,
		Graph:          g,
	}); err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}
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

// seedRejectWorkflowDef admits one execution at a time and refuses the rest.
// The mode is explicit because a bare limit means QUEUE mode, and this test is
// about the refusal, not about parking.
func seedRejectWorkflowDef() *types.WorkflowDef {
	def := seedWorkflowDef()
	def.Name = "wf-seed-limit"
	def.Settings = &types.WorkflowSettings{
		Concurrency: &types.ConcurrencySettings{
			Limit: 1,
			Mode:  types.ConcurrencyModeReject,
		},
	}
	return def
}

// TestSeedExecutionEndpointRefusesOverLimitWith429 is the seed-path counterpart
// of TestExecuteWorkflowRefusesOverLimitWith429: the second seed of a workflow
// whose one allowed slot is taken answers 429 with the refusal state in the
// body, NOT 200.
//
// The status is the load-bearing part. A 200 here would make the runner commit
// the Kafka offset for a result that was never persisted — the message would be
// gone with no execution behind it. A 5xx would withhold the offset too, but it
// would file a designed throttle as a server fault, so an operator would see an
// outage on a deployment that is behaving exactly as configured.
func TestSeedExecutionEndpointRefusesOverLimitWith429(t *testing.T) {
	srv := newSeedTestServerWithWorkflow(t, seedRejectWorkflowDef(), false)

	seedOnce := func(key string) *http.Response {
		t.Helper()
		return postSeed(t, srv.URL, "", protocol.SeedExecutionRequest{
			ProtocolVersion: protocol.EntrySeedProtocolVersion,
			WorkflowID:      "wf-seed-limit",
			WorkflowVersion: "v1",
			EntryUnitID:     "kafka-in",
			AdmissionKey:    key,
			Outcome:         "success",
			Exits:           []protocol.BoundaryExit{{NodeName: "kafka-in", Port: "main", Data: map[string]any{"offset": float64(1)}}},
		})
	}

	// First seed takes the only slot and, with no runner consuming the queue, the
	// execution stays live — so nothing has to run for the refusal below.
	first := seedOnce("default/wf-seed-limit/v1/kafka-in/topic/0/1-1")
	defer func() { _ = first.Body.Close() }()
	if first.StatusCode != http.StatusOK {
		t.Fatalf("first seed status = %d, want 200", first.StatusCode)
	}

	second := seedOnce("default/wf-seed-limit/v1/kafka-in/topic/0/2-2")
	defer func() { _ = second.Body.Close() }()
	if second.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second seed status = %d, want 429", second.StatusCode)
	}
	// A floor rather than a promise — no slot-release time is knowable — but it
	// must be present and parseable, or a runner that only reads the status has
	// nothing to back off against.
	if got := second.Header.Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want \"1\"", got)
	}
	var refusal protocol.SeedExecutionRefusal
	if err := json.NewDecoder(second.Body).Decode(&refusal); err != nil {
		t.Fatalf("decode refusal body: %v", err)
	}
	if refusal.State != protocol.SeedStateOverLimit {
		t.Fatalf("state = %q, want %q", refusal.State, protocol.SeedStateOverLimit)
	}
	if refusal.Error != "workflow_concurrency_limit" {
		t.Fatalf("error = %q, want workflow_concurrency_limit", refusal.Error)
	}
	// The body carries the same floor as the header so an IN-PROCESS caller — the
	// server-hosted webhook admission, which builds its own HTTP response and has
	// no header to copy — can state a Retry-After at all.
	if refusal.RetryAfterSeconds != concurrencyRetryAfterSeconds {
		t.Fatalf("retry_after_seconds = %d, want %d", refusal.RetryAfterSeconds, concurrencyRetryAfterSeconds)
	}
}

// TestSeedExecutionEndpointMapsConcurrencyUnsupportedTo503 pins the fail-closed
// case on the seed face: the seeded workflow declares a concurrency limit and
// this deployment's store cannot enforce one, so the engine refuses the seed
// rather than starting an execution the limit was written to prevent.
//
// 503 with the same code the submission face uses, not 500: it is a designed
// refusal with a known cause, and a runner reading internal_error would retry a
// batch that no retry can admit. Unknown-429 handling covers the reverse
// direction; this is the one the SERVER reports.
func TestSeedExecutionEndpointMapsConcurrencyUnsupportedTo503(t *testing.T) {
	for _, err := range []error{engine.ErrConcurrencyUnsupported, engine.ErrConcurrencyQueueUnsupported} {
		t.Run(err.Error(), func(t *testing.T) {
			mux := newControlMux(&fakeControlFacade{seedErr: err})
			body, marshalErr := json.Marshal(protocol.SeedExecutionRequest{
				WorkflowID:   "wf-1",
				EntryUnitID:  "kafka-in",
				AdmissionKey: "default/wf-1/v1/kafka-in/topic/0/5-5",
				Outcome:      "success",
			})
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			req := httptest.NewRequest(http.MethodPost, "/v1/executions", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503", rec.Code)
			}
			if got := rec.Header().Get("Retry-After"); got != "" {
				t.Fatalf("Retry-After = %q on an unavailable capability, want none", got)
			}
			// Bare body, not an envelope: entry-seed is a runner-protocol-face
			// endpoint (§0.1/§8.2) and its failure bodies are load-bearing shapes.
			var failure struct {
				Error string `json:"error"`
				Code  string `json:"code"`
			}
			if decodeErr := json.Unmarshal(rec.Body.Bytes(), &failure); decodeErr != nil {
				t.Fatalf("decode body: %v", decodeErr)
			}
			if failure.Error != "workflow_concurrency_unavailable" {
				t.Fatalf("error = %q, want workflow_concurrency_unavailable", failure.Error)
			}
			if failure.Code != "" {
				t.Fatalf("body carries an envelope code field (%q); the runner face is not enveloped", failure.Code)
			}
		})
	}
}

// TestSeedExecutionEndpointQueuesThenRefusesFullQueue covers the queue-mode
// half of the seed admission end to end: over the limit a seed is PARKED and
// still accepted (200, queued with its position), and only a seed past
// max_queued is refused.
//
// The parked case must not be a 429. The execution is durable and will run, so
// the runner has to commit the offset — refusing it would redeliver a batch
// whose work is already recorded. The position is what lets the runner's log
// say the batch is waiting in line rather than stuck.
func TestSeedExecutionEndpointQueuesThenRefusesFullQueue(t *testing.T) {
	def := seedWorkflowDef()
	def.Name = "wf-seed-queue"
	def.Settings = &types.WorkflowSettings{
		Concurrency: &types.ConcurrencySettings{
			Limit:     1,
			Mode:      types.ConcurrencyModeQueue,
			MaxQueued: 1,
		},
	}
	srv := newSeedTestServerWithWorkflow(t, def, false)

	seedAt := func(offset int) *http.Response {
		t.Helper()
		return postSeed(t, srv.URL, "", protocol.SeedExecutionRequest{
			ProtocolVersion: protocol.EntrySeedProtocolVersion,
			WorkflowID:      "wf-seed-queue",
			WorkflowVersion: "v1",
			EntryUnitID:     "kafka-in",
			AdmissionKey:    fmt.Sprintf("default/wf-seed-queue/v1/kafka-in/topic/0/%d-%d", offset, offset),
			Outcome:         "success",
			Exits:           []protocol.BoundaryExit{{NodeName: "kafka-in", Port: "main", Data: map[string]any{"offset": float64(offset)}}},
		})
	}

	// 1st: takes the slot. No runner consumes the queue, so the execution stays
	// live and the slot stays held — nothing has to run for the parking below.
	first := seedAt(1)
	defer func() { _ = first.Body.Close() }()
	if first.StatusCode != http.StatusOK {
		t.Fatalf("first seed status = %d, want 200", first.StatusCode)
	}

	// 2nd: over the limit, but the queue has room → parked, admitted, 200.
	second := seedAt(2)
	defer func() { _ = second.Body.Close() }()
	if second.StatusCode != http.StatusOK {
		t.Fatalf("parked seed status = %d, want 200 (a parked seed is an admitted one)", second.StatusCode)
	}
	var parked protocol.SeedExecutionResponse
	if err := json.NewDecoder(second.Body).Decode(&parked); err != nil {
		t.Fatalf("decode parked response: %v", err)
	}
	if parked.State != "accepted" || parked.ExecutionID == "" {
		t.Fatalf("parked seed = %+v, want an accepted admission with an execution id", parked)
	}
	if !parked.Queued || parked.QueuePosition != 1 {
		t.Fatalf("parked seed queued/position = %v/%d, want true/1", parked.Queued, parked.QueuePosition)
	}

	// 3rd: the backlog is at max_queued → refused, nothing persisted.
	third := seedAt(3)
	defer func() { _ = third.Body.Close() }()
	if third.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("third seed status = %d, want 429", third.StatusCode)
	}
	if got := third.Header.Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want \"1\"", got)
	}
	var refusal protocol.SeedExecutionRefusal
	if err := json.NewDecoder(third.Body).Decode(&refusal); err != nil {
		t.Fatalf("decode refusal body: %v", err)
	}
	if refusal.State != protocol.SeedStateQueueFull {
		t.Fatalf("state = %q, want %q", refusal.State, protocol.SeedStateQueueFull)
	}
	if refusal.Error != "workflow_concurrency_queue_full" {
		t.Fatalf("error = %q, want workflow_concurrency_queue_full", refusal.Error)
	}
}
