package apiserver

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/types"
)

// seedCaptureState records the entry-admission request the engine hands to the
// store. The in-memory entry-admission backend does not persist the definition
// hash itself, so the request is the observable seam on this route: it is what
// a hash-aware backend would write onto the execution.
type seedCaptureState struct {
	engine.StateStore

	mu    sync.Mutex
	seeds []engine.SeedExecutionFromEntryRequest
}

func (s *seedCaptureState) SeedExecutionFromEntry(ctx context.Context, req engine.SeedExecutionFromEntryRequest) (engine.SeedExecutionFromEntryResponse, error) {
	s.mu.Lock()
	s.seeds = append(s.seeds, req)
	s.mu.Unlock()
	admitter, ok := s.StateStore.(engine.EntryAdmissionStore)
	if !ok {
		return engine.SeedExecutionFromEntryResponse{}, errors.New("captured state does not admit entry seeds")
	}
	return admitter.SeedExecutionFromEntry(ctx, req)
}

func (s *seedCaptureState) lastSeed(t *testing.T) engine.SeedExecutionFromEntryRequest {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.seeds) == 0 {
		t.Fatal("no entry-admission request reached the store")
	}
	return s.seeds[len(s.seeds)-1]
}

// seedCaptureProvider keeps every provider capability of the embedded local
// backend except State, whose admission calls are recorded.
type seedCaptureProvider struct {
	*local.Backend
	state *seedCaptureState
}

func (p *seedCaptureProvider) State() engine.StateStore { return p.state }

// newWebhookSeedCaptureServer builds the production webhook wiring (a real
// control plane and its registry) over a local backend whose seed admissions
// are observable.
func newWebhookSeedCaptureServer(t *testing.T) (*APIServer, store.Supplies, *seedCaptureProvider) {
	t.Helper()
	base := local.New()
	provider := &seedCaptureProvider{
		Backend: base,
		state:   &seedCaptureState{StateStore: base.State()},
	}
	cp, err := control.NewControlPlane(control.Config{
		Backend: provider,
		// Same in-memory activation store buildControlPlane wires on the
		// single-node path: the webhook route mounts only where entry units are
		// durable activations.
		EntryActivationStore: control.NewMemoryEntryActivationStore(),
	})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	supplies := memstore.New()
	srv, err := New(Config{
		PrincipalAuth: staticPrincipalAuth{err: errors.New("no principal")},
		Authorizer:    ScopeAuthorizer{},
		AuditSink:     NewInMemoryAuditSink(),
		Supplies:      supplies,
	}, WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	return srv, supplies, provider
}

// TestWebhookEntrySeedsTheRegistryDefinitionHash pins that the server-hosted
// webhook route carries the hash of the registry record it resolved into the
// entry admission. The hash is copied from that record — the route has no
// execution in hand and must not derive one.
func TestWebhookEntrySeedsTheRegistryDefinitionHash(t *testing.T) {
	srv, supplies, provider := newWebhookSeedCaptureServer(t)
	putWebhookSecret(t, supplies, "hook-secret", webhookTestSecret)
	id, _, err := srv.RegisterWorkflow(context.Background(), namespace.Default, webhookWorkflowDef(webhookTestParams()))
	if err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	rec, err := provider.WorkflowRegistry().GetWorkflow(context.Background(), id)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if rec.DefinitionHash == "" {
		t.Fatal("registered record carries no definition hash; the fixture cannot distinguish a copied hash from a default")
	}

	body := []byte(`{"order":"A-1"}`)
	req := newWebhookRequest(t, id, "in", "evt-defhash", body)
	recorder := serveWebhook(t, srv, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d (%s), want 200", recorder.Code, recorder.Body.String())
	}

	seed := provider.state.lastSeed(t)
	if seed.WorkflowID != id {
		t.Fatalf("seed workflow id = %q, want %q", seed.WorkflowID, id)
	}
	if seed.DefinitionHash != rec.DefinitionHash {
		t.Fatalf("seed definition hash = %q, want the registry record's %q", seed.DefinitionHash, rec.DefinitionHash)
	}
}

func newWebhookRequest(t *testing.T, id types.WorkflowID, entryUnit, eventID string, body []byte) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, webhookHookPath(id, entryUnit), strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("new webhook request: %v", err)
	}
	req.Header.Set("X-Event-Id", eventID)
	signWebhookRequest(req, body, webhookTestSecret)
	return req
}
