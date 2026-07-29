package runner

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// TriggerHandlerLookup resolves a trigger node type to its registered
// TriggerHandler. Task 9 constructs an implementation from the registered
// trigger nodes (e.g. backed by node/registry.LookupTrigger). Keeping it as an
// interface here lets the runner dispatch any registered trigger type without
// hardcoding Kafka.
type TriggerHandlerLookup interface {
	Trigger(nodeType string) (types.TriggerHandler, bool)
}

// TriggerActivationHandler is the production ActivationHandler. Given an
// ActivateDirective it looks up the trigger handler for the directive's
// NodeType, starts a real trigger subscription whose seed Runtime stamps the
// activation's generation, and on Deactivate closes that subscription.
//
// The ActivationTracker owns per-activation ctx cancellation and
// generation-aware restart; this handler only starts and stops subscriptions.
//
// Security (org policy §7): authToken is a Bearer secret and d.Params may
// reference credential capabilities — neither is ever logged, and neither
// appears in returned errors.
type TriggerActivationHandler struct {
	seedBaseURL string
	authToken   string
	triggers    TriggerHandlerLookup
	seedClient  *http.Client // injected via WithSeedHTTPClient; nil uses http.DefaultClient

	mu   sync.Mutex
	subs map[activationID]types.TriggerSubscription
}

// TriggerActivationHandlerOption configures a TriggerActivationHandler.
type TriggerActivationHandlerOption func(*TriggerActivationHandler)

// WithSeedHTTPClient sets the *http.Client used for entry-seed admission
// requests. When nil (the default), http.DefaultClient is used — matching
// the fallback in node.HTTPEntrySeedRuntime.
//
// The client's Timeout should be set larger than the per-request context
// timeout (entrySeedRequestTimeout = 15s in entry_seed_runtime.go) so that
// the context deadline governs normal cancellation while the client Timeout
// acts as an absolute safety net covering connection setup and body reads.
// Recommended: 30s.
func WithSeedHTTPClient(c *http.Client) TriggerActivationHandlerOption {
	return func(h *TriggerActivationHandler) { h.seedClient = c }
}

var _ ActivationHandler = (*TriggerActivationHandler)(nil)

// NewTriggerActivationHandler constructs a TriggerActivationHandler. seedBaseURL
// is the control-plane origin for entry-seed admissions; authToken is the Bearer
// token sent with each seed request; triggers resolves NodeType to a handler.
// Options (e.g. WithSeedHTTPClient) configure optional fields.
func NewTriggerActivationHandler(seedBaseURL string, authToken string, triggers TriggerHandlerLookup, opts ...TriggerActivationHandlerOption) *TriggerActivationHandler {
	h := &TriggerActivationHandler{
		seedBaseURL: seedBaseURL,
		authToken:   authToken,
		triggers:    triggers,
		subs:        make(map[activationID]types.TriggerSubscription),
	}
	for _, o := range opts {
		o(h)
	}
	return h
}

// Activate looks up the trigger handler for d.NodeType and starts a subscription
// whose seed runtime is stamped with d.Generation. The subscription is stored
// keyed by activation identity so Deactivate can close it. Returns an error
// (fail closed) if the NodeType has no registered handler.
func (h *TriggerActivationHandler) Activate(ctx context.Context, d protocol.ActivateDirective) error {
	handler, ok := h.triggers.Trigger(d.NodeType)
	if !ok {
		// Fail closed: unknown trigger type must not silently no-op.
		return fmt.Errorf("no trigger handler registered for node type %q", d.NodeType)
	}

	input := &types.TriggerActivateInput{
		WorkflowID: types.WorkflowID(d.WorkflowID),
		NodeName:   d.EntryUnitID,
		Params:     withEntrySeedParams(d),
		Runtime: &node.HTTPEntrySeedRuntime{
			BaseURL:    h.seedBaseURL,
			Client:     h.seedHTTPClient(),
			Token:      h.authToken,
			Generation: d.Generation,
		},
	}

	sub, err := handler.Activate(ctx, input)
	if err != nil {
		return err
	}

	id := activationID{WorkflowID: d.WorkflowID, EntryUnitID: d.EntryUnitID}
	h.mu.Lock()
	// Stale-close on generation upgrade: if a subscription already exists for
	// this activation identity, close it so it does not leak resources (the
	// tracker already canceled its context, but Close deterministically releases
	// the consumer/connection).
	//
	// Concurrency invariant: ActivationTracker.ProcessDirectives holds t.mu for
	// the entire directive batch, serializing calls to this handler's Activate
	// per activation identity. Therefore concurrent Activate calls for the same
	// (WorkflowID, EntryUnitID) cannot race here, and no additional lock
	// ordering or CAS is required.
	if old, exists := h.subs[id]; exists && old != nil {
		h.mu.Unlock()
		_ = old.Close(ctx)
		h.mu.Lock()
	}
	h.subs[id] = sub
	h.mu.Unlock()
	return nil
}

// seedHTTPClient returns the configured client or http.DefaultClient when none
// was injected. This matches the nil-fallback semantics of
// node.HTTPEntrySeedRuntime.Client.
func (h *TriggerActivationHandler) seedHTTPClient() *http.Client {
	if h.seedClient != nil {
		return h.seedClient
	}
	return http.DefaultClient
}

// Deactivate closes and removes the stored subscription for the directive's
// identity. Deactivating an unknown or already-closed activation is a safe
// no-op (idempotent).
func (h *TriggerActivationHandler) Deactivate(d protocol.DeactivateDirective) error {
	id := activationID{WorkflowID: d.WorkflowID, EntryUnitID: d.EntryUnitID}
	h.mu.Lock()
	sub, ok := h.subs[id]
	if ok {
		delete(h.subs, id)
	}
	h.mu.Unlock()

	if !ok || sub == nil {
		// Unknown / already removed — idempotent no-op.
		return nil
	}
	return sub.Close(context.Background())
}

// withEntrySeedParams returns a copy of d.Params merged with the entry-seed
// selecting keys so the trigger routes messages through the entry-unit seed
// admission path (see node/internal/trigger.isEntrySeedActivation and
// seedKafkaEntryBatch). The original directive params are never mutated. Params
// are treated as opaque and never logged.
func withEntrySeedParams(d protocol.ActivateDirective) map[string]any {
	merged := make(map[string]any, len(d.Params)+3)
	for k, v := range d.Params {
		merged[k] = v
	}
	merged["entry_seed"] = true
	merged["entry_unit_id"] = d.EntryUnitID
	merged["workflow_version"] = d.WorkflowVersion
	return merged
}
