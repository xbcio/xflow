package runner

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"sync"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/supply"
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

	// gate, when set, is the activation-time supply readiness gate. nil means no
	// gating (a runner with no supply-consuming workflows, or an older wiring).
	gate *SupplyGate

	// groupRuntime executes trigger-group batches locally. nil means group
	// activations (NodeType == engine.GroupNodeType) are rejected — a runner
	// that never wires GroupRuntime cannot host trigger-groups. Set via
	// WithGroupRuntime.
	groupRuntime *GroupRuntime

	// artifactCode resolves a wasm module's bytes by artifact digest. Activation
	// needs it to compile a module BEFORE registering its supply consumer; see
	// registerSupplyConsumers for why the order is load-bearing. nil means a
	// directive carrying SupplyConsumers is rejected (fail closed).
	artifactCode func(ctx context.Context, digest string) ([]byte, error)

	mu   sync.Mutex
	subs map[activationID]activationState
}

// activationState is what the handler retains per live activation: the trigger
// subscription to close, and the supply-consumer bindings it registered.
// DeactivateDirective carries only the activation identity, so the bindings must
// be remembered here to be undone.
type activationState struct {
	sub      types.TriggerSubscription
	bindings []engine.SupplyConsumerBinding
}

// TriggerActivationHandlerOption configures a TriggerActivationHandler.
type TriggerActivationHandlerOption func(*TriggerActivationHandler)

// WithSeedHTTPClient sets the *http.Client used for entry-seed admission
// requests. When nil (the default), http.DefaultClient is used — matching
// the fallback in protocol.HTTPEntrySeedRuntime.
//
// The client's Timeout should be set larger than the per-request context
// timeout (entrySeedRequestTimeout = 15s in entry_seed_runtime.go) so that
// the context deadline governs normal cancellation while the client Timeout
// acts as an absolute safety net covering connection setup and body reads.
// Recommended: 30s.
func WithSeedHTTPClient(c *http.Client) TriggerActivationHandlerOption {
	return func(h *TriggerActivationHandler) { h.seedClient = c }
}

// WithSupplyGate installs the activation-time supply readiness gate. Without it
// a directive's Supplies are ignored — acceptable only where no workflow
// consumes a supply.
func WithSupplyGate(g *SupplyGate) TriggerActivationHandlerOption {
	return func(h *TriggerActivationHandler) { h.gate = g }
}

// WithGroupRuntime installs the local group-execution runtime used to host
// trigger-group activations (NodeType == engine.GroupNodeType). Without it,
// Activate fails closed for any group directive.
func WithGroupRuntime(gr *GroupRuntime) TriggerActivationHandlerOption {
	return func(h *TriggerActivationHandler) { h.groupRuntime = gr }
}

// WithArtifactCodeResolver installs the digest -> module bytes resolver used to
// compile a wasm module at activation time, before its supply consumer registers.
// Pass the same closure the runner installs as Config.ArtifactCodeResolver: it
// already fronts the local artifact cache, so a repeat activation costs no fetch.
//
// Without it, a directive carrying SupplyConsumers is rejected rather than
// registered against an uncompiled module — see registerSupplyConsumers.
func WithArtifactCodeResolver(fn func(ctx context.Context, digest string) ([]byte, error)) TriggerActivationHandlerOption {
	return func(h *TriggerActivationHandler) { h.artifactCode = fn }
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
		subs:        make(map[activationID]activationState),
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
	// Readiness gate FIRST: not after the subscription starts, and not inside the
	// per-message path. Once a Kafka subscription is live it commits offsets, and
	// a message whose supply is missing then has nowhere safe to go. Declining
	// here leaves the traffic in Kafka with consumer-group lag as the signal.
	if h.gate != nil {
		if err := h.gate.Admit(ctx, d.WorkflowID, d.Supplies); err != nil {
			return err
		}
	}

	// Supply consumers register AFTER Admit and BEFORE the subscription starts.
	// After Admit, because RegisterConsumer notifies immediately when the content
	// is already cached — registering first would deliver nothing and never
	// retry. Before the subscription, because a live subscription is already
	// committing offsets while an unregistered module passes traffic untagged.
	if err := h.registerSupplyConsumers(ctx, d.SupplyConsumers); err != nil {
		return err
	}

	if d.NodeType == engine.GroupNodeType {
		return h.activateGroup(ctx, d)
	}

	handler, ok := h.triggers.Trigger(d.NodeType)
	if !ok {
		// Fail closed: unknown trigger type must not silently no-op.
		return fmt.Errorf("no trigger handler registered for node type %q", d.NodeType)
	}

	input := &types.TriggerActivateInput{
		WorkflowID: types.WorkflowID(d.WorkflowID),
		NodeName:   d.EntryUnitID,
		Params:     withEntrySeedParams(d),
		Runtime: &protocol.HTTPEntrySeedRuntime{
			BaseURL:    h.seedBaseURL,
			Client:     h.seedHTTPClient(),
			Token:      h.authToken,
			Generation: d.Generation,
		},
		Supplies: resolveSuppliesForTrigger(d.Supplies),
	}

	sub, err := handler.Activate(ctx, input)
	if err != nil {
		return err
	}
	h.storeSubscription(ctx, activationID{WorkflowID: d.WorkflowID, EntryUnitID: d.EntryUnitID}, sub, d.SupplyConsumers)
	return nil
}

// activateGroup hosts a trigger-group activation: it resolves the real
// trigger member from the projected package (never "xflow.group" itself —
// that synthetic type has no registered handler), and installs a Runtime
// that executes the group locally per batch via GroupRuntime before
// admitting the real resulting exits (spec 2026-08-07 §3.3-§3.4).
func (h *TriggerActivationHandler) activateGroup(ctx context.Context, d protocol.ActivateDirective) error {
	if h.groupRuntime == nil {
		return fmt.Errorf("trigger-group activation %q: no GroupRuntime configured on this runner", d.EntryUnitID)
	}
	if d.Package == nil {
		return fmt.Errorf("trigger-group activation %q: directive carries no package", d.EntryUnitID)
	}
	pkg := d.Package
	entryNodeDef, ok := findPackageNode(pkg, pkg.EntryNode)
	if !ok {
		return fmt.Errorf("trigger-group activation %q: entry node %q not found in package", d.EntryUnitID, pkg.EntryNode)
	}
	handler, ok := h.triggers.Trigger(entryNodeDef.Type)
	if !ok {
		return fmt.Errorf("no trigger handler registered for node type %q (group %q entry)", entryNodeDef.Type, d.EntryUnitID)
	}

	input := &types.TriggerActivateInput{
		WorkflowID: types.WorkflowID(d.WorkflowID),
		NodeName:   pkg.EntryNode,
		Params:     withGroupEntrySeedParams(entryNodeDef.Parameters, d),
		Runtime: &groupExecTriggerRuntime{
			HTTPEntrySeedRuntime: &protocol.HTTPEntrySeedRuntime{
				BaseURL:    h.seedBaseURL,
				Client:     h.seedHTTPClient(),
				Token:      h.authToken,
				Generation: d.Generation,
			},
			runtime:     h.groupRuntime,
			pkg:         pkg,
			packageHash: d.PackageHash,
		},
		Supplies: resolveSuppliesForTrigger(d.Supplies),
	}

	sub, err := handler.Activate(ctx, input)
	if err != nil {
		return err
	}
	h.storeSubscription(ctx, activationID{WorkflowID: d.WorkflowID, EntryUnitID: d.EntryUnitID}, sub, d.SupplyConsumers)
	return nil
}

// findPackageNode returns the NodeDef named name within pkg.Def, or false if
// absent. Used to resolve the entry node's real trigger type + Parameters —
// the group's own NodeType ("xflow.group") is a synthetic routing label with
// no registered handler; the entry MEMBER's type is what must be looked up.
func findPackageNode(pkg *graph.SubgraphPackage, name string) (types.NodeDef, bool) {
	if pkg == nil || pkg.Def == nil {
		return types.NodeDef{}, false
	}
	for _, n := range pkg.Def.Nodes {
		if n.Name == name {
			return n, true
		}
	}
	return types.NodeDef{}, false
}

// withGroupEntrySeedParams merges the entry node's own Parameters (from the
// projected package) with the entry-seed selecting keys, using the GROUP's
// name (d.EntryUnitID) as entry_unit_id — the admission-key and downstream
// derivation are keyed on the group's entry unit, not the member node's own
// name. Mirrors withEntrySeedParams's merge shape for the standalone-trigger
// path, but sourced from the package instead of d.Params (a group's
// EntryActivation.Params is always empty — see EntryUnitActivation in
// entry_activation_manager.go).
func withGroupEntrySeedParams(nodeParams map[string]any, d protocol.ActivateDirective) map[string]any {
	merged := make(map[string]any, len(nodeParams)+3)
	for k, v := range nodeParams {
		merged[k] = v
	}
	merged["entry_seed"] = true
	merged["entry_unit_id"] = d.EntryUnitID
	merged["workflow_version"] = d.WorkflowVersion
	return merged
}

// registerSupplyConsumers compiles each bound wasm module and then registers it
// as a consumer of its supply node, so a content change rebuilds that module's
// instance pool.
//
// The compile is not an optimization — it is what makes the registration take
// effect. RegisterConsumer notifies immediately when the supply content is
// already cached (which Admit just made true), and the wasm host's notify
// handler silently succeeds when the module has not been compiled yet. The
// registry records that as "this content was accepted", so its re-notify
// condition never fires again. Meanwhile registration marks the module
// source-driven, and a source-driven module with no configured pool refuses
// every message. Compiling first is what closes that window; the module bytes
// come from the artifact cache, so the cost is paid here instead of inside the
// first message's deadline.
//
// Fail closed throughout: a module that cannot be compiled or registered would
// otherwise host traffic with no rules and no diagnostic, which is the exact
// failure this wiring exists to remove. Errors carry only the digest and the
// supply node name — never directive params.
func (h *TriggerActivationHandler) registerSupplyConsumers(ctx context.Context, bindings []engine.SupplyConsumerBinding) error {
	if len(bindings) == 0 {
		return nil
	}
	if h.artifactCode == nil {
		return fmt.Errorf("supply consumer registration requires an artifact code resolver (%d binding(s), first: module %s -> supply %q)",
			len(bindings), bindings[0].ModuleDigest, bindings[0].SupplyNode)
	}
	compiled := make(map[string]bool, len(bindings))
	for _, b := range bindings {
		if !compiled[b.ModuleDigest] {
			raw, err := h.artifactCode(ctx, b.ModuleDigest)
			if err != nil {
				return fmt.Errorf("fetch wasm module %s for supply %q: %w", b.ModuleDigest, b.SupplyNode, err)
			}
			if err := node.CompileWasmModule(ctx, base64.StdEncoding.EncodeToString(raw)); err != nil {
				return fmt.Errorf("compile wasm module %s for supply %q: %w", b.ModuleDigest, b.SupplyNode, err)
			}
			compiled[b.ModuleDigest] = true
		}
		if err := node.RegisterWasmSupplyConsumerByDigest(b.ModuleDigest, b.SupplyNode); err != nil {
			return fmt.Errorf("register wasm module %s as consumer of supply %q: %w", b.ModuleDigest, b.SupplyNode, err)
		}
	}
	return nil
}

// storeSubscription records sub under id, closing any previously stored
// subscription for the same identity (stale-close on generation upgrade —
// see the concurrency-invariant comment this replaces, which still applies:
// ActivationTracker.ProcessDirectives serializes calls to this handler's
// Activate per activation identity).
//
// It also reconciles the supply-consumer registrations by DIFFERENCE: only
// bindings the old activation had and the new one does not are unregistered. A
// generation upgrade normally re-sends identical bindings, and a registration
// key is derived from (module digest, supply node) alone — so "unregister
// everything old, then register everything new" would remove the registration
// the caller just made.
func (h *TriggerActivationHandler) storeSubscription(ctx context.Context, id activationID, sub types.TriggerSubscription, bindings []engine.SupplyConsumerBinding) {
	h.mu.Lock()
	old, exists := h.subs[id]
	h.subs[id] = activationState{sub: sub, bindings: bindings}
	h.mu.Unlock()

	if exists {
		unregisterSupplyConsumers(removedBindings(old.bindings, bindings))
		if old.sub != nil {
			_ = old.sub.Close(ctx)
		}
	}
}

// removedBindings returns the bindings present in old but not in next.
func removedBindings(old, next []engine.SupplyConsumerBinding) []engine.SupplyConsumerBinding {
	if len(old) == 0 {
		return nil
	}
	keep := make(map[engine.SupplyConsumerBinding]bool, len(next))
	for _, b := range next {
		keep[b] = true
	}
	var out []engine.SupplyConsumerBinding
	for _, b := range old {
		if !keep[b] {
			out = append(out, b)
		}
	}
	return out
}

// unregisterSupplyConsumers drops each binding's consumer registration. It is a
// no-op for a pair that was never registered, so it is safe on any subset.
func unregisterSupplyConsumers(bindings []engine.SupplyConsumerBinding) {
	for _, b := range bindings {
		node.UnregisterWasmSupplyConsumerByDigest(b.ModuleDigest, b.SupplyNode)
	}
}

// seedHTTPClient returns the configured client or http.DefaultClient when none
// was injected. This matches the nil-fallback semantics of
// protocol.HTTPEntrySeedRuntime.Client.
func (h *TriggerActivationHandler) seedHTTPClient() *http.Client {
	if h.seedClient != nil {
		return h.seedClient
	}
	return http.DefaultClient
}

// Deactivate closes and removes the stored subscription for the directive's
// identity, and unregisters the supply consumers that activation registered.
// Deactivating an unknown or already-closed activation is a safe no-op
// (idempotent).
func (h *TriggerActivationHandler) Deactivate(d protocol.DeactivateDirective) error {
	id := activationID{WorkflowID: d.WorkflowID, EntryUnitID: d.EntryUnitID}
	h.mu.Lock()
	st, ok := h.subs[id]
	if ok {
		delete(h.subs, id)
	}
	h.mu.Unlock()

	if !ok {
		// Unknown / already removed — idempotent no-op.
		return nil
	}
	unregisterSupplyConsumers(st.bindings)
	if st.sub == nil {
		return nil
	}
	return st.sub.Close(context.Background())
}

// withEntrySeedParams returns a copy of d.Params merged with the entry-seed
// selecting keys so the trigger routes messages through the entry-unit seed
// admission path (see node/trigger/kafka.isEntrySeedActivation and
// seedEntryBatch). The original directive params are never mutated. Params
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

// resolveSuppliesForTrigger reads decoded supply content from the process-wide
// registry for each supply requirement the trigger declares. The SupplyGate has
// already admitted (fetched + applied) these before this call, so
// supply.Default.Decoded() is guaranteed to contain them when RequireReady was
// true. nil is returned when there are no supply requirements (the common
// case for triggers without a DependsOn edge to a supply node).
func resolveSuppliesForTrigger(reqs []engine.SupplyRequirement) map[string]any {
	if len(reqs) == 0 {
		return nil
	}
	decoded := supply.Default.Decoded()
	if len(decoded) == 0 {
		return nil
	}
	supplies := make(map[string]any, len(reqs))
	for _, req := range reqs {
		if content, ok := decoded[req.Node]; ok {
			supplies[req.Node] = content
		}
	}
	if len(supplies) == 0 {
		return nil
	}
	return supplies
}
