package runner

import (
	"context"
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
	// Computed once and threaded through: registerSupplyConsumers uses it as the
	// owner identity for each legacy binding's digest-keyed registration (spec
	// Z.4 / Z.8), and the deferred rollback below must release the SAME owner
	// identity that call registered under.
	id := activationIDFromActivate(d)

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
	if err := h.registerSupplyConsumers(ctx, id, d.SupplyConsumers); err != nil {
		return err
	}

	// INVARIANT (fix round 2, FINDING 3): registerSupplyConsumers just
	// succeeded, which means EVERY binding in d.SupplyConsumers is now
	// registered/declared/warmed (registerSupplyConsumers is itself atomic —
	// see its own comment). From here to the end of this call, that
	// registration must either reach storeSubscription — whose caller then
	// owns releasing it on the next Deactivate or generation upgrade — or be
	// released here before Activate returns an error. Skipping this leaks one
	// reference per binding on every early return below, and
	// ActivationTracker.activateLocked does not record a failed Activate into
	// t.active, so a retried directive for the same generation acquires
	// again on every retry: the leak is unbounded, not merely long-lived, and
	// on this project's runners (never restarted) it is permanent. A
	// stranded warm-up consumer whose module later fails to resolve then
	// makes supply.Registry.isReadyLocked report the ENTIRE pointer supply
	// not-ready for every OTHER workflow sharing it, with no activation left
	// to Deactivate and clear it.
	//
	// reachedStore is set true ONLY immediately alongside the one
	// storeSubscription call this invocation can make (either directly below,
	// or inside activateGroup, which reports it back explicitly rather than
	// Activate inferring success from activateGroup's own control flow) — so
	// a future return path added to either function defaults to the safe
	// (compensate) side instead of silently skipping compensation. On the
	// success path this must NOT fire: storeSubscription reconciles the OLD
	// registration, which is a different reference than the one just
	// acquired for this call.
	reachedStore := false
	defer func() {
		if !reachedStore {
			unregisterSupplyConsumers(id, d.SupplyConsumers)
		}
	}()

	if d.NodeType == engine.GroupNodeType {
		ok, err := h.activateGroup(ctx, d)
		reachedStore = ok
		return err
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
			BaseURL:      h.seedBaseURL,
			Client:       h.seedHTTPClient(),
			Token:        h.authToken,
			Generation:   d.Generation,
			ReplicaIndex: d.ReplicaIndex,
		},
		Supplies: resolveSuppliesForTrigger(d.Supplies),
	}

	sub, err := handler.Activate(ctx, input)
	if err != nil {
		return err
	}
	reachedStore = true
	h.storeSubscription(ctx, id, sub, d.SupplyConsumers)
	return nil
}

// activateGroup hosts a trigger-group activation: it resolves the real
// trigger member from the projected package (never "xflow.group" itself —
// that synthetic type has no registered handler), and installs a Runtime
// that executes the group locally per batch via GroupRuntime before
// admitting the real resulting exits (spec 2026-08-07 §3.3-§3.4).
//
// The bool return reports whether storeSubscription was reached (i.e.
// activation actually took hold) — Activate uses it to decide whether the
// supply-consumer registrations registerSupplyConsumers made for this
// directive must be released (fix round 2, FINDING 3). It is set true
// exactly once, immediately alongside the only storeSubscription call in
// this function; every early return above that point reports false.
func (h *TriggerActivationHandler) activateGroup(ctx context.Context, d protocol.ActivateDirective) (reachedStore bool, err error) {
	if h.groupRuntime == nil {
		return false, fmt.Errorf("trigger-group activation %q: no GroupRuntime configured on this runner", d.EntryUnitID)
	}
	if d.Package == nil {
		return false, fmt.Errorf("trigger-group activation %q: directive carries no package", d.EntryUnitID)
	}
	pkg := d.Package
	entryNodeDef, ok := findPackageNode(pkg, pkg.EntryNode)
	if !ok {
		return false, fmt.Errorf("trigger-group activation %q: entry node %q not found in package", d.EntryUnitID, pkg.EntryNode)
	}
	handler, ok := h.triggers.Trigger(entryNodeDef.Type)
	if !ok {
		return false, fmt.Errorf("no trigger handler registered for node type %q (group %q entry)", entryNodeDef.Type, d.EntryUnitID)
	}

	input := &types.TriggerActivateInput{
		WorkflowID: types.WorkflowID(d.WorkflowID),
		NodeName:   pkg.EntryNode,
		Params:     withGroupEntrySeedParams(entryNodeDef.Parameters, d),
		Runtime: &groupExecTriggerRuntime{
			HTTPEntrySeedRuntime: &protocol.HTTPEntrySeedRuntime{
				BaseURL:      h.seedBaseURL,
				Client:       h.seedHTTPClient(),
				Token:        h.authToken,
				Generation:   d.Generation,
				ReplicaIndex: d.ReplicaIndex,
			},
			runtime:     h.groupRuntime,
			pkg:         pkg,
			packageHash: d.PackageHash,
		},
		Supplies: resolveSuppliesForTrigger(d.Supplies),
	}

	sub, err := handler.Activate(ctx, input)
	if err != nil {
		return false, err
	}
	h.storeSubscription(ctx, activationIDFromActivate(d), sub, d.SupplyConsumers)
	return true, nil
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

// bindingIdentity renders the part of a binding that names WHICH consumer a
// diagnostic is about. A declaration carries no digest yet — the digest only
// exists once supply content resolves — so printing ModuleDigest for one yields
// an empty string. Never include DigestExpr: it is node-authored text and
// belongs in no log line.
func bindingIdentity(b engine.SupplyConsumerBinding) string {
	if b.IsDeclaration() {
		return fmt.Sprintf("declaration %s/%s", b.WorkflowName, b.NodeName)
	}
	return fmt.Sprintf("module %s", b.ModuleDigest)
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
// This call is atomic across bindings (fix round 2, FINDING 3): if any
// binding fails partway through the loop, every reference already
// acquired/declared/warmed for EARLIER bindings in this same call is rolled
// back before returning, via the same unregisterSupplyConsumers a full
// Deactivate uses. Without this, a binding slice that fails partway (e.g. a
// legacy binding's artifact fetch erroring after an earlier declaration
// binding already acquired its two refcounts) would strand those earlier
// references permanently — nothing else in the codebase ever revisits a
// failed Activate to clean up what it partially registered, and a caller
// that only checks "did this call return an error" (as Activate does) must
// be able to treat non-nil as "nothing changed."
//
// Fail closed throughout: a module that cannot be compiled or registered would
// otherwise host traffic with no rules and no diagnostic, which is the exact
// failure this wiring exists to remove. Errors carry only the digest and the
// supply node name — never directive params.
//
// id is this activation's identity, rendered via id.supplyConsumerOwner() into
// the owner string passed to node.RegisterWasmSupplyConsumerByDigest for every
// legacy binding (spec Z.4 / Z.8). It is what lets this activation's own
// eventual Deactivate release exactly its own hold on a (digest, supplyNode)
// registration without disturbing another replica's, or another call site's
// (the warm-up consumer's or the execution-time guard's), hold on the same
// slot.
func (h *TriggerActivationHandler) registerSupplyConsumers(ctx context.Context, id activationID, bindings []engine.SupplyConsumerBinding) error {
	if len(bindings) == 0 {
		return nil
	}
	if h.artifactCode == nil {
		return fmt.Errorf("supply consumer registration requires an artifact code resolver (%d binding(s), first: %s -> supply %q)",
			len(bindings), bindingIdentity(bindings[0]), bindings[0].SupplyNode)
	}
	compiled := make(map[string]bool, len(bindings))
	// acquired tracks every binding THIS call has already registered/declared/
	// warmed, in order, so a later binding's failure can roll all of them
	// back atomically — see the deferred rollback below.
	var acquired []engine.SupplyConsumerBinding
	succeeded := false
	defer func() {
		if !succeeded {
			unregisterSupplyConsumers(id, acquired)
		}
	}()
	owner := id.supplyConsumerOwner()
	for _, b := range bindings {
		if b.IsDeclaration() {
			// Declaration shape: the digest is not known yet. Record it; the
			// execution-time guard in node/internal/code/script does the compile,
			// the registration, and the fail-closed check once boundary
			// evaluation has produced a real digest (spec §4.3.1).
			node.DeclareWasmSupplyConsumers(b.WorkflowName, b.NodeName, []string{b.SupplyNode})
			// The warm-up consumer additionally pre-compiles the module the
			// pointer supply currently names, synchronously inside
			// supply.Registry.Apply, so a pointer flip does not make the first
			// message pay the compile (see wasmWarmupConsumer's doc comment for
			// why synchronous is load-bearing rather than an oversight). Routed
			// through warmupConsumers.acquire, not a bare RegisterConsumer call,
			// because several replicas of this same activation may declare the
			// identical (workflow, node, supply) pair on this runner and compute
			// the identical warmupConsumerKey — see warmupConsumerRegistry.
			warmupConsumers.acquire(warmupConsumerKey(b), b.SupplyNode, &wasmWarmupConsumer{
				digestExpr: b.DigestExpr,
				supplyNode: b.SupplyNode,
				workflow:   b.WorkflowName,
				node:       b.NodeName,
				artifact:   h.artifactCode,
			})
			acquired = append(acquired, b)
			continue
		}
		if !compiled[b.ModuleDigest] {
			raw, err := h.artifactCode(ctx, b.ModuleDigest)
			if err != nil {
				return fmt.Errorf("fetch wasm module %s for supply %q: %w", b.ModuleDigest, b.SupplyNode, err)
			}
			if err := node.CompileWasmModuleBytes(ctx, raw); err != nil {
				return fmt.Errorf("compile wasm module %s for supply %q: %w", b.ModuleDigest, b.SupplyNode, err)
			}
			compiled[b.ModuleDigest] = true
		}
		if err := node.RegisterWasmSupplyConsumerByDigest(b.ModuleDigest, b.SupplyNode, owner); err != nil {
			return fmt.Errorf("register wasm module %s as consumer of supply %q: %w", b.ModuleDigest, b.SupplyNode, err)
		}
		acquired = append(acquired, b)
	}
	succeeded = true
	return nil
}

// storeSubscription records sub under id, closing any previously stored
// subscription for the same identity (stale-close on generation upgrade —
// see the concurrency-invariant comment this replaces, which still applies:
// ActivationTracker.ProcessDirectives serializes calls to this handler's
// Activate per activation identity).
//
// It also reconciles the OLD activation's supply-consumer registrations —
// but the rule differs by binding shape, because the two shapes are governed
// by different registries with different semantics:
//
//   - Legacy bindings (ModuleDigest+SupplyNode) register into an OWNER SET
//     keyed on (digest, supply node) (spec Z.4 / Z.8): the same owner
//     registering twice is idempotent, and releasing that owner removes the
//     underlying registry entry only once no OTHER owner (a different
//     activation/replica, or the warm-up consumer / execution-time guard's
//     "node:" owner) still holds it. A generation upgrade normally re-sends
//     identical legacy bindings under the SAME owner (id.supplyConsumerOwner()
//     does not depend on generation), so only the DIFFERENCE (old minus next)
//     may be released here — releasing a retained pair would drop this
//     activation's own hold on the very registration registerSupplyConsumers
//     just (re-)acquired for the new generation. removedBindings governs this
//     shape.
//
//   - Declaration bindings (WorkflowName+NodeName+SupplyNode) carry TWO
//     independent registrations, and BOTH are REFCOUNTED, not overwritten:
//     the execution-time declaration table
//     (node/internal/code/script/supply_declaration.go) and the warm-up
//     consumer's registration on supply.Default (warmupConsumerRegistry, in
//     wasm_supply_declaration.go). Both exist to survive the SAME scenario —
//     several replicas of one activation declaring the identical pair — so
//     both use the SAME rule here: registerSupplyConsumers has already added
//     one reference per binding in the new `bindings` set (to both tables)
//     before this method runs, so EVERY old declaration binding is released
//     from both here, unconditionally, not just the removed ones. If a
//     re-sent identical declaration were instead excluded from release the
//     way a legacy binding is (by difference), a refcount would only ever
//     grow (gen1 declares -> 1; gen2 declares again -> 2, difference is empty
//     so nothing releases -> stays 2; a later single Deactivate drops it to
//     1, never 0) — stranding the declaration, or the warm-up consumer,
//     alive forever after the LAST Deactivate. removedBindings does NOT
//     govern this pass; do not unify the two branches onto one difference
//     call.
//
// unregisterSupplyConsumers is deliberately NOT called here: it undoes both
// declaration-side registrations unconditionally per binding (see its own
// comment), which is what this pass wants too, but it also handles the
// legacy shape by a plain per-binding unregister with no difference logic —
// wrong for a generation upgrade that retains legacy bindings. Hence the
// split: pass 1 uses unregisterSupplyConsumers only on the legacy DIFFERENCE,
// pass 2 inlines the declaration-side release loop directly.
func (h *TriggerActivationHandler) storeSubscription(ctx context.Context, id activationID, sub types.TriggerSubscription, bindings []engine.SupplyConsumerBinding) {
	h.mu.Lock()
	old, exists := h.subs[id]
	h.subs[id] = activationState{sub: sub, bindings: bindings}
	h.mu.Unlock()

	if exists {
		var oldLegacy, oldDeclarations []engine.SupplyConsumerBinding
		for _, b := range old.bindings {
			if b.IsDeclaration() {
				oldDeclarations = append(oldDeclarations, b)
			} else {
				oldLegacy = append(oldLegacy, b)
			}
		}
		// Pass 1: legacy owner set, by difference. id is the SAME activation
		// identity (and therefore the same owner string) that registered these
		// bindings, old generation or new — releasing under any other id would
		// release the wrong owner's hold, or none at all.
		unregisterSupplyConsumers(id, removedBindings(oldLegacy, bindings))
		// Pass 2: declaration side, BOTH refcounts, unconditionally for every
		// old entry — see the doc comment above for why difference does not
		// apply here.
		for _, b := range oldDeclarations {
			warmupConsumers.release(warmupConsumerKey(b), b.SupplyNode)
			node.UndeclareWasmSupplyConsumers(b.WorkflowName, b.NodeName, []string{b.SupplyNode})
		}
		if old.sub != nil {
			_ = old.sub.Close(ctx)
		}
	}
}

// removedBindings returns the bindings present in old but not in next. Callers
// must pass only legacy (non-declaration) bindings as old — see the shape
// discussion in storeSubscription for why declarations are reconciled by a
// different rule and never go through this function.
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

// unregisterSupplyConsumers undoes each binding unconditionally — for a
// declaration binding, BOTH refcounted registrations it carries (the warm-up
// consumer's registration on supply.Default, via warmupConsumers.release, and
// the execution-time declaration refcount) — plus, for a legacy binding, id's
// OWN hold on the digest-keyed owner set (spec Z.4 / Z.8; see
// wasm.RegisterSupplyConsumerByDigest). Each call here removes exactly ONE
// reference per binding, so this function is safe to call once per
// activation-worth of bindings even when several replicas share the same
// declaration; the underlying refcounts/owner sets (not this function) are
// what make repeated release calls converge to empty only after every
// acquire has been matched. It is a no-op for a pair that was never
// registered/declared, so it is safe on any subset.
//
// id must be the SAME activation identity that registerSupplyConsumers used
// to register these bindings — releasing under a different id would either
// no-op (id never held that owner) or, if some other id happened to collide,
// release the wrong owner. Every call site below passes the id that owns the
// bindings being released.
//
// This unconditional-per-binding shape is correct for a full teardown
// (Deactivate, which wants every reference THIS activation held gone) and
// also for storeSubscription's legacy pass (which pre-filters to the
// DIFFERENCE before calling this). storeSubscription does NOT route
// declarations through this function — see its own comment for why the
// declaration side needs its own inlined loop instead.
func unregisterSupplyConsumers(id activationID, bindings []engine.SupplyConsumerBinding) {
	owner := id.supplyConsumerOwner()
	for _, b := range bindings {
		if b.IsDeclaration() {
			warmupConsumers.release(warmupConsumerKey(b), b.SupplyNode)
			node.UndeclareWasmSupplyConsumers(b.WorkflowName, b.NodeName, []string{b.SupplyNode})
			continue
		}
		node.UnregisterWasmSupplyConsumerByDigest(b.ModuleDigest, b.SupplyNode, owner)
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
	id := activationIDFromDeactivate(d)
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
	unregisterSupplyConsumers(id, st.bindings)
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
