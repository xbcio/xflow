package xflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// ErrTriggerRuntimeClosed is returned by ReconcileWorkflow once the runtime has
// been closed (Engine.Stop). New reconciliations are rejected so a late
// AddWorkflow cannot resurrect subscriptions on a shut-down engine.
var ErrTriggerRuntimeClosed = errors.New("xflow: trigger runtime closed")

type triggerRuntime struct {
	eng        *Engine
	primitives backend.TriggerPrimitives
	webhooks   *webhookRuntime
	// overrides holds the runtime overlays an operator has placed on trigger
	// nodes, read at activation time. nil means no overlay store is configured,
	// which is not a degraded mode: it is the deployment that never serves the
	// schedules API, and every trigger then runs exactly what its definition says.
	overrides store.ScheduleOverrideStore
	mu        sync.Mutex
	// idle is signaled when inflight drops to zero so Close can wait for
	// in-flight activations to finish before tearing everything down.
	idle *sync.Cond
	// subs holds committed (activated) subscriptions keyed by
	// workflowID + "/" + nodeName.
	subs map[string]types.TriggerSubscription
	// reservations records in-flight Activate calls keyed the same way, each
	// tagged with a unique token. Phase 3 only commits a subscription if its
	// reservation token is still present, so a concurrent Close/RemoveWorkflow
	// that cleared the reservation causes the freshly-activated sub to be
	// closed instead of written back (preventing leaks and ghost triggers).
	reservations map[string]uint64
	// nextToken hands out unique reservation tokens.
	nextToken uint64
	// inflight counts ReconcileWorkflow calls currently between phase 1 and
	// phase 3 (activation in progress).
	inflight int
	// closed rejects new reconciliations after Close.
	closed bool
}

type activatedSub struct {
	key   string
	token uint64
	sub   types.TriggerSubscription
}

func newTriggerRuntime(e *Engine, p backend.TriggerPrimitives) *triggerRuntime {
	var logger engine.Logger
	if e != nil {
		logger = e.logger
	}
	r := &triggerRuntime{
		eng:          e,
		primitives:   p,
		webhooks:     newWebhookRuntime(logger),
		subs:         make(map[string]types.TriggerSubscription),
		reservations: make(map[string]uint64),
	}
	if e != nil {
		r.overrides = e.scheduleOverrides
	}
	r.idle = sync.NewCond(&r.mu)
	return r
}

func (r *triggerRuntime) ReconcileWorkflow(ctx context.Context, rec backend.WorkflowRecord) error {
	if rec.Definition == nil {
		return nil
	}

	// Phase 1: under the lock, determine which triggers need activation and
	// reserve them with a unique token to prevent duplicate concurrent Activate
	// calls for the same key and to detect a concurrent Close/Remove later.
	type pending struct {
		key   string
		token uint64
		nd    types.NodeDef
	}
	var toActivate []pending

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrTriggerRuntimeClosed
	}
	for _, nd := range rec.Definition.Nodes {
		if nd.Kind != types.NodeKindTrigger {
			continue
		}
		key := string(rec.ID) + "/" + nd.Name
		if _, exists := r.subs[key]; exists {
			continue
		}
		if _, reserved := r.reservations[key]; reserved {
			continue
		}
		token := r.nextToken
		r.nextToken++
		r.reservations[key] = token
		toActivate = append(toActivate, pending{key: key, token: token, nd: nd})
	}
	if len(toActivate) == 0 {
		r.mu.Unlock()
		return nil
	}
	// Mark this reconciliation in flight so Close waits for it to finish.
	r.inflight++
	r.mu.Unlock()

	// Phase 2: look up handlers and activate outside the lock to avoid blocking
	// Close/Remove during potentially slow network I/O.
	var activated []activatedSub
	var activateErr error
	for _, p := range toActivate {
		h, ok := lookupTriggerForNode(p.nd)
		if !ok {
			activateErr = fmt.Errorf("trigger handler %q not registered", p.nd.Type)
			break
		}
		params, paused, err := r.activationParams(ctx, rec, p.nd)
		if err != nil {
			activateErr = err
			break
		}
		// settings.timezone, already validated when the definition compiled.
		var workflowTZ string
		if rec.Graph != nil {
			workflowTZ = rec.Graph.Timezone()
		} else if rec.Definition.Settings != nil {
			workflowTZ = rec.Definition.Settings.Timezone
		}
		// A paused schedule is hosted with a runtime that drops its firings
		// rather than not hosted at all; see pausedRuntime for why the
		// difference is the whole point of pausing.
		var runtime types.TriggerRuntime = r
		if paused {
			runtime = pausedRuntime{inner: r}
		}
		sub, err := h.Activate(ctx, &types.TriggerActivateInput{
			WorkflowID: rec.ID,
			NodeName:   p.nd.Name,
			Params:     params,
			Runtime:    runtime,
			// The workflow definition's own namespace scopes the supply view: the
			// in-process runtime may host workflows from more than one namespace,
			// and a supply node name is only an identity within one of them.
			Supplies: supply.Default.Decoded(namespace.Namespace(rec.Definition.Namespace)),

			WorkflowTimezone: workflowTZ,
		})
		if err != nil {
			activateErr = err
			break
		}
		activated = append(activated, activatedSub{key: p.key, token: p.token, sub: sub})
	}

	if activateErr != nil {
		// Rollback: drop our reservations and close already-activated subs.
		r.mu.Lock()
		for _, p := range toActivate {
			if tok, ok := r.reservations[p.key]; ok && tok == p.token {
				delete(r.reservations, p.key)
			}
		}
		r.mu.Unlock()
		_ = closeSubscriptions(ctx, detachSubs(activated))
		r.finishInflight()
		return activateErr
	}

	// Phase 3: commit activated subscriptions, but only for reservations that
	// still belong to us. If Close/RemoveWorkflow cleared the reservation (or
	// the runtime is closing), close the freshly-activated sub instead of
	// writing it back — otherwise it would leak and fire ghost triggers.
	r.mu.Lock()
	var toClose []types.TriggerSubscription
	for _, a := range activated {
		tok, ok := r.reservations[a.key]
		if !ok || tok != a.token || r.closed {
			toClose = append(toClose, a.sub)
			continue
		}
		delete(r.reservations, a.key)
		r.subs[a.key] = a.sub
	}
	r.mu.Unlock()

	// Close discarded subs before signaling completion so Close() genuinely
	// waits for in-flight activations to be torn down.
	closeErr := closeSubscriptions(ctx, toClose)
	r.finishInflight()
	return closeErr
}

// activationParams resolves the parameters one trigger node activates with.
//
// Two layers sit on top of the definition here, and their order is load-bearing:
//
//  1. $config/$vars templates are rendered, exactly as the control plane does
//     for a remote-hosted trigger. This path reads the raw WorkflowDef rather
//     than the compiled Graph, so without this call the two deployment modes
//     disagree on what the same workflow means: distributed subscribes to the
//     rendered topic, in-process to the template text.
//  2. The node's schedule overlay, if an operator has written one, is applied
//     over the rendered values. It goes second because an overlay is a literal
//     an operator typed — it is not a template and must never be fed back
//     through $config.
//
// A store read that FAILS fails the activation rather than falling back to the
// definition. That direction is deliberate: the overlays a read would have
// returned include the paused ones, and activating on the strength of a failed
// read is how a pause stops meaning anything. A trigger that is not a schedule,
// and a schedule that is a member of a co-location group (a unit the API never
// writes an overlay for), skip the read entirely.
func (r *triggerRuntime) activationParams(ctx context.Context, rec backend.WorkflowRecord, nd types.NodeDef) (map[string]any, bool, error) {
	params, err := graph.EvaluateActivationParams(rec.Graph, nd.Name, nd.Type, nd.Parameters)
	if err != nil {
		return nil, false, err
	}
	if r.overrides == nil {
		return params, false, nil
	}
	kind, isSchedule := control.ScheduleKindOf(nd.Type)
	if !isSchedule || !control.IsIndependentSchedule(rec.Graph, nd.Name) {
		return params, false, nil
	}
	ns := fafNamespace(rec.Namespace)
	ov, err := r.overrides.GetScheduleOverride(ctx, ns, rec.ID, nd.Name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return params, false, nil
		}
		return nil, false, fmt.Errorf("read schedule override for %s/%s: %w", rec.ID, nd.Name, err)
	}
	// A row that says nothing about pausing does not pause: pausing is the state
	// an operator has to ask for.
	paused := ov != nil && ov.Paused != nil && *ov.Paused
	return control.MergeScheduleOverrideParams(kind, params, ov), paused, nil
}

// ReloadSchedule re-reads one node's overlay and re-points the hosted
// subscription at what it now says. It is what the schedule API calls after a
// mutation, and it is the only path by which an operator's change reaches a
// trigger that is ALREADY hosted — a change that waited for the next activation
// would take effect only on the next restart.
//
// It implements the schedule API's reloader by signature rather than by
// importing the control package's interface: this is the local trigger host,
// and the API's manager is told about it as a callback.
//
// Two cases are answered with nil rather than an error, because neither is the
// host's to act on and neither leaves the operator's change unapplied:
//
//   - the workflow is not hosted by this process (another process hosts it, or
//     it was never registered here) — the overlay is stored, and that host will
//     read it at its next activation;
//   - the node is not a trigger of the hosted version, which is the orphaned
//     overlay the API lists rather than applies.
//
// A subscription that is already being activated is reported as an error
// instead: the caller asked for a reload that did not happen, and the API turns
// that into "stored, not announced" rather than a success it cannot back.
func (r *triggerRuntime) ReloadSchedule(ctx context.Context, ns namespace.Namespace, workflowID types.WorkflowID, node string) error {
	rec, hosted, err := r.hostedWorkflow(ctx, ns, workflowID)
	if err != nil || !hosted {
		return err
	}
	nd, ok := triggerNodeDef(rec.Definition, node)
	if !ok {
		return nil
	}
	params, paused, err := r.activationParams(ctx, rec, nd)
	if err != nil {
		return err
	}
	h, ok := lookupTriggerForNode(nd)
	if !ok {
		return fmt.Errorf("trigger handler %q not registered", nd.Type)
	}

	// Phase 1: take the node's key. The subscription that exists keeps running
	// until phase 3 installs the new one, so a reload that fails to activate
	// leaves the schedule as it was rather than stopping it — the failure an
	// operator can live with is "the change did not take effect yet".
	key := string(workflowID) + "/" + node
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrTriggerRuntimeClosed
	}
	if _, busy := r.reservations[key]; busy {
		r.mu.Unlock()
		return fmt.Errorf("trigger %s is already being activated", key)
	}
	token := r.nextToken
	r.nextToken++
	r.reservations[key] = token
	previous := r.subs[key]
	r.inflight++
	r.mu.Unlock()

	// Phase 2: activate outside the lock, exactly like ReconcileWorkflow, and
	// with the same runtime selection.
	var runtime types.TriggerRuntime = r
	if paused {
		runtime = pausedRuntime{inner: r}
	}
	var workflowTZ string
	if rec.Graph != nil {
		workflowTZ = rec.Graph.Timezone()
	} else if rec.Definition.Settings != nil {
		workflowTZ = rec.Definition.Settings.Timezone
	}
	sub, err := h.Activate(ctx, &types.TriggerActivateInput{
		WorkflowID:       rec.ID,
		NodeName:         node,
		Params:           params,
		Runtime:          runtime,
		Supplies:         supply.Default.Decoded(namespace.Namespace(rec.Definition.Namespace)),
		WorkflowTimezone: workflowTZ,
	})
	if err != nil {
		r.mu.Lock()
		if tok, ok := r.reservations[key]; ok && tok == token {
			delete(r.reservations, key)
		}
		r.mu.Unlock()
		r.finishInflight()
		return err
	}

	// Phase 3: commit, unless the node was removed or the runtime closed while
	// the activation was in flight — in which case the freshly-activated sub is
	// closed instead of written back, exactly as in ReconcileWorkflow.
	r.mu.Lock()
	tok, ok := r.reservations[key]
	discard := !ok || tok != token || r.closed
	if !discard {
		delete(r.reservations, key)
		r.subs[key] = sub
	}
	r.mu.Unlock()
	r.finishInflight()
	if discard {
		return closeSubscriptions(ctx, []types.TriggerSubscription{sub})
	}
	if previous != nil {
		// The old subscription is closed only now: for the moment in between,
		// both are running. The firing identity is the scheduled instant either
		// way (the trigger's own dedup covers it), so the cost of the overlap is
		// a firing that might be admitted twice and then folded, never one that
		// is missed.
		return closeSubscriptions(ctx, []types.TriggerSubscription{previous})
	}
	return nil
}

// hostedWorkflow resolves the workflow this process is hosting, and reports
// ok=false when it is not hosting it at all.
func (r *triggerRuntime) hostedWorkflow(ctx context.Context, ns namespace.Namespace, workflowID types.WorkflowID) (backend.WorkflowRecord, bool, error) {
	if r.eng == nil || r.eng.workflowRegistry == nil {
		return backend.WorkflowRecord{}, false, nil
	}
	rec, err := r.eng.workflowRegistry.GetWorkflow(ctx, workflowID)
	if err != nil {
		if errors.Is(err, backend.ErrWorkflowNotFound) {
			return backend.WorkflowRecord{}, false, nil
		}
		return backend.WorkflowRecord{}, false, err
	}
	if rec.Definition == nil {
		return backend.WorkflowRecord{}, false, nil
	}
	// The namespace is compared, not defaulted away: an id that resolves to
	// another namespace's workflow is a caller that went around the registry's
	// namespace-scoped read, and it is not this host's schedule to reload.
	if fafNamespace(string(ns)) != fafNamespace(rec.Namespace) {
		return backend.WorkflowRecord{}, false, nil
	}
	return rec, true, nil
}

// triggerNodeDef finds one trigger node of a definition.
func triggerNodeDef(def *types.WorkflowDef, name string) (types.NodeDef, bool) {
	if def == nil {
		return types.NodeDef{}, false
	}
	for _, nd := range def.Nodes {
		if nd.Name == name && nd.Kind == types.NodeKindTrigger {
			return nd, true
		}
	}
	return types.NodeDef{}, false
}

// pausedRuntime is the runtime a PAUSED schedule is hosted with: every firing is
// dropped, and nothing else changes.
//
// Pausing by not hosting the trigger at all is the obvious implementation and
// the wrong one. A cron activation decides what to do about the instants that
// went by while nobody hosted it from the watermark it finds in the state store;
// a watermark frozen for the length of a pause turns the whole paused window
// into a catch-up target, so an operator who paused a schedule for a maintenance
// window and resumed it afterwards would have fire_once or fire_all replay
// exactly what they paused. Hosting the schedule with its firings dropped keeps
// the watermark moving — each instant is acknowledged, none is emitted — so a
// resume starts from the next instant and the paused window is never replayed.
// Replaying it is what an explicit backfill is for.
//
// The wrapper deliberately does NOT expose the entry-seed capability a runtime
// may have: a trigger that could admit through the control plane would take that
// path and this wrapper would have nothing to drop.
type pausedRuntime struct {
	inner types.TriggerRuntime
}

func (p pausedRuntime) Emit(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
	return "", nil
}

func (p pausedRuntime) Dedup(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return p.inner.Dedup(ctx, key, ttl)
}

func (p pausedRuntime) TryLock(ctx context.Context, key string, ttl time.Duration) (types.TriggerLock, bool, error) {
	return p.inner.TryLock(ctx, key, ttl)
}

func (p pausedRuntime) State(ctx context.Context, scope string) types.TriggerState {
	return p.inner.State(ctx, scope)
}

// GetExecutionStatus passes the optional capability through: a paused schedule
// makes no admission decisions, but the trigger hosting it still asks, and
// answering "unavailable" here would have it report a capability gap this
// runtime does not have.
func (p pausedRuntime) GetExecutionStatus(ctx context.Context, id types.ExecutionID) (types.ExecutionStatus, bool, error) {
	if reader, ok := p.inner.(types.ExecutionStatusReader); ok {
		return reader.GetExecutionStatus(ctx, id)
	}
	return "", false, types.ErrExecutionStatusUnavailable
}

// emitScheduleInstant fires one instant of a schedule this process hosts. It is
// what the schedule API's backfill calls, and the API reaches it through
// Engine.EmitScheduleInstant.
//
// The firing is built by the node's own trigger kind (types.ScheduledTrigger),
// so the event carries the identity a live firing of that instant would have —
// the instant in the kind's own spelling. The deduplication, though, is the
// BACKFILL's own, and deliberately not the key a live activation claims: that
// key means "this instant has been decided", and it is claimed for the firings
// the live trigger DROPS — while paused, while the overlap policy holds it back,
// or when the workflow's concurrency limit refuses it. A backfill exists to run
// exactly those instants, so sharing the key would answer "duplicate" to every
// instant of a paused window, which is the case the API documents as the reason
// backfill exists.
func (r *triggerRuntime) emitScheduleInstant(ctx context.Context, ns namespace.Namespace, workflowID types.WorkflowID, node string, instant time.Time) (types.ExecutionID, bool, error) {
	rec, hosted, err := r.hostedWorkflow(ctx, ns, workflowID)
	if err != nil {
		return "", false, err
	}
	if !hosted {
		return "", false, fmt.Errorf("backfill %s/%s: workflow is not hosted by this process", workflowID, node)
	}
	nd, ok := triggerNodeDef(rec.Definition, node)
	if !ok {
		return "", false, fmt.Errorf("backfill %s/%s: not a trigger node of this workflow", workflowID, node)
	}
	h, ok := lookupTriggerForNode(nd)
	if !ok {
		return "", false, fmt.Errorf("trigger handler %q not registered", nd.Type)
	}
	builder, ok := h.(types.ScheduledTrigger)
	if !ok {
		return "", false, fmt.Errorf("trigger %q has no schedule to backfill", nd.Type)
	}
	// The activation parameters, overlay included: a node the operator retimed is
	// backfilled against the schedule it is actually running, which is the same
	// schedule the API plans the range from.
	params, _, err := r.activationParams(ctx, rec, nd)
	if err != nil {
		return "", false, err
	}
	event, err := builder.ScheduledEvent(workflowID, node, params, instant)
	if err != nil {
		return "", false, err
	}
	ok, err = r.primitives.Dedup(ctx, backfillDedupKey(workflowID, node, event), backfillDedupTTL)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", true, nil
	}
	id, err := r.Emit(ctx, workflowID, node, event)
	if err != nil {
		return "", false, err
	}
	return id, false, nil
}

// backfillDedupTTL is how long one backfilled instant stays claimed.
//
// A day is longer than any backfill is re-run by hand and short enough that an
// operator who deliberately wants to re-run a range a day later is not silently
// refused: the claim expires and the instants run again. It is not a
// correctness boundary — the executions themselves are the record — but a
// mistake in a range (a typo'd `from`) is worth catching within the same working
// session.
const backfillDedupTTL = 24 * time.Hour

// backfillDedupKey is the deduplicator key one backfilled instant claims. It is
// namespaced apart from the live key (see emitScheduleInstant) so a repeated
// backfill is idempotent per instant without ever standing in for the trigger's
// own decision about that instant.
func backfillDedupKey(workflowID types.WorkflowID, node string, event *types.TriggerEvent) string {
	return "backfill:" + string(workflowID) + ":" + node + ":" + event.ID
}

// finishInflight decrements the in-flight counter and wakes a waiting Close
// once no activations remain.
func (r *triggerRuntime) finishInflight() {
	r.mu.Lock()
	r.inflight--
	if r.inflight == 0 {
		r.idle.Broadcast()
	}
	r.mu.Unlock()
}

func (r *triggerRuntime) RemoveWorkflow(ctx context.Context, workflowID types.WorkflowID) error {
	prefix := string(workflowID) + "/"
	subs := make([]types.TriggerSubscription, 0)
	r.mu.Lock()
	for key, sub := range r.subs {
		if !hasPrefix(key, prefix) {
			continue
		}
		if sub != nil {
			subs = append(subs, sub)
		}
		delete(r.subs, key)
	}
	// Also drop any in-flight reservations for this workflow so a concurrent
	// activation is discarded in phase 3 rather than resurrected.
	for key := range r.reservations {
		if hasPrefix(key, prefix) {
			delete(r.reservations, key)
		}
	}
	r.mu.Unlock()
	return closeSubscriptions(ctx, subs)
}

func (r *triggerRuntime) Close(ctx context.Context) error {
	r.mu.Lock()
	r.closed = true
	// Drop all outstanding reservations so any in-flight activation is
	// discarded (and closed) by its phase 3 instead of committed here.
	for key := range r.reservations {
		delete(r.reservations, key)
	}
	// Wait for in-flight activations to finish tearing down before we collect
	// and close the committed subscriptions.
	for r.inflight > 0 {
		r.idle.Wait()
	}
	subs := make([]types.TriggerSubscription, 0, len(r.subs))
	for key, sub := range r.subs {
		if sub != nil {
			subs = append(subs, sub)
		}
		delete(r.subs, key)
	}
	r.mu.Unlock()
	return closeSubscriptions(ctx, subs)
}

func (r *triggerRuntime) Emit(ctx context.Context, workflowID types.WorkflowID, nodeName string, event *types.TriggerEvent) (types.ExecutionID, error) {
	if event == nil {
		event = &types.TriggerEvent{}
	}
	if event.Time.IsZero() {
		event.Time = time.Now()
	}
	return r.eng.Invoke(ctx, workflowID, Trigger(nodeName), map[string]any{"trigger": event})
}

func (r *triggerRuntime) Dedup(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return r.primitives.Dedup(ctx, key, ttl)
}

func (r *triggerRuntime) TryLock(ctx context.Context, key string, ttl time.Duration) (types.TriggerLock, bool, error) {
	return r.primitives.TryLock(ctx, key, ttl)
}

func (r *triggerRuntime) State(ctx context.Context, scope string) types.TriggerState {
	return r.primitives.State(ctx, scope)
}

// GetExecutionStatus implements the optional types.ExecutionStatusReader
// capability by asking the primitives. That capability is not part of
// backend.TriggerPrimitives, so a provider without one is a normal case rather
// than an error: it is reported as types.ErrExecutionStatusUnavailable, which
// callers treat exactly like an absent capability. Reporting it here means the
// degrade decision lives with the trigger that has the safe default, not in
// every provider.
func (r *triggerRuntime) GetExecutionStatus(ctx context.Context, id types.ExecutionID) (types.ExecutionStatus, bool, error) {
	reader, ok := r.primitives.(types.ExecutionStatusReader)
	if !ok {
		return "", false, types.ErrExecutionStatusUnavailable
	}
	return reader.GetExecutionStatus(ctx, id)
}

func (r *triggerRuntime) Webhooks() types.WebhookRuntime { return r.webhooks }

// hasPrefix reports whether key starts with prefix.
func hasPrefix(key, prefix string) bool {
	return len(key) >= len(prefix) && key[:len(prefix)] == prefix
}

// lookupTriggerForNode resolves the trigger handler honoring NodeDef.Version
// when set. Falls back to the latest registered handler when no version is
// pinned. Returns false if no handler is registered at all so the caller can
// report a registration error.
func lookupTriggerForNode(nd types.NodeDef) (types.TriggerHandler, bool) {
	if nd.Version > 0 {
		if h, ok := registry.LookupTriggerVersion(nd.Type, nd.Version); ok {
			return h, true
		}
	}
	return registry.LookupTrigger(nd.Type)
}

func closeSubscriptions(ctx context.Context, subs []types.TriggerSubscription) error {
	var closeErr error
	for _, sub := range subs {
		if err := sub.Close(ctx); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}

func detachSubs(activated []activatedSub) []types.TriggerSubscription {
	if len(activated) == 0 {
		return nil
	}
	out := make([]types.TriggerSubscription, 0, len(activated))
	for i := len(activated) - 1; i >= 0; i-- {
		out = append(out, activated[i].sub)
	}
	return out
}
