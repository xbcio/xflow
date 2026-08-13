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
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/node/supply"
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
	mu         sync.Mutex
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
		// Render $config/$vars templates exactly as the control plane does for
		// a remote-hosted trigger. This path reads the raw WorkflowDef rather
		// than the compiled Graph, so without this call the two deployment
		// modes disagree on what the same workflow means: distributed
		// subscribes to the rendered topic, in-process to the template text.
		params, err := graph.EvaluateActivationParams(rec.Graph, p.nd.Name, p.nd.Type, p.nd.Parameters)
		if err != nil {
			activateErr = err
			break
		}
		sub, err := h.Activate(ctx, &types.TriggerActivateInput{
			WorkflowID: rec.ID,
			NodeName:   p.nd.Name,
			Params:     params,
			Runtime:    r,
			Supplies:   supply.Default.Decoded(),
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
