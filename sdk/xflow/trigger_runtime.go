package xflow

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/nodes/node"
	"github.com/xbcio/xflow/types"
)

type triggerRuntime struct {
	eng        *Engine
	primitives backend.TriggerPrimitives
	webhooks   *webhookRuntime
	mu         sync.Mutex
	subs       map[string]types.TriggerSubscription
}

type activatedSub struct {
	key string
	sub types.TriggerSubscription
}

func newTriggerRuntime(e *Engine, p backend.TriggerPrimitives) *triggerRuntime {
	return &triggerRuntime{eng: e, primitives: p, webhooks: newWebhookRuntime(), subs: make(map[string]types.TriggerSubscription)}
}

func (r *triggerRuntime) ReconcileWorkflow(ctx context.Context, rec backend.WorkflowRecord) error {
	var activated []activatedSub

	r.mu.Lock()
	for _, nd := range rec.Definition.Nodes {
		if nd.Kind != types.NodeKindTrigger {
			continue
		}
		h, ok := lookupTriggerForNode(nd)
		if !ok {
			rollback := detachActivatedSubscriptions(r.subs, activated)
			r.mu.Unlock()
			_ = closeSubscriptions(ctx, rollback)
			return fmt.Errorf("trigger handler %q not registered", nd.Type)
		}
		key := string(rec.ID) + "/" + nd.Name
		_, exists := r.subs[key]
		if exists {
			continue
		}
		sub, err := h.Activate(ctx, &types.TriggerActivateInput{
			WorkflowID: rec.ID,
			NodeName:   nd.Name,
			Params:     nd.Parameters,
			Runtime:    r,
		})
		if err != nil {
			rollback := detachActivatedSubscriptions(r.subs, activated)
			r.mu.Unlock()
			_ = closeSubscriptions(ctx, rollback)
			return err
		}
		r.subs[key] = sub
		activated = append(activated, activatedSub{key: key, sub: sub})
	}
	r.mu.Unlock()
	return nil
}

func (r *triggerRuntime) RemoveWorkflow(ctx context.Context, workflowID types.WorkflowID) error {
	prefix := string(workflowID) + "/"
	subs := make([]types.TriggerSubscription, 0)
	r.mu.Lock()
	for key, sub := range r.subs {
		if len(key) < len(prefix) || key[:len(prefix)] != prefix {
			continue
		}
		subs = append(subs, sub)
		delete(r.subs, key)
	}
	r.mu.Unlock()
	return closeSubscriptions(ctx, subs)
}

func (r *triggerRuntime) Close(ctx context.Context) error {
	subs := make([]types.TriggerSubscription, 0, len(r.subs))
	r.mu.Lock()
	for key, sub := range r.subs {
		subs = append(subs, sub)
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

// lookupTriggerForNode resolves the trigger handler honoring NodeDef.Version
// when set. Falls back to the latest registered handler when no version is
// pinned. Returns false if no handler is registered at all so the caller can
// report a registration error.
func lookupTriggerForNode(nd types.NodeDef) (types.TriggerHandler, bool) {
	if nd.Version > 0 {
		if h, ok := node.LookupTriggerVersion(nd.Type, nd.Version); ok {
			return h, true
		}
	}
	return node.LookupTrigger(nd.Type)
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

func detachActivatedSubscriptions(subs map[string]types.TriggerSubscription, activated []activatedSub) []types.TriggerSubscription {
	if len(activated) == 0 {
		return nil
	}
	detached := make([]types.TriggerSubscription, 0, len(activated))
	for i := len(activated) - 1; i >= 0; i-- {
		delete(subs, activated[i].key)
		detached = append(detached, activated[i].sub)
	}
	return detached
}
