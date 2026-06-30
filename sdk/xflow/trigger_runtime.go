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
	mu         sync.Mutex
	subs       map[string]types.TriggerSubscription
}

func newTriggerRuntime(e *Engine, p backend.TriggerPrimitives) *triggerRuntime {
	return &triggerRuntime{eng: e, primitives: p, subs: make(map[string]types.TriggerSubscription)}
}

func (r *triggerRuntime) ReconcileWorkflow(ctx context.Context, rec backend.WorkflowRecord) error {
	for _, nd := range rec.Definition.Nodes {
		if nd.Kind != types.NodeKindTrigger {
			continue
		}
		h, ok := node.LookupTrigger(nd.Type)
		if !ok {
			return fmt.Errorf("trigger handler %q not registered", nd.Type)
		}
		key := string(rec.ID) + "/" + nd.Name
		r.mu.Lock()
		_, exists := r.subs[key]
		r.mu.Unlock()
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
			return err
		}
		r.mu.Lock()
		r.subs[key] = sub
		r.mu.Unlock()
	}
	return nil
}

func (r *triggerRuntime) RemoveWorkflow(ctx context.Context, workflowID types.WorkflowID) error {
	prefix := string(workflowID) + "/"
	var closeErr error
	r.mu.Lock()
	for key, sub := range r.subs {
		if len(key) < len(prefix) || key[:len(prefix)] != prefix {
			continue
		}
		if err := sub.Close(ctx); err != nil && closeErr == nil {
			closeErr = err
		}
		delete(r.subs, key)
	}
	r.mu.Unlock()
	return closeErr
}

func (r *triggerRuntime) Close(ctx context.Context) error {
	var closeErr error
	r.mu.Lock()
	for key, sub := range r.subs {
		if err := sub.Close(ctx); err != nil && closeErr == nil {
			closeErr = err
		}
		delete(r.subs, key)
	}
	r.mu.Unlock()
	return closeErr
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
