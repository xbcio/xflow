package rstate

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xbcio/xflow/backend/tenant"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

func doneChannel(t tenant.TenantID, id types.ExecutionID) string {
	return fmt.Sprintf("xflow:t%s:exec:{%s}:done", t, id)
}

func (s *Store) PublishExecutionEvent(ctx context.Context, event engine.ExecutionEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal execution event: %w", err)
	}
	t := tenant.FromContext(ctx)
	return s.rdb.Publish(ctx, doneChannel(t, event.ExecutionID), data).Err()
}

func (s *Store) WatchExecution(ctx context.Context, id types.ExecutionID) (<-chan engine.ExecutionEvent, error) {
	t := tenant.FromContext(ctx)
	pubsub := s.rdb.Subscribe(ctx, doneChannel(t, id))
	if _, err := pubsub.Receive(ctx); err != nil {
		_ = pubsub.Close()
		return nil, fmt.Errorf("subscribe execution events %q: %w", id, err)
	}
	out := make(chan engine.ExecutionEvent, 8)
	go func() {
		defer close(out)
		defer func() { _ = pubsub.Close() }()
		ch := pubsub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				var event engine.ExecutionEvent
				if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
					continue
				}
				select {
				case out <- event:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

// ---------------------------------------------------------------------------
// Sub-execution support
// ---------------------------------------------------------------------------
