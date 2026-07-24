package rstate

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xbcio/xflow/backend/tenant"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

func (s *Store) CreateSubExecution(ctx context.Context, sub *engine.SubExecution) error {
	t := tenant.FromContext(ctx)
	key := subExecutionKey(t, sub.ParentExecID, sub.ParentNode)
	data, err := json.Marshal(sub)
	if err != nil {
		return fmt.Errorf("marshal sub-execution %q/%q: %w", sub.ParentExecID, sub.ParentNode, err)
	}
	if err := s.rdb.HSet(ctx, key, string(sub.ChildExecID), data).Err(); err != nil {
		return err
	}
	// The subs hash is written lazily via HSet, so it is not covered by the
	// structural-key TTL set at CreateExecution and refreshTransientTTL is a
	// no-op. Set an explicit TTL here or the key leaks permanently after the
	// parent execution's other keys expire.
	if err := s.rdb.Expire(ctx, key, s.getExecTTL(sub.ParentExecID)).Err(); err != nil {
		return fmt.Errorf("set sub-execution ttl %q/%q: %w", sub.ParentExecID, sub.ParentNode, err)
	}
	return s.refreshTransientTTL(ctx, sub.ParentExecID, key)
}

func (s *Store) CompleteSubExecution(ctx context.Context, parentExecID types.ExecutionID, parentNode string, childExecID types.ExecutionID, status types.ExecutionStatus, result map[string]any) (bool, error) {
	t := tenant.FromContext(ctx)
	key := subExecutionKey(t, parentExecID, parentNode)

	sub := &engine.SubExecution{
		ParentExecID: parentExecID,
		ParentNode:   parentNode,
		ChildExecID:  childExecID,
		Status:       status,
		Result:       result,
	}
	data, err := json.Marshal(sub)
	if err != nil {
		return false, fmt.Errorf("marshal sub-execution %q/%q: %w", parentExecID, parentNode, err)
	}
	if err := s.rdb.HSet(ctx, key, string(childExecID), data).Err(); err != nil {
		return false, err
	}
	// Renew the subs hash TTL (see CreateSubExecution): the key is not covered
	// by the structural-key TTL and refreshTransientTTL is a no-op.
	if err := s.rdb.Expire(ctx, key, s.getExecTTL(parentExecID)).Err(); err != nil {
		return false, fmt.Errorf("set sub-execution ttl %q/%q: %w", parentExecID, parentNode, err)
	}
	if err := s.refreshTransientTTL(ctx, parentExecID, key); err != nil {
		return false, err
	}

	// Check if all sub-executions are done.
	all, err := s.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return false, err
	}
	for _, v := range all {
		var entry engine.SubExecution
		if err := json.Unmarshal([]byte(v), &entry); err != nil {
			continue
		}
		if entry.Status == types.ExecutionStatusRunning {
			return false, nil
		}
	}
	return true, nil
}

func (s *Store) GetSubExecutionResults(ctx context.Context, parentExecID types.ExecutionID, parentNode string) ([]map[string]any, error) {
	t := tenant.FromContext(ctx)
	key := subExecutionKey(t, parentExecID, parentNode)
	all, err := s.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	results := make([]map[string]any, 0, len(all))
	for _, v := range all {
		var entry engine.SubExecution
		if err := json.Unmarshal([]byte(v), &entry); err != nil {
			continue
		}
		if entry.Result != nil {
			results = append(results, entry.Result)
		}
	}
	return results, nil
}
