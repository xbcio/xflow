package rstate

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// writeTransientSubExecution binds a lazily-created subs hash to the parent
// status key's remaining TTL. It deliberately never renews an existing subs
// hash: transient TTL is an active window beginning at execution creation,
// not a sliding window driven by child completion.
//
// KEYS[1] is the subs hash and KEYS[2] is the parent status key. A missing or
// non-expiring parent status has no valid transient lifetime, so the script
// removes the subs key rather than creating an immortal orphan.
var writeTransientSubExecutionLua = redis.NewScript(`
local parentTTL = redis.call('PTTL', KEYS[2])
if parentTTL <= 0 then
    redis.call('DEL', KEYS[1])
    return 0
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
if redis.call('PTTL', KEYS[1]) < 0 then
    redis.call('PEXPIRE', KEYS[1], parentTTL)
end
return 1
`)

func (s *Store) writeTransientSubExecution(ctx context.Context, key, statusKey string, childID types.ExecutionID, data string) (bool, error) {
	result, err := writeTransientSubExecutionLua.Run(ctx, s.rdb, []string{key, statusKey}, string(childID), data).Int64()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (s *Store) CreateSubExecution(ctx context.Context, sub *engine.SubExecution) error {
	t := namespace.FromContext(ctx)
	key := subExecutionKey(t, sub.ParentExecID, sub.ParentNode)
	data, err := json.Marshal(sub)
	if err != nil {
		return fmt.Errorf("marshal sub-execution %q/%q: %w", sub.ParentExecID, sub.ParentNode, err)
	}
	if s.isTransient(ctx, sub.ParentExecID) {
		written, err := s.writeTransientSubExecution(ctx, key, execKey(t, sub.ParentExecID, "status"), sub.ChildExecID, string(data))
		if err != nil {
			return fmt.Errorf("write transient sub-execution %q/%q: %w", sub.ParentExecID, sub.ParentNode, err)
		}
		if !written {
			return nil
		}
		return nil
	}
	if err := s.rdb.HSet(ctx, key, string(sub.ChildExecID), data).Err(); err != nil {
		return err
	}
	if err := s.rdb.Expire(ctx, key, s.getExecTTL(ctx, sub.ParentExecID)).Err(); err != nil {
		return fmt.Errorf("set sub-execution ttl %q/%q: %w", sub.ParentExecID, sub.ParentNode, err)
	}
	return s.refreshTransientTTL(ctx, sub.ParentExecID, key)
}

func (s *Store) CompleteSubExecution(ctx context.Context, parentExecID types.ExecutionID, parentNode string, childExecID types.ExecutionID, status types.ExecutionStatus, result map[string]any) (bool, error) {
	t := namespace.FromContext(ctx)
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
	if s.isTransient(ctx, parentExecID) {
		written, err := s.writeTransientSubExecution(ctx, key, execKey(t, parentExecID, "status"), childExecID, string(data))
		if err != nil {
			return false, fmt.Errorf("write transient sub-execution %q/%q: %w", parentExecID, parentNode, err)
		}
		if !written {
			return false, nil
		}
	} else {
		if err := s.rdb.HSet(ctx, key, string(childExecID), data).Err(); err != nil {
			return false, err
		}
		if err := s.rdb.Expire(ctx, key, s.getExecTTL(ctx, parentExecID)).Err(); err != nil {
			return false, fmt.Errorf("set sub-execution ttl %q/%q: %w", parentExecID, parentNode, err)
		}
		if err := s.refreshTransientTTL(ctx, parentExecID, key); err != nil {
			return false, err
		}
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
	t := namespace.FromContext(ctx)
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
