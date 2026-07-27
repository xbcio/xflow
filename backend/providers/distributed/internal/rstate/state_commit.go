package rstate

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

var _ engine.AtomicStateStore = (*Store)(nil)
var _ engine.LegacyNodeCommitter = (*Store)(nil)

// CommitLeasedNode reuses the fenced Redis node transition for legacy cycle
// paths, whose follow-up scheduling remains outside CommitNode's DAG counter.
func (s *Store) CommitLeasedNode(ctx context.Context, req engine.CommitNodeRequest) (engine.CommitNodeResult, error) {
	return s.CommitNode(ctx, req)
}

// ResetNodeForRetryWithOutbox persists the retry reset and delayed delivery
// intent in one Redis Lua transition.
func (s *Store) ResetNodeForRetryWithOutbox(ctx context.Context, id types.ExecutionID, nodeName string, token engine.LeaseToken, entry engine.OutboxEntry) (bool, error) {
	if entry.ID == "" {
		return false, fmt.Errorf("retry outbox for %q/%q has empty ID", id, nodeName)
	}
	encoded, err := marshalRedisOutboxEntry(entry.ID, entry.Task, entry.AvailableAt)
	if err != nil {
		return false, err
	}
	availableAt := time.Now().UTC().UnixMilli()
	if !entry.AvailableAt.IsZero() {
		availableAt = entry.AvailableAt.UTC().UnixMilli()
	}
	ttl := s.getExecTTL(id)
	t := namespace.FromContext(ctx)
	result, err := resetNodeForRetryWithOutboxLua.Run(ctx, s.rdb, []string{
		nodeStatusKey(t, id, nodeName),
		nodeMetaKey(t, id, nodeName),
		leaseExpiryZSetKey(t, id),
		outboxReadyKey(t, id),
		outboxBodyKey(t, id),
	}, string(token), int(ttl.Seconds()), leaseExpiryMember(id, nodeName), entry.ID, encoded, availableAt).Int64()
	if err != nil && err != redis.Nil {
		return false, fmt.Errorf("reset node for retry with outbox %q/%q: %w", id, nodeName, err)
	}
	if result != 1 {
		return false, nil
	}
	if err := s.refreshTransientTTL(ctx, id,
		nodeStatusKey(t, id, nodeName),
		nodeMetaKey(t, id, nodeName),
		leaseExpiryZSetKey(t, id),
		outboxReadyKey(t, id),
		outboxBodyKey(t, id),
	); err != nil {
		return false, err
	}
	return true, nil
}

// RevokeLeaseWithOutbox persists a token-fenced lease release and the exact
// redelivery task in one Redis Lua transition.
func (s *Store) RevokeLeaseWithOutbox(ctx context.Context, id types.ExecutionID, nodeName string, token engine.LeaseToken, entry engine.OutboxEntry) (bool, error) {
	if token == "" {
		return false, nil
	}
	if entry.ID == "" {
		return false, fmt.Errorf("requeue outbox for %q/%q has empty ID", id, nodeName)
	}
	encoded, err := marshalRedisOutboxEntry(entry.ID, entry.Task, entry.AvailableAt)
	if err != nil {
		return false, err
	}
	availableAt := time.Now().UTC().UnixMilli()
	if !entry.AvailableAt.IsZero() {
		availableAt = entry.AvailableAt.UTC().UnixMilli()
	}
	ttl := s.getExecTTL(id)
	t := namespace.FromContext(ctx)
	result, err := revokeLeaseWithOutboxLua.Run(ctx, s.rdb, []string{
		nodeStatusKey(t, id, nodeName),
		nodeMetaKey(t, id, nodeName),
		leaseExpiryZSetKey(t, id),
		outboxReadyKey(t, id),
		outboxBodyKey(t, id),
	}, string(token), int(ttl.Seconds()), leaseExpiryMember(id, nodeName), entry.ID, encoded, availableAt).Int64()
	if err != nil && err != redis.Nil {
		return false, fmt.Errorf("revoke lease with outbox %q/%q: %w", id, nodeName, err)
	}
	if result != 1 {
		return false, nil
	}
	if err := s.refreshTransientTTL(ctx, id,
		nodeStatusKey(t, id, nodeName),
		nodeMetaKey(t, id, nodeName),
		leaseExpiryZSetKey(t, id),
		outboxReadyKey(t, id),
		outboxBodyKey(t, id),
	); err != nil {
		return false, err
	}
	return true, nil
}

// CommitNode implements engine.AtomicStateStore using one fenced Redis Lua
// transition. SQL projection remains best-effort and runs only after Redis has
// accepted the authoritative state.
func (s *Store) CommitNode(ctx context.Context, req engine.CommitNodeRequest) (engine.CommitNodeResult, error) {
	ttl := s.getExecTTL(req.ExecutionID)
	outputJSON := ""
	if req.StoreOutput {
		encoded, err := json.Marshal(req.Output)
		if err != nil {
			return engine.CommitNodeResult{}, fmt.Errorf("marshal output %q/%q: %w", req.ExecutionID, req.NodeName, err)
		}
		outputJSON = string(encoded)
	}
	advanceID := ""
	advanceJSON := ""
	if req.AdvanceTask != nil {
		advanceID = redisAdvanceOutboxID(req.ExecutionID, req.NodeName, req.ActivationID)
		encoded, err := marshalRedisOutboxEntry(advanceID, *req.AdvanceTask, time.Time{})
		if err != nil {
			return engine.CommitNodeResult{}, err
		}
		advanceJSON = encoded
	}
	storeOutput := 0
	if req.StoreOutput {
		storeOutput = 1
	}
	system := 0
	if req.System {
		system = 1
	}
	fatal := 0
	if req.Fatal {
		fatal = 1
	}
	allowCycles := 0
	if g, err := s.LoadGraph(ctx, req.ExecutionID); err == nil && g != nil && g.AllowCycles() {
		allowCycles = 1
	}
	cyclicComplete := 0
	cyclicFinalStatus := ""
	cyclicFinalError := ""
	if req.CyclicComplete {
		cyclicComplete = 1
		cyclicFinalStatus = string(req.CyclicFinalStatus)
		cyclicFinalError = req.CyclicFinalError
	}
	cyclicArgs := make([]any, 0, len(req.CyclicOutbox)*2)
	for _, entry := range req.CyclicOutbox {
		body, err := marshalRedisOutboxEntry(entry.ID, entry.Task, entry.AvailableAt)
		if err != nil {
			return engine.CommitNodeResult{}, err
		}
		cyclicArgs = append(cyclicArgs, entry.ID, body)
	}
	args := []any{
		string(req.Status), string(req.LeaseID), string(req.LeaseToken), req.Attempt,
		req.ActivationID, req.AutoDepth, storeOutput, outputJSON, req.Port, req.Error,
		system, fatal, int(ttl.Seconds()), leaseExpiryMember(req.ExecutionID, req.NodeName),
		advanceID, allowCycles, advanceJSON, time.Now().UTC().UnixMilli(),
		cyclicComplete, cyclicFinalStatus, cyclicFinalError, len(req.CyclicOutbox),
	}
	args = append(args, cyclicArgs...)
	t := namespace.FromContext(ctx)
	result, err := commitNodeLua.Run(ctx, s.rdb, []string{
		execKey(t, req.ExecutionID, "status"),
		execKey(t, req.ExecutionID, "error"),
		remainingNodesKey(t, req.ExecutionID),
		failedNodesKey(t, req.ExecutionID),
		nodeStatusKey(t, req.ExecutionID, req.NodeName),
		nodeMetaKey(t, req.ExecutionID, req.NodeName),
		outputKey(t, req.ExecutionID, req.NodeName),
		leaseExpiryZSetKey(t, req.ExecutionID),
		outboxReadyKey(t, req.ExecutionID),
		outboxBodyKey(t, req.ExecutionID),
		scheduleKey(t, req.ExecutionID, req.NodeIdx),
	}, args...).Slice()
	if err != nil {
		return engine.CommitNodeResult{}, fmt.Errorf("commit node %q/%q: %w", req.ExecutionID, req.NodeName, err)
	}
	if len(result) != 3 {
		return engine.CommitNodeResult{}, fmt.Errorf("commit node %q/%q: unexpected result %v", req.ExecutionID, req.NodeName, result)
	}
	code := redisResultInt(result[0])
	out := engine.CommitNodeResult{}
	switch code {
	case 0:
		out.Outcome = engine.CommitOutcomeStaleToken
	case 1:
		out.Outcome = engine.CommitOutcomeAccepted
		out.Applied = true
		out.ExecutionDone = redisResultInt(result[1]) == 1
		out.ExecutionStatus = types.ExecutionStatus(redisResultString(result[2]))
		if !out.ExecutionDone && !req.Fatal {
			if advanceID != "" {
				out.OutboxIDs = append(out.OutboxIDs, advanceID)
			}
			for _, entry := range req.CyclicOutbox {
				out.OutboxIDs = append(out.OutboxIDs, entry.ID)
			}
		}
	case 2:
		out.Outcome = engine.CommitOutcomeDuplicateTerminal
	case 3:
		out.Outcome = engine.CommitOutcomeExecutionInactive
	default:
		return engine.CommitNodeResult{}, fmt.Errorf("commit node %q/%q: unknown outcome %d", req.ExecutionID, req.NodeName, code)
	}
	if out.ExecutionDone {
		s.evictExecutionCaches(req.ExecutionID)
	}
	if out.Applied && s.db != nil && !s.transient {
		output, _ := json.Marshal(req.Output)
		rec := &store.NodeRecord{
			ExecutionID: req.ExecutionID,
			NodeName:    req.NodeName,
			Status:      req.Status,
			LeaseID:     string(req.LeaseID),
			LeaseToken:  string(req.LeaseToken),
			Attempt:     req.Attempt,
			Output:      output,
			Port:        req.Port,
			UpdatedAt:   time.Now(),
		}
		s.auditWrite(ctx, "commit_node", func(ctx context.Context) error { return s.db.UpsertNode(ctx, rec) })
	}
	return out, nil
}

// AdvanceNode implements engine.AtomicStateStore by atomically applying all
// destination arrivals and creating durable execution/skip intents.
func (s *Store) AdvanceNode(ctx context.Context, req engine.AdvanceNodeRequest) (engine.AdvanceNodeResult, error) {
	if len(req.Arrivals) == 0 {
		return engine.AdvanceNodeResult{Applied: true}, nil
	}
	ttl := s.getExecTTL(req.ExecutionID)
	t := namespace.FromContext(ctx)
	keys := []string{
		execKey(t, req.ExecutionID, "status"),
		nodeStatusKey(t, req.ExecutionID, req.NodeName),
		nodeMetaKey(t, req.ExecutionID, req.NodeName),
		advanceMarkerKey(t, req.ExecutionID, req.NodeName, req.ActivationID),
		outboxReadyKey(t, req.ExecutionID),
		outboxBodyKey(t, req.ExecutionID),
	}
	args := []any{req.ActivationID, int(ttl.Seconds()), len(req.Arrivals), time.Now().UTC().UnixMilli()}
	outboxIDs := make([]string, 0, len(req.Arrivals))
	for _, arrival := range req.Arrivals {
		keys = append(keys,
			inDegreeKey(t, req.ExecutionID, arrival.UnitIdx),
			activeInputsKey(t, req.ExecutionID, arrival.UnitIdx),
			scheduleKey(t, req.ExecutionID, arrival.UnitIdx),
		)
		execTaskType := arrival.ExecTaskType
		if execTaskType == 0 {
			execTaskType = engine.TaskTypeNodeExec
		}
		executeID := redisExecuteOutboxID(req.ExecutionID, arrival.NodeName, req.ActivationID)
		skipID := redisSkipOutboxID(req.ExecutionID, arrival.NodeName, req.ActivationID)
		executeJSON, err := marshalRedisOutboxEntry(executeID, engine.Task{
			ExecutionID:  req.ExecutionID,
			NodeName:     arrival.NodeName,
			NodeIdx:      arrival.NodeIdx,
			UnitIdx:      arrival.UnitIdx,
			Type:         execTaskType,
			ActivationID: req.ActivationID,
			AutoDepth:    req.AutoDepth,
		}, time.Time{})
		if err != nil {
			return engine.AdvanceNodeResult{}, err
		}
		skipJSON, err := marshalRedisOutboxEntry(skipID, engine.Task{
			ExecutionID:  req.ExecutionID,
			NodeName:     arrival.NodeName,
			NodeIdx:      arrival.NodeIdx,
			UnitIdx:      arrival.UnitIdx,
			Type:         engine.TaskTypeNodeSkip,
			ActivationID: req.ActivationID,
			AutoDepth:    req.AutoDepth,
		}, time.Time{})
		if err != nil {
			return engine.AdvanceNodeResult{}, err
		}
		args = append(args, arrival.ArrivalCount, arrival.ActiveCount, arrival.MergeMode, executeID, executeJSON, skipID, skipJSON)
		outboxIDs = append(outboxIDs, executeID, skipID)
	}
	applied, err := advanceNodeLua.Run(ctx, s.rdb, keys, args...).Int64()
	if err != nil {
		return engine.AdvanceNodeResult{}, fmt.Errorf("advance node %q/%q: %w", req.ExecutionID, req.NodeName, err)
	}
	if applied == 0 {
		return engine.AdvanceNodeResult{}, nil
	}
	return engine.AdvanceNodeResult{Applied: true, OutboxIDs: outboxIDs}, nil
}
