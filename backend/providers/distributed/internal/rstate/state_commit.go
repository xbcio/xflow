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
	ttl := s.getExecTTL(ctx, id)
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
	s.markOutboxReadyIndex(ctx, t, id)
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
	ttl := s.getExecTTL(ctx, id)
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
	s.markOutboxReadyIndex(ctx, t, id)
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
	if err := req.Validate(); err != nil {
		return engine.CommitNodeResult{}, err
	}
	ttl := s.getExecTTL(ctx, req.ExecutionID)
	outputJSON := ""
	if req.StoreOutput {
		encoded, err := s.encodeOutputValue(req.Output)
		if err != nil {
			return engine.CommitNodeResult{}, fmt.Errorf("marshal output %q/%q: %w", req.ExecutionID, req.NodeName, err)
		}
		outputJSON = encoded
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
	// Empty string rather than an omitted argument: the trailing layout above
	// is positional, so the slot must always be sent. An empty detail is the
	// encoder's "no detail" and decodes back to a nil map.
	errorDetailsJSON := ""
	if len(req.ErrorDetails) > 0 {
		encoded, err := json.Marshal(req.ErrorDetails)
		if err != nil {
			return engine.CommitNodeResult{}, fmt.Errorf("marshal error details %q/%q: %w", req.ExecutionID, req.NodeName, err)
		}
		errorDetailsJSON = string(encoded)
	}
	privateOutput := 0
	if req.PrivateOutput {
		privateOutput = 1
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
	if req.AllowCycles {
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
	reclaimKeys := make([]string, 0, len(req.ReclaimOutputNames))
	for _, name := range req.ReclaimOutputNames {
		reclaimKeys = append(reclaimKeys, outputKey(t, req.ExecutionID, name))
	}
	// Three trailing arguments live after the variable-length cyclic outbox
	// suffix, and commitNodeLua reads them from the END of ARGV so neither the
	// reclaim count nor the privacy/error-detail fields can perturb the suffix's
	// fixed base index. Their order is the contract: reclaim count, privacy bit,
	// then the node's structured error detail.
	args = append(args, len(reclaimKeys), privateOutput, errorDetailsJSON)
	keys := []string{
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
		// KEYS[12]: re-EXPIREd by the script so the per-execution transient
		// marker cannot lapse while the execution is still committing. See the
		// note in commitNodeLua — a lapsed marker makes a transient execution
		// read as durable, which projects its node output into SQL.
		transientMarkKey(t, req.ExecutionID),
	}
	// KEYS[13...] are source outputs that the engine proved have no remaining
	// consumers. They are deleted by commitNodeLua only after the commit fence
	// accepts this terminal transition.
	keys = append(keys, reclaimKeys...)
	result, err := commitNodeLua.Run(ctx, s.rdb, keys, args...).Slice()
	if err != nil {
		return engine.CommitNodeResult{}, fmt.Errorf("commit node %q/%q: %w", req.ExecutionID, req.NodeName, err)
	}
	if len(result) != 4 {
		return engine.CommitNodeResult{}, fmt.Errorf("commit node %q/%q: unexpected result %v", req.ExecutionID, req.NodeName, result)
	}
	effectivePrivate, err := redisResultBit(result[3])
	if err != nil {
		return engine.CommitNodeResult{}, fmt.Errorf("commit node %q/%q: invalid private-output result: %w", req.ExecutionID, req.NodeName, err)
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
	if out.Applied {
		// The commit is what appends the node's advance intent (and any cyclic
		// intents), so it is the transition that makes the execution ready
		// again. An unapplied commit writes no outbox entry and is not marked.
		s.markOutboxReadyIndex(ctx, t, req.ExecutionID)
	}
	if out.ExecutionDone {
		// Apply the completion TTL HERE, not only in UpdateExecutionStatus:
		// commitNodeLua finalizes the execution inside the script, so a terminal
		// CommitNode never passes through that method. Omitting it leaves every
		// finished execution's output at its full active TTL.
		//
		// Order matters — before evictExecutionCaches, which drops the cached
		// transient decision this needs.
		s.shortenTransientCompletionTTLBestEffort(ctx, req.ExecutionID)
		s.evictExecutionCaches(req.ExecutionID)
	}
	// isTransient, not s.transient: req.Output is the node's payload, so a
	// per-workflow transient execution must skip this even when the control
	// plane's global transient mode is off. Same reason as UpsertNode.
	if out.Applied && s.db != nil && !s.isTransient(ctx, req.ExecutionID) {
		// The Lua response supplies the monotonic privacy decision at the same
		// linearization point as the accepted Redis transition. Do not HGET the
		// marker here: it could expire or otherwise change before this SQL
		// projection, turning a private output public.
		var output []byte
		if !effectivePrivate {
			output, _ = json.Marshal(req.Output)
		}
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
		// The execution's terminal transition happens INSIDE commitNodeLua, so
		// this is the only place it can reach the audit trail. Without it the
		// SQL row stays "running" forever, and once the Redis keys expire
		// (DefaultExecTTL) that row is the only surviving record.
		if out.ExecutionDone {
			s.projectExecutionStatus(ctx, req.ExecutionID, out.ExecutionStatus,
				terminalExecutionError(out.ExecutionStatus, req.Error, req.CyclicFinalError))
		}
	}
	return out, nil
}

// AdvanceNode implements engine.AtomicStateStore by atomically applying all
// destination arrivals and creating durable execution/skip intents.
func (s *Store) AdvanceNode(ctx context.Context, req engine.AdvanceNodeRequest) (engine.AdvanceNodeResult, error) {
	// Nothing downstream to schedule, so nothing the Lua script could mutate:
	// with zero arrivals its loop body never runs, and the only remaining work
	// would be the guards and the advance marker. Skipping the round trip is
	// worth it because this is not a rare shape — every acyclic graph ends in at
	// least one node with no outgoing edge, and each produces one such advance
	// per execution.
	//
	// It does cost something: Applied is reported true without consulting the
	// marker, so a redelivered advance for such a node claims Applied twice
	// where the memory backend reports false on the second. That field's only
	// consumer is the optional evidence buffer, never control flow, and the
	// AdvanceNodeResult doc says so. Do not turn this into a dedup signal
	// without deleting this branch first.
	if len(req.Arrivals) == 0 {
		return engine.AdvanceNodeResult{Applied: true}, nil
	}
	ttl := s.getExecTTL(ctx, req.ExecutionID)
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
		// The destination node name rides along in an eighth per-arrival slot,
		// read only by the skip branch: the script names the units it skipped so
		// the caller does not have to re-derive them from a count. Nothing
		// replays an advance intent out of a durable body written by an older
		// revision — engine.LeaseNotRecoverable exists precisely because advance
		// and skip tasks never leave the engine — so the slot cannot be absent
		// for a body this binary produced.
		args = append(args, arrival.ArrivalCount, arrival.ActiveCount, arrival.MergeMode, executeID, executeJSON, skipID, skipJSON, arrival.NodeName)
		outboxIDs = append(outboxIDs, executeID, skipID)
	}
	applied, err := runAdvanceNodeLua(ctx, advanceNodeLua, s.rdb, keys, args)
	if err != nil {
		return engine.AdvanceNodeResult{}, fmt.Errorf("advance node %q/%q: %w", req.ExecutionID, req.NodeName, err)
	}
	if len(applied) == 0 || redisResultInt(applied[0]) == 0 {
		return engine.AdvanceNodeResult{}, nil
	}
	// An applied advance is what schedules the next hop's execute/skip intents,
	// so the execution is ready again the moment this transition lands.
	s.markOutboxReadyIndex(ctx, t, req.ExecutionID)
	out := engine.AdvanceNodeResult{Applied: true, OutboxIDs: outboxIDs}
	out.Skipped = skippedUnitsFromLua(applied[1])
	return out, nil
}

// runAdvanceNodeLua runs advanceNodeLua and normalizes its two legal reply
// shapes into one slice.
//
// The script returns {1, skips} on the applied path, but a Lua table with a
// single trailing element that Redis renders as a scalar — and every early
// `return 0` guard — makes the reply an integer instead. Both are part of the
// same contract, so the caller must not assume a slice: doing so turns a fenced
// or duplicate advance, which is a normal outcome, into an error that the
// caller then reports as a delivery failure.
//
// A scalar reply is treated as {nil, applied}: the number of elements that
// reaches redigo for `{1, {}}` is ambiguous (it decodes as the scalar 1), so the
// payload cannot be distinguished from a missing one, and the applied flag is
// the only thing that must survive.
//
// The script is a parameter so the normalizer can be exercised against a
// literal reply shape rather than only through the production script.
func runAdvanceNodeLua(ctx context.Context, script *redis.Script, rdb redis.UniversalClient, keys []string, args []any) ([]any, error) {
	cmd := script.Run(ctx, rdb, keys, args...)
	res, err := cmd.Slice()
	if err == nil {
		return res, nil
	}
	applied, intErr := cmd.Int64()
	if intErr != nil {
		return nil, err
	}
	return []any{applied}, nil
}

// skippedUnitsFromLua decodes the flat {name, count, name, count, ...} list the
// scheduling scripts return for the units they resolved as skip. A malformed
// trailing element is dropped rather than failing the caller: the transition it
// describes has already been applied by Redis, and no observation is worth
// turning that into an error.
func skippedUnitsFromLua(value any) []engine.SkippedUnit {
	items, ok := value.([]any)
	if !ok || len(items) < 2 {
		return nil
	}
	out := make([]engine.SkippedUnit, 0, len(items)/2)
	for i := 0; i+1 < len(items); i += 2 {
		count := int(redisResultInt(items[i+1]))
		if count <= 0 {
			continue
		}
		out = append(out, engine.SkippedUnit{
			NodeName: redisResultString(items[i]),
			Count:    count,
		})
	}
	return out
}
