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
var _ engine.ArrivalFoldingCommitter = (*Store)(nil)

// FoldsArrivalsIntoCommit declares the arrival-folding capability: CommitNode
// applies CommitNodeRequest.Arrivals inside commitNodeLua, in the same fenced
// Redis transition as the terminal node write. Without this explicit marker the
// engine treats the store as unable to fold — CommitNode's signature alone is
// not a capability signal — and keeps emitting AdvanceTask, so the declaration
// has to stay here for as long as the Lua transition honours the field.
func (s *Store) FoldsArrivalsIntoCommit() bool { return true }

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
		// KEYS[11]: the scheduling marker the system/skip guard resolves. It is
		// unit-indexed, matching what AdvanceNode wrote (in-degree, active-input
		// and schedule are all one counter space). NodeIdx is not a substitute --
		// it is shifted by every declaration-only node the definition declares,
		// so reading it here resolved a neighbouring unit's marker, refused the
		// skip, and left the execution running with every executed node reporting
		// success.
		scheduleKey(t, req.ExecutionID, req.UnitIdx),
		// KEYS[12]: re-EXPIREd by the script so the per-execution mode marker
		// (transient or durable) cannot lapse while the execution is still
		// committing. See the note in commitNodeLua — a lapsed transient marker
		// makes a transient execution read as durable, which projects its node
		// output into SQL.
		transientMarkKey(t, req.ExecutionID),
	}
	// KEYS[13...] are source outputs that the engine proved have no remaining
	// consumers. They are deleted by commitNodeLua only after the commit fence
	// accepts this terminal transition.
	keys = append(keys, reclaimKeys...)
	// Folded arrivals follow the reclaim keys: three counter keys per arrival,
	// in the same order AdvanceNode builds them. Their ARGV block follows the
	// cyclic suffix, then the arrival count, and only then the three trailing
	// arguments commitNodeLua reads from the END. Getting either base wrong
	// silently mis-decodes a counter key or a slot, so both paths share
	// appendArrivalArgs.
	arrivalIDs := make([]string, 0, len(req.Arrivals)*2)
	for _, arrival := range req.Arrivals {
		var executeID, skipID string
		var err error
		keys, args, executeID, skipID, err = appendArrivalArgs(keys, args, t,
			req.ExecutionID, req.ActivationID, req.AutoDepth, arrival)
		if err != nil {
			return engine.CommitNodeResult{}, err
		}
		arrivalIDs = append(arrivalIDs, executeID, skipID)
	}
	args = append(args, len(req.Arrivals))
	// Three trailing arguments live after the variable-length cyclic outbox and
	// arrival suffixes, and commitNodeLua reads them from the END of ARGV so
	// neither the reclaim count nor the privacy/error-detail fields can perturb
	// either fixed base index. Their order is the contract: reclaim count,
	// privacy bit, then the node's structured error detail.
	args = append(args, len(reclaimKeys), privateOutput, errorDetailsJSON)
	result, err := commitNodeLua.Run(ctx, s.rdb, keys, args...).Slice()
	if err != nil {
		return engine.CommitNodeResult{}, fmt.Errorf("commit node %q/%q: %w", req.ExecutionID, req.NodeName, err)
	}
	// Four elements on every early return; five on the accepted path, whose last
	// element is the skip list. A trailing empty Lua table may collapse on the
	// wire, which is why a four-element accepted reply is legal too.
	if len(result) != 4 && len(result) != 5 {
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
		if len(result) == 5 {
			// The skip list is the fifth element of the accepted reply. An empty
			// Lua table can collapse on the wire, which is why the shape check
			// above accepts four elements too; skippedUnitsFromLua treats a
			// non-list as "no skips" either way.
			out.Skipped = skippedUnitsFromLua(result[4])
		}
		if !out.ExecutionDone && !req.Fatal {
			if advanceID != "" {
				out.OutboxIDs = append(out.OutboxIDs, advanceID)
			}
			for _, entry := range req.CyclicOutbox {
				out.OutboxIDs = append(out.OutboxIDs, entry.ID)
			}
			// Both candidate intents per arrival: only the script knows which
			// of the two it wrote, so OutboxIDs is a candidate hint rather
			// than a write log (see engine.CommitNodeResult.OutboxIDs). The
			// memory backend reports the same candidates, including when its
			// canceling guard suppressed the fold, so the two agree.
			out.OutboxIDs = append(out.OutboxIDs, arrivalIDs...)
		}
	case 2:
		out.Outcome = engine.CommitOutcomeDuplicateTerminal
	case 3:
		out.Outcome = engine.CommitOutcomeExecutionInactive
	default:
		return engine.CommitNodeResult{}, fmt.Errorf("commit node %q/%q: unknown outcome %d", req.ExecutionID, req.NodeName, code)
	}
	if out.Applied && len(out.OutboxIDs) > 0 {
		// Register only a commit that carries delivery intent. OutboxIDs is the
		// candidate hint for exactly the transitions that can append one — the
		// advance intent, the cyclic intents, and the two candidates per folded
		// arrival — and it is empty for the commits that cannot: one that
		// finalizes the execution, a fatal one (both suppress the outbox branches
		// inside commitNodeLua), and a leaf commit with no outgoing edge (no
		// advance task and no arrivals). Those used to pay for a registration
		// that could never name work.
		//
		// The hint is conservative rather than exact, and that is the harmless
		// direction: a candidate the script did not write still marks. The
		// canceling guard suppressing a folded arrival is the case — the commit
		// reports both candidates anyway, so this writes one member whose next
		// claim finds nothing and re-arms or prunes it. The direction that would
		// lose work cannot happen, because every branch in commitNodeLua that
		// writes an entry is guarded by done/fatal — the same two conditions
		// that decide whether OutboxIDs is populated — so a written entry always
		// has a candidate. See engine.CommitNodeResult.OutboxIDs: a non-empty
		// slice is a conservative "a flush may have work" signal, the empty one
		// says nothing about the durable outbox.
		//
		// Not marking does not hide the backlog a commit leaves behind: entries
		// appended by an earlier transition were registered by that transition,
		// and a registration lost for any other reason is rediscovered by the
		// OutboxDispatcher's keyspace sweep, the documented fallback in
		// state_outbox_index.go.
		s.markOutboxReadyIndex(ctx, t, req.ExecutionID)
	}
	if out.ExecutionDone {
		// commitNodeLua finalizes the status inside the script, so this is the
		// only place its updated_at can be stamped outside UpdateExecutionStatus.
		s.touchExecutionUpdatedAt(ctx, req.ExecutionID)
		// Apply the completion TTL HERE, not only in UpdateExecutionStatus:
		// commitNodeLua finalizes the execution inside the script, so a terminal
		// CommitNode never passes through that method. Omitting it leaves every
		// finished execution's output at its full active TTL.
		//
		// Order matters — before evictExecutionCaches, which drops the cached
		// transient decision this needs. The terminal marker goes in the same
		// transient window, right after shortening has resolved that decision.
		s.shortenTransientCompletionTTLBestEffort(ctx, req.ExecutionID)
		// Separate pass, same window: this is the transition that finalizes the
		// execution for every transient group that is not the last unit itself.
		// It is not gated on isTransient — the group's mode is the workflow
		// author's declaration and applies inside a durable workflow too.
		s.shortenTransientGroupCompletionTTLBestEffort(ctx, req.ExecutionID)
		s.markExecutionTerminalBestEffort(ctx, req.ExecutionID, out.ExecutionStatus)
		s.evictExecutionCaches(req.ExecutionID)
		s.releaseWorkflowSlotAfterTerminal(ctx, req.ExecutionID, out.ExecutionStatus)
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
		// The terminal commit is the node's end line. started_at was written by
		// the acquire projection; the column-level COALESCE keeps it.
		if types.IsTerminalNodeStatus(req.Status) {
			finishedAt := time.Now().UTC()
			rec.FinishedAt = &finishedAt
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

// appendArrivalArgs appends one destination's three scheduling-counter keys
// (in-degree, active inputs, schedule) and its eight ARGV slots, in the exact
// order commitNodeLua and advanceNodeLua read them.
//
// It returns the two candidate outbox IDs. The script writes exactly one of
// them, depending on the counters it observes; which one is derivable only in
// Lua, which is why OutboxIDs reports both and Skipped reports the answer.
//
// Both commit paths share this builder because both scripts read ONE layout. A
// slot order that drifted between them — or between a Go writer and its Lua
// reader — would not fail to run: it would decode an arrival count as an active
// count, or a body as an ID, and corrupt the scheduling counters silently.
//
// The destination node name rides along in the eighth slot, read only by the
// skip branch: the script names the units it skipped so the caller does not
// have to re-derive them from a count. Nothing replays an advance intent out of
// a durable body written by an older revision — engine.LeaseNotRecoverable
// exists precisely because advance and skip tasks never leave the engine — so
// the slot cannot be absent for a body this binary produced.
func appendArrivalArgs(keys []string, args []any, t namespace.Namespace, id types.ExecutionID, activationID, autoDepth int, arrival engine.DownstreamArrival) ([]string, []any, string, string, error) {
	keys = append(keys,
		inDegreeKey(t, id, arrival.UnitIdx),
		activeInputsKey(t, id, arrival.UnitIdx),
		scheduleKey(t, id, arrival.UnitIdx),
	)
	execTaskType := arrival.ExecTaskType
	if execTaskType == 0 {
		execTaskType = engine.TaskTypeNodeExec
	}
	executeID := redisExecuteOutboxID(id, arrival.NodeName, activationID)
	skipID := redisSkipOutboxID(id, arrival.NodeName, activationID)
	executeJSON, err := marshalRedisOutboxEntry(executeID, engine.Task{
		ExecutionID:  id,
		NodeName:     arrival.NodeName,
		NodeIdx:      arrival.NodeIdx,
		UnitIdx:      arrival.UnitIdx,
		Type:         execTaskType,
		ActivationID: activationID,
		AutoDepth:    autoDepth,
	}, time.Time{})
	if err != nil {
		return nil, nil, "", "", err
	}
	skipJSON, err := marshalRedisOutboxEntry(skipID, engine.Task{
		ExecutionID:  id,
		NodeName:     arrival.NodeName,
		NodeIdx:      arrival.NodeIdx,
		UnitIdx:      arrival.UnitIdx,
		Type:         engine.TaskTypeNodeSkip,
		ActivationID: activationID,
		AutoDepth:    autoDepth,
	}, time.Time{})
	if err != nil {
		return nil, nil, "", "", err
	}
	args = append(args, arrival.ArrivalCount, arrival.ActiveCount, arrival.MergeMode, executeID, executeJSON, skipID, skipJSON, arrival.NodeName)
	return keys, args, executeID, skipID, nil
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
	outboxIDs := make([]string, 0, len(req.Arrivals)*2)
	for _, arrival := range req.Arrivals {
		var executeID, skipID string
		var err error
		keys, args, executeID, skipID, err = appendArrivalArgs(keys, args, t,
			req.ExecutionID, req.ActivationID, req.AutoDepth, arrival)
		if err != nil {
			return engine.AdvanceNodeResult{}, err
		}
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
