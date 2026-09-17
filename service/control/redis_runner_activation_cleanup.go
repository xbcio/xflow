package control

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

var _ DeactivationObligationDirectory = (*RedisRunnerDirectory)(nil)

// EnsureDeactivationObligation persists a pending-fence cleanup intent under
// the same Redis Cluster hash tag as the runner control record. A stale
// reconciler that races a resume gets applicable=false and must not fence the
// activation on behalf of that old drain generation.
func (d *RedisRunnerDirectory) EnsureDeactivationObligation(ctx context.Context, obligation DeactivationObligation) (bool, error) {
	if err := validateDeactivationObligation(obligation); err != nil {
		return false, err
	}
	status, err := d.evalStatus(ctx, redisEnsureDeactivationObligationLua, []string{
		d.keys.runnerSession,
		d.keys.runnerControlDesired,
		d.keys.runnerControlGeneration,
		d.keys.deactivationObligationState,
		d.keys.deactivationObligationRunner,
		d.keys.deactivationObligationSession,
		d.keys.deactivationObligationNamespace,
		d.keys.deactivationObligationWorkflowID,
		d.keys.deactivationObligationWorkflowVersion,
		d.keys.deactivationObligationEntryUnitID,
		d.keys.deactivationObligationReplicaIndex,
		d.keys.deactivationObligationGeneration,
		d.keys.deactivationObligationDrainGeneration,
		d.keys.runnerActivationInventory,
	}, deactivationObligationID(obligation), obligation.RunnerID, obligation.SessionID,
		string(obligation.Namespace), string(obligation.WorkflowID), obligation.WorkflowVersion,
		obligation.EntryUnitID, strconv.FormatUint(uint64(obligation.ReplicaIndex), 10),
		strconv.FormatUint(obligation.Generation, 10), strconv.FormatUint(obligation.DrainGeneration, 10))
	if err != nil {
		return false, fmt.Errorf("ensure redis deactivation obligation: %w", err)
	}
	switch status {
	case "applicable":
		return true, nil
	case "not_applicable":
		return false, nil
	default:
		return false, fmt.Errorf("ensure redis deactivation obligation: unexpected result %q", status)
	}
}

// MarkDeactivationObligationReady publishes a pending intent only after the
// EntryActivationStore fence completed. Repeating it is safe across a process
// crash between Fence and this call.
func (d *RedisRunnerDirectory) MarkDeactivationObligationReady(ctx context.Context, obligation DeactivationObligation) error {
	if err := validateDeactivationObligation(obligation); err != nil {
		return err
	}
	status, err := d.evalStatus(ctx, redisMarkDeactivationObligationReadyLua, []string{
		d.keys.deactivationObligationState,
	}, deactivationObligationID(obligation))
	if err != nil {
		return fmt.Errorf("mark redis deactivation obligation ready: %w", err)
	}
	switch status {
	case "ready", "already_ready":
		return nil
	case "missing":
		return ErrDeactivationObligationNotFound
	default:
		return fmt.Errorf("mark redis deactivation obligation ready: unexpected result %q", status)
	}
}

// CancelPendingDeactivationObligation compensates a failed authority fence. It
// refuses to delete ready work, because a fenced old owner must remain an
// observable cleanup blocker until its receipt arrives.
func (d *RedisRunnerDirectory) CancelPendingDeactivationObligation(ctx context.Context, obligation DeactivationObligation) error {
	if err := validateDeactivationObligation(obligation); err != nil {
		return err
	}
	status, err := d.evalStatus(ctx, redisCancelPendingDeactivationObligationLua, d.deactivationObligationKeys(), deactivationObligationID(obligation))
	if err != nil {
		return fmt.Errorf("cancel redis pending deactivation obligation: %w", err)
	}
	if status != "cancelled" && status != "noop" {
		return fmt.Errorf("cancel redis pending deactivation obligation: unexpected result %q", status)
	}
	return nil
}

// PendingDeactivationObligations reads every unfinished cleanup record. These
// fields are persistent, so the leader can resume a crash after intent creation
// without relying on an in-memory directive queue.
func (d *RedisRunnerDirectory) PendingDeactivationObligations(ctx context.Context) ([]DeactivationObligation, error) {
	pipe := d.rdb.Pipeline()
	states := pipe.HGetAll(ctx, d.keys.deactivationObligationState)
	runners := pipe.HGetAll(ctx, d.keys.deactivationObligationRunner)
	sessions := pipe.HGetAll(ctx, d.keys.deactivationObligationSession)
	namespaces := pipe.HGetAll(ctx, d.keys.deactivationObligationNamespace)
	workflowIDs := pipe.HGetAll(ctx, d.keys.deactivationObligationWorkflowID)
	workflowVersions := pipe.HGetAll(ctx, d.keys.deactivationObligationWorkflowVersion)
	entryUnitIDs := pipe.HGetAll(ctx, d.keys.deactivationObligationEntryUnitID)
	replicas := pipe.HGetAll(ctx, d.keys.deactivationObligationReplicaIndex)
	generations := pipe.HGetAll(ctx, d.keys.deactivationObligationGeneration)
	drainGenerations := pipe.HGetAll(ctx, d.keys.deactivationObligationDrainGeneration)
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("read redis deactivation obligations: %w", err)
	}
	return decodeRedisDeactivationObligations(
		states.Val(), runners.Val(), sessions.Val(), namespaces.Val(), workflowIDs.Val(),
		workflowVersions.Val(), entryUnitIDs.Val(), replicas.Val(), generations.Val(), drainGenerations.Val(),
	)
}

// RebindDeactivationObligations is also invoked after Register as a defensive
// retry. Register itself includes the same transition, so a control-plane crash
// between those two calls cannot lose the runner's reconnect proof.
func (d *RedisRunnerDirectory) RebindDeactivationObligations(ctx context.Context, runnerID, sessionID string, reported []protocol.ActivationInventoryItem) error {
	inventory, err := marshalRedisActivationInventory(reported)
	if err != nil {
		return err
	}
	status, err := d.evalStatus(ctx, redisRebindDeactivationObligationsLua, []string{
		d.keys.runnerSession,
		d.keys.deactivationObligationRunner,
		d.keys.deactivationObligationSession,
		d.keys.deactivationObligationWorkflowID,
		d.keys.deactivationObligationWorkflowVersion,
		d.keys.deactivationObligationEntryUnitID,
		d.keys.deactivationObligationReplicaIndex,
		d.keys.deactivationObligationGeneration,
		d.keys.runnerActivationInventory,
	}, runnerID, sessionID, inventory)
	if err != nil {
		return fmt.Errorf("rebind redis deactivation obligations: %w", err)
	}
	switch status {
	case "rebound":
		return nil
	case "not_found":
		return ErrRunnerNotFound
	case "stale":
		return ErrRunnerSessionStale
	default:
		return fmt.Errorf("rebind redis deactivation obligations: unexpected result %q", status)
	}
}

// DeactivationDirectives derives every retryable directive for the current
// session from durable ready obligations. It deliberately does not mutate those
// obligations: a lost heartbeat response is indistinguishable from a runner
// that has not acted yet, so both must retry until a receipt arrives.
func (d *RedisRunnerDirectory) DeactivationDirectives(ctx context.Context, runnerID, sessionID string) ([]protocol.DeactivateDirective, error) {
	current, err := d.rdb.HGet(ctx, d.keys.runnerSession, runnerID).Result()
	if errors.Is(err, redis.Nil) || current == "" {
		return nil, ErrRunnerNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read redis runner session for deactivation directives: %w", err)
	}
	if current != sessionID {
		return nil, ErrRunnerSessionStale
	}
	obligations, err := d.PendingDeactivationObligations(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]protocol.DeactivateDirective, 0)
	for _, obligation := range obligations {
		if obligation.RunnerID == runnerID && obligation.SessionID == sessionID && obligation.State == DeactivationObligationReady {
			out = append(out, obligation.Directive())
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		if out[i].WorkflowID != out[j].WorkflowID {
			return out[i].WorkflowID < out[j].WorkflowID
		}
		if out[i].WorkflowVersion != out[j].WorkflowVersion {
			return out[i].WorkflowVersion < out[j].WorkflowVersion
		}
		if out[i].EntryUnitID != out[j].EntryUnitID {
			return out[i].EntryUnitID < out[j].EntryUnitID
		}
		return out[i].ReplicaIndex < out[j].ReplicaIndex
	})
	return out, nil
}

// AcknowledgeDeactivation atomically fences the current runner session and
// deletes the matched ready obligation. Stale/duplicate receipts are harmless
// no-ops; they must not clear cleanup owned by a replacement session.
func (d *RedisRunnerDirectory) AcknowledgeDeactivation(ctx context.Context, ack protocol.ActivationAck) (bool, error) {
	if ack.Status != protocol.ActivationStatusDeactivated || ack.RunnerID == "" || ack.SessionID == "" || ack.WorkflowVersion == "" {
		return false, ErrInvalidDeactivationReceipt
	}
	obligation := deactivationObligationFromAck(namespace.FromContext(ctx), ack)
	status, err := d.evalStatus(ctx, redisAcknowledgeDeactivationLua, append([]string{d.keys.runnerSession}, d.deactivationObligationKeys()...),
		deactivationObligationID(obligation), ack.RunnerID, ack.SessionID)
	if err != nil {
		return false, fmt.Errorf("acknowledge redis deactivation: %w", err)
	}
	switch status {
	case "acknowledged":
		return true, nil
	case "noop":
		return false, nil
	default:
		return false, fmt.Errorf("acknowledge redis deactivation: unexpected result %q", status)
	}
}

func (d *RedisRunnerDirectory) deactivationObligationKeys() []string {
	return []string{
		d.keys.deactivationObligationState,
		d.keys.deactivationObligationRunner,
		d.keys.deactivationObligationSession,
		d.keys.deactivationObligationNamespace,
		d.keys.deactivationObligationWorkflowID,
		d.keys.deactivationObligationWorkflowVersion,
		d.keys.deactivationObligationEntryUnitID,
		d.keys.deactivationObligationReplicaIndex,
		d.keys.deactivationObligationGeneration,
		d.keys.deactivationObligationDrainGeneration,
	}
}

func decodeRedisDeactivationObligations(states, runners, sessions, namespaces, workflowIDs, workflowVersions, entryUnitIDs, replicas, generations, drainGenerations map[string]string) ([]DeactivationObligation, error) {
	ids := make([]string, 0, len(states))
	for id := range states {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]DeactivationObligation, 0, len(ids))
	for _, id := range ids {
		replica, err := strconv.ParseUint(replicas[id], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("decode redis deactivation obligation %q replica index: %w", id, err)
		}
		generation, err := strconv.ParseUint(generations[id], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("decode redis deactivation obligation %q generation: %w", id, err)
		}
		drainGeneration, err := strconv.ParseUint(drainGenerations[id], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("decode redis deactivation obligation %q drain generation: %w", id, err)
		}
		obligation := DeactivationObligation{
			RunnerID:        runners[id],
			SessionID:       sessions[id],
			Namespace:       namespace.Namespace(namespaces[id]),
			WorkflowID:      types.WorkflowID(workflowIDs[id]),
			WorkflowVersion: workflowVersions[id],
			EntryUnitID:     entryUnitIDs[id],
			ReplicaIndex:    uint32(replica),
			Generation:      generation,
			DrainGeneration: drainGeneration,
			State:           DeactivationObligationState(states[id]),
		}
		if err := validateDeactivationObligation(obligation); err != nil {
			return nil, fmt.Errorf("decode redis deactivation obligation %q: %w", id, err)
		}
		if obligation.State != DeactivationObligationPendingFence && obligation.State != DeactivationObligationReady {
			return nil, fmt.Errorf("decode redis deactivation obligation %q state %q", id, obligation.State)
		}
		out = append(out, obligation)
	}
	return out, nil
}

const redisEnsureDeactivationObligationLua = `
local currentSession = redis.call('HGET', KEYS[1], ARGV[2])
if not currentSession then return 'not_applicable' end
if redis.call('HGET', KEYS[2], ARGV[2]) ~= 'draining' then return 'not_applicable' end
if (redis.call('HGET', KEYS[3], ARGV[2]) or '0') ~= ARGV[10] then return 'not_applicable' end
if redis.call('HGET', KEYS[4], ARGV[1]) then return 'applicable' end
local ownerSession = ARGV[3]
-- A reconnected session may receive a drain before the activation store has
-- been rewritten with its session ID. It may inherit delivery only when the
-- atomic Register inventory explicitly reports this exact old generation.
local inventory = cjson.decode(redis.call('HGET', KEYS[14], ARGV[2]) or '[]')
for _, item in ipairs(inventory) do
  local replica = tostring(item.replica_index or 0)
  if (item.workflow_id == ARGV[5]) and (item.workflow_version == ARGV[6]) and
     (item.entry_unit_id == ARGV[7]) and (replica == ARGV[8]) and
     (item.generation == ARGV[9]) then
    ownerSession = currentSession
    break
  end
end
if ownerSession == '' then ownerSession = currentSession end
redis.call('HSET', KEYS[4], ARGV[1], 'pending_fence')
redis.call('HSET', KEYS[5], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[6], ARGV[1], ownerSession)
redis.call('HSET', KEYS[7], ARGV[1], ARGV[4])
redis.call('HSET', KEYS[8], ARGV[1], ARGV[5])
redis.call('HSET', KEYS[9], ARGV[1], ARGV[6])
redis.call('HSET', KEYS[10], ARGV[1], ARGV[7])
redis.call('HSET', KEYS[11], ARGV[1], ARGV[8])
redis.call('HSET', KEYS[12], ARGV[1], ARGV[9])
redis.call('HSET', KEYS[13], ARGV[1], ARGV[10])
return 'applicable'
`

const redisMarkDeactivationObligationReadyLua = `
local state = redis.call('HGET', KEYS[1], ARGV[1])
if not state then return 'missing' end
if state == 'ready' then return 'already_ready' end
if state ~= 'pending_fence' then return 'missing' end
redis.call('HSET', KEYS[1], ARGV[1], 'ready')
return 'ready'
`

// KEYS use deactivationObligationKeys order: state, runner, session,
// namespace, workflowID, workflowVersion, entryUnitID, replica, generation,
// drainGeneration.
const redisCancelPendingDeactivationObligationLua = `
if redis.call('HGET', KEYS[1], ARGV[1]) ~= 'pending_fence' then return 'noop' end
for index = 1, #KEYS do redis.call('HDEL', KEYS[index], ARGV[1]) end
return 'cancelled'
`

const redisRebindDeactivationObligationsLua = `
local current = redis.call('HGET', KEYS[1], ARGV[1])
if not current then return 'not_found' end
if current ~= ARGV[2] then return 'stale' end
local inventory = cjson.decode(ARGV[3])
redis.call('HSET', KEYS[9], ARGV[1], ARGV[3])
for _, obligationID in ipairs(redis.call('HKEYS', KEYS[2])) do
  if redis.call('HGET', KEYS[2], obligationID) == ARGV[1] and redis.call('HGET', KEYS[3], obligationID) ~= ARGV[2] then
    for _, item in ipairs(inventory) do
      local replica = tostring(item.replica_index or 0)
      if (item.workflow_id == redis.call('HGET', KEYS[4], obligationID)) and
         (item.workflow_version == redis.call('HGET', KEYS[5], obligationID)) and
         (item.entry_unit_id == redis.call('HGET', KEYS[6], obligationID)) and
         (replica == redis.call('HGET', KEYS[7], obligationID)) and
         (item.generation == redis.call('HGET', KEYS[8], obligationID)) then
        redis.call('HSET', KEYS[3], obligationID, ARGV[2])
        break
      end
    end
  end
end
return 'rebound'
`

// KEYS: runner session, then deactivationObligationKeys order.
const redisAcknowledgeDeactivationLua = `
if redis.call('HGET', KEYS[1], ARGV[2]) ~= ARGV[3] then return 'noop' end
if redis.call('HGET', KEYS[2], ARGV[1]) ~= 'ready' then return 'noop' end
if redis.call('HGET', KEYS[3], ARGV[1]) ~= ARGV[2] then return 'noop' end
if redis.call('HGET', KEYS[4], ARGV[1]) ~= ARGV[3] then return 'noop' end
for index = 2, #KEYS do redis.call('HDEL', KEYS[index], ARGV[1]) end
return 'acknowledged'
`
