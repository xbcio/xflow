package control

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var _ RunnerControlDirectory = (*RedisRunnerDirectory)(nil)

// SetRunnerControl stores a durable, idempotent desired-state transition. The
// Lua script reads/writes the receipt and desired state in one Redis Cluster
// slot, while redisClaimAssignmentLua reads the same desired-state hash before
// making a new queue reservation.
func (d *RedisRunnerDirectory) SetRunnerControl(ctx context.Context, req RunnerControlRequest) (RunnerControlSnapshot, error) {
	if err := validateRunnerControlRequest(req); err != nil {
		return RunnerControlSnapshot{}, err
	}
	now := req.Now
	if now.IsZero() {
		now = d.clockNow()
	}
	receiptID := runnerControlReceiptID(req)
	expiresAt := now.Add(d.runnerControlReceiptRetention())
	drainDeadline := now.Add(d.runnerDrainDeadline())
	status, err := d.evalStatus(ctx, redisSetRunnerControlLua, []string{
		d.keys.runnerSession,
		d.keys.runnerControlDesired,
		d.keys.runnerControlGeneration,
		d.keys.runnerControlRequestedAt,
		d.keys.runnerControlActor,
		d.keys.runnerControlReason,
		d.keys.runnerControlReceiptHash,
		d.keys.runnerControlReceiptDesired,
		d.keys.runnerControlReceiptGeneration,
		d.keys.runnerControlReceiptRequestedAt,
		d.keys.runnerControlReceiptReason,
		d.keys.runnerControlReceiptClaims,
		d.keys.runnerControlReceiptLeases,
		d.keys.runnerClaimCount,
		d.keys.runnerLeaseCount,
		d.keys.handoffRunner,
		d.keys.handoffState,
		d.keys.runnerControlReceiptUnsettledDebt,
		d.keys.runnerControlReceiptHandoffDebt,
		d.keys.runnerControlReceiptLeaseMayExistDebt,
		d.keys.runnerControlReceiptReplayableDebt,
		d.keys.runnerControlReceiptPendingActivationCleanup,
		d.keys.deactivationObligationRunner,
		d.keys.deactivationObligationState,
		d.keys.runnerDrainObservation,
		d.keys.runnerControlReceiptStoredAt,
		d.keys.runnerControlReceiptStatus,
		d.keys.runnerControlReceiptExpiry,
		d.keys.runnerControlAudit,
		// KEYS[30+] were appended after the established control transition
		// contract. Do not reorder KEYS[1..29]: rolling control-plane
		// upgrades may execute either script revision against the same data.
		d.keys.runnerControlDrainDeadline,
		d.keys.runnerControlReceiptDrainDeadline,
	}, req.RunnerID, string(req.DesiredState), req.Actor, req.Reason, receiptID,
		req.RequestHash, strconv.FormatInt(now.UnixMilli(), 10),
		strconv.FormatInt(expiresAt.UnixMilli(), 10), strconv.FormatInt(d.runnerControlAuditMaxLen(), 10),
		runnerControlAction(req), req.RequestID, strconv.FormatInt(drainDeadline.UnixMilli(), 10))
	if err != nil {
		return RunnerControlSnapshot{}, fmt.Errorf("set redis runner control: %w", err)
	}
	switch status {
	case "stored", "replayed":
		// The receipt contains the complete first projection, including durable
		// handoff debt. Response-loss retries therefore stay fully idempotent.
		return d.redisControlReceipt(ctx, receiptID)
	case "conflict":
		return RunnerControlSnapshot{}, ErrRunnerControlRequestConflict
	case "not_found":
		return RunnerControlSnapshot{}, ErrRunnerNotFound
	default:
		return RunnerControlSnapshot{}, fmt.Errorf("set redis runner control: unexpected result %q", status)
	}
}

// RunnerControl reads the durable control projection for a registered runner.
// Missing legacy fields mean ACTIVE/generation=0; Register initializes them for
// newly registered runners, but this fallback keeps rolling upgrades passive.
func (d *RedisRunnerDirectory) RunnerControl(ctx context.Context, runnerID string) (RunnerControlSnapshot, bool, error) {
	return d.redisCurrentControl(ctx, runnerID)
}

// RunnerControlState reads only the scalar control projection. Unlike
// RunnerControl it does not aggregate the handoff and deactivation ledgers,
// which are keyed by claim/obligation across the whole fleet and are not
// consumed by the poll or register paths that call this.
func (d *RedisRunnerDirectory) RunnerControlState(ctx context.Context, runnerID string) (RunnerControlState, bool, error) {
	pipe := d.rdb.Pipeline()
	session := pipe.HGet(ctx, d.keys.runnerSession, runnerID)
	desired := pipe.HGet(ctx, d.keys.runnerControlDesired, runnerID)
	generation := pipe.HGet(ctx, d.keys.runnerControlGeneration, runnerID)
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return RunnerControlState{}, false, fmt.Errorf("read runner control state: %w", err)
	}
	if session.Val() == "" {
		return RunnerControlState{}, false, nil
	}
	state, err := decodeRunnerControlState(desired.Val(), generation.Val())
	if err != nil {
		return RunnerControlState{}, false, err
	}
	return state, true, nil
}

func decodeRunnerControlState(desiredRaw, generationRaw string) (RunnerControlState, error) {
	desired := RunnerDesiredState(desiredRaw)
	if desired == "" {
		desired = RunnerDesiredStateActive
	}
	if desired != RunnerDesiredStateActive && desired != RunnerDesiredStateDraining {
		return RunnerControlState{}, fmt.Errorf("decode runner control desired state %q", desired)
	}
	if generationRaw == "" {
		generationRaw = "0"
	}
	generation, err := strconv.ParseUint(generationRaw, 10, 64)
	if err != nil {
		return RunnerControlState{}, fmt.Errorf("decode runner control generation: %w", err)
	}
	return RunnerControlState{DesiredState: desired, Generation: generation}, nil
}

// runnerControlStateSnapshot projects the scalar control state as a snapshot.
//
// The debt-bearing drain projection is deliberately absent: it aggregates the
// fleet-wide handoff and deactivation ledgers, which is a management concern
// served by its own accessor (RunnerControl). Attaching it to every
// single-runner read made each one scan the whole fleet's debt.
func runnerControlStateSnapshot(state RunnerControlState) *RunnerControlSnapshot {
	return &RunnerControlSnapshot{DesiredState: state.DesiredState, Generation: state.Generation}
}

// redisControlLedger is the fleet-wide debt state every control projection
// aggregates. Both hashes are keyed by claim or obligation rather than by
// runner, so projecting a whole fleet reads them once and reuses the result
// instead of re-reading them for every runner in the list.
type redisControlLedger struct {
	handoffRunners      map[string]string
	handoffStates       map[string]string
	deactivationRunners map[string]string
	deactivationStates  map[string]string
}

// redisControlLedgerCommands are the four queued ledger reads. Keeping the
// commands (not just their values) lets a caller share one pipeline with the
// per-runner scalars it is already reading.
type redisControlLedgerCommands struct {
	handoffRunners      *redis.MapStringStringCmd
	handoffStates       *redis.MapStringStringCmd
	deactivationRunners *redis.MapStringStringCmd
	deactivationStates  *redis.MapStringStringCmd
}

func (d *RedisRunnerDirectory) queueRedisControlLedger(ctx context.Context, pipe redis.Pipeliner) redisControlLedgerCommands {
	return redisControlLedgerCommands{
		handoffRunners:      pipe.HGetAll(ctx, d.keys.handoffRunner),
		handoffStates:       pipe.HGetAll(ctx, d.keys.handoffState),
		deactivationRunners: pipe.HGetAll(ctx, d.keys.deactivationObligationRunner),
		deactivationStates:  pipe.HGetAll(ctx, d.keys.deactivationObligationState),
	}
}

func (c redisControlLedgerCommands) ledger() redisControlLedger {
	return redisControlLedger{
		handoffRunners:      c.handoffRunners.Val(),
		handoffStates:       c.handoffStates.Val(),
		deactivationRunners: c.deactivationRunners.Val(),
		deactivationStates:  c.deactivationStates.Val(),
	}
}

// handoffDebtStats counts runnerID's slice of the shared ledger.
func (l redisControlLedger) handoffDebtStats(runnerID string, claims, leases int) handoffDebtStats {
	stats := handoffDebtStats{activeClaims: claims, leasedTasks: leases}
	for claimID, owner := range l.handoffRunners {
		if owner != runnerID {
			continue
		}
		stats.unsettledDebt++
		switch HandoffDebtState(l.handoffStates[claimID]) {
		case HandoffDebtLeaseMayExist:
			stats.handoffDebt++
			stats.leaseMayExistDebt++
		case HandoffDebtLeaseCreated:
			stats.handoffDebt++
		case HandoffDebtFinalized:
			stats.replayableDebt++
		}
	}
	return stats
}

func (l redisControlLedger) pendingActivationCleanup(runnerID string) int {
	return countRedisDeactivationObligations(runnerID, l.deactivationRunners, l.deactivationStates)
}

func (d *RedisRunnerDirectory) redisCurrentControl(ctx context.Context, runnerID string) (RunnerControlSnapshot, bool, error) {
	// A single pipeline, but NOT a cheap one: the ledger reads below return the
	// fleet-wide handoff and deactivation debt in full. That aggregation is
	// correct for a management snapshot and is why the recurring poll/register
	// paths must use RunnerControlState instead of this method.
	pipe := d.rdb.Pipeline()
	session := pipe.HGet(ctx, d.keys.runnerSession, runnerID)
	desired := pipe.HGet(ctx, d.keys.runnerControlDesired, runnerID)
	generation := pipe.HGet(ctx, d.keys.runnerControlGeneration, runnerID)
	requestedAt := pipe.HGet(ctx, d.keys.runnerControlRequestedAt, runnerID)
	reason := pipe.HGet(ctx, d.keys.runnerControlReason, runnerID)
	drainDeadline := pipe.HGet(ctx, d.keys.runnerControlDrainDeadline, runnerID)
	claims := pipe.HGet(ctx, d.keys.runnerClaimCount, runnerID)
	leases := pipe.HGet(ctx, d.keys.runnerLeaseCount, runnerID)
	ledger := d.queueRedisControlLedger(ctx, pipe)
	drainObservation := pipe.HGet(ctx, d.keys.runnerDrainObservation, runnerID)
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return RunnerControlSnapshot{}, false, fmt.Errorf("read runner control projection: %w", err)
	}
	if session.Val() == "" {
		return RunnerControlSnapshot{}, false, nil
	}
	shared := ledger.ledger()
	return decodeRunnerControlProjection(
		desired.Val(), generation.Val(), requestedAt.Val(), drainDeadline.Val(), reason.Val(), session.Val(),
		unmarshalRedisRunnerDrainObservation(drainObservation.Val()),
		shared.handoffDebtStats(runnerID, parseRedisInt(claims.Val()), parseRedisInt(leases.Val())),
		shared.pendingActivationCleanup(runnerID),
		d.clockNow(), d.runnerDrainObservationFreshness(),
	)
}

func countRedisDeactivationObligations(runnerID string, owners, states map[string]string) int {
	pending := 0
	for id, owner := range owners {
		if owner != runnerID {
			continue
		}
		if state := DeactivationObligationState(states[id]); state == DeactivationObligationPendingFence || state == DeactivationObligationReady {
			pending++
		}
	}
	return pending
}

func parseRedisInt(raw string) int {
	if raw == "" {
		return 0
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0
	}
	return value
}

func (d *RedisRunnerDirectory) redisControlReceipt(ctx context.Context, receiptID string) (RunnerControlSnapshot, error) {
	pipe := d.rdb.Pipeline()
	desired := pipe.HGet(ctx, d.keys.runnerControlReceiptDesired, receiptID)
	generation := pipe.HGet(ctx, d.keys.runnerControlReceiptGeneration, receiptID)
	requestedAt := pipe.HGet(ctx, d.keys.runnerControlReceiptRequestedAt, receiptID)
	reason := pipe.HGet(ctx, d.keys.runnerControlReceiptReason, receiptID)
	claims := pipe.HGet(ctx, d.keys.runnerControlReceiptClaims, receiptID)
	leases := pipe.HGet(ctx, d.keys.runnerControlReceiptLeases, receiptID)
	unsettled := pipe.HGet(ctx, d.keys.runnerControlReceiptUnsettledDebt, receiptID)
	handoff := pipe.HGet(ctx, d.keys.runnerControlReceiptHandoffDebt, receiptID)
	leaseMayExist := pipe.HGet(ctx, d.keys.runnerControlReceiptLeaseMayExistDebt, receiptID)
	replayable := pipe.HGet(ctx, d.keys.runnerControlReceiptReplayableDebt, receiptID)
	pendingActivationCleanup := pipe.HGet(ctx, d.keys.runnerControlReceiptPendingActivationCleanup, receiptID)
	drainDeadline := pipe.HGet(ctx, d.keys.runnerControlReceiptDrainDeadline, receiptID)
	storedAt := pipe.HGet(ctx, d.keys.runnerControlReceiptStoredAt, receiptID)
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return RunnerControlSnapshot{}, fmt.Errorf("read runner control receipt: %w", err)
	}
	// A receipt is a historical response, not a live projection. Use the time
	// at which Lua stored it so a retry cannot turn a previously returned drain
	// response into timed_out merely because wall time has advanced.
	frozenAt, err := decodeRedisRunnerControlTime(storedAt.Val(), "receipt stored at")
	if err != nil {
		return RunnerControlSnapshot{}, err
	}
	snapshot, _, err := decodeRunnerControlProjection(desired.Val(), generation.Val(), requestedAt.Val(), drainDeadline.Val(), reason.Val(), "", nil, handoffDebtStats{
		activeClaims:      parseRedisInt(claims.Val()),
		leasedTasks:       parseRedisInt(leases.Val()),
		unsettledDebt:     parseRedisInt(unsettled.Val()),
		handoffDebt:       parseRedisInt(handoff.Val()),
		leaseMayExistDebt: parseRedisInt(leaseMayExist.Val()),
		replayableDebt:    parseRedisInt(replayable.Val()),
	}, parseRedisInt(pendingActivationCleanup.Val()), frozenAt, d.runnerDrainObservationFreshness())
	if err != nil {
		return RunnerControlSnapshot{}, err
	}
	return snapshot, nil
}

func decodeRunnerControlProjection(desiredRaw, generationRaw, requestedAtRaw, deadlineRaw, reason, sessionID string, observation *runnerDrainObservation, stats handoffDebtStats, pendingActivationCleanup int, now time.Time, observationFreshness time.Duration) (RunnerControlSnapshot, bool, error) {
	desired := RunnerDesiredState(desiredRaw)
	if desired == "" {
		desired = RunnerDesiredStateActive
	}
	if desired != RunnerDesiredStateActive && desired != RunnerDesiredStateDraining {
		return RunnerControlSnapshot{}, false, fmt.Errorf("decode runner control desired state %q", desired)
	}
	if generationRaw == "" {
		generationRaw = "0"
	}
	generation, err := strconv.ParseUint(generationRaw, 10, 64)
	if err != nil {
		return RunnerControlSnapshot{}, false, fmt.Errorf("decode runner control generation: %w", err)
	}
	requestedAt, err := decodeRedisRunnerControlTime(requestedAtRaw, "requested at")
	if err != nil {
		return RunnerControlSnapshot{}, false, err
	}
	drainDeadline, err := decodeRedisRunnerControlTime(deadlineRaw, "drain deadline")
	if err != nil {
		return RunnerControlSnapshot{}, false, err
	}
	control := memoryRunnerControl{
		desired:          desired,
		generation:       generation,
		requestedAt:      requestedAt,
		reason:           reason,
		drainDeadline:    drainDeadline,
		drainObservation: observation,
	}
	return runnerControlProjection(control, sessionID, stats, pendingActivationCleanup, now, observationFreshness), true, nil
}

func decodeRedisRunnerControlTime(raw, field string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	millis, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("decode runner control %s: %w", field, err)
	}
	return time.UnixMilli(millis).UTC(), nil
}

func redisValue(values []interface{}, index int) string {
	if index >= len(values) || values[index] == nil {
		return ""
	}
	value, _ := values[index].(string)
	return value
}

const redisSetRunnerControlLua = `
local function deleteReceipt(receiptID)
  for index = 7, 13 do redis.call('HDEL', KEYS[index], receiptID) end
  for index = 18, 22 do redis.call('HDEL', KEYS[index], receiptID) end
  redis.call('HDEL', KEYS[26], receiptID)
  redis.call('HDEL', KEYS[27], receiptID)
  redis.call('ZREM', KEYS[28], receiptID)
  redis.call('HDEL', KEYS[31], receiptID)
end

local previousHash = redis.call('HGET', KEYS[7], ARGV[5])
if previousHash then
  -- Receipts written before expiry indexing was introduced remain replayable
  -- until an explicit lifecycle migration removes them.
  local expiry = redis.call('ZSCORE', KEYS[28], ARGV[5])
  local expiryMillis = expiry and tonumber(expiry)
  if expiryMillis and expiryMillis <= tonumber(ARGV[7]) then
    deleteReceipt(ARGV[5])
  else
    if previousHash ~= ARGV[6] then return 'conflict' end
    return 'replayed'
  end
end

if not redis.call('HGET', KEYS[1], ARGV[1]) then return 'not_found' end

-- Bound cleanup work so one control request cannot turn an expiry backlog into
-- an unbounded Lua operation. The current receipt was handled above first.
local expired = redis.call('ZRANGEBYSCORE', KEYS[28], '-inf', ARGV[7], 'LIMIT', 0, 100)
for _, receiptID in ipairs(expired) do
  deleteReceipt(receiptID)
end

local desired = redis.call('HGET', KEYS[2], ARGV[1]) or 'active'
local previousDesired = desired
local generation = tonumber(redis.call('HGET', KEYS[3], ARGV[1]) or '0')
local requestedAt = redis.call('HGET', KEYS[4], ARGV[1]) or ''
local reason = redis.call('HGET', KEYS[6], ARGV[1]) or ''
local drainDeadline = redis.call('HGET', KEYS[30], ARGV[1]) or ''
local transitioned = false
if desired ~= ARGV[2] then
  transitioned = true
  desired = ARGV[2]
  generation = generation + 1
  requestedAt = ARGV[7]
  reason = ARGV[4]
  redis.call('HSET', KEYS[2], ARGV[1], desired)
  redis.call('HSET', KEYS[3], ARGV[1], tostring(generation))
  redis.call('HSET', KEYS[4], ARGV[1], requestedAt)
  redis.call('HSET', KEYS[5], ARGV[1], ARGV[3])
  redis.call('HSET', KEYS[6], ARGV[1], reason)
  redis.call('HDEL', KEYS[25], ARGV[1])
  if previousDesired == 'active' and desired == 'draining' then
    drainDeadline = ARGV[12]
    redis.call('HSET', KEYS[30], ARGV[1], drainDeadline)
  elseif previousDesired == 'draining' and desired == 'active' then
    drainDeadline = ''
    redis.call('HDEL', KEYS[30], ARGV[1])
  end
end

local claims = redis.call('HGET', KEYS[14], ARGV[1]) or '0'
local leases = redis.call('HGET', KEYS[15], ARGV[1]) or '0'
local unsettled = 0
local handoff = 0
local leaseMayExist = 0
local replayable = 0
local pendingActivationCleanup = 0
local owners = redis.call('HGETALL', KEYS[16])
for index = 1, #owners, 2 do
  local claimID = owners[index]
  if owners[index + 1] == ARGV[1] then
    unsettled = unsettled + 1
    local state = redis.call('HGET', KEYS[17], claimID)
    if state == 'lease_may_exist' then
      handoff = handoff + 1
      leaseMayExist = leaseMayExist + 1
    elseif state == 'lease_created' then
      handoff = handoff + 1
    elseif state == 'finalized' then
      replayable = replayable + 1
    end
  end
end
local activationOwners = redis.call('HGETALL', KEYS[23])
for index = 1, #activationOwners, 2 do
  local obligationID = activationOwners[index]
  if activationOwners[index + 1] == ARGV[1] then
    local state = redis.call('HGET', KEYS[24], obligationID)
    if state == 'pending_fence' or state == 'ready' then pendingActivationCleanup = pendingActivationCleanup + 1 end
  end
end

if transitioned then
  redis.call('XADD', KEYS[29], 'MAXLEN', '=', ARGV[9], '*',
    'runner_id', ARGV[1],
    'previous_desired_state', previousDesired,
    'desired_state', desired,
    'generation', tostring(generation),
    'actor', ARGV[3],
    'reason', ARGV[4],
    'action', ARGV[10],
    'request_id', ARGV[11],
    'requested_at', requestedAt,
    'result', 'stored')
end

redis.call('HSET', KEYS[7], ARGV[5], ARGV[6])
redis.call('HSET', KEYS[8], ARGV[5], desired)
redis.call('HSET', KEYS[9], ARGV[5], tostring(generation))
redis.call('HSET', KEYS[10], ARGV[5], requestedAt)
redis.call('HSET', KEYS[11], ARGV[5], reason)
redis.call('HSET', KEYS[12], ARGV[5], claims)
redis.call('HSET', KEYS[13], ARGV[5], leases)
redis.call('HSET', KEYS[18], ARGV[5], tostring(unsettled))
redis.call('HSET', KEYS[19], ARGV[5], tostring(handoff))
redis.call('HSET', KEYS[20], ARGV[5], tostring(leaseMayExist))
redis.call('HSET', KEYS[21], ARGV[5], tostring(replayable))
redis.call('HSET', KEYS[22], ARGV[5], tostring(pendingActivationCleanup))
redis.call('HSET', KEYS[26], ARGV[5], ARGV[7])
redis.call('HSET', KEYS[27], ARGV[5], '200')
redis.call('ZADD', KEYS[28], ARGV[8], ARGV[5])
if drainDeadline ~= '' then
  redis.call('HSET', KEYS[31], ARGV[5], drainDeadline)
else
  redis.call('HDEL', KEYS[31], ARGV[5])
end
return 'stored'
`
