package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/service/protocol"
)

var _ ActivationDeliveryDirectory = (*RedisRunnerDirectory)(nil)

// redisHostedActivationsReport is the stored form of one runner's hosted
// activation report. Activations reuses the inventory codec (generation as a
// string) for the same reason RegisterRunnerRequest.Activations does: JSON
// numbers would lose exact uint64 fencing values above 2^53.
type redisHostedActivationsReport struct {
	SessionID   string          `json:"session_id"`
	Activations json.RawMessage `json:"activations"`
	ObservedAt  int64           `json:"observed_at_unix_ms"`
}

// EnqueueActivationDirective writes one pending Activate under the session's
// directive hash. The HSet+Expire pair runs in one MULTI/EXEC so a directive
// can never be stored without its TTL (an untimed queue key for a dead session
// would be a permanent leak). Overwriting a field for the same activation
// identity is intended — see activationDirectiveKey.
func (d *RedisRunnerDirectory) EnqueueActivationDirective(ctx context.Context, runnerID, sessionID string, directive protocol.ActivateDirective) error {
	if runnerID == "" || sessionID == "" {
		return ErrRunnerSessionRequired
	}
	payload, err := json.Marshal(directive)
	if err != nil {
		return fmt.Errorf("marshal activate directive: %w", err)
	}
	return d.enqueueActivationDirectiveField(ctx, runnerID, sessionID,
		activationDirectiveFieldActivate+activationDirectiveKeyOfActivate(directive), string(payload))
}

// EnqueueDeactivationDirective mirrors EnqueueActivationDirective for stops.
func (d *RedisRunnerDirectory) EnqueueDeactivationDirective(ctx context.Context, runnerID, sessionID string, directive protocol.DeactivateDirective) error {
	if runnerID == "" || sessionID == "" {
		return ErrRunnerSessionRequired
	}
	payload, err := json.Marshal(directive)
	if err != nil {
		return fmt.Errorf("marshal deactivate directive: %w", err)
	}
	return d.enqueueActivationDirectiveField(ctx, runnerID, sessionID,
		activationDirectiveFieldDeactivate+activationDirectiveKeyOfDeactivate(directive), string(payload))
}

func (d *RedisRunnerDirectory) enqueueActivationDirectiveField(ctx context.Context, runnerID, sessionID, field, payload string) error {
	key := d.keys.activationDirectiveQueueKey(runnerID, sessionID)
	pipe := d.rdb.TxPipeline()
	pipe.HSet(ctx, key, field, payload)
	pipe.Expire(ctx, key, activationDirectiveQueueTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("enqueue redis activation directive: %w", err)
	}
	return nil
}

// ActivationDirectives drains the session's directive hash. The HGETALL+DEL
// pair is a single-key Lua script so two control pods serving concurrent
// heartbeats for the same runner cannot both drain the same directive: the
// second sees an empty (already deleted) hash. Single-key means no Redis
// Cluster cross-slot concern — the key carries the directory prefix's hash tag.
func (d *RedisRunnerDirectory) ActivationDirectives(ctx context.Context, runnerID, sessionID string) ([]protocol.ActivateDirective, []protocol.DeactivateDirective, error) {
	if runnerID == "" || sessionID == "" {
		return nil, nil, ErrRunnerSessionRequired
	}
	key := d.keys.activationDirectiveQueueKey(runnerID, sessionID)
	raw, err := d.rdb.Eval(ctx, redisDrainActivationDirectivesLua, []string{key}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, nil, fmt.Errorf("drain redis activation directives: %w", err)
	}
	entries, err := decodeRedisDrainedDirectives(raw)
	if err != nil {
		return nil, nil, err
	}
	return resolveActivationDirectiveConflicts(entries)
}

// RecordHostedActivations stores the runner's report under its own key with a
// fresh TTL, so freshness is enforced by Redis itself rather than by a
// timestamp comparison in the reader. The session is recorded inside the
// payload: the key is per runner, and a replacement session's report must be
// distinguishable from the old session's still-fresh one.
func (d *RedisRunnerDirectory) RecordHostedActivations(ctx context.Context, runnerID, sessionID string, items []protocol.ActivationInventoryItem) error {
	if runnerID == "" || sessionID == "" {
		return ErrRunnerSessionRequired
	}
	encoded, err := marshalRedisActivationInventory(items)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(redisHostedActivationsReport{
		SessionID:   sessionID,
		Activations: json.RawMessage(encoded),
		ObservedAt:  d.clockNow().UnixMilli(),
	})
	if err != nil {
		return fmt.Errorf("marshal runner hosted activations: %w", err)
	}
	if err := d.rdb.Set(ctx, d.keys.runnerHostedActivationsKey(runnerID), payload, activationHostedReportTTL).Err(); err != nil {
		return fmt.Errorf("record runner hosted activations: %w", err)
	}
	return nil
}

// HostedActivations reads the runner's report. A missing key is "no report"
// (never heartbeated with the field, or the report aged out): both mean the
// caller must not reconcile against it, so they collapse to ok=false. A
// damaged payload also reads as absent — the reconciler's redelivery is a
// recovery mechanism, and guessing at a corrupt report would turn a storage
// fault into a directive storm.
func (d *RedisRunnerDirectory) HostedActivations(ctx context.Context, runnerID string) (HostedActivationsReport, bool, error) {
	if runnerID == "" {
		return HostedActivationsReport{}, false, nil
	}
	raw, err := d.rdb.Get(ctx, d.keys.runnerHostedActivationsKey(runnerID)).Result()
	if errors.Is(err, redis.Nil) {
		return HostedActivationsReport{}, false, nil
	}
	if err != nil {
		return HostedActivationsReport{}, false, fmt.Errorf("read runner hosted activations: %w", err)
	}
	var stored redisHostedActivationsReport
	if err := json.Unmarshal([]byte(raw), &stored); err != nil || stored.SessionID == "" {
		return HostedActivationsReport{}, false, nil
	}
	items, err := unmarshalRedisActivationInventory(stored.Activations)
	if err != nil {
		return HostedActivationsReport{}, false, nil
	}
	return HostedActivationsReport{SessionID: stored.SessionID, Items: items}, true, nil
}

// decodeRedisDrainedDirectives converts the Lua script's flat
// [field, payload, field, payload, ...] reply into a field map. An empty or
// nil reply (no pending directives) yields an empty map.
func decodeRedisDrainedDirectives(raw interface{}) (map[string]string, error) {
	entries := make(map[string]string)
	if raw == nil {
		return entries, nil
	}
	flat, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("drain redis activation directives: unexpected reply type %T", raw)
	}
	for index := 0; index+1 < len(flat); index += 2 {
		field, fieldOK := flat[index].(string)
		payload, payloadOK := flat[index+1].(string)
		if !fieldOK || !payloadOK {
			return nil, fmt.Errorf("drain redis activation directives: malformed entry at index %d", index)
		}
		entries[field] = payload
	}
	return entries, nil
}

// KEYS[1] = the session's directive hash. The drain is one round trip so a
// crash between "read" and "delete" cannot duplicate directives, and the
// delete happens only when something was read.
const redisDrainActivationDirectivesLua = `
local entries = redis.call('HGETALL', KEYS[1])
if #entries > 0 then redis.call('DEL', KEYS[1]) end
return entries
`

// unmarshalRedisActivationInventory is the inverse of
// marshalRedisActivationInventory. Absent content decodes to nil (no items).
func unmarshalRedisActivationInventory(payload []byte) ([]protocol.ActivationInventoryItem, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	var encoded []redisActivationInventoryItem
	if err := json.Unmarshal(payload, &encoded); err != nil {
		return nil, fmt.Errorf("decode runner activation inventory: %w", err)
	}
	items := make([]protocol.ActivationInventoryItem, 0, len(encoded))
	for _, item := range encoded {
		generation, err := strconv.ParseUint(item.Generation, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("decode runner activation inventory generation: %w", err)
		}
		items = append(items, protocol.ActivationInventoryItem{
			WorkflowID:      item.WorkflowID,
			WorkflowVersion: item.WorkflowVersion,
			EntryUnitID:     item.EntryUnitID,
			ReplicaIndex:    item.ReplicaIndex,
			Generation:      generation,
		})
	}
	return items, nil
}
