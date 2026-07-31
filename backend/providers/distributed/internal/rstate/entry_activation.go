package rstate

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// Compile-time interface satisfaction.
var _ engine.EntryActivationStore = (*EntryActivationStore)(nil)

// EntryActivationStore is a Redis-backed engine.EntryActivationStore. Each
// activation is a single hash whose key is hash-tagged by the activation
// identity so all future per-activation keys share one slot. Assign and Fence
// are single Lua CAS transitions on the stored generation, giving atomic
// first-writer-wins semantics with monotonic generation fencing.
type EntryActivationStore struct {
	rdb redis.UniversalClient
	ttl time.Duration
}

// NewEntryActivationStore returns a Redis-backed EntryActivationStore. ttl
// bounds how long an untouched activation record survives; every write refreshes
// it.
func NewEntryActivationStore(rdb redis.UniversalClient, ttl time.Duration) *EntryActivationStore {
	return &EntryActivationStore{rdb: rdb, ttl: ttl}
}

// entryActivationRedisKey builds the hash key for one activation. The identity
// tuple (workflow|version|unit) is the hash tag so all keys for one activation
// map to the same cluster slot; the namespace stays outside the tag.
func entryActivationRedisKey(ns namespace.Namespace, wf types.WorkflowID, ver, unit string) string {
	return fmt.Sprintf("xflow:ns:%s:entryact:{%s|%s|%s}", ns, wf, ver, unit)
}

func entryActivationScanPattern(ns namespace.Namespace) string {
	return fmt.Sprintf("xflow:ns:%s:entryact:{*}", ns)
}

func (s *EntryActivationStore) keyFor(k engine.EntryActivationKey) string {
	return entryActivationRedisKey(k.Namespace, k.WorkflowID, k.WorkflowVersion, k.EntryUnitID)
}

// assignEntryActivationLua atomically claims an activation. It is a no-op
// (returns 0) when the activation does not exist or when the supplied generation
// does not strictly exceed the stored generation.
//
// KEYS: 1=activation hash
// ARGV: 1=runnerID 2=sessionID 3=generation 4=leaseDeadlineUnixNano 5=ttl_s
// Returns 1 on success, 0 on rejection.
var assignEntryActivationLua = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
    return 0
end
local cur = tonumber(redis.call('HGET', KEYS[1], 'generation') or '0')
local gen = tonumber(ARGV[3])
if gen <= cur then
    return 0
end
local pkg = redis.call('HGET', KEYS[1], 'package_hash') or ''
redis.call('HSET', KEYS[1],
    'runner_id', ARGV[1],
    'session_id', ARGV[2],
    'generation', ARGV[3],
    'lease_deadline', ARGV[4],
    'assigned_package_hash', pkg)
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[5]))
return 1
`)

// fenceEntryActivationLua invalidates the current owner and raises the
// generation floor to at least the supplied generation. No-op when absent.
//
// KEYS: 1=activation hash
// ARGV: 1=generation 2=ttl_s
var fenceEntryActivationLua = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
    return 0
end
local cur = tonumber(redis.call('HGET', KEYS[1], 'generation') or '0')
local gen = tonumber(ARGV[1])
if gen > cur then
    redis.call('HSET', KEYS[1], 'generation', ARGV[1])
end
redis.call('HSET', KEYS[1],
    'runner_id', '',
    'session_id', '',
    'lease_deadline', '0',
    'assigned_package_hash', '')
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
return 1
`)

// renewEntryActivationLua extends the lease deadline of the current owner
// WITHOUT advancing the generation. Generation-gated: succeeds (returns 1) only
// when the supplied generation EQUALS the stored generation and an owner is set.
// No-op (returns 0) when the activation does not exist, is unowned, or the
// generation does not match.
//
// KEYS: 1=activation hash
// ARGV: 1=generation 2=leaseDeadlineUnixNano 3=ttl_s
var renewEntryActivationLua = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
    return 0
end
local cur = tonumber(redis.call('HGET', KEYS[1], 'generation') or '0')
local gen = tonumber(ARGV[1])
if gen ~= cur then
    return 0
end
if (redis.call('HGET', KEYS[1], 'runner_id') or '') == '' then
    return 0
end
redis.call('HSET', KEYS[1], 'lease_deadline', ARGV[2])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
return 1
`)

// Upsert writes the desired-state fields of an activation without touching the
// assignment fields (runner_id/session_id/generation/lease_deadline) of an
// existing record — those are owned by Assign/Fence.
func (s *EntryActivationStore) Upsert(ctx context.Context, act engine.EntryActivation) error {
	key := entryActivationRedisKey(act.Namespace, act.WorkflowID, act.WorkflowVersion, act.EntryUnitID)

	var selectorJSON string
	if act.Selector != nil {
		b, err := json.Marshal(act.Selector)
		if err != nil {
			return fmt.Errorf("marshal selector: %w", err)
		}
		selectorJSON = string(b)
	}

	var requirementsJSON string
	if len(act.Requirements) > 0 {
		b, err := json.Marshal(act.Requirements)
		if err != nil {
			return fmt.Errorf("marshal requirements: %w", err)
		}
		requirementsJSON = string(b)
	}

	var paramsJSON string
	if len(act.Params) > 0 {
		b, err := json.Marshal(act.Params)
		if err != nil {
			return fmt.Errorf("marshal params: %w", err)
		}
		paramsJSON = string(b)
	}

	var suppliesJSON string
	if len(act.Supplies) > 0 {
		b, err := json.Marshal(act.Supplies)
		if err != nil {
			return fmt.Errorf("marshal supplies: %w", err)
		}
		suppliesJSON = string(b)
	}

	desired := "0"
	if act.Desired {
		desired = "1"
	}

	pipe := s.rdb.TxPipeline()
	pipe.HSet(ctx, key,
		"namespace", string(act.Namespace),
		"workflow_id", string(act.WorkflowID),
		"workflow_version", act.WorkflowVersion,
		"entry_unit_id", act.EntryUnitID,
		"node_type", act.NodeType,
		"params", paramsJSON,
		"package_hash", act.PackageHash,
		"selector", selectorJSON,
		"requirements", requirementsJSON,
		"supplies", suppliesJSON,
		"desired", desired,
	)
	pipe.Expire(ctx, key, s.ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("upsert entry activation %q: %w", key, err)
	}
	return nil
}

// Get returns the activation for key; the bool is false when absent.
func (s *EntryActivationStore) Get(ctx context.Context, key engine.EntryActivationKey) (engine.EntryActivation, bool, error) {
	fields, err := s.rdb.HGetAll(ctx, s.keyFor(key)).Result()
	if err != nil {
		return engine.EntryActivation{}, false, fmt.Errorf("get entry activation: %w", err)
	}
	if len(fields) == 0 {
		return engine.EntryActivation{}, false, nil
	}
	act, err := decodeEntryActivation(fields)
	if err != nil {
		return engine.EntryActivation{}, false, err
	}
	return act, true, nil
}

// List returns all activations in the namespace.
func (s *EntryActivationStore) List(ctx context.Context, ns namespace.Namespace) ([]engine.EntryActivation, error) {
	pattern := entryActivationScanPattern(ns)
	var out []engine.EntryActivation
	var cursor uint64
	for {
		keys, next, err := s.rdb.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, fmt.Errorf("scan entry activations: %w", err)
		}
		for _, k := range keys {
			fields, err := s.rdb.HGetAll(ctx, k).Result()
			if err != nil {
				return nil, fmt.Errorf("read entry activation %q: %w", k, err)
			}
			if len(fields) == 0 {
				continue
			}
			act, err := decodeEntryActivation(fields)
			if err != nil {
				return nil, err
			}
			out = append(out, act)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return out, nil
}

// Assign atomically claims the activation via a single Lua CAS. First-writer-
// wins and monotonic: succeeds only when gen strictly exceeds the stored
// generation.
func (s *EntryActivationStore) Assign(ctx context.Context, key engine.EntryActivationKey, runnerID, sessionID string, gen uint64, deadline time.Time) (bool, error) {
	res, err := assignEntryActivationLua.Run(ctx, s.rdb,
		[]string{s.keyFor(key)},
		runnerID, sessionID, gen, deadlineToNano(deadline), int(s.ttl.Seconds()),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("assign entry activation: %w", err)
	}
	return res == 1, nil
}

// Renew extends the lease deadline of the current owner without advancing the
// generation, via a single Lua CAS. Generation-gated: succeeds only when gen
// equals the stored generation and an owner is set.
func (s *EntryActivationStore) Renew(ctx context.Context, key engine.EntryActivationKey, gen uint64, deadline time.Time) (bool, error) {
	res, err := renewEntryActivationLua.Run(ctx, s.rdb,
		[]string{s.keyFor(key)},
		gen, deadlineToNano(deadline), int(s.ttl.Seconds()),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("renew entry activation: %w", err)
	}
	return res == 1, nil
}

// Fence invalidates the current owner and raises the generation floor via a
// single Lua CAS. No-op when the activation does not exist.
func (s *EntryActivationStore) Fence(ctx context.Context, key engine.EntryActivationKey, gen uint64) error {
	if _, err := fenceEntryActivationLua.Run(ctx, s.rdb,
		[]string{s.keyFor(key)},
		gen, int(s.ttl.Seconds()),
	).Result(); err != nil {
		return fmt.Errorf("fence entry activation: %w", err)
	}
	return nil
}

func deadlineToNano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func decodeEntryActivation(fields map[string]string) (engine.EntryActivation, error) {
	act := engine.EntryActivation{
		Namespace:           namespace.Namespace(fields["namespace"]),
		WorkflowID:          types.WorkflowID(fields["workflow_id"]),
		WorkflowVersion:     fields["workflow_version"],
		EntryUnitID:         fields["entry_unit_id"],
		NodeType:            fields["node_type"],
		PackageHash:         fields["package_hash"],
		Desired:             fields["desired"] == "1",
		RunnerID:            fields["runner_id"],
		SessionID:           fields["session_id"],
		AssignedPackageHash: fields["assigned_package_hash"],
	}
	// Params is absent on records written before the field existed; decode
	// tolerates absence (leaves Params nil).
	if p := fields["params"]; p != "" {
		var pm map[string]any
		if err := json.Unmarshal([]byte(p), &pm); err != nil {
			return engine.EntryActivation{}, fmt.Errorf("unmarshal params: %w", err)
		}
		act.Params = pm
	}
	if sel := fields["selector"]; sel != "" {
		var rs types.RunnerSelector
		if err := json.Unmarshal([]byte(sel), &rs); err != nil {
			return engine.EntryActivation{}, fmt.Errorf("unmarshal selector: %w", err)
		}
		act.Selector = &rs
	}
	// Requirements is absent on records written before the field existed; decode
	// tolerates absence (leaves Requirements nil).
	if reqs := fields["requirements"]; reqs != "" {
		var rr []engine.CapabilityRequirement
		if err := json.Unmarshal([]byte(reqs), &rr); err != nil {
			return engine.EntryActivation{}, fmt.Errorf("unmarshal requirements: %w", err)
		}
		act.Requirements = rr
	}
	// Supplies is absent on records written before the field existed; decode
	// tolerates absence (leaves Supplies nil).
	if sup := fields["supplies"]; sup != "" {
		if err := json.Unmarshal([]byte(sup), &act.Supplies); err != nil {
			return engine.EntryActivation{}, fmt.Errorf("unmarshal supplies: %w", err)
		}
	}
	if g := fields["generation"]; g != "" {
		gen, err := strconv.ParseUint(g, 10, 64)
		if err != nil {
			return engine.EntryActivation{}, fmt.Errorf("parse generation %q: %w", g, err)
		}
		act.Generation = gen
	}
	if d := fields["lease_deadline"]; d != "" && d != "0" {
		nano, err := strconv.ParseInt(d, 10, 64)
		if err != nil {
			return engine.EntryActivation{}, fmt.Errorf("parse lease_deadline %q: %w", d, err)
		}
		act.LeaseDeadline = time.Unix(0, nano)
	}
	return act, nil
}
