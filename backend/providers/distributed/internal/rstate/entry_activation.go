package rstate

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// Compile-time interface satisfaction.
var (
	_ engine.EntryActivationStore         = (*EntryActivationStore)(nil)
	_ engine.EntryActivationRevisionStore = (*EntryActivationStore)(nil)
)

// EntryActivationStore is a Redis-backed engine.EntryActivationStore. Modern
// activation hashes share a workflow-scoped Redis Cluster hash tag with the
// workflow revision watermark. Legacy activation-scoped hashes remain readable
// during migration. Assign and Fence are single Lua CAS transitions on the
// selected hash, giving atomic first-writer-wins semantics with monotonic
// generation fencing.
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
func entryActivationRedisKey(ns namespace.Namespace, wf types.WorkflowID, ver, unit string, replica uint32) string {
	base := fmt.Sprintf("xflow:ns:%s:entryact:{%s|%s|%s}", ns, wf, ver, unit)
	if replica == 0 {
		return base
	}
	return fmt.Sprintf("%s:replica:%d", base, replica)
}

// entryActivationWorkflowTag is a safe, fixed-width Redis Cluster hash tag.
// Length-prefixing prevents ambiguous namespace/workflow concatenations; the
// digest prevents braces in either caller-controlled value from changing the
// selected hash tag.
func entryActivationWorkflowTag(ns namespace.Namespace, wf types.WorkflowID) string {
	payload := fmt.Sprintf("%d:%s%d:%s", len(ns), ns, len(wf), wf)
	return fmt.Sprintf("wf-%x", sha256.Sum256([]byte(payload)))
}

// workflowScopedEntryActivationRedisKey is the revision-aware key layout. All
// activations for one namespaced workflow and its watermark share the same safe
// hash tag, so Upsert can compare the watermark and write the activation in one
// cluster-safe Lua operation.
func workflowScopedEntryActivationRedisKey(ns namespace.Namespace, wf types.WorkflowID, ver, unit string, replica uint32) string {
	payload := fmt.Sprintf("%d:%s%d:%s", len(ver), ver, len(unit), unit)
	identity := sha256.Sum256([]byte(payload))
	return fmt.Sprintf("xflow:ns:%s:entryact:{%s}:act:%x:replica:%d", entryActivationNamespacePath(ns), entryActivationWorkflowTag(ns, wf), identity, replica)
}

func entryActivationWorkflowRevisionRedisKey(ns namespace.Namespace, wf types.WorkflowID) string {
	return fmt.Sprintf("xflow:ns:%s:entryactrev:{%s}", entryActivationNamespacePath(ns), entryActivationWorkflowTag(ns, wf))
}

// entryActivationNamespacePath keeps caller-controlled braces out of the key
// prefix. Redis Cluster uses the first {...} pair as its hash tag, so escaping
// the prefix is necessary for the workflow digest tag to be authoritative.
func entryActivationNamespacePath(ns namespace.Namespace) string {
	return url.PathEscape(string(ns))
}

// entryActivationScanPattern is the legacy layout scan pattern and is retained
// unchanged for replica-zero compatibility.
func entryActivationScanPattern(ns namespace.Namespace) string {
	return fmt.Sprintf("xflow:ns:%s:entryact:{*}*", ns)
}

func workflowScopedEntryActivationScanPattern(ns namespace.Namespace) string {
	return fmt.Sprintf("xflow:ns:%s:entryact:{*}*", entryActivationNamespacePath(ns))
}

func (s *EntryActivationStore) keyFor(k engine.EntryActivationKey) string {
	return workflowScopedEntryActivationRedisKey(k.Namespace, k.WorkflowID, k.WorkflowVersion, k.EntryUnitID, k.ReplicaIndex)
}

func (s *EntryActivationStore) legacyKeyFor(k engine.EntryActivationKey) string {
	return entryActivationRedisKey(k.Namespace, k.WorkflowID, k.WorkflowVersion, k.EntryUnitID, k.ReplicaIndex)
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

// Redis Lua numbers are IEEE-754 doubles and cannot exactly represent all
// uint64 registry revisions. Compare normalized decimal strings by length and
// then lexicographically instead.
const compareUint64DecimalLua = `
local function normalize_uint64(value)
    local normalized = string.gsub(tostring(value or '0'), '^0+', '')
    if normalized == '' then
        return '0'
    end
    return normalized
end

local function compare_uint64(left, right)
    left = normalize_uint64(left)
    right = normalize_uint64(right)
    if string.len(left) < string.len(right) then
        return -1
    end
    if string.len(left) > string.len(right) then
        return 1
    end
    if left < right then
        return -1
    end
    if left > right then
        return 1
    end
    return 0
end
`

// advanceEntryActivationWorkflowRevisionLua monotonically advances one
// workflow watermark. Revision zero deliberately does not materialize a key.
//
// KEYS: 1=workflow watermark
// ARGV: 1=registry revision
var advanceEntryActivationWorkflowRevisionLua = redis.NewScript(compareUint64DecimalLua + `
local incoming = normalize_uint64(ARGV[1])
local current = normalize_uint64(redis.call('GET', KEYS[1]) or '0')
if compare_uint64(incoming, current) > 0 then
    redis.call('SET', KEYS[1], incoming)
end
return current
`)

// upsertEntryActivationLua atomically compares the workflow watermark and the
// activation revision before writing desired-state fields. Watermark and record
// keys share a workflow digest hash tag, so this script is Redis Cluster safe.
// Assignment fields are initialized only when the modern record is first
// created; values copied from a legacy key keep an existing owner visible during
// the layout transition.
//
// KEYS: 1=workflow watermark 2=activation hash
// ARGV: 1=revision 2=ttl_s, 3..15=desired fields,
//
//	16..20=legacy assignment defaults, 21=legacy registry revision
//
// Returns 1 when applied, 0 when rejected as stale.
var upsertEntryActivationLua = redis.NewScript(compareUint64DecimalLua + `
local incoming = normalize_uint64(ARGV[1])
local watermark = normalize_uint64(redis.call('GET', KEYS[1]) or '0')
if compare_uint64(incoming, watermark) < 0 then
    return 0
end

local current = normalize_uint64(redis.call('HGET', KEYS[2], 'registry_revision') or '0')
local legacy = normalize_uint64(ARGV[21])
local next_watermark = watermark
if compare_uint64(current, next_watermark) > 0 then
    next_watermark = current
end
if compare_uint64(legacy, next_watermark) > 0 then
    next_watermark = legacy
end
if compare_uint64(incoming, next_watermark) > 0 then
    next_watermark = incoming
end
if compare_uint64(next_watermark, watermark) > 0 then
    redis.call('SET', KEYS[1], next_watermark)
end

if compare_uint64(incoming, current) < 0 or compare_uint64(incoming, legacy) < 0 then
    return 0
end

local existed = redis.call('EXISTS', KEYS[2])
redis.call('HSET', KEYS[2],
    'namespace', ARGV[3],
    'workflow_id', ARGV[4],
    'workflow_version', ARGV[5],
    'entry_unit_id', ARGV[6],
    'replica_index', ARGV[7],
    'node_type', ARGV[8],
    'params', ARGV[9],
    'package_hash', ARGV[10],
    'selector', ARGV[11],
    'requirements', ARGV[12],
    'supplies', ARGV[13],
    'supply_consumers', ARGV[14],
    'desired', ARGV[15],
    'registry_revision', incoming)
if existed == 0 then
    redis.call('HSET', KEYS[2],
        'runner_id', ARGV[16],
        'session_id', ARGV[17],
        'generation', ARGV[18],
        'lease_deadline', ARGV[19],
        'assigned_package_hash', ARGV[20])
end
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[2]))
return 1
`)

// AdvanceWorkflowRevision monotonically advances the workflow-wide
// desired-state watermark before a manager lists or upserts activation keys.
func (s *EntryActivationStore) AdvanceWorkflowRevision(ctx context.Context, ns namespace.Namespace, workflowID types.WorkflowID, revision uint64) error {
	key := entryActivationWorkflowRevisionRedisKey(ns, workflowID)
	if _, err := advanceEntryActivationWorkflowRevisionLua.Run(ctx, s.rdb, []string{key}, revision).Result(); err != nil {
		return fmt.Errorf("advance entry activation workflow revision %q: %w", key, err)
	}
	return nil
}

// Upsert writes the desired-state fields of an activation without touching the
// assignment fields (runner_id/session_id/generation/lease_deadline) of an
// existing record — those are owned by Assign/Fence. A lower record revision or
// workflow watermark makes the operation a successful no-op.
func (s *EntryActivationStore) Upsert(ctx context.Context, act engine.EntryActivation) error {
	key := workflowScopedEntryActivationRedisKey(act.Namespace, act.WorkflowID, act.WorkflowVersion, act.EntryUnitID, act.ReplicaIndex)
	watermarkKey := entryActivationWorkflowRevisionRedisKey(act.Namespace, act.WorkflowID)

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

	var supplyConsumersJSON string
	if len(act.SupplyConsumers) > 0 {
		b, err := json.Marshal(act.SupplyConsumers)
		if err != nil {
			return fmt.Errorf("marshal supply consumers: %w", err)
		}
		supplyConsumersJSON = string(b)
	}

	desired := "0"
	if act.Desired {
		desired = "1"
	}

	// Read assignment defaults from the old layout before creating the modern
	// record. The legacy hash remains discoverable by List and is never deleted;
	// once a watermark advances, reads expose it as non-desired.
	legacyFields, err := s.rdb.HGetAll(ctx, s.legacyKeyFor(entryActivationKeyFromActivation(act))).Result()
	if err != nil {
		return fmt.Errorf("read legacy entry activation for upsert %q: %w", key, err)
	}
	if _, err := upsertEntryActivationLua.Run(ctx, s.rdb,
		[]string{watermarkKey, key},
		act.RegistryRevision, int(s.ttl.Seconds()),
		string(act.Namespace), string(act.WorkflowID), act.WorkflowVersion,
		act.EntryUnitID, act.ReplicaIndex, act.NodeType, paramsJSON,
		act.PackageHash, selectorJSON, requirementsJSON, suppliesJSON,
		supplyConsumersJSON, desired,
		legacyFields["runner_id"], legacyFields["session_id"],
		defaultRedisField(legacyFields, "generation", "0"),
		defaultRedisField(legacyFields, "lease_deadline", "0"),
		legacyFields["assigned_package_hash"],
		defaultRedisField(legacyFields, "registry_revision", "0"),
	).Result(); err != nil {
		return fmt.Errorf("upsert entry activation %q: %w", key, err)
	}
	return nil
}

func entryActivationKeyFromActivation(act engine.EntryActivation) engine.EntryActivationKey {
	return engine.EntryActivationKey{
		Namespace:       act.Namespace,
		WorkflowID:      act.WorkflowID,
		WorkflowVersion: act.WorkflowVersion,
		EntryUnitID:     act.EntryUnitID,
		ReplicaIndex:    act.ReplicaIndex,
	}
}

func defaultRedisField(fields map[string]string, name, fallback string) string {
	if value := fields[name]; value != "" {
		return value
	}
	return fallback
}

// Get returns the activation for key; the bool is false when absent. Modern
// records take precedence, with a legacy-key fallback so pre-migration records
// remain visible. Records below the workflow watermark are returned as
// non-desired without destroying their assignment state.
func (s *EntryActivationStore) Get(ctx context.Context, key engine.EntryActivationKey) (engine.EntryActivation, bool, error) {
	fields, err := s.rdb.HGetAll(ctx, s.keyFor(key)).Result()
	if err != nil {
		return engine.EntryActivation{}, false, fmt.Errorf("get entry activation: %w", err)
	}
	if len(fields) == 0 {
		fields, err = s.rdb.HGetAll(ctx, s.legacyKeyFor(key)).Result()
		if err != nil {
			return engine.EntryActivation{}, false, fmt.Errorf("get legacy entry activation: %w", err)
		}
	}
	if len(fields) == 0 {
		return engine.EntryActivation{}, false, nil
	}
	act, err := decodeEntryActivation(fields)
	if err != nil {
		return engine.EntryActivation{}, false, err
	}
	act, err = s.authoritativeEntryActivation(ctx, act)
	if err != nil {
		return engine.EntryActivation{}, false, err
	}
	return act, true, nil
}

type scannedEntryActivation struct {
	activation engine.EntryActivation
	modern     bool
}

// List returns all activations in the namespace. Both modern and legacy key
// layouts match the scan pattern. Duplicate identities are collapsed with the
// modern record taking precedence; a legacy-only record remains visible.
func (s *EntryActivationStore) List(ctx context.Context, ns namespace.Namespace) ([]engine.EntryActivation, error) {
	patterns := []string{entryActivationScanPattern(ns)}
	if modernPattern := workflowScopedEntryActivationScanPattern(ns); modernPattern != patterns[0] {
		patterns = append(patterns, modernPattern)
	}
	records := make(map[engine.EntryActivationKey]scannedEntryActivation)
	for _, pattern := range patterns {
		var cursor uint64
		for {
			keys, next, err := s.rdb.Scan(ctx, cursor, pattern, 100).Result()
			if err != nil {
				return nil, fmt.Errorf("scan entry activations: %w", err)
			}
			for _, redisKey := range keys {
				fields, err := s.rdb.HGetAll(ctx, redisKey).Result()
				if err != nil {
					return nil, fmt.Errorf("read entry activation %q: %w", redisKey, err)
				}
				if len(fields) == 0 {
					continue
				}
				act, err := decodeEntryActivation(fields)
				if err != nil {
					return nil, err
				}
				// A legacy namespace containing Redis glob metacharacters can make
				// its old scan pattern over-inclusive. Trust the stored identity.
				if act.Namespace != ns {
					continue
				}
				identity := entryActivationKeyFromActivation(act)
				modern := redisKey == s.keyFor(identity)
				previous, exists := records[identity]
				if !exists || modern && !previous.modern {
					records[identity] = scannedEntryActivation{activation: act, modern: modern}
				}
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
	}

	out := make([]engine.EntryActivation, 0, len(records))
	for _, record := range records {
		act, err := s.authoritativeEntryActivation(ctx, record.activation)
		if err != nil {
			return nil, err
		}
		out = append(out, act)
	}
	return out, nil
}

func (s *EntryActivationStore) authoritativeEntryActivation(ctx context.Context, act engine.EntryActivation) (engine.EntryActivation, error) {
	watermark, err := s.workflowRevision(ctx, act.Namespace, act.WorkflowID)
	if err != nil {
		return engine.EntryActivation{}, err
	}
	if act.RegistryRevision < watermark {
		act.Desired = false
	}
	return act, nil
}

func (s *EntryActivationStore) workflowRevision(ctx context.Context, ns namespace.Namespace, workflowID types.WorkflowID) (uint64, error) {
	key := entryActivationWorkflowRevisionRedisKey(ns, workflowID)
	value, err := s.rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get entry activation workflow revision %q: %w", key, err)
	}
	revision, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse entry activation workflow revision %q: %w", value, err)
	}
	return revision, nil
}

// storageKeyForExisting chooses the modern layout when present and otherwise
// falls back to the legacy layout. The checks are intentionally separate:
// modern and legacy keys do not share a Redis Cluster slot.
func (s *EntryActivationStore) storageKeyForExisting(ctx context.Context, key engine.EntryActivationKey) (string, error) {
	modern := s.keyFor(key)
	exists, err := s.rdb.Exists(ctx, modern).Result()
	if err != nil {
		return "", fmt.Errorf("check entry activation %q: %w", modern, err)
	}
	if exists != 0 {
		return modern, nil
	}
	legacy := s.legacyKeyFor(key)
	exists, err = s.rdb.Exists(ctx, legacy).Result()
	if err != nil {
		return "", fmt.Errorf("check legacy entry activation %q: %w", legacy, err)
	}
	if exists != 0 {
		return legacy, nil
	}
	return modern, nil
}

// Assign atomically claims the activation via a single Lua CAS. First-writer-
// wins and monotonic: succeeds only when gen strictly exceeds the stored
// generation.
func (s *EntryActivationStore) Assign(ctx context.Context, key engine.EntryActivationKey, runnerID, sessionID string, gen uint64, deadline time.Time) (bool, error) {
	storageKey, err := s.storageKeyForExisting(ctx, key)
	if err != nil {
		return false, err
	}
	res, err := assignEntryActivationLua.Run(ctx, s.rdb,
		[]string{storageKey},
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
	storageKey, err := s.storageKeyForExisting(ctx, key)
	if err != nil {
		return false, err
	}
	res, err := renewEntryActivationLua.Run(ctx, s.rdb,
		[]string{storageKey},
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
	storageKey, err := s.storageKeyForExisting(ctx, key)
	if err != nil {
		return err
	}
	if _, err := fenceEntryActivationLua.Run(ctx, s.rdb,
		[]string{storageKey},
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
	if replica := fields["replica_index"]; replica != "" {
		parsed, err := strconv.ParseUint(replica, 10, 32)
		if err != nil {
			return engine.EntryActivation{}, fmt.Errorf("parse replica_index %q: %w", replica, err)
		}
		act.ReplicaIndex = uint32(parsed)
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
	// SupplyConsumers is absent on records written before the field existed;
	// decode tolerates absence (leaves SupplyConsumers nil), which leaves the
	// hosting runner registering no consumers — the behaviour that preceded it.
	if sc := fields["supply_consumers"]; sc != "" {
		if err := json.Unmarshal([]byte(sc), &act.SupplyConsumers); err != nil {
			return engine.EntryActivation{}, fmt.Errorf("unmarshal supply consumers: %w", err)
		}
	}
	if revision := fields["registry_revision"]; revision != "" {
		parsed, err := strconv.ParseUint(revision, 10, 64)
		if err != nil {
			return engine.EntryActivation{}, fmt.Errorf("parse registry_revision %q: %w", revision, err)
		}
		act.RegistryRevision = parsed
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
