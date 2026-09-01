package workflowreg

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

const (
	revisionPlaceholder               = `"registry_revision":0`
	workflowProjectionNamespaceSetKey = "xflow:wfreg:v2:projection:namespaces"
)

type Registry struct {
	rdb redis.UniversalClient
}

type storedWorkflowRecord struct {
	ID               types.WorkflowID   `json:"id"`
	Key              string             `json:"key"`
	Namespace        string             `json:"namespace"`
	Name             string             `json:"name"`
	Version          string             `json:"version"`
	DefinitionHash   string             `json:"definition_hash"`
	RegistryRevision uint64             `json:"registry_revision"`
	AuditFingerprint string             `json:"audit_fingerprint,omitempty"`
	Definition       *types.WorkflowDef `json:"definition,omitempty"`
	// Graph is decoded as raw JSON here (not *graph.Graph) so a Graph decode
	// failure does not abort decoding the whole record. unmarshalWorkflowRecord
	// can then fall back to recompiling Definition for legacy snapshots.
	Graph json.RawMessage `json:"graph,omitempty"`
}

var (
	_ backend.WorkflowReplaceCapability        = (*Registry)(nil)
	_ backend.DurableWorkflowReplaceCapability = (*Registry)(nil)
)

func New(rdb redis.UniversalClient) *Registry {
	return &Registry{rdb: rdb}
}

// Registry v2 deliberately co-locates every mutable authority key for one
// namespace in one Redis Cluster slot. The namespace itself is represented by
// a digest, so neither a logical workflow key containing braces nor any other
// caller-controlled suffix can change the first (and effective) hash tag.
//
//	xflow:wfreg:v2:{ns:<sha256(namespace)>}:bykey:<sha256(logical-key)>
//	xflow:wfreg:v2:{ns:<sha256(namespace)>}:byid:<workflow-id>
//	xflow:wfreg:v2:{ns:<sha256(namespace)>}:meta:<workflow-id>
//	xflow:wfreg:v2:{ns:<sha256(namespace)>}:revision
//	xflow:wfreg:v2:{ns:<sha256(namespace)>}:op:<sha256(mutation-id)>
//	xflow:wfreg:v2:{ns:<sha256(namespace)>}:projection:pending
//	xflow:wfreg:v2:{ns:<sha256(namespace)>}:projection:lease:<sha256(mutation-id)>
//	xflow:wfreg:v2:{ns:<sha256(namespace)>}:legacy:bykey:<sha256(logical-key)>
//	xflow:wfreg:v2:{ns:<sha256(namespace)>}:legacy:byid:<sha256(workflow-id)>
//
// bykey and byid are Redis hashes containing the same payload and compact CAS
// metadata. meta repeats only the compact metadata. Keeping a complete payload
// under both lookup dimensions makes GetWorkflow and GetWorkflowByKey each a
// single-snapshot HGET, while Lua can compare metadata without cjson-decoding a
// potentially deeply nested Definition/Graph.
func registryPrefix(t namespace.Namespace) string {
	sum := sha256.Sum256([]byte(t))
	return "xflow:wfreg:v2:{ns:" + hex.EncodeToString(sum[:]) + "}:"
}

func digestKeyComponent(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func workflowByKeyKey(t namespace.Namespace, key string) string {
	return registryPrefix(t) + "bykey:" + digestKeyComponent(key)
}

func workflowByIDKey(t namespace.Namespace, _ string, id types.WorkflowID) string {
	return workflowByIDKeyPrefix(t, "") + string(id)
}

// workflowByIDKeyPrefix remains for compatibility with existing migration
// tests and tooling. v2 no longer needs the logical key to address a record by
// ID because the namespace digest, rather than the logical key, is the slot tag.
func workflowByIDKeyPrefix(t namespace.Namespace, _ string) string {
	return registryPrefix(t) + "byid:"
}

func workflowIDMapKey(t namespace.Namespace, id types.WorkflowID) string {
	return registryPrefix(t) + "meta:" + string(id)
}

// These helpers retain their old package-private signatures for migration test
// compilation. Definition hashes now live as hash fields on bykey/byid/meta;
// this v2-shaped compatibility key is not part of the authority write path.
func workflowDefHashKey(t namespace.Namespace, _ string, id types.WorkflowID) string {
	return workflowDefHashKeyPrefix(t, "") + string(id)
}

func workflowDefHashKeyPrefix(t namespace.Namespace, _ string) string {
	return registryPrefix(t) + "defhash:"
}

func workflowRevisionKey(t namespace.Namespace) string {
	return registryPrefix(t) + "revision"
}

func workflowOperationKey(t namespace.Namespace, mutationID string) string {
	return registryPrefix(t) + "op:" + digestKeyComponent(mutationID)
}

func workflowProjectionPendingKey(t namespace.Namespace) string {
	return registryPrefix(t) + "projection:pending"
}

func workflowProjectionLeaseKey(t namespace.Namespace, mutationID string) string {
	return registryPrefix(t) + "projection:lease:" + digestKeyComponent(mutationID)
}

// Migration markers are permanent v2 tombstones for a legacy identity. They
// are installed atomically with the imported record and intentionally survive
// remove and rename. Without them, a later v2 miss would rediscover the
// read-only v1 record and resurrect a workflow that had already been mutated.
func workflowLegacyByKeyMarker(t namespace.Namespace, key string) string {
	return registryPrefix(t) + "legacy:bykey:" + digestKeyComponent(key)
}

func workflowLegacyByIDMarker(t namespace.Namespace, id types.WorkflowID) string {
	return registryPrefix(t) + "legacy:byid:" + digestKeyComponent(string(id))
}

// Legacy v1 helpers are deliberately private and read-only. All upgraded
// writers target the namespace-digest v2 keys above; these helpers exist only
// for lazy read-through/import of records deployed before registry v2.
func legacyWorkflowByKeyKey(t namespace.Namespace, key string) string {
	return "xflow:ns:" + string(t) + ":workflow:{" + key + "}:bykey"
}

func legacyWorkflowByIDKey(t namespace.Namespace, key string, id types.WorkflowID) string {
	return "xflow:ns:" + string(t) + ":workflow:{" + key + "}:byid:" + string(id)
}

func legacyWorkflowIDMapKey(t namespace.Namespace, id types.WorkflowID) string {
	return "xflow:ns:" + string(t) + ":workflow:idmap:" + string(id)
}

func legacyWorkflowDefHashKey(t namespace.Namespace, key string, id types.WorkflowID) string {
	return "xflow:ns:" + string(t) + ":workflow:{" + key + "}:defhash:" + string(id)
}

// KeyByID, KeyByKey, and KeyIDMap preserve the public compatibility signatures
// while returning v2 keys. key is intentionally ignored by KeyByID: all records
// in a namespace now share the namespace digest slot.
func KeyByID(t namespace.Namespace, key string, id types.WorkflowID) string {
	return workflowByIDKey(t, key, id)
}

func KeyByKey(t namespace.Namespace, key string) string {
	return workflowByKeyKey(t, key)
}

func KeyIDMap(t namespace.Namespace, id types.WorkflowID) string {
	return workflowIDMapKey(t, id)
}

// Every Redis key touched by these scripts is supplied through KEYS. ARGV only
// carries values and is never concatenated into, or otherwise used as, a key.
var addWorkflowRecordLua = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
	local existingHash = redis.call('HGET', KEYS[1], 'hash')
	local existingPayload = redis.call('HGET', KEYS[1], 'payload')
	if existingHash == ARGV[4] and existingPayload then
		return {'existing', existingPayload}
	end
	return {'conflict', ''}
end
if redis.call('EXISTS', KEYS[2]) == 1 or redis.call('EXISTS', KEYS[3]) == 1 then
	return {'id_conflict', ''}
end
if not string.find(ARGV[6], '"registry_revision":0', 1, true) then
	return redis.error_reply('workflow payload has no revision placeholder')
end
local revision = redis.call('INCR', KEYS[4])
local payload = string.gsub(ARGV[6], '"registry_revision":0', '"registry_revision":' .. tostring(revision), 1)
redis.call('HSET', KEYS[1],
	'id', ARGV[1], 'key', ARGV[2], 'version', ARGV[3], 'hash', ARGV[4],
	'revision', tostring(revision), 'fingerprint', ARGV[5], 'payload', payload)
redis.call('HSET', KEYS[2],
	'id', ARGV[1], 'key', ARGV[2], 'version', ARGV[3], 'hash', ARGV[4],
	'revision', tostring(revision), 'fingerprint', ARGV[5], 'payload', payload)
redis.call('HSET', KEYS[3],
	'id', ARGV[1], 'key', ARGV[2], 'version', ARGV[3], 'hash', ARGV[4],
	'revision', tostring(revision), 'fingerprint', ARGV[5])
return {'created', payload}
`)

// importLegacyWorkflowRecordLua atomically claims both v1 identity markers and
// installs a complete v2 record. A marker observed without an exact live v2
// record means the legacy identity was already removed or renamed and must not
// be resurrected. Every dynamic Redis key is supplied through KEYS.
var importLegacyWorkflowRecordLua = redis.NewScript(`
local function matches(redisKey)
	return redis.call('HGET', redisKey, 'id') == ARGV[1]
		and redis.call('HGET', redisKey, 'key') == ARGV[2]
end
local exact = redis.call('EXISTS', KEYS[1]) == 1
	and redis.call('EXISTS', KEYS[2]) == 1
	and redis.call('EXISTS', KEYS[3]) == 1
	and matches(KEYS[1]) and matches(KEYS[2]) and matches(KEYS[3])
if exact then
	local payload = redis.call('HGET', KEYS[2], 'payload')
	if not payload then
		return {'id_conflict', ''}
	end
	redis.call('SET', KEYS[5], ARGV[1])
	redis.call('SET', KEYS[6], ARGV[2])
	return {'existing', payload}
end
if redis.call('EXISTS', KEYS[5]) == 1 or redis.call('EXISTS', KEYS[6]) == 1 then
	return {'retired', ''}
end
if redis.call('EXISTS', KEYS[1]) == 1 then
	return {'key_conflict', ''}
end
if redis.call('EXISTS', KEYS[2]) == 1 or redis.call('EXISTS', KEYS[3]) == 1 then
	return {'id_conflict', ''}
end
if not string.find(ARGV[6], '"registry_revision":0', 1, true) then
	return redis.error_reply('workflow payload has no revision placeholder')
end
local revision = redis.call('INCR', KEYS[4])
local payload = string.gsub(ARGV[6], '"registry_revision":0', '"registry_revision":' .. tostring(revision), 1)
redis.call('HSET', KEYS[1],
	'id', ARGV[1], 'key', ARGV[2], 'version', ARGV[3], 'hash', ARGV[4],
	'revision', tostring(revision), 'fingerprint', ARGV[5], 'payload', payload)
redis.call('HSET', KEYS[2],
	'id', ARGV[1], 'key', ARGV[2], 'version', ARGV[3], 'hash', ARGV[4],
	'revision', tostring(revision), 'fingerprint', ARGV[5], 'payload', payload)
redis.call('HSET', KEYS[3],
	'id', ARGV[1], 'key', ARGV[2], 'version', ARGV[3], 'hash', ARGV[4],
	'revision', tostring(revision), 'fingerprint', ARGV[5])
redis.call('SET', KEYS[5], ARGV[1])
redis.call('SET', KEYS[6], ARGV[2])
return {'imported', payload}
`)

var updateWorkflowHashLua = redis.NewScript(`
local function matches(redisKey)
	return redis.call('HGET', redisKey, 'id') == ARGV[1]
		and redis.call('HGET', redisKey, 'key') == ARGV[2]
		and redis.call('HGET', redisKey, 'revision') == ARGV[4]
end
if redis.call('EXISTS', KEYS[1]) == 0
	or redis.call('EXISTS', KEYS[2]) == 0
	or redis.call('EXISTS', KEYS[3]) == 0 then
	return {'notfound', ''}
end
if not matches(KEYS[1]) or not matches(KEYS[2]) or not matches(KEYS[3]) then
	return {'conflict', ''}
end
local currentHash = redis.call('HGET', KEYS[1], 'hash')
if currentHash == ARGV[5] then
	return {'unchanged', redis.call('HGET', KEYS[1], 'payload') or ''}
end
if currentHash ~= ARGV[3] then
	return {'conflict', ''}
end
if not string.find(ARGV[7], '"registry_revision":0', 1, true) then
	return redis.error_reply('workflow payload has no revision placeholder')
end
local revision = redis.call('INCR', KEYS[4])
local payload = string.gsub(ARGV[7], '"registry_revision":0', '"registry_revision":' .. tostring(revision), 1)
redis.call('HSET', KEYS[1],
	'id', ARGV[1], 'key', ARGV[2], 'version', ARGV[8], 'hash', ARGV[5],
	'revision', tostring(revision), 'fingerprint', ARGV[6], 'payload', payload)
redis.call('HSET', KEYS[2],
	'id', ARGV[1], 'key', ARGV[2], 'version', ARGV[8], 'hash', ARGV[5],
	'revision', tostring(revision), 'fingerprint', ARGV[6], 'payload', payload)
redis.call('HSET', KEYS[3],
	'id', ARGV[1], 'key', ARGV[2], 'version', ARGV[8], 'hash', ARGV[5],
	'revision', tostring(revision), 'fingerprint', ARGV[6])
return {'ok', payload}
`)

var removeWorkflowRecordLua = redis.NewScript(`
local function recordMatches(redisKey)
	return redis.call('HGET', redisKey, 'id') == ARGV[1]
		and redis.call('HGET', redisKey, 'key') == ARGV[2]
		and redis.call('HGET', redisKey, 'revision') == ARGV[3]
end
local hasByID = redis.call('EXISTS', KEYS[1]) == 1
local hasByKey = redis.call('EXISTS', KEYS[2]) == 1
local hasMeta = redis.call('EXISTS', KEYS[3]) == 1
if hasByID and not recordMatches(KEYS[1]) then
	return {'retry'}
end
if hasByKey and not recordMatches(KEYS[2]) then
	return {'retry'}
end
if hasMeta and not recordMatches(KEYS[3]) then
	return {'retry'}
end
redis.call('DEL', KEYS[1], KEYS[2], KEYS[3])
if not hasByID and not hasByKey then
	return {'notfound'}
end
return {'ok'}
`)

var compareAndReplaceWorkflowLua = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
	local ledgerMutationID = redis.call('HGET', KEYS[1], 'mutation_id')
	local ledgerFingerprint = redis.call('HGET', KEYS[1], 'fingerprint')
	if ledgerMutationID ~= ARGV[1] or ledgerFingerprint ~= ARGV[2] then
		return {'mutation_reused', '', '', '', '', '', ''}
	end
	return {
		redis.call('HGET', KEYS[1], 'status') or '',
		redis.call('HGET', KEYS[1], 'current_payload') or '',
		redis.call('HGET', KEYS[1], 'previous_id') or '',
		redis.call('HGET', KEYS[1], 'previous_key') or '',
		redis.call('HGET', KEYS[1], 'previous_version') or '',
		redis.call('HGET', KEYS[1], 'previous_hash') or '',
		redis.call('HGET', KEYS[1], 'previous_revision') or ''
	}
end

local function sourceMatches(redisKey)
	return redis.call('HGET', redisKey, 'id') == ARGV[3]
		and redis.call('HGET', redisKey, 'key') == ARGV[4]
		and redis.call('HGET', redisKey, 'hash') == ARGV[5]
		and redis.call('HGET', redisKey, 'revision') == ARGV[6]
		and redis.call('HGET', redisKey, 'version') == ARGV[15]
end
if redis.call('EXISTS', KEYS[2]) == 0
	or redis.call('EXISTS', KEYS[3]) == 0
	or redis.call('EXISTS', KEYS[4]) == 0
	or not sourceMatches(KEYS[2])
	or not sourceMatches(KEYS[3])
	or not sourceMatches(KEYS[4]) then
	return {'stale_revision', '', '', '', '', '', ''}
end

if ARGV[13] == '1' or (ARGV[8] ~= ARGV[4] and redis.call('EXISTS', KEYS[6]) == 1) then
	return {'destination_key_occupied', '', '', '', '', '', ''}
end
if ARGV[14] == '1' or (ARGV[7] ~= ARGV[3]
	and (redis.call('EXISTS', KEYS[5]) == 1 or redis.call('EXISTS', KEYS[7]) == 1)) then
	return {'destination_id_occupied', '', '', '', '', '', ''}
end

local previousVersion = redis.call('HGET', KEYS[2], 'version') or ''
local previousPayload = redis.call('HGET', KEYS[2], 'payload') or ''
local previousFingerprint = redis.call('HGET', KEYS[2], 'fingerprint') or ''
if ARGV[7] == ARGV[3] and ARGV[8] == ARGV[4] and ARGV[11] == previousFingerprint then
	redis.call('HSET', KEYS[1],
		'mutation_id', ARGV[1], 'fingerprint', ARGV[2], 'status', 'unchanged',
		'current_payload', previousPayload, 'previous_id', ARGV[3],
		'previous_key', ARGV[4], 'previous_version', previousVersion,
		'previous_hash', ARGV[5], 'previous_revision', ARGV[6],
		'projection_namespace', ARGV[16], 'projection_state', 'pending')
	redis.call('SADD', KEYS[9], ARGV[1])
	return {'unchanged', previousPayload, ARGV[3], ARGV[4], previousVersion, ARGV[5], ARGV[6]}
end

if not string.find(ARGV[12], '"registry_revision":0', 1, true) then
	return redis.error_reply('workflow payload has no revision placeholder')
end
local revision = redis.call('INCR', KEYS[8])
local payload = string.gsub(ARGV[12], '"registry_revision":0', '"registry_revision":' .. tostring(revision), 1)
redis.call('DEL', KEYS[2], KEYS[3], KEYS[4])
redis.call('HSET', KEYS[5],
	'id', ARGV[7], 'key', ARGV[8], 'version', ARGV[9], 'hash', ARGV[10],
	'revision', tostring(revision), 'fingerprint', ARGV[11], 'payload', payload)
redis.call('HSET', KEYS[6],
	'id', ARGV[7], 'key', ARGV[8], 'version', ARGV[9], 'hash', ARGV[10],
	'revision', tostring(revision), 'fingerprint', ARGV[11], 'payload', payload)
redis.call('HSET', KEYS[7],
	'id', ARGV[7], 'key', ARGV[8], 'version', ARGV[9], 'hash', ARGV[10],
	'revision', tostring(revision), 'fingerprint', ARGV[11])
redis.call('HSET', KEYS[1],
	'mutation_id', ARGV[1], 'fingerprint', ARGV[2], 'status', 'replaced',
	'current_payload', payload, 'previous_id', ARGV[3],
	'previous_key', ARGV[4], 'previous_version', previousVersion,
	'previous_hash', ARGV[5], 'previous_revision', ARGV[6],
	'projection_namespace', ARGV[16], 'projection_state', 'pending')
redis.call('SADD', KEYS[9], ARGV[1])
return {'replaced', payload, ARGV[3], ARGV[4], previousVersion, ARGV[5], ARGV[6]}
`)

// Claim and Ack use Redis TIME rather than a caller clock. The lease deadline
// is therefore authoritative across control-plane replicas. Every dynamic key
// is supplied in KEYS, and all three keys share the namespace digest slot.
var claimWorkflowActivationProjectionLua = redis.NewScript(`
local function nowMilliseconds()
	local now = redis.call('TIME')
	return tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
end

local function millisecondsText(value)
	return string.format('%.0f', value or 0)
end

local function intentResult(state, deadline)
	return {
		state,
		redis.call('HGET', KEYS[1], 'projection_namespace') or '',
		redis.call('HGET', KEYS[1], 'mutation_id') or '',
		redis.call('HGET', KEYS[1], 'current_payload') or '',
		redis.call('HGET', KEYS[1], 'previous_id') or '',
		redis.call('HGET', KEYS[1], 'previous_key') or '',
		redis.call('HGET', KEYS[1], 'previous_version') or '',
		redis.call('HGET', KEYS[1], 'previous_hash') or '',
		redis.call('HGET', KEYS[1], 'previous_revision') or '',
		millisecondsText(deadline)
	}
end

if redis.call('EXISTS', KEYS[1]) == 0 then
	return {'missing', '', '', '', '', '', '', '', '', '0'}
end
if redis.call('HGET', KEYS[1], 'mutation_id') ~= ARGV[1] then
	return {'invalid', '', '', '', '', '', '', '', '', '0'}
end
local projectionState = redis.call('HGET', KEYS[1], 'projection_state') or ''
if projectionState == 'applied' then
	return intentResult('applied', 0)
end
if projectionState ~= 'pending' or redis.call('SISMEMBER', KEYS[2], ARGV[1]) ~= 1 then
	return {'missing', '', '', '', '', '', '', '', '', '0'}
end

local now = nowMilliseconds()
local owner = redis.call('HGET', KEYS[3], 'token') or ''
local deadline = tonumber(redis.call('HGET', KEYS[3], 'deadline_ms') or '0')
if owner ~= '' and deadline > now then
	if owner == ARGV[2] then
		return intentResult('acquired', deadline)
	end
	return {'busy', '', '', '', '', '', '', '', '', millisecondsText(deadline)}
end

deadline = now + tonumber(ARGV[3])
local deadlineText = millisecondsText(deadline)
redis.call('HSET', KEYS[3], 'token', ARGV[2], 'deadline_ms', deadlineText)
redis.call('PEXPIREAT', KEYS[3], deadlineText)
return intentResult('acquired', deadline)
`)

var ackWorkflowActivationProjectionLua = redis.NewScript(`
local function nowMilliseconds()
	local now = redis.call('TIME')
	return tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
end

local function millisecondsText(value)
	return string.format('%.0f', value or 0)
end

if redis.call('EXISTS', KEYS[1]) == 0 then
	return {'missing'}
end
if redis.call('HGET', KEYS[1], 'mutation_id') ~= ARGV[1] then
	return {'invalid'}
end
local projectionState = redis.call('HGET', KEYS[1], 'projection_state') or ''
if projectionState == 'applied' then
	redis.call('SREM', KEYS[2], ARGV[1])
	redis.call('DEL', KEYS[3])
	return {'applied'}
end
if projectionState ~= 'pending' then
	return {'missing'}
end
local now = nowMilliseconds()
local owner = redis.call('HGET', KEYS[3], 'token') or ''
local deadline = tonumber(redis.call('HGET', KEYS[3], 'deadline_ms') or '0')
if owner == '' or owner ~= ARGV[2] or deadline <= now then
	return {'stale'}
end
redis.call('HSET', KEYS[1], 'projection_state', 'applied', 'projection_applied_at_ms', millisecondsText(now))
redis.call('SREM', KEYS[2], ARGV[1])
redis.call('DEL', KEYS[3])
return {'acked'}
`)

// Pending refs are sorted and paged from one Redis snapshot. The cursor is an
// opaque zero-based offset into that snapshot. Concurrent set changes may make
// refs repeat or move between pages, which the at-least-once contract permits;
// restarting a pass at cursor zero eventually rediscovers every pending ref.
var listPendingWorkflowActivationProjectionsLua = redis.NewScript(`
local members = redis.call('SMEMBERS', KEYS[1])
table.sort(members)

local offset = tonumber(ARGV[1]) or 0
local limit = tonumber(ARGV[2]) or 0
local result = {}
if offset >= #members or limit <= 0 then
	result[1] = '0'
	return result
end

local last = math.min(offset + limit, #members)
if last < #members then
	result[1] = tostring(last)
else
	result[1] = '0'
end
for index = offset + 1, last do
	result[#result + 1] = members[index]
end
return result
`)

func (r *Registry) AddWorkflow(ctx context.Context, rec backend.WorkflowRecord) (backend.WorkflowRecord, error) {
	t := namespace.FromContext(ctx)
	if err := namespace.Validate(t); err != nil {
		return backend.WorkflowRecord{}, fmt.Errorf("add workflow: %w", err)
	}

	// A v1 by-key record must be imported before Add evaluates its normal
	// key/hash semantics. GetWorkflowByKey checks v2 first and performs the
	// conflict-safe lazy import only on a miss.
	existing, err := r.GetWorkflowByKey(ctx, rec.Key)
	if err == nil {
		if existing.DefinitionHash != rec.DefinitionHash {
			return backend.WorkflowRecord{}, backend.ErrWorkflowConflict
		}
		return existing, nil
	}
	if !errors.Is(err, backend.ErrWorkflowNotFound) {
		if errors.Is(err, backend.ErrWorkflowConflict) || errors.Is(err, backend.ErrWorkflowMutationIndeterminate) {
			return backend.WorkflowRecord{}, err
		}
		return backend.WorkflowRecord{}, indeterminateMutation("add workflow "+strconv.Quote(rec.Key), err)
	}

	stored, err := marshalWorkflowRecord(rec)
	if err != nil {
		return backend.WorkflowRecord{}, err
	}
	// Preserve v1 ID uniqueness as well as key uniqueness. Importing an old
	// occupant makes the v2 Add script's ID conflict check authoritative.
	if _, lookupErr := r.GetWorkflow(ctx, stored.ID); lookupErr != nil && !errors.Is(lookupErr, backend.ErrWorkflowNotFound) {
		if errors.Is(lookupErr, backend.ErrWorkflowConflict) || errors.Is(lookupErr, backend.ErrWorkflowMutationIndeterminate) {
			return backend.WorkflowRecord{}, lookupErr
		}
		return backend.WorkflowRecord{}, indeterminateMutation("add workflow "+strconv.Quote(stored.Key), lookupErr)
	}

	return r.addWorkflowV2(ctx, t, stored)
}

func (r *Registry) addWorkflowV2(ctx context.Context, t namespace.Namespace, stored *marshaledWorkflowRecord) (backend.WorkflowRecord, error) {
	result, err := addWorkflowRecordLua.Run(
		ctx,
		r.rdb,
		[]string{
			workflowByKeyKey(t, stored.Key),
			workflowByIDKey(t, stored.Key, stored.ID),
			workflowIDMapKey(t, stored.ID),
			workflowRevisionKey(t),
			workflowProjectionPendingKey(t),
		},
		string(stored.ID),
		stored.Key,
		stored.Version,
		stored.DefinitionHash,
		stored.fingerprint,
		stored.payload,
	).Result()
	if err != nil {
		return backend.WorkflowRecord{}, indeterminateMutation("add workflow "+strconv.Quote(stored.Key), err)
	}

	state, payload, err := decodeTwoPartScriptResult(result)
	if err != nil {
		return backend.WorkflowRecord{}, indeterminateMutation("add workflow "+strconv.Quote(stored.Key), err)
	}
	switch state {
	case "conflict", "id_conflict":
		return backend.WorkflowRecord{}, backend.ErrWorkflowConflict
	case "created", "existing":
		record, decodeErr := unmarshalWorkflowRecord(payload)
		if decodeErr != nil {
			return backend.WorkflowRecord{}, indeterminateMutation("add workflow "+strconv.Quote(stored.Key), decodeErr)
		}
		return record, nil
	default:
		return backend.WorkflowRecord{}, indeterminateMutation(
			"add workflow "+strconv.Quote(stored.Key),
			fmt.Errorf("unexpected script state %q", state),
		)
	}
}

func (r *Registry) GetWorkflow(ctx context.Context, id types.WorkflowID) (backend.WorkflowRecord, error) {
	t := namespace.FromContext(ctx)
	if err := namespace.Validate(t); err != nil {
		return backend.WorkflowRecord{}, fmt.Errorf("get workflow: %w", err)
	}

	raw, err := r.rdb.HGet(ctx, workflowByIDKey(t, "", id), "payload").Bytes()
	if errors.Is(err, redis.Nil) {
		legacy, found, legacyErr := r.loadLegacyWorkflowByID(ctx, t, id)
		if legacyErr != nil {
			return backend.WorkflowRecord{}, fmt.Errorf("get workflow %q: %w", id, legacyErr)
		}
		if !found {
			// A concurrent lazy import can create the v2 record and its permanent
			// legacy marker after the first HGET but before loadLegacyWorkflowByID.
			// Re-read v2 once so the marker is not mistaken for a completed remove.
			raw, retryErr := r.rdb.HGet(ctx, workflowByIDKey(t, "", id), "payload").Bytes()
			if retryErr == nil {
				record, decodeErr := unmarshalWorkflowRecord(raw)
				if decodeErr != nil {
					return backend.WorkflowRecord{}, fmt.Errorf("get workflow %q: %w", id, decodeErr)
				}
				if record.ID == id {
					return record, nil
				}
			}
			if retryErr != nil && !errors.Is(retryErr, redis.Nil) {
				return backend.WorkflowRecord{}, fmt.Errorf("get workflow %q: %w", id, retryErr)
			}
			return backend.WorkflowRecord{}, backend.ErrWorkflowNotFound
		}
		imported, importErr := r.importLegacyWorkflow(ctx, t, legacy)
		if importErr != nil {
			return backend.WorkflowRecord{}, importErr
		}
		if imported.ID != id {
			return backend.WorkflowRecord{}, backend.ErrWorkflowNotFound
		}
		return imported, nil
	}
	if err != nil {
		return backend.WorkflowRecord{}, fmt.Errorf("get workflow %q: %w", id, err)
	}
	record, err := unmarshalWorkflowRecord(raw)
	if err != nil {
		return backend.WorkflowRecord{}, fmt.Errorf("get workflow %q: %w", id, err)
	}
	if record.ID != id {
		return backend.WorkflowRecord{}, backend.ErrWorkflowNotFound
	}
	return record, nil
}

func (r *Registry) GetWorkflowByKey(ctx context.Context, key string) (backend.WorkflowRecord, error) {
	t := namespace.FromContext(ctx)
	if err := namespace.Validate(t); err != nil {
		return backend.WorkflowRecord{}, fmt.Errorf("get workflow by key: %w", err)
	}

	raw, err := r.rdb.HGet(ctx, workflowByKeyKey(t, key), "payload").Bytes()
	if errors.Is(err, redis.Nil) {
		legacy, found, legacyErr := r.loadLegacyWorkflowByKey(ctx, t, key)
		if legacyErr != nil {
			return backend.WorkflowRecord{}, fmt.Errorf("get workflow by key %q: %w", key, legacyErr)
		}
		if !found {
			// See GetWorkflow: a concurrent import may have installed v2 between
			// our first miss and observing the legacy tombstone.
			raw, retryErr := r.rdb.HGet(ctx, workflowByKeyKey(t, key), "payload").Bytes()
			if retryErr == nil {
				record, decodeErr := unmarshalWorkflowRecord(raw)
				if decodeErr != nil {
					return backend.WorkflowRecord{}, fmt.Errorf("get workflow by key %q: %w", key, decodeErr)
				}
				if record.Key == key {
					return record, nil
				}
			}
			if retryErr != nil && !errors.Is(retryErr, redis.Nil) {
				return backend.WorkflowRecord{}, fmt.Errorf("get workflow by key %q: %w", key, retryErr)
			}
			return backend.WorkflowRecord{}, backend.ErrWorkflowNotFound
		}
		imported, importErr := r.importLegacyWorkflow(ctx, t, legacy)
		if importErr != nil {
			return backend.WorkflowRecord{}, importErr
		}
		if imported.Key != key {
			return backend.WorkflowRecord{}, backend.ErrWorkflowNotFound
		}
		return imported, nil
	}
	if err != nil {
		return backend.WorkflowRecord{}, fmt.Errorf("get workflow by key %q: %w", key, err)
	}
	record, err := unmarshalWorkflowRecord(raw)
	if err != nil {
		return backend.WorkflowRecord{}, fmt.Errorf("get workflow by key %q: %w", key, err)
	}
	if record.Key != key {
		return backend.WorkflowRecord{}, backend.ErrWorkflowNotFound
	}
	return record, nil
}

func (r *Registry) loadLegacyWorkflowByID(ctx context.Context, t namespace.Namespace, id types.WorkflowID) (backend.WorkflowRecord, bool, error) {
	retired, err := r.rdb.Exists(ctx, workflowLegacyByIDMarker(t, id)).Result()
	if err != nil {
		return backend.WorkflowRecord{}, false, err
	}
	if retired != 0 {
		return backend.WorkflowRecord{}, false, nil
	}
	key, err := r.rdb.Get(ctx, legacyWorkflowIDMapKey(t, id)).Result()
	if errors.Is(err, redis.Nil) {
		return backend.WorkflowRecord{}, false, nil
	}
	if err != nil {
		return backend.WorkflowRecord{}, false, err
	}
	return r.loadLegacyWorkflowRecord(ctx, t, key, id)
}

func (r *Registry) loadLegacyWorkflowByKey(ctx context.Context, t namespace.Namespace, key string) (backend.WorkflowRecord, bool, error) {
	retired, err := r.rdb.Exists(ctx, workflowLegacyByKeyMarker(t, key)).Result()
	if err != nil {
		return backend.WorkflowRecord{}, false, err
	}
	if retired != 0 {
		return backend.WorkflowRecord{}, false, nil
	}
	idText, err := r.rdb.Get(ctx, legacyWorkflowByKeyKey(t, key)).Result()
	if errors.Is(err, redis.Nil) {
		return backend.WorkflowRecord{}, false, nil
	}
	if err != nil {
		return backend.WorkflowRecord{}, false, err
	}
	return r.loadLegacyWorkflowRecord(ctx, t, key, types.WorkflowID(idText))
}

func (r *Registry) loadLegacyWorkflowRecord(ctx context.Context, t namespace.Namespace, key string, id types.WorkflowID) (backend.WorkflowRecord, bool, error) {
	if key == "" || id == "" {
		return backend.WorkflowRecord{}, false, nil
	}
	raw, err := r.rdb.Get(ctx, legacyWorkflowByIDKey(t, key, id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return backend.WorkflowRecord{}, false, nil
	}
	if err != nil {
		return backend.WorkflowRecord{}, false, err
	}
	record, err := unmarshalWorkflowRecord(raw)
	if err != nil {
		return backend.WorkflowRecord{}, false, err
	}
	if record.ID != id || record.Key != key {
		return backend.WorkflowRecord{}, false, fmt.Errorf("legacy workflow index mismatch: pointer id=%q key=%q, payload id=%q key=%q", id, key, record.ID, record.Key)
	}
	return record, true, nil
}

func (r *Registry) importLegacyWorkflow(ctx context.Context, t namespace.Namespace, rec backend.WorkflowRecord) (backend.WorkflowRecord, error) {
	if rec.ID == "" || rec.Key == "" {
		return backend.WorkflowRecord{}, errors.New("import legacy workflow: record identity is empty")
	}
	stored, err := marshalWorkflowRecord(rec)
	if err != nil {
		return backend.WorkflowRecord{}, fmt.Errorf("import legacy workflow %q: %w", rec.Key, err)
	}
	result, err := importLegacyWorkflowRecordLua.Run(
		ctx,
		r.rdb,
		[]string{
			workflowByKeyKey(t, stored.Key),
			workflowByIDKey(t, stored.Key, stored.ID),
			workflowIDMapKey(t, stored.ID),
			workflowRevisionKey(t),
			workflowLegacyByKeyMarker(t, stored.Key),
			workflowLegacyByIDMarker(t, stored.ID),
		},
		string(stored.ID),
		stored.Key,
		stored.Version,
		stored.DefinitionHash,
		stored.fingerprint,
		stored.payload,
	).Result()
	if err != nil {
		return backend.WorkflowRecord{}, indeterminateMutation("import legacy workflow "+strconv.Quote(stored.Key), err)
	}
	state, payload, err := decodeTwoPartScriptResult(result)
	if err != nil {
		return backend.WorkflowRecord{}, indeterminateMutation("import legacy workflow "+strconv.Quote(stored.Key), err)
	}
	switch state {
	case "imported", "existing":
		record, decodeErr := unmarshalWorkflowRecord(payload)
		if decodeErr != nil {
			return backend.WorkflowRecord{}, indeterminateMutation("import legacy workflow "+strconv.Quote(stored.Key), decodeErr)
		}
		return record, nil
	case "retired":
		return backend.WorkflowRecord{}, backend.ErrWorkflowNotFound
	case "key_conflict", "id_conflict":
		return backend.WorkflowRecord{}, backend.ErrWorkflowConflict
	default:
		return backend.WorkflowRecord{}, indeterminateMutation(
			"import legacy workflow "+strconv.Quote(stored.Key),
			fmt.Errorf("unexpected script state %q", state),
		)
	}
}

// UpdateDefinitionHash upgrades the stored hash only if expectedOldHash and the
// record revision read for payload re-marshalling are still authoritative. A
// successful update receives a fresh namespace-global revision; setting the
// already-current hash is an idempotent no-op and does not consume a revision.
func (r *Registry) UpdateDefinitionHash(ctx context.Context, id types.WorkflowID, expectedOldHash, newHash string) error {
	t := namespace.FromContext(ctx)
	if err := namespace.Validate(t); err != nil {
		return fmt.Errorf("update workflow hash: %w", err)
	}

	existing, err := r.GetWorkflow(ctx, id)
	if err != nil {
		if errors.Is(err, backend.ErrWorkflowNotFound) || errors.Is(err, backend.ErrWorkflowConflict) || errors.Is(err, backend.ErrWorkflowMutationIndeterminate) {
			return err
		}
		return indeterminateMutation("update workflow hash "+strconv.Quote(string(id)), err)
	}
	if existing.DefinitionHash == newHash {
		return nil
	}
	if existing.DefinitionHash != expectedOldHash {
		return backend.ErrWorkflowConflict
	}

	expectedRevision := existing.RegistryRevision
	existing.DefinitionHash = newHash
	existing.RegistryRevision = 0
	payload, err := marshalWorkflowRecordPayload(existing)
	if err != nil {
		return fmt.Errorf("update workflow hash %q: %w", id, err)
	}
	fingerprint := semanticFingerprint(payload)
	result, err := updateWorkflowHashLua.Run(
		ctx,
		r.rdb,
		[]string{
			workflowByIDKey(t, existing.Key, id),
			workflowByKeyKey(t, existing.Key),
			workflowIDMapKey(t, id),
			workflowRevisionKey(t),
		},
		string(id),
		existing.Key,
		expectedOldHash,
		strconv.FormatUint(expectedRevision, 10),
		newHash,
		fingerprint,
		payload,
		existing.Version,
	).Result()
	if err != nil {
		return indeterminateMutation("update workflow hash "+strconv.Quote(string(id)), err)
	}
	state, _, err := decodeTwoPartScriptResult(result)
	if err != nil {
		return indeterminateMutation("update workflow hash "+strconv.Quote(string(id)), err)
	}
	switch state {
	case "ok", "unchanged":
		return nil
	case "conflict":
		return backend.ErrWorkflowConflict
	case "notfound":
		return backend.ErrWorkflowNotFound
	default:
		return indeterminateMutation(
			"update workflow hash "+strconv.Quote(string(id)),
			fmt.Errorf("unexpected script state %q", state),
		)
	}
}

func (r *Registry) RemoveWorkflow(ctx context.Context, id types.WorkflowID) error {
	t := namespace.FromContext(ctx)
	if err := namespace.Validate(t); err != nil {
		return fmt.Errorf("remove workflow: %w", err)
	}

	const maxCASRetries = 16
	for attempt := 0; attempt < maxCASRetries; attempt++ {
		key, revision, found, err := r.removeSnapshot(ctx, t, id)
		if err != nil {
			if errors.Is(err, backend.ErrWorkflowMutationIndeterminate) {
				return err
			}
			return indeterminateMutation("remove workflow "+strconv.Quote(string(id)), err)
		}
		if !found {
			return backend.ErrWorkflowNotFound
		}

		result, err := removeWorkflowRecordLua.Run(
			ctx,
			r.rdb,
			[]string{
				workflowByIDKey(t, key, id),
				workflowByKeyKey(t, key),
				workflowIDMapKey(t, id),
			},
			string(id),
			key,
			strconv.FormatUint(revision, 10),
		).Result()
		if err != nil {
			return indeterminateMutation("remove workflow "+strconv.Quote(string(id)), err)
		}
		state, err := decodeSingleStateScriptResult(result)
		if err != nil {
			return indeterminateMutation("remove workflow "+strconv.Quote(string(id)), err)
		}
		switch state {
		case "ok":
			return nil
		case "notfound":
			return backend.ErrWorkflowNotFound
		case "retry":
			continue
		default:
			return indeterminateMutation(
				"remove workflow "+strconv.Quote(string(id)),
				fmt.Errorf("unexpected script state %q", state),
			)
		}
	}
	return fmt.Errorf("remove workflow %q: registry record changed during %d compare-and-delete attempts: %w", id, maxCASRetries, backend.ErrWorkflowConflict)
}

func (r *Registry) removeSnapshot(ctx context.Context, t namespace.Namespace, id types.WorkflowID) (string, uint64, bool, error) {
	values, err := r.rdb.HMGet(ctx, workflowIDMapKey(t, id), "key", "revision").Result()
	if err != nil {
		return "", 0, false, err
	}
	if len(values) == 2 && values[0] != nil && values[1] != nil {
		key, keyOK := values[0].(string)
		revisionText, revisionOK := values[1].(string)
		if keyOK && revisionOK {
			revision, parseErr := strconv.ParseUint(revisionText, 10, 64)
			if parseErr != nil {
				return "", 0, false, fmt.Errorf("parse registry revision: %w", parseErr)
			}
			return key, revision, true, nil
		}
	}

	// A missing meta hash is corrupt but recoverable when byid still has a full
	// payload. Use it only to identify the complete KEYS set; the Lua script
	// re-validates all surviving indexes before deleting anything.
	record, err := r.GetWorkflow(ctx, id)
	if errors.Is(err, backend.ErrWorkflowNotFound) {
		return "", 0, false, nil
	}
	if err != nil {
		return "", 0, false, err
	}
	return record.Key, record.RegistryRevision, true, nil
}

// CompareAndReplaceWorkflow replaces one authoritative record and every index
// in one Lua transaction. Its durable operation ledger is checked before the
// source CAS, so retrying after a lost response returns the original result
// even when the caller has re-read and supplied the now-current revision.
func (r *Registry) CompareAndReplaceWorkflow(ctx context.Context, req backend.WorkflowReplaceRequest) (backend.WorkflowReplaceResult, error) {
	t := namespace.FromContext(ctx)
	if err := namespace.Validate(t); err != nil {
		return backend.WorkflowReplaceResult{}, fmt.Errorf("replace workflow: %w", err)
	}
	if req.MutationID == "" {
		return backend.WorkflowReplaceResult{}, errors.New("replace workflow: mutation id is required")
	}
	if req.Expected.ID == "" {
		return backend.WorkflowReplaceResult{}, &backend.WorkflowReplaceConflictError{Kind: backend.WorkflowReplaceConflictStaleRevision}
	}

	replacement := req.Replacement
	if replacement.ID == "" {
		replacement.ID = deterministicReplacementID(t, req.MutationID, req.Expected.ID)
	}
	replacement.RegistryRevision = 0
	stored, err := marshalWorkflowRecord(replacement)
	if err != nil {
		return backend.WorkflowReplaceResult{}, fmt.Errorf("replace workflow: %w", err)
	}
	requestFingerprint := workflowReplaceRequestFingerprint(req.Expected.ID, stored.payload)
	// Namespace discovery is append-only and intentionally outside the
	// namespace-tagged CAS. It must succeed first: a committed pending intent
	// without discoverable namespace membership could otherwise be stranded.
	if err := r.rdb.SAdd(ctx, workflowProjectionNamespaceSetKey, string(t)).Err(); err != nil {
		return backend.WorkflowReplaceResult{}, indeterminateMutation("register workflow activation projection namespace "+strconv.Quote(string(t)), err)
	}
	if replay, found, replayErr := r.lookupWorkflowOperation(ctx, t, req.MutationID, requestFingerprint); replayErr != nil {
		return backend.WorkflowReplaceResult{}, replayErr
	} else if found {
		return replay, nil
	}

	legacyKeyOccupied, legacyIDOccupied, err := r.legacyDestinationOccupancy(ctx, t, req.Expected, stored.ID, stored.Key)
	if err != nil {
		return backend.WorkflowReplaceResult{}, indeterminateMutation("replace workflow "+strconv.Quote(req.MutationID), err)
	}
	// Compare-and-replace itself only operates on v2. A source that still lives
	// in v1 is first imported using the same conflict-safe path as Get.
	if _, sourceErr := r.GetWorkflow(ctx, req.Expected.ID); sourceErr != nil {
		switch {
		case errors.Is(sourceErr, backend.ErrWorkflowNotFound):
			// Let the Lua source CAS return the explicit stale_revision result.
		case errors.Is(sourceErr, backend.ErrWorkflowConflict):
			return backend.WorkflowReplaceResult{}, &backend.WorkflowReplaceConflictError{Kind: backend.WorkflowReplaceConflictStaleRevision}
		case errors.Is(sourceErr, backend.ErrWorkflowMutationIndeterminate):
			return backend.WorkflowReplaceResult{}, sourceErr
		default:
			return backend.WorkflowReplaceResult{}, indeterminateMutation("replace workflow "+strconv.Quote(req.MutationID), sourceErr)
		}
	}

	result, err := compareAndReplaceWorkflowLua.Run(
		ctx,
		r.rdb,
		[]string{
			workflowOperationKey(t, req.MutationID),
			workflowByIDKey(t, req.Expected.Key, req.Expected.ID),
			workflowByKeyKey(t, req.Expected.Key),
			workflowIDMapKey(t, req.Expected.ID),
			workflowByIDKey(t, stored.Key, stored.ID),
			workflowByKeyKey(t, stored.Key),
			workflowIDMapKey(t, stored.ID),
			workflowRevisionKey(t),
			workflowProjectionPendingKey(t),
		},
		req.MutationID,
		requestFingerprint,
		string(req.Expected.ID),
		req.Expected.Key,
		req.Expected.DefinitionHash,
		strconv.FormatUint(req.Expected.RegistryRevision, 10),
		string(stored.ID),
		stored.Key,
		stored.Version,
		stored.DefinitionHash,
		stored.fingerprint,
		stored.payload,
		boolScriptValue(legacyKeyOccupied),
		boolScriptValue(legacyIDOccupied),
		req.Expected.Version,
		string(t),
	).Result()
	if err != nil {
		return backend.WorkflowReplaceResult{}, indeterminateMutation("replace workflow "+strconv.Quote(req.MutationID), err)
	}

	values, err := decodeScriptValues(result, 7)
	if err != nil {
		return backend.WorkflowReplaceResult{}, indeterminateMutation("replace workflow "+strconv.Quote(req.MutationID), err)
	}
	state, err := scriptString(values[0])
	if err != nil {
		return backend.WorkflowReplaceResult{}, indeterminateMutation("replace workflow "+strconv.Quote(req.MutationID), err)
	}
	switch state {
	case "stale_revision":
		return backend.WorkflowReplaceResult{}, &backend.WorkflowReplaceConflictError{Kind: backend.WorkflowReplaceConflictStaleRevision}
	case "destination_key_occupied":
		return backend.WorkflowReplaceResult{}, &backend.WorkflowReplaceConflictError{Kind: backend.WorkflowReplaceConflictDestinationKey}
	case "destination_id_occupied":
		return backend.WorkflowReplaceResult{}, &backend.WorkflowReplaceConflictError{Kind: backend.WorkflowReplaceConflictDestinationID}
	case "mutation_reused":
		return backend.WorkflowReplaceResult{}, &backend.WorkflowReplaceConflictError{Kind: backend.WorkflowReplaceConflictMutationIDReuse}
	case string(backend.WorkflowReplaceUnchanged), string(backend.WorkflowReplaceReplaced):
		return decodeWorkflowReplaceResult(state, values)
	default:
		return backend.WorkflowReplaceResult{}, indeterminateMutation(
			"replace workflow "+strconv.Quote(req.MutationID),
			fmt.Errorf("unexpected script state %q", state),
		)
	}
}

func (r *Registry) lookupWorkflowOperation(ctx context.Context, t namespace.Namespace, mutationID, requestFingerprint string) (backend.WorkflowReplaceResult, bool, error) {
	operation := "replace workflow " + strconv.Quote(mutationID)
	ledger, err := r.rdb.HGetAll(ctx, workflowOperationKey(t, mutationID)).Result()
	if err != nil {
		return backend.WorkflowReplaceResult{}, false, indeterminateMutation(operation, err)
	}
	if len(ledger) == 0 {
		return backend.WorkflowReplaceResult{}, false, nil
	}
	if ledger["mutation_id"] != mutationID || ledger["fingerprint"] != requestFingerprint {
		return backend.WorkflowReplaceResult{}, false, &backend.WorkflowReplaceConflictError{Kind: backend.WorkflowReplaceConflictMutationIDReuse}
	}
	state := ledger["status"]
	if state != string(backend.WorkflowReplaceUnchanged) && state != string(backend.WorkflowReplaceReplaced) {
		return backend.WorkflowReplaceResult{}, false, indeterminateMutation(operation, fmt.Errorf("unexpected operation ledger state %q", state))
	}
	result, err := decodeWorkflowReplaceResult(state, []any{
		state,
		ledger["current_payload"],
		ledger["previous_id"],
		ledger["previous_key"],
		ledger["previous_version"],
		ledger["previous_hash"],
		ledger["previous_revision"],
	})
	if err != nil {
		return backend.WorkflowReplaceResult{}, false, err
	}
	return result, true, nil
}

// ListWorkflowActivationProjectionNamespaces returns the append-only namespace
// discovery set used by recovery workers. Namespace membership deliberately
// survives Ack so a replay can never make an unacknowledged namespace
// undiscoverable through membership cleanup races.
func (r *Registry) ListWorkflowActivationProjectionNamespaces(ctx context.Context) ([]namespace.Namespace, error) {
	members, err := r.rdb.SMembers(ctx, workflowProjectionNamespaceSetKey).Result()
	if err != nil {
		return nil, fmt.Errorf("list workflow activation projection namespaces: %w", err)
	}
	sort.Strings(members)
	result := make([]namespace.Namespace, 0, len(members))
	for _, member := range members {
		ns := namespace.Namespace(member)
		if err := namespace.Validate(ns); err != nil {
			return nil, fmt.Errorf("list workflow activation projection namespaces: invalid stored namespace %q: %w", member, err)
		}
		result = append(result, ns)
	}
	return result, nil
}

// ListPendingWorkflowActivationProjections returns one deterministic page from
// the namespace-local pending set. The cursor is opaque to callers and results
// remain at-least-once under concurrent mutation.
func (r *Registry) ListPendingWorkflowActivationProjections(
	ctx context.Context,
	ns namespace.Namespace,
	cursor uint64,
	limit int,
) ([]backend.WorkflowActivationProjectionRef, uint64, error) {
	if err := namespace.Validate(ns); err != nil {
		return nil, 0, fmt.Errorf("list pending workflow activation projections: %w", err)
	}
	if limit <= 0 {
		return nil, 0, fmt.Errorf("list pending workflow activation projections: limit must be positive")
	}
	result, err := listPendingWorkflowActivationProjectionsLua.Run(
		ctx,
		r.rdb,
		[]string{workflowProjectionPendingKey(ns)},
		strconv.FormatUint(cursor, 10),
		strconv.Itoa(limit),
	).Result()
	if err != nil {
		return nil, 0, fmt.Errorf("list pending workflow activation projections for namespace %q: %w", ns, err)
	}
	values, ok := result.([]any)
	if !ok || len(values) == 0 {
		return nil, 0, fmt.Errorf("list pending workflow activation projections for namespace %q: unexpected script result %#v", ns, result)
	}
	nextText, err := scriptString(values[0])
	if err != nil {
		return nil, 0, fmt.Errorf("list pending workflow activation projections for namespace %q: %w", ns, err)
	}
	next, err := strconv.ParseUint(nextText, 10, 64)
	if err != nil {
		return nil, 0, fmt.Errorf("list pending workflow activation projections for namespace %q: parse next cursor: %w", ns, err)
	}
	refs := make([]backend.WorkflowActivationProjectionRef, 0, len(values)-1)
	for _, value := range values[1:] {
		mutationID, decodeErr := scriptString(value)
		if decodeErr != nil {
			return nil, 0, fmt.Errorf("list pending workflow activation projections for namespace %q: %w", ns, decodeErr)
		}
		refs = append(refs, backend.WorkflowActivationProjectionRef{MutationID: mutationID})
	}
	return refs, next, nil
}

// ClaimWorkflowActivationProjection obtains a server-time lease for one
// pending intent. The same token is idempotent and keeps its original deadline;
// a different token can take over only after Redis says that deadline expired.
func (r *Registry) ClaimWorkflowActivationProjection(
	ctx context.Context,
	ns namespace.Namespace,
	mutationID string,
	leaseToken string,
	leaseTTL time.Duration,
) (backend.WorkflowActivationProjectionClaim, error) {
	if err := namespace.Validate(ns); err != nil {
		return backend.WorkflowActivationProjectionClaim{}, fmt.Errorf("claim workflow activation projection: %w", err)
	}
	if mutationID == "" {
		return backend.WorkflowActivationProjectionClaim{}, fmt.Errorf("claim workflow activation projection: mutation id is required")
	}
	if leaseToken == "" {
		return backend.WorkflowActivationProjectionClaim{}, fmt.Errorf("claim workflow activation projection: lease token is required")
	}
	if leaseTTL <= 0 {
		return backend.WorkflowActivationProjectionClaim{}, fmt.Errorf("claim workflow activation projection: lease TTL must be positive")
	}
	leaseMilliseconds := leaseTTL.Milliseconds()
	if leaseMilliseconds == 0 {
		leaseMilliseconds = 1
	}
	result, err := claimWorkflowActivationProjectionLua.Run(
		ctx,
		r.rdb,
		[]string{
			workflowOperationKey(ns, mutationID),
			workflowProjectionPendingKey(ns),
			workflowProjectionLeaseKey(ns, mutationID),
		},
		mutationID,
		leaseToken,
		strconv.FormatInt(leaseMilliseconds, 10),
	).Result()
	if err != nil {
		return backend.WorkflowActivationProjectionClaim{}, fmt.Errorf("claim workflow activation projection %q/%q: %w", ns, mutationID, err)
	}
	values, err := decodeScriptValues(result, 10)
	if err != nil {
		return backend.WorkflowActivationProjectionClaim{}, fmt.Errorf("claim workflow activation projection %q/%q: %w", ns, mutationID, err)
	}
	state, err := scriptString(values[0])
	if err != nil {
		return backend.WorkflowActivationProjectionClaim{}, fmt.Errorf("claim workflow activation projection %q/%q: %w", ns, mutationID, err)
	}
	deadlineText, err := scriptString(values[9])
	if err != nil {
		return backend.WorkflowActivationProjectionClaim{}, fmt.Errorf("claim workflow activation projection %q/%q: %w", ns, mutationID, err)
	}
	deadlineMilliseconds, err := strconv.ParseInt(deadlineText, 10, 64)
	if err != nil {
		return backend.WorkflowActivationProjectionClaim{}, fmt.Errorf("claim workflow activation projection %q/%q: parse lease deadline: %w", ns, mutationID, err)
	}
	claim := backend.WorkflowActivationProjectionClaim{}
	if deadlineMilliseconds > 0 {
		claim.LeaseDeadline = time.UnixMilli(deadlineMilliseconds).UTC()
	}
	switch state {
	case string(backend.WorkflowActivationProjectionClaimBusy):
		claim.State = backend.WorkflowActivationProjectionClaimBusy
		return claim, nil
	case string(backend.WorkflowActivationProjectionClaimAcquired), string(backend.WorkflowActivationProjectionClaimApplied):
		intent, decodeErr := decodeWorkflowActivationProjectionIntent(values[1:9])
		if decodeErr != nil {
			return backend.WorkflowActivationProjectionClaim{}, fmt.Errorf("claim workflow activation projection %q/%q: %w", ns, mutationID, decodeErr)
		}
		if intent.Namespace != ns || intent.MutationID != mutationID {
			return backend.WorkflowActivationProjectionClaim{}, fmt.Errorf(
				"claim workflow activation projection %q/%q: ledger identity is %q/%q",
				ns, mutationID, intent.Namespace, intent.MutationID,
			)
		}
		claim.State = backend.WorkflowActivationProjectionClaimState(state)
		claim.Intent = intent
		return claim, nil
	case "missing":
		return backend.WorkflowActivationProjectionClaim{}, fmt.Errorf("claim workflow activation projection %q/%q: intent not found", ns, mutationID)
	case "invalid":
		return backend.WorkflowActivationProjectionClaim{}, fmt.Errorf("claim workflow activation projection %q/%q: ledger identity mismatch", ns, mutationID)
	default:
		return backend.WorkflowActivationProjectionClaim{}, fmt.Errorf("claim workflow activation projection %q/%q: unexpected state %q", ns, mutationID, state)
	}
}

// AckWorkflowActivationProjection atomically fences by token and unexpired
// server-time deadline, marks the durable ledger applied, removes pending, and
// drops the lease. An already-applied ledger is an idempotent success.
func (r *Registry) AckWorkflowActivationProjection(
	ctx context.Context,
	ns namespace.Namespace,
	mutationID string,
	leaseToken string,
) (bool, error) {
	if err := namespace.Validate(ns); err != nil {
		return false, fmt.Errorf("ack workflow activation projection: %w", err)
	}
	if mutationID == "" {
		return false, fmt.Errorf("ack workflow activation projection: mutation id is required")
	}
	if leaseToken == "" {
		return false, fmt.Errorf("ack workflow activation projection: lease token is required")
	}
	result, err := ackWorkflowActivationProjectionLua.Run(
		ctx,
		r.rdb,
		[]string{
			workflowOperationKey(ns, mutationID),
			workflowProjectionPendingKey(ns),
			workflowProjectionLeaseKey(ns, mutationID),
		},
		mutationID,
		leaseToken,
	).Result()
	if err != nil {
		return false, fmt.Errorf("ack workflow activation projection %q/%q: %w", ns, mutationID, err)
	}
	state, err := decodeSingleStateScriptResult(result)
	if err != nil {
		return false, fmt.Errorf("ack workflow activation projection %q/%q: %w", ns, mutationID, err)
	}
	switch state {
	case "acked", "applied":
		return true, nil
	case "stale":
		return false, nil
	case "missing":
		return false, fmt.Errorf("ack workflow activation projection %q/%q: intent not found", ns, mutationID)
	case "invalid":
		return false, fmt.Errorf("ack workflow activation projection %q/%q: ledger identity mismatch", ns, mutationID)
	default:
		return false, fmt.Errorf("ack workflow activation projection %q/%q: unexpected state %q", ns, mutationID, state)
	}
}

func decodeWorkflowActivationProjectionIntent(values []any) (backend.WorkflowActivationProjectionIntent, error) {
	if len(values) != 8 {
		return backend.WorkflowActivationProjectionIntent{}, fmt.Errorf("unexpected workflow activation projection intent: %#v", values)
	}
	projectionNamespace, err := scriptString(values[0])
	if err != nil {
		return backend.WorkflowActivationProjectionIntent{}, err
	}
	mutationID, err := scriptString(values[1])
	if err != nil {
		return backend.WorkflowActivationProjectionIntent{}, err
	}
	currentPayload, err := scriptBytes(values[2])
	if err != nil {
		return backend.WorkflowActivationProjectionIntent{}, err
	}
	current, err := unmarshalWorkflowRecord(currentPayload)
	if err != nil {
		return backend.WorkflowActivationProjectionIntent{}, err
	}
	previousID, err := scriptString(values[3])
	if err != nil {
		return backend.WorkflowActivationProjectionIntent{}, err
	}
	previousKey, err := scriptString(values[4])
	if err != nil {
		return backend.WorkflowActivationProjectionIntent{}, err
	}
	previousVersion, err := scriptString(values[5])
	if err != nil {
		return backend.WorkflowActivationProjectionIntent{}, err
	}
	previousHash, err := scriptString(values[6])
	if err != nil {
		return backend.WorkflowActivationProjectionIntent{}, err
	}
	previousRevisionText, err := scriptString(values[7])
	if err != nil {
		return backend.WorkflowActivationProjectionIntent{}, err
	}
	previousRevision, err := strconv.ParseUint(previousRevisionText, 10, 64)
	if err != nil {
		return backend.WorkflowActivationProjectionIntent{}, fmt.Errorf("parse previous registry revision: %w", err)
	}
	return backend.WorkflowActivationProjectionIntent{
		Namespace:  namespace.Namespace(projectionNamespace),
		MutationID: mutationID,
		Previous: backend.WorkflowRevision{
			ID:               types.WorkflowID(previousID),
			Key:              previousKey,
			Version:          previousVersion,
			DefinitionHash:   previousHash,
			RegistryRevision: previousRevision,
		},
		Current: current,
	}, nil
}

func (r *Registry) legacyDestinationOccupancy(
	ctx context.Context,
	t namespace.Namespace,
	expected backend.WorkflowRevision,
	destinationID types.WorkflowID,
	destinationKey string,
) (bool, bool, error) {
	byKey, found, err := r.loadLegacyWorkflowByKey(ctx, t, destinationKey)
	if err != nil {
		return false, false, err
	}
	keyOccupied := found && (byKey.ID != expected.ID || byKey.Key != expected.Key)

	byID, found, err := r.loadLegacyWorkflowByID(ctx, t, destinationID)
	if err != nil {
		return false, false, err
	}
	idOccupied := found && (byID.ID != expected.ID || byID.Key != expected.Key)
	return keyOccupied, idOccupied, nil
}

func boolScriptValue(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

func decodeWorkflowReplaceResult(state string, values []any) (backend.WorkflowReplaceResult, error) {
	payload, err := scriptBytes(values[1])
	if err != nil {
		return backend.WorkflowReplaceResult{}, indeterminateMutation("decode workflow replacement", err)
	}
	current, err := unmarshalWorkflowRecord(payload)
	if err != nil {
		return backend.WorkflowReplaceResult{}, indeterminateMutation("decode workflow replacement", err)
	}
	previousID, err := scriptString(values[2])
	if err != nil {
		return backend.WorkflowReplaceResult{}, indeterminateMutation("decode workflow replacement", err)
	}
	previousKey, err := scriptString(values[3])
	if err != nil {
		return backend.WorkflowReplaceResult{}, indeterminateMutation("decode workflow replacement", err)
	}
	previousVersion, err := scriptString(values[4])
	if err != nil {
		return backend.WorkflowReplaceResult{}, indeterminateMutation("decode workflow replacement", err)
	}
	previousHash, err := scriptString(values[5])
	if err != nil {
		return backend.WorkflowReplaceResult{}, indeterminateMutation("decode workflow replacement", err)
	}
	previousRevisionText, err := scriptString(values[6])
	if err != nil {
		return backend.WorkflowReplaceResult{}, indeterminateMutation("decode workflow replacement", err)
	}
	previousRevision, err := strconv.ParseUint(previousRevisionText, 10, 64)
	if err != nil {
		return backend.WorkflowReplaceResult{}, indeterminateMutation("decode workflow replacement", fmt.Errorf("parse previous revision: %w", err))
	}
	return backend.WorkflowReplaceResult{
		Status: backend.WorkflowReplaceStatus(state),
		Previous: backend.WorkflowRevision{
			ID:               types.WorkflowID(previousID),
			Key:              previousKey,
			Version:          previousVersion,
			DefinitionHash:   previousHash,
			RegistryRevision: previousRevision,
		},
		Current: current,
	}, nil
}

type marshaledWorkflowRecord struct {
	ID             types.WorkflowID
	Key            string
	Version        string
	DefinitionHash string
	fingerprint    string
	payload        []byte
}

func marshalWorkflowRecord(rec backend.WorkflowRecord) (*marshaledWorkflowRecord, error) {
	if rec.ID == "" {
		rec.ID = types.WorkflowID(uuid.NewString())
	}
	// Revisions are exclusively registry-assigned. A zero value is also the
	// fixed JSON placeholder atomically replaced by Lua after INCR.
	rec.RegistryRevision = 0
	payload, err := marshalWorkflowRecordPayload(rec)
	if err != nil {
		return nil, err
	}
	if !containsRevisionPlaceholder(payload) {
		return nil, fmt.Errorf("marshal workflow %q: revision placeholder missing", rec.Key)
	}
	return &marshaledWorkflowRecord{
		ID:             rec.ID,
		Key:            rec.Key,
		Version:        rec.Version,
		DefinitionHash: rec.DefinitionHash,
		fingerprint:    semanticFingerprint(payload),
		payload:        payload,
	}, nil
}

// marshalWorkflowRecordPayload re-marshals an already-stored record while
// preserving its ID and supplied RegistryRevision. Mutation callers set the
// revision to zero before invoking it so Lua can install its allocated value.
func marshalWorkflowRecordPayload(rec backend.WorkflowRecord) ([]byte, error) {
	rawGraph, err := graphToRaw(rec.Graph)
	if err != nil {
		return nil, fmt.Errorf("marshal graph for workflow %q: %w", rec.Key, err)
	}
	if len(rawGraph) == 0 && rec.Definition != nil {
		compiled, compileErr := graph.Compile(rec.Definition)
		if compileErr != nil {
			return nil, fmt.Errorf("compile workflow %q: %w", rec.Key, compileErr)
		}
		rawGraph, err = graphToRaw(compiled)
		if err != nil {
			return nil, fmt.Errorf("marshal compiled graph for workflow %q: %w", rec.Key, err)
		}
	}
	stored := storedWorkflowRecord{
		ID:               rec.ID,
		Key:              rec.Key,
		Namespace:        rec.Namespace,
		Name:             rec.Name,
		Version:          rec.Version,
		DefinitionHash:   rec.DefinitionHash,
		RegistryRevision: rec.RegistryRevision,
		AuditFingerprint: rec.AuditFingerprint,
		Definition:       rec.Definition,
		Graph:            rawGraph,
	}
	payload, err := json.Marshal(stored)
	if err != nil {
		return nil, fmt.Errorf("marshal workflow payload %q: %w", stored.Key, err)
	}
	return payload, nil
}

func containsRevisionPlaceholder(payload []byte) bool {
	return jsonContainsLiteral(payload, revisionPlaceholder)
}

func jsonContainsLiteral(payload []byte, literal string) bool {
	if len(literal) == 0 || len(payload) < len(literal) {
		return false
	}
	for i := 0; i <= len(payload)-len(literal); i++ {
		if string(payload[i:i+len(literal)]) == literal {
			return true
		}
	}
	return false
}

func semanticFingerprint(payload []byte) string {
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// workflowReplaceRequestFingerprint intentionally binds only the source ID
// from Expected. Expected key/hash/revision/version are re-read after a lost
// response and can describe the just-committed current record. The complete
// revision-zero replacement payload binds all replacement semantics, including
// its ID, key, definition hash, Definition and compiled Graph.
func workflowReplaceRequestFingerprint(expectedID types.WorkflowID, replacementPayload []byte) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "xflow-workflow-replace-v2\x00%d:", len(expectedID))
	_, _ = h.Write([]byte(expectedID))
	_, _ = fmt.Fprintf(h, "%d:", len(replacementPayload))
	_, _ = h.Write(replacementPayload)
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

func deterministicReplacementID(t namespace.Namespace, mutationID string, expectedID types.WorkflowID) types.WorkflowID {
	material := registryPrefix(t) + mutationID + "\x00" + string(expectedID)
	return types.WorkflowID(uuid.NewSHA1(uuid.NameSpaceOID, []byte(material)).String())
}

func graphToRaw(g *graph.Graph) (json.RawMessage, error) {
	if g == nil {
		return nil, nil
	}
	data, err := json.Marshal(g)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(data), nil
}

func unmarshalWorkflowRecord(raw []byte) (backend.WorkflowRecord, error) {
	var stored storedWorkflowRecord
	if err := json.Unmarshal(raw, &stored); err != nil {
		return backend.WorkflowRecord{}, fmt.Errorf("unmarshal workflow record: %w", err)
	}

	record := backend.WorkflowRecord{
		ID:               stored.ID,
		Key:              stored.Key,
		Namespace:        stored.Namespace,
		Name:             stored.Name,
		Version:          stored.Version,
		DefinitionHash:   stored.DefinitionHash,
		RegistryRevision: stored.RegistryRevision,
		AuditFingerprint: stored.AuditFingerprint,
		Definition:       stored.Definition,
	}
	if len(stored.Graph) > 0 {
		g := &graph.Graph{}
		if err := json.Unmarshal(stored.Graph, g); err == nil {
			record.Graph = g
		}
	}
	if record.Graph == nil && record.Definition != nil {
		g, err := graph.Compile(record.Definition)
		if err != nil {
			return backend.WorkflowRecord{}, fmt.Errorf("compile workflow %q: %w", record.Key, err)
		}
		record.Graph = g
	}
	return record, nil
}

func indeterminateMutation(operation string, err error) error {
	return fmt.Errorf("%s: %w", operation, &backend.WorkflowMutationIndeterminateError{Err: err})
}

func decodeTwoPartScriptResult(result any) (string, []byte, error) {
	values, err := decodeScriptValues(result, 2)
	if err != nil {
		return "", nil, err
	}
	state, err := scriptString(values[0])
	if err != nil {
		return "", nil, err
	}
	payload, err := scriptBytes(values[1])
	if err != nil {
		return "", nil, err
	}
	return state, payload, nil
}

func decodeSingleStateScriptResult(result any) (string, error) {
	values, err := decodeScriptValues(result, 1)
	if err != nil {
		return "", err
	}
	return scriptString(values[0])
}

func decodeScriptValues(result any, expected int) ([]any, error) {
	values, ok := result.([]any)
	if !ok || len(values) != expected {
		return nil, fmt.Errorf("unexpected workflow script result: %#v", result)
	}
	return values, nil
}

func scriptString(value any) (string, error) {
	switch value := value.(type) {
	case string:
		return value, nil
	case []byte:
		return string(value), nil
	case int64:
		return strconv.FormatInt(value, 10), nil
	default:
		return "", fmt.Errorf("unexpected workflow script value: %#v", value)
	}
}

func scriptBytes(value any) ([]byte, error) {
	text, err := scriptString(value)
	if err != nil {
		return nil, err
	}
	return []byte(text), nil
}
