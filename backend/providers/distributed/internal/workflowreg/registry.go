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
	"strings"
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
	// versionRetention is how many retired versions one workflow keeps. Zero
	// means unbounded; the default is backend.DefaultWorkflowVersionRetention.
	versionRetention int
}

type Option func(*Registry)

// WithWorkflowVersionRetention sets how many retired versions one workflow
// keeps. Retiring beyond the limit evicts the oldest retired version, which
// only shortens how far back a rollback can reach. A value <= 0 keeps every
// retired version.
func WithWorkflowVersionRetention(n int) Option {
	return func(r *Registry) { r.versionRetention = n }
}

func New(rdb redis.UniversalClient, opts ...Option) *Registry {
	r := &Registry{rdb: rdb, versionRetention: backend.DefaultWorkflowVersionRetention}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// retentionScriptValue is the ARGV form of versionRetention: a negative limit
// is sent as "0", which the scripts read as "keep every retired version".
func (r *Registry) retentionScriptValue() string {
	if r.versionRetention <= 0 {
		return "0"
	}
	return strconv.Itoa(r.versionRetention)
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
	// EditorMetadata mirrors backend.WorkflowRecord.EditorMetadata. Its
	// absence (a legacy record written before this field existed) decodes to
	// nil without error -- no migration pass is needed.
	EditorMetadata *types.WorkflowEditorMetadata `json:"editor_metadata,omitempty"`
	// Graph is decoded as raw JSON here (not *graph.Graph) so a Graph decode
	// failure does not abort decoding the whole record. unmarshalWorkflowRecord
	// can then fall back to recompiling Definition for legacy snapshots.
	Graph json.RawMessage `json:"graph,omitempty"`
}

var (
	_ backend.WorkflowReplaceCapability        = (*Registry)(nil)
	_ backend.DurableWorkflowReplaceCapability = (*Registry)(nil)
	_ backend.WorkflowVersionCapability        = (*Registry)(nil)
)

// Registry v2 deliberately co-locates every mutable authority key for one
// namespace in one Redis Cluster slot. The namespace itself is represented by
// a digest, so neither a logical workflow key containing braces nor any other
// caller-controlled suffix can change the first (and effective) hash tag.
//
//	xflow:wfreg:v2:{ns:<sha256(namespace)>}:bykey:<sha256(logical-key)>
//	xflow:wfreg:v2:{ns:<sha256(namespace)>}:byid:<workflow-id>
//	xflow:wfreg:v2:{ns:<sha256(namespace)>}:meta:<workflow-id>
//	xflow:wfreg:v2:{ns:<sha256(namespace)>}:name:<sha256(name)>
//	xflow:wfreg:v2:{ns:<sha256(namespace)>}:versions:<workflow-id>
//	xflow:wfreg:v2:{ns:<sha256(namespace)>}:versions:<workflow-id>:meta
//	xflow:wfreg:v2:{ns:<sha256(namespace)>}:revision
//	xflow:wfreg:v2:{ns:<sha256(namespace)>}:op:<sha256(mutation-id)>
//	xflow:wfreg:v2:{ns:<sha256(namespace)>}:projection:pending
//	xflow:wfreg:v2:{ns:<sha256(namespace)>}:projection:lease:<sha256(mutation-id)>
//	xflow:wfreg:v2:{ns:<sha256(namespace)>}:legacy:bykey:<sha256(logical-key)>
//	xflow:wfreg:v2:{ns:<sha256(namespace)>}:legacy:byid:<sha256(workflow-id)>
//	xflow:wfreg:v2:{ns:<sha256(namespace)>}:index
//
// bykey and byid are Redis hashes containing the same payload and compact CAS
// metadata. meta repeats only the compact metadata. Keeping a complete payload
// under both lookup dimensions makes GetWorkflow and GetWorkflowByKey each a
// single-snapshot HGET, while Lua can compare metadata without cjson-decoding a
// potentially deeply nested Definition/Graph.
//
// name is the namespace's live-name index: one name maps to the one live
// workflow id that holds it, so a mutation can refuse a second live record with
// the same (namespace, name) inside the same atomic script.
//
// versions and versions:...:meta are one workflow id's retired-version history
// (version label -> payload, version label -> "<definition-hash> <revision>").
// The meta hash is small and carries everything the version list, the
// immutability check, retention eviction and retirement ordering need, so none
// of those has to read (or decode) a retired payload.
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

// workflowNameIndexKey is the namespace's live-name index: the id of the one
// live record holding name, or absent. It is read and written inside the same
// Lua script as the record mutation it guards, so "at most one live record per
// (namespace, name)" holds atomically and not merely between two round trips.
func workflowNameIndexKey(t namespace.Namespace, name string) string {
	return registryPrefix(t) + "name:" + digestKeyComponent(name)
}

// workflowVersionHistoryKey is one workflow id's retired-version history:
// version label -> the complete record payload captured when that version was
// retired. Keyed by id because the version line follows the workflow id, which
// a rename or version change preserves.
func workflowVersionHistoryKey(t namespace.Namespace, id types.WorkflowID) string {
	return registryPrefix(t) + "versions:" + string(id)
}

// workflowVersionMetaKey is the compact sibling of the history hash:
// version label -> "<definition-hash> <registry-revision>". The revision is the
// retired payload's registry revision, which is allocated per install and is
// therefore also the retirement order within one workflow.
func workflowVersionMetaKey(t namespace.Namespace, id types.WorkflowID) string {
	return registryPrefix(t) + "versions:" + string(id) + ":meta"
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

// workflowIndexKey is the per-namespace enumeration index: a ZSET of workflow
// ids belonging to exactly one namespace. It lives in that namespace's digest
// slot so every index mutation runs inside the same Lua script — and therefore
// the same Redis Cluster slot — as the record mutation it mirrors.
//
// The score is the negation of the record's registry revision, so a plain
// ascending ZRANGE lists the namespace's records newest-revision-first, and a
// replace that allocates a fresh revision naturally re-scores the id to the
// front. Revisions come from the namespace-global INCR counter, so two live ids
// in one namespace cannot share a score and the ZSET's lexicographic tie-break
// over equal scores is never load-bearing.
func workflowIndexKey(t namespace.Namespace) string {
	return registryPrefix(t) + "index"
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
		redis.call('SET', KEYS[7], ARGV[1])
		return {'existing', existingPayload}
	end
	-- The (name, version) key is live under a different definition: the version
	-- label is already published, and a label never resolves to two
	-- definitions.
	return {'version_immutable', ARGV[3]}
end
if redis.call('EXISTS', KEYS[2]) == 1 or redis.call('EXISTS', KEYS[3]) == 1 then
	return {'id_conflict', ''}
end
-- At most one live record may hold a (namespace, name). A record with no name
-- to index (a test fixture or a caller that never set one) is exempt because
-- it has no name identity to enforce.
if ARGV[7] ~= '' then
	local nameHolder = redis.call('GET', KEYS[7])
	if nameHolder and nameHolder ~= ARGV[1] then
		return {'name_conflict', nameHolder}
	end
end
if not string.find(ARGV[6], '"registry_revision":0', 1, true) then
	return redis.error_reply('workflow payload has no revision placeholder')
end
local revision = redis.call('INCR', KEYS[4])
local payload = string.gsub(ARGV[6], '"registry_revision":0', '"registry_revision":' .. tostring(revision), 1)
redis.call('HSET', KEYS[1],
	'id', ARGV[1], 'key', ARGV[2], 'version', ARGV[3], 'hash', ARGV[4], 'name', ARGV[7],
	'revision', tostring(revision), 'fingerprint', ARGV[5], 'payload', payload)
redis.call('HSET', KEYS[2],
	'id', ARGV[1], 'key', ARGV[2], 'version', ARGV[3], 'hash', ARGV[4], 'name', ARGV[7],
	'revision', tostring(revision), 'fingerprint', ARGV[5], 'payload', payload)
redis.call('HSET', KEYS[3],
	'id', ARGV[1], 'key', ARGV[2], 'version', ARGV[3], 'hash', ARGV[4], 'name', ARGV[7],
	'revision', tostring(revision), 'fingerprint', ARGV[5])
redis.call('ZADD', KEYS[6], -revision, ARGV[1])
if ARGV[7] ~= '' then
	redis.call('SET', KEYS[7], ARGV[1])
end
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
	if ARGV[7] ~= '' then
		redis.call('SET', KEYS[8], ARGV[1])
	end
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
if ARGV[7] ~= '' then
	local nameHolder = redis.call('GET', KEYS[8])
	if nameHolder and nameHolder ~= ARGV[1] then
		return {'name_conflict', nameHolder}
	end
end
if not string.find(ARGV[6], '"registry_revision":0', 1, true) then
	return redis.error_reply('workflow payload has no revision placeholder')
end
local revision = redis.call('INCR', KEYS[4])
local payload = string.gsub(ARGV[6], '"registry_revision":0', '"registry_revision":' .. tostring(revision), 1)
redis.call('HSET', KEYS[1],
	'id', ARGV[1], 'key', ARGV[2], 'version', ARGV[3], 'hash', ARGV[4], 'name', ARGV[7],
	'revision', tostring(revision), 'fingerprint', ARGV[5], 'payload', payload)
redis.call('HSET', KEYS[2],
	'id', ARGV[1], 'key', ARGV[2], 'version', ARGV[3], 'hash', ARGV[4], 'name', ARGV[7],
	'revision', tostring(revision), 'fingerprint', ARGV[5], 'payload', payload)
redis.call('HSET', KEYS[3],
	'id', ARGV[1], 'key', ARGV[2], 'version', ARGV[3], 'hash', ARGV[4], 'name', ARGV[7],
	'revision', tostring(revision), 'fingerprint', ARGV[5])
redis.call('SET', KEYS[5], ARGV[1])
redis.call('SET', KEYS[6], ARGV[2])
if ARGV[7] ~= '' then
	redis.call('SET', KEYS[8], ARGV[1])
end
redis.call('ZADD', KEYS[7], -revision, ARGV[1])
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
redis.call('ZADD', KEYS[5], -revision, ARGV[1])
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
-- Removing a workflow removes its identity: the freed name may be taken by a
-- later registration, and its retired versions are unreachable once the live
-- record they hang off is gone.
if ARGV[4] ~= '' and redis.call('GET', KEYS[5]) == ARGV[1] then
	redis.call('DEL', KEYS[5])
end
redis.call('DEL', KEYS[6], KEYS[7])
redis.call('ZREM', KEYS[4], ARGV[1])
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
	-- Repair the name index an idempotent no-op would otherwise never write
	-- (a record registered before the index existed).
	if ARGV[17] ~= '' then
		redis.call('SET', KEYS[11], ARGV[7])
	end
	return {'unchanged', previousPayload, ARGV[3], ARGV[4], previousVersion, ARGV[5], ARGV[6]}
end

if not string.find(ARGV[12], '"registry_revision":0', 1, true) then
	return redis.error_reply('workflow payload has no revision placeholder')
end

-- At most one live record may hold a (namespace, name): a second one is hosted
-- beside the first (its entry activations never leave Desired) and silently
-- splits the workflow the name refers to.
if ARGV[17] ~= '' then
	local nameHolder = redis.call('GET', KEYS[11])
	if nameHolder and nameHolder ~= ARGV[3] and nameHolder ~= ARGV[7] then
		return {'name_conflict', nameHolder, '', '', '', '', ''}
	end
end

-- A retired version label is immutable: its stored payload is what a pinned
-- replay or a subworkflow reference resolves to, so installing it under a
-- different definition would silently repoint both. Re-installing the label
-- with the payload it was retired with (a rollback) is allowed. A replace that
-- keeps the current label retires nothing, so it is not checked here: the live
-- record stays updatable in place.
local retired = redis.call('HGET', KEYS[14], ARGV[9])
if retired then
	local separator = string.find(retired, ' ', 1, true)
	local retiredHash = retired
	if separator then
		retiredHash = string.sub(retired, 1, separator - 1)
	end
	if retiredHash ~= ARGV[10] then
		return {'version_immutable', ARGV[9], '', '', '', '', ''}
	end
end

local revision = redis.call('INCR', KEYS[8])
local payload = string.gsub(ARGV[12], '"registry_revision":0', '"registry_revision":' .. tostring(revision), 1)
redis.call('DEL', KEYS[2], KEYS[3], KEYS[4])
redis.call('HSET', KEYS[5],
	'id', ARGV[7], 'key', ARGV[8], 'version', ARGV[9], 'hash', ARGV[10], 'name', ARGV[17],
	'revision', tostring(revision), 'fingerprint', ARGV[11], 'payload', payload)
redis.call('HSET', KEYS[6],
	'id', ARGV[7], 'key', ARGV[8], 'version', ARGV[9], 'hash', ARGV[10], 'name', ARGV[17],
	'revision', tostring(revision), 'fingerprint', ARGV[11], 'payload', payload)
redis.call('HSET', KEYS[7],
	'id', ARGV[7], 'key', ARGV[8], 'version', ARGV[9], 'hash', ARGV[10], 'name', ARGV[17],
	'revision', tostring(revision), 'fingerprint', ARGV[11])

-- History follows the surviving record id. A replace that keeps the label
-- (an in-place update of the current version) retires nothing: writing the
-- previous payload under the live label would leave one label resolving to two
-- definitions and would trip the immutability check above on the next replace.
if ARGV[15] ~= ARGV[9] then
	if ARGV[3] ~= ARGV[7] then
		local history = redis.call('HGETALL', KEYS[13])
		if #history > 0 then
			redis.call('HSET', KEYS[15], unpack(history))
		end
		redis.call('DEL', KEYS[13])
		local meta = redis.call('HGETALL', KEYS[14])
		if #meta > 0 then
			redis.call('HSET', KEYS[16], unpack(meta))
		end
		redis.call('DEL', KEYS[14])
	end
	redis.call('HSET', KEYS[15], ARGV[15], previousPayload)
	-- The retired payload's registry revision is allocated per install, so it
	-- orders retirements within one workflow as well as reporting the revision
	-- the row was retired at.
	redis.call('HSET', KEYS[16], ARGV[15], ARGV[5] .. ' ' .. ARGV[6])
end
-- The installed version is live again, so it is no longer retired.
redis.call('HDEL', KEYS[15], ARGV[9])
redis.call('HDEL', KEYS[16], ARGV[9])

-- Best-effort retention: evict the oldest retired versions beyond the limit.
-- No reader of the live record depends on history, so a lost eviction only
-- means a longer rollback reach, never a wrong answer.
local retention = tonumber(ARGV[19]) or 0
if retention > 0 then
	local meta = redis.call('HGETALL', KEYS[16])
	local retiredCount = #meta / 2
	if retiredCount > retention then
		local retiredVersions = {}
		for index = 1, #meta, 2 do
			local value = meta[index + 1]
			local separator = string.find(value, ' ', 1, true)
			local retiredRevision = 0
			if separator then
				retiredRevision = tonumber(string.sub(value, separator + 1)) or 0
			end
			retiredVersions[#retiredVersions + 1] = {version = meta[index], revision = retiredRevision}
		end
		table.sort(retiredVersions, function(a, b) return a.revision > b.revision end)
		for index = retention + 1, #retiredVersions do
			redis.call('HDEL', KEYS[15], retiredVersions[index].version)
			redis.call('HDEL', KEYS[16], retiredVersions[index].version)
		end
	end
end

-- The destination name now resolves to the surviving id; a rename drops the
-- source name's entry so the freed name can be taken again.
if ARGV[17] ~= '' then
	redis.call('SET', KEYS[11], ARGV[7])
end
if ARGV[18] ~= ARGV[17] and ARGV[18] ~= '' and redis.call('GET', KEYS[12]) == ARGV[3] then
	redis.call('DEL', KEYS[12])
end

redis.call('HSET', KEYS[1],
	'mutation_id', ARGV[1], 'fingerprint', ARGV[2], 'status', 'replaced',
	'current_payload', payload, 'previous_id', ARGV[3],
	'previous_key', ARGV[4], 'previous_version', previousVersion,
	'previous_hash', ARGV[5], 'previous_revision', ARGV[6],
	'projection_namespace', ARGV[16], 'projection_state', 'pending')
redis.call('SADD', KEYS[9], ARGV[1])
-- A replacement usually retires the source id and installs a different
-- destination id, so the index must lose the source membership as well as gain
-- the destination one. When both ids are equal this is a re-score, and ZREM
-- before ZADD leaves exactly one membership at the new revision.
redis.call('ZREM', KEYS[10], ARGV[3])
redis.call('ZADD', KEYS[10], -revision, ARGV[7])
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

// reconcileWorkflowIndexLua repairs the per-namespace workflow index against
// the records that are actually live in the same namespace, and is the only
// index operation that is not part of a record mutation.
//
// It is not a mutation of authority state, so it does not need to be atomic
// with one: it never writes a bykey/byid/meta/revision key, only the index, and
// every membership it inserts or drops is derived from keys observed inside the
// same script run. A concurrent add/replace/remove either happens before the
// SCAN (and is therefore observed) or after the script (and maintains the index
// itself), so the repair can never invent or destroy a membership that the
// authority write path would not.
//
// It exists for the upgrade case: records written before this index key
// existed carry no membership. SCAN is safe here precisely because KEYS[2] is
// the namespace-prefixed byid pattern, so the traversal is bounded to one
// namespace and one Cluster slot rather than being a keyspace-wide walk.
//
// KEYS[1] is the byid prefix (also the id-stripping base), KEYS[2] is that
// prefix with the trailing wildcard, and KEYS[3] is the index.
const reconcileWorkflowIndexScript = `
local function revisionOf(key)
	return tonumber(redis.call('HGET', key, 'revision'))
end

local revisions = {}
local cursor = '0'
repeat
	local batch = redis.call('SCAN', cursor, 'MATCH', KEYS[2], 'COUNT', 256)
	cursor = batch[1]
	for _, key in ipairs(batch[2]) do
		local revision = revisionOf(key)
		if revision then
			revisions[key] = revision
		end
	end
until cursor == '0'

local existing = redis.call('ZRANGE', KEYS[3], 0, -1, 'WITHSCORES')
local indexed = {}
for i = 1, #existing, 2 do
	indexed[existing[i]] = tonumber(existing[i + 1])
end

local added = 0
for key, revision in pairs(revisions) do
	local id = string.sub(key, string.len(KEYS[1]) + 1)
	if indexed[id] ~= -revision then
		redis.call('ZADD', KEYS[3], -revision, id)
		added = added + 1
	end
end

local removed = 0
for id, _ in pairs(indexed) do
	if revisions[KEYS[1] .. id] == nil then
		redis.call('ZREM', KEYS[3], id)
		removed = removed + 1
	end
end
return {tostring(added), tostring(removed)}
`

var reconcileWorkflowIndexLua = redis.NewScript(reconcileWorkflowIndexScript)

// ListWorkflows returns one page of the namespace's workflow ids, newest
// registry revision first and then by id ascending.
//
// ns is a required scope that is validated and then used as the only source of
// the keys this read touches, so an empty, malformed, or unknown namespace can
// never widen the scope: cross-namespace enumeration is structurally
// impossible rather than merely unimplemented. The context namespace is
// deliberately not consulted, so a caller cannot get a different scope than the
// one it named.
//
// Two properties matter for the caller's correctness:
//
//   - Only live ids are returned. Each candidate is confirmed against its byid
//     record inside one pipeline, so a membership left behind by an out-of-band
//     key deletion is skipped rather than handed to a caller that would then
//     fail to resolve it. The read itself never writes.
//   - The index is self-healing for data that predates it. When a page starts
//     at offset zero and resolves to no live ids at all, the index is
//     reconciled against the namespace's live byid records and the read is
//     retried once. That is what makes records written before this key existed
//     enumerable; a populated namespace pays a single ZRANGE plus one pipeline
//     on the hot path and never scans.
func (r *Registry) ListWorkflows(ctx context.Context, ns namespace.Namespace, opts backend.WorkflowListOptions) ([]types.WorkflowID, error) {
	if err := namespace.Validate(ns); err != nil {
		return nil, fmt.Errorf("list workflows: %w", err)
	}
	if opts.Limit < 0 || opts.Offset < 0 {
		return nil, fmt.Errorf("list workflows: limit and offset must not be negative (limit=%d offset=%d)", opts.Limit, opts.Offset)
	}
	offset := int64(opts.Offset)
	stop := int64(-1)
	if opts.Limit > 0 {
		stop = offset + int64(opts.Limit) - 1
	}

	ids, err := r.liveWorkflowIDs(ctx, ns, offset, stop)
	if err != nil {
		return nil, err
	}
	if len(ids) > 0 || opts.Offset != 0 {
		return ids, nil
	}

	if _, reconcileErr := reconcileWorkflowIndexLua.Run(
		ctx,
		r.rdb,
		[]string{
			workflowByIDKeyPrefix(ns, ""),
			workflowByIDKeyPrefix(ns, "") + "*",
			workflowIndexKey(ns),
		},
	).Result(); reconcileErr != nil {
		return nil, fmt.Errorf("list workflows for namespace %q: reconcile index: %w", ns, reconcileErr)
	}
	return r.liveWorkflowIDs(ctx, ns, offset, stop)
}

// liveWorkflowIDs reads one index page and drops members whose byid record is
// gone. The confirmation runs as a pipeline so a page costs one extra round
// trip regardless of size, and it issues reads only: pruning is the reconcile
// path's job, so a list can never surprise a concurrent writer.
func (r *Registry) liveWorkflowIDs(ctx context.Context, ns namespace.Namespace, offset, stop int64) ([]types.WorkflowID, error) {
	members, err := r.rdb.ZRange(ctx, workflowIndexKey(ns), offset, stop).Result()
	if err != nil {
		return nil, fmt.Errorf("list workflows for namespace %q: %w", ns, err)
	}
	if len(members) == 0 {
		return nil, nil
	}

	pipe := r.rdb.Pipeline()
	exists := make([]*redis.IntCmd, len(members))
	for i, member := range members {
		exists[i] = pipe.Exists(ctx, workflowByIDKey(ns, "", types.WorkflowID(member)))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("list workflows for namespace %q: confirm index members: %w", ns, err)
	}

	ids := make([]types.WorkflowID, 0, len(members))
	for i, member := range members {
		live, cmdErr := exists[i].Result()
		if cmdErr != nil {
			return nil, fmt.Errorf("list workflows for namespace %q: confirm index member %q: %w", ns, member, cmdErr)
		}
		if live == 0 {
			continue
		}
		ids = append(ids, types.WorkflowID(member))
	}
	return ids, nil
}

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
			// The fast path enforces the same rule as the add script: the
			// (name, version) key is live under a different definition, so the
			// label is already published and cannot resolve to two definitions.
			return backend.WorkflowRecord{}, &backend.WorkflowVersionImmutableError{Version: rec.Version}
		}
		// Repair a name index entry an idempotent add would otherwise never
		// write (a record registered before the index existed).
		if rec.Name != "" {
			if err := r.rdb.Set(ctx, workflowNameIndexKey(t, rec.Name), string(existing.ID), 0).Err(); err != nil {
				return backend.WorkflowRecord{}, indeterminateMutation("add workflow "+strconv.Quote(rec.Key), err)
			}
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
			workflowIndexKey(t),
			workflowNameIndexKey(t, stored.Name),
		},
		string(stored.ID),
		stored.Key,
		stored.Version,
		stored.DefinitionHash,
		stored.fingerprint,
		stored.payload,
		stored.Name,
	).Result()
	if err != nil {
		return backend.WorkflowRecord{}, indeterminateMutation("add workflow "+strconv.Quote(stored.Key), err)
	}

	state, payload, err := decodeTwoPartScriptResult(result)
	if err != nil {
		return backend.WorkflowRecord{}, indeterminateMutation("add workflow "+strconv.Quote(stored.Key), err)
	}
	switch state {
	case "id_conflict":
		return backend.WorkflowRecord{}, backend.ErrWorkflowConflict
	case "version_immutable":
		return backend.WorkflowRecord{}, &backend.WorkflowVersionImmutableError{Version: stored.Version}
	case "name_conflict":
		holder, holderErr := scriptString(payload)
		if holderErr != nil {
			return backend.WorkflowRecord{}, indeterminateMutation("add workflow "+strconv.Quote(stored.Key), holderErr)
		}
		return backend.WorkflowRecord{}, &backend.WorkflowNameConflictError{Holder: types.WorkflowID(holder)}
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

// ListWorkflowVersions returns the current version first, then every retired
// version newest retirement first. Retired rows are ordered by the registry
// revision the payload carried while it was live, which the retirement
// recorded, so the order is the order the versions were retired in.
func (r *Registry) ListWorkflowVersions(ctx context.Context, id types.WorkflowID) ([]backend.WorkflowVersionInfo, error) {
	t := namespace.FromContext(ctx)
	if err := namespace.Validate(t); err != nil {
		return nil, fmt.Errorf("list workflow versions: %w", err)
	}
	// GetWorkflow, not a raw HGET: a workflow that still lives in v1 storage is
	// imported on read, and the current row must describe the same record the
	// caller would get from GetWorkflow.
	current, err := r.GetWorkflow(ctx, id)
	if err != nil {
		return nil, err
	}
	meta, err := r.rdb.HGetAll(ctx, workflowVersionMetaKey(t, id)).Result()
	if err != nil {
		return nil, fmt.Errorf("list workflow versions %q: %w", id, err)
	}
	retired := make([]backend.WorkflowVersionInfo, 0, len(meta))
	for version, value := range meta {
		if version == current.Version {
			// Promotion removes a label from history in the same script that
			// installs it, so this can only describe a record written before
			// that invariant existed. The current row already reports it.
			continue
		}
		hash, revision := splitVersionMeta(value)
		retired = append(retired, backend.WorkflowVersionInfo{
			Version:          version,
			DefinitionHash:   hash,
			RegistryRevision: revision,
			Archived:         true,
		})
	}
	sort.Slice(retired, func(i, j int) bool {
		if retired[i].RegistryRevision != retired[j].RegistryRevision {
			return retired[i].RegistryRevision > retired[j].RegistryRevision
		}
		return retired[i].Version < retired[j].Version
	})
	rows := make([]backend.WorkflowVersionInfo, 0, len(retired)+1)
	rows = append(rows, backend.WorkflowVersionInfo{
		Version:          current.Version,
		DefinitionHash:   current.DefinitionHash,
		RegistryRevision: current.RegistryRevision,
		Current:          true,
	})
	return append(rows, retired...), nil
}

// GetWorkflowVersion returns the addressed version of the workflow. The current
// version (and an empty version, which callers use for "the current one")
// resolves through GetWorkflow; anything else comes from the workflow's
// retired-version history verbatim, including its Key and Name as they were
// when it was retired.
func (r *Registry) GetWorkflowVersion(ctx context.Context, id types.WorkflowID, version string) (backend.WorkflowRecord, error) {
	t := namespace.FromContext(ctx)
	if err := namespace.Validate(t); err != nil {
		return backend.WorkflowRecord{}, fmt.Errorf("get workflow version: %w", err)
	}
	current, err := r.GetWorkflow(ctx, id)
	if err != nil {
		return backend.WorkflowRecord{}, err
	}
	if version == "" || version == current.Version {
		return current, nil
	}
	raw, err := r.rdb.HGet(ctx, workflowVersionHistoryKey(t, id), version).Bytes()
	if errors.Is(err, redis.Nil) {
		return backend.WorkflowRecord{}, backend.ErrWorkflowVersionNotFound
	}
	if err != nil {
		return backend.WorkflowRecord{}, fmt.Errorf("get workflow version %q of %q: %w", version, id, err)
	}
	record, err := unmarshalWorkflowRecord(raw)
	if err != nil {
		return backend.WorkflowRecord{}, fmt.Errorf("get workflow version %q of %q: %w", version, id, err)
	}
	return record, nil
}

// splitVersionMeta decodes one retired-version meta entry, stored as
// "<definition-hash> <registry-revision>". A value without a revision only
// happens for a record written by an older script; its history row then sorts
// last rather than being dropped.
func splitVersionMeta(value string) (string, uint64) {
	hash, revisionText, found := strings.Cut(value, " ")
	if !found {
		return value, 0
	}
	revision, err := strconv.ParseUint(revisionText, 10, 64)
	if err != nil {
		return hash, 0
	}
	return hash, revision
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
			workflowIndexKey(t),
			workflowNameIndexKey(t, stored.Name),
		},
		string(stored.ID),
		stored.Key,
		stored.Version,
		stored.DefinitionHash,
		stored.fingerprint,
		stored.payload,
		stored.Name,
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
	case "name_conflict":
		holder, holderErr := scriptString(payload)
		if holderErr != nil {
			return backend.WorkflowRecord{}, indeterminateMutation("import legacy workflow "+strconv.Quote(stored.Key), holderErr)
		}
		return backend.WorkflowRecord{}, &backend.WorkflowNameConflictError{Holder: types.WorkflowID(holder)}
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
			workflowIndexKey(t),
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
		key, name, revision, found, err := r.removeSnapshot(ctx, t, id)
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
				workflowIndexKey(t),
				workflowNameIndexKey(t, name),
				workflowVersionHistoryKey(t, id),
				workflowVersionMetaKey(t, id),
			},
			string(id),
			key,
			strconv.FormatUint(revision, 10),
			name,
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

func (r *Registry) removeSnapshot(ctx context.Context, t namespace.Namespace, id types.WorkflowID) (string, string, uint64, bool, error) {
	values, err := r.rdb.HMGet(ctx, workflowIDMapKey(t, id), "key", "revision", "version", "name").Result()
	if err != nil {
		return "", "", 0, false, err
	}
	if len(values) == 4 && values[0] != nil && values[1] != nil {
		key, keyOK := values[0].(string)
		revisionText, revisionOK := values[1].(string)
		if keyOK && revisionOK {
			revision, parseErr := strconv.ParseUint(revisionText, 10, 64)
			if parseErr != nil {
				return "", "", 0, false, fmt.Errorf("parse registry revision: %w", parseErr)
			}
			version, _ := values[2].(string)
			name, _ := values[3].(string)
			if name == "" {
				// A record written before the name field existed. The name is
				// recoverable from the key it was registered under, which the
				// server built as "<namespace>/<name>@<version>".
				name = workflowNameFromKey(t, key, version)
			}
			return key, name, revision, true, nil
		}
	}

	// A missing meta hash is corrupt but recoverable when byid still has a full
	// payload. Use it only to identify the complete KEYS set; the Lua script
	// re-validates all surviving indexes before deleting anything.
	record, err := r.GetWorkflow(ctx, id)
	if errors.Is(err, backend.ErrWorkflowNotFound) {
		return "", "", 0, false, nil
	}
	if err != nil {
		return "", "", 0, false, err
	}
	return record.Key, record.Name, record.RegistryRevision, true, nil
}

// workflowNameFromKey recovers a record's name from the registry key the server
// built for it. It returns "" when the key does not have that shape, which
// leaves the name index untouched rather than deleting a possibly-live entry.
func workflowNameFromKey(t namespace.Namespace, key, version string) string {
	prefix := string(t) + "/"
	if !strings.HasPrefix(key, prefix) || version == "" {
		return ""
	}
	remainder := key[len(prefix):]
	suffix := "@" + version
	if !strings.HasSuffix(remainder, suffix) || len(remainder) <= len(suffix) {
		return ""
	}
	return remainder[:len(remainder)-len(suffix)]
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
	// in v1 is first imported using the same conflict-safe path as Get. The
	// imported record also supplies the source's name for the script's name
	// index maintenance: the script re-validates the source revision, so a name
	// read here cannot belong to a record the CAS did not just accept.
	sourceName := ""
	source, sourceErr := r.GetWorkflow(ctx, req.Expected.ID)
	if sourceErr != nil {
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
	} else {
		sourceName = source.Name
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
			workflowIndexKey(t),
			workflowNameIndexKey(t, stored.Name),
			workflowNameIndexKey(t, sourceName),
			workflowVersionHistoryKey(t, req.Expected.ID),
			workflowVersionMetaKey(t, req.Expected.ID),
			workflowVersionHistoryKey(t, stored.ID),
			workflowVersionMetaKey(t, stored.ID),
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
		stored.Name,
		sourceName,
		r.retentionScriptValue(),
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
	case "name_conflict":
		holder, holderErr := scriptString(values[1])
		if holderErr != nil {
			return backend.WorkflowReplaceResult{}, indeterminateMutation("replace workflow "+strconv.Quote(req.MutationID), holderErr)
		}
		return backend.WorkflowReplaceResult{}, &backend.WorkflowNameConflictError{Holder: types.WorkflowID(holder)}
	case "version_immutable":
		version, versionErr := scriptString(values[1])
		if versionErr != nil {
			return backend.WorkflowReplaceResult{}, indeterminateMutation("replace workflow "+strconv.Quote(req.MutationID), versionErr)
		}
		return backend.WorkflowReplaceResult{}, &backend.WorkflowVersionImmutableError{Version: version}
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
	ID  types.WorkflowID
	Key string
	// Name is the server-issued workflow name, indexed separately from the key
	// so "at most one live record per (namespace, name)" can be enforced. It is
	// empty for a caller that never set one, which exempts the record from the
	// name index.
	Name           string
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
		Name:           rec.Name,
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
		EditorMetadata:   rec.EditorMetadata,
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
		EditorMetadata:   stored.EditorMetadata,
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
