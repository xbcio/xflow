package workflowreg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/tenant"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
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
	AuditFingerprint string             `json:"audit_fingerprint,omitempty"`
	Definition       *types.WorkflowDef `json:"definition,omitempty"`
	Graph            *graph.Graph       `json:"graph,omitempty"`
}

func New(rdb redis.UniversalClient) *Registry {
	return &Registry{rdb: rdb}
}

// Key schema (Redis Cluster-safe, G2 Phase 2 / Task 2.1 + Task 7.2 tenant scope).
//
// A workflow is addressed by its human-meaningful `key` (namespace/name@version).
// All per-workflow records live under a single hash-tagged slot so they can be
// touched atomically by one Lua script without triggering CROSSSLOT:
//
//	xflow:t<tenant>:workflow:{<key>}:bykey        -> workflow ID (the "exists" pointer)
//	xflow:t<tenant>:workflow:{<key>}:byid:<id>    -> storedWorkflowRecord payload (JSON)
//
// `{<key>}` is the literal Redis Cluster hash tag (the bytes between the first
// `{` and the first subsequent `}`), so bykey and byid:<id> always hash to the
// same slot regardless of <id>. The tenant prefix is intentionally brace-less:
// had it been `{tenant}`, the first {...} would steal the hash tag and collapse
// an entire tenant's workflows onto one slot. With `t<tenant>` the first `{`
// still opens the workflow key, so the hash tag remains `{<key>}`.
//
// GetWorkflow(id) is given only an id, which is not enough to construct a
// `{<key>}`-tagged key. We therefore keep a per-tenant reverse index
//
//	xflow:t<tenant>:workflow:idmap:<id>           -> key   (no hash tag, single-key op)
//
// which is cluster-safe on its own (single-key commands are never CROSSSLOT).
// It is written strictly after the tagged record is durable and is idempotent,
// so a crash between the two writes leaves the workflow addressable by key and
// re-registration self-heals the index (see AddWorkflow).
func workflowByKeyKey(t tenant.TenantID, key string) string {
	return "xflow:t" + string(t) + ":workflow:{" + key + "}:bykey"
}

func workflowByIDKey(t tenant.TenantID, key string, id types.WorkflowID) string {
	return "xflow:t" + string(t) + ":workflow:{" + key + "}:byid:" + string(id)
}

// workflowByIDKeyPrefix returns the byid key prefix INCLUDING the `{<key>}`
// hash tag and the brace-less tenant prefix. The addWorkflowRecordLua script
// concatenates this prefix with an existing workflow id to address an existing
// byid key; because the prefix already carries the tag, every key the script
// constructs lands in the same slot as the declared KEYS.
func workflowByIDKeyPrefix(t tenant.TenantID, key string) string {
	return "xflow:t" + string(t) + ":workflow:{" + key + "}:byid:"
}

func workflowIDMapKey(t tenant.TenantID, id types.WorkflowID) string {
	return "xflow:t" + string(t) + ":workflow:idmap:" + string(id)
}

// workflowDefHashKey stores only the definition_hash for a workflow in the same
// `{<key>}` hash-tagged slot as bykey/byid. This avoids cjson.decode of the
// full record inside Lua scripts that only need to compare hashes.
func workflowDefHashKey(t tenant.TenantID, key string, id types.WorkflowID) string {
	return "xflow:t" + string(t) + ":workflow:{" + key + "}:defhash:" + string(id)
}

// workflowDefHashKeyPrefix returns the defhash key prefix for Lua scripts that
// need to construct the full key by appending an existing workflow ID.
func workflowDefHashKeyPrefix(t tenant.TenantID, key string) string {
	return "xflow:t" + string(t) + ":workflow:{" + key + "}:defhash:"
}

// KeyByID, KeyByKey, and KeyIDMap expose the registry's Redis key schema for
// out-of-package readers (e.g. Backend-level tests and ops tooling) that must
// address the same keys the registry writes. KeyByID takes the workflow `key`
// because the byid record lives in the `{<key>}` hash-tagged slot. The tenant
// must come from the request context (tenant.FromContext), never a client
// request body.
func KeyByID(t tenant.TenantID, key string, id types.WorkflowID) string {
	return workflowByIDKey(t, key, id)
}
func KeyByKey(t tenant.TenantID, key string) string          { return workflowByKeyKey(t, key) }
func KeyIDMap(t tenant.TenantID, id types.WorkflowID) string { return workflowIDMapKey(t, id) }

// addWorkflowRecordLua registers a workflow atomically.
// KEYS[1] = bykey, KEYS[2] = byid:<newID>
// ARGV[1] = byid key prefix, ARGV[2] = new definition_hash,
// ARGV[3] = new workflow ID, ARGV[4] = new payload JSON,
// ARGV[5] = defhash key prefix
//
// Uses a lightweight defhash key (same slot) to compare hashes instead of
// cjson.decode on the full record — avoids cjson 1000-layer nesting limit and
// reduces decode overhead for large Definition+Graph payloads.
// Falls back to cjson.decode if the defhash key is absent (backward compat with
// records written before the defhash key was introduced).
var addWorkflowRecordLua = redis.NewScript(`
local existingID = redis.call('GET', KEYS[1])
if existingID then
	local existingRaw = redis.call('GET', ARGV[1] .. existingID)
	if existingRaw then
		local storedHash = redis.call('GET', ARGV[5] .. existingID)
		if not storedHash then
			-- Backward compat: defhash key missing, fall back to record decode.
			local existing = cjson.decode(existingRaw)
			storedHash = existing.definition_hash
			-- Backfill the defhash key for future lookups.
			redis.call('SET', ARGV[5] .. existingID, storedHash)
		end
		if storedHash ~= ARGV[2] then
			return {'conflict', existingRaw}
		end
		return {'existing', existingRaw}
	end
	redis.call('DEL', KEYS[1])
end
redis.call('SET', KEYS[1], ARGV[3])
redis.call('SET', KEYS[2], ARGV[4])
redis.call('SET', ARGV[5] .. ARGV[3], ARGV[2])
return {'created', ARGV[4]}
`)

// removeWorkflowRecordLua atomically deletes bykey, byid, and defhash keys.
// KEYS[1] = bykey, KEYS[2] = byid:<id>, KEYS[3] = defhash:<id>
// All share the `{<key>}` hash tag -> same slot -> Lua-safe.
var removeWorkflowRecordLua = redis.NewScript(`
local n = redis.call('DEL', KEYS[1], KEYS[2], KEYS[3])
return n
`)

// updateWorkflowHashLua atomically updates the definition_hash field of the
// record stored at byid:<id> (addressed as ARGV[1]..existingID, in the same
// slot as KEYS[1]). KEYS[1] is the bykey pointer (used only to derive the
// existing ID via GET); ARGV[1] is the byid key prefix; ARGV[2] is the
// expected old hash for CAS; ARGV[3] is the new payload (full re-marshalled
// record with the updated hash); ARGV[4] is the defhash key prefix;
// ARGV[5] is the new definition hash value.
//
// Uses the lightweight defhash key for CAS comparison instead of cjson.decode
// on the full record — avoids cjson nesting limits and large-object overhead.
// Falls back to cjson.decode if defhash key absent (backward compat).
//
// All keys the script touches share the `{<key>}` hash tag, so the script is
// Redis Cluster-safe.
var updateWorkflowHashLua = redis.NewScript(`
local existingID = redis.call('GET', KEYS[1])
if not existingID then
	return {'notfound', ''}
end
local raw = redis.call('GET', ARGV[1] .. existingID)
if not raw then
	return {'notfound', ''}
end
local storedHash = redis.call('GET', ARGV[4] .. existingID)
if not storedHash then
	-- Backward compat: defhash key missing, fall back to record decode.
	local rec = cjson.decode(raw)
	storedHash = rec.definition_hash
end
if storedHash ~= ARGV[2] then
	return {'conflict', raw}
end
redis.call('SET', ARGV[1] .. existingID, ARGV[3])
redis.call('SET', ARGV[4] .. existingID, ARGV[5])
return {'ok', ARGV[3]}
`)

func (r *Registry) AddWorkflow(ctx context.Context, rec backend.WorkflowRecord) (backend.WorkflowRecord, error) {
	t := tenant.FromContext(ctx)
	if err := tenant.Validate(t); err != nil {
		return backend.WorkflowRecord{}, fmt.Errorf("add workflow: %w", err)
	}

	stored, err := marshalWorkflowRecord(rec)
	if err != nil {
		return backend.WorkflowRecord{}, err
	}

	result, err := addWorkflowRecordLua.Run(
		ctx,
		r.rdb,
		[]string{workflowByKeyKey(t, stored.Key), workflowByIDKey(t, stored.Key, stored.ID)},
		workflowByIDKeyPrefix(t, stored.Key),
		stored.DefinitionHash,
		string(stored.ID),
		stored.payload,
		workflowDefHashKeyPrefix(t, stored.Key),
	).Result()
	if err != nil {
		return backend.WorkflowRecord{}, fmt.Errorf("add workflow %q: %w", stored.Key, err)
	}

	state, payload, err := decodeWorkflowScriptResult(result)
	if err != nil {
		return backend.WorkflowRecord{}, fmt.Errorf("add workflow %q: %w", stored.Key, err)
	}
	if state == "conflict" {
		return backend.WorkflowRecord{}, backend.ErrWorkflowConflict
	}

	record, err := unmarshalWorkflowRecord(payload)
	if err != nil {
		return backend.WorkflowRecord{}, fmt.Errorf("add workflow %q: %w", stored.Key, err)
	}

	// Maintain the id -> key reverse index so GetWorkflow(id) can resolve the
	// `{<key>}`-tagged record keys. Single-key op on an untagged key
	// (cluster-safe on its own), written after the tagged record is durable and
	// idempotent, so the "existing" path self-heals a partially-written index.
	if err := r.rdb.Set(ctx, workflowIDMapKey(t, record.ID), record.Key, 0).Err(); err != nil {
		return backend.WorkflowRecord{}, fmt.Errorf("add workflow %q: %w", stored.Key, err)
	}

	return record, nil
}

func (r *Registry) GetWorkflow(ctx context.Context, id types.WorkflowID) (backend.WorkflowRecord, error) {
	t := tenant.FromContext(ctx)
	key, err := r.rdb.Get(ctx, workflowIDMapKey(t, id)).Result()
	if errors.Is(err, redis.Nil) {
		return backend.WorkflowRecord{}, backend.ErrWorkflowNotFound
	}
	if err != nil {
		return backend.WorkflowRecord{}, fmt.Errorf("get workflow %q: %w", id, err)
	}

	raw, err := r.rdb.Get(ctx, workflowByIDKey(t, key, id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return backend.WorkflowRecord{}, backend.ErrWorkflowNotFound
	}
	if err != nil {
		return backend.WorkflowRecord{}, fmt.Errorf("get workflow %q: %w", id, err)
	}

	record, err := unmarshalWorkflowRecord(raw)
	if err != nil {
		return backend.WorkflowRecord{}, fmt.Errorf("get workflow %q: %w", id, err)
	}
	return record, nil
}

// GetWorkflowByKey fetches the record currently registered under key. The
// bykey pointer and the byid:<id> record share the `{<key>}` hash tag, so the
// two GETs are cluster-safe single-key ops in the same slot. Engine.AddWorkflow
// uses this to inspect a conflicting existing record for legacy-hash
// reconciliation.
func (r *Registry) GetWorkflowByKey(ctx context.Context, key string) (backend.WorkflowRecord, error) {
	t := tenant.FromContext(ctx)
	if err := tenant.Validate(t); err != nil {
		return backend.WorkflowRecord{}, fmt.Errorf("get workflow by key: %w", err)
	}

	id, err := r.rdb.Get(ctx, workflowByKeyKey(t, key)).Result()
	if errors.Is(err, redis.Nil) {
		return backend.WorkflowRecord{}, backend.ErrWorkflowNotFound
	}
	if err != nil {
		return backend.WorkflowRecord{}, fmt.Errorf("get workflow by key %q: %w", key, err)
	}

	raw, err := r.rdb.Get(ctx, workflowByIDKey(t, key, types.WorkflowID(id))).Bytes()
	if errors.Is(err, redis.Nil) {
		return backend.WorkflowRecord{}, backend.ErrWorkflowNotFound
	}
	if err != nil {
		return backend.WorkflowRecord{}, fmt.Errorf("get workflow by key %q: %w", key, err)
	}

	record, err := unmarshalWorkflowRecord(raw)
	if err != nil {
		return backend.WorkflowRecord{}, fmt.Errorf("get workflow by key %q: %w", key, err)
	}
	return record, nil
}

// UpdateDefinitionHash atomically upgrades the DefinitionHash of the record
// holding id, but only if the currently-stored hash still equals
// expectedOldHash. The CAS check + SET runs in a single Lua script on the
// `{<key>}` slot, so concurrent upgrades from multiple registrars cannot
// overwrite each other.
//
// The new payload is fully re-marshalled in Go (not by Lua cjson round-trip)
// to avoid any encoding drift on the complex nested Definition/Graph fields.
// Returns ErrWorkflowConflict if the stored hash no longer matches
// expectedOldHash (someone else upgraded it).
func (r *Registry) UpdateDefinitionHash(ctx context.Context, id types.WorkflowID, expectedOldHash, newHash string) error {
	t := tenant.FromContext(ctx)
	if err := tenant.Validate(t); err != nil {
		return fmt.Errorf("update workflow hash: %w", err)
	}

	// Resolve the workflow key from the (untagged, single-key) idmap reverse
	// index so we can address the `{<key>}`-tagged record slots.
	key, err := r.rdb.Get(ctx, workflowIDMapKey(t, id)).Result()
	if errors.Is(err, redis.Nil) {
		return backend.ErrWorkflowNotFound
	}
	if err != nil {
		return fmt.Errorf("update workflow hash %q: %w", id, err)
	}

	// Fetch existing record so we can re-marshal it with the updated hash.
	raw, err := r.rdb.Get(ctx, workflowByIDKey(t, key, id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return backend.ErrWorkflowNotFound
	}
	if err != nil {
		return fmt.Errorf("update workflow hash %q: %w", id, err)
	}
	existing, err := unmarshalWorkflowRecord(raw)
	if err != nil {
		return fmt.Errorf("update workflow hash %q: %w", id, err)
	}

	// Short-circuit: if the stored hash already equals newHash, another
	// registrar won the race; treat as success (idempotent).
	if existing.DefinitionHash == newHash {
		return nil
	}
	if existing.DefinitionHash != expectedOldHash {
		return backend.ErrWorkflowConflict
	}

	existing.DefinitionHash = newHash
	newPayload, err := marshalWorkflowRecordPayload(existing)
	if err != nil {
		return fmt.Errorf("update workflow hash %q: %w", id, err)
	}

	result, err := updateWorkflowHashLua.Run(
		ctx,
		r.rdb,
		[]string{workflowByKeyKey(t, key)},
		workflowByIDKeyPrefix(t, key),
		expectedOldHash,
		string(newPayload),
		workflowDefHashKeyPrefix(t, key),
		newHash,
	).Result()
	if err != nil {
		return fmt.Errorf("update workflow hash %q: %w", id, err)
	}

	state, _, err := decodeWorkflowScriptResult(result)
	if err != nil {
		return fmt.Errorf("update workflow hash %q: %w", id, err)
	}
	switch state {
	case "ok":
		return nil
	case "conflict":
		return backend.ErrWorkflowConflict
	case "notfound":
		return backend.ErrWorkflowNotFound
	default:
		return fmt.Errorf("update workflow hash %q: unexpected script state %q", id, state)
	}
}

func (r *Registry) RemoveWorkflow(ctx context.Context, id types.WorkflowID) error {
	t := tenant.FromContext(ctx)
	key, err := r.rdb.Get(ctx, workflowIDMapKey(t, id)).Result()
	if errors.Is(err, redis.Nil) {
		return backend.ErrWorkflowNotFound
	}
	if err != nil {
		return fmt.Errorf("remove workflow %q: %w", id, err)
	}

	// bykey, byid:<id>, and defhash:<id> share the `{<key>}` tag -> same slot -> Lua-safe.
	res, err := removeWorkflowRecordLua.Run(
		ctx,
		r.rdb,
		[]string{workflowByKeyKey(t, key), workflowByIDKey(t, key, id), workflowDefHashKey(t, key, id)},
	).Result()
	if err != nil {
		return fmt.Errorf("remove workflow %q: %w", id, err)
	}

	// Delete the untagged reverse index (single-key op, different slot so cannot
	// be in the Lua script). Do not swallow errors — a leftover idmap entry would
	// leave a dangling pointer for GetWorkflow(id).
	if err := r.rdb.Del(ctx, workflowIDMapKey(t, id)).Err(); err != nil {
		return fmt.Errorf("remove workflow %q idmap cleanup: %w", id, err)
	}

	// A zero count means neither bykey nor byid existed: the workflow was not
	// actually present, so report it as not found even if the idmap index was
	// stale (corrupt state).
	if n, ok := res.(int64); ok && n == 0 {
		return backend.ErrWorkflowNotFound
	}
	return nil
}

type marshaledWorkflowRecord struct {
	ID             types.WorkflowID
	Key            string
	DefinitionHash string
	payload        []byte
}

func marshalWorkflowRecord(rec backend.WorkflowRecord) (*marshaledWorkflowRecord, error) {
	stored := storedWorkflowRecord{
		ID:               rec.ID,
		Key:              rec.Key,
		Namespace:        rec.Namespace,
		Name:             rec.Name,
		Version:          rec.Version,
		DefinitionHash:   rec.DefinitionHash,
		AuditFingerprint: rec.AuditFingerprint,
		Definition:       rec.Definition,
		Graph:            rec.Graph,
	}
	if stored.ID == "" {
		stored.ID = types.WorkflowID(uuid.NewString())
	}
	if stored.Graph == nil && stored.Definition != nil {
		g, err := graph.Compile(stored.Definition)
		if err != nil {
			return nil, fmt.Errorf("compile workflow %q: %w", stored.Key, err)
		}
		stored.Graph = g
	}

	payload, err := json.Marshal(stored)
	if err != nil {
		return nil, fmt.Errorf("marshal workflow %q: %w", stored.Key, err)
	}

	return &marshaledWorkflowRecord{
		ID:             stored.ID,
		Key:            stored.Key,
		DefinitionHash: stored.DefinitionHash,
		payload:        payload,
	}, nil
}

// marshalWorkflowRecordPayload re-marshals an already-stored record (with
// updated fields such as DefinitionHash) back to the on-wire JSON payload,
// preserving the ID and Graph that were loaded from storage. It is used by
// UpdateDefinitionHash to write back the full record after a hash upgrade
// without going through a Lua cjson round-trip.
func marshalWorkflowRecordPayload(rec backend.WorkflowRecord) ([]byte, error) {
	stored := storedWorkflowRecord{
		ID:               rec.ID,
		Key:              rec.Key,
		Namespace:        rec.Namespace,
		Name:             rec.Name,
		Version:          rec.Version,
		DefinitionHash:   rec.DefinitionHash,
		AuditFingerprint: rec.AuditFingerprint,
		Definition:       rec.Definition,
		Graph:            rec.Graph,
	}
	payload, err := json.Marshal(stored)
	if err != nil {
		return nil, fmt.Errorf("marshal workflow payload %q: %w", stored.Key, err)
	}
	return payload, nil
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
		AuditFingerprint: stored.AuditFingerprint,
		Definition:       stored.Definition,
		Graph:            stored.Graph,
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

func decodeWorkflowScriptResult(result any) (string, []byte, error) {
	values, ok := result.([]any)
	if !ok || len(values) != 2 {
		return "", nil, fmt.Errorf("unexpected workflow script result: %#v", result)
	}

	state, ok := values[0].(string)
	if !ok {
		return "", nil, fmt.Errorf("unexpected workflow script state: %#v", values[0])
	}

	switch payload := values[1].(type) {
	case string:
		return state, []byte(payload), nil
	case []byte:
		return state, payload, nil
	default:
		return "", nil, fmt.Errorf("unexpected workflow script payload: %#v", values[1])
	}
}
