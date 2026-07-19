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

var addWorkflowRecordLua = redis.NewScript(`
local existingID = redis.call('GET', KEYS[1])
if existingID then
	local existingRaw = redis.call('GET', ARGV[1] .. existingID)
	if existingRaw then
		local existing = cjson.decode(existingRaw)
		if existing.definition_hash ~= ARGV[2] then
			return {'conflict', existingRaw}
		end
		return {'existing', existingRaw}
	end
	redis.call('DEL', KEYS[1])
end
redis.call('SET', KEYS[1], ARGV[3])
redis.call('SET', KEYS[2], ARGV[4])
return {'created', ARGV[4]}
`)

var removeWorkflowRecordLua = redis.NewScript(`
local n = redis.call('DEL', KEYS[1], KEYS[2])
return n
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

func (r *Registry) RemoveWorkflow(ctx context.Context, id types.WorkflowID) error {
	t := tenant.FromContext(ctx)
	key, err := r.rdb.Get(ctx, workflowIDMapKey(t, id)).Result()
	if errors.Is(err, redis.Nil) {
		return backend.ErrWorkflowNotFound
	}
	if err != nil {
		return fmt.Errorf("remove workflow %q: %w", id, err)
	}

	// bykey and byid:<id> share the `{<key>}` tag -> same slot -> Lua-safe.
	res, err := removeWorkflowRecordLua.Run(
		ctx,
		r.rdb,
		[]string{workflowByKeyKey(t, key), workflowByIDKey(t, key, id)},
	).Result()
	if err != nil {
		return fmt.Errorf("remove workflow %q: %w", id, err)
	}

	// Best-effort cleanup of the untagged reverse index (single-key op).
	_ = r.rdb.Del(ctx, workflowIDMapKey(t, id)).Err()

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
