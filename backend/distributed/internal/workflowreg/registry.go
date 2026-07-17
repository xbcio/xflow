package workflowreg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

type Registry struct {
	rdb *redis.Client
}

type storedWorkflowRecord struct {
	ID             types.WorkflowID   `json:"id"`
	Key            string             `json:"key"`
	Namespace      string             `json:"namespace"`
	Name           string             `json:"name"`
	Version        string             `json:"version"`
	DefinitionHash string             `json:"definition_hash"`
	Definition     *types.WorkflowDef `json:"definition,omitempty"`
	Graph          *graph.Graph       `json:"graph,omitempty"`
}

func New(rdb *redis.Client) *Registry {
	return &Registry{rdb: rdb}
}

func workflowByIDKey(id types.WorkflowID) string { return "xflow:workflow:id:" + string(id) }

func workflowByKeyKey(key string) string { return "xflow:workflow:key:" + key }

// KeyByID and KeyByKey expose the registry's Redis key schema for out-of-package
// readers (e.g. Backend-level tests and ops tooling) that must address the same
// keys the registry writes.
func KeyByID(id types.WorkflowID) string { return workflowByIDKey(id) }
func KeyByKey(key string) string         { return workflowByKeyKey(key) }

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
local existingRaw = redis.call('GET', KEYS[1])
if not existingRaw then
	return 0
end
local existing = cjson.decode(existingRaw)
redis.call('DEL', KEYS[1])
if existing.key then
	redis.call('DEL', ARGV[1] .. existing.key)
end
return 1
`)

func (r *Registry) AddWorkflow(ctx context.Context, rec backend.WorkflowRecord) (backend.WorkflowRecord, error) {
	stored, err := marshalWorkflowRecord(rec)
	if err != nil {
		return backend.WorkflowRecord{}, err
	}

	result, err := addWorkflowRecordLua.Run(
		ctx,
		r.rdb,
		[]string{workflowByKeyKey(stored.Key), workflowByIDKey(stored.ID)},
		"xflow:workflow:id:",
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
	return record, nil
}

func (r *Registry) GetWorkflow(ctx context.Context, id types.WorkflowID) (backend.WorkflowRecord, error) {
	raw, err := r.rdb.Get(ctx, workflowByIDKey(id)).Bytes()
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
	removed, err := removeWorkflowRecordLua.Run(
		ctx,
		r.rdb,
		[]string{workflowByIDKey(id)},
		"xflow:workflow:key:",
	).Int64()
	if err != nil {
		return fmt.Errorf("remove workflow %q: %w", id, err)
	}
	if removed == 0 {
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
		ID:             rec.ID,
		Key:            rec.Key,
		Namespace:      rec.Namespace,
		Name:           rec.Name,
		Version:        rec.Version,
		DefinitionHash: rec.DefinitionHash,
		Definition:     rec.Definition,
		Graph:          rec.Graph,
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
		ID:             stored.ID,
		Key:            stored.Key,
		Namespace:      stored.Namespace,
		Name:           stored.Name,
		Version:        stored.Version,
		DefinitionHash: stored.DefinitionHash,
		Definition:     stored.Definition,
		Graph:          stored.Graph,
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
