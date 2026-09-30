package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/namespace"
)

var _ RunnerDescriptorDirectory = (*RedisRunnerDirectory)(nil)

// redisRunnerDescriptorRecord is the value stored per runner in
// keys.runnerDescriptors. It is written by redisRegisterRunnerLua in the same
// transition that replaces the session, and deleted by redisRemoveRunnerLua.
// The pool attribution and registration time travel with the descriptors so
// one field is the whole record.
type redisRunnerDescriptorRecord struct {
	PoolID             string                  `json:"pool_id,omitempty"`
	PoolName           string                  `json:"pool_name,omitempty"`
	RegisteredAtMillis int64                   `json:"registered_at_ms"`
	Descriptors        []redisRunnerDescriptor `json:"descriptors"`
}

type redisRunnerDescriptor struct {
	Type       string          `json:"type"`
	Version    int             `json:"version"`
	Hash       string          `json:"hash"`
	Descriptor json.RawMessage `json:"descriptor"`
}

// marshalRedisRunnerDescriptors encodes the register request's descriptors.
// It returns "" when there are none, which tells the Lua script to clear the
// field rather than store an empty record.
func marshalRedisRunnerDescriptors(req RegisterRunnerRequest, now time.Time) (string, error) {
	if len(req.Descriptors) == 0 {
		return "", nil
	}
	record := redisRunnerDescriptorRecord{
		PoolID:             req.PoolID,
		PoolName:           req.PoolName,
		RegisteredAtMillis: now.UnixMilli(),
		Descriptors:        make([]redisRunnerDescriptor, 0, len(req.Descriptors)),
	}
	for _, d := range req.Descriptors {
		record.Descriptors = append(record.Descriptors, redisRunnerDescriptor{
			Type: d.Type, Version: d.Version, Hash: d.Hash, Descriptor: d.JSON,
		})
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return "", fmt.Errorf("marshal runner descriptors: %w", err)
	}
	return string(raw), nil
}

func unmarshalRedisRunnerDescriptors(raw string) (redisRunnerDescriptorRecord, []RunnerNodeDescriptor, bool) {
	var record redisRunnerDescriptorRecord
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		return redisRunnerDescriptorRecord{}, nil, false
	}
	out := make([]RunnerNodeDescriptor, 0, len(record.Descriptors))
	for _, d := range record.Descriptors {
		out = append(out, RunnerNodeDescriptor{Type: d.Type, Version: d.Version, Hash: d.Hash, JSON: d.Descriptor})
	}
	sortRunnerNodeDescriptors(out)
	return record, out, true
}

// LiveRunnerDescriptors implements RunnerDescriptorDirectory. Unlike
// ListLiveRunners it propagates Redis errors, so a caller can tell a down
// Redis from a fleet that reports nothing. A record that fails to decode, or
// whose runner has no session, is skipped.
func (d *RedisRunnerDirectory) LiveRunnerDescriptors(ctx context.Context, now time.Time) ([]RunnerDescriptorRecord, error) {
	stored, err := d.rdb.HGetAll(ctx, d.keys.runnerDescriptors).Result()
	if err != nil {
		return nil, fmt.Errorf("read runner descriptors: %w", err)
	}
	if len(stored) == 0 {
		return nil, nil
	}
	runnerIDs := make([]string, 0, len(stored))
	for runnerID := range stored {
		runnerIDs = append(runnerIDs, runnerID)
	}
	pipe := d.rdb.Pipeline()
	sessionCmd := pipe.HMGet(ctx, d.keys.runnerSession, runnerIDs...)
	heartbeatCmd := pipe.HMGet(ctx, d.keys.runnerHeartbeat, runnerIDs...)
	namespacesCmd := pipe.HMGet(ctx, d.keys.runnerNamespaces, runnerIDs...)
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("read runner descriptor liveness: %w", err)
	}
	sessions := sessionCmd.Val()
	heartbeats := heartbeatCmd.Val()
	namespacesList := namespacesCmd.Val()

	var out []RunnerDescriptorRecord
	for i, runnerID := range runnerIDs {
		if hmgetString(sessions, i) == "" {
			continue
		}
		heartbeatMillis, err := strconv.ParseInt(hmgetString(heartbeats, i), 10, 64)
		if err != nil || !runnerDescriptorsLive(time.UnixMilli(heartbeatMillis).UTC(), now) {
			continue
		}
		record, descriptors, ok := unmarshalRedisRunnerDescriptors(stored[runnerID])
		if !ok || len(descriptors) == 0 {
			continue
		}
		namespaces := normalizeRunnerNamespaces(nil)
		if raw := hmgetString(namespacesList, i); raw != "" {
			var decoded []namespace.Namespace
			if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
				continue
			}
			namespaces = normalizeRunnerNamespaces(decoded)
		}
		out = append(out, RunnerDescriptorRecord{
			RunnerID:     runnerID,
			PoolID:       record.PoolID,
			PoolName:     record.PoolName,
			Namespaces:   namespaces,
			RegisteredAt: time.UnixMilli(record.RegisteredAtMillis).UTC(),
			Descriptors:  descriptors,
		})
	}
	sortRunnerDescriptorRecords(out)
	return out, nil
}
