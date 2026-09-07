package rstate

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

func (s *Store) PutOutput(ctx context.Context, id types.ExecutionID, name string, data map[string]any) error {
	t := namespace.FromContext(ctx)
	b, _ := json.Marshal(data) // json.Marshal of map[string]any cannot fail
	if err := s.rdb.Set(ctx, outputKey(t, id, name), string(b), s.getExecTTL(ctx, id)).Err(); err != nil {
		return err
	}
	return s.refreshTransientTTL(ctx, id, outputKey(t, id, name))
}

func (s *Store) GetOutput(ctx context.Context, id types.ExecutionID, name string) (map[string]any, error) {
	t := namespace.FromContext(ctx)
	raw, err := s.rdb.Get(ctx, outputKey(t, id, name)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("unmarshal output %q/%q: %w", id, name, err)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// TTL renewal
// ---------------------------------------------------------------------------

// suspendNodeTTLKeys enumerates every per-node key that must stay alive while a
// node is parked waiting for a signal, derived from the suspend spec so the set
// mirrors exactly what the suspend write paths (SuspendTaskLease /
// SuspendOrConsume) persist. Keeping this the single source of truth for the
// per-node key set prevents TTL-renewal and cancellation cleanup from drifting
// out of sync with the writers and silently leaking or losing keys.
//
// Signal-name-keyed keys (waiter/signal) are only included when spec is
// non-nil; the caller supplies the spec it used to park the node.
func suspendNodeTTLKeys(t namespace.Namespace, id types.ExecutionID, nodeName string, spec *types.SuspendSpec) []string {
	keys := []string{
		nodeStatusKey(t, id, nodeName),
		nodeMetaKey(t, id, nodeName),
		outputKey(t, id, nodeName),
		waiterSpecKey(t, id, nodeName),
		signalBatchKey(t, id, nodeName),
	}
	if spec != nil {
		for _, sigName := range spec.Signals {
			keys = append(keys, waiterKey(t, id, sigName), signalKey(t, id, sigName))
		}
	}
	return keys
}

// extendExecTTL renews the TTL on all keys related to an execution and the
// specific suspended node. Called when a node is parked (suspended) to prevent
// keys from expiring while waiting for a signal. A failure here is surfaced
// rather than swallowed: if the TTL is not extended the execution/node keys may
// expire while a node is suspended, causing the eventual resume to silently
// target missing state.
//
// spec is the suspend spec the node was parked with; it is required so the
// waiter/signal/meta keys the resume path reads (nodeMeta carries
// activation_id/committed_lease_token, waiterSpec/signalBatch carry the
// multi-signal quorum state) are renewed alongside the execution-level keys. A
// nil spec renews only the node-name-keyed subset.
func (s *Store) extendExecTTL(ctx context.Context, id types.ExecutionID, nodeName string, spec *types.SuspendSpec, ttl time.Duration) error {
	t := namespace.FromContext(ctx)
	pipe := s.rdb.Pipeline()
	pipe.Expire(ctx, execKey(t, id, "status"), ttl)
	pipe.Expire(ctx, execKey(t, id, "params"), ttl)
	pipe.Expire(ctx, execKey(t, id, "runtime"), ttl)
	pipe.Expire(ctx, execKey(t, id, "trace_id"), ttl)
	pipe.Expire(ctx, execKey(t, id, "span_id"), ttl)
	pipe.Expire(ctx, execKey(t, id, "trace_carrier"), ttl)
	pipe.Expire(ctx, execKey(t, id, "graph"), ttl)
	pipe.Expire(ctx, suspendedNodesKey(t, id), ttl)
	pipe.Expire(ctx, timeoutZSetKey(t, id), ttl)
	for _, key := range suspendNodeTTLKeys(t, id, nodeName, spec) {
		pipe.Expire(ctx, key, ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return fmt.Errorf("extend exec ttl %q/%q: %w", id, nodeName, err)
	}
	return nil
}

// suspendTTL returns the TTL to use when a node is suspended.
// It picks the larger of the execution's base TTL and spec.Timeout + 1 hour.
//
// The base TTL comes from getExecTTL, which resolves the explicit per-execution
// override first, then the per-workflow transient TTL, then the adapter default.
// Reading s.execTTLs directly would miss the transient case: a per-workflow
// transient execution never writes that map, so its suspended keys would be
// EXPIREd to the durable retention window -- hours of extra life for an
// execution that declared itself ephemeral.
func (s *Store) suspendTTL(ctx context.Context, id types.ExecutionID, spec *types.SuspendSpec) time.Duration {
	ttl := s.getExecTTL(ctx, id)

	if spec != nil && spec.Timeout > 0 {
		candidate := spec.Timeout + 1*time.Hour
		if candidate > ttl {
			ttl = candidate
		}
	}
	return ttl
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// doneChannel returns the Pub/Sub channel name for execution completion events.
