package rstate

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

func (s *Store) PutOutput(ctx context.Context, id types.ExecutionID, name string, data map[string]any) error {
	t := namespace.FromContext(ctx)
	output := outputKey(t, id, name)
	meta := nodeMetaKey(t, id, name)
	ttl := s.getExecTTL(ctx, id)
	payload, err := s.encodeOutputValue(data)
	if err != nil {
		return fmt.Errorf("marshal output %q/%q: %w", id, name, err)
	}

	// PutOutput is intentionally policy-agnostic for runtime consumers. If a
	// previous fenced transition marked this output private, renew that marker
	// in the same Redis transaction as the output so it cannot expire before a
	// newly refreshed value. EXPIRE on an absent metadata hash is a no-op and
	// never creates a public marker.
	pipe := s.rdb.TxPipeline()
	pipe.Set(ctx, output, payload, ttl)
	pipe.Expire(ctx, meta, ttl)
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return err
	}
	return s.refreshTransientTTL(ctx, id, output, meta)
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
	out, err := s.decodeOutputValue(raw)
	if err != nil {
		return nil, fmt.Errorf("unmarshal output %q/%q: %w", id, name, err)
	}
	return out, nil
}

// GetOutputs is the batch form of GetOutput (engine.BatchOutputReader). One
// MGET reads every requested output of the execution.
//
// The keys share the execution's {id} hash tag (see keys.go), so the whole
// batch lands in one Redis Cluster slot: a cluster client can serve it as a
// single MGET instead of splitting it per slot or refusing it. That tag is what
// makes this method safe on the clustered deployment this backend targets, so
// do not build these keys any other way.
//
// A name with no stored output is absent from the result (MGET reports it as
// nil), which is the same absence GetOutput encodes as nil, nil. A present but
// corrupt value fails the whole batch with the offending name in the error:
// partial success would let a node run on a silently truncated fan-in.
func (s *Store) GetOutputs(ctx context.Context, id types.ExecutionID, names []string) (map[string]map[string]any, error) {
	if len(names) == 0 {
		return map[string]map[string]any{}, nil
	}
	t := namespace.FromContext(ctx)
	keys := make([]string, 0, len(names))
	unique := make([]string, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		unique = append(unique, name)
		keys = append(keys, outputKey(t, id, name))
	}
	raws, err := s.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("mget outputs %q: %w", id, err)
	}
	// MGet returns one element per key, in key order. Treating a length
	// mismatch as a backend failure keeps the positional mapping honest rather
	// than decoding a value under the wrong name.
	if len(raws) != len(unique) {
		return nil, fmt.Errorf("mget outputs %q: got %d values for %d keys", id, len(raws), len(unique))
	}
	outputs := make(map[string]map[string]any, len(unique))
	for i, raw := range raws {
		if raw == nil {
			continue
		}
		var payload []byte
		switch v := raw.(type) {
		case string:
			payload = []byte(v)
		case []byte:
			payload = v
		default:
			return nil, fmt.Errorf("unexpected output value type %T for %q/%q", raw, id, unique[i])
		}
		out, err := s.decodeOutputValue(payload)
		if err != nil {
			return nil, fmt.Errorf("unmarshal output %q/%q: %w", id, unique[i], err)
		}
		outputs[unique[i]] = out
	}
	return outputs, nil
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
	pipe.Expire(ctx, execKey(t, id, "test_run"), ttl)
	pipe.Expire(ctx, execKey(t, id, "graph"), ttl)
	pipe.Expire(ctx, execKey(t, id, "namespace"), ttl)
	// The workflow identity is written once with the execution, so a suspended
	// execution that outlives its initial TTL must renew it here too: otherwise
	// the status survives the wait while Inspect reports the execution as
	// unattributed.
	pipe.Expire(ctx, execKey(t, id, "workflow_id"), ttl)
	pipe.Expire(ctx, execKey(t, id, "workflow_version"), ttl)
	// The execution's clock is written once with the execution too, and must
	// survive a wait that outlives the creation retention for the same reason
	// the identity must: a resumed execution whose status lives on but whose
	// timestamps expired reports no start line and no duration.
	pipe.Expire(ctx, execKey(t, id, "created_at_ms"), ttl)
	pipe.Expire(ctx, execKey(t, id, "updated_at_ms"), ttl)
	// The time budget is written once with the execution as well. If it lapsed
	// while status lived on, GetExecution would read back a zero Deadline and
	// ExpireExecution would report the execution as never due: a silent failure.
	pipe.Expire(ctx, execKey(t, id, "deadline_ms"), ttl)
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
