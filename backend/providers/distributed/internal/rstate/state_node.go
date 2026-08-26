package rstate

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
)

func (s *Store) LoadGraph(ctx context.Context, id types.ExecutionID) (*graph.Graph, error) {
	// Check in-memory cache first.
	s.mu.RLock()
	g := s.graphs[id]
	s.mu.RUnlock()
	if g != nil {
		return g, nil
	}

	// Load from Redis.
	t := namespace.FromContext(ctx)
	raw, err := s.rdb.Get(ctx, execKey(t, id, "graph")).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load graph %q: %w", id, err)
	}

	g = &graph.Graph{}
	if err := json.Unmarshal([]byte(raw), g); err != nil {
		return nil, fmt.Errorf("unmarshal graph %q: %w", id, err)
	}

	// Re-populate the in-memory cache.
	s.mu.Lock()
	s.graphs[id] = g
	s.mu.Unlock()

	return g, nil
}

// ---------------------------------------------------------------------------
// NodeStore
// ---------------------------------------------------------------------------

// UpsertNode writes a node snapshot. Unlike the fenced AcquireTaskLease (which
// performs status + meta + lease-index in one Lua transition), this path writes
// status (upsertNodeLua), then meta (HSet), then the lease index (ZAdd) as
// separate commands — a crash between them can leave the meta hash stale
// relative to the status key. This is the snapshot/recovery path, not the
// active lease-acquisition path, and the LeaseSweeper self-heals the divergence
// by pruning lease-index entries whose meta lacks a token. Callers that need a
// fenced transition must use AcquireTaskLease / CommitNode instead.
func (s *Store) UpsertNode(ctx context.Context, n *engine.NodeSnapshot) error {
	t := namespace.FromContext(ctx)
	key := nodeStatusKey(t, n.ExecutionID, n.Name)
	outKey := outputKey(t, n.ExecutionID, n.Name)
	metaKey := nodeMetaKey(t, n.ExecutionID, n.Name)

	var outputJSON string
	if n.Output != nil {
		b, _ := json.Marshal(n.Output) // json.Marshal of map[string]any cannot fail
		outputJSON = string(b)
	}
	var leasePayloadJSON string
	if n.LeasePayload != nil {
		encoded, err := json.Marshal(n.LeasePayload)
		if err != nil {
			return fmt.Errorf("marshal node lease payload %q/%q: %w", n.ExecutionID, n.Name, err)
		}
		leasePayloadJSON = string(encoded)
	}

	ttl := s.getExecTTL(ctx, n.ExecutionID)
	_, err := upsertNodeLua.Run(ctx, s.rdb,
		[]string{key, outKey, metaKey},
		string(n.Status), outputJSON, int(ttl.Seconds()), n.ActivationID,
	).Int64()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("upsert node %q/%q: %w", n.ExecutionID, n.Name, err)
	}
	keys := []string{key}
	if outputJSON != "" {
		keys = append(keys, outKey)
	}
	if n.LeaseID != "" || n.LeaseToken != "" || n.Attempt != 0 || n.ActivationID != 0 || n.AutoDepth != 0 || !n.LeaseIssuedAt.IsZero() || n.Port != "" || n.Error != "" || n.CommittedLeaseToken != "" || n.CommittedAttempt != 0 {
		meta := map[string]any{
			"lease_id":        string(n.LeaseID),
			"lease_token":     string(n.LeaseToken),
			"attempt":         n.Attempt,
			"activation_id":   n.ActivationID,
			"auto_depth":      n.AutoDepth,
			"node_idx":        n.NodeIdx,
			"lease_task_type": int(n.LeaseTaskType),
			"lease_payload":   leasePayloadJSON,
		}
		if !n.LeaseIssuedAt.IsZero() {
			meta["lease_issued_at_ms"] = n.LeaseIssuedAt.UnixMilli()
		}
		if n.LeaseTTL > 0 {
			meta["lease_ttl_ms"] = n.LeaseTTL.Milliseconds()
			if !n.LeaseIssuedAt.IsZero() {
				meta["lease_deadline_ms"] = n.LeaseIssuedAt.Add(n.LeaseTTL).UnixMilli()
			}
		}
		if n.Port != "" {
			meta["port"] = n.Port
		}
		if n.Error != "" {
			meta["error"] = n.Error
		}
		if n.CommittedLeaseToken != "" {
			meta["committed_lease_token"] = string(n.CommittedLeaseToken)
			meta["committed_attempt"] = n.CommittedAttempt
		}
		if err := s.rdb.HSet(ctx, metaKey, meta).Err(); err != nil {
			return fmt.Errorf("upsert node lease %q/%q: %w", n.ExecutionID, n.Name, err)
		}
		if err := s.rdb.Expire(ctx, metaKey, ttl).Err(); err != nil {
			return fmt.Errorf("expire node lease %q/%q: %w", n.ExecutionID, n.Name, err)
		}
		keys = append(keys, metaKey)
	}
	// Lease-expiry discovery is per execution so it shares the hash tag with
	// the node status and metadata. AcquireTaskLease updates all three in one
	// Lua command; this path keeps generic snapshot upserts recoverable too.
	leaseIndexKey := leaseExpiryZSetKey(t, n.ExecutionID)
	member := leaseExpiryMember(n.ExecutionID, n.Name)
	keys = append(keys, leaseIndexKey)
	if (n.Status == types.NodeStatusRunning || n.Status == types.NodeStatusCommitting || n.Status == types.NodeStatusWaiting) && n.LeaseToken != "" && !n.LeaseIssuedAt.IsZero() && n.LeaseTTL > 0 {
		expiryMs := float64(n.LeaseIssuedAt.Add(n.LeaseTTL).UnixMilli())
		if err := s.rdb.ZAdd(ctx, leaseIndexKey, redis.Z{Score: expiryMs, Member: member}).Err(); err != nil {
			return fmt.Errorf("index lease expiry %q/%q: %w", n.ExecutionID, n.Name, err)
		}
	} else if n.Status != types.NodeStatusRunning && n.Status != types.NodeStatusCommitting && n.Status != types.NodeStatusWaiting {
		// Terminal, suspended, and pending nodes have no recoverable lease.
		if err := s.rdb.ZRem(ctx, leaseIndexKey, member).Err(); err != nil {
			return fmt.Errorf("remove lease expiry %q/%q: %w", n.ExecutionID, n.Name, err)
		}
	}
	if err := s.refreshTransientTTL(ctx, n.ExecutionID, keys...); err != nil {
		return err
	}

	// isTransient, not s.transient: this row carries n.Output, the node's actual
	// payload. A per-workflow transient execution running on a control plane
	// whose global mode is off would otherwise persist it in full.
	if s.db != nil && !s.isTransient(ctx, n.ExecutionID) {
		var outBytes []byte
		if n.Output != nil {
			outBytes, _ = json.Marshal(n.Output) // json.Marshal of map[string]any cannot fail
		}
		rec := &store.NodeRecord{
			ExecutionID: n.ExecutionID,
			NodeName:    n.Name,
			Status:      n.Status,
			LeaseID:     string(n.LeaseID),
			LeaseToken:  string(n.LeaseToken),
			Attempt:     n.Attempt,
			Output:      outBytes,
			Port:        n.Port,
			UpdatedAt:   time.Now(),
		}
		s.auditWrite(ctx, "upsert_node", func(ctx context.Context) error {
			return s.db.UpsertNode(ctx, rec)
		})
	}
	return nil
}

// GetNode reads a node's status key and its meta hash. The two are issued in
// one pipeline rather than back to back: they are independent reads of the same
// node, so sequencing them buys nothing and costs a full network round trip on
// every call.
//
// The advance path used to call this for every acyclic node, which made it
// per-node hot-path traffic — engine/atomic.go's TaskTypeNodeAdvance branch
// read the source node back purely to learn the port the same commit had just
// written. That read is gone; the port now rides on the advance task, and the
// remaining calls from that branch are the legacy fallback (a pre-upgrade
// outbox entry) and the diagnostic on an advance that applied nothing. Both
// are subsets of what used to be every advance, so the pipelining still earns
// its keep, just on less traffic than this comment used to claim.
//
// Pipelining means the meta hash is fetched even when the status key is absent,
// where the sequential version returned early. That is one extra COMMAND on the
// miss path, not one extra round trip, and a node with meta but no status is
// exactly the half-expired shape callers want to see as absent anyway.
func (s *Store) GetNode(ctx context.Context, id types.ExecutionID, name string) (*engine.NodeSnapshot, error) {
	t := namespace.FromContext(ctx)
	pipe := s.rdb.Pipeline()
	statusCmd := pipe.Get(ctx, nodeStatusKey(t, id, name))
	metaCmd := pipe.HGetAll(ctx, nodeMetaKey(t, id, name))
	// Exec reports the first command error; the per-command errors below are the
	// authoritative ones, so a redis.Nil from the status GET must not abort here.
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("get node %q/%q: %w", id, name, err)
	}
	val, err := statusCmd.Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get node %q/%q: %w", id, name, err)
	}
	ns := &engine.NodeSnapshot{ExecutionID: id, Name: name, Status: types.NodeStatus(val)}
	meta, err := metaCmd.Result()
	if err != nil {
		return nil, fmt.Errorf("get node lease %q/%q: %w", id, name, err)
	}
	ns.LeaseID = engine.LeaseID(meta["lease_id"])
	ns.LeaseToken = engine.LeaseToken(meta["lease_token"])
	if nodeIdx := meta["node_idx"]; nodeIdx != "" {
		var parsed int
		if _, scanErr := fmt.Sscanf(nodeIdx, "%d", &parsed); scanErr == nil {
			ns.NodeIdx = parsed
		}
	}
	if attempt := meta["attempt"]; attempt != "" {
		var parsed int
		if _, scanErr := fmt.Sscanf(attempt, "%d", &parsed); scanErr == nil {
			ns.Attempt = parsed
		}
	}
	if activationID := meta["activation_id"]; activationID != "" {
		var parsed int
		if _, scanErr := fmt.Sscanf(activationID, "%d", &parsed); scanErr == nil {
			ns.ActivationID = parsed
		}
	}
	if autoDepth := meta["auto_depth"]; autoDepth != "" {
		var parsed int
		if _, scanErr := fmt.Sscanf(autoDepth, "%d", &parsed); scanErr == nil {
			ns.AutoDepth = parsed
		}
	}
	if issued := meta["lease_issued_at_ms"]; issued != "" {
		var ms int64
		if _, scanErr := fmt.Sscanf(issued, "%d", &ms); scanErr == nil && ms > 0 {
			ns.LeaseIssuedAt = time.UnixMilli(ms).UTC()
		}
	}
	if ttlMs := meta["lease_ttl_ms"]; ttlMs != "" {
		var ms int64
		if _, scanErr := fmt.Sscanf(ttlMs, "%d", &ms); scanErr == nil && ms > 0 {
			ns.LeaseTTL = time.Duration(ms) * time.Millisecond
		}
	}
	if taskType := meta["lease_task_type"]; taskType != "" {
		var parsed int
		if _, scanErr := fmt.Sscanf(taskType, "%d", &parsed); scanErr == nil {
			ns.LeaseTaskType = engine.TaskType(parsed)
		}
	}
	if rawPayload := meta["lease_payload"]; rawPayload != "" {
		var payload types.SignalPayload
		if err := json.Unmarshal([]byte(rawPayload), &payload); err != nil {
			return nil, fmt.Errorf("decode node lease payload %q/%q: %w", id, name, err)
		}
		ns.LeasePayload = &payload
	}
	ns.Port = meta["port"]
	ns.Error = meta["error"]
	ns.CommittedLeaseToken = engine.LeaseToken(meta["committed_lease_token"])
	if committedAttempt := meta["committed_attempt"]; committedAttempt != "" {
		var parsed int
		if _, scanErr := fmt.Sscanf(committedAttempt, "%d", &parsed); scanErr == nil {
			ns.CommittedAttempt = parsed
		}
	}
	return ns, nil
}
