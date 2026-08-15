package rstate

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

const DefaultExecTTL = 24 * time.Hour

// ---------------------------------------------------------------------------
// Redis key helpers — all keys use {id} as a hash tag for cluster compatibility.
//
// Namespace boundary (Task 7.1): every execution-scoped key carries a
// brace-less namespace prefix, producing the shape
//
//	xflow:ns:<namespace>:exec:{<id>}:<suffix>
//
// The namespace prefix is intentionally NOT wrapped in braces. Redis Cluster's
// hash-tag rule takes the substring between the first '{' and the first '}'.
// Had the namespace been written as '{namespace}', the first {...} would be the
// namespace segment and every key for one namespace would collapse onto a single
// slot — both a hot-namespace hotspot and a break of the per-execution slot
// distribution. With the brace-less `ns:<namespace>` prefix the first '{' still
// opens the execution ID, so the hash tag remains {<id>}: keys for one
// execution stay co-located (Lua CROSSSLOT never triggers) while different
// executions still spread across slots. The namespace prefix is a pure
// namespace isolation layer.
// ---------------------------------------------------------------------------

// execScanPattern returns the SCAN glob for an execution-scoped key suffix
// scoped to one namespace. The {*} glob covers the execution id hash tag and
// introduces no hash tag of its own.
func execScanPattern(t namespace.Namespace, suffix string) string {
	return fmt.Sprintf("xflow:ns:%s:exec:{*}:%s", t, suffix)
}

func execKey(t namespace.Namespace, id types.ExecutionID, suffix string) string {
	return fmt.Sprintf("xflow:ns:%s:exec:{%s}:%s", t, id, suffix)
}

// transientMarkKey holds the per-execution transient marker. It shares the
// {id} hash tag with the rest of the execution's keys so a replica can read it
// in the same slot, and it is written inside CreateExecution's transaction so
// no replica can observe the execution before its transient decision.
func transientMarkKey(t namespace.Namespace, id types.ExecutionID) string {
	return execKey(t, id, "transient")
}

func remainingNodesKey(t namespace.Namespace, id types.ExecutionID) string {
	return execKey(t, id, "remaining_nodes")
}
func failedNodesKey(t namespace.Namespace, id types.ExecutionID) string {
	return execKey(t, id, "failed_nodes")
}
func advanceMarkerKey(t namespace.Namespace, id types.ExecutionID, name string, activationID int) string {
	return execKey(t, id, fmt.Sprintf("node:%s:advance:%d", name, activationID))
}
func scheduleKey(t namespace.Namespace, id types.ExecutionID, nodeIdx int) string {
	return execKey(t, id, fmt.Sprintf("schedule:%d", nodeIdx))
}
func outboxReadyKey(t namespace.Namespace, id types.ExecutionID) string {
	return execKey(t, id, "outbox:ready")
}
func outboxBodyKey(t namespace.Namespace, id types.ExecutionID) string {
	return execKey(t, id, "outbox:body")
}
func outboxAttemptsKey(t namespace.Namespace, id types.ExecutionID) string {
	return execKey(t, id, "outbox:attempts")
}
func outboxDeadKey(t namespace.Namespace, id types.ExecutionID) string {
	return execKey(t, id, "outbox:dead")
}
func outboxDeadBodyKey(t namespace.Namespace, id types.ExecutionID) string {
	return execKey(t, id, "outbox:dead:body")
}

// outboxDeadMetaKey holds compact immutable per-entry metadata (node + activation)
// as a per-entry hash, written at dead-letter time so the activation-safe replay
// guard can read node/activation without parsing the JSON body. Per-entry hashing
// avoids any delimiter ambiguity under Redis Lua 5.1.
func outboxDeadMetaKey(t namespace.Namespace, id types.ExecutionID, entryID string) string {
	return execKey(t, id, "outbox:dead:meta:"+entryID)
}

// outboxReplayEntryIdxKey maps a dead-letter entry ID to the RequestID of the
// replay that moved it, so a concurrent or retried replay with a different
// RequestID returns already_replayed (with the original receipt) instead of
// degrading to not_found once the dead body is gone.
func outboxReplayEntryIdxKey(t namespace.Namespace, id types.ExecutionID) string {
	return execKey(t, id, "replay:entryidx")
}

// outboxReplayReceiptKey holds the authoritative immutable receipt for one
// replay RequestID. It is written atomically with the dead→ready move and
// survives the loss of the dead body, so a retry with the same RequestID
// recovers the original outcome and AuditID.
func outboxReplayReceiptKey(t namespace.Namespace, id types.ExecutionID, requestID string) string {
	return execKey(t, id, "replay:receipt:"+requestID)
}

func nodeStatusKey(t namespace.Namespace, id types.ExecutionID, name string) string {
	return fmt.Sprintf("xflow:ns:%s:exec:{%s}:node:%s:status", t, id, name)
}

func nodeMetaKey(t namespace.Namespace, id types.ExecutionID, name string) string {
	return fmt.Sprintf("xflow:ns:%s:exec:{%s}:node:%s:meta", t, id, name)
}

func outputKey(t namespace.Namespace, id types.ExecutionID, name string) string {
	return fmt.Sprintf("xflow:ns:%s:exec:{%s}:output:%s", t, id, name)
}

func signalKey(t namespace.Namespace, id types.ExecutionID, signalName string) string {
	return fmt.Sprintf("xflow:ns:%s:exec:{%s}:signal:%s", t, id, signalName)
}

func waiterKey(t namespace.Namespace, id types.ExecutionID, signalName string) string {
	return fmt.Sprintf("xflow:ns:%s:exec:{%s}:waiter:%s", t, id, signalName)
}

func waiterSpecKey(t namespace.Namespace, id types.ExecutionID, nodeName string) string {
	return fmt.Sprintf("xflow:ns:%s:exec:{%s}:node:%s:waiter_spec", t, id, nodeName)
}

func signalBatchKey(t namespace.Namespace, id types.ExecutionID, nodeName string) string {
	return fmt.Sprintf("xflow:ns:%s:exec:{%s}:node:%s:signals", t, id, nodeName)
}

func inDegreeKey(t namespace.Namespace, id types.ExecutionID, nodeIdx int) string {
	return fmt.Sprintf("xflow:ns:%s:exec:{%s}:indegree:%d", t, id, nodeIdx)
}

func activeInputsKey(t namespace.Namespace, id types.ExecutionID, nodeIdx int) string {
	return fmt.Sprintf("xflow:ns:%s:exec:{%s}:active_inputs:%d", t, id, nodeIdx)
}

func resumeLockKey(t namespace.Namespace, id types.ExecutionID, name string) string {
	return fmt.Sprintf("xflow:ns:%s:exec:{%s}:node:%s:resume_lock", t, id, name)
}

func suspendedNodesKey(t namespace.Namespace, id types.ExecutionID) string {
	return fmt.Sprintf("xflow:ns:%s:exec:{%s}:suspended_nodes", t, id)
}

func subExecutionKey(t namespace.Namespace, id types.ExecutionID, parentNode string) string {
	return fmt.Sprintf("xflow:ns:%s:exec:{%s}:subs:%s", t, id, parentNode)
}

// leaseExpiryZSetKey is the execution-scoped lease-deadline index used by
// the sweeper. It shares the execution hash tag, so lease state, metadata,
// and expiry discovery can be updated in one Redis Cluster-safe Lua script.
func leaseExpiryZSetKey(t namespace.Namespace, id types.ExecutionID) string {
	return execKey(t, id, "leases")
}

// timeoutZSetKey is the execution-scoped timeout index keyed by the node
// timeout deadline. It shares the execution hash tag ({id}) so that
// suspend/deliver/resume state and the timeout index can be updated in one
// Redis Cluster-safe Lua transition, and so the global timeout workload is
// sharded across slots instead of concentrating on a single hot key.
func timeoutZSetKey(t namespace.Namespace, id types.ExecutionID) string {
	return execKey(t, id, "timeouts")
}

// timeoutMember builds the ZSET member string for a given execution + node.
func timeoutMember(id types.ExecutionID, nodeName string) string {
	return string(id) + "\x00" + nodeName
}

// leaseExpiryMember packs execID and node name into a ZSET member. Retaining
// the execution ID makes index reconciliation robust against malformed keys
// and legacy members during rolling upgrades.
func leaseExpiryMember(id types.ExecutionID, name string) string {
	return string(id) + "|" + name
}

// splitLeaseMember reverses leaseExpiryMember. Returns ok=false when the
// member is malformed (should never happen in prod but guards against dirty
// data).
func splitLeaseMember(member string) (types.ExecutionID, string, bool) {
	idx := strings.IndexByte(member, '|')
	if idx <= 0 {
		return "", "", false
	}
	return types.ExecutionID(member[:idx]), member[idx+1:], true
}

// ---------------------------------------------------------------------------
// Namespace-prefixed key parsing
// ---------------------------------------------------------------------------

const (
	namespaceExecPrefix = "xflow:ns:" // brace-less namespace prefix start
	execInfix           = ":exec:{"   // marker between namespace and the id hash tag
)

// parseNamespaceExecKey reverses the xflow:ns:<namespace>:exec:{<id>}:... shape. It
// returns the namespace, the execution id, and ok=false when the key does not
// match the namespace-scoped execution schema (e.g. a legacy xflow:exec:{...}
// key written before Task 7.1, or an unrelated key).
func parseNamespaceExecKey(key string) (namespace.Namespace, types.ExecutionID, bool) {
	if !strings.HasPrefix(key, namespaceExecPrefix) {
		return "", "", false
	}
	rest := key[len(namespaceExecPrefix):]
	idx := strings.Index(rest, execInfix)
	if idx <= 0 {
		return "", "", false
	}
	t := namespace.Namespace(rest[:idx])
	rest = rest[idx+len(execInfix):]
	end := strings.IndexByte(rest, '}')
	if end <= 0 {
		return "", "", false
	}
	return t, types.ExecutionID(rest[:end]), true
}

// executionIDFromKey returns the execution id embedded in a namespace-scoped
// execution key, discarding the namespace. Callers that need the namespace too
// should use parseNamespaceExecKey directly.
func executionIDFromKey(key string) (types.ExecutionID, bool) {
	_, id, ok := parseNamespaceExecKey(key)
	return id, ok
}

// ---------------------------------------------------------------------------
// Group unit keys — all share the {id} hash tag for Redis Cluster safety.
// ---------------------------------------------------------------------------

// groupUnitStatusKey holds the durable status of a group unit
// (pending/running/done) — the group-level analogue of nodeStatusKey.
func groupUnitStatusKey(t namespace.Namespace, id types.ExecutionID, unitIdx int) string {
	return execKey(t, id, fmt.Sprintf("group:%d:status", unitIdx))
}

// groupUnitMetaKey holds the group unit's lease fields (lease_id, lease_token,
// attempt, committed_lease_token) — the group-level analogue of nodeMetaKey.
func groupUnitMetaKey(t namespace.Namespace, id types.ExecutionID, unitIdx int) string {
	return execKey(t, id, fmt.Sprintf("group:%d:meta", unitIdx))
}

// groupLeaseMember packs an execution ID and group unit index into a ZSET
// member for the lease expiry index. The "group:" infix avoids collisions with
// node-level leaseExpiryMember values.
func groupLeaseMember(id types.ExecutionID, unitIdx int) string {
	return string(id) + "|group:" + strconv.Itoa(unitIdx)
}

// parseGroupLeaseMember recognizes the group encoding produced by
// groupLeaseMember from the remainder splitLeaseMember returns. Node names
// cannot collide: a graph node named "group:0" would still resolve to a
// different Redis key space, and the expiry scan needs the unit index either
// way to read the group unit's own status/meta keys.
func parseGroupLeaseMember(remainder string) (int, bool) {
	const prefix = "group:"
	if !strings.HasPrefix(remainder, prefix) {
		return 0, false
	}
	idx, err := strconv.Atoi(remainder[len(prefix):])
	if err != nil || idx < 0 {
		return 0, false
	}
	return idx, true
}
