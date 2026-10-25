package rstate

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/xbcio/xflow/engine/graph"
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

// transientMarkKey holds the per-execution mode marker: field "transient" for
// an execution that opted out of SQL projection, field "durable" for one whose
// row is expected there. Creation writes it -- for every execution -- before or
// in the transaction that makes the execution visible, so no replica can
// observe the execution without its decision.
//
// The durable field is what keeps a lagging SQL projection from being read as
// evidence of transience. Its absence remains meaningful in the opposite
// direction: an execution that outlived its transient marker also has no marker,
// which is why markerless lookups still fail closed.
//
// It shares the {id} hash tag with the rest of the execution's keys so a
// replica can read it in the same slot.
func transientMarkKey(t namespace.Namespace, id types.ExecutionID) string {
	return execKey(t, id, "transient")
}

func remainingNodesKey(t namespace.Namespace, id types.ExecutionID) string {
	return execKey(t, id, "remaining_nodes")
}
func failedNodesKey(t namespace.Namespace, id types.ExecutionID) string {
	return execKey(t, id, "failed_nodes")
}

// terminalMarkKey holds the terminal marker: the execution status recorded when
// a transition terminalized the execution, with the execution's ACTIVE
// retention and deliberately excluded from completion-time TTL shortening.
//
// It exists because the status key's lifetime is not the classification window.
// A transient execution's status key is shortened to the completion TTL the
// moment it goes terminal, and a durable execution's status key simply expires;
// when a queued task for that execution is consumed long afterwards (the queue
// backlog this system can accumulate), "no status" would otherwise read as
// "work never ran" — a false loss verdict on a benign late duplicate, on every
// execution that ever finished. The marker is what keeps the terminal verdict
// readable for the whole active retention.
//
// It shares the {id} hash tag with the rest of the execution's keys, so a
// classifier can read it in the same slot as the status key.
func terminalMarkKey(t namespace.Namespace, id types.ExecutionID) string {
	return execKey(t, id, "terminal")
}

// retentionRecordKey holds the retention an execution was actually created
// with, written for every execution inside the create transaction (and by the
// entry-seed admission path). It exists because the process-local
// per-execution override cache is deleted at terminalization
// (evictExecutionCaches) and never survives a restart or a replica change:
// without the record, resolution would fall back to getExecTTL and could
// report the longer global TTL for an execution whose writes used a shorter
// override, over-stating the evidence window and turning a benign late
// duplicate into a reported loss.
//
// Absence is deliberately meaningful: it is the "cannot confirm" answer, not
// a licence to use the fallback. GetExecutionRetention answers
// engine.ExecutionRetentionUnknown when this key is gone, and the classifier
// lands such drops in unattributed. The record is sized by
// retentionRecordTTL to outlive the evidence it describes by one further
// retention; an execution that outlives even that (or one created before the
// record existed) degrades to "unknown", which under-reports rather than
// over-reports the loss claim.
func retentionRecordKey(t namespace.Namespace, id types.ExecutionID) string {
	return execKey(t, id, "retention")
}
func advanceMarkerKey(t namespace.Namespace, id types.ExecutionID, name string, activationID int) string {
	return execKey(t, id, fmt.Sprintf("node:%s:advance:%d", name, activationID))
}
func scheduleKey(t namespace.Namespace, id types.ExecutionID, unitIdx int) string {
	return execKey(t, id, fmt.Sprintf("schedule:%d", unitIdx))
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

func inDegreeKey(t namespace.Namespace, id types.ExecutionID, unitIdx int) string {
	return fmt.Sprintf("xflow:ns:%s:exec:{%s}:indegree:%d", t, id, unitIdx)
}

func activeInputsKey(t namespace.Namespace, id types.ExecutionID, unitIdx int) string {
	return fmt.Sprintf("xflow:ns:%s:exec:{%s}:active_inputs:%d", t, id, unitIdx)
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

// ---------------------------------------------------------------------------
// Workflow-scoped keys — these carry NO {id} tag, because they are shared by
// every execution of one workflow across the cluster.
//
// They are deliberately in a different slot from the execution keys they
// reason about. Redis Cluster hashes a key by its first {...} pair, so an
// execution's keys land in slot {<id>} while a workflow's land in
// slot {<workflow digest>}; no Lua script may touch both. That is why slot
// admission and terminal release are separate transitions from the execution
// writes they bracket, and why a reconciler (not a rollback) is the recovery
// path for the window between them.
// ---------------------------------------------------------------------------

// workflowDigestTag builds the hash tag for a workflow's shared keys. It is
// length-prefixed so ("a", "bc") and ("ab", "c") cannot collide, and hashed so
// caller-controlled braces cannot change which slot is selected. The hash
// treats the (namespace, name) pair exactly as graph.ConcurrencyKey does, so
// the tag one side persists and the other recomputes agree.
//
// The tag deliberately carries no version: slots are keyed by workflow NAME,
// so a version bump cannot orphan the slots the previous version's executions
// still hold.
func workflowDigestTag(ns namespace.Namespace, workflow string) string {
	return graph.ConcurrencyKey(string(ns), workflow)
}

// concurrencySlotKey is the workflow's in-flight slot set: a ZSET whose members
// are execution IDs and whose scores are admission times. A ZSET rather than a
// counter because a counter cannot be reconciled — the members are what let the
// reconciler tell a leaked slot from a live one.
func concurrencySlotKey(ns namespace.Namespace, workflow string) string {
	return concurrencySlotKeyForTag(ns, workflowDigestTag(ns, workflow))
}

// concurrencySlotKeyForTag builds the same key from an already-computed tag,
// which is all a release has after the execution's back-reference told it which
// workflow to leave.
func concurrencySlotKeyForTag(ns namespace.Namespace, workflowTag string) string {
	return fmt.Sprintf("xflow:ns:%s:conc:{%s}:slots", entryActivationNamespacePath(ns), workflowTag)
}

// concurrencyQueueKeyForTag is the workflow's FIFO line: a ZSET whose members
// are execution IDs and whose scores are a monotonic sequence, so promotion
// order is admission order and not the order Redis happens to return members
// in. Everything the line needs to be promoted shares this key's hash slot —
// the sequence it draws from, the capacity it fills to, and the deadlines that
// expire it — which is what lets one script do all of it at once.
func concurrencyQueueKeyForTag(ns namespace.Namespace, workflowTag string) string {
	return fmt.Sprintf("xflow:ns:%s:conc:{%s}:queue", entryActivationNamespacePath(ns), workflowTag)
}

// concurrencyQueueSeqKeyForTag is the line's monotonic sequence counter. Scores
// drawn from it never repeat, which is what makes FIFO order survive members
// added and removed concurrently.
func concurrencyQueueSeqKeyForTag(ns namespace.Namespace, workflowTag string) string {
	return fmt.Sprintf("xflow:ns:%s:conc:{%s}:seq", entryActivationNamespacePath(ns), workflowTag)
}

// concurrencyCapacityKeyForTag is the capacity the workflow was last submitted
// with, as a plain string. Promotion happens at release time, when the graph
// that declared the limit is long gone, so the number has to be remembered
// somewhere the release can reach — and it is written on every admission, so a
// re-published definition's new limit takes effect on the next submission, as
// the design requires.
func concurrencyCapacityKeyForTag(ns namespace.Namespace, workflowTag string) string {
	return fmt.Sprintf("xflow:ns:%s:conc:{%s}:limit", entryActivationNamespacePath(ns), workflowTag)
}

// concurrencyDeadlineKeyForTag indexes the queued executions that declared a
// queue_timeout, scored by the instant they expire. A separate ZSET from the
// line because a member's position is its wait order and its deadline is not:
// two submissions with different timeouts keep their arrival order in the line
// while their deadlines sort independently here.
func concurrencyDeadlineKeyForTag(ns namespace.Namespace, workflowTag string) string {
	return fmt.Sprintf("xflow:ns:%s:conc:{%s}:dl", entryActivationNamespacePath(ns), workflowTag)
}

// concurrencyWorkflowIndexKey enumerates the workflow tags that have a slot
// ZSET in this namespace, so the reconciler can find the sets to sweep. It
// deliberately carries no hash tag: it aggregates every workflow in the
// namespace, so it cannot belong to any single workflow's slot.
func concurrencyWorkflowIndexKey(ns namespace.Namespace) string {
	return fmt.Sprintf("xflow:ns:%s:conc:wfs", entryActivationNamespacePath(ns))
}

// leaseExpiryZSetKey is the execution-scoped lease-deadline index used by
// the sweeper. It shares the execution hash tag, so lease state, metadata,
// and expiry discovery can be updated in one Redis Cluster-safe Lua script.
func leaseExpiryZSetKey(t namespace.Namespace, id types.ExecutionID) string {
	return execKey(t, id, "leases")
}

// cancelingIndexKey is the namespace-scoped index of executions parked in the
// canceling status: a ZSET whose member is the execution ID and whose score is
// the millisecond instant the backend first recorded the canceling transition.
//
// It deliberately carries no execution hash tag and is never touched inside
// Lua: Redis Cluster refuses a script that spans slots, so the registration is
// an ordinary ZADD performed BEFORE the per-execution status write rather than
// atomically with it. That ordering is what keeps the index complete — a status
// write is only attempted after its registration succeeded, so no execution can
// reach canceling without an entry (engine.CancelingExecutionLister documents
// the invariant; see backend/providers/distributed/internal/rstate/canceling_index.go
// for the full argument).
func cancelingIndexKey(t namespace.Namespace) string {
	return fmt.Sprintf("xflow:ns:%s:cancel:index", t)
}

// executionDeadlineIndexKey is the namespace-scoped ZSET of executions that carry
// a settings.timeout deadline: member = execution ID, score = the deadline in
// unix milliseconds (the same value deadline_ms stores).
//
// Like cancelingIndexKey it carries no execution hash tag, so it can never be a
// KEYS entry of a per-execution Lua script: it is registered by an ordinary ZADD
// before the execution is written and withdrawn by an ordinary ZREM after a
// terminal write (execution_deadline_index.go has the full argument). The name
// does not match the `exec:{*}:` SCAN pattern.
func executionDeadlineIndexKey(t namespace.Namespace) string {
	return fmt.Sprintf("xflow:ns:%s:exec_deadlines", t)
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
