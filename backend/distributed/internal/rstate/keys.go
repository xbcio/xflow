package rstate

import (
	"fmt"
	"strings"
	"time"

	"github.com/xbcio/xflow/types"
)

const DefaultExecTTL = 24 * time.Hour

// ---------------------------------------------------------------------------
// Redis key helpers — all keys use {id} as a hash tag for cluster compatibility
// ---------------------------------------------------------------------------

func execKey(id types.ExecutionID, suffix string) string {
	return fmt.Sprintf("xflow:exec:{%s}:%s", id, suffix)
}

func nodeStatusKey(id types.ExecutionID, name string) string {
	return fmt.Sprintf("xflow:exec:{%s}:node:%s:status", id, name)
}

func nodeMetaKey(id types.ExecutionID, name string) string {
	return fmt.Sprintf("xflow:exec:{%s}:node:%s:meta", id, name)
}

func outputKey(id types.ExecutionID, name string) string {
	return fmt.Sprintf("xflow:exec:{%s}:output:%s", id, name)
}

func signalKey(id types.ExecutionID, signalName string) string {
	return fmt.Sprintf("xflow:exec:{%s}:signal:%s", id, signalName)
}

func waiterKey(id types.ExecutionID, signalName string) string {
	return fmt.Sprintf("xflow:exec:{%s}:waiter:%s", id, signalName)
}

func waiterSpecKey(id types.ExecutionID, nodeName string) string {
	return fmt.Sprintf("xflow:exec:{%s}:node:%s:waiter_spec", id, nodeName)
}

func signalBatchKey(id types.ExecutionID, nodeName string) string {
	return fmt.Sprintf("xflow:exec:{%s}:node:%s:signals", id, nodeName)
}

func inDegreeKey(id types.ExecutionID, nodeIdx int) string {
	return fmt.Sprintf("xflow:exec:{%s}:indegree:%d", id, nodeIdx)
}

func activeInputsKey(id types.ExecutionID, nodeIdx int) string {
	return fmt.Sprintf("xflow:exec:{%s}:active_inputs:%d", id, nodeIdx)
}

func resumeLockKey(id types.ExecutionID, name string) string {
	return fmt.Sprintf("xflow:exec:{%s}:node:%s:resume_lock", id, name)
}

func suspendedNodesKey(id types.ExecutionID) string {
	return fmt.Sprintf("xflow:exec:{%s}:suspended_nodes", id)
}

// leaseExpiryZSetKey is the execution-scoped lease-deadline index used by
// the sweeper. It shares the execution hash tag, so lease state, metadata,
// and expiry discovery can be updated in one Redis Cluster-safe Lua script.
func leaseExpiryZSetKey(id types.ExecutionID) string {
	return execKey(id, "leases")
}

// timeoutZSetKey is the execution-scoped timeout index keyed by the node
// timeout deadline. It shares the execution hash tag ({id}) so that
// suspend/deliver/resume state and the timeout index can be updated in one
// Redis Cluster-safe Lua transition, and so the global timeout workload is
// sharded across slots instead of concentrating on a single hot key.
func timeoutZSetKey(id types.ExecutionID) string {
	return execKey(id, "timeouts")
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
