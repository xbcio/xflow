package rstate

import (
	"fmt"
	"strings"
	"time"

	"github.com/xbcio/xflow/backend/tenant"
	"github.com/xbcio/xflow/types"
)

const DefaultExecTTL = 24 * time.Hour

// ---------------------------------------------------------------------------
// Redis key helpers — all keys use {id} as a hash tag for cluster compatibility.
//
// Tenant boundary (Task 7.1): every execution-scoped key carries a
// brace-less tenant prefix, producing the shape
//
//	xflow:t<tenant>:exec:{<id>}:<suffix>
//
// The tenant prefix is intentionally NOT wrapped in braces. Redis Cluster's
// hash-tag rule takes the substring between the first '{' and the first '}'.
// Had the tenant been written as '{tenant}', the first {...} would be the
// tenant segment and every key for one tenant would collapse onto a single
// slot — both a hot-tenant hotspot and a break of the per-execution slot
// distribution. With the brace-less 't<tenant>' prefix the first '{' still
// opens the execution ID, so the hash tag remains {<id>}: keys for one
// execution stay co-located (Lua CROSSSLOT never triggers) while different
// executions still spread across slots. The tenant prefix is a pure
// namespace isolation layer.
// ---------------------------------------------------------------------------

// execScanPattern returns the SCAN glob for an execution-scoped key suffix
// scoped to one tenant. The {*} glob covers the execution id hash tag and
// introduces no hash tag of its own.
func execScanPattern(t tenant.TenantID, suffix string) string {
	return fmt.Sprintf("xflow:t%s:exec:{*}:%s", t, suffix)
}

func execKey(t tenant.TenantID, id types.ExecutionID, suffix string) string {
	return fmt.Sprintf("xflow:t%s:exec:{%s}:%s", t, id, suffix)
}

func nodeStatusKey(t tenant.TenantID, id types.ExecutionID, name string) string {
	return fmt.Sprintf("xflow:t%s:exec:{%s}:node:%s:status", t, id, name)
}

func nodeMetaKey(t tenant.TenantID, id types.ExecutionID, name string) string {
	return fmt.Sprintf("xflow:t%s:exec:{%s}:node:%s:meta", t, id, name)
}

func outputKey(t tenant.TenantID, id types.ExecutionID, name string) string {
	return fmt.Sprintf("xflow:t%s:exec:{%s}:output:%s", t, id, name)
}

func signalKey(t tenant.TenantID, id types.ExecutionID, signalName string) string {
	return fmt.Sprintf("xflow:t%s:exec:{%s}:signal:%s", t, id, signalName)
}

func waiterKey(t tenant.TenantID, id types.ExecutionID, signalName string) string {
	return fmt.Sprintf("xflow:t%s:exec:{%s}:waiter:%s", t, id, signalName)
}

func waiterSpecKey(t tenant.TenantID, id types.ExecutionID, nodeName string) string {
	return fmt.Sprintf("xflow:t%s:exec:{%s}:node:%s:waiter_spec", t, id, nodeName)
}

func signalBatchKey(t tenant.TenantID, id types.ExecutionID, nodeName string) string {
	return fmt.Sprintf("xflow:t%s:exec:{%s}:node:%s:signals", t, id, nodeName)
}

func inDegreeKey(t tenant.TenantID, id types.ExecutionID, nodeIdx int) string {
	return fmt.Sprintf("xflow:t%s:exec:{%s}:indegree:%d", t, id, nodeIdx)
}

func activeInputsKey(t tenant.TenantID, id types.ExecutionID, nodeIdx int) string {
	return fmt.Sprintf("xflow:t%s:exec:{%s}:active_inputs:%d", t, id, nodeIdx)
}

func resumeLockKey(t tenant.TenantID, id types.ExecutionID, name string) string {
	return fmt.Sprintf("xflow:t%s:exec:{%s}:node:%s:resume_lock", t, id, name)
}

func suspendedNodesKey(t tenant.TenantID, id types.ExecutionID) string {
	return fmt.Sprintf("xflow:t%s:exec:{%s}:suspended_nodes", t, id)
}

func subExecutionKey(t tenant.TenantID, id types.ExecutionID, parentNode string) string {
	return fmt.Sprintf("xflow:t%s:exec:{%s}:subs:%s", t, id, parentNode)
}

// leaseExpiryZSetKey is the execution-scoped lease-deadline index used by
// the sweeper. It shares the execution hash tag, so lease state, metadata,
// and expiry discovery can be updated in one Redis Cluster-safe Lua script.
func leaseExpiryZSetKey(t tenant.TenantID, id types.ExecutionID) string {
	return execKey(t, id, "leases")
}

// timeoutZSetKey is the execution-scoped timeout index keyed by the node
// timeout deadline. It shares the execution hash tag ({id}) so that
// suspend/deliver/resume state and the timeout index can be updated in one
// Redis Cluster-safe Lua transition, and so the global timeout workload is
// sharded across slots instead of concentrating on a single hot key.
func timeoutZSetKey(t tenant.TenantID, id types.ExecutionID) string {
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
// Tenant-prefixed key parsing
// ---------------------------------------------------------------------------

const (
	tenantExecPrefix = "xflow:t" // brace-less tenant prefix start
	execInfix         = ":exec:{" // marker between tenant and the id hash tag
)

// parseTenantExecKey reverses the xflow:t<tenant>:exec:{<id>}:... shape. It
// returns the tenant, the execution id, and ok=false when the key does not
// match the tenant-scoped execution schema (e.g. a legacy xflow:exec:{...}
// key written before Task 7.1, or an unrelated key).
func parseTenantExecKey(key string) (tenant.TenantID, types.ExecutionID, bool) {
	if !strings.HasPrefix(key, tenantExecPrefix) {
		return "", "", false
	}
	rest := key[len(tenantExecPrefix):]
	idx := strings.Index(rest, execInfix)
	if idx <= 0 {
		return "", "", false
	}
	t := tenant.TenantID(rest[:idx])
	rest = rest[idx+len(execInfix):]
	end := strings.IndexByte(rest, '}')
	if end <= 0 {
		return "", "", false
	}
	return t, types.ExecutionID(rest[:end]), true
}

// executionIDFromKey returns the execution id embedded in a tenant-scoped
// execution key, discarding the tenant. Callers that need the tenant too
// should use parseTenantExecKey directly.
func executionIDFromKey(key string) (types.ExecutionID, bool) {
	_, id, ok := parseTenantExecKey(key)
	return id, ok
}
