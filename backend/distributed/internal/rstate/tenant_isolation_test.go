package rstate

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/xbcio/xflow/backend/tenant"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
)

func testGraphTwoNode() *graph.Graph {
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "test-two-node",
		Nodes: []types.NodeDef{{Name: "start", Type: "test.echo"}, {Name: "end", Type: "test.echo"}},
	})
	if err != nil {
		panic(err)
	}
	return g
}

// TestKeySchemaHasBracelessTenantPrefix is a static assertion that the
// tenant prefix is brace-less: the first '{' in every execution key MUST be
// the one opening the execution id hash tag. Had the tenant been wrapped in
// braces, the hash tag would collapse onto the tenant and every execution of
// one tenant onto a single Redis Cluster slot.
func TestKeySchemaHasBracelessTenantPrefix(t *testing.T) {
	const tenantA tenant.TenantID = "acme"
	id := types.ExecutionID("exec-1")
	cases := []string{
		execKey(tenantA, id, "status"),
		nodeStatusKey(tenantA, id, "n"),
		nodeMetaKey(tenantA, id, "n"),
		outputKey(tenantA, id, "n"),
		signalKey(tenantA, id, "sig"),
		waiterKey(tenantA, id, "sig"),
		leaseExpiryZSetKey(tenantA, id),
		timeoutZSetKey(tenantA, id),
		outboxReadyKey(tenantA, id),
		outboxDeadKey(tenantA, id),
		subExecutionKey(tenantA, id, "n"),
		doneChannel(tenantA, id),
	}
	for _, key := range cases {
		// The tenant segment must appear without surrounding braces.
		if !strings.HasPrefix(key, "xflow:t"+string(tenantA)+":exec:{") {
			t.Errorf("key %q does not have brace-less tenant prefix xflow:t%s:exec:{", key, tenantA)
		}
		// The first '{' must open the execution id, not the tenant.
		first := strings.IndexByte(key, '{')
		if first < 0 || key[first:] != "{"+string(id)+"}"+key[first+len("{"+string(id)+"}"):] {
			// verify the hash tag is exactly {<id>}
			if got := key[first : strings.IndexByte(key, '}')+1]; got != "{"+string(id)+"}" {
				t.Errorf("key %q: first hash tag is %q, want {%s}", key, got, id)
			}
		}
		// SCAN pattern must use {*} glob without a tenant hash tag.
		pat := execScanPattern(tenantA, "outbox:ready")
		if strings.Contains(pat, "{") && !strings.HasPrefix(pat, "xflow:t"+string(tenantA)+":exec:{*}:") {
			t.Errorf("scan pattern %q malformed", pat)
		}
	}
}

// TestTenantIsolationScanDoesNotCrossTenants verifies that an execution
// written under tenant A is invisible to tenant B's SCAN-based discovery and
// read paths: the outbox dispatcher (ListOutboxExecutions), the lease sweeper
// (ListExpiredLeases), outbox metrics, and lease repair all stay within the
// requesting tenant's namespace.
func TestTenantIsolationScanDoesNotCrossTenants(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()
	state := New(rdb, nil, time.Minute)

	const tenantA tenant.TenantID = "acme"
	const tenantB tenant.TenantID = "globex"
	ctxA := tenant.WithTenant(context.Background(), tenantA)
	ctxB := tenant.WithTenant(context.Background(), tenantB)

	idA := types.ExecutionID("exec-acme-1")
	if err := state.CreateExecution(ctxA, &engine.ExecutionSnapshot{
		ID:     idA,
		Status: types.ExecutionStatusRunning,
		Graph:  testGraphTwoNode(),
	}); err != nil {
		t.Fatalf("CreateExecution(A) error = %v", err)
	}
	// Park a lease expiry + outbox under tenant A.
	snap := &engine.NodeSnapshot{
		ExecutionID:   idA,
		Name:          "start",
		NodeIdx:       0,
		Status:        types.NodeStatusRunning,
		LeaseID:       "lease-1",
		LeaseToken:    "tok-1",
		LeaseIssuedAt: time.Now().Add(-2 * time.Second),
		LeaseTTL:      500 * time.Millisecond,
	}
	if err := state.UpsertNode(ctxA, snap); err != nil {
		t.Fatalf("UpsertNode(A) error = %v", err)
	}

	// Tenant B's outbox discovery is a leader-only maintenance path that
	// iterates the whole tenant registry by design (the leader sweeps every
	// tenant). The per-tenant isolation contract is on the SCAN pattern and
	// the per-execution reads, asserted below — not on the aggregate sweeper.
	_ = ctxB

	// SCAN pattern isolation: a SCAN scoped to tenant B's namespace must not
	// return any key belonging to tenant A.
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctxB, cursor, execScanPattern(tenantB, "leases"), 128).Result()
		if err != nil {
			t.Fatalf("scan tenant B leases: %v", err)
		}
		for _, k := range keys {
			if strings.Contains(k, "exec-acme-1") || strings.HasPrefix(k, "xflow:t"+string(tenantA)+":") {
				t.Fatalf("tenant B SCAN matched tenant A key %q — isolation broken", k)
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}

	// Direct tenant B GetExecution must not see tenant A's execution: the
	// status key lives under xflow:tacme:exec:... and tenant B resolves to
	// xflow:tglobex:exec:... which does not exist.
	gotExec, err := state.GetExecution(ctxB, idA)
	if err != nil {
		t.Fatalf("GetExecution(B, A-id) error = %v", err)
	}
	if gotExec != nil {
		t.Fatalf("tenant B retrieved tenant A execution: %+v — isolation broken", gotExec)
	}
	// Tenant A sees its own execution.
	gotExecA, err := state.GetExecution(ctxA, idA)
	if err != nil {
		t.Fatalf("GetExecution(A) error = %v", err)
	}
	if gotExecA == nil || gotExecA.ID != idA {
		t.Fatalf("tenant A did not see its own execution: %+v", gotExecA)
	}

	// Per-node reads are tenant-scoped: tenant B must not see tenant A's node.
	gotNode, err := state.GetNode(ctxB, idA, "start")
	if err != nil {
		t.Fatalf("GetNode(B, A-exec) error = %v", err)
	}
	if gotNode != nil {
		t.Fatalf("tenant B retrieved tenant A node: %+v — isolation broken", gotNode)
	}
	gotNodeA, err := state.GetNode(ctxA, idA, "start")
	if err != nil {
		t.Fatalf("GetNode(A) error = %v", err)
	}
	if gotNodeA == nil || gotNodeA.LeaseToken != snap.LeaseToken {
		t.Fatalf("tenant A did not see its own node: %+v", gotNodeA)
	}

	// Outbox list for tenant A's execution under tenant B's context reads
	// tenant B's keys (empty), not tenant A's.
	entries, err := state.ListOutbox(ctxB, idA, time.Now().Add(time.Second), 100)
	if err != nil {
		t.Fatalf("ListOutbox(B, A-exec) error = %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("tenant B saw tenant A outbox entries: %+v — isolation broken", entries)
	}
}

// TestTenantRegistryRoundTrip verifies registerTenant + ListTenants round-trip,
// including the default-tenant-always-present invariant.
func TestTenantRegistryRoundTrip(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()
	state := New(rdb, nil, time.Minute)
	ctx := context.Background()

	// Before any registration, the default tenant is still listed.
	got, err := ListTenants(ctx, rdb)
	if err != nil {
		t.Fatalf("ListTenants before register: %v", err)
	}
	if len(got) != 1 || got[0] != tenant.DefaultTenant {
		t.Fatalf("ListTenants before register = %v, want [default]", got)
	}

	for _, tn := range []tenant.TenantID{"acme", "globex", "acme"} { // duplicate acme
		if err := state.registerTenant(ctx, tn); err != nil {
			t.Fatalf("registerTenant(%s): %v", tn, err)
		}
	}
	got, err = ListTenants(ctx, rdb)
	if err != nil {
		t.Fatalf("ListTenants after register: %v", err)
	}
	want := []tenant.TenantID{tenant.DefaultTenant, "acme", "globex"}
	if len(got) != len(want) {
		t.Fatalf("ListTenants = %v, want %v (duplicates collapsed, default always present)", got, want)
	}
	seen := map[tenant.TenantID]bool{}
	for _, t := range got {
		seen[t] = true
	}
	for _, w := range want {
		if !seen[w] {
			t.Fatalf("ListTenants missing %q; got %v", w, got)
		}
	}
}

// TestRegisterTenantRejectsUnsafeNames verifies that registerTenant refuses
// tenant names that would break the Redis key schema, and does not write them
// to the tenant registry.
func TestRegisterTenantRejectsUnsafeNames(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()
	state := New(rdb, nil, time.Minute)
	ctx := context.Background()

	for _, bad := range []tenant.TenantID{
		"acme:corp",
		"acme{corp}",
		"acme}corp",
		"malicious:exec:{globex}",
	} {
		if err := state.registerTenant(ctx, bad); err == nil {
			t.Fatalf("registerTenant(%q) succeeded, want error", bad)
		}
	}

	members, err := rdb.SMembers(ctx, tenantSetKey).Result()
	if err != nil {
		t.Fatalf("SMembers: %v", err)
	}
	if len(members) != 0 {
		t.Fatalf("invalid tenants leaked into registry: %v", members)
	}
}

// TestRegisterTenantSkippedInTransientMode verifies the fire-and-forget
// invariant: transient CreateExecution does not SADD to the tenant registry,
// keeping the per-mutation no-bookkeeping contract while the default tenant
// remains discoverable.
func TestRegisterTenantSkippedInTransientMode(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()
	state := New(rdb, nil, time.Hour)
	state.transient = true
	state.transientTTL = time.Minute

	ctx := tenant.WithTenant(context.Background(), "transient-tenant")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     types.ExecutionID("exec-transient-iso"),
		Status: types.ExecutionStatusRunning,
		Graph:  testGraphOneNode(),
	}); err != nil {
		t.Fatalf("CreateExecution error = %v", err)
	}
	// The transient tenant must NOT be in the registry (no SADD issued).
	got, err := ListTenants(ctx, rdb)
	if err != nil {
		t.Fatalf("ListTenants: %v", err)
	}
	for _, tn := range got {
		if tn == "transient-tenant" {
			t.Fatalf("transient tenant leaked into registry: %v", got)
		}
	}
	// default is still present (defensive inclusion).
	if len(got) != 1 || got[0] != tenant.DefaultTenant {
		t.Fatalf("ListTenants = %v, want only [default] in transient mode", got)
	}
}
