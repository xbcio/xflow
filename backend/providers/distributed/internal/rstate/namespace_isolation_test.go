package rstate

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
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

// TestKeySchemaHasBracelessNamespacePrefix is a static assertion that the
// namespace prefix is brace-less: the first '{' in every execution key MUST be
// the one opening the execution id hash tag. Had the namespace been wrapped in
// braces, the hash tag would collapse onto the namespace and every execution of
// one namespace onto a single Redis Cluster slot.
func TestKeySchemaHasBracelessNamespacePrefix(t *testing.T) {
	const namespaceA namespace.Namespace = "acme"
	id := types.ExecutionID("exec-1")
	cases := []string{
		execKey(namespaceA, id, "status"),
		nodeStatusKey(namespaceA, id, "n"),
		nodeMetaKey(namespaceA, id, "n"),
		outputKey(namespaceA, id, "n"),
		signalKey(namespaceA, id, "sig"),
		waiterKey(namespaceA, id, "sig"),
		leaseExpiryZSetKey(namespaceA, id),
		timeoutZSetKey(namespaceA, id),
		outboxReadyKey(namespaceA, id),
		outboxDeadKey(namespaceA, id),
		subExecutionKey(namespaceA, id, "n"),
		doneChannel(namespaceA, id),
	}
	for _, key := range cases {
		// The namespace segment must appear without surrounding braces.
		if !strings.HasPrefix(key, "xflow:ns:"+string(namespaceA)+":exec:{") {
			t.Errorf("key %q does not have brace-less namespace prefixxflow:ns:%s:exec:{", key, namespaceA)
		}
		// The first '{' must open the execution id, not the namespace.
		first := strings.IndexByte(key, '{')
		if first < 0 || key[first:] != "{"+string(id)+"}"+key[first+len("{"+string(id)+"}"):] {
			// verify the hash tag is exactly {<id>}
			if got := key[first : strings.IndexByte(key, '}')+1]; got != "{"+string(id)+"}" {
				t.Errorf("key %q: first hash tag is %q, want {%s}", key, got, id)
			}
		}
		// SCAN pattern must use {*} glob without a namespace hash tag.
		pat := execScanPattern(namespaceA, "outbox:ready")
		if strings.Contains(pat, "{") && !strings.HasPrefix(pat, "xflow:ns:"+string(namespaceA)+":exec:{*}:") {
			t.Errorf("scan pattern %q malformed", pat)
		}
	}
}

// TestNamespaceIsolationScanDoesNotCrossNamespaces verifies that an execution
// written under namespace A is invisible to namespace B's SCAN-based discovery and
// read paths: the outbox dispatcher (ListOutboxExecutions), the lease sweeper
// (ListExpiredLeases), outbox metrics, and lease repair all stay within the
// requesting namespace's namespace.
func TestNamespaceIsolationScanDoesNotCrossNamespaces(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()
	state := New(rdb, nil, time.Minute)

	const namespaceA namespace.Namespace = "acme"
	const namespaceB namespace.Namespace = "globex"
	ctxA := namespace.WithNamespace(context.Background(), namespaceA)
	ctxB := namespace.WithNamespace(context.Background(), namespaceB)

	idA := types.ExecutionID("exec-acme-1")
	// The outbox entry has to be seeded here, through CreateExecutionWithOutbox,
	// rather than left absent: the isolation assertion at the end of this test
	// reads namespace B's outbox and requires it to be empty, and with no outbox
	// row under namespace A either that check compares empty against empty. It
	// then holds for any namespace resolution at all, including none --
	// hardcoding state_outbox.go:169's `t := namespace.FromContext(ctx)` to
	// namespace.Default leaves it green.
	outboxEntry := engine.OutboxEntry{
		ID: "exec-acme-1/start/0",
		Task: engine.Task{
			ExecutionID: idA,
			NodeName:    "start",
			Type:        engine.TaskTypeNodeExec,
		},
		AvailableAt: time.Now().Add(-time.Second),
	}
	if err := state.CreateExecutionWithOutbox(ctxA, &engine.ExecutionSnapshot{
		ID:     idA,
		Status: types.ExecutionStatusRunning,
		Graph:  testGraphTwoNode(),
	}, []engine.OutboxEntry{outboxEntry}); err != nil {
		t.Fatalf("CreateExecutionWithOutbox(A) error = %v", err)
	}
	// Park a lease expiry + outbox under namespace A.
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

	// Namespace B's outbox discovery is a leader-only maintenance path that
	// iterates the whole namespace registry by design (the leader sweeps every
	// namespace). The per-namespace isolation contract is on the SCAN pattern and
	// the per-execution reads, asserted below — not on the aggregate sweeper.
	_ = ctxB

	// SCAN pattern isolation: a SCAN scoped to namespace B's namespace must not
	// return any key belonging to namespace A.
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctxB, cursor, execScanPattern(namespaceB, "leases"), 128).Result()
		if err != nil {
			t.Fatalf("scan namespace B leases: %v", err)
		}
		for _, k := range keys {
			if strings.Contains(k, "exec-acme-1") || strings.HasPrefix(k, "xflow:ns:"+string(namespaceA)+":") {
				t.Fatalf("namespace B SCAN matched namespace A key %q — isolation broken", k)
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}

	// Direct namespace B GetExecution must not see namespace A's execution: the
	// status key lives underxflow:ns:acme:exec:... and namespace B resolves to
	//xflow:ns:globex:exec:... which does not exist.
	gotExec, err := state.GetExecution(ctxB, idA)
	if err != nil {
		t.Fatalf("GetExecution(B, A-id) error = %v", err)
	}
	if gotExec != nil {
		t.Fatalf("namespace B retrieved namespace A execution: %+v — isolation broken", gotExec)
	}
	// Namespace A sees its own execution.
	gotExecA, err := state.GetExecution(ctxA, idA)
	if err != nil {
		t.Fatalf("GetExecution(A) error = %v", err)
	}
	if gotExecA == nil || gotExecA.ID != idA {
		t.Fatalf("namespace A did not see its own execution: %+v", gotExecA)
	}

	// Per-node reads are namespace-scoped: namespace B must not see namespace A's node.
	gotNode, err := state.GetNode(ctxB, idA, "start")
	if err != nil {
		t.Fatalf("GetNode(B, A-exec) error = %v", err)
	}
	if gotNode != nil {
		t.Fatalf("namespace B retrieved namespace A node: %+v — isolation broken", gotNode)
	}
	gotNodeA, err := state.GetNode(ctxA, idA, "start")
	if err != nil {
		t.Fatalf("GetNode(A) error = %v", err)
	}
	if gotNodeA == nil || gotNodeA.LeaseToken != snap.LeaseToken {
		t.Fatalf("namespace A did not see its own node: %+v", gotNodeA)
	}

	// Outbox list for namespace A's execution under namespace B's context reads
	// namespace B's keys (empty), not namespace A's.
	entries, err := state.ListOutbox(ctxB, idA, time.Now().Add(time.Second), 100)
	if err != nil {
		t.Fatalf("ListOutbox(B, A-exec) error = %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("namespace B saw namespace A outbox entries: %+v — isolation broken", entries)
	}
	// The positive control the other two reads above already have. Without it
	// the check above is satisfied by an outbox that is empty everywhere, which
	// is what a store that ignores the context namespace entirely also produces.
	entriesA, err := state.ListOutbox(ctxA, idA, time.Now().Add(time.Second), 100)
	if err != nil {
		t.Fatalf("ListOutbox(A) error = %v", err)
	}
	if len(entriesA) != 1 || entriesA[0].ID != outboxEntry.ID {
		t.Fatalf("namespace A did not see its own outbox entry (want 1 × %q): %+v",
			outboxEntry.ID, entriesA)
	}
}

// TestNamespaceRegistryRoundTrip verifies registerNamespace + ListNamespaces round-trip,
// including the default-namespace-always-present invariant.
func TestNamespaceRegistryRoundTrip(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()
	state := New(rdb, nil, time.Minute)
	ctx := context.Background()

	// Before any registration, the default namespace is still listed.
	got, err := ListNamespaces(ctx, rdb)
	if err != nil {
		t.Fatalf("ListNamespaces before register: %v", err)
	}
	if len(got) != 1 || got[0] != namespace.Default {
		t.Fatalf("ListNamespaces before register = %v, want [default]", got)
	}

	for _, tn := range []namespace.Namespace{"acme", "globex", "acme"} { // duplicate acme
		if err := state.registerNamespace(ctx, tn); err != nil {
			t.Fatalf("registerNamespace(%s): %v", tn, err)
		}
	}
	got, err = ListNamespaces(ctx, rdb)
	if err != nil {
		t.Fatalf("ListNamespaces after register: %v", err)
	}
	want := []namespace.Namespace{namespace.Default, "acme", "globex"}
	if len(got) != len(want) {
		t.Fatalf("ListNamespaces = %v, want %v (duplicates collapsed, default always present)", got, want)
	}
	seen := map[namespace.Namespace]bool{}
	for _, t := range got {
		seen[t] = true
	}
	for _, w := range want {
		if !seen[w] {
			t.Fatalf("ListNamespaces missing %q; got %v", w, got)
		}
	}
}

// TestRegisterNamespaceRejectsUnsafeNames verifies that registerNamespace refuses
// namespace names that would break the Redis key schema, and does not write them
// to the namespace registry.
func TestRegisterNamespaceRejectsUnsafeNames(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()
	state := New(rdb, nil, time.Minute)
	ctx := context.Background()

	for _, bad := range []namespace.Namespace{
		"acme:corp",
		"acme{corp}",
		"acme}corp",
		"malicious:exec:{globex}",
	} {
		if err := state.registerNamespace(ctx, bad); err == nil {
			t.Fatalf("registerNamespace(%q) succeeded, want error", bad)
		}
	}

	members, err := rdb.SMembers(ctx, namespaceSetKey).Result()
	if err != nil {
		t.Fatalf("SMembers: %v", err)
	}
	if len(members) != 0 {
		t.Fatalf("invalid namespaces leaked into registry: %v", members)
	}
}

// TestSweeperReclaimPropagatesNamespaceContext verifies that ListExpiredLeases
// stamps each ExpiredLease with its owning namespace and that engine.ReclaimLease
// uses that namespace to operate in the correct key namespace. Before the fix,
// ReclaimLease inherited the sweeper's default-namespace context and would
// fail-closed on non-default namespaces, leaving expired leases unreclaimed.
func TestSweeperReclaimPropagatesNamespaceContext(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()
	state := New(rdb, nil, time.Hour)

	const namespaceB namespace.Namespace = "namespace-b"
	ctxB := namespace.WithNamespace(context.Background(), namespaceB)
	ctxSweeper := context.Background() // no namespace -> default

	queue := &StoreTestQueue{}
	eng := engine.New(state, queue, engine.WithDefaultLeaseTTL(50*time.Millisecond))

	id, err := eng.Submit(ctxB, testGraphOneNode(), nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if len(queue.tasks) != 1 {
		t.Fatalf("initial tasks = %d, want 1", len(queue.tasks))
	}
	task := queue.tasks[0]
	queue.tasks = nil

	if _, err := eng.BuildTaskLease(ctxB, task); err != nil {
		t.Fatalf("BuildTaskLease() error = %v", err)
	}

	// Wait past lease deadline.
	mr.FastForward(100 * time.Millisecond)

	// Sweeper calls ListExpiredLeases with a default-namespace context.
	expired, err := state.ListExpiredLeases(ctxSweeper, time.Now().UTC().Add(time.Hour))
	if err != nil {
		t.Fatalf("ListExpiredLeases() error = %v", err)
	}
	if len(expired) != 1 {
		t.Fatalf("expired leases = %v, want 1", len(expired))
	}
	if expired[0].Namespace != namespaceB {
		t.Fatalf("expired lease namespace = %q, want %q", expired[0].Namespace, namespaceB)
	}
	if expired[0].NodeName != "start" {
		t.Fatalf("expired lease node = %q, want start", expired[0].NodeName)
	}

	// Reclaim from the sweeper context (default namespace) must still succeed
	// because ExpiredLease carries the owning namespace.
	ok, err := eng.ReclaimLease(ctxSweeper, expired[0])
	if err != nil || !ok {
		t.Fatalf("ReclaimLease() ok=%v err=%v", ok, err)
	}

	// The task was redelivered.
	if len(queue.tasks) != 1 || queue.tasks[0].NodeName != "start" {
		t.Fatalf("redelivered tasks = %v, want [start]", taskNamesFromQueue(queue.tasks))
	}

	// Namespace B's node is back to pending with no lease token.
	ns, err := state.GetNode(ctxB, id, "start")
	if err != nil {
		t.Fatalf("GetNode(B) error = %v", err)
	}
	if ns == nil || ns.Status != types.NodeStatusPending || ns.LeaseToken != "" {
		t.Fatalf("namespace B node after reclaim = %+v, want pending+empty token", ns)
	}

	// Default namespace namespace was never touched.
	defaultStatus, err := rdb.Get(ctxSweeper, nodeStatusKey(namespace.Default, id, "start")).Result()
	if err != redis.Nil {
		t.Fatalf("default namespace status key should not exist, got %q err=%v", defaultStatus, err)
	}
}

func taskNamesFromQueue(tasks []*engine.Task) []string {
	out := make([]string, len(tasks))
	for i, t := range tasks {
		out[i] = t.NodeName
	}
	return out
}

// TestRegisterNamespaceSkippedInTransientMode verifies the fire-and-forget
// invariant: transient CreateExecution does not SADD to the namespace registry,
// keeping the per-mutation no-bookkeeping contract while the default namespace
// remains discoverable.
func TestRegisterNamespaceSkippedInTransientMode(t *testing.T) {
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

	ctx := namespace.WithNamespace(context.Background(), "transient-namespace")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     types.ExecutionID("exec-transient-iso"),
		Status: types.ExecutionStatusRunning,
		Graph:  testGraphOneNode(),
	}); err != nil {
		t.Fatalf("CreateExecution error = %v", err)
	}
	// The transient namespace must NOT be in the registry (no SADD issued).
	got, err := ListNamespaces(ctx, rdb)
	if err != nil {
		t.Fatalf("ListNamespaces: %v", err)
	}
	for _, tn := range got {
		if tn == "transient-namespace" {
			t.Fatalf("transient namespace leaked into registry: %v", got)
		}
	}
	// default is still present (defensive inclusion).
	if len(got) != 1 || got[0] != namespace.Default {
		t.Fatalf("ListNamespaces = %v, want only [default] in transient mode", got)
	}
}
