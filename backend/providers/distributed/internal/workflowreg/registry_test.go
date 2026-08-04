package workflowreg

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

func newTestRegistry(t *testing.T) (*Registry, *miniredis.Miniredis) {
	t.Helper()

	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	t.Cleanup(srv.Close)

	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	return New(rdb), srv
}

func testRecord(t *testing.T, nameSuffix, hash string) backend.WorkflowRecord {
	t.Helper()

	def := &types.WorkflowDef{
		Namespace: "test",
		Name:      "workflow-" + nameSuffix,
		Version:   "v1",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "review", Type: "xflow.function"},
		},
		Connections: types.Connections{
			"start": {"main": types.PortConnections{Targets: []types.Connection{{Node: "review", Input: "main"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("graph.Compile: %v", err)
	}
	return backend.WorkflowRecord{
		Key:            def.Namespace + "/" + def.Name + "@" + def.Version,
		Namespace:      def.Namespace,
		Name:           def.Name,
		Version:        def.Version,
		DefinitionHash: hash,
		Definition:     def,
		Graph:          g,
	}
}

// slotTag extracts the Redis Cluster hash tag (the substring between the first
// `{` and the first subsequent `}`). Empty string means "no tag" — the key's
// full content determines its slot, which is cluster-safe only for single-key
// operations.
func slotTag(key string) string {
	i := strings.IndexByte(key, '{')
	if i < 0 {
		return ""
	}
	j := strings.IndexByte(key[i+1:], '}')
	if j < 0 {
		return ""
	}
	return key[i+1 : i+1+j]
}

// TestRegistryKeysAreClusterSafe statically guarantees every key the Lua scripts
// touch — declared KEYS and the byid key the add script reconstructs — share the
// same `{<key>}` hash tag, i.e. land in one slot. The idmap reverse index is
// intentionally untagged (single-key op, cluster-safe on its own).
func TestRegistryKeysAreClusterSafe(t *testing.T) {
	const key = "ns/name@v1"
	id := types.WorkflowID(uuid.NewString())
	tn := namespace.Namespace("namespace-a")

	bykey := workflowByKeyKey(tn, key)
	byid := workflowByIDKey(tn, key, id)
	byidPrefix := workflowByIDKeyPrefix(tn, key)
	reconstructedByid := byidPrefix + string(id) // mirrors addWorkflowRecordLua's ARGV[1]..existingID

	if byid != reconstructedByid {
		t.Fatalf("byid key %q != script-constructed key %q", byid, reconstructedByid)
	}
	tag := slotTag(bykey)
	if tag != key {
		t.Fatalf("bykey tag = %q, want %q (raw key %q)", tag, key, bykey)
	}
	if slotTag(byid) != tag {
		t.Fatalf("byid tag %q != bykey tag %q", slotTag(byid), tag)
	}
	if slotTag(reconstructedByid) != tag {
		t.Fatalf("script-constructed byid tag %q != bykey tag %q", slotTag(reconstructedByid), tag)
	}
	if tag := slotTag(workflowIDMapKey(tn, id)); tag != "" {
		t.Fatalf("idmap key must be untagged for single-key safety, got tag %q (%q)", tag, workflowIDMapKey(tn, id))
	}
	// Namespace prefix must be brace-free so the hash tag stays on {<key>}.
	if strings.Contains(bykey, "{"+string(tn)+"}") {
		t.Fatalf("bykey must not wrap namespace in braces: %q", bykey)
	}
}

func TestRegistryAddGetRemoveRoundtrip(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("namespace-a"))
	reg, srv := newTestRegistry(t)

	created, err := reg.AddWorkflow(ctx, testRecord(t, uuid.NewString(), "sha256:abc"))
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	// bykey, byid, and idmap must all be present.
	if !srv.Exists(workflowByKeyKey(namespace.FromContext(ctx), created.Key)) {
		t.Fatalf("bykey not set for %q", created.Key)
	}
	if !srv.Exists(workflowByIDKey(namespace.FromContext(ctx), created.Key, created.ID)) {
		t.Fatalf("byid not set for %q", created.Key)
	}
	if !srv.Exists(workflowIDMapKey(namespace.FromContext(ctx), created.ID)) {
		t.Fatalf("idmap not set for %q", created.ID)
	}

	got, err := reg.GetWorkflow(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if got.ID != created.ID || got.Key != created.Key {
		t.Fatalf("GetWorkflow = id=%q key=%q, want id=%q key=%q", got.ID, got.Key, created.ID, created.Key)
	}

	if err := reg.RemoveWorkflow(ctx, created.ID); err != nil {
		t.Fatalf("RemoveWorkflow: %v", err)
	}
	if srv.Exists(workflowByKeyKey(namespace.FromContext(ctx), created.Key)) || srv.Exists(workflowByIDKey(namespace.FromContext(ctx), created.Key, created.ID)) || srv.Exists(workflowIDMapKey(namespace.FromContext(ctx), created.ID)) {
		t.Fatalf("keys still present after RemoveWorkflow for %q", created.Key)
	}

	if _, err := reg.GetWorkflow(ctx, created.ID); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("GetWorkflow after remove = %v, want ErrWorkflowNotFound", err)
	}
}

func TestRegistryAddIsIdempotentByKeyAndHash(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("namespace-a"))
	reg, _ := newTestRegistry(t)

	created, err := reg.AddWorkflow(ctx, testRecord(t, uuid.NewString(), "sha256:shared"))
	if err != nil {
		t.Fatalf("AddWorkflow first: %v", err)
	}

	same := testRecord(t, created.Name, "sha256:shared")
	same.Key = created.Key
	got, err := reg.AddWorkflow(ctx, same)
	if err != nil {
		t.Fatalf("AddWorkflow second: %v", err)
	}
	if got.ID != created.ID {
		t.Fatalf("idempotent AddWorkflow ID = %q, want %q", got.ID, created.ID)
	}
}

func TestRegistryAddConflict(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("namespace-a"))
	reg, _ := newTestRegistry(t)

	created, err := reg.AddWorkflow(ctx, testRecord(t, uuid.NewString(), "sha256:a"))
	if err != nil {
		t.Fatalf("AddWorkflow first: %v", err)
	}

	conflict := testRecord(t, created.Name, "sha256:b")
	conflict.Key = created.Key
	if _, err := reg.AddWorkflow(ctx, conflict); !errors.Is(err, backend.ErrWorkflowConflict) {
		t.Fatalf("AddWorkflow conflict = %v, want ErrWorkflowConflict", err)
	}
}

func TestRegistryRemoveMissing(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("namespace-a"))
	reg, _ := newTestRegistry(t)

	if err := reg.RemoveWorkflow(ctx, types.WorkflowID(uuid.NewString())); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("RemoveWorkflow missing = %v, want ErrWorkflowNotFound", err)
	}
}

func TestRegistryRemoveCorrupt(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("namespace-a"))
	reg, srv := newTestRegistry(t)

	rec := testRecord(t, uuid.NewString(), "sha256:abc")
	created, err := reg.AddWorkflow(ctx, rec)
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	// Simulate a corrupt index: the idmap points to the key, but the tagged
	// records have been lost. RemoveWorkflow should report not-found and clean
	// up the stale idmap.
	srv.Del(workflowByKeyKey(namespace.FromContext(ctx), created.Key))
	srv.Del(workflowByIDKey(namespace.FromContext(ctx), created.Key, created.ID))
	srv.Del(workflowDefHashKey(namespace.FromContext(ctx), created.Key, created.ID))

	if err := reg.RemoveWorkflow(ctx, created.ID); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("RemoveWorkflow corrupt = %v, want ErrWorkflowNotFound", err)
	}
	if srv.Exists(workflowIDMapKey(namespace.FromContext(ctx), created.ID)) {
		t.Fatalf("stale idmap still present after RemoveWorkflow for corrupt workflow %q", created.Key)
	}
}

// TestRegistryKeyByIDSharesTag locks in the exported schema: KeyByID must place
// the byid record in the same slot as KeyByKey (both carry `{<key>}`).
func TestRegistryKeyByIDSharesTag(t *testing.T) {
	const key = "ns/name@v1"
	id := types.WorkflowID(uuid.NewString())
	tn := namespace.Namespace("namespace-a")

	a := KeyByKey(tn, key)
	b := KeyByID(tn, key, id)
	if slotTag(a) == "" || slotTag(a) != slotTag(b) {
		t.Fatalf("KeyByKey %q and KeyByID %q must share a non-empty hash tag", a, b)
	}
}

// TestRegistryNamespaceIsolation asserts that two namespaces can register the same
// workflow key independently and cannot read each other's records.
func TestRegistryNamespaceIsolation(t *testing.T) {
	ctxA := namespace.WithNamespace(context.Background(), namespace.Namespace("namespace-a"))
	ctxB := namespace.WithNamespace(context.Background(), namespace.Namespace("namespace-b"))
	reg, _ := newTestRegistry(t)

	rec := testRecord(t, uuid.NewString(), "sha256:isolated")
	createdA, err := reg.AddWorkflow(ctxA, rec)
	if err != nil {
		t.Fatalf("AddWorkflow(namespace-a): %v", err)
	}

	// Namespace B can register the same key without conflict.
	createdB, err := reg.AddWorkflow(ctxB, rec)
	if err != nil {
		t.Fatalf("AddWorkflow(namespace-b): %v", err)
	}
	if createdB.ID == createdA.ID {
		t.Fatalf("namespace-b workflow ID = namespace-a ID %q; want distinct records", createdA.ID)
	}

	// Namespace B cannot retrieve namespace A's workflow by ID.
	if _, err := reg.GetWorkflow(ctxB, createdA.ID); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("GetWorkflow(namespace-b, namespace-a id) = %v, want ErrWorkflowNotFound", err)
	}

	// Namespace A still sees its own workflow.
	gotA, err := reg.GetWorkflow(ctxA, createdA.ID)
	if err != nil {
		t.Fatalf("GetWorkflow(namespace-a): %v", err)
	}
	if gotA.ID != createdA.ID {
		t.Fatalf("GetWorkflow(namespace-a) ID = %q, want %q", gotA.ID, createdA.ID)
	}

	// Removing from namespace B must not affect namespace A.
	if err := reg.RemoveWorkflow(ctxB, createdA.ID); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("RemoveWorkflow(namespace-b, namespace-a id) = %v, want ErrWorkflowNotFound", err)
	}
	if _, err := reg.GetWorkflow(ctxA, createdA.ID); err != nil {
		t.Fatalf("GetWorkflow(namespace-a) after cross-namespace remove: %v", err)
	}

	// Removing from namespace A succeeds.
	if err := reg.RemoveWorkflow(ctxA, createdA.ID); err != nil {
		t.Fatalf("RemoveWorkflow(namespace-a): %v", err)
	}
	if _, err := reg.GetWorkflow(ctxA, createdA.ID); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("GetWorkflow(namespace-a) after remove = %v, want ErrWorkflowNotFound", err)
	}
}

// TestRegistryDefaultNamespaceFallsBack verifies that a context without an explicit
// namespace uses the default namespace, preserving single-namespace behavior.
func TestRegistryDefaultNamespaceFallsBack(t *testing.T) {
	ctx := context.Background()
	reg, _ := newTestRegistry(t)

	created, err := reg.AddWorkflow(ctx, testRecord(t, uuid.NewString(), "sha256:default"))
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	got, err := reg.GetWorkflow(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if got.ID != created.ID {
		t.Fatalf("GetWorkflow ID = %q, want %q", got.ID, created.ID)
	}
}

// TestRegistryGetWorkflowByKey asserts the by-key lookup returns the stored
// record, used by Engine.AddWorkflow for legacy-hash reconciliation.
func TestRegistryGetWorkflowByKey(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("namespace-a"))
	reg, _ := newTestRegistry(t)

	if _, err := reg.GetWorkflowByKey(ctx, "default/wf@v1"); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("GetWorkflowByKey missing = %v, want ErrWorkflowNotFound", err)
	}

	created, err := reg.AddWorkflow(ctx, testRecord(t, uuid.NewString(), "sha256:abc"))
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	got, err := reg.GetWorkflowByKey(ctx, created.Key)
	if err != nil {
		t.Fatalf("GetWorkflowByKey: %v", err)
	}
	if got.ID != created.ID || got.DefinitionHash != "sha256:abc" {
		t.Fatalf("GetWorkflowByKey = id=%q hash=%q, want id=%q hash=sha256:abc", got.ID, got.DefinitionHash, created.ID)
	}
}

// TestRegistryUpdateDefinitionHash exercises the CAS contract and verifies
// the stored record's hash is upgraded in place.
func TestRegistryUpdateDefinitionHash(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("namespace-a"))
	reg, _ := newTestRegistry(t)

	if err := reg.UpdateDefinitionHash(ctx, "missing", "sha256:old", "runtime-sha256:v1:new"); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("UpdateDefinitionHash missing = %v, want ErrWorkflowNotFound", err)
	}

	created, err := reg.AddWorkflow(ctx, testRecord(t, uuid.NewString(), "sha256:old"))
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	if err := reg.UpdateDefinitionHash(ctx, created.ID, "sha256:wrong", "runtime-sha256:v1:new"); !errors.Is(err, backend.ErrWorkflowConflict) {
		t.Fatalf("UpdateDefinitionHash CAS mismatch = %v, want ErrWorkflowConflict", err)
	}

	if err := reg.UpdateDefinitionHash(ctx, created.ID, "sha256:old", "runtime-sha256:v1:new"); err != nil {
		t.Fatalf("UpdateDefinitionHash: %v", err)
	}

	got, err := reg.GetWorkflow(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if got.DefinitionHash != "runtime-sha256:v1:new" {
		t.Fatalf("DefinitionHash = %q, want runtime-sha256:v1:new", got.DefinitionHash)
	}

	// Idempotent: re-upgrading against the new hash with the same expected hash
	// (matching current) is a no-op success.
	if err := reg.UpdateDefinitionHash(ctx, created.ID, "runtime-sha256:v1:new", "runtime-sha256:v1:new"); err != nil {
		t.Fatalf("UpdateDefinitionHash idempotent: %v", err)
	}
}

// TestRegistryUpdateDefinitionHashConcurrent asserts two concurrent upgrades
// against the same expectedOldHash cannot both succeed. The loser must see
// ErrWorkflowConflict. Exercises the Lua CAS path under -race.
func TestRegistryUpdateDefinitionHashConcurrent(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("namespace-a"))
	reg, _ := newTestRegistry(t)

	created, err := reg.AddWorkflow(ctx, testRecord(t, uuid.NewString(), "sha256:old"))
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- reg.UpdateDefinitionHash(ctx, created.ID, "sha256:old", "runtime-sha256:v1:winner") }()
	go func() { second <- reg.UpdateDefinitionHash(ctx, created.ID, "sha256:old", "runtime-sha256:v1:loser") }()

	err1 := <-first
	err2 := <-second
	if err1 == nil && err2 == nil {
		t.Fatalf("both concurrent upgrades succeeded; expected exactly one ErrWorkflowConflict")
	}
	if err1 != nil && !errors.Is(err1, backend.ErrWorkflowConflict) {
		t.Fatalf("first upgrade err = %v, want ErrWorkflowConflict or nil", err1)
	}
	if err2 != nil && !errors.Is(err2, backend.ErrWorkflowConflict) {
		t.Fatalf("second upgrade err = %v, want ErrWorkflowConflict or nil", err2)
	}

	got, err := reg.GetWorkflow(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if got.DefinitionHash != "runtime-sha256:v1:winner" && got.DefinitionHash != "runtime-sha256:v1:loser" {
		t.Fatalf("DefinitionHash = %q after concurrent upgrade", got.DefinitionHash)
	}
}
