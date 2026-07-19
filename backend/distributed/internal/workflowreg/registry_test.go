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
			"start": {"main": {{Node: "review", Input: "main"}}},
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

	bykey := workflowByKeyKey(key)
	byid := workflowByIDKey(key, id)
	byidPrefix := workflowByIDKeyPrefix(key)
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
	if tag := slotTag(workflowIDMapKey(id)); tag != "" {
		t.Fatalf("idmap key must be untagged for single-key safety, got tag %q (%q)", tag, workflowIDMapKey(id))
	}
}

func TestRegistryAddGetRemoveRoundtrip(t *testing.T) {
	ctx := context.Background()
	reg, srv := newTestRegistry(t)

	created, err := reg.AddWorkflow(ctx, testRecord(t, uuid.NewString(), "sha256:abc"))
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	// bykey, byid, and idmap must all be present.
	if !srv.Exists(workflowByKeyKey(created.Key)) {
		t.Fatalf("bykey not set for %q", created.Key)
	}
	if !srv.Exists(workflowByIDKey(created.Key, created.ID)) {
		t.Fatalf("byid not set for %q", created.Key)
	}
	if !srv.Exists(workflowIDMapKey(created.ID)) {
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
	if srv.Exists(workflowByKeyKey(created.Key)) || srv.Exists(workflowByIDKey(created.Key, created.ID)) || srv.Exists(workflowIDMapKey(created.ID)) {
		t.Fatalf("keys still present after RemoveWorkflow for %q", created.Key)
	}

	if _, err := reg.GetWorkflow(ctx, created.ID); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("GetWorkflow after remove = %v, want ErrWorkflowNotFound", err)
	}
}

func TestRegistryAddIsIdempotentByKeyAndHash(t *testing.T) {
	ctx := context.Background()
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
	ctx := context.Background()
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
	ctx := context.Background()
	reg, _ := newTestRegistry(t)

	if err := reg.RemoveWorkflow(ctx, types.WorkflowID(uuid.NewString())); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("RemoveWorkflow missing = %v, want ErrWorkflowNotFound", err)
	}
}

// TestRegistryKeyByIDSharesTag locks in the exported schema: KeyByID must place
// the byid record in the same slot as KeyByKey (both carry `{<key>}`).
func TestRegistryKeyByIDSharesTag(t *testing.T) {
	const key = "ns/name@v1"
	id := types.WorkflowID(uuid.NewString())

	a := KeyByKey(key)
	b := KeyByID(key, id)
	if slotTag(a) == "" || slotTag(a) != slotTag(b) {
		t.Fatalf("KeyByKey %q and KeyByID %q must share a non-empty hash tag", a, b)
	}
}
