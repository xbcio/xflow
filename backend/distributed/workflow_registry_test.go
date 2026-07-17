package distributed

import (
	"context"
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/distributed/internal/workflowreg"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

func TestWorkflowRegistryIsSharedAcrossInstances(t *testing.T) {
	ctx := context.Background()
	first := newWorkflowRegistryTestBackend(t)
	second := newWorkflowRegistryTestBackend(t)

	rec := workflowRegistryTestRecord(t, uuid.NewString(), "sha256:shared")
	created, err := first.WorkflowRegistry().AddWorkflow(ctx, rec)
	if err != nil {
		t.Fatalf("AddWorkflow(first) error = %v", err)
	}
	t.Cleanup(func() {
		cleanupWorkflowRegistryRecord(t, first.rdb, created)
	})

	got, err := second.WorkflowRegistry().GetWorkflow(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetWorkflow(second) error = %v", err)
	}

	assertWorkflowRegistryRecord(t, got, created)
}

func TestWorkflowRegistryAddIsIdempotentByKeyAndHashAcrossInstances(t *testing.T) {
	ctx := context.Background()
	first := newWorkflowRegistryTestBackend(t)
	second := newWorkflowRegistryTestBackend(t)

	rec := workflowRegistryTestRecord(t, uuid.NewString(), "sha256:shared")
	created, err := first.WorkflowRegistry().AddWorkflow(ctx, rec)
	if err != nil {
		t.Fatalf("AddWorkflow(first) error = %v", err)
	}
	t.Cleanup(func() {
		cleanupWorkflowRegistryRecord(t, first.rdb, created)
	})

	same := workflowRegistryTestRecord(t, created.Name, created.DefinitionHash)
	same.Key = created.Key
	same.Namespace = created.Namespace
	same.Name = created.Name
	same.Version = created.Version

	got, err := second.WorkflowRegistry().AddWorkflow(ctx, same)
	if err != nil {
		t.Fatalf("AddWorkflow(second) error = %v", err)
	}
	if got.ID == "" {
		t.Fatal("AddWorkflow(second) returned empty workflow ID")
	}
	if got.ID != created.ID {
		t.Fatalf("AddWorkflow(second) ID = %q, want %q", got.ID, created.ID)
	}

	assertWorkflowRegistryRecord(t, got, created)
}

func TestWorkflowRegistryConflictsAcrossInstances(t *testing.T) {
	ctx := context.Background()
	first := newWorkflowRegistryTestBackend(t)
	second := newWorkflowRegistryTestBackend(t)

	rec := workflowRegistryTestRecord(t, uuid.NewString(), "sha256:a")
	created, err := first.WorkflowRegistry().AddWorkflow(ctx, rec)
	if err != nil {
		t.Fatalf("AddWorkflow(first) error = %v", err)
	}
	t.Cleanup(func() {
		cleanupWorkflowRegistryRecord(t, first.rdb, created)
	})

	conflict := workflowRegistryTestRecord(t, created.Name, "sha256:b")
	conflict.Key = created.Key
	conflict.Namespace = created.Namespace
	conflict.Name = created.Name
	conflict.Version = created.Version

	_, err = second.WorkflowRegistry().AddWorkflow(ctx, conflict)
	if !errors.Is(err, backend.ErrWorkflowConflict) {
		t.Fatalf("AddWorkflow(second) error = %v, want ErrWorkflowConflict", err)
	}
}

func TestWorkflowRegistryRemoveDeletesKeyAndID(t *testing.T) {
	ctx := context.Background()
	first := newWorkflowRegistryTestBackend(t)
	second := newWorkflowRegistryTestBackend(t)

	rec := workflowRegistryTestRecord(t, uuid.NewString(), "sha256:original")
	created, err := first.WorkflowRegistry().AddWorkflow(ctx, rec)
	if err != nil {
		t.Fatalf("AddWorkflow(first) error = %v", err)
	}

	if err := second.WorkflowRegistry().RemoveWorkflow(ctx, created.ID); err != nil {
		t.Fatalf("RemoveWorkflow(second) error = %v", err)
	}

	_, err = first.WorkflowRegistry().GetWorkflow(ctx, created.ID)
	if !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("GetWorkflow(first) after remove error = %v, want ErrWorkflowNotFound", err)
	}

	replacement := workflowRegistryTestRecord(t, created.Name, "sha256:replacement")
	replacement.Key = created.Key
	replacement.Namespace = created.Namespace
	replacement.Name = created.Name
	replacement.Version = created.Version

	recreated, err := first.WorkflowRegistry().AddWorkflow(ctx, replacement)
	if err != nil {
		t.Fatalf("AddWorkflow(first) replacement error = %v", err)
	}
	t.Cleanup(func() {
		cleanupWorkflowRegistryRecord(t, first.rdb, recreated)
	})

	if recreated.ID == created.ID {
		t.Fatalf("replacement ID = %q, want new workflow ID after remove", recreated.ID)
	}
}

func newWorkflowRegistryTestBackend(t *testing.T) *Backend {
	t.Helper()

	addr := os.Getenv("XFLOW_REDIS_ADDR")
	if addr == "" {
		t.Skip("XFLOW_REDIS_ADDR is required")
	}

	b, err := New(addr, nil, WithConsumer(false))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		_ = b.transport.Close()
		_ = b.rdb.Close()
	})
	return b
}

func workflowRegistryTestRecord(t *testing.T, nameSuffix, hash string) backend.WorkflowRecord {
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
			"start": {
				"main": {{Node: "review", Input: "main"}},
			},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
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

func cleanupWorkflowRegistryRecord(t *testing.T, rdb *redis.Client, rec backend.WorkflowRecord) {
	t.Helper()

	if rec.ID == "" {
		return
	}
	if err := rdb.Del(context.Background(), workflowreg.KeyByID(rec.ID), workflowreg.KeyByKey(rec.Key)).Err(); err != nil {
		t.Fatalf("Del(%q, %q) error = %v", workflowreg.KeyByID(rec.ID), workflowreg.KeyByKey(rec.Key), err)
	}
}

func assertWorkflowRegistryRecord(t *testing.T, got, want backend.WorkflowRecord) {
	t.Helper()

	if got.ID != want.ID {
		t.Fatalf("ID = %q, want %q", got.ID, want.ID)
	}
	if got.Key != want.Key {
		t.Fatalf("Key = %q, want %q", got.Key, want.Key)
	}
	if got.Namespace != want.Namespace {
		t.Fatalf("Namespace = %q, want %q", got.Namespace, want.Namespace)
	}
	if got.Name != want.Name {
		t.Fatalf("Name = %q, want %q", got.Name, want.Name)
	}
	if got.Version != want.Version {
		t.Fatalf("Version = %q, want %q", got.Version, want.Version)
	}
	if got.DefinitionHash != want.DefinitionHash {
		t.Fatalf("DefinitionHash = %q, want %q", got.DefinitionHash, want.DefinitionHash)
	}
	if !reflect.DeepEqual(got.Definition, want.Definition) {
		t.Fatalf("Definition = %#v, want %#v", got.Definition, want.Definition)
	}
	if got.Graph == nil {
		t.Fatal("Graph = nil, want compiled graph")
	}
	if want.Graph == nil {
		t.Fatal("want.Graph = nil, test fixture is invalid")
	}
	// Compare graph structure via the stable compiled hash (covers nodes,
	// edges, index, in-degree, and all other structural fields).
	if got.Graph.Hash() != want.Graph.Hash() {
		t.Fatalf("Graph.Hash = %q, want %q (graphs differ structurally)", got.Graph.Hash(), want.Graph.Hash())
	}
}
