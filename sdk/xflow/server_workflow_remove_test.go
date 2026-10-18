package xflow

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/namespace"
)

// TestServerRemoveWorkflowByName pins the embedded removal path an owner uses
// at startup to retire a workflow it persisted on an earlier run but no longer
// wants running (SAS: collection is on but its storage switch is off, and a
// registration from a previous run would otherwise keep being delivered).
//
// The namespace is the one ctx carries: a name registered in another namespace
// must not be reachable, and must not be disturbed by the failed attempt.
func TestServerRemoveWorkflowByName(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	srv, err := NewServer(ServerConfig{}, WithServerInsecureNoRunnerAuth())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer func() { _ = srv.Shutdown(context.Background()) }()

	if _, err := srv.ReplaceWorkflow(ctx, pinVersionWorkflow("srv-remove-by-name", "v1", "only")); err != nil {
		t.Fatalf("ReplaceWorkflow: %v", err)
	}
	key := "default/srv-remove-by-name@v1"

	otherNS := namespace.WithNamespace(ctx, "namespace-b")
	if err := srv.RemoveWorkflowByName(otherNS, "srv-remove-by-name", "v1"); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("removal from another namespace error = %v, want backend.ErrWorkflowNotFound", err)
	}
	if _, err := srv.Engine().workflowRegistry.GetWorkflowByKey(ctx, key); err != nil {
		t.Fatalf("a removal attempted from another namespace disturbed the record: %v", err)
	}

	if err := srv.RemoveWorkflowByName(ctx, "srv-remove-by-name", "v1"); err != nil {
		t.Fatalf("RemoveWorkflowByName: %v", err)
	}
	if _, err := srv.Engine().workflowRegistry.GetWorkflowByKey(ctx, key); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("record lookup after removal = %v, want backend.ErrWorkflowNotFound", err)
	}

	// Removal is the cleanup step of a boot sequence, so a retry after a
	// successful removal must report the same well-defined not-found sentinel
	// rather than an error the caller has to classify by string.
	if err := srv.RemoveWorkflowByName(ctx, "srv-remove-by-name", "v1"); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("repeated removal error = %v, want backend.ErrWorkflowNotFound", err)
	}
}

// TestServerRemoveWorkflowByID pins the by-id face and its argument handling.
func TestServerRemoveWorkflowByID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	srv, err := NewServer(ServerConfig{}, WithServerInsecureNoRunnerAuth())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer func() { _ = srv.Shutdown(context.Background()) }()

	id, err := srv.ReplaceWorkflow(ctx, pinVersionWorkflow("srv-remove-by-id", "v1", "only"))
	if err != nil {
		t.Fatalf("ReplaceWorkflow: %v", err)
	}

	if err := srv.RemoveWorkflow(ctx, id); err != nil {
		t.Fatalf("RemoveWorkflow(%q): %v", id, err)
	}
	if _, err := srv.GetWorkflowVersion(ctx, id, "v1"); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("GetWorkflowVersion after removal = %v, want backend.ErrWorkflowNotFound", err)
	}

	if err := srv.RemoveWorkflow(ctx, ""); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("RemoveWorkflow(empty id) = %v, want backend.ErrWorkflowNotFound", err)
	}
	if err := srv.RemoveWorkflowByName(ctx, "", "v1"); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("RemoveWorkflowByName(empty name) = %v, want backend.ErrWorkflowNotFound", err)
	}
}
