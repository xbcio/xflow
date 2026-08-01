package control

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
)

// newAckTestServer builds a control server with auth enforcing and a wired
// entry reconciler backed by a MemoryEntryActivationStore. It returns the
// httptest server, the activation store (for seed/assert), and the runner
// directory (for registering runners with specific namespaces).
func newAckTestServer(t *testing.T) (*httptest.Server, *MemoryEntryActivationStore, *MemoryRunnerDirectory) {
	t.Helper()
	store, err := NewFilePolicyStoreFromConfig(PolicyConfig{
		Version: 1,
		Runners: []PolicyEntry{{
			Name:     "ack-runner",
			IDPrefix: "runner-",
			Token:    "valid-token",
		}},
	}, false)
	if err != nil {
		t.Fatal(err)
	}

	actStore := NewMemoryEntryActivationStore()
	dir := NewMemoryRunnerDirectory()
	rec := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store:  actStore,
		Lister: dir,
	})
	fake := &fakeControlEngine{}
	srv := NewServer(fake, dir, WithAuthenticator(store))
	srv.core.entryReconciler = rec

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, actStore, dir
}

// TestActivationAckRequiresAuth verifies that an unauthenticated ack request is
// rejected with 401 and the store is NOT fenced.
func TestActivationAckRequiresAuth(t *testing.T) {
	ts, actStore, _ := newAckTestServer(t)

	// Seed an activation so we can verify it is NOT fenced.
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	key := engine.EntryActivationKey{
		Namespace:       namespace.Default,
		WorkflowID:      "wf-1",
		WorkflowVersion: "v1",
		EntryUnitID:     "entry-1",
	}
	_ = actStore.Upsert(ctx, engine.EntryActivation{
		Namespace:       namespace.Default,
		WorkflowID:      "wf-1",
		WorkflowVersion: "v1",
		EntryUnitID:     "entry-1",
		Desired:         true,
		RunnerID:        "runner-1",
		Generation:      1,
		LeaseDeadline:   time.Now().Add(time.Minute),
	})

	// Send ack with no token — should be rejected.
	ack := protocol.ActivationAck{
		RunnerID:        "runner-1",
		SessionID:       "sess-1",
		WorkflowID:      "wf-1",
		WorkflowVersion: "v1",
		GroupID:         "entry-1",
		Generation:      1,
		Status:          protocol.ActivationStatusFailed,
		Error:           "supply not available",
	}
	resp := postAuthed(t, ts.URL+protocol.ActivationAckPath, "", ack)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated ack: status = %d, want 401", resp.StatusCode)
	}

	// Verify store was NOT fenced.
	got, ok, _ := actStore.Get(ctx, key)
	if !ok || got.RunnerID != "runner-1" {
		t.Fatalf("unauthenticated ack must not fence; RunnerID = %q", got.RunnerID)
	}

	// Verify error response does not leak internals.
	var errResp errorResponse
	_ = json.NewDecoder(resp.Body).Decode(&errResp)
	if errResp.Error != "unauthenticated" {
		t.Fatalf("error body = %q, want generic 'unauthenticated'", errResp.Error)
	}
}

// TestActivationAckMissingWorkflowVersion verifies that an ack without
// workflow_version returns 400.
func TestActivationAckMissingWorkflowVersion(t *testing.T) {
	ts, _, _ := newAckTestServer(t)

	ack := protocol.ActivationAck{
		RunnerID:   "runner-1",
		SessionID:  "sess-1",
		WorkflowID: "wf-1",
		// WorkflowVersion intentionally empty
		GroupID:    "entry-1",
		Generation: 1,
		Status:     protocol.ActivationStatusFailed,
		Error:      "supply not available",
	}
	resp := postAuthed(t, ts.URL+protocol.ActivationAckPath, "valid-token", ack)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing workflow_version: status = %d, want 400", resp.StatusCode)
	}
}

// TestActivationAckFencesOnFailure verifies the happy path: a valid
// authenticated ack with Status=failed causes the activation to be fenced.
func TestActivationAckFencesOnFailure(t *testing.T) {
	ts, actStore, _ := newAckTestServer(t)

	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	key := engine.EntryActivationKey{
		Namespace:       namespace.Default,
		WorkflowID:      "wf-1",
		WorkflowVersion: "v1",
		EntryUnitID:     "entry-1",
	}
	_ = actStore.Upsert(ctx, engine.EntryActivation{
		Namespace:       namespace.Default,
		WorkflowID:      "wf-1",
		WorkflowVersion: "v1",
		EntryUnitID:     "entry-1",
		Desired:         true,
		RunnerID:        "runner-1",
		Generation:      1,
		LeaseDeadline:   time.Now().Add(time.Minute),
	})

	ack := protocol.ActivationAck{
		RunnerID:        "runner-1",
		SessionID:       "sess-1",
		WorkflowID:      "wf-1",
		WorkflowVersion: "v1",
		GroupID:         "entry-1",
		Generation:      1,
		Status:          protocol.ActivationStatusFailed,
		Error:           "supply not available",
	}
	resp := postAuthed(t, ts.URL+protocol.ActivationAckPath, "valid-token", ack)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("valid ack: status = %d, want 200", resp.StatusCode)
	}

	// Verify the activation was fenced (RunnerID cleared).
	got, ok, _ := actStore.Get(ctx, key)
	if !ok {
		t.Fatal("activation not found after ack")
	}
	if got.RunnerID != "" {
		t.Fatalf("after failed ack: RunnerID should be cleared (fenced), got %q", got.RunnerID)
	}
}

// TestActivationAckFencesNonDefaultNamespace verifies that a runner registered
// with a non-Default namespace can still fence an activation in that namespace
// via the ack endpoint. This is the regression test for the multi-namespace
// deployment scenario where namespace.FromContext(ctx) on the runner-protocol
// path would always return Default, causing the Store.Get to miss.
func TestActivationAckFencesNonDefaultNamespace(t *testing.T) {
	ts, actStore, dir := newAckTestServer(t)

	const customNS = namespace.Namespace("tenant-acme")

	// Register the runner with the non-Default namespace so the directory
	// returns it from Runner(). This is the server-side authoritative record.
	_, err := dir.Register(context.Background(), RegisterRunnerRequest{
		RunnerID:   "runner-1",
		Capacity:   1,
		Namespaces: []namespace.Namespace{customNS},
		Now:        time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Seed an activation in the non-Default namespace.
	ctx := namespace.WithNamespace(context.Background(), customNS)
	key := engine.EntryActivationKey{
		Namespace:       customNS,
		WorkflowID:      "wf-2",
		WorkflowVersion: "v1",
		EntryUnitID:     "entry-2",
	}
	_ = actStore.Upsert(ctx, engine.EntryActivation{
		Namespace:       customNS,
		WorkflowID:      "wf-2",
		WorkflowVersion: "v1",
		EntryUnitID:     "entry-2",
		Desired:         true,
		RunnerID:        "runner-1",
		Generation:      1,
		LeaseDeadline:   time.Now().Add(time.Minute),
	})

	ack := protocol.ActivationAck{
		RunnerID:        "runner-1",
		SessionID:       "sess-1",
		WorkflowID:      "wf-2",
		WorkflowVersion: "v1",
		GroupID:         "entry-2",
		Generation:      1,
		Status:          protocol.ActivationStatusFailed,
		Error:           "supply not available",
	}
	resp := postAuthed(t, ts.URL+protocol.ActivationAckPath, "valid-token", ack)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("non-default ns ack: status = %d, want 200", resp.StatusCode)
	}

	// The activation in the custom namespace must be fenced.
	got, ok, _ := actStore.Get(ctx, key)
	if !ok {
		t.Fatal("activation not found after ack")
	}
	if got.RunnerID != "" {
		t.Fatalf("non-default ns: RunnerID should be cleared (fenced), got %q", got.RunnerID)
	}
}
