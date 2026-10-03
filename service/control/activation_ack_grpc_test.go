package control

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/service/protocol/runnerpb"
)

// dialGRPCServer serves srv over an in-memory bufconn listener and returns a
// client dialed against it. Separate from grpc_server_test.go's
// startGRPCTestServer because that helper constructs its own *GRPCServer
// internally and never exposes it, but AckActivation's tests need the
// *GRPCServer handle beforehand to attach an entryReconciler the same way
// newAckTestServer (activation_ack_http_test.go) attaches one to Server.core.
func dialGRPCServer(t *testing.T, srv *GRPCServer) *protocol.GRPCClient {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	gs := grpc.NewServer()
	runnerpb.RegisterRunnerProtocolServer(gs, srv)
	go func() {
		_ = gs.Serve(lis)
	}()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufnet: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		gs.Stop()
	})
	return protocol.NewGRPCClient(conn)
}

// newGRPCAckTestServer mirrors newAckTestServer (activation_ack_http_test.go)
// but wires the gRPC transport, so the two test files can be read side by
// side as the transport-parity proof for AckActivation.
func newGRPCAckTestServer(t *testing.T) (*protocol.GRPCClient, *MemoryEntryActivationStore, *MemoryRunnerDirectory) {
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
	srv := NewGRPCServer(fake, dir, WithGRPCAuthenticator(store))
	srv.core.entryReconciler = rec
	client := dialGRPCServer(t, srv)
	return client, actStore, dir
}

func TestGRPCActivationAckRequiresAuth(t *testing.T) {
	client, actStore, _ := newGRPCAckTestServer(t)

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

	// No token set on the client — the server-side authenticator must reject.
	err := client.ActivationAck(context.Background(), protocol.ActivationAck{
		RunnerID:        "runner-1",
		SessionID:       "sess-1",
		WorkflowID:      "wf-1",
		WorkflowVersion: "v1",
		GroupID:         "entry-1",
		Generation:      1,
		Status:          protocol.ActivationStatusFailed,
		Error:           "supply not available",
	})
	if err == nil {
		t.Fatal("expected unauthenticated error")
	}
	if got := status.Code(err); got != codes.Unauthenticated {
		t.Fatalf("status code = %v, want Unauthenticated", got)
	}

	got, ok, _ := actStore.Get(ctx, key)
	if !ok || got.RunnerID != "runner-1" {
		t.Fatalf("unauthenticated ack must not fence; RunnerID = %q", got.RunnerID)
	}
}

func TestGRPCActivationAckMissingWorkflowVersion(t *testing.T) {
	client, _, _ := newGRPCAckTestServer(t)
	client = client.WithToken("valid-token")

	err := client.ActivationAck(context.Background(), protocol.ActivationAck{
		RunnerID:   "runner-1",
		SessionID:  "sess-1",
		WorkflowID: "wf-1",
		GroupID:    "entry-1",
		Generation: 1,
		Status:     protocol.ActivationStatusFailed,
		Error:      "supply not available",
	})
	if err == nil {
		t.Fatal("expected missing workflow_version error")
	}
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("status code = %v, want InvalidArgument", got)
	}
}

func TestGRPCActivationAckFencesOnFailure(t *testing.T) {
	client, actStore, _ := newGRPCAckTestServer(t)
	client = client.WithToken("valid-token")

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
	if err := client.ActivationAck(context.Background(), ack); err != nil {
		t.Fatalf("ActivationAck() error = %v", err)
	}

	got, ok, _ := actStore.Get(ctx, key)
	if !ok {
		t.Fatal("activation not found after ack")
	}
	if got.RunnerID != "" {
		t.Fatalf("after failed ack: RunnerID should be cleared (fenced), got %q", got.RunnerID)
	}
}

// TestGRPCActivationAckWrongRunnerIgnored proves a wrong-runner ack is a
// silent no-op (matching MarkActivationFailed's "ack_runner != store_runner"
// branch): the activation stays owned by the runner the store actually
// recorded, exactly as the HTTP transport behaves (both call the same
// Core.activationAck / MarkActivationFailed).
func TestGRPCActivationAckWrongRunnerIgnored(t *testing.T) {
	client, actStore, dir := newGRPCAckTestServer(t)
	client = client.WithToken("valid-token")

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
	// Register a second runner under the same policy prefix so it can
	// authenticate, then ack as that runner for an activation it does not own.
	if _, err := dir.Register(context.Background(), RegisterRunnerRequest{
		RunnerID: "runner-2",
		Capacity: 1,
		Now:      time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	ack := protocol.ActivationAck{
		RunnerID:        "runner-2",
		SessionID:       "sess-2",
		WorkflowID:      "wf-1",
		WorkflowVersion: "v1",
		GroupID:         "entry-1",
		Generation:      1,
		Status:          protocol.ActivationStatusFailed,
		Error:           "wrong runner",
	}
	if err := client.ActivationAck(context.Background(), ack); err != nil {
		t.Fatalf("ActivationAck() error = %v, want nil (silent no-op)", err)
	}

	got, ok, _ := actStore.Get(ctx, key)
	if !ok {
		t.Fatal("activation not found")
	}
	if got.RunnerID != "runner-1" {
		t.Fatalf("activation RunnerID = %q, want unchanged runner-1 (wrong-runner ack must not fence)", got.RunnerID)
	}
}

// TestGRPCActivationAckStaleGenerationIgnored proves an ack carrying an older
// generation than the store currently holds is a silent no-op, matching the
// "ack_generation != store_generation" branch of MarkActivationFailed.
func TestGRPCActivationAckStaleGenerationIgnored(t *testing.T) {
	client, actStore, _ := newGRPCAckTestServer(t)
	client = client.WithToken("valid-token")

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
		Generation:      2, // already moved past generation 1
		LeaseDeadline:   time.Now().Add(time.Minute),
	})

	ack := protocol.ActivationAck{
		RunnerID:        "runner-1",
		SessionID:       "sess-1",
		WorkflowID:      "wf-1",
		WorkflowVersion: "v1",
		GroupID:         "entry-1",
		Generation:      1, // stale
		Status:          protocol.ActivationStatusFailed,
		Error:           "supply not available",
	}
	if err := client.ActivationAck(context.Background(), ack); err != nil {
		t.Fatalf("ActivationAck() error = %v, want nil (silent no-op)", err)
	}

	got, ok, _ := actStore.Get(ctx, key)
	if !ok {
		t.Fatal("activation not found")
	}
	if got.RunnerID != "runner-1" || got.Generation != 2 {
		t.Fatalf("stale ack must not fence: got RunnerID=%q Generation=%d, want runner-1/2", got.RunnerID, got.Generation)
	}
}

// TestGRPCActivationAckDuplicateIsIdempotent proves sending the same failed
// ack twice fences exactly once and the second call is a harmless no-op: the
// store no longer shows RunnerID == ack.RunnerID after the first fence, so
// MarkActivationFailed's owner-mismatch guard makes the repeat inert rather
// than an error or a second fence.
func TestGRPCActivationAckDuplicateIsIdempotent(t *testing.T) {
	client, actStore, _ := newGRPCAckTestServer(t)
	client = client.WithToken("valid-token")

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
	if err := client.ActivationAck(context.Background(), ack); err != nil {
		t.Fatalf("first ActivationAck() error = %v", err)
	}
	got, ok, _ := actStore.Get(ctx, key)
	if !ok || got.RunnerID != "" {
		t.Fatalf("after first ack: RunnerID should be cleared, got %q", got.RunnerID)
	}

	// Repeat the exact same ack. The store no longer has RunnerID ==
	// "runner-1" (it was fenced to "" above), so the owner-mismatch branch
	// makes this a no-op rather than a second fence or an error.
	if err := client.ActivationAck(context.Background(), ack); err != nil {
		t.Fatalf("duplicate ActivationAck() error = %v, want nil (idempotent)", err)
	}
	got2, ok2, _ := actStore.Get(ctx, key)
	if !ok2 || got2.RunnerID != "" {
		t.Fatalf("after duplicate ack: RunnerID should remain cleared, got %q", got2.RunnerID)
	}
}
