//go:build integration

// Package integration: this file pins the runner-namespace authorization
// chain (control.Core.register's entitlement gate, plus
// RedisRunnerDirectory.ClaimForRunner's canServeNamespace filter) against
// REAL Redis, not miniredis/in-memory. The chain landed with unit coverage
// only against NewMemoryRunnerDirectory
// (service/control/runner_namespace_entitlement_test.go); RunnerSnapshot's
// Redis encode/decode path (redis_runner_directory.go, decodeRunnerSnapshot)
// had never been exercised.
package integration

import (
	"context"
	"fmt"
	"net"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/service/protocol/runnerpb"
	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
)

// --- shared helpers ---------------------------------------------------

// nsbSeq makes every call to uniqueNSBID within a single process distinct
// even when two calls land in the same nanosecond (observed as a real
// collision risk on a fast machine with unique-suffix helpers elsewhere in
// this repo's history).
var nsbSeq int64

// uniqueNSBID returns a per-run-unique identifier so runner IDs, tokens, and
// namespace names cannot collide with a leftover key from an earlier run or
// another test in this package (the runner-directory Redis keys are a
// single global prefix, not scoped per test -- see redis_runner_directory_codec.go).
func uniqueNSBID(prefix string) string {
	n := atomic.AddInt64(&nsbSeq, 1)
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), n)
}

// freshRealRunnerDirectory builds a *control.RedisRunnerDirectory against the
// real Redis the harness discovers, flushing xflow:* keys at construction
// (not only in t.Cleanup) so a rerun of this file never collides with a
// previous run's runner/assignment state left over from a t.Fatal midway.
func freshRealRunnerDirectory(t *testing.T) (*control.RedisRunnerDirectory, *redis.Client) {
	t.Helper()
	addr := requireRedis(t)
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	flushXflowKeys(ctx, t, rdb)

	return control.NewRedisRunnerDirectory(rdb), rdb
}

// newNSBGRPCClient starts a bufconn gRPC server around dir/auth and returns a
// protocol.GRPCClient talking to it. Modeled on
// service/control/grpc_server_test.go's startGRPCTestServer (same package as
// the code under test, so not importable from here); reproduced using only
// exported symbols. engine=nil is safe: register/heartbeat/renewLease never
// touch it, only pollTask's final BuildTaskLease step does (guarded by a nil
// check), and this file never calls Poll.
//
// gRPC (not HTTP) is used where the test needs to see ErrAuthNamespaceDenied's
// own message text: grpc_server.go's runnerStatus default branch preserves
// err.Error() inside codes.Internal, whereas server.go's writeRunnerError
// default branch redacts it to a generic "internal server error" body (its
// own comment cites "§7 forbids exposing internals"). This is a deliberate,
// reported substitution for errors.Is(err, control.ErrAuthNamespaceDenied):
// that sentinel is unexported-package-internal from here (Core.register
// itself is unexported), so an external test can only observe it as message
// text over the wire, never reconstruct it as a typed error.
func newNSBGRPCClient(t *testing.T, dir control.RunnerDirectory, auth control.Authenticator) *protocol.GRPCClient {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	runnerpb.RegisterRunnerProtocolServer(srv, control.NewGRPCServer(nil, dir, control.WithGRPCAuthenticator(auth)))
	go func() { _ = srv.Serve(lis) }()

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
		srv.Stop()
	})
	return protocol.NewGRPCClient(conn)
}

// newNSBHTTPClient starts a real httptest.Server around dir/auth and returns
// a base protocol.Client (unauthenticated; callers attach a token via
// .WithToken). Used for judgement 4, where the HTTP status code itself
// (401 for ErrUnauthenticated) is the assertion and is not redacted the way
// ErrAuthNamespaceDenied's message is.
func newNSBHTTPClient(t *testing.T, dir control.RunnerDirectory, auth control.Authenticator) *protocol.Client {
	t.Helper()
	srv := control.NewServer(nil, dir, control.WithAuthenticator(auth))
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return protocol.NewClient(ts.URL, ts.Client())
}

// assertNeverPersisted polls (never sleeps, never reads once) that runnerID
// never reaches the real Redis directory. A denied register that merely
// "returns an error" is a different, weaker claim than "was never written" --
// this repo's own history is that reading state immediately after a
// should-not-happen event is a fake probe, so this waits out a window before
// declaring the negative proven.
func assertNeverPersisted(t *testing.T, dir *control.RedisRunnerDirectory, runnerID string) {
	t.Helper()
	deadline := time.Now().Add(300 * time.Millisecond)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, ok := dir.Runner(ctx, runnerID)
		cancel()
		if ok {
			t.Fatalf("runner %q was persisted to the real Redis directory despite a denied registration", runnerID)
		}
		if time.Now().After(deadline) {
			return
		}
		<-ticker.C
	}
}

// --- judgement 1: over-claiming a namespace is denied, and never persisted ---

// TestRunnerNamespaceBoundaryRealRedisRejectsUnauthorizedNamespace pins that
// a runner whose policy authorizes only one namespace cannot register into a
// different one, and that the rejection is not merely an error return: the
// runner must never reach the real Redis-backed directory.
func TestRunnerNamespaceBoundaryRealRedisRejectsUnauthorizedNamespace(t *testing.T) {
	dir, _ := freshRealRunnerDirectory(t)
	ctx := context.Background()

	grantedNS := uniqueNSBID("team-a")
	otherNS := uniqueNSBID("team-b")
	idPrefix := uniqueNSBID("nsb1-runner")
	token := uniqueNSBID("nsb1-token")

	auth, err := control.NewStaticTokenAuthenticator(idPrefix, token, []string{grantedNS}, []string{"*"})
	if err != nil {
		t.Fatalf("NewStaticTokenAuthenticator: %v", err)
	}
	client := newNSBGRPCClient(t, dir, auth)

	t.Run("OverclaimingNamespaceIsDenied", func(t *testing.T) {
		runnerID := idPrefix // starts with idPrefix trivially
		_, err := client.WithToken(token).Register(ctx, protocol.RegisterRunnerRequest{
			RunnerID:    runnerID,
			Concurrency: 1,
			Namespaces:  []string{otherNS},
		})
		if err == nil {
			t.Fatalf("register accepted namespace %q though the policy only grants %q", otherNS, grantedNS)
		}
		if !strings.Contains(err.Error(), control.ErrAuthNamespaceDenied.Error()) {
			t.Fatalf("register error = %q, want it to contain %q", err.Error(), control.ErrAuthNamespaceDenied.Error())
		}
		assertNeverPersisted(t, dir, runnerID)
	})

	// Positive control: the same policy, over the same transport, must still
	// accept the namespace it actually grants. Without this, the denial above
	// could pass against a server that rejects every registration.
	t.Run("PositiveControlGrantedNamespaceIsAccepted", func(t *testing.T) {
		runnerID := idPrefix + "-ok"
		if _, err := client.WithToken(token).Register(ctx, protocol.RegisterRunnerRequest{
			RunnerID:    runnerID,
			Concurrency: 1,
			Namespaces:  []string{grantedNS},
		}); err != nil {
			t.Fatalf("register rejected the granted namespace %q: %v", grantedNS, err)
		}
		snap, ok := dir.Runner(ctx, runnerID)
		if !ok {
			t.Fatalf("runner %q not found in the real Redis directory after an accepted register", runnerID)
		}
		if len(snap.Namespaces) != 1 || snap.Namespaces[0] != namespace.Namespace(grantedNS) {
			t.Fatalf("stored namespaces = %v, want exactly [%q]", snap.Namespaces, grantedNS)
		}
	})
}

// --- judgement 2: the undeclared-namespace effective-value gate ---

// TestRunnerNamespaceBoundaryRealRedisUndeclaredNamespaceGate pins the
// effective-value gate at Core.register: a runner that declares no
// Namespaces at all is not asking for "no namespace" -- downstream
// normalizes that to [namespace.Default], and the entitlement check must be
// applied against THAT effective value, not the (possibly empty) declared
// one.
func TestRunnerNamespaceBoundaryRealRedisUndeclaredNamespaceGate(t *testing.T) {
	dir, _ := freshRealRunnerDirectory(t)
	ctx := context.Background()

	// 2a: policy excludes default entirely -> undeclared must be rejected.
	t.Run("RejectedWhenPolicyExcludesDefault", func(t *testing.T) {
		grantedNS := uniqueNSBID("team-a")
		idPrefix := uniqueNSBID("nsb2a-runner")
		token := uniqueNSBID("nsb2a-token")
		auth, err := control.NewStaticTokenAuthenticator(idPrefix, token, []string{grantedNS}, []string{"*"})
		if err != nil {
			t.Fatalf("NewStaticTokenAuthenticator: %v", err)
		}
		client := newNSBGRPCClient(t, dir, auth)

		runnerID := idPrefix
		_, err = client.WithToken(token).Register(ctx, protocol.RegisterRunnerRequest{
			RunnerID:    runnerID,
			Concurrency: 1,
			// Namespaces intentionally omitted.
		})
		if err == nil {
			t.Fatal("register accepted an undeclared namespace under a policy that excludes default")
		}
		if !strings.Contains(err.Error(), control.ErrAuthNamespaceDenied.Error()) {
			t.Fatalf("register error = %q, want it to contain %q", err.Error(), control.ErrAuthNamespaceDenied.Error())
		}
		assertNeverPersisted(t, dir, runnerID)
	})

	// 2b: positive control. Policy's AllowedNamespaces is empty (= default
	// only, per AllowsNamespace's own doc comment), and the runner declares no
	// Namespaces. This must SUCCEED, and the snapshot read back from real
	// Redis must show exactly [default]. Without this control, 2a could pass
	// against a regression that rejects every undeclared namespace
	// unconditionally -- a materially different (and wrong) behavior that
	// would break every legacy runner.
	t.Run("PositiveControlAcceptsAndPersistsDefaultWhenPolicyIsEmpty", func(t *testing.T) {
		idPrefix := uniqueNSBID("nsb2b-runner")
		token := uniqueNSBID("nsb2b-token")
		auth, err := control.NewStaticTokenAuthenticator(idPrefix, token, nil, []string{"*"})
		if err != nil {
			t.Fatalf("NewStaticTokenAuthenticator: %v", err)
		}
		client := newNSBGRPCClient(t, dir, auth)

		runnerID := idPrefix
		if _, err := client.WithToken(token).Register(ctx, protocol.RegisterRunnerRequest{
			RunnerID:    runnerID,
			Concurrency: 1,
			// Namespaces intentionally omitted.
		}); err != nil {
			t.Fatalf("register rejected an undeclared namespace under a default-only (empty AllowedNamespaces) policy: %v", err)
		}

		snap, ok := dir.Runner(ctx, runnerID)
		if !ok {
			t.Fatalf("runner %q not found in the real Redis directory after an accepted register", runnerID)
		}
		if len(snap.Namespaces) != 1 || snap.Namespaces[0] != namespace.Default {
			t.Fatalf("stored namespaces (from real Redis) = %v, want exactly [%q]", snap.Namespaces, namespace.Default)
		}
	})
}

// --- judgement 3: ClaimForRunner scopes claims by namespace, exactly ---

// nsbTask builds a minimal synthetic engine.Task shaped like what
// control.Dispatcher.HandleTask constructs in production. engine.Task carries
// no node type of its own -- routing by node type happens on the enclosing
// Assignment's engine.TaskRouting, which nsbEnqueue sets -- so this helper
// deliberately takes no nodeType argument.
func nsbTask(execID types.ExecutionID, nodeName string, nodeIdx int) engine.Task {
	return engine.Task{
		ExecutionID: execID,
		NodeName:    nodeName,
		NodeIdx:     nodeIdx,
		Type:        engine.TaskTypeNodeExec,
	}
}

// nsbEnqueue enqueues one synthetic assignment for ns/nodeType and fails the
// test if it is rejected as a duplicate (which would mean nodeName/nodeIdx
// were not actually unique -- a bug in this test, not in the code under
// test).
func nsbEnqueue(t *testing.T, ctx context.Context, dir *control.RedisRunnerDirectory, execID types.ExecutionID, nodeName string, nodeIdx int, nodeType string, ns namespace.Namespace) control.AssignmentID {
	t.Helper()
	task := nsbTask(execID, nodeName, nodeIdx)
	assignment := control.Assignment{
		AssignmentID: control.BuildAssignmentID(&task),
		Task:         task,
		Routing:      engine.TaskRouting{NodeType: nodeType},
		Namespace:    ns,
	}
	enqueued, err := dir.EnqueueAssignment(ctx, assignment)
	if err != nil {
		t.Fatalf("EnqueueAssignment(%s, ns=%s): %v", nodeName, ns, err)
	}
	if !enqueued {
		t.Fatalf("EnqueueAssignment(%s, ns=%s) reported duplicate; nodeName/nodeIdx were not unique", nodeName, ns)
	}
	return assignment.AssignmentID
}

// nsbDrain repeatedly calls ClaimForRunner until it reports no more matching
// work, returning the exact multiset of namespaces claimed. This is the
// counting primitive judgement 3 needs: "claimed only namespace A" must be an
// exact set/count equality, never ">= 1" or "contains" (this repo's own
// history: a leak that fires 10001 times still satisfies ">= 1").
func nsbDrain(t *testing.T, ctx context.Context, dir *control.RedisRunnerDirectory, runnerID, sessionID, nodeType string, maxClaims int) map[namespace.Namespace]int {
	t.Helper()
	got := map[namespace.Namespace]int{}
	for i := 0; i < maxClaims; i++ {
		claim, ok, err := dir.ClaimForRunner(ctx, control.ClaimRequest{
			RunnerID:     runnerID,
			SessionID:    sessionID,
			Capacity:     maxClaims,
			Capabilities: []protocol.Capability{{NodeType: nodeType}},
			Now:          time.Now(),
		})
		if err != nil {
			t.Fatalf("ClaimForRunner(%s) iteration %d: %v", runnerID, i, err)
		}
		if !ok {
			break
		}
		got[claim.Assignment.Namespace]++
	}
	return got
}

// TestRunnerNamespaceBoundaryRealRedisClaimForRunnerScopesByNamespace pins
// canServeNamespace (redis_runner_directory.go), the function ClaimForRunner
// uses to filter queued assignments by the claiming runner's persisted
// namespaces -- read back from the same real-Redis-encoded RunnerSnapshot
// judgements 1/2 pin the write side of. Two runners are registered through
// the real entitlement gate (control.Core.register, over gRPC) into two
// distinct namespaces; three assignments are queued in each namespace under
// one process-unique node type; a runner scoped to namespace A must drain
// EXACTLY its own three, and namespace B's three must remain claimable
// (not silently dropped) by a runner scoped to namespace B.
func TestRunnerNamespaceBoundaryRealRedisClaimForRunnerScopesByNamespace(t *testing.T) {
	dir, _ := freshRealRunnerDirectory(t)
	ctx := context.Background()

	tag := uniqueNSBID("nsb3")
	nsA := namespace.Namespace(tag + "-ns-a")
	nsB := namespace.Namespace(tag + "-ns-b")
	nodeType := tag + ".probe" // globally unique: nothing else in the shared queue can match it
	execID := types.ExecutionID(tag + "-exec")

	tokenA := uniqueNSBID("nsb3-token-a")
	tokenB := uniqueNSBID("nsb3-token-b")
	idA := uniqueNSBID("nsb3-runner-a")
	idB := uniqueNSBID("nsb3-runner-b")

	policyStore, err := control.NewFilePolicyStoreFromConfig(control.PolicyConfig{
		Version: 1,
		Runners: []control.PolicyEntry{
			{Name: "nsb3-a", IDPrefix: idA, Token: tokenA, AllowedNodeTypes: []string{"*"}, AllowedNamespaces: []string{string(nsA)}},
			{Name: "nsb3-b", IDPrefix: idB, Token: tokenB, AllowedNodeTypes: []string{"*"}, AllowedNamespaces: []string{string(nsB)}},
		},
	}, false)
	if err != nil {
		t.Fatalf("NewFilePolicyStoreFromConfig: %v", err)
	}
	client := newNSBGRPCClient(t, dir, policyStore)

	regA, err := client.WithToken(tokenA).Register(ctx, protocol.RegisterRunnerRequest{
		RunnerID: idA, Concurrency: 10, Namespaces: []string{string(nsA)},
	})
	if err != nil {
		t.Fatalf("register runner A: %v", err)
	}
	regB, err := client.WithToken(tokenB).Register(ctx, protocol.RegisterRunnerRequest{
		RunnerID: idB, Concurrency: 10, Namespaces: []string{string(nsB)},
	})
	if err != nil {
		t.Fatalf("register runner B: %v", err)
	}

	const perNamespace = 3
	for i := 0; i < perNamespace; i++ {
		nsbEnqueue(t, ctx, dir, execID, fmt.Sprintf("node-a-%d", i), i, nodeType, nsA)
	}
	for i := 0; i < perNamespace; i++ {
		nsbEnqueue(t, ctx, dir, execID, fmt.Sprintf("node-b-%d", i), i, nodeType, nsB)
	}

	// Drain runner A FIRST, while namespace B's three assignments are still
	// sitting unclaimed in the (globally shared) queue: this is the case that
	// actually exercises the filter, not merely "there happened to be nothing
	// else to claim".
	gotA := nsbDrain(t, ctx, dir, idA, regA.SessionID, nodeType, perNamespace*2)
	wantA := map[namespace.Namespace]int{nsA: perNamespace}
	if len(gotA) != len(wantA) || gotA[nsA] != wantA[nsA] {
		t.Fatalf("runner A (namespace %q) claimed = %v, want exactly %v", nsA, gotA, wantA)
	}

	// Runner B must still be able to claim its own three -- proving A's scan
	// skipped them rather than silently discarding or consuming them.
	gotB := nsbDrain(t, ctx, dir, idB, regB.SessionID, nodeType, perNamespace*2)
	wantB := map[namespace.Namespace]int{nsB: perNamespace}
	if len(gotB) != len(wantB) || gotB[nsB] != wantB[nsB] {
		t.Fatalf("runner B (namespace %q) claimed = %v, want exactly %v", nsB, gotB, wantB)
	}
}

// --- judgement 4: the ongoing path (renewLease, heartbeat) rejects a bad token ---

// TestRunnerNamespaceBoundaryRealRedisOngoingAuthRejectsBadToken pins that
// the ongoing-request path re-authenticates on every call, not only at
// register time. renewLease is prioritized as the newest-added path; a
// second endpoint (heartbeat) is included so a fix scoped to only one of
// them cannot pass unnoticed. HTTP is used here (not gRPC) because the
// assertion is the transport status code itself: ErrUnauthenticated maps to
// a distinguishable 401 on both transports, unlike ErrAuthNamespaceDenied.
func TestRunnerNamespaceBoundaryRealRedisOngoingAuthRejectsBadToken(t *testing.T) {
	dir, _ := freshRealRunnerDirectory(t)
	ctx := context.Background()

	idPrefix := uniqueNSBID("nsb4-runner")
	goodToken := uniqueNSBID("nsb4-good-token")
	badToken := uniqueNSBID("nsb4-bad-token")

	auth, err := control.NewStaticTokenAuthenticator(idPrefix, goodToken, []string{"*"}, []string{"*"})
	if err != nil {
		t.Fatalf("NewStaticTokenAuthenticator: %v", err)
	}
	base := newNSBHTTPClient(t, dir, auth)

	runnerID := idPrefix
	goodClient := base.WithToken(goodToken)
	reg, err := goodClient.Register(ctx, protocol.RegisterRunnerRequest{RunnerID: runnerID, Concurrency: 1})
	if err != nil {
		t.Fatalf("register with the good token: %v", err)
	}

	badClient := base.WithToken(badToken)

	t.Run("RenewLeaseRejectsBadToken", func(t *testing.T) {
		_, err := badClient.RenewLease(ctx, protocol.RenewLeaseRequest{
			RunnerID:  runnerID,
			SessionID: reg.SessionID,
			LeaseID:   "no-such-lease", // auth must fail before any lease lookup
			Extend:    1000,
		})
		if err == nil {
			t.Fatal("renewLease accepted a bad auth token")
		}
		if !strings.Contains(err.Error(), "status 401") {
			t.Fatalf("renewLease error = %q, want it to report status 401", err.Error())
		}
	})

	t.Run("HeartbeatRejectsBadToken", func(t *testing.T) {
		_, err := badClient.Heartbeat(ctx, protocol.HeartbeatRequest{
			RunnerID:  runnerID,
			SessionID: reg.SessionID,
			Capacity:  1,
		})
		if err == nil {
			t.Fatal("heartbeat accepted a bad auth token")
		}
		if !strings.Contains(err.Error(), "status 401") {
			t.Fatalf("heartbeat error = %q, want it to report status 401", err.Error())
		}
	})

	// Positive control: the SAME session, with the good token, must still be
	// able to heartbeat. Without this, a server that rejected every heartbeat
	// unconditionally would also pass the negative assertion above.
	t.Run("PositiveControlGoodTokenHeartbeatSucceeds", func(t *testing.T) {
		if _, err := goodClient.Heartbeat(ctx, protocol.HeartbeatRequest{
			RunnerID:  runnerID,
			SessionID: reg.SessionID,
			Capacity:  1,
		}); err != nil {
			t.Fatalf("heartbeat with the good token: %v", err)
		}
	})
}
