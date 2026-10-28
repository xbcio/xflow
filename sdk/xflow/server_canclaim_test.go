package xflow

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

const (
	claimTypeA = "test.claim.a"
	claimTypeB = "test.claim.b"
)

func newClaimTestServer(t *testing.T) *Server {
	t.Helper()
	srv, err := NewServer(ServerConfig{}, WithServerInsecureNoRunnerAuth())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return srv
}

// claimTestDef is trigger -> a -> b, plus an unconnected supply node. Only a
// and b are dispatched to a runner; the trigger is placed by the activation
// reconciler and the supply node is control-plane data, so neither can ever be
// a claim gap.
func claimTestDef(name, ns string, workflowSelector *types.RunnerSelector, aSel, bSel *types.RunnerSelector) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:           name,
		Namespace:      ns,
		RunnerSelector: workflowSelector,
		Nodes: []types.NodeDef{
			{Name: "tick", Type: "test.claim.trigger", Version: 1, Kind: types.NodeKindTrigger},
			{Name: "rules", Type: "test.claim.supply", Kind: types.NodeKindSupply},
			{Name: "a", Type: claimTypeA, Version: 1, RunnerSelector: aSel},
			{Name: "b", Type: claimTypeB, Version: 1, RunnerSelector: bSel},
		},
		Connections: types.Connections{
			"tick": {"main": types.PortConnections{Targets: []types.Connection{{Node: "a", Input: "main"}}}},
			"a":    {"main": types.PortConnections{Targets: []types.Connection{{Node: "b", Input: "main"}}}},
		},
	}
}

func registerClaimWorkflow(t *testing.T, srv *Server, ns string, def *types.WorkflowDef) types.WorkflowID {
	t.Helper()
	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace(ns))
	res, err := srv.api.RegisterWorkflowReport(ctx, namespace.Namespace(ns), def)
	if err != nil {
		t.Fatalf("RegisterWorkflowReport: %v", err)
	}
	return res.ID
}

// registerClaimRunner registers a live runner through the in-process runner
// protocol, so it goes through the same Core.register the wire transports use.
func registerClaimRunner(t *testing.T, srv *Server, id string, labels map[string]string, namespaces []string, nodeTypes ...string) {
	t.Helper()
	caps := make([]protocol.Capability, 0, len(nodeTypes))
	for _, nt := range nodeTypes {
		caps = append(caps, protocol.Capability{NodeType: nt})
	}
	_, err := srv.ControlServer().InProcessRunnerClient("").Register(context.Background(), protocol.RegisterRunnerRequest{
		RunnerID:     id,
		Concurrency:  1,
		Labels:       labels,
		Capabilities: caps,
		Namespaces:   namespaces,
		InstanceUID:  "uid-" + id,
	})
	if err != nil {
		t.Fatalf("Register(%s): %v", id, err)
	}
}

func canClaim(t *testing.T, srv *Server, ns string, id types.WorkflowID) (bool, map[string]ClaimGap) {
	t.Helper()
	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace(ns))
	ok, gaps, err := srv.CanClaim(ctx, id)
	if err != nil {
		t.Fatalf("CanClaim: %v", err)
	}
	byNode := make(map[string]ClaimGap, len(gaps))
	for _, g := range gaps {
		byNode[g.NodeID] = g
	}
	if ok != (len(gaps) == 0) {
		t.Fatalf("ok = %v with %d gaps; ok must mean exactly 'no gaps'", ok, len(gaps))
	}
	return ok, byNode
}

func wantGaps(t *testing.T, got map[string]ClaimGap, want map[string]string) {
	t.Helper()
	gotReasons := make(map[string]string, len(got))
	for node, gap := range got {
		gotReasons[node] = gap.Reason
	}
	if len(gotReasons) != len(want) {
		t.Fatalf("gaps = %v, want %v", gotReasons, want)
	}
	for node, reason := range want {
		if gotReasons[node] != reason {
			t.Fatalf("gaps = %v, want %v", gotReasons, want)
		}
	}
}

func TestCanClaim(t *testing.T) {
	prod := map[string]string{"env": "prod"}
	staging := map[string]string{"env": "staging"}
	defaultProd := &types.RunnerSelector{Mode: types.RunnerSelectorModeDefault, MatchLabels: prod}
	requiredProd := &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired, MatchLabels: prod}

	t.Run("no runner registered", func(t *testing.T) {
		srv := newClaimTestServer(t)
		id := registerClaimWorkflow(t, srv, "default", claimTestDef("no-runner", "default", nil, nil, nil))
		ok, gaps := canClaim(t, srv, "default", id)
		if ok {
			t.Fatal("CanClaim = true with no runner registered")
		}
		// Only the two dispatched nodes. The trigger and the supply node are
		// absent: counting them would report gaps no runner could close.
		wantGaps(t, gaps, map[string]string{"a": control.SelectorRejectNamespace, "b": control.SelectorRejectNamespace})
		if gaps["a"].NodeType != claimTypeA || gaps["b"].NodeType != claimTypeB {
			t.Fatalf("gap node types = %q, %q, want %q, %q", gaps["a"].NodeType, gaps["b"].NodeType, claimTypeA, claimTypeB)
		}
	})

	t.Run("capable runner and no selector", func(t *testing.T) {
		srv := newClaimTestServer(t)
		registerClaimRunner(t, srv, "r1", nil, nil, claimTypeA, claimTypeB)
		id := registerClaimWorkflow(t, srv, "default", claimTestDef("plain", "default", nil, nil, nil))
		if ok, gaps := canClaim(t, srv, "default", id); !ok {
			t.Fatalf("CanClaim = false, gaps %v", gaps)
		}
	})

	t.Run("label mismatch under a default selector is a gap, not a fallback", func(t *testing.T) {
		srv := newClaimTestServer(t)
		registerClaimRunner(t, srv, "r1", staging, nil, claimTypeA, claimTypeB)
		id := registerClaimWorkflow(t, srv, "default", claimTestDef("label-mismatch", "default", defaultProd, nil, nil))
		ok, gaps := canClaim(t, srv, "default", id)
		if ok {
			t.Fatal("CanClaim = true although no runner carries the declared labels")
		}
		wantGaps(t, gaps, map[string]string{"a": control.SelectorRejectLabelPending, "b": control.SelectorRejectLabelPending})
	})

	t.Run("label match", func(t *testing.T) {
		srv := newClaimTestServer(t)
		registerClaimRunner(t, srv, "r1", prod, nil, claimTypeA, claimTypeB)
		id := registerClaimWorkflow(t, srv, "default", claimTestDef("label-match", "default", defaultProd, nil, nil))
		if ok, gaps := canClaim(t, srv, "default", id); !ok {
			t.Fatalf("CanClaim = false, gaps %v", gaps)
		}
	})

	t.Run("required selector reports label_required", func(t *testing.T) {
		srv := newClaimTestServer(t)
		registerClaimRunner(t, srv, "r1", staging, nil, claimTypeA, claimTypeB)
		id := registerClaimWorkflow(t, srv, "default", claimTestDef("required", "default", requiredProd, nil, nil))
		_, gaps := canClaim(t, srv, "default", id)
		wantGaps(t, gaps, map[string]string{"a": control.SelectorRejectLabelRequired, "b": control.SelectorRejectLabelRequired})
	})

	t.Run("missing capability", func(t *testing.T) {
		srv := newClaimTestServer(t)
		registerClaimRunner(t, srv, "r1", prod, nil, "some.other.node")
		id := registerClaimWorkflow(t, srv, "default", claimTestDef("no-capability", "default", defaultProd, nil, nil))
		_, gaps := canClaim(t, srv, "default", id)
		wantGaps(t, gaps, map[string]string{"a": control.SelectorRejectCapability, "b": control.SelectorRejectCapability})
	})

	t.Run("multi-node workflow reports only the nodes that have a gap", func(t *testing.T) {
		srv := newClaimTestServer(t)
		registerClaimRunner(t, srv, "r1", prod, nil, claimTypeA)
		id := registerClaimWorkflow(t, srv, "default", claimTestDef("partial", "default", nil, nil, nil))
		ok, gaps := canClaim(t, srv, "default", id)
		if ok {
			t.Fatal("CanClaim = true although node b has no capable runner")
		}
		wantGaps(t, gaps, map[string]string{"b": control.SelectorRejectCapability})
	})

	t.Run("node selector overrides the workflow selector", func(t *testing.T) {
		srv := newClaimTestServer(t)
		registerClaimRunner(t, srv, "r-prod", prod, nil, claimTypeA, claimTypeB)
		// The workflow asks for prod, node b asks for staging. Under a default
		// workflow selector the node's own selector replaces it. A node selector
		// carries labels only; the mode is a workflow-level setting.
		bStaging := &types.RunnerSelector{MatchLabels: staging}
		id := registerClaimWorkflow(t, srv, "default", claimTestDef("override", "default", defaultProd, nil, bStaging))

		ok, gaps := canClaim(t, srv, "default", id)
		if ok {
			t.Fatal("CanClaim = true although only a prod runner exists and node b requires staging")
		}
		wantGaps(t, gaps, map[string]string{"b": control.SelectorRejectLabelPending})

		registerClaimRunner(t, srv, "r-staging", staging, nil, claimTypeB)
		if ok, gaps := canClaim(t, srv, "default", id); !ok {
			t.Fatalf("CanClaim = false after a staging runner registered, gaps %v", gaps)
		}
	})

	t.Run("runner serving only another namespace", func(t *testing.T) {
		srv := newClaimTestServer(t)
		registerClaimRunner(t, srv, "r1", prod, []string{"tenant-x"}, claimTypeA, claimTypeB)
		id := registerClaimWorkflow(t, srv, "default", claimTestDef("other-ns-runner", "default", nil, nil, nil))
		_, gaps := canClaim(t, srv, "default", id)
		wantGaps(t, gaps, map[string]string{"a": control.SelectorRejectNamespace, "b": control.SelectorRejectNamespace})
	})

	t.Run("workflow in a named namespace is served by a runner in that namespace", func(t *testing.T) {
		srv := newClaimTestServer(t)
		registerClaimRunner(t, srv, "r1", prod, []string{"tenant-x"}, claimTypeA, claimTypeB)
		id := registerClaimWorkflow(t, srv, "tenant-x", claimTestDef("tenant-wf", "tenant-x", nil, nil, nil))
		if ok, gaps := canClaim(t, srv, "tenant-x", id); !ok {
			t.Fatalf("CanClaim = false, gaps %v", gaps)
		}
	})

	t.Run("workflow in a named namespace is not served by a default-namespace runner", func(t *testing.T) {
		srv := newClaimTestServer(t)
		registerClaimRunner(t, srv, "r1", prod, nil, claimTypeA, claimTypeB)
		id := registerClaimWorkflow(t, srv, "tenant-x", claimTestDef("tenant-wf", "tenant-x", nil, nil, nil))
		_, gaps := canClaim(t, srv, "tenant-x", id)
		wantGaps(t, gaps, map[string]string{"a": control.SelectorRejectNamespace, "b": control.SelectorRejectNamespace})
	})

	t.Run("runner whose heartbeat expired is not live", func(t *testing.T) {
		srv := newClaimTestServer(t)
		registerClaimRunner(t, srv, "r1", prod, nil, claimTypeA, claimTypeB)
		id := registerClaimWorkflow(t, srv, "default", claimTestDef("expired", "default", nil, nil, nil))
		if ok, gaps := canClaim(t, srv, "default", id); !ok {
			t.Fatalf("CanClaim = false for a fresh runner, gaps %v", gaps)
		}
		// Age the fleet past the liveness window instead of sleeping.
		srv.claimNow = func() time.Time { return time.Now().Add(control.DefaultRunnerSelector().LiveTTL + time.Minute) }
		ok, gaps := canClaim(t, srv, "default", id)
		if ok {
			t.Fatal("CanClaim = true for a runner whose heartbeat is older than the live TTL")
		}
		wantGaps(t, gaps, map[string]string{"a": control.SelectorRejectNamespace, "b": control.SelectorRejectNamespace})
	})

	t.Run("workflow with only trigger and supply nodes is trivially claimable", func(t *testing.T) {
		srv := newClaimTestServer(t)
		def := &types.WorkflowDef{
			Name: "trigger-only",
			Nodes: []types.NodeDef{
				{Name: "tick", Type: "test.claim.trigger", Version: 1, Kind: types.NodeKindTrigger},
				{Name: "rules", Type: "test.claim.supply", Kind: types.NodeKindSupply},
			},
		}
		id := registerClaimWorkflow(t, srv, "default", def)
		if ok, gaps := canClaim(t, srv, "default", id); !ok {
			t.Fatalf("CanClaim = false with no runner, gaps %v; nothing here is dispatched to a runner", gaps)
		}
	})
}

// TestClaimReasonsMatchTheClaimPathLabels pins the re-exported constants to the
// values the claim path reports, so the two vocabularies cannot drift and a
// caller comparing ClaimGap.Reason against ClaimReason* sees what the metric
// shows.
func TestClaimReasonsMatchTheClaimPathLabels(t *testing.T) {
	for _, tt := range []struct{ name, got, want string }{
		{"label_pending", ClaimReasonLabelPending, control.SelectorRejectLabelPending},
		{"label_required", ClaimReasonLabelRequired, control.SelectorRejectLabelRequired},
		{"capability", ClaimReasonCapability, control.SelectorRejectCapability},
		{"policy", ClaimReasonPolicy, control.SelectorRejectPolicy},
		{"namespace", ClaimReasonNamespace, control.SelectorRejectNamespace},
	} {
		if tt.got != tt.want || tt.got != tt.name {
			t.Errorf("ClaimReason %q = %q, control = %q; all three must be equal", tt.name, tt.got, tt.want)
		}
	}
}

// TestCanClaimReportsARunnerDirectoryFailure: when the runner directory cannot
// be read, CanClaim must say so. Treating the failure as "no runners" would
// report every node as a namespace gap; treating it as success would report all
// clear. The failure is confined to the directory enumeration (HKEYS) so the
// workflow-registry read, which uses HGET, still succeeds and the error can only
// come from the directory path.
func TestCanClaimReportsARunnerDirectoryFailure(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	var refuse atomic.Bool
	mr.Server().SetPreHook(func(p *server.Peer, cmd string, _ ...string) bool {
		if refuse.Load() && strings.EqualFold(cmd, "hkeys") {
			p.WriteError("injected runner directory failure")
			return true
		}
		return false
	})
	srv, err := NewServer(ServerConfig{RedisAddr: mr.Addr()}, WithServerInsecureNoRunnerAuth())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	registerClaimRunner(t, srv, "r1", nil, nil, claimTypeA, claimTypeB)
	id := registerClaimWorkflow(t, srv, "default", claimTestDef("dir-failure", "default", nil, nil, nil))

	if ok, gaps := canClaim(t, srv, "default", id); !ok {
		t.Fatalf("CanClaim = false before the failure, gaps %v", gaps)
	}

	refuse.Store(true)
	ok, gaps, err := srv.CanClaim(context.Background(), id)
	if err == nil {
		t.Fatalf("CanClaim = (%v, %v, nil), want an error when the runner directory cannot be read", ok, gaps)
	}
	if ok || gaps != nil {
		t.Fatalf("CanClaim = (%v, %v) alongside the error, want (false, nil)", ok, gaps)
	}
	if !strings.Contains(err.Error(), "runner directory") {
		t.Fatalf("err = %v, want it to name the runner directory", err)
	}
	if errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("err = %v, must not read as a missing workflow", err)
	}
}

func TestCanClaimWorkflowNotFound(t *testing.T) {
	srv := newClaimTestServer(t)
	ctx := context.Background()

	t.Run("unknown id", func(t *testing.T) {
		ok, gaps, err := srv.CanClaim(ctx, types.WorkflowID("does-not-exist"))
		if ok || gaps != nil {
			t.Fatalf("CanClaim = (%v, %v), want (false, nil) on error", ok, gaps)
		}
		if !errors.Is(err, backend.ErrWorkflowNotFound) {
			t.Fatalf("err = %v, want one wrapping backend.ErrWorkflowNotFound", err)
		}
	})

	// A missing workflow must not read as "all clear" nor as "has gaps": it is
	// an error, so a gate built on CanClaim cannot mistake it for either.
	t.Run("registered in another namespace", func(t *testing.T) {
		id := registerClaimWorkflow(t, srv, "tenant-x", claimTestDef("tenant-only", "tenant-x", nil, nil, nil))
		ok, gaps, err := srv.CanClaim(ctx, id)
		if ok || gaps != nil {
			t.Fatalf("CanClaim = (%v, %v), want (false, nil) on error", ok, gaps)
		}
		if !errors.Is(err, backend.ErrWorkflowNotFound) {
			t.Fatalf("err = %v, want one wrapping backend.ErrWorkflowNotFound", err)
		}
	})
}
