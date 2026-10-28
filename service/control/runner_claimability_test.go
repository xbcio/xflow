package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

func claimTestSnapshot(id string, now time.Time, labels map[string]string, nodeType string, namespaces ...namespace.Namespace) RunnerSnapshot {
	return RunnerSnapshot{
		RunnerID:      id,
		Capacity:      1,
		Labels:        labels,
		Capabilities:  []protocol.Capability{{NodeType: nodeType}},
		Namespaces:    namespaces,
		LastHeartbeat: now,
	}
}

func claimTestRouting(nodeType string, sel *types.RunnerSelector) engine.TaskRouting {
	return engine.TaskRouting{NodeType: nodeType, RunnerSelector: sel}
}

// TestClassifyUnitClaim walks every gate the claim path applies and checks the
// verdict names the first gate that has no survivors. The default-versus-
// required split is the load-bearing one: the claim path would eventually
// relax a default selector, the classifier must not.
func TestClassifyUnitClaim(t *testing.T) {
	now := time.Date(2026, 10, 28, 9, 0, 0, 0, time.UTC)
	const nodeType = "http.request"
	defaultSel := &types.RunnerSelector{Mode: types.RunnerSelectorModeDefault, MatchLabels: map[string]string{"env": "prod"}}
	unsetModeSel := &types.RunnerSelector{MatchLabels: map[string]string{"env": "prod"}}
	requiredSel := &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired, MatchLabels: map[string]string{"env": "prod"}}
	prod := map[string]string{"env": "prod", "zone": "a"}
	staging := map[string]string{"env": "staging"}
	draining := func(s RunnerSnapshot) RunnerSnapshot {
		s.Control = &RunnerControlSnapshot{DesiredState: RunnerDesiredStateDraining}
		return s
	}
	expired := func(s RunnerSnapshot) RunnerSnapshot {
		s.LastHeartbeat = now.Add(-DefaultRunnerSelector().LiveTTL - time.Second)
		return s
	}

	tests := []struct {
		name       string
		snapshots  []RunnerSnapshot
		routing    engine.TaskRouting
		ns         namespace.Namespace
		wantOK     bool
		wantReason string
	}{
		{
			name:       "no runner registered",
			routing:    claimTestRouting(nodeType, nil),
			ns:         namespace.Default,
			wantReason: SelectorRejectNamespace,
		},
		{
			name:      "no selector, capable runner",
			snapshots: []RunnerSnapshot{claimTestSnapshot("r1", now, nil, nodeType)},
			routing:   claimTestRouting(nodeType, nil),
			ns:        namespace.Default,
			wantOK:    true,
		},
		{
			name:      "label match",
			snapshots: []RunnerSnapshot{claimTestSnapshot("r1", now, prod, nodeType)},
			routing:   claimTestRouting(nodeType, defaultSel),
			ns:        namespace.Default,
			wantOK:    true,
		},
		{
			name:       "default selector, label mismatch is pending, not served by fallback",
			snapshots:  []RunnerSnapshot{claimTestSnapshot("r1", now, staging, nodeType)},
			routing:    claimTestRouting(nodeType, defaultSel),
			ns:         namespace.Default,
			wantReason: SelectorRejectLabelPending,
		},
		{
			name:       "empty selector mode behaves as default",
			snapshots:  []RunnerSnapshot{claimTestSnapshot("r1", now, staging, nodeType)},
			routing:    claimTestRouting(nodeType, unsetModeSel),
			ns:         namespace.Default,
			wantReason: SelectorRejectLabelPending,
		},
		{
			name:       "required selector, label mismatch",
			snapshots:  []RunnerSnapshot{claimTestSnapshot("r1", now, staging, nodeType)},
			routing:    claimTestRouting(nodeType, requiredSel),
			ns:         namespace.Default,
			wantReason: SelectorRejectLabelRequired,
		},
		{
			name:       "capability missing",
			snapshots:  []RunnerSnapshot{claimTestSnapshot("r1", now, prod, "other.node")},
			routing:    claimTestRouting(nodeType, defaultSel),
			ns:         namespace.Default,
			wantReason: SelectorRejectCapability,
		},
		{
			name:       "an incapable runner does not mask the label gap of a capable one",
			snapshots:  []RunnerSnapshot{claimTestSnapshot("r1", now, prod, "other.node"), claimTestSnapshot("r2", now, staging, nodeType)},
			routing:    claimTestRouting(nodeType, requiredSel),
			ns:         namespace.Default,
			wantReason: SelectorRejectLabelRequired,
		},
		{
			name:       "runner serves only another namespace",
			snapshots:  []RunnerSnapshot{claimTestSnapshot("r1", now, prod, nodeType, "tenant-x")},
			routing:    claimTestRouting(nodeType, defaultSel),
			ns:         namespace.Default,
			wantReason: SelectorRejectNamespace,
		},
		{
			name:      "runner serves the requested namespace",
			snapshots: []RunnerSnapshot{claimTestSnapshot("r1", now, prod, nodeType, "tenant-x")},
			routing:   claimTestRouting(nodeType, defaultSel),
			ns:        "tenant-x",
			wantOK:    true,
		},
		{
			name:       "default-namespace runner does not serve a named namespace",
			snapshots:  []RunnerSnapshot{claimTestSnapshot("r1", now, prod, nodeType)},
			routing:    claimTestRouting(nodeType, nil),
			ns:         "tenant-x",
			wantReason: SelectorRejectNamespace,
		},
		{
			name:       "expired heartbeat is not a candidate",
			snapshots:  []RunnerSnapshot{expired(claimTestSnapshot("r1", now, prod, nodeType))},
			routing:    claimTestRouting(nodeType, defaultSel),
			ns:         namespace.Default,
			wantReason: SelectorRejectNamespace,
		},
		{
			name:       "zero heartbeat is not live",
			snapshots:  []RunnerSnapshot{claimTestSnapshot("r1", time.Time{}, prod, nodeType)},
			routing:    claimTestRouting(nodeType, nil),
			ns:         namespace.Default,
			wantReason: SelectorRejectNamespace,
		},
		{
			name:       "draining runner receives no claims",
			snapshots:  []RunnerSnapshot{draining(claimTestSnapshot("r1", now, prod, nodeType))},
			routing:    claimTestRouting(nodeType, defaultSel),
			ns:         namespace.Default,
			wantReason: SelectorRejectNamespace,
		},
		{
			name: "one matching runner among non-matching ones is enough",
			snapshots: []RunnerSnapshot{
				claimTestSnapshot("r1", now, staging, nodeType),
				expired(claimTestSnapshot("r2", now, prod, nodeType)),
				claimTestSnapshot("r3", now, prod, nodeType),
			},
			routing: claimTestRouting(nodeType, requiredSel),
			ns:      namespace.Default,
			wantOK:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ok, reason := ClassifyUnitClaim(tt.snapshots, tt.routing, tt.ns, now)
			if ok != tt.wantOK || reason != tt.wantReason {
				t.Fatalf("ClassifyUnitClaim = (%v, %q), want (%v, %q)", ok, reason, tt.wantOK, tt.wantReason)
			}
		})
	}
}

// TestClassifyUnitClaimAgreesWithTheClaimPath ties the classifier to the memory
// directory's real claim decision: wherever the classifier says "claimable" a
// runner must be able to claim the assignment immediately, and where it says a
// strict label gap the claim path must still hold the assignment back inside
// the grace window.
func TestClassifyUnitClaimAgreesWithTheClaimPath(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, 10, 28, 9, 0, 0, 0, time.UTC)
	sel := &types.RunnerSelector{Mode: types.RunnerSelectorModeDefault, MatchLabels: map[string]string{"env": "prod"}}
	routing := engine.TaskRouting{NodeType: "http.request", RunnerSelector: sel}

	for _, tt := range []struct {
		name      string
		labels    map[string]string
		wantOK    bool
		wantClaim bool
	}{
		{name: "matching labels", labels: map[string]string{"env": "prod"}, wantOK: true, wantClaim: true},
		{name: "mismatched labels inside grace", labels: map[string]string{"env": "staging"}, wantOK: false, wantClaim: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			clock := newGraceTestClock(start)
			dir := NewMemoryRunnerDirectory(WithMemoryRunnerDirectoryClock(clock.Now))
			session := graceTestRunner(t, dir, "r1", tt.labels)

			snapshots, err := EnumerateRunnerSnapshots(ctx, dir)
			if err != nil {
				t.Fatalf("EnumerateRunnerSnapshots: %v", err)
			}
			if ok, _ := ClassifyUnitClaim(snapshots, routing, namespace.Default, clock.Now()); ok != tt.wantOK {
				t.Fatalf("classifier claimable = %v, want %v", ok, tt.wantOK)
			}

			if _, err := dir.EnqueueAssignment(ctx, graceTestAssignment("a1", sel)); err != nil {
				t.Fatalf("EnqueueAssignment: %v", err)
			}
			_, claimed := claimFor(t, dir, session, "r1")
			if claimed != tt.wantClaim {
				t.Fatalf("claim path claimed = %v, want %v", claimed, tt.wantClaim)
			}
		})
	}
}

func TestEnumerateRunnerSnapshots(t *testing.T) {
	ctx := context.Background()

	t.Run("nil directory is incompatible, not empty", func(t *testing.T) {
		if _, err := EnumerateRunnerSnapshots(ctx, nil); !errors.Is(err, ErrRunnerDirectoryIncompatible) {
			t.Fatalf("err = %v, want ErrRunnerDirectoryIncompatible", err)
		}
	})

	t.Run("directory without an enumeration capability is incompatible", func(t *testing.T) {
		opaque := struct{ RunnerDirectory }{}
		if _, err := EnumerateRunnerSnapshots(ctx, opaque); !errors.Is(err, ErrRunnerDirectoryIncompatible) {
			t.Fatalf("err = %v, want ErrRunnerDirectoryIncompatible", err)
		}
	})

	t.Run("memory directory lists registered runners", func(t *testing.T) {
		dir := NewMemoryRunnerDirectory()
		graceTestRunner(t, dir, "r1", map[string]string{"env": "prod"})
		graceTestRunner(t, dir, "r2", nil)
		snaps, err := EnumerateRunnerSnapshots(ctx, dir)
		if err != nil || len(snaps) != 2 {
			t.Fatalf("snapshots = %d, err = %v, want 2 runners", len(snaps), err)
		}
	})

	t.Run("redis directory lists registered runners", func(t *testing.T) {
		_, rdb := newRedisRunnerDirectoryTestClient(t)
		dir := NewRedisRunnerDirectory(rdb)
		registerRedisDirectoryRunner(t, ctx, dir, "r1", 1, "tenant-x")
		snaps, err := EnumerateRunnerSnapshots(ctx, dir)
		if err != nil || len(snaps) != 1 || snaps[0].RunnerID != "r1" {
			t.Fatalf("snapshots = %+v, err = %v, want runner r1", snaps, err)
		}
		if len(snaps[0].Namespaces) != 1 || snaps[0].Namespaces[0] != "tenant-x" {
			t.Fatalf("namespaces = %v, want [tenant-x]", snaps[0].Namespaces)
		}
	})

	// The reason ListRunnerSnapshots exists: ListLiveRunners returns nil for an
	// unreachable Redis exactly as for an empty fleet.
	t.Run("redis failure is an error, not an empty fleet", func(t *testing.T) {
		server, rdb := newRedisRunnerDirectoryTestClient(t)
		dir := NewRedisRunnerDirectory(rdb)
		registerRedisDirectoryRunner(t, ctx, dir, "r1", 1)
		server.Close()

		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if _, err := EnumerateRunnerSnapshots(callCtx, dir); err == nil {
			t.Fatal("EnumerateRunnerSnapshots against a dead Redis returned no error")
		}
		if live := dir.ListLiveRunners(callCtx); live != nil {
			t.Fatalf("ListLiveRunners = %v, want nil (errors stay swallowed for the existing callers)", live)
		}
	})
}
