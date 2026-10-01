package control

import (
	"context"
	"testing"
	"time"

	backendlocal "github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/service/protocol"
)

func registerNodeTypesRunner(t *testing.T, dir RunnerDirectory, runnerID string, ns namespace.Namespace, now time.Time, d RunnerNodeDescriptor) {
	t.Helper()
	if _, err := dir.Register(context.Background(), RegisterRunnerRequest{
		RunnerID:     runnerID,
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: d.Type, NodeVersion: d.Version}},
		Descriptors:  []RunnerNodeDescriptor{d},
		PoolName:     "pool-" + runnerID,
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}, AllowedNamespaces: []string{"*"}},
		Namespaces:   []namespace.Namespace{ns},
		InstanceUID:  "instance-" + runnerID,
		Now:          now,
	}); err != nil {
		t.Fatalf("Register(%s): %v", runnerID, err)
	}
}

// Metrics are configured so RunnerDirectory() returns the management
// decorator: LiveRunnerNodeTypes must still reach the raw directory's
// descriptor capability.
func TestControlPlaneLiveRunnerNodeTypes(t *testing.T) {
	dir := NewMemoryRunnerDirectory()
	cp, err := NewControlPlane(Config{Backend: backendlocal.New(), RunnerDirectory: dir, Metrics: metrics.New()})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	base := time.Now().UTC()
	registerNodeTypesRunner(t, dir, "runner-a", "tenant-a", base, contractDescriptor("acme.a", 1, "A1"))
	registerNodeTypesRunner(t, dir, "runner-b", "tenant-b", base, contractDescriptor("acme.b", 1, "B1"))

	got, err := cp.LiveRunnerNodeTypes(context.Background(), "tenant-a", base.Add(time.Second))
	if err != nil {
		t.Fatalf("LiveRunnerNodeTypes: %v", err)
	}
	if len(got) != 1 || got[0].Type != "acme.a" || got[0].Version != 1 {
		t.Fatalf("LiveRunnerNodeTypes(tenant-a) = %+v, want only acme.a v1", got)
	}
	if len(got[0].Pools) != 1 || got[0].Pools[0] != "pool-runner-a" {
		t.Fatalf("Pools = %v, want [pool-runner-a]", got[0].Pools)
	}

	expired, err := cp.LiveRunnerNodeTypes(context.Background(), "tenant-a", base.Add(DefaultRunnerLiveTTL+time.Second))
	if err != nil {
		t.Fatalf("LiveRunnerNodeTypes past TTL: %v", err)
	}
	if len(expired) != 0 {
		t.Fatalf("LiveRunnerNodeTypes past TTL = %+v, want none", expired)
	}
}

// descriptorlessDirectory hides every optional capability of the directory
// it wraps, RunnerDescriptorDirectory included.
type descriptorlessDirectory struct{ RunnerDirectory }

func TestControlPlaneLiveRunnerNodeTypesWithoutDescriptorDirectory(t *testing.T) {
	dir := NewMemoryRunnerDirectory()
	cp, err := NewControlPlane(Config{Backend: backendlocal.New(), RunnerDirectory: descriptorlessDirectory{dir}})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	now := time.Now().UTC()
	registerNodeTypesRunner(t, dir, "runner-a", "tenant-a", now, contractDescriptor("acme.a", 1, "A1"))

	got, err := cp.LiveRunnerNodeTypes(context.Background(), "tenant-a", now)
	if err != nil || got != nil {
		t.Fatalf("LiveRunnerNodeTypes = %+v, %v; want nil, nil", got, err)
	}
}

// The conflict gauge is computed over the unfiltered fleet: a type whose
// reporters disagree across two namespaces reads as conflicting whichever
// namespace was requested, even one that sees only a single hash.
func TestControlPlaneLiveRunnerNodeTypesGaugesTheWholeFleet(t *testing.T) {
	dir := NewMemoryRunnerDirectory()
	cp, err := NewControlPlane(Config{Backend: backendlocal.New(), RunnerDirectory: dir})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	observer := &recordingConflictObserver{}
	cp.runnerDescriptorConflicts = newRunnerDescriptorConflictReporter(nil, observer)
	base := time.Now().UTC()
	registerNodeTypesRunner(t, dir, "runner-a", "tenant-a", base, contractDescriptor("acme.t", 1, "A"))
	registerNodeTypesRunner(t, dir, "runner-b", "tenant-b", base, contractDescriptor("acme.t", 1, "B"))

	for i, ns := range []namespace.Namespace{"tenant-a", "tenant-b", "tenant-c"} {
		observer.got = nil
		// Step past the read cache so every request re-aggregates the fleet.
		now := base.Add(time.Second + time.Duration(i)*runnerNodeTypesCacheTTL)
		got, err := cp.LiveRunnerNodeTypes(context.Background(), ns, now)
		if err != nil {
			t.Fatalf("LiveRunnerNodeTypes(%s): %v", ns, err)
		}
		if ns != "tenant-c" && (len(got) != 1 || got[0].DistinctHashes != 1) {
			t.Fatalf("LiveRunnerNodeTypes(%s) = %+v, want one non-conflicting entry", ns, got)
		}
		if observer.got["acme.t"] != 1 {
			t.Fatalf("after a %s request: gauges = %v, want acme.t=1", ns, observer.got)
		}
	}
}
